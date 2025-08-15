from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Tuple, List
import logging, json, csv

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import listing, details, io
from .utils import throttle, setup_logging, DiskCache


def run_pipeline(
    start_url: str,
    pages: int,
    books_csv: Path,
    errors_csv: Path,
    tmin: float,
    tmax: float,
    *,
    jsonl_path: Path,
    min_mp3_size_bytes: int,
    require_full_mp3: bool,
    workers: int,
    log_file: Path | None,
    log_level: str,
    cache_dir: Path | None,
):
    """
    Full pipeline: crawl IDs in-memory -> enrich -> write CSV/JSONL
    - Skips incomplete rows (Book_Title/Player_Link empty) -> goes to errors.csv
    - After pass 1, runs up to 2 sweep passes over errors.csv with stronger backoff
    """
    # ---- logging + dirs ----
    setup_logging(log_file, log_level)
    log = logging.getLogger()

    for p in [books_csv, errors_csv, jsonl_path]:
        p.parent.mkdir(parents=True, exist_ok=True)
    if cache_dir:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)

    cache = DiskCache(cache_dir) if cache_dir else None

    # ---- crawl listing pages (in-memory) ----
    pairs: List[Tuple[int, str]] = listing.crawl_taglist(start_url, pages)
    log.info(f"[crawl] collected {len(pairs)} IDs (pages 1..{pages})")

    # ---- merge existing CSV if present (so we overwrite by AudioBook_ID) ----
    merged: Dict[int, Dict[str, Any]] = {}
    if books_csv.exists():
        try:
            df_prev = pd.read_csv(books_csv, encoding="utf-8-sig")
            for _, r in df_prev.iterrows():
                if "AudioBook_ID" in r and pd.notna(r["AudioBook_ID"]):
                    merged[int(r["AudioBook_ID"])] = dict(r)
            log.info(f"[merge] loaded {len(merged)} existing rows from {books_csv.name}")
        except Exception as e:
            log.warning(f"[merge] failed to load existing CSV: {e}")

    # ---- helper: process a single item ----
    def _process_one(
        item: Tuple[int, str],
        *,
        min_mp3_size_bytes: int,
        require_full_mp3: bool,
        cache: DiskCache | None,
    ):
        audio_id, url = item
        try:
            html = cache.get(url) if cache else None
            if not html:
                resp = details.req_get(url)
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}")
                html = resp.text
                if cache:
                    cache.set(url, html)

            parsed = details.parse_details_page(html, url)
            if not parsed.get("AudioBook_ID"):
                parsed["AudioBook_ID"] = audio_id

            attid = parsed.get("AudioBook_attID")
            parsed["Player_Link"] = details.build_player_link(
                parsed.get("AudioBook_ID"), attid
            )

            # MP3s via official API
            mp3s = (
                details.get_mp3s_from_api(parsed.get("AudioBook_ID"), attid)
                if (parsed.get("AudioBook_ID") and attid)
                else []
            )
            mp3s = [m for m in mp3s if (m.get("size") or 0) >= min_mp3_size_bytes]
            if require_full_mp3 and not mp3s:
                return ("skipped", audio_id, "no mp3 meets filters", parsed)

            if mp3s:
                best = max(
                    mp3s,
                    key=lambda m: (m.get("size") or 0, int(m.get("bitrate") or 0)),
                )
                parsed["FullBook_MP3_URL"] = best["url"]
                parsed["All_MP3s_Found"] = ", ".join(
                    dict.fromkeys([m["url"] for m in mp3s])
                )
            else:
                parsed["FullBook_MP3_URL"] = None
                parsed["All_MP3s_Found"] = None

            # minimal completeness validation:
            if not parsed.get("Book_Title") or not parsed.get("Player_Link"):
                return (
                    "err",
                    audio_id,
                    "parsed record incomplete (missing title/player)",
                    parsed,
                )

            return ("ok", audio_id, None, parsed)
        except Exception as e:
            return ("err", audio_id, str(e), {"AudioBook_ID": audio_id, "Source_URL": url})

    # ---- PASS 1 (parallel mild) ----
    success = 0
    failed = 0
    skipped = 0

    # JSONL append handle for pass 1
    jsonl_f = jsonl_path.open("a", encoding="utf-8")
    pending_errors: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(workers, 1)) as ex:
        futs = [
            ex.submit(
                _process_one,
                it,
                min_mp3_size_bytes=min_mp3_size_bytes,
                require_full_mp3=require_full_mp3,
                cache=cache,
            )
            for it in pairs
        ]
        for fut in as_completed(futs):
            status, audio_id, msg, parsed = fut.result()
            if status == "ok":
                success += 1
                merged[int(parsed["AudioBook_ID"])] = parsed
                io.atomic_write_csv(books_csv, list(merged.values()), details.CSV_FIELDS)
                jsonl_f.write(json.dumps(parsed, ensure_ascii=False) + "\n")
                jsonl_f.flush()
                logging.info(
                    f"✓ {parsed.get('AudioBook_ID')}  «{(parsed.get('Book_Title') or '')[:40]}»"
                )
            elif status == "skipped":
                skipped += 1
                pending_errors.append(
                    {
                        "AudioBook_ID": audio_id,
                        "URL": parsed.get("Source_URL", ""),
                        "Error": msg,
                    }
                )
                logging.info(f"— skipped {audio_id}: {msg}")
            else:
                failed += 1
                pending_errors.append(
                    {
                        "AudioBook_ID": audio_id,
                        "URL": parsed.get("Source_URL", ""),
                        "Error": msg,
                    }
                )
                logging.error(f"✗ {audio_id}: {msg}")
            throttle(tmin, tmax)

    jsonl_f.close()

    # write errors for pass 1 (UTF-8-SIG for Persian readability)
    if pending_errors:
        with errors_csv.open("w", newline="", encoding="utf-8-sig") as ef:
            writer = csv.DictWriter(ef, fieldnames=["AudioBook_ID", "URL", "Error"])
            writer.writeheader()
            for row in pending_errors:
                writer.writerow(row)

    log.info(
        f"[pass 1] Success: {success} | Skipped: {skipped} | Failed: {failed} | Total: {len(pairs)}"
    )

    # ---- SWEEPS: up to 2 more passes over errors.csv ----
    MAX_SWEEPS = 2
    for sweep in range(1, MAX_SWEEPS + 1):
        try:
            err_df = pd.read_csv(errors_csv, encoding="utf-8-sig")
        except Exception:
            err_df = None

        if err_df is None or err_df.empty:
            logging.info(f"[sweep] no errors to retry; stopping.")
            break

        to_retry: List[Tuple[int, str]] = [
            (int(r["AudioBook_ID"]), str(r["URL"]))
            for _, r in err_df.iterrows()
            if pd.notna(r["URL"])
        ]
        logging.info(
            f"[sweep {sweep}] retrying {len(to_retry)} items… (serial, stronger backoff)"
        )

        # stronger throttle: slower than pass 1
        sweep_min = max(tmin * 3, 1.0)
        sweep_max = max(tmax * 6, sweep_min + 0.5)

        new_errors: List[Dict[str, Any]] = []
        for item in to_retry:
            status, audio_id, msg, parsed = _process_one(
                item,
                min_mp3_size_bytes=min_mp3_size_bytes,
                require_full_mp3=require_full_mp3,
                cache=cache,
            )
            if status == "ok":
                success += 1
                merged[int(parsed["AudioBook_ID"])] = parsed
                io.atomic_write_csv(books_csv, list(merged.values()), details.CSV_FIELDS)
                with jsonl_path.open("a", encoding="utf-8") as jf:
                    jf.write(json.dumps(parsed, ensure_ascii=False) + "\n")
                logging.info(f"[sweep {sweep}] ✓ {parsed.get('AudioBook_ID')}")
            else:
                new_errors.append(
                    {
                        "AudioBook_ID": audio_id,
                        "URL": parsed.get("Source_URL", "") if isinstance(parsed, dict) else "",
                        "Error": msg,
                    }
                )
                logging.warning(f"[sweep {sweep}] ✗ {audio_id}: {msg}")
            throttle(sweep_min, sweep_max)

        # rewrite errors.csv with remaining (UTF-8-SIG)
        with errors_csv.open("w", newline="", encoding="utf-8-sig") as ef:
            writer = csv.DictWriter(ef, fieldnames=["AudioBook_ID", "URL", "Error"])
            writer.writeheader()
            for row in new_errors:
                writer.writerow(row)

        if not new_errors:
            logging.info(f"[sweep {sweep}] all errors resolved; stopping.")
            break

    log.info(
        f"✅ Done. Success (incl. sweeps): {success} | Remaining errors: "
        f"{0 if not errors_csv.exists() else pd.read_csv(errors_csv, encoding='utf-8-sig').shape[0]} | Total initial IDs: {len(pairs)}"
    )

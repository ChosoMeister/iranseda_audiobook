
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List, Tuple
import logging, json

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import listing, details, io
from .utils import throttle, setup_logging, DiskCache

def run_pipeline(start_url: str, pages: int, books_csv: Path, errors_csv: Path,
                 tmin: float, tmax: float, *, jsonl_path: Path, min_mp3_size_bytes: int,
                 require_full_mp3: bool, workers: int, log_file: Path | None, log_level: str,
                 cache_dir: Path | None):
    print("[IRANSEDA] Collecting listing pages and enriching…")
    setup_logging(log_file, log_level)
    log = logging.getLogger()

    cache = DiskCache(cache_dir) if cache_dir else None

    # Crawl IDs in-memory (pages 1..N)
    pairs = listing.crawl_taglist(start_url, pages)
    log.info(f"Collected {len(pairs)} IDs in-memory from 1..{pages}")

    # Enrich (parallel mild)
    merged: Dict[int, Dict[str,Any]] = {}
    if books_csv.exists():
        try:
            df = pd.read_csv(books_csv, encoding="utf-8")
            for _, r in df.iterrows():
                if "AudioBook_ID" in r and not pd.isna(r["AudioBook_ID"]):
                    merged[int(r["AudioBook_ID"])] = dict(r)
        except Exception:
            pass

    err_f, err_writer = io.ensure_error_csv(errors_csv)
    jsonl_f = jsonl_path.open("a", encoding="utf-8")

    success = 0; failed = 0; skipped = 0

def _process_one(item: Tuple[int,str], *, min_mp3_size_bytes: int, require_full_mp3: bool, cache, tmin: float, tmax: float):
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
        parsed["Player_Link"] = details.build_player_link(parsed.get("AudioBook_ID"), attid)

        mp3s = details.get_mp3s_from_api(parsed.get("AudioBook_ID"), attid) if (parsed.get("AudioBook_ID") and attid) else []
        mp3s = [m for m in mp3s if (m.get("size") or 0) >= min_mp3_size_bytes]
        if require_full_mp3 and not mp3s:
            return ("skipped", audio_id, "no mp3 meets filters", parsed)

        if mp3s:
            best = max(mp3s, key=lambda m: (m.get("size") or 0, int(m.get("bitrate") or 0)))
            parsed["FullBook_MP3_URL"] = best["url"]
            parsed["All_MP3s_Found"] = ", ".join(dict.fromkeys([m["url"] for m in mp3s]))
        else:
            parsed["FullBook_MP3_URL"] = None
            parsed["All_MP3s_Found"] = None

        # minimal completeness validation
        if not parsed.get("Book_Title") or not parsed.get("Player_Link"):
            return ("err", audio_id, "parsed record incomplete (missing title/player)", parsed)

        return ("ok", audio_id, None, parsed)
    except Exception as e:
        return ("err", audio_id, str(e), {"AudioBook_ID": audio_id, "Source_URL": url})


    def process(item: Tuple[int,str]):
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
            parsed["Player_Link"] = details.build_player_link(parsed.get("AudioBook_ID"), attid)

            mp3s = details.get_mp3s_from_api(parsed.get("AudioBook_ID"), attid) if (parsed.get("AudioBook_ID") and attid) else []
            mp3s = [m for m in mp3s if (m.get("size") or 0) >= min_mp3_size_bytes]
            if require_full_mp3 and not mp3s:
                return ("skipped", audio_id, "no mp3 meets filters", parsed)

            if mp3s:
                best = max(mp3s, key=lambda m: (m.get("size") or 0, int(m.get("bitrate") or 0)))
                parsed["FullBook_MP3_URL"] = best["url"]
                parsed["All_MP3s_Found"] = ", ".join(dict.fromkeys([m["url"] for m in mp3s]))
            else:
                parsed["FullBook_MP3_URL"] = None
                parsed["All_MP3s_Found"] = None

            return ("ok", audio_id, None, parsed)
        except Exception as e:
            return ("err", audio_id, str(e), {"AudioBook_ID": audio_id, "Source_URL": url})

    
with ThreadPoolExecutor(max_workers=max(workers,1)) as ex:
    futs = [ex.submit(_process_one, it, min_mp3_size_bytes=min_mp3_size_bytes, require_full_mp3=require_full_mp3, cache=cache, tmin=tmin, tmax=tmax) for it in pairs]
    pending_errors = []
    for fut in as_completed(futs):
        status, audio_id, msg, parsed = fut.result()
        if status == "ok":
            success += 1
            merged[int(parsed["AudioBook_ID"])] = parsed
            io.atomic_write_csv(books_csv, list(merged.values()), details.CSV_FIELDS)
            jsonl_f.write(json.dumps(parsed, ensure_ascii=False) + "
")
            jsonl_f.flush()
            logging.info(f"✓ {parsed.get('AudioBook_ID')}  «{(parsed.get('Book_Title') or '')[:40]}»")
        elif status == "skipped":
            skipped += 1
            pending_errors.append({"AudioBook_ID": audio_id, "URL": parsed.get('Source_URL',''), "Error": msg})
            logging.info(f"— skipped {audio_id}: {msg}")
        else:
            failed += 1
            pending_errors.append({"AudioBook_ID": audio_id, "URL": parsed.get('Source_URL',''), "Error": msg})
            logging.error(f"✗ {audio_id}: {msg}")
        throttle(tmin, tmax)
    # Write errors for pass 1
    if pending_errors:
        err_f, err_writer = io.ensure_error_csv(errors_csv)
        for row in pending_errors:
            err_writer.writerow(row)
        err_f.close()

jsonl_f.close()

# === SWEEP PASSES (up to 2 more passes) ===
import pandas as pd, csv
MAX_SWEEPS = 2  # total passes = 1 + MAX_SWEEPS
for sweep in range(1, MAX_SWEEPS+1):
    try:
        err_df = pd.read_csv(errors_csv, encoding="utf-8")
    except Exception:
        err_df = None
    if err_df is None or err_df.empty:
        logging.info(f"[sweep] no errors to retry; stopping.")
        break
    to_retry = [(int(r["AudioBook_ID"]), r["URL"]) for _, r in err_df.iterrows() if pd.notna(r["URL"])]
    logging.info(f"[sweep {sweep}] retrying {len(to_retry)} items… (serial, stronger backoff)")

    # Stronger throttle for sweep
    sweep_min = max(tmin*3, 1.0)
    sweep_max = max(tmax*6, sweep_min+0.5)

    new_errors = []
    for item in to_retry:
        status, audio_id, msg, parsed = _process_one(item, min_mp3_size_bytes=min_mp3_size_bytes, require_full_mp3=require_full_mp3, cache=cache, tmin=sweep_min, tmax=sweep_max)
        if status == "ok":
            success += 1
            merged[int(parsed["AudioBook_ID"])] = parsed
            io.atomic_write_csv(books_csv, list(merged.values()), details.CSV_FIELDS)
            with jsonl_path.open("a", encoding="utf-8") as jf:
                jf.write(json.dumps(parsed, ensure_ascii=False) + "
")
            logging.info(f"[sweep {sweep}] ✓ {parsed.get('AudioBook_ID')}")
        else:
            new_errors.append({"AudioBook_ID": audio_id, "URL": parsed.get('Source_URL',''), "Error": msg})
            logging.warning(f"[sweep {sweep}] ✗ {audio_id}: {msg}")
        throttle(sweep_min, sweep_max)

    # Rewrite errors.csv with remaining
    with open(errors_csv, "w", newline="", encoding="utf-8-sig") as ef:
        writer = csv.DictWriter(ef, fieldnames=["AudioBook_ID","URL","Error"])
        writer.writeheader()
        for row in new_errors:
            writer.writerow(row)

    if not new_errors:
        logging.info(f"[sweep {sweep}] all errors resolved; stopping.")
        break

log.info(f"✅ Done. Success: {success} | Skipped: {skipped} | Failed (remaining): {len(new_errors) if 'new_errors' in locals() else 0} | Total: {len(pairs)}")
(f"✅ Done. Success: {success} | Skipped: {skipped} | Failed: {failed} | Total: {len(pairs)}")

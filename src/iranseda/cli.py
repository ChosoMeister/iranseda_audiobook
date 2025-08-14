
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

from . import listing, details, io, pipeline, config
from .utils import setup_logging

def cmd_crawl(args):
    setup_logging(Path(args.log_file) if args.log_file else None, args.log_level)
    pairs = listing.crawl_taglist(args.url, args.pages)
    out_csv = Path(args.output)
    out_csv.write_text("AudioBookID,URL\n", encoding="utf-8")
    with out_csv.open("a", newline="", encoding="utf-8") as f:
        for g,u in pairs:
            f.write(f"{g},{u}\n")
    print(f"✓ wrote {len(pairs)} ids to {out_csv}")

def cmd_enrich(args):
    setup_logging(Path(args.log_file) if args.log_file else None, args.log_level)
    ids = pd.read_csv(args.input, encoding="utf-8")
    out_csv = Path(args.output)
    jsonl_path = Path(args.jsonl)
    merged = {}
    if out_csv.exists():
        try:
            df = pd.read_csv(out_csv, encoding="utf-8")
            for _, r in df.iterrows():
                if "AudioBook_ID" in r and not pd.isna(r["AudioBook_ID"]):
                    merged[int(r["AudioBook_ID"])] = dict(r)
        except Exception:
            pass
    err_f, err_writer = io.ensure_error_csv(Path(args.errors))

    import json, logging
    from .utils import DiskCache, throttle
    cache = DiskCache(Path(args.cache_dir)) if args.cache else None

    total = len(ids)
    for idx, row in ids.iterrows():
        audio_id = int(row["AudioBookID"])
        url = listing.fix_url(str(row["URL"]))
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
            mp3s = [m for m in mp3s if (m.get("size") or 0) >= args.min_mp3_size]
            if args.require_full and not mp3s:
                logging.info(f"— skipped {audio_id}: no mp3 meets filters")
                continue

            if mp3s:
                best = max(mp3s, key=lambda m: (m.get("size") or 0, int(m.get("bitrate") or 0)))
                parsed["FullBook_MP3_URL"] = best["url"]
                parsed["All_MP3s_Found"] = ", ".join(dict.fromkeys([m["url"] for m in mp3s]))
            else:
                parsed["FullBook_MP3_URL"] = None
                parsed["All_MP3s_Found"] = None

            merged[int(parsed["AudioBook_ID"])] = parsed
            io.atomic_write_csv(out_csv, list(merged.values()), details.CSV_FIELDS)
            jsonl_path.open("a", encoding="utf-8").write(json.dumps(parsed, ensure_ascii=False) + "\n")
            logging.info(f"[{idx+1}/{total}] ✓ {parsed.get('AudioBook_ID')}  «{(parsed.get('Book_Title') or '')[:40]}»")
        except Exception as e:
            err_writer.writerow({"AudioBook_ID": audio_id, "URL": url, "Error": str(e)})
            err_f.flush()
            logging.error(f"[{idx+1}/{total}] ✗ {audio_id}: {e}")
        throttle(args.min_delay, args.max_delay)

    err_f.close()
    print(f"done. wrote {len(merged)} rows to {out_csv}")

def cmd_run(args):
    cfg = config.load_config(args.config)

    print(f"[IRANSEDA] Starting RUN with config: {args.config}")
    from pathlib import Path
    for p in [cfg.outputs.books_csv, cfg.outputs.errors_csv, cfg.outputs.jsonl, cfg.cache.dir]:
        if p:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            
    pipeline.run_pipeline(
        cfg.start_url_template,
        cfg.pages,
        Path(cfg.outputs.books_csv),
        Path(cfg.outputs.errors_csv),
        cfg.throttle.min,
        cfg.throttle.max,
        jsonl_path=Path(cfg.outputs.jsonl),
        min_mp3_size_bytes=cfg.filters.min_mp3_size_bytes,
        require_full_mp3=cfg.filters.require_full_mp3,
        workers=cfg.parallel.workers,
        log_file=Path(cfg.logging.file) if cfg.logging.file else None,
        log_level=cfg.logging.level,
        cache_dir=Path(cfg.cache.dir) if cfg.cache.enabled else None,
    )

def build_parser():
    p = argparse.ArgumentParser(prog="iranseda", description="IranSeda audiobook crawler & enricher")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("crawl", help="Crawl taglist pages to collect audiobook IDs")
    p1.add_argument("--url", required=True, help="Listing URL template with {page} placeholder")
    p1.add_argument("--pages", type=int, required=True, help="Number of pages to crawl")
    p1.add_argument("--output", default="audiobooks.csv")
    p1.add_argument("--log-level", default="INFO")
    p1.add_argument("--log-file", default=None)
    p1.set_defaults(func=cmd_crawl)

    p2 = sub.add_parser("enrich", help="Enrich IDs CSV to full book metadata + MP3 links")
    p2.add_argument("--input", default="audiobooks.csv")
    p2.add_argument("--output", default="books_with_attid.csv")
    p2.add_argument("--errors", default="errors.csv")
    p2.add_argument("--jsonl", default="books_with_attid.jsonl")
    p2.add_argument("--min-delay", type=float, default=0.1, dest="min_delay")
    p2.add_argument("--max-delay", type=float, default=0.3, dest="max_delay")
    p2.add_argument("--min-mp3-size", type=int, default=0, dest="min_mp3_size")
    p2.add_argument("--require-full", action="store_true", default=False)
    p2.add_argument("--workers", type=int, default=1)
    p2.add_argument("--cache", action="store_true", default=False)
    p2.add_argument("--cache-dir", default=".cache/details")
    p2.add_argument("--log-level", default="INFO")
    p2.add_argument("--log-file", default=None)
    p2.set_defaults(func=cmd_enrich)

    p3 = sub.add_parser("run", help="Run full pipeline (crawl + enrich) using YAML config")
    p3.add_argument("--config", required=True, help="Path to YAML config")
    p3.set_defaults(func=cmd_run)
    return p

def main(argv: list[str] | None = None):
    args = build_parser().parse_args(argv)
    args.func(args)

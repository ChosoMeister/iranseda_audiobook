import csv
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from . import details, io


def run_pipeline(cfg):
    """Run scraping pipeline with multi-pass retry based on config."""
    max_passes = cfg.get("pipeline", {}).get("max_passes", 3)
    delay_first_min = cfg.get("pipeline", {}).get("delay_first_min", 0.1)
    delay_first_max = cfg.get("pipeline", {}).get("delay_first_max", 0.3)
    delay_retry_min = cfg.get("pipeline", {}).get("delay_retry_min", 1.0)
    delay_retry_max = cfg.get("pipeline", {}).get("delay_retry_max", 2.0)
    workers_first = cfg.get("pipeline", {}).get("workers_first", cfg.get("workers", 2))
    workers_retry = cfg.get("pipeline", {}).get("workers_retry", 1)

    outputs = cfg.get("outputs", {})
    books_csv = Path(outputs.get("books_csv", "books_with_attid.csv"))
    errors_csv = Path(outputs.get("errors_csv", "errors.csv"))
    jsonl_file = Path(outputs.get("jsonl", "books_with_attid.jsonl"))

    # Ensure output dirs exist
    for p in [books_csv, errors_csv, jsonl_file]:
        p.parent.mkdir(parents=True, exist_ok=True)

    # Pass loop
    for attempt in range(1, max_passes + 1):
        is_retry = attempt > 1
        print(f"\n=== Pass {attempt} of {max_passes} ===")

        # Determine source file for this pass
        if is_retry:
            if not errors_csv.exists() or errors_csv.stat().st_size == 0:
                print("[INFO] No errors to retry. Stopping.")
                break
            source_file = errors_csv
        else:
            source_file = cfg["inputs"]["id_list_csv"]

        # Load IDs
        with open(source_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            id_rows = list(reader)

        if not id_rows:
            print("[INFO] No IDs to process. Stopping.")
            break

        # Prepare output mode
        mode = "a" if books_csv.exists() and is_retry else "w"
        jsonl_mode = "a" if jsonl_file.exists() and is_retry else "w"

        with open(books_csv, mode, newline="", encoding="utf-8-sig") as csv_f, \
             open(errors_csv, "w", newline="", encoding="utf-8-sig") as err_f, \
             open(jsonl_file, jsonl_mode, encoding="utf-8-sig") as jsonl_f:

            fieldnames = [
                "Book_Title", "Book_Subtitle", "Author", "Narrator",
                "Description", "Category", "Cover_Image", "Player_Link",
                "MP3_Link", "Source_URL"
            ]
            csv_writer = csv.DictWriter(csv_f, fieldnames=fieldnames)
            err_writer = csv.DictWriter(err_f, fieldnames=source_file.read_text(encoding="utf-8-sig").splitlines()[0].split(","))
            if mode == "w":
                csv_writer.writeheader()
            err_writer.writeheader()

            # Select worker count and delay range
            if is_retry:
                workers = max(workers_retry, 1)
                delay_min, delay_max = delay_retry_min, delay_retry_max
            else:
                workers = max(workers_first, 1)
                delay_min, delay_max = delay_first_min, delay_first_max

            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {
                    ex.submit(details.fetch_book_details, row, cfg): row for row in id_rows
                }
                for fut in as_completed(futures):
                    row = futures[fut]
                    try:
                        parsed = fut.result()
                        # Filter out incomplete
                        if not parsed.get("Book_Title") or not parsed.get("Player_Link"):
                            err_writer.writerow(row)
                        else:
                            csv_writer.writerow(parsed)
                            jsonl_f.write(json.dumps(parsed, ensure_ascii=False) + "\n")
                    except Exception as e:
                        print(f"[ERROR] {row} -> {e}")
                        err_writer.writerow(row)
                    time.sleep(random.uniform(delay_min, delay_max))

        # If errors file empty, break
        if errors_csv.stat().st_size == 0:
            print("[INFO] All done, no errors left.")
            break

    print("[INFO] Pipeline finished.")

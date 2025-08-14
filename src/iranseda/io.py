from __future__ import annotations
import csv
from pathlib import Path
from typing import Iterable, Dict, List

def atomic_write_csv(csv_path: Path, rows: Iterable[Dict], fieldnames: List[str]) -> None:
    tmp = csv_path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
        f.flush()
    tmp.replace(csv_path)

def ensure_error_csv(path: Path):
    newfile = not path.exists()
    f = path.open("a", newline="", encoding="utf-8-sig")
    writer = csv.DictWriter(f, fieldnames=["AudioBook_ID","URL","Error"])
    if newfile:
        writer.writeheader()
        f.flush()
    return f, writer

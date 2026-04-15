import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def canonicalize_title(title: str) -> str:
    import re

    t = (title or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def parse_date_safe(s: str):
    try:
        return datetime.strptime((s or "0000-01-01"), "%Y-%m-%d")
    except Exception:
        return datetime.strptime("0000-01-01", "%Y-%m-%d")


def read_csv(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def write_csv(path: Path, rows: List[Dict[str, str]]):
    fieldnames = ["date", "title", "url"]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def dedupe_by_title(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def score(r: Dict[str, str]):
        dt = parse_date_safe(r.get("date", ""))
        title_len = len((r.get("title") or "").strip())
        return (dt, title_len)

    best: Dict[str, Dict[str, str]] = {}
    for r in rows:
        key = canonicalize_title((r.get("title") or r.get("url") or "").strip())
        if not key:
            continue
        prev = best.get(key)
        if not prev or score(r) > score(prev):
            best[key] = r

    out = list(best.values())
    out.sort(key=lambda r: (parse_date_safe(r.get("date", "")).date().isoformat(), (r.get("title") or "").lower()), reverse=True)
    return out


def main():
    ap = argparse.ArgumentParser(description="Remove duplicate rows by canonicalized title and sort by date desc.")
    ap.add_argument("input", type=str, help="Input CSV path")
    ap.add_argument("--output", type=str, default=None, help="Output CSV path (default: overwrite input)")
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.output) if args.output else inp
    rows = read_csv(inp)
    deduped = dedupe_by_title(rows)
    write_csv(out, deduped)
    print(f"Deduped by title {len(rows)} -> {len(deduped)} rows and wrote to {out.as_posix()}")


if __name__ == "__main__":
    main()







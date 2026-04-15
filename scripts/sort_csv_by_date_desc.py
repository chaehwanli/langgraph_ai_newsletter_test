import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sort a CSV by 'date' column in descending order.")
    parser.add_argument("input", type=str, help="Input CSV path")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV path (default: overwrite input)",
    )
    return parser.parse_args()


def read_csv(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        # Default header when file is empty
        fieldnames = ["date", "title", "url"]
    else:
        fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def sort_by_date_desc(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def parse_date(s: str) -> datetime:
        try:
            return datetime.strptime((s or "0000-01-01"), "%Y-%m-%d")
        except Exception:
            return datetime.strptime("0000-01-01", "%Y-%m-%d")

    return sorted(rows, key=lambda r: (parse_date(r.get("date", "")).date().isoformat(), (r.get("title") or "").lower()), reverse=True)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    rows = read_csv(input_path)
    sorted_rows = sort_by_date_desc(rows)
    write_csv(output_path, sorted_rows)
    print(f"Sorted {len(sorted_rows)} rows by date desc -> {output_path.as_posix()}")


if __name__ == "__main__":
    main()



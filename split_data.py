#!/usr/bin/env python3
import argparse
import csv
import os
import time
from collections import OrderedDict
from datetime import datetime
from typing import Optional

def parse_year(dt_str: str) -> int:
    """
    Split dataset into years

    Parse year from strings like:
      - "1996-08-09 13:30:00"
      - "1996-08-09T13:30:00"
      - "1996-08-09"
    """

    s = (dt_str or "").strip()
    if not s:
        raise ValueError("Empty datetime string")

    # Fast-path: ISO-ish formats
    # fromisoformat supports "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DDTHH:MM:SS"
    try:
        return datetime.fromisoformat(s).year
    except ValueError:
        pass

    # Fallback formats if CSV is inconsistent
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).year
        except ValueError:
            continue

    # Last-resort: take first 4 chars if they look like a year
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])

    raise ValueError(f"Unrecognized datetime format: {dt_str!r}")

class WriterCache:
    def __init__(self, out_dir: str, header: list[str], max_open: int = 8, max_rows_per_file: Optional[int] = None):
        self.out_dir = out_dir
        self.header = header
        self.max_open = max_open
        self.max_rows_per_file = max_rows_per_file

        # Tracks (rows_written, chunk_idx) per year
        self._year_state: dict[int, tuple[int, int]] = {}

        # LRU cache: year -> (file handle, csv writer)
        self._cache: "OrderedDict[int, tuple[object, csv.DictWriter]]" = OrderedDict()

    def get_writer(self, year: int) -> csv.DictWriter:
        rows_written, chunk_idx = self._year_state.get(year, (0, 0))

        # Roll over to a new file if max_rows_per_file reached
        if self.max_rows_per_file and rows_written >= self.max_rows_per_file:
            # close existing file if open
            if year in self._cache:
                fh, _ = self._cache.pop(year)
                fh.close()
            chunk_idx += 1
            rows_written = 0

        if year in self._cache:
            fh, writer = self._cache.pop(year)
            self._cache[year] = (fh, writer)  # mark as most-recent
        else:
            # Evict LRU if needed
            while len(self._cache) >= self.max_open:
                old_year, (old_fh, _) = self._cache.popitem(last=False)
                old_fh.close()

            os.makedirs(self.out_dir, exist_ok=True)
            out_path = os.path.join(self.out_dir, f"weather_{year}_part_{chunk_idx:02d}.csv")
            file_exists = os.path.exists(out_path)
            fh = open(out_path, "a", newline="", encoding="utf-8")
            writer = csv.DictWriter(fh, fieldnames=self.header, extrasaction="ignore")

            # Write header only once when file is first created
            if not file_exists or os.path.getsize(out_path) == 0:
                writer.writeheader()

            self._cache[year] = (fh, writer)
        
        self._year_state[year] = (rows_written, chunk_idx)
        return self._cache[year][1]

    def increment_row(self, year: int):
        rows_written, chunk_idx = self._year_state.get(year, (0, 0))
        self._year_state[year] = (rows_written + 1, chunk_idx)

    def close_all(self) -> None:
        for fh, _ in self._cache.values():
            fh.close()
        self._cache.clear()

def split_csv_by_year(
    input_csv: str,
    out_dir: str,
    datetime_col: str = "datetime",
    max_open: int = 8,
    max_rows_per_file: Optional[int] = None,
    bad_rows_csv: Optional[str] = None
) -> None:
    os.makedirs(out_dir, exist_ok=True)

    total = 0
    bad = 0
    start_time = time.time()
    PROGRESS_INTERVAL = 500000 

    print(f"Starting split process for: {input_csv}")
    print(f"Output directory: {out_dir}")
    print(f"{'='*60}")

    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV appears to have no header row.")

        if datetime_col not in reader.fieldnames:
            raise RuntimeError(
                f"Missing required column {datetime_col!r}. "
                f"Found columns: {reader.fieldnames}"
            )

        header = reader.fieldnames
        cache = WriterCache(out_dir=out_dir, header=header, max_open=max_open, max_rows_per_file=max_rows_per_file)

        bad_writer = None
        bad_fh = None
        if bad_rows_csv:
            bad_fh = open(bad_rows_csv, "w", newline="", encoding="utf-8")
            bad_writer = csv.DictWriter(bad_fh, fieldnames=header + ["_error"])
            bad_writer.writeheader()

        try:
            for row in reader:
                total += 1

                if total % PROGRESS_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    speed = total / elapsed if elapsed > 0 else 0
                    print(f"Processed {total:,} rows... ({speed:.0f} rows/sec)")

                try:
                    year = parse_year(row.get(datetime_col, ""))
                    writer = cache.get_writer(year)
                    writer.writerow(row)
                    cache.increment_row(year)
                except Exception as e:
                    bad += 1
                    if bad_writer:
                        row2 = dict(row)
                        row2["_error"] = str(e)
                        bad_writer.writerow(row2)
                    # otherwise: silently skip bad row
        finally:
            cache.close_all()
            if bad_fh:
                bad_fh.close()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Splitting done!")
    print(f"{'='*60}")
    print(f"Rows processed: {total:,}")
    print(f"Bad rows: {bad:,}")
    print(f"Output directory: {out_dir}")
    print(f"Time taken: {total_time:.2f}s")

def main():
    ap = argparse.ArgumentParser(description="Split a large weather CSV into per-year CSVs.")
    ap.add_argument("--input", required=True, help="Path to input CSV (on NFS).")
    ap.add_argument("--outdir", required=True, help="Directory to write split CSVs (on NFS).")
    ap.add_argument("--datetime-col", default="datetime", help="Datetime column name (default: datetime).")
    ap.add_argument("--max-open", type=int, default=8, help="Max number of output files kept open (default: 8).")
    ap.add_argument("--max-rows-per-file", type=int, default=None, help="Max rows per year file before splitting into part files.")
    ap.add_argument("--bad-rows", default=None, help="Optional path to write rows that fail parsing.")
    args = ap.parse_args()

    split_csv_by_year(
        input_csv=args.input,
        out_dir=args.outdir,
        datetime_col=args.datetime_col,
        max_open=args.max_open,
        max_rows_per_file=args.max_rows_per_file,
        bad_rows_csv=args.bad_rows
    )

if __name__ == "__main__":
    main()

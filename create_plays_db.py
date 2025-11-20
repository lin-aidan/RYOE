#!/usr/bin/env python3
"""
Create an SQLite database from team_data_combined/plays.csv with a single table 'plays'.

Features:
- Chunked CSV loading to keep memory usage modest.
- Heuristic conversion of object columns to numeric where appropriate, so numbers are stored as numbers in SQLite.
- Reports row count and inferred SQLite column types upon completion.

Usage (from repo root):
  python3 create_plays_db.py

Outputs:
- plays.db in the repo root containing table 'plays'.
"""

from __future__ import annotations

import os
import re
import sqlite3
from typing import Iterable

import pandas as pd


CSV_PATH = os.path.join("team_data_combined", "plays.csv")
DB_PATH = "plays.db"
TABLE_NAME = "plays"


def looks_numeric(series: pd.Series, sample_size: int = 1000, threshold: float = 0.95) -> bool:
    """Return True if the object-dtype series appears numeric based on a sample.

    We count values matching a permissive numeric regex (ints/floats with optional sign).
    If >= threshold of sampled non-null values look numeric, we consider the whole column numeric.
    """
    if series.empty:
        return False
    # Work on strings for regex matching
    sample = series.dropna().astype(str)
    if sample.empty:
        return False
    sample = sample.head(sample_size)
    numeric_pat = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
    matches = sum(1 for v in sample if numeric_pat.match(v.strip()))
    return (matches / len(sample)) >= threshold


def coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Attempt to convert object columns that look numeric into numeric dtypes.

    - Avoids converting mixed text columns.
    - Preserves non-numeric object columns as-is.
    """
    obj_cols = [c for c in df.columns if df[c].dtype == "object"]
    for col in obj_cols:
        s = df[col]
        if looks_numeric(s):
            df[col] = pd.to_numeric(s, errors="coerce")
    return df


def write_chunk(conn: sqlite3.Connection, chunk: pd.DataFrame, if_exists: str) -> None:
    chunk.to_sql(TABLE_NAME, conn, if_exists=if_exists, index=False)


def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    # Remove existing DB to ensure a clean 'plays' table build
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        first = True
        total_rows = 0
        # Adjust chunksize based on available memory; 100_000 is usually fine for ~100MB CSV
        for chunk in pd.read_csv(CSV_PATH, chunksize=100_000, low_memory=False):
            # Try to upcast numeric-looking object columns
            chunk = coerce_numeric_columns(chunk)
            write_chunk(conn, chunk, if_exists="replace" if first else "append")
            first = False
            total_rows += len(chunk)

        # Verify results
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cur.fetchone()[0]

        cur.execute(f"PRAGMA table_info({TABLE_NAME})")
        schema_rows = cur.fetchall()

        print(f"Created {DB_PATH} with table '{TABLE_NAME}'.")
        print(f"Imported rows: {count} (from streaming total {total_rows}).")
        print("Columns:")
        for cid, name, ctype, notnull, dflt, pk in schema_rows:
            print(f"  - {name}: {ctype}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

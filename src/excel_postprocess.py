#!/usr/bin/env python3
"""
Convert 'retrieved_at' from timezone aware to naive and create a YYYYMMDD
date_id column.

Example:
    python src/excel_postprocess.py \
        --src result/intermediate.xlsx \
        --dst result/parsed.xlsx
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def make_date_id(dt):
    if pd.isna(dt):
        return np.nan
    return dt.year * 10000 + dt.month * 100 + dt.day


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--dst", required=True)
    ap.add_argument("--sheet", default=0)
    args = ap.parse_args()

    df = pd.read_excel(args.src, sheet_name=args.sheet, engine="openpyxl")

    if "retrieved_at" not in df.columns:
        raise KeyError("Column 'retrieved_at' not found")

    df["retrieved_datetime"] = (
        pd.to_datetime(df["retrieved_at"], errors="coerce", utc=True)
          .dt.tz_localize(None)
    )
    df["date_id"] = df["retrieved_datetime"].apply(make_date_id)

    out = Path(args.dst)
    if out.suffix.lower() == ".csv":
        df.to_csv(out, index=False)
    else:
        df.to_excel(out, index=False)

    print("Saved cleaned file:", out)


if __name__ == "__main__":
    main()

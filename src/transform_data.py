#!/usr/bin/env python3
"""
transform_data.py
-----------------
Read a line-delimited JSON export from OffeneRegister.de (or an SQLite dump
converted to JSON), clean & deduplicate it (Kimball SCD-2), and write a
partitioned Apache Parquet file.

Example:
    python src/transform_data.py \
        --input data/raw/companies.jsonl \
        --output data/curated/companies_scd2.parquet \
        --chunk 100000
"""
from __future__ import annotations
import argparse, logging
from pathlib import Path
from datetime import timedelta

import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm
import pyarrow  # ensures pyarrow is installed; otherwise Parquet fails


# ────────────────────────────────────────────────────────────── helpers
def normalise_name(name: str) -> str:
    """lower-case, keep only alphanumerics, collapse spaces"""
    clean = "".join(c.lower() if c.isalnum() else " " for c in str(name))
    return " ".join(clean.split())


def add_scd2(df: pd.DataFrame) -> pd.DataFrame:
    """append valid_from / valid_to / is_current columns (Type-2 history)"""
    df.sort_values(["cluster_key", "retrieved_at"], inplace=True)
    df["valid_from"] = pd.to_datetime(df["retrieved_at"], utc=True)
    df["valid_to"] = (
        df.groupby("cluster_key")["valid_from"]
          .shift(-1)
          .sub(timedelta(milliseconds=1))
    )
    df["is_current"] = df["valid_to"].isna()
    return df


def fuzzy_dedupe(df: pd.DataFrame, threshold: int = 92) -> pd.DataFrame:
    """within the same registration number remove rows whose names are >= threshold similar"""
    keep_mask = ~df.duplicated("cluster_key", keep="last")  # exact dupe first
    for reg, grp in df.groupby("registration_number"):
        idx = grp.index.tolist()
        for i in idx:
            if not keep_mask[i]:
                continue
            name_i = grp.at[i, "company_name"]
            for j in idx:
                if i == j or not keep_mask[j]:
                    continue
                if fuzz.token_sort_ratio(
                    name_i,
                    grp.at[j, "company_name"],
                    score_cutoff=threshold,
                ) >= threshold:
                    keep_mask[j] = False
    return df.loc[keep_mask]


# ────────────────────────────────────────────────────────────── main ETL
def run_etl(in_path: Path, out_path: Path, chunk_size: int):
    logging.info("Reading %s → writing %s", in_path, out_path)
    frames: list[pd.DataFrame] = []

    reader = pd.read_json(in_path, lines=True, chunksize=chunk_size)
    for chunk in tqdm(reader, desc="chunks", unit="chunk"):
        chunk["cluster_key"] = (
            chunk["company_name"].map(normalise_name)
            + "_"
            + chunk["registration_number"].astype(str)
        )
        frames.append(chunk)

    df = pd.concat(frames, ignore_index=True)
    logging.info("Raw rows: %d", len(df))

    df = fuzzy_dedupe(df)
    logging.info("After fuzzy deduplication: %d", len(df))

    add_scd2(df).to_parquet(out_path, index=False)
    logging.info("Parquet written: %s", out_path)


def main(argv: list[str] | None = None):
    ap = argparse.ArgumentParser(description="OffeneRegister JSON → Parquet ETL")
    ap.add_argument("--input", required=True, help="input JSONL path")
    ap.add_argument("--output", required=True, help="output Parquet file")
    ap.add_argument("--chunk", type=int, default=100000, help="rows per chunk")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    run_etl(Path(args.input), Path(args.output), args.chunk)


if __name__ == "__main__":
    main()

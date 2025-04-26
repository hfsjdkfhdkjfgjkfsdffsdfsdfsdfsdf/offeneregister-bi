#!/usr/bin/env python3
"""Decompress or straight-copy a .bz2 archive.

Usage:
    python src/bz2_copy.py input.jsonl.bz2 output.jsonl
"""
import bz2, sys
from pathlib import Path


def main(src: str, dst: str):
    src_p, dst_p = Path(src), Path(dst)
    with bz2.open(src_p, "rb") as fin, dst_p.open("wb") as fout:
        fout.write(fin.read())
    print(f"Decompressed {src_p.name} → {dst_p.name}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: python src/bz2_copy.py <in.bz2> <out.jsonl>")
    main(sys.argv[1], sys.argv[2])

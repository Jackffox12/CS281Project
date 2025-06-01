"""
Offline SOC coder using O*NET titles + RapidFuzz.

Example:
  python -m src.tools.attach_soc_local \
         --infile  data/processed/jobs_15-0000.csv \
         --title-col job_title \
         --outfile  data/processed/jobs_15-0000_soc.csv
"""

import argparse, pathlib, sys
import pandas as pd
from rapidfuzz import process, fuzz

ROOT = pathlib.Path(__file__).resolve().parents[2]  # project root
ONET_DIR = ROOT / "external" / "onet"


def build_title_index():
    occ = pd.read_csv(
        ONET_DIR / "occupation_data.txt", sep="\t", usecols=["O*NET-SOC Code", "Title"]
    )
    alt = pd.read_csv(
        ONET_DIR / "alternate_titles.txt",
        sep="\t",
        usecols=["O*NET-SOC Code", "Alternate Title"],
    )
    alt.columns = ["O*NET-SOC Code", "Title"]
    full = pd.concat([occ, alt]).drop_duplicates().reset_index(drop=True)
    full["title_lc"] = full["Title"].str.lower()
    return full[["title_lc", "O*NET-SOC Code"]]


TITLE_IDX = build_title_index()


def code_one(title: str, *, scorer=fuzz.WRatio) -> str | None:
    """Return best-match 6-digit SOC or None if below 80%."""
    if not title or not isinstance(title, str):
        return None
    query = title.lower()[:60]
    best = process.extractOne(
        query, TITLE_IDX["title_lc"], scorer=scorer, score_cutoff=80
    )
    if not best:
        return None
    idx = TITLE_IDX.index[TITLE_IDX["title_lc"] == best[0]][0]
    return TITLE_IDX.at[idx, "O*NET-SOC Code"]


def attach_soc(df: pd.DataFrame, title_col: str) -> pd.DataFrame:
    df = df.copy()
    df["soc6"] = df[title_col].apply(code_one)
    df.dropna(subset=["soc6"], inplace=True)
    df["soc_broad"] = df["soc6"].str.slice(0, 4)
    df["soc_major"] = df["soc6"].str.slice(0, 2)
    return df


def main(args):
    df = pd.read_csv(args.infile)
    if args.title_from_text:
        df["pseudo_title"] = (
            df[args.title_col]  # e.g. full résumé text
            .astype(str)
            .str.split()
            .str[:6]  # take first 6 tokens (adjust as you like)
            .str.join(" ")
        )
        title_col = "pseudo_title"
    else:
        title_col = args.title_col

    coded = attach_soc(df, title_col)
    coded.to_csv(args.outfile, index=False)
    kept = len(coded)
    print(f"{kept}/{len(df)} rows coded → {args.outfile}")
    if kept < len(df):
        print(
            "⚠ Some rows dropped (no ≥80 % match);"
            " increase pool or lower threshold if needed."
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument(
        "--title-col", required=True, help="column containing a job title-like string"
    )
    ap.add_argument("--outfile", required=True)
    ap.add_argument(
        "--title-from-text",
        action="store_true",
        help="derive pseudo title from first 6 tokens of the text column",
    )
    args = ap.parse_args()
    main(args)

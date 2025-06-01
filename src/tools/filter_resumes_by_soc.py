"""
Select ≥220 resumes per SOC family (15-/27-/29-) using keyword heuristics.

Usage:
  python -m src.tools.filter_resumes_by_soc \
         --infile  data/raw/kaggle_resumes/cleaned_resume.csv \
         --outdir  data/processed \
         --pool    5000
"""

import argparse, pathlib, re, pandas as pd
from bs4 import BeautifulSoup
from src.config import NEW_SOC_FAMILIES

# reuse / expand the Step-2 keyword lists
KW = {
    "15-0000": r"\b(software|developer|engineer|programmer|data|machine learning|ai|ml|cloud|devops|cyber|security|it|network|database|qa)\b",
    "27-0000": r"\b(design|designer|creative|graphics?|ux|ui|video|content|media|marketing|brand|copywriter|animation|film|journalist|editor)\b",
    "29-0000": r"\b(nurse|rn|physician|doctor|md|clinic|hospital|healthcare?|patient|pharmacy|therapist|medical|lab|laboratory|icu|surgical|emt|paramedic)\b",
}

def clean_html(raw: str) -> str:
    return BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)

def main(infile: str, outdir: str, pool: int):
    df = pd.read_csv(infile, usecols=["ID", "Resume_str"])
    df["text"] = df["Resume_str"].astype(str).map(clean_html).str.lower()

    outdir = pathlib.Path(outdir); outdir.mkdir(parents=True, exist_ok=True)

    for soc in NEW_SOC_FAMILIES:
        mask = df["text"].str.contains(KW[soc], regex=True, na=False)
        sub = (df.loc[mask, ["ID", "text"]]
                 .drop_duplicates(subset="text")
                 .sample(frac=1, random_state=42)
                 .head(pool)                   # take a big pool first
                 .head(220)                    # then cap at 220
                 .assign(soc_family=soc))
        fout = outdir / f"resumes_{soc}.csv"
        sub.to_csv(fout, index=False)
        print(f"{soc}: wrote {len(sub)} → {fout}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--pool", type=int, default=5000)
    args = ap.parse_args()
    main(args.infile, args.outdir, args.pool)

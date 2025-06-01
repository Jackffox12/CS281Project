"""
Filter a big job-description CSV down to N postings per SOC family.
"""

import argparse, pathlib, re, pandas as pd
from src.config import NEW_SOC_FAMILIES

# 1-A. Broaden the keyword lists (esp. 29-0000)
SOC_KEYWORDS = {
    "15-0000": [
        "software","developer","engineer","programmer","data","ml","ai","cloud",
        "backend","frontend","full stack","devops","cyber","security","it",
        "network","database","qa","web","tech","technology"
    ],
    "27-0000": [
        "designer","graphic","creative","ux","ui","video","content","media",
        "journalist","copywriter","marketing","brand","advertising","film",
        "animation","music","audio","podcast","producer","editor", "design",
        r"\b(design|creative|marketing|media|content)\b" 
    ],
    "29-0000": [
        "nurse", "rn", "registered nurse", "nursing", "lpn", "cna",
        "physician", "doctor", "md", "np", "practitioner",
        "clinic", "clinical", "hospital", "care",
        "pharmacy", "pharmacist", "lab", "laboratory", "technician",
        "therapist", "physical therapist", "occupational therapist",
        "speech", "radiologic", "icu", "surgical",
        "paramedic", "emt",      
        r"\b(health|healthcare|patient|clinic|medical)\b",
    ],
}

def main(infile: str, outdir: str, max_n: int = 2000):
    df = pd.read_csv(infile)
    # Build ONE giant text column that concatenates every relevant field
    text_cols = [c for c in df.columns
                 if any(k in c.lower() for k in
                        ["title","description","role","skills","responsibilities"])]
    df["__text__"] = df[text_cols].fillna("").agg(" ".join, axis=1).str.lower()

    outdir = pathlib.Path(outdir); outdir.mkdir(parents=True, exist_ok=True)

    for soc in NEW_SOC_FAMILIES:
        regex = "|".join(re.escape(x) for x in SOC_KEYWORDS[soc])
        mask = df["__text__"].str.contains(regex, na=False)

        # random-shuffle, then keep the first 220
        sub = (df.loc[mask, ["Job Title","Job Description"]]
                 .sample(frac=1, random_state=42)
                 .head(220)
                 .assign(soc_family=soc)
                 .rename(columns={"Job Title":"job_title",
                                  "Job Description":"job_description"}))

        fout = outdir / f"jobs_{soc}.csv"
        sub.to_csv(fout, index=False)
        print(f"{soc}: kept {len(sub)} rows → {fout}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--max", type=int, default=2000)  # pool size
    args = ap.parse_args()
    main(args.infile, args.outdir, args.max)

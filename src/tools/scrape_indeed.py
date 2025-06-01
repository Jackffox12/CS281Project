"""
Scrape Indeed job postings for a given SOC major group.

Usage:
    python -m src.tools.scrape_indeed --soc 15-0000 --max 250 \
          --out data/raw/jobs_soc/15-0000.jsonl
"""
import argparse, json, time, re, pathlib, random
from typing import List
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent  # pip install fake_useragent

BASE = "https://www.indeed.com/jobs"

def build_query(soc_code: str) -> str:
    major_name = {
        "15-0000": "Computer+Mathematical",
        "27-0000": "Arts+Design+Media",
        "29-0000": "Healthcare+Practitioners",
    }[soc_code]
    return f"{BASE}?q={major_name}&limit=50"

def extract_cards(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = []
    for card in soup.select("a.tapItem"):
        cards.append({
            "title": card.select_one("h2 span").text.strip(),
            "company": (card.select_one("span.companyName") or "").text.strip(),
            "location": (card.select_one("div.companyLocation") or "").text.strip(),
            "description": (card.select_one("div.job-snippet") or "").text.strip(),
            "url": "https://www.indeed.com" + card["href"].split("?")[0],
        })
    return cards

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--soc", required=True)
    ap.add_argument("--max", type=int, default=250)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    ua = UserAgent()
    fetched, page = [], 0
    while len(fetched) < args.max:
        url = build_query(args.soc) + f"&start={page*50}"
        r = requests.get(url, headers={"User-Agent": ua.random}, timeout=15)
        r.raise_for_status()
        cards = extract_cards(r.text)
        if not cards:
            break
        fetched.extend(cards)
        page += 1
        time.sleep(random.uniform(1.5, 3.0))   # polite pause

    fetched = fetched[: args.max]
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        for row in fetched:
            row["soc_family"] = args.soc
            json.dump(row, f)
            f.write("\n")
    print(f"Saved {len(fetched)} postings → {args.out}")

if __name__ == "__main__":
    main()

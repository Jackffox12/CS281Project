"""
Run GPT-2 (distilgpt2) on each résumé and score the probability that
the *next* token is “ hired ” – a rough proxy for callback likelihood.
"""
import argparse, pathlib, json, torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from tqdm import tqdm
import pandas as pd

def score(text, tok, model, device):
    prompt = f"{text.strip()}\n\nThis applicant was"
    ids = tok.encode(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=1023,   # truncate long résumés safely
          ).to(device)
    with torch.no_grad():
        logits = model(ids).logits[0, -1]        # next-token dist
    hired_id = tok.encode(" hired")[0]
    return torch.softmax(logits, dim=-1)[hired_id].item()

def run(df, col, out_json, batch=32, device="cpu"):
    tok    = AutoTokenizer.from_pretrained("distilgpt2")
    model  = AutoModelForCausalLM.from_pretrained("distilgpt2").to(device)
    model.eval()

    scores = []
    for text in tqdm(df[col], desc="infer"):
        scores.append(score(text, tok, model, device))

    df["gpt2_score"] = scores
    df.to_csv(out_json.with_suffix(".csv"), index=False)
    df[[col, "gpt2_score"]].to_json(out_json, orient="records", lines=True)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--text-col", default="text")
    ap.add_argument("--outfile", required=True)
    ap.add_argument("--device", default="cpu")
    args = ap.parse_args()

    df = pd.read_csv(args.infile)
    out = pathlib.Path(args.outfile)
    run(df, args.text_col, out, device=args.device)

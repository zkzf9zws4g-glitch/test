"""
Take data/final_rows.jsonl (all candidates surviving filters 1-6),
apply the sampling rule from the task brief, and write edgar_sample.csv.

Rule: if >50 survive, random sample 50 with seed=42 (recorded here).
If <25 survive, do NOT sample -- report and stop (handled by caller).
"""
import csv
import json
import os
import random

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FINAL_PATH = os.path.join(REPO_ROOT, "data", "final_rows.jsonl")
OUT_CSV = os.path.join(REPO_ROOT, "edgar_sample.csv")
SEED = 42

COLUMNS = [
    "cik", "filer_name", "issuer_name", "ticker", "exchange", "filing_date",
    "transaction_date", "transaction_code", "shares", "price_per_share",
    "transaction_value", "filer_role", "issuer_market_cap_at_filing",
    "sic_code", "source_filing_url",
]


def load_rows():
    rows = []
    with open(FINAL_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def to_csv_row(c):
    return {
        "cik": c["owner_cik"],  # filer (reporting owner) CIK -- see filter_log.txt
        "filer_name": c["owner_name"],
        "issuer_name": c["issuer_name"],
        "ticker": c.get("ticker") or c.get("issuer_ticker"),
        "exchange": c["exchange"],
        "filing_date": c["filing_date"],
        "transaction_date": c["transaction_date"],
        "transaction_code": c["transaction_code"],
        "shares": c["shares"],
        "price_per_share": c["price_per_share"],
        "transaction_value": round(c["transaction_value"], 2),
        "filer_role": c["filer_role"],
        "issuer_market_cap_at_filing": round(c["issuer_market_cap_at_filing"], 2),
        "sic_code": c["sic_code"],
        "source_filing_url": c["source_filing_url"],
    }


def main():
    rows = load_rows()
    n_pre_sample = len(rows)
    print(f"Pre-sample survivor count: {n_pre_sample}")

    sampled_note = ""
    if n_pre_sample > 50:
        rng = random.Random(SEED)
        rows = rng.sample(rows, 50)
        sampled_note = f"Randomly sampled 50 of {n_pre_sample} using seed={SEED}"
        print(sampled_note)
    elif n_pre_sample < 25:
        print(f"WARNING: only {n_pre_sample} rows survive all filters (<25). "
              f"Per instructions, NOT loosening filters. Stopping for review.")
    else:
        sampled_note = f"{n_pre_sample} rows survived (between 25-50); no down-sampling needed."
        print(sampled_note)

    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for c in rows:
            w.writerow(to_csv_row(c))

    print(f"Wrote {len(rows)} rows to {OUT_CSV}")
    return n_pre_sample, len(rows), sampled_note


if __name__ == "__main__":
    main()

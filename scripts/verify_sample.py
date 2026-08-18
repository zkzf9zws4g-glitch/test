"""
Independent re-verification: pick 5 random rows from edgar_sample.csv,
re-fetch source_filing_url fresh (bypassing any local cache), re-parse,
and confirm transaction code/shares/price match what's in the CSV.
Writes verification_report.md. Reports pass/fail plainly -- no papering
over mismatches.
"""
import csv
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get
from parse_form4 import parse_form4_xml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(REPO_ROOT, "edgar_sample.csv")
OUT_PATH = os.path.join(REPO_ROOT, "verification_report.md")
SEED = 42  # separate draw from the sampling seed, same fixed value for reproducibility


def main():
    with open(CSV_PATH) as f:
        rows = list(csv.DictReader(f))

    rng = random.Random(SEED)
    n = min(5, len(rows))
    picked = rng.sample(rows, n)

    lines = [
        "# Verification Report",
        "",
        f"5 rows randomly selected (seed={SEED}) from `edgar_sample.csv` "
        f"({len(rows)} total rows), each re-fetched live from its "
        f"`source_filing_url` and independently re-parsed, to confirm the "
        f"CSV matches the live filing.",
        "",
    ]

    all_pass = True
    for i, row in enumerate(picked, 1):
        url = row["source_filing_url"]
        r = get(url)
        lines.append(f"## Row {i}: {row['filer_name']} / {row['issuer_name']} ({row['ticker']})")
        lines.append(f"- URL: {url}")
        lines.append(f"- HTTP status: {r.status_code}")
        if r.status_code != 200:
            lines.append(f"- **FAIL**: could not re-fetch (status {r.status_code})")
            all_pass = False
            lines.append("")
            continue

        parsed = parse_form4_xml(r.content)
        if "error" in parsed:
            lines.append(f"- **FAIL**: could not re-parse ({parsed['error']})")
            all_pass = False
            lines.append("")
            continue

        p_txns = [t for t in parsed["transactions"] if t["transaction_code"] == "P"]
        # match on transaction_date + shares to find the specific line this CSV row refers to
        match = None
        for t in p_txns:
            try:
                if (t["transaction_date"] == row["transaction_date"]
                        and abs(float(t["shares"]) - float(row["shares"])) < 0.001):
                    match = t
                    break
            except (TypeError, ValueError):
                continue
        if match is None and p_txns:
            match = p_txns[0]

        checks = []
        row_pass = True
        if match is None:
            checks.append(f"- transaction_code: CSV=P, live=NO P TRANSACTION FOUND -- **FAIL**")
            row_pass = False
        else:
            code_ok = match["transaction_code"] == row["transaction_code"]
            shares_ok = abs(float(match["shares"]) - float(row["shares"])) < 0.001
            price_ok = abs(float(match["price_per_share"]) - float(row["price_per_share"])) < 0.001
            checks.append(f"- transaction_code: CSV={row['transaction_code']}, live={match['transaction_code']} -- {'OK' if code_ok else 'FAIL'}")
            checks.append(f"- shares: CSV={row['shares']}, live={match['shares']} -- {'OK' if shares_ok else 'FAIL'}")
            checks.append(f"- price_per_share: CSV={row['price_per_share']}, live={match['price_per_share']} -- {'OK' if price_ok else 'FAIL'}")
            row_pass = code_ok and shares_ok and price_ok

        lines.extend(checks)
        lines.append(f"- **{'PASS' if row_pass else 'FAIL'}**")
        lines.append("")
        all_pass = all_pass and row_pass

    lines.insert(4, f"**Overall: {'ALL 5 ROWS PASSED' if all_pass else 'AT LEAST ONE ROW FAILED -- see below'}**\n")

    with open(OUT_PATH, "w") as f:
        f.write("\n".join(lines))

    print(f"Wrote {OUT_PATH}. all_pass={all_pass}")
    return all_pass


if __name__ == "__main__":
    main()

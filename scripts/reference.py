"""Reference data: CIK -> exchange/ticker map from SEC's own file."""
import json
import os

from common import get, REPO_ROOT

CACHE_PATH = os.path.join(REPO_ROOT, "data", "cache", "company_tickers_exchange.json")


def load_exchange_map():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH) as f:
            raw = json.load(f)
    else:
        r = get("https://www.sec.gov/files/company_tickers_exchange.json")
        raw = r.json()
        with open(CACHE_PATH, "w") as f:
            json.dump(raw, f)

    fields = raw["fields"]  # ["cik","name","ticker","exchange"]
    idx = {f: i for i, f in enumerate(fields)}
    by_cik = {}
    for row in raw["data"]:
        cik = str(row[idx["cik"]])
        by_cik.setdefault(cik, []).append({
            "name": row[idx["name"]],
            "ticker": row[idx["ticker"]],
            "exchange": row[idx["exchange"]],
        })
    return by_cik

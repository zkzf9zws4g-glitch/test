"""
Pull standard financial KPIs for every unique issuer in edgar_sample.csv,
from SEC's own XBRL company facts API -- most-recent-available-today basis
(latest annual/10-K figures as currently reported), not point-in-time.

KPIs: Revenue, Net Income, Total Assets, Total Liabilities, Stockholders'
Equity, EPS (diluted), Operating Cash Flow, Shares Outstanding.

Each concept has a documented list of XBRL tag fallbacks, since different
filers (REITs, LPs/MLPs, BDCs, foreign private issuers, biotech) use
different standard tags for economically-equivalent line items. Every
value pulled records exactly which tag, fiscal period, and filing it came
from. Anything genuinely unavailable is recorded as such -- never
estimated or filled in.
"""
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get, REPO_ROOT

DATA_DIR = os.path.join(REPO_ROOT, "data")
CIK_MAP_PATH = os.path.join(DATA_DIR, "final_rows.jsonl")
SAMPLE_CSV = os.path.join(REPO_ROOT, "edgar_sample.csv")
CACHE_DIR = os.path.join(DATA_DIR, "cache", "companyfacts_full")
OUT_JSON = os.path.join(DATA_DIR, "company_kpis.json")
os.makedirs(CACHE_DIR, exist_ok=True)

# concept_key -> (display_label, [xbrl tag fallbacks in priority order], kind)
# kind: "flow" (annual-duration items: revenue/income/cashflow) or
#       "instant" (point-in-time balance-sheet items)
CONCEPTS = {
    "revenue": ("Revenue", [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "InterestAndDividendIncomeOperating",
        "GrossInvestmentIncomeOperating",   # BDCs' revenue-equivalent (e.g. Ares Capital, WhiteHorse Finance)
        "InterestIncomeOperating",          # mortgage REITs' revenue-equivalent (e.g. NexPoint REF)
    ], "flow"),
    "net_income": ("Net Income", [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    ], "flow"),
    "total_assets": ("Total Assets", ["Assets"], "instant"),
    "total_liabilities": ("Total Liabilities", ["Liabilities"], "instant"),
    "stockholders_equity": ("Stockholders' Equity", [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "PartnersCapital",  # LPs
        "MembersEquity",    # LLCs
    ], "instant"),
    "eps_diluted": ("EPS (diluted)", [
        "EarningsPerShareDiluted",
        "EarningsPerShareBasicAndDiluted",
    ], "flow"),
    "operating_cash_flow": ("Operating Cash Flow", [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ], "flow"),
    "shares_outstanding": ("Shares Outstanding", [
        "dei:EntityCommonStockSharesOutstanding",
        "CommonStockSharesOutstanding",
    ], "instant"),
}


def load_issuers():
    with open(SAMPLE_CSV) as f:
        sample_issuers = {row["issuer_name"] for row in csv.DictReader(f)}
    issuers = {}
    with open(CIK_MAP_PATH) as f:
        for line in f:
            d = json.loads(line)
            if d["issuer_name"] in sample_issuers and d["issuer_name"] not in issuers:
                issuers[d["issuer_name"]] = {
                    "cik": d["issuer_cik"],
                    "cik_padded": d["issuer_cik_padded"],
                    "ticker": d.get("issuer_ticker") or d.get("ticker"),
                }
    return issuers


def fetch_companyfacts(cik_padded):
    cache_path = os.path.join(CACHE_DIR, f"{cik_padded}.json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    r = get(url)
    if r.status_code != 200:
        return None
    try:
        d = r.json()
    except ValueError:
        return None
    with open(cache_path, "w") as f:
        json.dump(d, f)
    return d


def best_value(facts, tags, kind):
    """Search every fallback tag (us-gaap, or dei for shares) and return
    the single most-recent-as-currently-reported value across ALL of
    them -- not just the first tag that has any data at all. Companies
    migrate between economically-equivalent tags over time (e.g. Cedar
    Fair reports equity under StockholdersEquityIncludingPortion... only
    through 2018, then switches to PartnersCapital from 2019 on); taking
    the first tag with *any* entry would silently pin the value to a
    six-year-stale figure. 'flow' concepts prefer full-year (fp=='FY',
    form 10-K) entries when available; 'instant' concepts just take the
    latest 'end' date. Full provenance (tag, period, form, accession) is
    kept with every value."""
    candidates = []
    for tag in tags:
        if tag.startswith("dei:"):
            ns, name = "dei", tag.split(":", 1)[1]
        else:
            ns, name = "us-gaap", tag
        node = facts.get("facts", {}).get(ns, {}).get(name)
        if not node:
            continue
        entries = []
        for unit, vals in node.get("units", {}).items():
            for v in vals:
                entries.append({**v, "unit": unit, "_tag": f"{ns}:{name}"})
        if not entries:
            continue

        if kind == "flow":
            annual = [
                e for e in entries
                if e.get("form", "").startswith("10-K")
                and e.get("fp") == "FY"
                and "start" in e and "end" in e
            ]
            pool = annual if annual else entries
        else:
            pool = entries

        pool = sorted(pool, key=lambda e: e.get("end", ""))
        if pool:
            candidates.append(pool[-1])

    if not candidates:
        return None
    chosen = max(candidates, key=lambda e: e.get("end", ""))
    return {
        "tag": chosen["_tag"],
        "value": chosen["val"],
        "unit": chosen.get("unit"),
        "period_end": chosen.get("end"),
        "period_start": chosen.get("start"),
        "form": chosen.get("form"),
        "fy": chosen.get("fy"),
        "fp": chosen.get("fp"),
        "filed": chosen.get("filed"),
        "accn": chosen.get("accn"),
    }


def main():
    issuers = load_issuers()
    print(f"{len(issuers)} unique issuers to pull")

    results = {}
    for name, info in sorted(issuers.items()):
        cik_padded = info["cik_padded"]
        facts = fetch_companyfacts(cik_padded)
        row = {"issuer_name": name, "ticker": info["ticker"], "cik": info["cik"]}
        if facts is None:
            row["_fetch_error"] = "companyfacts fetch failed (no XBRL data at this CIK -- possibly a 20-F/foreign private issuer or non-accelerated filer with no XBRL submissions)"
            for key in CONCEPTS:
                row[key] = None
            results[name] = row
            print(f"  {name}: FETCH FAILED")
            continue

        entity_name = facts.get("entityName")
        row["_entity_name_per_sec"] = entity_name
        n_found = 0
        for key, (label, tags, kind) in CONCEPTS.items():
            val = best_value(facts, tags, kind)
            row[key] = val
            if val:
                n_found += 1

        # Fallback: some filers don't separately tag total Liabilities
        # (it's arithmetically implied by Assets - Equity on the balance
        # sheet). Derive it only when both real tags are present, and
        # label it clearly as derived, not a direct XBRL tag.
        if row["total_liabilities"] is None and row["total_assets"] and row["stockholders_equity"]:
            ta, se = row["total_assets"], row["stockholders_equity"]
            if ta["period_end"] == se["period_end"] and ta["unit"] == se["unit"]:
                row["total_liabilities"] = {
                    "tag": "derived: Assets - StockholdersEquity",
                    "value": ta["value"] - se["value"],
                    "unit": ta["unit"],
                    "period_end": ta["period_end"],
                    "period_start": None,
                    "form": ta["form"],
                    "fy": ta["fy"],
                    "fp": ta["fp"],
                    "filed": ta["filed"],
                    "accn": ta["accn"],
                }
                n_found += 1

        results[name] = row
        print(f"  {name}: {n_found}/{len(CONCEPTS)} KPIs found")

    with open(OUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {OUT_JSON}")


if __name__ == "__main__":
    main()

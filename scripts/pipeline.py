"""
Main orchestrator for the Form 4 insider-purchase sample pull.

Sampling design (see filter_log.txt for the full writeup):
  The full population of Form 4 filings in the 2014-01-01..2024-06-30
  window is far larger (SEC's full-text search caps any single query at
  10,000 hits, and a single business day already averages ~500-700
  Form 4 filings, implying >1,000,000 filings across the window) than
  can be exhaustively fetched and XML-parsed under SEC's <10 req/s fair
  access limit inside one working session. So this is a two-stage
  cluster random sample, seeded (42), fully reproducible:

    Stage A: business days in the window are the sampling clusters.
             random.Random(42).shuffle() on the full list of weekdays
             determines the order clusters are drawn in.
    Stage B: within each sampled day, EVERY Form 4 filing that day is
             examined (a full census within the cluster) -- so there is
             no additional within-day subsampling/bias.

  Days are drawn and fully processed, one at a time, until either the
  pool of filings surviving all 6 filters reaches a comfortable buffer
  above the 40-50 target, or a hard day budget is exhausted (in which
  case the run stops and reports the funnel honestly, per the task's
  "don't loosen filters yourself" instruction).
"""
import datetime
import json
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import get, REPO_ROOT
from parse_form4 import parse_form4_xml
from reference import load_exchange_map

SEED = 42
WINDOW_START = datetime.date(2014, 1, 1)
WINDOW_END = datetime.date(2024, 6, 30)

DATA_DIR = os.path.join(REPO_ROOT, "data")
STATE_DIR = os.path.join(DATA_DIR, "state")
LOG_DIR = os.path.join(DATA_DIR, "logs")
OWNER_CACHE_DIR = os.path.join(DATA_DIR, "cache", "owner_history")
ISSUER_CACHE_DIR = os.path.join(DATA_DIR, "cache", "issuer_facts")
for d in (STATE_DIR, LOG_DIR, OWNER_CACHE_DIR, ISSUER_CACHE_DIR):
    os.makedirs(d, exist_ok=True)

STATE_PATH = os.path.join(STATE_DIR, "pipeline_state.json")
PROCESSED_FILING_IDS_PATH = os.path.join(STATE_DIR, "processed_filing_ids.txt")
STAGE12_PATH = os.path.join(DATA_DIR, "candidates_stage1_2.jsonl")   # code P, not 10b5-1, >=$250k
STAGE3_PATH = os.path.join(DATA_DIR, "candidates_stage3.jsonl")      # + opportunistic
STAGE4_PATH = os.path.join(DATA_DIR, "candidates_stage4.jsonl")      # + director/officer
FINAL_PATH = os.path.join(DATA_DIR, "final_rows.jsonl")              # + market cap + exchange
ERROR_LOG_PATH = os.path.join(LOG_DIR, "processing_errors.jsonl")
DAY_LOG_PATH = os.path.join(LOG_DIR, "day_progress.jsonl")

# Stop pulling more days once we have this many full survivors (buffer
# above the 40-50 target so the final random-sample-of-50 step, if
# triggered, has real headroom).
TARGET_FINAL_BUFFER = 52
MAX_DAYS_BUDGET = 160
MAX_PRIOR_FILINGS_PER_OWNER = 20  # see filter_log.txt "opportunistic screen" note

FTS_BASE = "https://efts.sec.gov/LATEST/search-index"
PRICE_MISSING_SENTINEL = object()


_error_log_lock = threading.Lock()


def log_error(kind, detail):
    with _error_log_lock:
        with open(ERROR_LOG_PATH, "a") as f:
            f.write(json.dumps({"ts": time.time(), "kind": kind, "detail": detail}) + "\n")


def business_days(start, end):
    d = start
    out = []
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri; SEC holidays simply return 0 hits, handled gracefully
            out.append(d)
        d += datetime.timedelta(days=1)
    return out


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {
        "processed_days": [],
        "funnel": {
            "total_filings_examined": 0,
            "filter1_code_p_not_10b5_1": 0,
            "filter2_value_250k": 0,
            "filter3_opportunistic": 0,
            "filter4_director_officer": 0,
            "filter5_market_cap": 0,
            "filter6_exchange": 0,
        },
        "days_attempted": 0,
    }


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_PATH)


def append_jsonl(path, obj):
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")


def count_lines(path):
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for _ in f)


def fetch_day_filings(day):
    """Full census of Form 4 filings for one calendar day via EDGAR
    full-text search, paginated 100/page. Returns list of hit dicts."""
    ds = day.isoformat()
    hits = []
    frm = 0
    page_size = 100
    while True:
        params = {
            "q": '"Form 4"',
            "forms": "4",
            "dateRange": "custom",
            "startdt": ds,
            "enddt": ds,
            "from": frm,
        }
        r = get(FTS_BASE, params=params)
        if r.status_code != 200:
            log_error("fts_day_fetch_failed", {"day": ds, "status": r.status_code, "from": frm})
            break
        d = r.json()
        total = d["hits"]["total"]["value"]
        page_hits = d["hits"]["hits"]
        for h in page_hits:
            src = h["_source"]
            if src.get("form") != "4":  # exclude 4/A amendments etc.
                continue
            if src.get("file_type") != "4":  # exclude exhibits (POA, etc.) indexed under the same accession
                continue
            filename = h["_id"].split(":", 1)[1]
            if not filename.lower().endswith(".xml"):  # primary Form 4 doc is always XML
                continue
            file_nums = src.get("file_num") or []
            hits.append({
                "id": h["_id"],
                "adsh": src["adsh"],
                "ciks": src.get("ciks", []),
                "file_date": src.get("file_date"),
                "display_names": src.get("display_names", []),
                "sics": src.get("sics", []),
                "file_num": file_nums[0] if file_nums else None,
            })
        frm += page_size
        if frm >= total or frm >= 9900:  # stay under the 10k window cap
            break
        if not page_hits:
            break

    # Elasticsearch's default sort has ties on relevance score for a query
    # this broad (q='"Form 4"' matches nearly every hit near-identically),
    # so consecutive "from"-paginated pages can occasionally overlap and
    # return the same hit twice. Deduplicate by id defensively -- caught
    # in the wild as one duplicated filing producing two identical rows
    # in an early run (see filter_log.txt "DATA QUALITY" notes).
    seen_ids = set()
    deduped = []
    for h in hits:
        if h["id"] in seen_ids:
            continue
        seen_ids.add(h["id"])
        deduped.append(h)
    return deduped


def build_xml_url(hit, cik_for_path):
    adsh_nodash = hit["adsh"].replace("-", "")
    filename = hit["id"].split(":", 1)[1]
    return f"https://www.sec.gov/Archives/edgar/data/{cik_for_path}/{adsh_nodash}/{filename}"


def get_issuer_cik_for_path(hit, parsed_issuer_cik):
    # issuer CIK is reliably one of hit['ciks']; fall back to parsed value
    if parsed_issuer_cik:
        return parsed_issuer_cik.lstrip("0") or "0"
    for c in hit["ciks"]:
        return c.lstrip("0")
    return None


def fetch_and_parse_filing(hit):
    cik_guess = hit["ciks"][0].lstrip("0") if hit["ciks"] else None
    if cik_guess is None:
        return None
    url = build_xml_url(hit, cik_guess)
    r = get(url)
    if r.status_code != 200:
        log_error("xml_fetch_failed", {"url": url, "status": r.status_code})
        return None
    parsed = parse_form4_xml(r.content)
    if "error" in parsed:
        log_error("xml_parse_failed", {"url": url, "error": parsed["error"]})
        return None
    parsed["_source_url"] = url
    parsed["_file_date"] = hit["file_date"]
    parsed["_sics"] = hit.get("sics", [])
    parsed["_file_num"] = hit.get("file_num")
    return parsed


def is_10b5_1(parsed):
    if parsed.get("doc_level_10b5_1"):
        return True
    blob = (parsed.get("footnote_blob") or "").lower()
    return "10b5-1" in blob or "10b5 1" in blob


def owner_history_cache_path(owner_cik):
    return os.path.join(OWNER_CACHE_DIR, f"{owner_cik}.json")


def get_owner_prior_purchases(owner_cik_padded, before_date_str):
    """Returns list of (date_str, value) for this owner's own prior code-P
    non-derivative purchases, drawn from up to MAX_PRIOR_FILINGS_PER_OWNER
    of their most recent Form 4 filings strictly before before_date_str.
    Cached per owner so repeat sightings of the same insider are free."""
    cache_path = owner_history_cache_path(owner_cik_padded)
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            cached = json.load(f)
    else:
        url = f"https://data.sec.gov/submissions/CIK{owner_cik_padded}.json"
        r = get(url)
        if r.status_code != 200:
            log_error("owner_submissions_failed", {"owner_cik": owner_cik_padded, "status": r.status_code})
            cached = {"purchases": [], "note": "submissions_fetch_failed"}
            with open(cache_path, "w") as f:
                json.dump(cached, f)
            return []
        d = r.json()
        recent = d.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        entries = []
        for i, form in enumerate(forms):
            if form != "4":
                continue
            entries.append({
                "filingDate": recent["filingDate"][i],
                "accessionNumber": recent["accessionNumber"][i],
                "primaryDocument": recent["primaryDocument"][i],
            })
        # most recent first, cap the depth we actually fetch XML for
        entries.sort(key=lambda e: e["filingDate"], reverse=True)
        entries = entries[:MAX_PRIOR_FILINGS_PER_OWNER]

        purchases = []
        for e in entries:
            adsh_nodash = e["accessionNumber"].replace("-", "")
            url2 = f"https://www.sec.gov/Archives/edgar/data/{owner_cik_padded.lstrip('0')}/{adsh_nodash}/{e['primaryDocument']}"
            r2 = get(url2)
            if r2.status_code != 200:
                log_error("owner_prior_filing_fetch_failed", {"url": url2, "status": r2.status_code})
                continue
            p = parse_form4_xml(r2.content)
            if "error" in p:
                continue
            for t in p.get("transactions", []):
                if t["transaction_code"] != "P":
                    continue
                try:
                    val = float(t["shares"]) * float(t["price_per_share"])
                except (TypeError, ValueError):
                    continue
                purchases.append({"date": t["transaction_date"] or e["filingDate"], "value": val})
        cached = {
            "purchases": purchases,
            "note": f"scanned up to {MAX_PRIOR_FILINGS_PER_OWNER} most-recent-at-cache-time prior Form 4s",
            "n_filings_scanned": len(entries),
        }
        with open(cache_path, "w") as f:
            json.dump(cached, f)

    return [p for p in cached["purchases"] if p["date"] and p["date"] < before_date_str]


def issuer_shares_outstanding(issuer_cik_padded, as_of_date_str):
    cache_path = os.path.join(ISSUER_CACHE_DIR, f"{issuer_cik_padded}.json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            series = json.load(f)
    else:
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{issuer_cik_padded}.json"
        r = get(url)
        series = []
        if r.status_code == 200:
            try:
                d = r.json()
            except ValueError:
                d = {}
            facts = d.get("facts", {})
            candidates = [
                ("dei", "EntityCommonStockSharesOutstanding"),
                ("us-gaap", "CommonStockSharesOutstanding"),
            ]
            for ns, tag in candidates:
                node = facts.get(ns, {}).get(tag)
                if not node:
                    continue
                for unit_vals in node.get("units", {}).values():
                    for v in unit_vals:
                        if "end" in v and "val" in v:
                            series.append({"end": v["end"], "val": v["val"]})
                if series:
                    break
        else:
            log_error("companyfacts_fetch_failed", {"issuer_cik": issuer_cik_padded, "status": r.status_code})
        with open(cache_path, "w") as f:
            json.dump(series, f)

    if not series:
        return None
    prior = [s for s in series if s["end"] <= as_of_date_str]
    pool = prior if prior else series
    pool = sorted(pool, key=lambda s: s["end"])
    return pool[-1]["val"] if pool else None


def load_processed_filing_ids():
    if not os.path.exists(PROCESSED_FILING_IDS_PATH):
        return set()
    with open(PROCESSED_FILING_IDS_PATH) as f:
        return set(line.strip() for line in f if line.strip())


def mark_filing_processed(filing_id):
    with open(PROCESSED_FILING_IDS_PATH, "a") as f:
        f.write(filing_id + "\n")


_PROCESSED_FILING_IDS = None


FETCH_WORKERS = 8  # concurrent XML fetches per day; the real bottleneck observed
                    # in this environment is per-request network/proxy latency
                    # (~0.2-1s), not SEC's rate cap, so parallel in-flight
                    # requests (still globally paced by common.get()'s shared
                    # rate limiter -> real aggregate stays under 10 req/s)
                    # give a large wall-clock speedup with no extra risk.


def _fetch_and_prefilter(hit):
    """Runs in a worker thread: fetch + parse + the two cheap, purely-local
    checks (transaction code, 10b5-1). Returns (hit, parsed_or_None,
    p_txns_or_None) -- no shared-state mutation happens in here."""
    parsed = fetch_and_parse_filing(hit)
    if parsed is None:
        return hit, None, None
    if is_10b5_1(parsed):
        return hit, parsed, []
    p_txns = [t for t in parsed["transactions"] if t["transaction_code"] == "P"]
    return hit, parsed, p_txns


def process_day(day, state, exchange_map):
    global _PROCESSED_FILING_IDS
    if _PROCESSED_FILING_IDS is None:
        _PROCESSED_FILING_IDS = load_processed_filing_ids()

    ds = day.isoformat()
    hits = fetch_day_filings(day)
    day_summary = {"day": ds, "n_filings": len(hits)}

    # filing-level idempotency: if a crash interrupted a previous run
    # mid-day, this day gets fully re-walked on resume (it's not yet in
    # processed_days), but any filing already recorded is skipped so
    # funnel counts / output rows never get double-counted.
    todo = [h for h in hits if h["id"] not in _PROCESSED_FILING_IDS]
    for h in todo:
        mark_filing_processed(h["id"])
        _PROCESSED_FILING_IDS.add(h["id"])

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        futures = [pool.submit(_fetch_and_prefilter, h) for h in todo]
        for fut in as_completed(futures):
            hit, parsed, p_txns = fut.result()
            state["funnel"]["total_filings_examined"] += 1
            if parsed is None or not p_txns:
                continue
            state["funnel"]["filter1_code_p_not_10b5_1"] += 1

            # A small number of real Form 4 filings list the exact same
            # transaction line twice within nonDerivativeTable (a filer/
            # filing-agent submission artifact, not something SEC
            # validates away -- confirmed by hand on a live filing; see
            # filter_log.txt). Dedupe identical (date, shares, price)
            # lines within one filing so the same real trade isn't
            # counted, and doesn't inflate one insider's presence in the
            # sample, twice.
            seen_txn_keys = set()
            deduped_txns = []
            for t in p_txns:
                k = (t["transaction_date"], t["shares"], t["price_per_share"])
                if k in seen_txn_keys:
                    continue
                seen_txn_keys.add(k)
                deduped_txns.append(t)

            for t in deduped_txns:
                try:
                    shares = float(t["shares"])
                    price = float(t["price_per_share"])
                except (TypeError, ValueError):
                    continue
                value = shares * price
                if value < 250000:
                    continue
                state["funnel"]["filter2_value_250k"] += 1

                candidate = {
                    "owner_cik_padded": parsed["owner_cik_padded"],
                    "owner_cik": parsed["owner_cik"],
                    "owner_name": parsed["owner_name"],
                    "issuer_cik_padded": parsed["issuer_cik_padded"],
                    "issuer_cik": parsed["issuer_cik"],
                    "issuer_name": parsed["issuer_name"],
                    "issuer_ticker": parsed["issuer_ticker"],
                    "filing_date": parsed["_file_date"],
                    "transaction_date": t["transaction_date"],
                    "transaction_code": t["transaction_code"],
                    "shares": shares,
                    "price_per_share": price,
                    "transaction_value": value,
                    "is_director": parsed["is_director"],
                    "is_officer": parsed["is_officer"],
                    "is_ten_pct_owner": parsed["is_ten_pct_owner"],
                    "officer_title": parsed["officer_title"],
                    "sic_code": (parsed["_sics"][0] if parsed.get("_sics") else None),
                    "source_filing_url": parsed["_source_url"],
                    "file_num": parsed.get("_file_num"),
                }
                append_jsonl(STAGE12_PATH, candidate)

                # --- filter 3: opportunistic screen ---
                if not candidate["owner_cik_padded"] or not candidate["filing_date"]:
                    continue
                prior = get_owner_prior_purchases(candidate["owner_cik_padded"], candidate["filing_date"])
                if prior:
                    med = statistics.median(p["value"] for p in prior)
                    opp_reason = f"prior_median={med:.2f}, n_prior={len(prior)}"
                    passed3 = value >= 2 * med
                else:
                    opp_reason = "first_recorded_purchase"
                    passed3 = True
                candidate["opportunistic_reason"] = opp_reason
                if not passed3:
                    continue
                state["funnel"]["filter3_opportunistic"] += 1
                append_jsonl(STAGE3_PATH, candidate)

                # --- filter 4: director/officer (not just 10% owner) ---
                if not (candidate["is_director"] or candidate["is_officer"]):
                    continue
                state["funnel"]["filter4_director_officer"] += 1
                candidate["filer_role"] = "; ".join(
                    r for r, ok in [("Director", candidate["is_director"]),
                                     ("Officer" + (f" ({candidate['officer_title']})" if candidate["officer_title"] else ""), candidate["is_officer"])]
                    if ok
                )
                append_jsonl(STAGE4_PATH, candidate)

                # --- filter 5: market cap $200M-$20B at filing date ---
                if not candidate["issuer_cik_padded"]:
                    continue
                shares_out = issuer_shares_outstanding(candidate["issuer_cik_padded"], candidate["filing_date"])
                if shares_out is None:
                    continue
                mkt_cap = shares_out * price  # price-per-share from this open-market purchase used as the
                                               # filing-date price proxy -- see filter_log.txt "market cap" note
                if not (200_000_000 <= mkt_cap <= 20_000_000_000):
                    continue
                state["funnel"]["filter5_market_cap"] += 1
                candidate["issuer_market_cap_at_filing"] = mkt_cap
                candidate["shares_outstanding_used"] = shares_out

                # --- filter 6: exchange NYSE or Nasdaq ---
                # Primary source: SEC's own company_tickers_exchange.json.
                # This is a CURRENT-DAY snapshot, so an issuer that has
                # since been delisted/acquired/gone bankrupt (Revlon, Ion
                # Geophysical, etc. -- both legitimately NYSE-listed at
                # their filing date) simply won't appear in it, which
                # would silently survivorship-bias the sample toward
                # companies still trading today. Fallback: the filing's
                # own Exchange Act file number (captured at filing time,
                # from EDGAR full-text search metadata, no extra request)
                # -- "001-" prefix = registered under Sec 12(b), i.e.
                # listed on a national securities exchange at filing date;
                # "000-"/absent = Sec 12(g)/OTC. This can't distinguish
                # NYSE vs Nasdaq vs a minor exchange (NYSE American, etc.)
                # for delisted issuers, so such rows are labelled
                # accordingly rather than a hard NYSE/Nasdaq claim -- see
                # filter_log.txt.
                ex_rows = exchange_map.get(candidate["issuer_cik"], [])
                match = None
                for row in ex_rows:
                    if row["ticker"] == candidate["issuer_ticker"]:
                        match = row
                        break
                if match is None and ex_rows:
                    match = ex_rows[0]

                if match is not None:
                    if match["exchange"] not in ("NYSE", "Nasdaq"):
                        continue
                    exchange_label = match["exchange"]
                    ticker_label = match["ticker"] or candidate["issuer_ticker"]
                    exchange_source = "current_sec_ticker_exchange_file"
                else:
                    fn = candidate.get("file_num") or ""
                    if not fn.startswith("001-"):
                        continue
                    exchange_label = "NYSE/Nasdaq (inferred, not in current listing file)"
                    ticker_label = candidate["issuer_ticker"]
                    exchange_source = "file_num_prefix_fallback"

                state["funnel"]["filter6_exchange"] += 1
                candidate["exchange"] = exchange_label
                candidate["ticker"] = ticker_label
                candidate["exchange_source"] = exchange_source

                append_jsonl(FINAL_PATH, candidate)

            save_state(state)  # checkpoint after every filing, not just every day

    day_summary["funnel_after_day"] = dict(state["funnel"])
    day_summary["final_rows_so_far"] = count_lines(FINAL_PATH)
    append_jsonl(DAY_LOG_PATH, day_summary)
    print(f"[{ds}] filings={len(hits)} funnel={state['funnel']} final_so_far={day_summary['final_rows_so_far']}", flush=True)


def main():
    state = load_state()
    exchange_map = load_exchange_map()

    days = business_days(WINDOW_START, WINDOW_END)
    rng = random.Random(SEED)
    rng.shuffle(days)

    processed = set(state["processed_days"])
    for day in days:
        ds = day.isoformat()
        if ds in processed:
            continue
        final_count = count_lines(FINAL_PATH)
        if final_count >= TARGET_FINAL_BUFFER:
            print(f"Reached target buffer of {TARGET_FINAL_BUFFER} final survivors. Stopping.")
            break
        if state["days_attempted"] >= MAX_DAYS_BUDGET:
            print(f"Hit day budget of {MAX_DAYS_BUDGET}. Stopping.")
            break

        process_day(day, state, exchange_map)
        state["processed_days"].append(ds)
        state["days_attempted"] += 1
        save_state(state)

    print("=== FINAL FUNNEL ===")
    print(json.dumps(state["funnel"], indent=2))
    print("days_attempted:", state["days_attempted"])
    print("final_rows:", count_lines(FINAL_PATH))


if __name__ == "__main__":
    main()

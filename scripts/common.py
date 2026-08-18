"""
Shared HTTP client for the EDGAR pull pipeline.

- Enforces SEC's fair-access rules: real User-Agent, < 10 req/s.
- Logs every single request (URL + response fingerprint) to
  data/logs/request_log.jsonl so every call is traceable, per the
  task's anti-cached-response verification requirement.
"""
import hashlib
import json
import os
import time
import threading

import requests

USER_AGENT = "PeopleSignalPilot research donevtm@gmail.com"
HEADERS = {"User-Agent": USER_AGENT}

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(REPO_ROOT, "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
REQUEST_LOG_PATH = os.path.join(LOG_DIR, "request_log.jsonl")

_lock = threading.Lock()
_last_request_time = [0.0]
MIN_INTERVAL = 0.13  # ~7.5 req/s, safely under SEC's 10 req/s cap

_session = requests.Session()
_session.headers.update(HEADERS)

_log_fh = open(REQUEST_LOG_PATH, "a")


def _fingerprint(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()[:16]


def get(url, params=None, timeout=30, max_retries=5):
    """Rate-limited GET with full request/response logging and retry on
    429/5xx. Returns a requests.Response (raises on final failure)."""
    for attempt in range(max_retries):
        with _lock:
            now = time.time()
            wait = MIN_INTERVAL - (now - _last_request_time[0])
            if wait > 0:
                time.sleep(wait)
            _last_request_time[0] = time.time()
        t0 = time.time()
        try:
            resp = _session.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            entry = {
                "ts": time.time(),
                "url": resp.url if "resp" in dir() else url,
                "params": params,
                "error": str(e),
                "attempt": attempt,
            }
            _log_fh.write(json.dumps(entry) + "\n")
            _log_fh.flush()
            time.sleep(1.5 * (attempt + 1))
            continue

        elapsed = time.time() - t0
        entry = {
            "ts": time.time(),
            "url": resp.url,
            "status": resp.status_code,
            "elapsed_s": round(elapsed, 3),
            "fingerprint_sha256_16": _fingerprint(resp.content),
            "resp_len": len(resp.content),
            "resp_first200": resp.text[:200] if resp.text else "",
        }
        _log_fh.write(json.dumps(entry) + "\n")
        _log_fh.flush()

        if resp.status_code == 200:
            return resp
        if resp.status_code in (429, 403, 503):
            time.sleep(2.0 * (attempt + 1))
            continue
        return resp  # other status codes: let caller decide
    return resp

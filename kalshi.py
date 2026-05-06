"""
Kalshi API client — RSA-PSS auth, order placement, cancellation.
Copied from JumpBot with minimal modifications.
"""

import os
import time
import base64
import logging
import threading
from collections import deque
import requests as http_requests
from datetime import datetime
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding

log = logging.getLogger("homer")

PROD_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Module-level state
_creds = {}

# ---------------------------------------------------------------------------
# Client-side Kalshi REST rate tracking
# Records every outbound call so a monitor can aggregate live usage vs the
# tier cap (30 reads/sec, 30 writes/sec on Advanced).
# ---------------------------------------------------------------------------
_call_log = deque(maxlen=3600)  # plenty of headroom even at high QPS
_call_log_lock = threading.Lock()


def _record_call(method: str):
    with _call_log_lock:
        _call_log.append((time.time(), method))


def get_rate_stats():
    """Return rolling rate stats: per-second + 60s avg + 60s peak."""
    now = time.time()
    with _call_log_lock:
        recent = list(_call_log)

    last_1s = [(t, m) for t, m in recent if now - t < 1.0]
    last_60s = [(t, m) for t, m in recent if now - t < 60.0]

    reads_last_sec = sum(1 for _, m in last_1s if m == "GET")
    writes_last_sec = sum(1 for _, m in last_1s if m in ("POST", "DELETE"))

    reads_60s_total = sum(1 for _, m in last_60s if m == "GET")
    writes_60s_total = sum(1 for _, m in last_60s if m in ("POST", "DELETE"))

    # 60s peak = max calls in any single 1s bucket
    bucket_reads = {}
    bucket_writes = {}
    for t, m in last_60s:
        sec = int(t)
        if m == "GET":
            bucket_reads[sec] = bucket_reads.get(sec, 0) + 1
        else:
            bucket_writes[sec] = bucket_writes.get(sec, 0) + 1
    peak_reads = max(bucket_reads.values(), default=0)
    peak_writes = max(bucket_writes.values(), default=0)

    return {
        "reads_last_sec": reads_last_sec,
        "writes_last_sec": writes_last_sec,
        "reads_avg_60s": round(reads_60s_total / 60.0, 2),
        "writes_avg_60s": round(writes_60s_total / 60.0, 2),
        "reads_peak_60s": peak_reads,
        "writes_peak_60s": peak_writes,
        "total_tracked": len(recent),
    }


def init_creds():
    """Load credentials from environment variables."""
    global _creds
    api_key = os.environ.get("KALSHI_API_KEY", "").strip()
    pem_text = os.environ.get("KALSHI_PEM", "").strip()
    if not api_key or not pem_text:
        log.warning("KALSHI_API_KEY or KALSHI_PEM not set")
        return False
    # Railway may strip newlines from PEM
    pem_text = pem_text.replace("\\n", "\n")
    if "-----BEGIN" in pem_text and "\n" not in pem_text.split("-----")[2]:
        parts = pem_text.replace("-----BEGIN RSA PRIVATE KEY-----", "").replace("-----END RSA PRIVATE KEY-----", "").strip()
        pem_text = "-----BEGIN RSA PRIVATE KEY-----\n" + "\n".join([parts[i:i+64] for i in range(0, len(parts), 64)]) + "\n-----END RSA PRIVATE KEY-----\n"
    private_key = serialization.load_pem_private_key(
        pem_text.encode("utf-8"), password=None, backend=default_backend()
    )
    _creds = {"api_key_id": api_key, "key": private_key}
    log.info("Kalshi credentials loaded")
    return True


def is_authenticated():
    return bool(_creds)


def _sign_headers(method: str, path: str):
    ts = str(int(datetime.now().timestamp() * 1000))
    path_clean = path.split("?")[0]
    message = f"{ts}{method}{path_clean}".encode("utf-8")
    sig = _creds["key"].sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": _creds["api_key_id"],
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


def kalshi_get(path: str, params=None):
    _record_call("GET")
    url = PROD_BASE + path
    hdrs = _sign_headers("GET", "/trade-api/v2" + path)
    r = http_requests.get(url, headers=hdrs, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def kalshi_post(path: str, body: dict):
    _record_call("POST")
    url = PROD_BASE + path
    hdrs = _sign_headers("POST", "/trade-api/v2" + path)
    r = http_requests.post(url, headers=hdrs, json=body, timeout=10)
    r.raise_for_status()
    return r.json()


def kalshi_delete(path: str, body: dict = None):
    _record_call("DELETE")
    url = PROD_BASE + path
    hdrs = _sign_headers("DELETE", "/trade-api/v2" + path)
    if body:
        r = http_requests.delete(url, headers=hdrs, json=body, timeout=10)
    else:
        r = http_requests.delete(url, headers=hdrs, timeout=10)
    r.raise_for_status()


def place_order(ticker: str, side: str, price: int, size: int):
    """Place a limit order. Returns order dict or None."""
    body = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "type": "limit",
        "yes_price" if side == "yes" else "no_price": price,
        "count": size,
        "cancel_order_on_pause": True,
    }
    try:
        resp = kalshi_post("/portfolio/orders", body)
        order = resp.get("order", resp)
        order_id = order.get("order_id", "")
        log.info(f"PLACED {ticker} {side} @{price}c x{size} id={order_id}")
        return {
            "order_id": order_id,
            "price": price,
            "remaining": size,
            "filled_so_far": 0,
        }
    except Exception as e:
        log.error(f"PLACE_FAILED {ticker} {side} @{price}c x{size}: {e}")
        return None


def cancel_order(order_id: str):
    """Cancel an order. Returns True if successful."""
    try:
        kalshi_delete(f"/portfolio/orders/{order_id}")
        log.info(f"CANCELLED id={order_id}")
        return True
    except Exception as e:
        log.error(f"CANCEL_FAILED id={order_id}: {e}")
        return False


def amend_order(order_id: str, ticker: str, side: str, new_price: int, new_count: int = None):
    """Amend an existing order's price (and optionally count) in place.
    Returns new order dict or None on failure.
    Atomic — never off the book between cancel and place.
    """
    body = {
        "ticker": ticker,
        "side": side,
        "action": "buy",
        "no_price" if side == "no" else "yes_price": new_price,
    }
    if new_count is not None:
        body["count"] = new_count
    try:
        resp = kalshi_post(f"/portfolio/orders/{order_id}/amend", body)
        new_order = resp.get("order", resp)
        new_id = new_order.get("order_id", order_id)
        log.info(f"AMENDED {ticker} {side} @{new_price}c id={order_id}→{new_id}")
        return {
            "order_id": new_id,
            "price": new_price,
            "remaining": new_order.get("remaining_count", new_count or 0),
        }
    except Exception as e:
        log.error(f"AMEND_FAILED {ticker} {side} @{new_price}c id={order_id}: {e}")
        return None


def batch_cancel(order_ids: list):
    """Cancel multiple orders via batch endpoint. Falls back to sequential."""
    if not order_ids:
        return 0
    try:
        kalshi_delete("/portfolio/orders/batched", body={"order_ids": order_ids})
        log.info(f"BATCH_CANCELLED {len(order_ids)} orders")
        return len(order_ids)
    except Exception as e:
        log.warning(f"Batch cancel failed ({e}), falling back to sequential")
        cancelled = 0
        for oid in order_ids:
            if cancel_order(oid):
                cancelled += 1
        return cancelled


def get_resting_orders(ticker: str = None):
    """Get all resting orders, optionally filtered by ticker."""
    params = {"status": "resting", "limit": 200}
    if ticker:
        params["ticker"] = ticker
    try:
        resp = kalshi_get("/portfolio/orders", params=params)
        return resp.get("orders", [])
    except Exception:
        return []


def get_balance():
    """Get account balance in dollars."""
    try:
        resp = kalshi_get("/portfolio/balance")
        bal = resp.get("balance", resp)
        if isinstance(bal, dict):
            return float(bal.get("balance_cents", bal.get("balance", 0))) / 100
        return float(bal) / 100
    except Exception:
        return 0.0

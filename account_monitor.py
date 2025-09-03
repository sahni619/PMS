#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Account Balance Monitor with Cashflow-Adjusted Returns + TWR + Charts + Excel
-----------------------------------------------------------------------------------
- Multiple labeled accounts across Binance, Bybit, OKX via ccxt.
- 4pm America/New_York daily snapshot:
    * Per-account values in USDT + converted fiats (USD/EUR configurable)
    * Money-weighted daily return (flows-adjusted)
    * Time-Weighted Return (TWR) across subperiods segmented at flows
    * Portfolio totals
    * Optional charts (asset pie, venue bar) to Telegram
- Telegram notifications: startup, funding events (deposits/withdrawals), snapshot
- Robust funding polling using raw REST per venue when ccxt helpers are missing.
- Excel export (startup + 4pm) with Daily Return USD/%, Cumulative Return %, Rolling P&L USD.
- Flow tracer: per-account log of funding polling & events.

Env tips (keep your existing ones; these are optional):
  FLOW_LOOKBACK_DAYS=30
  FLOW_TRACE=1
  FLOW_TRUST_NONFINAL=0      # set 1 to accept all statuses (debug)
  BYBIT_ACCOUNT_TYPE=UNIFIED  # UNIFIED|CONTRACT|SPOT (raw v5)
  BYBIT_RAW_FALLBACK=v5,v3    # try order: v5 then legacy v3
  OKX_USE_STATE_FILTER=1      # request state=2 (success) on raw
  PRICING_EXCHANGE=binance
  QUOTE_CCY=USDT

Dependencies:
  pip install ccxt python-dotenv requests pytz matplotlib pandas openpyxl
"""

import os
import json
import time
import math
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import ccxt
from dotenv import load_dotenv
import requests
import pytz
from event_deduplicator import deduplicate_events

# plotting
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# data/export
import pandas as pd
from final_statuses import is_final as _status_is_final

NY_TZ = pytz.timezone("America/New_York")
STATE_PATH = os.environ.get("STATE_PATH", ".monitor_state.json")

# ====================== Telegram ============================================

def md_escape(s: str) -> str:
    if s is None:
        return ""
    for c in r"_*[]()~`>#+-=|{}.!":
        s = s.replace(c, f"\\{c}")
    return s

def tg_send(token: str, chat_id: str, text: str, disable_web_page_preview: bool = True) -> None:
    if not (token and chat_id and text):
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": disable_web_page_preview,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()
    except Exception:
        # fallback without Markdown (avoids 400 on bad escaping)
        try:
            r = requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=20)
            r.raise_for_status()
        except Exception as e2:
            print(f"[WARN] Telegram send failed: {e2}")

def tg_send_photo(token: str, chat_id: str, image_path: str, caption: Optional[str] = None) -> None:
    if not (token and chat_id and image_path and os.path.exists(image_path)):
        return
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with open(image_path, "rb") as f:
            files = {"photo": f}
            data = {"chat_id": chat_id, "parse_mode": "MarkdownV2"}
            if caption:
                data["caption"] = caption
            r = requests.post(url, files=files, data=data, timeout=30)
            r.raise_for_status()
    except Exception as e:
        print(f"[WARN] Telegram photo send failed ({image_path}): {e}")

def tg_send_document(token: str, chat_id: str, doc_path: str, caption: Optional[str] = None) -> None:
    if not (token and chat_id and doc_path and os.path.exists(doc_path)):
        return
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        with open(doc_path, "rb") as f:
            files = {"document": f}
            data = {"chat_id": chat_id, "parse_mode": "MarkdownV2"}
            if caption:
                data["caption"] = caption
            r = requests.post(url, files=files, data=data, timeout=30)
            r.raise_for_status()
    except Exception as e:
        print(f"[WARN] Telegram document send failed ({doc_path}): {e}")

# ====================== State ===============================================

def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, "".join([STATE_PATH, ""]))  # atomic replace

# ====================== Pricing =============================================

def make_pricing_exchange(name: str):
    return getattr(ccxt, name.lower())({"enableRateLimit": True})

def fetch_prices_usdt(pricing_ex, assets: List[str]) -> Dict[str, float]:
    stables = {"USDT", "USDC", "FDUSD", "TUSD", "USDD", "DAI"}
    prices: Dict[str, float] = {}
    need = set([a.upper() for a in assets if a.upper() not in stables])
    for s in stables:
        if s in [a.upper() for a in assets]:
            prices[s] = 1.0
    try:
        tickers = pricing_ex.fetch_tickers()
    except Exception:
        tickers = {}
    def last_price(sym: str) -> Optional[float]:
        t = tickers.get(sym)
        if t and t.get("last"):
            try:
                return float(t["last"])
            except Exception:
                pass
        try:
            t = pricing_ex.fetch_ticker(sym)
            return float(t.get("last") or t.get("close") or 0) or None
        except Exception:
            return None
    for asset in list(need):
        sym1 = f"{asset}/USDT"
        p = last_price(sym1)
        if p and p > 0:
            prices[asset] = p
            need.discard(asset); continue
        sym2 = f"USDT/{asset}"
        p = last_price(sym2)
        if p and p > 0:
            prices[asset] = 1.0 / p
            need.discard(asset); continue
    return prices

def get_fiat_rates_per_usdt(pricing_ex, fiats: List[str]) -> Dict[str, float]:
    out = {}
    try:
        tickers = pricing_ex.fetch_tickers()
    except Exception:
        tickers = {}
    def last_price(sym: str) -> Optional[float]:
        t = tickers.get(sym)
        if t and t.get("last"):
            try:
                return float(t["last"])
            except Exception:
                pass
        try:
            t = pricing_ex.fetch_ticker(sym)
            return float(t.get("last") or t.get("close") or 0) or None
        except Exception:
            return None
    for fiat in [f.upper() for f in fiats]:
        if fiat == "USD":
            out[fiat] = 1.0; continue
        p = last_price(f"USDT/{fiat}")
        if p and p > 0:
            out[fiat] = p; continue
        p = last_price(f"{fiat}/USDT")
        if p and p > 0:
            out[fiat] = 1.0 / p; continue
        out[fiat] = 1.0
    return out

def value_portfolio_usdt(bal_total: Dict[str, float], prices: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
    total = 0.0
    per_asset_val = {}
    for asset, amt in bal_total.items():
        a = asset.upper()
        if a in ("USDT", "USDC", "FDUSD", "TUSD", "USDD", "DAI"):
            v = float(amt)
        else:
            px = prices.get(a)
            if not px:
                continue
            v = float(amt) * float(px)
        per_asset_val[a] = per_asset_val.get(a, 0.0) + v
        total += v
    return total, per_asset_val

# ====================== CCXT helpers / wallets ===============================

def ensure_dir(p: str) -> str:
    Path(p).mkdir(parents=True, exist_ok=True)
    return p

def parse_account_label(full: str) -> Tuple[str, str]:
    parts = full.split("_", 1)
    base = parts[0].lower()
    label = full.upper()
    return base, label

def make_exchange(full_name: str, creds: Dict[str, str]):
    base, label = parse_account_label(full_name)
    if base not in ["binance", "bybit", "okx"]:
        raise ValueError(f"Unsupported exchange: {base}")
    kwargs = {"enableRateLimit": True, "timeout": 20000}
    key = creds.get(f"{label}_API_KEY", "")
    secret = creds.get(f"{label}_API_SECRET", "")
    password = creds.get(f"{label}_API_PASSPHRASE", "")  # OKX only
    if key and secret:
        kwargs["apiKey"] = key
        kwargs["secret"] = secret
    if base == "okx" and password:
        kwargs["password"] = password
    ex = getattr(ccxt, base)(kwargs)
    if base == "bybit":
        ex.options["defaultType"] = os.environ.get("BYBIT_DEFAULT_TYPE", "unified").lower()
    elif base == "binance":
        ex.options["defaultType"] = os.environ.get("BINANCE_DEFAULT_TYPE", "spot").lower()
    elif base == "okx":
        ex.options["defaultType"] = os.environ.get("OKX_DEFAULT_TYPE", "spot").lower()
    return ex, label

def _merge_totals(dst: Dict[str, float], src: Dict[str, float]) -> None:
    for a, v in (src or {}).items():
        if not v:
            continue
        dst[a] = float(dst.get(a, 0.0)) + float(v)

def _safe_fetch_balance(ex, params: dict) -> Dict[str, float]:
    try:
        bal = ex.fetch_balance(params or {})
        return bal.get("total", {}) or {}
    except Exception as e:
        print(f"[INFO] fetch_balance failed on {ex.id} with params={params}: {e}")
        return {}

def fetch_all_wallet_balances(ex) -> Dict[str, float]:
    base = ex.id.lower()
    merged: Dict[str, float] = {}
    if base == "binance":
        for p in [{"type": "spot"}, {"type": "future"}, {"type": "delivery"}, {"type": "margin"}]:
            _merge_totals(merged, _safe_fetch_balance(ex, p))
    elif base == "bybit":
        uni = _safe_fetch_balance(ex, {"type": "unified"})
        if any(uni.values()):
            _merge_totals(merged, uni)
        else:
            _merge_totals(merged, _safe_fetch_balance(ex, {"type": "contract"}))
            _merge_totals(merged, _safe_fetch_balance(ex, {"type": "spot"}))
    elif base == "okx":
        _merge_totals(merged, _safe_fetch_balance(ex, {"type": "trading"}))
        _merge_totals(merged, _safe_fetch_balance(ex, {"type": "funding"}))
    else:
        _merge_totals(merged, _safe_fetch_balance(ex, {}))
    return {k: float(v) for k, v in merged.items() if v and not math.isclose(float(v), 0.0, abs_tol=1e-12)}

# ====================== Funding (robust) =====================================

def _trace_flow(label: str, lines: List[str]) -> None:
    if os.environ.get("FLOW_TRACE", "1").lower() not in ("1","true","yes","on"):
        return
    ensure_dir(os.environ.get("FLOW_LOG_DIR", "./flow_logs"))
    path = os.path.join(os.environ.get("FLOW_LOG_DIR", "./flow_logs"), f"{label}.log")
    ts = datetime.now(NY_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    try:
        with open(path, "a") as f:
            for ln in lines:
                f.write(f"{ts} | {ln.rstrip()}\n")
    except Exception as e:
        print(f"[INFO] flow tracer write failed for {label}: {e}")

def _ms(ts) -> Optional[int]:
    if ts is None:
        return None
    try:
        t = float(ts)
    except Exception:
        return None
    if abs(t) < 10 ** 12:
        t *= 1000
    return int(t)

def _trust_nonfinal() -> bool:
    return os.environ.get("FLOW_TRUST_NONFINAL", "0").lower() in ("1","true","yes","on")


def _is_final(ex_id: str, kind: str, status) -> bool:
    if _trust_nonfinal():
        return True
    ex_id = (ex_id or "").lower()
    try:
        return _status_is_final(ex_id, kind, status)
    except ValueError:
        s = str(status).strip().lower()
        return s in {"ok", "completed", "complete", "success", "succeeded", "done"}

# ---- Raw per-venue (multi-endpoint) ----

def _binance_raw(ex, since_ms: int, until_ms: int) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    deposits, withdrawals = [], []
    chunk = 90 * 24 * 60 * 60 * 1000
    t0 = since_ms
    while t0 <= until_ms:
        t1 = min(t0 + chunk - 1, until_ms)
        try:
            dep = ex.sapiGetCapitalDepositHisrec({"startTime": t0, "endTime": t1}) or []
            for d in dep:
                ts = _ms(d.get("insertTime"))
                if ts is None or ts < since_ms or ts > until_ms:
                    continue
                deposits.append({
                    "id": str(d.get("txId") or f"binance_dep_{ts}_{d.get('coin')}"),
                    "type": "deposit",
                    "timestamp": ts,
                    "currency": str(d.get("coin") or "").upper(),
                    "amount": float(d.get("amount") or 0),
                    "status": d.get("status")
                })
        except Exception as e:
            _trace_flow(ex.id.upper(), [f"binance deposit raw failed: {repr(e)}"])
        try:
            wdr = ex.sapiGetCapitalWithdrawHistory({"startTime": t0, "endTime": t1}) or []
            for w in wdr:
                ts = _ms(w.get("applyTime")) or _ms(w.get("updateTime"))
                if ts is None and isinstance(w.get("applyTime"), str):
                    try:
                        ts = int(datetime.strptime(w.get("applyTime"), "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
                    except Exception:
                        ts = None
                if ts is None or ts < since_ms or ts > until_ms:
                    continue
                withdrawals.append({
                    "id": str(w.get("id") or w.get("txId") or f"binance_wdr_{ts}_{w.get('coin')}"),
                    "type": "withdrawal",
                    "timestamp": ts,
                    "currency": str(w.get("coin") or "").upper(),
                    "amount": float(w.get("amount") or 0),
                    "status": w.get("status")
                })
        except Exception as e:
            _trace_flow(ex.id.upper(), [f"binance withdraw raw failed: {repr(e)}"])
        t0 = t1 + 1
    return deposits, withdrawals

def _bybit_raw(ex, since_ms: int, until_ms: int) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    deps, wdrs = [], []
    acct_type = os.environ.get("BYBIT_ACCOUNT_TYPE", "UNIFIED").upper()  # UNIFIED|CONTRACT|SPOT
    order = [s.strip().lower() for s in os.environ.get("BYBIT_RAW_FALLBACK", "v5,v3").split(",") if s.strip()]
    # Helper to read different shapes
    def _rows(obj):
        if not obj:
            return []
        # common shapes across Bybit versions
        for k in ("rows","list","data","result"):
            v = obj.get(k)
            if isinstance(v, dict):
                # nested again
                for kk in ("rows","list","data"):
                    vv = v.get(kk)
                    if isinstance(vv, list):
                        return vv
            if isinstance(v, list):
                return v
        return []
    # pull recent, filter by time
    for ver in order:
        # Deposits
        try:
            if ver == "v5" and hasattr(ex, "privateGetV5AssetDepositQuery"):
                d = ex.privateGetV5AssetDepositQuery({"accountType": acct_type, "limit": 200}) or {}
            elif ver == "v3" and hasattr(ex, "privateGetAssetDepositQuery"):
                d = ex.privateGetAssetDepositQuery({}) or {}
            else:
                d = {}
            rows = _rows(d)
            for r in rows:
                ts = _ms(r.get("successAt")) or _ms(r.get("blockTime")) or _ms(r.get("updatedTime") or r.get("createdTime") or r.get("createTime"))
                if ts is None or ts < since_ms or ts > until_ms:
                    continue
                coin = str(r.get("coin") or r.get("currency") or "").upper()
                qty  = r.get("qty") or r.get("amount") or r.get("size") or 0
                deps.append({
                    "id": str(r.get("txID") or r.get("id") or f"bybit_dep_{ts}_{coin}"),
                    "type": "deposit",
                    "timestamp": ts,
                    "currency": coin,
                    "amount": float(qty or 0),
                    "status": r.get("status")
                })
        except Exception as e:
            _trace_flow(ex.id.upper(), [f"bybit {ver} deposit raw failed: {repr(e)}"])
        # Withdrawals
        try:
            if ver == "v5" and hasattr(ex, "privateGetV5AssetWithdrawQuery"):
                w = ex.privateGetV5AssetWithdrawQuery({"accountType": acct_type, "limit": 200}) or {}
            elif ver == "v3" and hasattr(ex, "privateGetAssetWithdrawQuery"):
                w = ex.privateGetAssetWithdrawQuery({}) or {}
            else:
                w = {}
            rows = _rows(w)
            for r in rows:
                ts = _ms(r.get("successAt")) or _ms(r.get("updatedTime")) or _ms(r.get("createdTime") or r.get("createTime"))
                if ts is None or ts < since_ms or ts > until_ms:
                    continue
                coin = str(r.get("coin") or r.get("currency") or "").upper()
                qty  = r.get("qty") or r.get("amount") or r.get("size") or 0
                wdrs.append({
                    "id": str(r.get("id") or r.get("withdrawId") or f"bybit_wdr_{ts}_{coin}"),
                    "type": "withdrawal",
                    "timestamp": ts,
                    "currency": coin,
                    "amount": float(qty or 0),
                    "status": r.get("status")
                })
        except Exception as e:
            _trace_flow(ex.id.upper(), [f"bybit {ver} withdraw raw failed: {repr(e)}"])
    return deps, wdrs

def _okx_raw(ex, since_ms: int, until_ms: int) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    deps, wdrs = [], []
    use_state = os.environ.get("OKX_USE_STATE_FILTER","1").lower() in ("1","true","yes","on")
    dep_params = {"limit": "100"}; wd_params = {"limit": "100"}
    if use_state:
        dep_params["state"] = "2"; wd_params["state"] = "2"
    try:
        d = ex.privateGetAssetDepositHistory(dep_params) or {}
        for r in (d.get("data") or []):
            ts = _ms(r.get("ts"))
            if ts is None or ts < since_ms or ts > until_ms:
                continue
            deps.append({
                "id": str(r.get("txId") or f"okx_dep_{ts}_{r.get('ccy')}"),
                "type": "deposit",
                "timestamp": ts,
                "currency": str(r.get("ccy") or "").upper(),
                "amount": float(r.get("amt") or 0),
                "status": r.get("state")
            })
    except Exception as e:
        _trace_flow(ex.id.upper(), [f"okx deposit raw failed: {repr(e)}"])
    try:
        w = ex.privateGetAssetWithdrawalHistory(wd_params) or {}
        for r in (w.get("data") or []):
            ts = _ms(r.get("ts"))
            if ts is None or ts < since_ms or ts > until_ms:
                continue
            wdrs.append({
                "id": str(r.get("wdId") or r.get("txId") or f"okx_wdr_{ts}_{r.get('ccy')}"),
                "type": "withdrawal",
                "timestamp": ts,
                "currency": str(r.get("ccy") or "").upper(),
                "amount": float(r.get("amt") or 0),
                "status": r.get("state")
            })
    except Exception as e:
        _trace_flow(ex.id.upper(), [f"okx withdraw raw failed: {repr(e)}"])
    return deps, wdrs

def fetch_funding_events_raw(ex, label: str, lookback_days: Optional[int] = None) -> List[Dict[str, Any]]:
    ex_id = ex.id.lower()
    lookback_days = lookback_days or int(os.environ.get("FLOW_LOOKBACK_DAYS", "14"))
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - lookback_days * 24 * 3600 * 1000
    until_ms = now_ms

    attempts = []
    ccxt_events: List[Dict[str, Any]] = []
    raw_events: List[Dict[str, Any]] = []

    # ccxt helpers first
    try:
        if hasattr(ex, "fetch_deposits"):
            _d = ex.fetch_deposits(since=since_ms) or []
            for d in _d:
                ts = _ms(d.get("timestamp"))
                if ts is None or ts < since_ms or ts > until_ms:
                    continue
                ccxt_events.append({
                    "id": str(d.get("id") or d.get("txid") or f"dep_{ts}"),
                    "type": "deposit",
                    "timestamp": ts,
                    "currency": str(d.get("currency") or "").upper(),
                    "amount": float(d.get("amount") or 0),
                    "status": d.get("status")
                })
            attempts.append({"method": "fetch_deposits", "ok": True, "count": len(_d)})
    except Exception as e:
        attempts.append({"method": "fetch_deposits", "ok": False, "err": repr(e)})

    try:
        if hasattr(ex, "fetch_withdrawals"):
            _w = ex.fetch_withdrawals(since=since_ms) or []
            for w in _w:
                ts = _ms(w.get("timestamp"))
                if ts is None or ts < since_ms or ts > until_ms:
                    continue
                ccxt_events.append({
                    "id": str(w.get("id") or w.get("txid") or f"wdr_{ts}"),
                    "type": "withdrawal",
                    "timestamp": ts,
                    "currency": str(w.get("currency") or "").upper(),
                    "amount": float(w.get("amount") or 0),
                    "status": w.get("status")
                })
            attempts.append({"method": "fetch_withdrawals", "ok": True, "count": len(_w)})
    except Exception as e:
        attempts.append({"method": "fetch_withdrawals", "ok": False, "err": repr(e)})

    # raw calls always
    try:
        if ex_id == "binance":
            d, w = _binance_raw(ex, since_ms, until_ms)
        elif ex_id == "bybit":
            d, w = _bybit_raw(ex, since_ms, until_ms)
        elif ex_id == "okx":
            d, w = _okx_raw(ex, since_ms, until_ms)
        else:
            d, w = [], []
        raw_events.extend(d + w)
        attempts.append({"method": f"{ex_id}_raw", "ok": True, "count": len(d) + len(w)})
    except Exception as e:
        attempts.append({"method": f"{ex_id}_raw", "ok": False, "err": repr(e)})

    # combine and deduplicate before final status filtering
    combined = deduplicate_events([ccxt_events, raw_events], ex_id)

    # keep only final (or all if FLOW_TRUST_NONFINAL=1)
    filtered: List[Dict[str, Any]] = []
    for ev in combined:
        kind = ev.get("type")
        st = ev.get("status")
        if _is_final(ex_id, kind, st):
            filtered.append(ev)

    out = sorted(filtered, key=lambda r: r.get("timestamp", 0))

    # tracing (full, untruncated)
    _trace_flow(label, [f"FUNDING attempts FULL: {json.dumps(attempts)}"])
    _trace_flow(label, [f"FUNDING results: {len(out)} final events in last {lookback_days}d"])
    return out

def format_funding_event(ev: Dict[str, Any], label: str, kind: str, val_usdt: Optional[float]) -> str:
    amount = ev.get("amount")
    currency = str(ev.get("currency"))
    status = ev.get("status")
    txid = (ev.get("txid") or ev.get("id") or "")
    address = ev.get("address") or ""
    tag = ev.get("tag") or ""
    ts = ev.get("timestamp")
    dt = datetime.fromtimestamp(ts / 1000, NY_TZ).strftime("%Y-%m-%d %H:%M:%S %Z") if ts else "n/a"
    parts = [
        f"*{md_escape(label)}* {md_escape(kind)}",
        f"• Amount: `{amount}` {md_escape(currency)}",
        f"• Status: `{md_escape(str(status))}`",
        f"• Time: `{md_escape(dt)}`",
    ]
    if val_usdt is not None:
        parts.append(f"• Valued ≈ `${val_usdt:,.2f}` USDT")
    if address:
        parts.append(f"• Address: `{md_escape(address)}`")
    if tag:
        parts.append(f"• Tag/Memo: `{md_escape(tag)}`")
    if txid:
        parts.append(f"• TxID: `{md_escape(str(txid))}`")
    return "\n".join(parts)

def poll_and_apply_funding(ex, label: str, prices: Dict[str, float], quote_ccy: str, st: Dict[str, Any]) -> int:
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat  = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    evs = fetch_funding_events_raw(ex, label)
    seen = set(st.get("seen_ids", []))
    applied = 0
    val_now_post = float(st.get("__val_now_post") or 0.0)

    for ev in evs:
        ev_id = str(ev.get("id"))
        if ev_id in seen:
            continue
        ccy = (ev.get("currency") or "").upper()
        amt = float(ev.get("amount") or 0.0)
        px  = prices.get(ccy) or (1.0 if ccy == quote_ccy else None)
        val = amt * px if px else None
        if val is None:
            _trace_flow(label, [f"valuation missing for {ccy}, using raw amount {amt}"])

        flow_val = val if val is not None else amt

        if ev.get("type") == "deposit":
            pre = max(val_now_post - (val or 0.0), 0.0)
            close_twr_segment(st, pre, V_start_next=val_now_post)
            st["net_flows"] = float(st.get("net_flows", 0.0)) + float(flow_val)
            kind = "Deposit"
        else:
            pre = val_now_post + (val or 0.0)
            close_twr_segment(st, pre, V_start_next=val_now_post)
            st["net_flows"] = float(st.get("net_flows", 0.0)) - float(flow_val)
            kind = "Withdrawal"

        try:
            msg = format_funding_event(ev, label, kind, val)
            tg_send(telegram_token, telegram_chat, msg)
        except Exception as e:
            print(f"[WARN] Telegram funding notice failed: {e}")

        seen.add(ev_id)
        applied += 1

    st["seen_ids"] = sorted(list(seen))
    _trace_flow(label, [f"APPLIED new_events={applied}, seen_total={len(seen)}"])
    return applied

# ====================== TWR ==================================================

def close_twr_segment(state_acct: Dict[str, Any], V_end_preflow: float, V_start_next: Optional[float] = None):
    denom = (state_acct.get("segment_start_value") or 0.0) + float(state_acct.get("segment_net_flows", 0.0) or 0.0)
    if denom > 0 and V_end_preflow is not None and V_end_preflow >= 0:
        factor = V_end_preflow / denom
        if factor > 0:
            state_acct["twr_factor"] = float(state_acct.get("twr_factor", 1.0)) * factor
    if V_start_next is not None:
        state_acct["segment_start_value"] = float(V_start_next)
    else:
        state_acct["segment_start_value"] = float(V_end_preflow)
    state_acct["segment_net_flows"] = 0.0

# ====================== Charts ==============================================

def chart_asset_breakdown(per_asset_usdt: Dict[str, float], out_path: str, top_n: int = 12):
    items = sorted(per_asset_usdt.items(), key=lambda kv: kv[1], reverse=True)
    top = items[:top_n]
    other_val = sum(v for _, v in items[top_n:])
    labels = [k for k, _ in top]
    sizes = [v for _, v in top]
    if other_val > 0:
        labels.append("OTHER")
        sizes.append(other_val)
    if not sizes:
        sizes = [1]; labels = ["EMPTY"]
    plt.figure()
    plt.title("Asset Breakdown (USDT)")
    plt.pie(sizes, labels=labels, autopct="%1.1f%%")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

def chart_venue_breakdown(per_label_usdt: Dict[str, float], out_path: str):
    labels = list(per_label_usdt.keys())
    values = [per_label_usdt[k] for k in labels]
    if not values:
        labels = ["EMPTY"]; values = [1]
    plt.figure()
    plt.title("Venue Breakdown (USDT)")
    plt.bar(range(len(labels)), values)
    plt.xticks(range(len(labels)), labels, rotation=20, ha="right")
    plt.ylabel("Value (USDT)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()

# ====================== Excel helpers =======================================

def _sanitize_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            if getattr(df[col].dt, "tz", None) is not None:
                try:
                    df[col] = df[col].dt.tz_convert(None)
                except Exception:
                    try:
                        df[col] = df[col].dt.tz_localize(None)
                    except Exception:
                        df[col] = df[col].astype(str)
    return df

def _history_csv_path() -> str:
    excel_dir = ensure_dir(os.environ.get("EXCEL_DIR", "./exports"))
    return os.path.join(excel_dir, "history.csv")

def _append_history(date_str: str, accounts_values: Dict[str, Dict[str, Any]], quote_ccy: str):
    rows = []
    for lb, dat in accounts_values.items():
        val = float(dat.get("value", 0.0))
        start = float(dat.get("start_value") or 0.0)
        flows = float(dat.get("net_flows") or 0.0)
        d_usd = val - start - flows
        d_pct = (d_usd / start) if start else 0.0
        rows.append({
            "date": date_str,
            "account": lb,
            f"value_{quote_ccy.lower()}": val,
            "start_value": start,
            "net_flows": flows,
            "daily_return_usd": d_usd,
            "daily_return_pct": d_pct,
        })
    hist_path = _history_csv_path()
    df_new = pd.DataFrame(rows)
    if os.path.exists(hist_path):
        try:
            df_old = pd.read_csv(hist_path)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            df_all = df_new
    else:
        df_all = df_new
    df_all = df_all.sort_values(["date","account"]).drop_duplicates(subset=["date","account"], keep="last")
    df_all.to_csv(hist_path, index=False)

def export_excel_snapshot(now_ny: datetime,
                          accounts_values: Dict[str, Dict[str, Any]],
                          per_asset_total: Dict[str, float],
                          per_label_values: Dict[str, float],
                          fiat_rates: Dict[str, float],
                          quote_ccy: str) -> str:
    excel_dir = ensure_dir(os.environ.get("EXCEL_DIR", "./exports"))
    fname = f"Daily_Valuation_Snapshot_{now_ny.strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(excel_dir, fname)

    snap_rows = []
    for lb, dat in sorted(accounts_values.items()):
        val = float(dat.get("value", 0.0))
        row = {
            "Account": lb,
            f"Value_{quote_ccy}": val,
            "Start_Value": float(dat.get("start_value") or 0.0),
            "Net_Flows": float(dat.get("net_flows") or 0.0),
            "TWR_Factor": float(dat.get("twr_factor") or 1.0),
        }
        for fiat, rate in fiat_rates.items():
            row[f"Value_{fiat}"] = val * float(rate)
        start = row["Start_Value"]; flows = row["Net_Flows"]
        d_usd = val - start - flows
        d_pct = (d_usd / start) if start else 0.0
        row["Daily_Return_USD"] = d_usd
        row["Daily_Return_Pct"] = d_pct
        snap_rows.append(row)
    df_snap = pd.DataFrame(snap_rows)

    frames = {"Snapshot": _sanitize_df_for_excel(df_snap)}

    # History (cumulative return % and rolling P&L)
    if os.environ.get("EXCEL_APPEND_HISTORY", "1").lower() in ("1","true","yes","on"):
        # Ensure the current snapshot is present in history before reading.
        # This allows the History sheet to include at least today's values on a
        # fresh run where no history.csv exists yet.
        _append_history(now_ny.strftime("%Y-%m-%d"), accounts_values, quote_ccy)
        hist_path = _history_csv_path()
        if os.path.exists(hist_path):
            df_hist = pd.read_csv(hist_path)
        else:
            df_hist = pd.DataFrame(columns=["date","account",f"value_{quote_ccy.lower()}",
                                            "start_value","net_flows","daily_return_usd","daily_return_pct"])
        if not df_hist.empty:
            df_hist["date"] = df_hist["date"].astype(str)
            df_hist = df_hist.sort_values(["account","date"])
            def _add_cum_rolling(g: pd.DataFrame) -> pd.DataFrame:
                g = g.copy()
                g["cumulative_return_pct"] = (1.0 + g["daily_return_pct"].fillna(0.0)).cumprod() - 1.0
                g["rolling_pnl_usd"] = g["daily_return_usd"].fillna(0.0).cumsum()
                return g
            df_hist = df_hist.groupby("account", group_keys=False).apply(_add_cum_rolling)
        frames["History"] = _sanitize_df_for_excel(df_hist)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in frames.items():
            df.to_excel(writer, index=False, sheet_name=name)

    return path

# ====================== Snapshot message =====================================

def format_snapshot_msg(accounts_values: Dict[str, Dict[str, Any]], total_value: float, date_str: str,
                        fiat_rates: Dict[str, float], quote_ccy: str) -> str:
    lines = []
    header = f"*Daily Snapshot* — {md_escape(date_str)} (4pm NY)"
    lines.append(header)
    lines.append("")
    for label, dat in sorted(accounts_values.items()):
        val = dat["value"]
        start = dat.get("start_value")
        flows = dat.get("net_flows", 0.0)
        money_ret = None
        if start and start != 0:
            money_ret = (val - start - flows) / start
        twr = dat.get("twr_factor", 1.0) - 1.0
        fiats_line = []
        for fiat, rate in fiat_rates.items():
            amt = val * rate
            fiats_line.append(f"{fiat}: `${amt:,.2f}`")
        conv_str = " | ".join(fiats_line)
        lines.append(f"*{md_escape(label)}*")
        lines.append(f"• Value ({md_escape(quote_ccy)}): `${val:,.2f}`")
        if conv_str:
            lines.append(f"• {md_escape(conv_str)}")
        if start is not None:
            lines.append(f"• Start: `${start:,.2f}`  • Flows: `${flows:,.2f}`")
            if money_ret is not None:
                lines.append(f"• Money\\-weighted: `{money_ret*100:.2f}%`")
        lines.append(f"• TWR: `{twr*100:.2f}%`")
        lines.append("")
    fiats_total = " | ".join([f"{k}: `${(total_value*v):,.2f}`" for k, v in fiat_rates.items()])
    lines.append(f"*Total Portfolio:* `{total_value:,.2f}` {md_escape(quote_ccy)}")
    if fiats_total:
        lines.append(f"*Converted:* {md_escape(fiats_total)}")
    return "\n".join(lines)

# ====================== Main =================================================

def main():
    load_dotenv(override=True)

    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat  = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not (telegram_token and telegram_chat):
        raise SystemExit("Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in your environment.")

    accounts_csv = os.environ.get("EXCHANGES", "binance_01,bybit_main,okx_01")
    account_names = [e.strip() for e in accounts_csv.split(",") if e.strip()]

    creds = {k: v for k, v in os.environ.items()
             if k.endswith("_API_KEY") or k.endswith("_API_SECRET") or k.endswith("_API_PASSPHRASE")}

    poll_seconds = int(os.environ.get("POLL_SECONDS", "60"))
    enable_polling = os.environ.get("ENABLE_POLLING", "1") in ("1","true","yes","on")

    quote_ccy = os.environ.get("QUOTE_CCY", "USDT").upper()
    pricing_ex_name = os.environ.get("PRICING_EXCHANGE", "binance").lower()
    pricing_ex = make_pricing_exchange(pricing_ex_name)

    report_fiats = [f.strip().upper() for f in os.environ.get("REPORT_FIATS", "USD,EUR").split(",") if f.strip()]
    charts_enabled = os.environ.get("ENABLE_CHARTS", "1") in ("1","true","yes","on")
    charts_dir = ensure_dir(os.environ.get("CHART_DIR", "./charts"))
    chart_top_assets = int(os.environ.get("CHART_TOP_ASSETS", "12"))

    excel_enabled = os.environ.get("EXCEL_EXPORT", "1").lower() in ("1","true","yes","on")
    excel_send_tg = os.environ.get("EXCEL_TELEGRAM", "1").lower() in ("1","true","yes","on")
    excel_on_start = os.environ.get("EXCEL_ON_STARTUP", "1").lower() in ("1","true","yes","on")

    # Build exchanges
    ex_objs: Dict[str, Any] = {}
    labels: List[str] = []
    for name in account_names:
        try:
            ex, label = make_exchange(name, creds)
            ex_objs[label] = ex
            labels.append(label)
        except Exception as e:
            print(f"[ERROR] Could not init {name}: {e}")

    # State load/init
    state = load_state()
    last_snap = state.get("last_snapshot_date_ny")
    accounts_state = state.get("accounts", {})
    for lb in labels:
        accounts_state.setdefault(lb, {
            "start_value": None,
            "net_flows": 0.0,
            "seen_ids": [],
            "twr_factor": 1.0,
            "segment_start_value": None,
            "segment_net_flows": 0.0,
        })

    print("[INFO] Multi-account monitor with TWR started.")
    sent_startup_ok = False
    did_startup_excel = False

    while True:
        try:
            now_ny = datetime.now(NY_TZ)

            # --- Fetch balances ---
            bal_per_account: Dict[str, Dict[str, float]] = {}
            all_assets = set([quote_ccy])
            for lb, ex in ex_objs.items():
                bals = fetch_all_wallet_balances(ex)
                bal_per_account[lb] = bals
                all_assets.update([a.upper() for a in bals.keys()])

            # --- Pricing ---
            prices = fetch_prices_usdt(pricing_ex, sorted(all_assets))
            prices[quote_ccy] = 1.0
            fiat_rates = get_fiat_rates_per_usdt(pricing_ex, report_fiats)

            # --- Current valuations ---
            current_values: Dict[str, float] = {}
            per_asset_total: Dict[str, float] = {}
            for lb, bals in bal_per_account.items():
                val, per_asset_val = value_portfolio_usdt(bals, prices)
                current_values[lb] = val
                for a, v in per_asset_val.items():
                    per_asset_total[a] = per_asset_total.get(a, 0.0) + v

            # --- Startup OK (once) ---
            if (not sent_startup_ok) and (os.environ.get("SEND_STARTUP_OK", "1").lower() in ("1","true","yes","on")):
                try:
                    total_now = sum(current_values.values())
                    ts_str = now_ny.strftime("%Y-%m-%d %H:%M:%S %Z")
                    lines = []
                    lines.append(f"*Monitor online* — {md_escape(ts_str)}")
                    lines.append(f"• Quote: {md_escape(quote_ccy)}  • Pricing: {md_escape(pricing_ex_name.upper())}")
                    for lb in sorted(current_values.keys()):
                        v = current_values.get(lb, 0.0)
                        lines.append(f"• {md_escape(lb)}: `${v:,.2f}`")
                    conv_bits = [f"{fiat}: `${(total_now*rate):,.2f}`" for fiat, rate in fiat_rates.items()]
                    lines.append(f"*Total:* `${total_now:,.2f}` {md_escape(quote_ccy)}  —  {md_escape(' | '.join(conv_bits))}")
                    tg_send(telegram_token, telegram_chat, "\n".join(lines))
                    sent_startup_ok = True
                except Exception as _e:
                    print(f"[WARN] Startup success notification failed: {_e}")

            # --- Init start/segment ---
            for lb in labels:
                st = accounts_state[lb]
                if st["start_value"] is None:
                    st["start_value"] = current_values.get(lb, 0.0)
                if st["segment_start_value"] is None:
                    st["segment_start_value"] = current_values.get(lb, 0.0)

            # --- Funding poll ---
            if enable_polling:
                for lb, ex in ex_objs.items():
                    st = accounts_state[lb]
                    st["__val_now_post"] = current_values.get(lb, 0.0)
                    try:
                        applied = poll_and_apply_funding(ex, lb, prices, quote_ccy, st)
                        accounts_state[lb] = st
                    except Exception as e:
                        print(f"[INFO] funding poll failed on {ex.id} [{lb}]: {e}")
                    finally:
                        st.pop("__val_now_post", None)

            # --- Compose snapshot dicts ---
            accounts_values: Dict[str, Dict[str, Any]] = {}
            total_value = 0.0
            for lb in labels:
                val = current_values.get(lb, 0.0)
                st = accounts_state.get(lb, {})
                accounts_values[lb] = {
                    "value": val,
                    "start_value": st.get("start_value"),
                    "net_flows": st.get("net_flows", 0.0),
                    "twr_factor": st.get("twr_factor", 1.0),
                }
                total_value += val

            # --- Excel on startup (optional) ---
            if excel_enabled and excel_on_start and not did_startup_excel:
                try:
                    if os.environ.get("STARTUP_EXPORT_APPEND", "0").lower() in ("1","true","yes","on"):
                        _append_history(now_ny.strftime("%Y-%m-%d"), accounts_values, quote_ccy)
                    xls = export_excel_snapshot(now_ny, accounts_values,
                                                per_asset_total,
                                                {lb: current_values.get(lb, 0.0) for lb in labels},
                                                fiat_rates, quote_ccy)
                    if excel_send_tg:
                        tg_send_document(telegram_token, telegram_chat, xls,
                                         caption=md_escape("Daily Valuation Snapshot (startup)"))
                    did_startup_excel = True
                except Exception as e:
                    print(f"[WARN] Startup Excel export failed: {e}")

            # --- Daily 4pm snapshot ---
            if last_snap != now_ny.strftime("%Y-%m-%d") and now_ny.hour >= 16:
                for lb in labels:
                    st = accounts_state[lb]
                    V_now = current_values.get(lb, 0.0)
                    close_twr_segment(st, V_now, V_start_next=V_now)

                date_str = now_ny.strftime("%Y-%m-%d %H:%M:%S %Z")
                msg = format_snapshot_msg(accounts_values, total_value, date_str, fiat_rates, quote_ccy)
                tg_send(telegram_token, telegram_chat, msg)

                if charts_enabled:
                    per_label_usdt = {lb: current_values.get(lb, 0.0) for lb in labels}
                    asset_chart = os.path.join(charts_dir, f"assets_{now_ny.strftime('%Y%m%d')}.png")
                    venue_chart = os.path.join(charts_dir, f"venues_{now_ny.strftime('%Y%m%d')}.png")
                    chart_asset_breakdown(per_asset_total, asset_chart, top_n=chart_top_assets)
                    chart_venue_breakdown(per_label_usdt, venue_chart)
                    tg_send_photo(telegram_token, telegram_chat, asset_chart, caption=md_escape("Asset breakdown (USDT)"))
                    tg_send_photo(telegram_token, telegram_chat, venue_chart, caption=md_escape("Venue breakdown (USDT)"))

                if excel_enabled:
                    try:
                        _append_history(now_ny.strftime("%Y-%m-%d"), accounts_values, quote_ccy)
                        xls = export_excel_snapshot(now_ny, accounts_values,
                                                    per_asset_total,
                                                    {lb: current_values.get(lb, 0.0) for lb in labels},
                                                    fiat_rates, quote_ccy)
                        if excel_send_tg:
                            tg_send_document(telegram_token, telegram_chat, xls,
                                             caption=md_escape("Daily Valuation Snapshot (4pm NY)"))
                    except Exception as e:
                        print(f"[WARN] Excel export failed: {e}")

                for lb in labels:
                    st = accounts_state[lb]
                    st["start_value"] = current_values.get(lb, 0.0)
                    st["net_flows"] = 0.0
                    st["twr_factor"] = 1.0
                    st["segment_start_value"] = current_values.get(lb, 0.0)
                    st["segment_net_flows"] = 0.0
                last_snap = now_ny.strftime("%Y-%m-%d")

            # Persist state
            save_state({"last_snapshot_date_ny": last_snap, "accounts": accounts_state})

        except KeyboardInterrupt:
            print("\n[INFO] Stopping monitor...")
            break
        except Exception:
            err = traceback.format_exc()
            print("[ERROR] Unhandled exception in loop:\n", err)
        finally:
            time.sleep(poll_seconds if enable_polling else 10)

if __name__ == "__main__":
    main()

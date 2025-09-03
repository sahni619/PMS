import json
import os
import sys
import types
from datetime import datetime

ccxt_stub = types.SimpleNamespace()
sys.modules.setdefault("ccxt", ccxt_stub)
dotenv_stub = types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
sys.modules.setdefault("dotenv", dotenv_stub)
requests_stub = types.ModuleType("requests")
pytz_stub = types.ModuleType("pytz")
pytz_stub.timezone = lambda name: types.SimpleNamespace()
matplotlib_stub = types.ModuleType("matplotlib")
matplotlib_stub.use = lambda *args, **kwargs: None
matplotlib_pyplot_stub = types.ModuleType("matplotlib.pyplot")
pandas_stub = types.ModuleType("pandas")
pandas_stub.DataFrame = type("DataFrame", (), {})
sys.modules.setdefault("requests", requests_stub)
sys.modules.setdefault("pytz", pytz_stub)
sys.modules.setdefault("matplotlib", matplotlib_stub)
sys.modules.setdefault("matplotlib.pyplot", matplotlib_pyplot_stub)
sys.modules.setdefault("pandas", pandas_stub)

import account_monitor as am


def test_pending_ccxt_event_replaced_by_final_raw(monkeypatch):
    class DummyEx:
        id = "binance"

        def fetch_deposits(self, since=None):
            return [
                {
                    "id": "1",
                    "timestamp": since + 1000,
                    "currency": "BTC",
                    "amount": 1,
                    "status": "pending",
                }
            ]

        def fetch_withdrawals(self, since=None):
            return []

    ex = DummyEx()

    called = {"raw": False}

    def fake_binance_raw(ex, since_ms, until_ms):
        called["raw"] = True
        return (
            [
                {
                    "id": "1",
                    "type": "deposit",
                    "timestamp": since_ms + 1000,
                    "currency": "BTC",
                    "amount": 1,
                    "status": "success",
                }
            ],
            [],
        )

    monkeypatch.setattr(am, "_binance_raw", fake_binance_raw)

    logs = []

    def fake_trace(label, lines):
        logs.extend(lines)

    monkeypatch.setattr(am, "_trace_flow", fake_trace)

    events = am.fetch_funding_events_raw(ex, "LBL", lookback_days=1)

    assert called["raw"] is True
    assert len(events) == 1
    ev = events[0]
    assert ev["id"] == "1"
    assert ev["status"] == "success"

    attempt_line = next(l for l in logs if l.startswith("FUNDING attempts FULL:"))
    attempts = json.loads(attempt_line.split(":", 1)[1].strip())
    methods = {a["method"] for a in attempts}
    assert "fetch_deposits" in methods
    assert "binance_raw" in methods



def test_non_final_event_logged_and_skipped(monkeypatch):

def test_final_withdrawal_event_survives(monkeypatch):
 
    class DummyEx:
        id = "binance"

        def fetch_deposits(self, since=None):

            return [
                {
                    "id": "1",
                    "timestamp": since + 1000,
                    "currency": "BTC",
                    "amount": 1,
                    "status": "pending",
                }
            ]

        def fetch_withdrawals(self, since=None):
            return []

    ex = DummyEx()

    # no raw events available
    monkeypatch.setattr(am, "_binance_raw", lambda ex, s, u: ([], []))

    logs = []

    def fake_trace(label, lines):
        logs.extend(lines)

    monkeypatch.setattr(am, "_trace_flow", fake_trace)

    events = am.fetch_funding_events_raw(ex, "LBL", lookback_days=1)

    assert events == []

    skipped = [l for l in logs if "skipped non-final" in l]
    assert skipped, "skip log missing"
    assert any("id=1" in l and "label=LBL" in l for l in skipped)


            return []

        def fetch_withdrawals(self, since=None):
            return [
                {
                    "id": "w1",
                    "timestamp": (since or 0) + 1000,
                    "currency": "BTC",
                    "amount": 1,
                    "status": "5",
                }
            ]

    monkeypatch.setattr(am, "_binance_raw", lambda ex, since_ms, until_ms: ([], []))

    events = am.fetch_funding_events_raw(DummyEx(), "LBL", lookback_days=1)

    assert len(events) == 1
    assert events[0]["id"] == "w1"
    assert events[0]["status"] == "5"


def test_missing_price_flows_reflected_in_excel(monkeypatch, tmp_path):
    events = [
        {"id": "1", "type": "deposit", "currency": "ABC", "amount": 10},
        {"id": "2", "type": "withdrawal", "currency": "DEF", "amount": 3},
    ]

    monkeypatch.setattr(am, "fetch_funding_events_raw", lambda ex, label: events)
    monkeypatch.setattr(am, "tg_send", lambda *a, **k: None)

    logs = []

    def fake_trace(label, lines):
        logs.extend(lines)

    monkeypatch.setattr(am, "_trace_flow", fake_trace)

    state = {"__val_now_post": 100.0}
    am.poll_and_apply_funding(object(), "LBL", {}, "USDT", state)

    assert state["net_flows"] == 7
    assert any("valuation missing" in l for l in logs)

    written = {}

    class DummyDF:
        def __init__(self, rows):
            self.rows = rows
            self.columns = list(rows[0].keys()) if rows else []

        def to_excel(self, writer, index=False, sheet_name=None):
            written[sheet_name] = self.rows

    class DummyWriter:
        def __init__(self, path, engine=None):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(
        am,
        "pd",
        types.SimpleNamespace(DataFrame=DummyDF, ExcelWriter=DummyWriter),
    )
    monkeypatch.setattr(am, "_sanitize_df_for_excel", lambda df: df)
    monkeypatch.setattr(am, "ensure_dir", lambda p: str(tmp_path))
    os.environ["EXCEL_APPEND_HISTORY"] = "0"
    os.environ["EXCEL_DIR"] = str(tmp_path)

    am.export_excel_snapshot(
        datetime(2023, 1, 1),
        {"LBL": state},
        {},
        {},
        {},
        "USDT",
    )

    assert written["Snapshot"][0]["Net_Flows"] == 7


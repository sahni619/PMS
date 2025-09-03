import json
import sys
import types

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


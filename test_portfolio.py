import logging
import pytest

from portfolio import Portfolio


def test_pending_event_when_price_missing(caplog):
    p = Portfolio(prices={})
    with caplog.at_level(logging.INFO):
        evt = p.add_funding_event("BTC", 1)
    assert evt.val is None
    assert evt.status == "pending valuation"
    assert evt in p.pending
    assert "Pending valuation" in caplog.text


def test_event_revaluated_when_price_available():
    p = Portfolio(prices={})
    p.add_funding_event("ETH", 2)
    p.update_prices({"ETH": 10})
    assert not p.pending
    assert p.net_flows == pytest.approx(20)
    assert p.twr != 1.0


def test_unresolved_assets_logged(caplog):
    p = Portfolio(prices={})
    p.add_funding_event("XRP", 5)
    with caplog.at_level(logging.WARNING):
        p.update_prices({})
    assert "Unresolved asset" in caplog.text

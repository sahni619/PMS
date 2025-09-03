import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

@dataclass
class FundingEvent:
    ccy: str
    amount: float
    val: Optional[float] = None
    status: str = "pending valuation"

class Portfolio:
    """A simple portfolio to track funding events and compute net flows/TWR."""
    def __init__(self, prices: Optional[Dict[str, float]] = None):
        self.prices: Dict[str, float] = prices or {}
        self.pending: List[FundingEvent] = []
        self.net_flows: float = 0.0
        self.twr: float = 1.0

    def add_funding_event(self, ccy: str, amount: float) -> FundingEvent:
        price = self.prices.get(ccy)
        if price is None:
            event = FundingEvent(ccy=ccy, amount=amount)
            self.pending.append(event)
            logger.info("Pending valuation for %s", ccy)
            return event
        val = amount * price
        event = FundingEvent(ccy=ccy, amount=amount, val=val, status="valued")
        self._apply_event(event)
        return event

    def update_prices(self, new_prices: Dict[str, float]) -> None:
        self.prices.update(new_prices)
        remaining: List[FundingEvent] = []
        for event in self.pending:
            price = self.prices.get(event.ccy)
            if price is None:
                logger.warning("Unresolved asset for valuation: %s", event.ccy)
                remaining.append(event)
                continue
            event.val = event.amount * price
            event.status = "valued"
            self._apply_event(event)
        self.pending = remaining

    def _apply_event(self, event: FundingEvent) -> None:
        assert event.val is not None
        self.net_flows += event.val
        # Simplified TWR adjustment: treat event value as return factor
        self.twr *= (1 + event.val)

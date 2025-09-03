# PMS


Utilities for determining whether deposit or withdrawal statuses returned by an
exchange represent a **final** state. The module exposes predefined sets for
Binance, Bybit and OKX and allows additional statuses to be injected through the
`EXTRA_FINAL_STATUSES` environment variable.

Example usage:

```python
from final_statuses import is_final

is_final("binance", "withdraw", 6)  # True for a completed withdrawal
```

Utility for processing event data.

- `event_deduplicator.py` contains logic to deduplicate events collected from
  multiple pages. It removes duplicates using exchange-specific unique IDs or
  a composite of `(txId, currency, timestamp)` and logs debug counts of pages
  and deduped totals to aid troubleshooting.

A minimal portfolio management system demonstrating funding event handling
and pending valuation reprocessing.



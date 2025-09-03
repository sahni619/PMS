# PMS

Utility for processing event data.

- `event_deduplicator.py` contains logic to deduplicate events collected from
  multiple pages. It removes duplicates using exchange-specific unique IDs or
  a composite of `(txId, currency, timestamp)` and logs debug counts of pages
  and deduped totals to aid troubleshooting.

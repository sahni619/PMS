import logging
from typing import Iterable, List, Dict, Any, Hashable, Optional, Dict as TDict

from final_statuses import is_final as _is_final

logger = logging.getLogger(__name__)

def _event_is_final(exchange: Optional[str], event: Dict[str, Any]) -> bool:
    """Return ``True`` if the event status is final for ``exchange``.

    The helper is deliberately defensive: any missing fields or unsupported
    values simply yield ``False`` rather than raising, mirroring the behaviour
    of the production code where best effort is preferred over strict
    validation.
    """

    if not exchange:
        return False

    kind = str(event.get("type") or "").lower()
    if kind == "withdrawal":
        kind = "withdraw"
    status = event.get("status")
    if kind not in ("deposit", "withdraw") or status is None:
        return False
    try:
        return _is_final(exchange, kind, status)
    except Exception:  # pragma: no cover - safety guard
        return False


def deduplicate_events(
    pages: Iterable[Iterable[Dict[str, Any]]], exchange: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Deduplicate events collected from multiple pages.

    Parameters:
        pages: Iterable of pages, each page being an iterable of event dicts.

    Returns:
        List of unique events. Events are deduplicated using an exchange-specific
        unique ID if present (e.g., 'uid', 'id', or 'exchange_id'). If no unique
        ID exists, a composite key of ('txId', 'currency', 'timestamp') is used.

        When duplicates share the same key, preference is given to the record
        whose status is considered *final* according to
        :func:`final_statuses.is_final`. This allows pending ccxt events to be
        replaced by later raw events that have reached a terminal state.

    Debug logs include the number of pages, total events processed, and the
    count after deduplication.
    """
    pages_list = list(pages)
    total_pages = len(pages_list)
    total_events = sum(len(page) for page in pages_list)
    logger.debug("Fetched %d pages containing %d events", total_pages, total_events)

    seen: TDict[Hashable, int] = {}
    unique_events: List[Dict[str, Any]] = []

    for page in pages_list:
        for event in page:
            unique_id = (
                event.get("uid")
                or event.get("id")
                or event.get("exchange_id")
            )
            key = unique_id if unique_id is not None else (
                event.get("txId"),
                event.get("currency"),
                event.get("timestamp"),
            )

            idx = seen.get(key)
            if idx is None:
                seen[key] = len(unique_events)
                unique_events.append(event)
            else:
                existing = unique_events[idx]
                if _event_is_final(exchange, event) and not _event_is_final(
                    exchange, existing
                ):
                    unique_events[idx] = event

    logger.debug("Deduplicated events count: %d", len(unique_events))
    return unique_events

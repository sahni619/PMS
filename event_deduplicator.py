import logging
from typing import Iterable, List, Dict, Any, Hashable, Set

logger = logging.getLogger(__name__)

def deduplicate_events(pages: Iterable[Iterable[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Deduplicate events collected from multiple pages.

    Parameters:
        pages: Iterable of pages, each page being an iterable of event dicts.

    Returns:
        List of unique events. Events are deduplicated using an exchange-specific
        unique ID if present (e.g., 'uid', 'id', or 'exchange_id'). If no unique
        ID exists, a composite key of ('txId', 'currency', 'timestamp') is used.

    Debug logs include the number of pages, total events processed, and the
    count after deduplication.
    """
    pages_list = list(pages)
    total_pages = len(pages_list)
    total_events = sum(len(page) for page in pages_list)
    logger.debug("Fetched %d pages containing %d events", total_pages, total_events)

    seen: Set[Hashable] = set()
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
            if key not in seen:
                seen.add(key)
                unique_events.append(event)

    logger.debug("Deduplicated events count: %d", len(unique_events))
    return unique_events

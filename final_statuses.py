"""Utilities for determining final statuses for exchange deposits/withdrawals.

The sets are based on the public exchange documentation and list the codes or
text values that represent *final* states for each exchange and direction.

An `EXTRA_FINAL_STATUSES` environment variable may be supplied with a comma
separated list of additional statuses that should be treated as final without
needing code changes.
"""
from __future__ import annotations

from typing import Dict, Set
import os

# Optional comma separated override, intended for quick experimentation or
# temporarily dealing with undocumented statuses discovered in the wild.
EXTRA_FINAL_STATUSES: Set[str] = {
    s.strip().lower()
    for s in os.environ.get("EXTRA_FINAL_STATUSES", "").split(",")
    if s.strip()
}

# Final statuses for Binance.
# Source: https://binance-docs.github.io/apidocs/spot/en/
_status_final_binance: Dict[str, Set[str]] = {
    # Deposit history returns status codes: 0 (pending), 1 (success),
    # 6 (credited but cannot withdraw). Only 1 and 6 are terminal.
    "deposit": {"1", "6", "success", "credited but cannot withdraw"},
    # Withdraw history returns status codes:
    # 0 (email sent), 1 (cancelled), 2 (awaiting approval), 3 (rejected),
    # 4 (processing), 5 (failure), 6 (completed). Final states exclude the
    # in‑flight ones.
    "withdraw": {
        "1",
        "3",
        "5",
        "6",
        "cancelled",
        "canceled",
        "rejected",
        "failure",
        "completed",
    },
}

# Final statuses for Bybit.
# Source: https://bybit-exchange.github.io/docs/
_status_final_bybit: Dict[str, Set[str]] = {
    # Deposit records: 0 (pending), 1 (to be confirmed), 2 (confirming),
    # 3 (success), 4 (failed).
    "deposit": {"3", "4", "success", "failed"},
    # Withdraw records: 0–4 (various pending stages), 5 (completed),
    # 6 (cancelled), 7 (rejected/failed), 8 (expired).
    "withdraw": {
        "5",
        "6",
        "7",
        "8",
        "cancelled",
        "rejected",
        "failed",
        "expired",
        "completed",
    },
}

# Final statuses for OKX.
# Source: https://www.okx.com/docs/
_status_final_okx: Dict[str, Set[str]] = {
    # Deposit: 0 (pending), 1 (confirmation), 2 (success), 8 (deposit credited),
    # 9 (failed).
    "deposit": {"2", "8", "9", "success", "credited", "failed"},
    # Withdraw: 0–5 (pending states), 6 (completed), 7 (cancelled),
    # 8 (awaiting confirmation – treated as final by OKX), 9 (failed),
    # 10 (rejected).
    "withdraw": {
        "6",
        "7",
        "8",
        "9",
        "10",
        "completed",
        "cancelled",
        "failed",
        "rejected",
    },
}

# Lookup table mapping exchange name to the dictionary of final statuses.
_FINAL_STATUS_MAP: Dict[str, Dict[str, Set[str]]] = {
    "binance": _status_final_binance,
    "bybit": _status_final_bybit,
    "okx": _status_final_okx,
}


def is_final(exchange: str, direction: str, status: str) -> bool:
    """Return ``True`` if ``status`` is considered final for ``exchange``.

    Parameters
    ----------
    exchange:
        Exchange name (``binance``, ``bybit`` or ``okx``).
    direction:
        ``"deposit"`` or ``"withdraw"``.
    status:
        Status value returned by the exchange. The comparison is case-insensitive
        and accepts both textual and numeric forms. Any statuses provided through
        ``EXTRA_FINAL_STATUSES`` are considered final for all exchanges.
    """

    status_normalised = str(status).strip().lower()
    if status_normalised in EXTRA_FINAL_STATUSES:
        return True
    try:
        exchange_map = _FINAL_STATUS_MAP[exchange.lower()][direction.lower()]
    except KeyError as exc:  # pragma: no cover - sanity guard
        raise ValueError(
            f"Unsupported exchange '{exchange}' or direction '{direction}'"
        ) from exc
    return status_normalised in exchange_map


__all__ = [
    "EXTRA_FINAL_STATUSES",
    "_status_final_binance",
    "_status_final_bybit",
    "_status_final_okx",
    "is_final",
]

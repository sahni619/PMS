import requests
from typing import Any, Dict, List, Optional, Tuple


BINANCE_DEPOSIT_URL = "https://api.binance.com/sapi/v1/capital/deposit/hisrec"
BINANCE_WITHDRAW_URL = "https://api.binance.com/sapi/v1/capital/withdraw/history"
BYBIT_DEPOSIT_URL = "https://api.bybit.com/v5/asset/deposit/query-deposit-list"
BYBIT_WITHDRAW_URL = "https://api.bybit.com/v5/asset/withdraw/query-withdraw-list"
OKX_DEPOSIT_URL = "https://www.okx.com/api/v5/asset/deposit-history"
OKX_WITHDRAW_URL = "https://www.okx.com/api/v5/asset/withdrawal-history"


def _binance_raw(api_key: str, start_time: int, end_time: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch Binance deposit and withdrawal history between ``start_time`` and
    ``end_time``.

    The Binance APIs do not expose cursors.  Pagination is achieved by
    repeatedly requesting a window using ``startTime``/``endTime`` and then
    advancing ``startTime`` to the timestamp of the last row retrieved.  All
    pages are aggregated before any status based filtering is applied.
    """
    headers = {"X-MBX-APIKEY": api_key}

    deposits: List[Dict[str, Any]] = []
    current = start_time
    while current < end_time:
        params = {"startTime": current, "endTime": end_time}
        response = requests.get(BINANCE_DEPOSIT_URL, params=params, headers=headers, timeout=10)
        page = response.json()
        if not page:
            break
        deposits.extend(page)
        current = max(int(row.get("insertTime", current)) for row in page) + 1

    withdrawals: List[Dict[str, Any]] = []
    current = start_time
    while current < end_time:
        params = {"startTime": current, "endTime": end_time}
        response = requests.get(BINANCE_WITHDRAW_URL, params=params, headers=headers, timeout=10)
        page = response.json()
        if not page:
            break
        withdrawals.extend(page)
        current = max(int(row.get("applyTime", current)) for row in page) + 1

    # status filtering happens after aggregation
    deposits = [row for row in deposits if str(row.get("status")) == "1"]
    withdrawals = [row for row in withdrawals if str(row.get("status")).lower() == "completed"]
    return deposits, withdrawals


def _bybit_raw(api_key: str, api_secret: str, start_time: Optional[int] = None, end_time: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch Bybit deposit and withdrawal history using v5 API.

    Pagination relies on the ``cursor`` field returned by Bybit.  We continue to
    request new pages until Bybit returns an empty page or no cursor.  All pages
    are combined before applying status filters.
    """
    headers = {"X-BAPI-API-KEY": api_key, "X-BAPI-SIGN": api_secret}

    deposits: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        params: Dict[str, Any] = {}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if cursor:
            params["cursor"] = cursor
        response = requests.get(BYBIT_DEPOSIT_URL, params=params, headers=headers, timeout=10)
        data = response.json().get("result", {})
        page = data.get("list", [])
        if not page:
            break
        deposits.extend(page)
        cursor = data.get("nextPageCursor")
        if not cursor:
            break

    withdrawals: List[Dict[str, Any]] = []
    cursor = None
    while True:
        params = {}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if cursor:
            params["cursor"] = cursor
        response = requests.get(BYBIT_WITHDRAW_URL, params=params, headers=headers, timeout=10)
        data = response.json().get("result", {})
        page = data.get("list", [])
        if not page:
            break
        withdrawals.extend(page)
        cursor = data.get("nextPageCursor")
        if not cursor:
            break

    deposits = [row for row in deposits if str(row.get("status")).lower() == "success"]
    withdrawals = [row for row in withdrawals if str(row.get("status")).lower() == "success"]
    return deposits, withdrawals


def _okx_raw(api_key: str, start_time: Optional[int] = None, end_time: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch OKX deposit and withdrawal history.

    OKX uses ``before``/``after`` style pagination.  We keep requesting earlier
    pages by setting ``before`` to the timestamp of the oldest row we've seen
    until an empty page is returned.  Aggregation of all pages happens before
    status filtering.
    """
    headers = {"OK-ACCESS-KEY": api_key}

    deposits: List[Dict[str, Any]] = []
    before: Optional[str] = None
    while True:
        params: Dict[str, Any] = {}
        if before:
            params["before"] = before
        if start_time is not None:
            params["after"] = start_time
        response = requests.get(OKX_DEPOSIT_URL, params=params, headers=headers, timeout=10)
        page = response.json().get("data", [])
        if not page:
            break
        deposits.extend(page)
        before = page[-1].get("ts")

    withdrawals: List[Dict[str, Any]] = []
    before = None
    while True:
        params = {}
        if before:
            params["before"] = before
        if start_time is not None:
            params["after"] = start_time
        response = requests.get(OKX_WITHDRAW_URL, params=params, headers=headers, timeout=10)
        page = response.json().get("data", [])
        if not page:
            break
        withdrawals.extend(page)
        before = page[-1].get("ts")

    deposits = [row for row in deposits if str(row.get("state")).lower() == "success"]
    withdrawals = [row for row in withdrawals if str(row.get("state")).lower() == "success"]
    return deposits, withdrawals

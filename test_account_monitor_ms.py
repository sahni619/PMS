import sys
import types
import json
import csv
from datetime import datetime

# Stub external dependencies of account_monitor
ccxt = types.ModuleType('ccxt')
sys.modules.setdefault('ccxt', ccxt)

dotenv = types.ModuleType('dotenv')
dotenv.load_dotenv = lambda *a, **k: None
sys.modules.setdefault('dotenv', dotenv)

requests = types.ModuleType('requests')
requests.post = lambda *a, **k: types.SimpleNamespace(raise_for_status=lambda: None)
sys.modules.setdefault('requests', requests)

pytz = types.ModuleType('pytz')
pytz.timezone = lambda name: types.SimpleNamespace()
sys.modules.setdefault('pytz', pytz)

matplotlib = types.ModuleType('matplotlib')
matplotlib.use = lambda *a, **k: None
sys.modules.setdefault('matplotlib', matplotlib)

plt = types.ModuleType('matplotlib.pyplot')
sys.modules.setdefault('matplotlib.pyplot', plt)

# Minimal pandas stub used by account_monitor
pandas = types.ModuleType('pandas')

class Series(list):
    def astype(self, typ):
        return Series([typ(v) for v in self])
    def fillna(self, val):
        return Series([val if v is None else v for v in self])
    def cumprod(self):
        res = []
        total = 1.0
        for v in self:
            total *= v
            res.append(total)
        return Series(res)
    def cumsum(self):
        res = []
        total = 0.0
        for v in self:
            total += v
            res.append(total)
        return Series(res)
    def __add__(self, other):
        return Series([v + other for v in self])
    __radd__ = __add__
    def __sub__(self, other):
        return Series([v - other for v in self])
    def __rsub__(self, other):
        return Series([other - v for v in self])

class DataFrame:
    def __init__(self, data=None, columns=None):
        self.data = data or []
        self.columns = columns or (list(data[0].keys()) if data else [])
    @property
    def empty(self):
        return len(self.data) == 0
    def to_excel(self, writer, index=False, sheet_name=None):
        writer.sheets[sheet_name] = [row.copy() for row in self.data]
    def to_csv(self, path, index=False):
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(self.data)
    def sort_values(self, cols):
        self.data.sort(key=lambda r: tuple(r[c] for c in cols))
        return self
    def drop_duplicates(self, subset=None, keep='last'):
        seen = {}
        for row in self.data:
            key = tuple(row[s] for s in subset) if subset else tuple(row.values())
            seen[key] = row
        self.data = list(seen.values())
        return self
    def __getitem__(self, col):
        return Series([row.get(col) for row in self.data])
    def __setitem__(self, col, series):
        for i, row in enumerate(self.data):
            row[col] = series[i]
        if col not in self.columns:
            self.columns.append(col)
    def copy(self):
        return DataFrame([row.copy() for row in self.data], self.columns[:])
    def groupby(self, key, group_keys=False):
        df = self
        class GroupBy:
            def __init__(self, df, key):
                self.df = df
                self.key = key
            def apply(self, func, include_groups=False):
                groups = {}
                for row in self.df.data:
                    groups.setdefault(row[self.key], []).append(row)
                result = []
                cols = None
                for rows in groups.values():
                    gdf = DataFrame([r.copy() for r in rows], self.df.columns[:])
                    res = func(gdf)
                    result.extend(res.data)
                    cols = res.columns
                return DataFrame(result, cols)
        return GroupBy(df, key)

def read_csv(path):
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            conv = {}
            for k, v in row.items():
                if k in ('date', 'account'):
                    conv[k] = v
                else:
                    conv[k] = float(v)
            rows.append(conv)
        return DataFrame(rows, reader.fieldnames)

def concat(dfs, ignore_index=False):
    rows = []
    cols = []
    for df in dfs:
        rows.extend([r.copy() for r in df.data])
        cols = df.columns
    return DataFrame(rows, cols)

class ExcelWriter:
    def __init__(self, path, engine=None):
        self.path = path
        self.sheets = {}
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        with open(self.path, 'w') as f:
            json.dump(self.sheets, f)

class Types:
    @staticmethod
    def is_datetime64_any_dtype(col):
        return False

pandas.Series = Series
pandas.DataFrame = DataFrame
pandas.read_csv = read_csv
pandas.concat = concat
pandas.ExcelWriter = ExcelWriter
pandas.api = types.SimpleNamespace(types=Types())

sys.modules.setdefault('pandas', pandas)

from account_monitor import _ms, export_excel_snapshot


def test_ms_converts_seconds_to_ms():
    assert _ms(1700000000) == 1700000000 * 1000


def test_ms_accepts_milliseconds():
    assert _ms(1700000000000) == 1700000000000


def test_history_sheet_contains_latest_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv('EXCEL_DIR', str(tmp_path))
    now = datetime(2024, 1, 2, 15, 0)
    accounts = {'acct': {'value': 100.0, 'start_value': 100.0, 'net_flows': 0.0, 'twr_factor': 1.0}}
    path = export_excel_snapshot(now, accounts, {}, {}, {}, 'USD')
    with open(path) as f:
        sheets = json.load(f)
    hist = sheets['History']
    assert any(row['date'] == now.strftime('%Y-%m-%d') for row in hist)

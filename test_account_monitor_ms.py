import sys
import types

# Stub external dependencies of account_monitor
ccxt = types.ModuleType('ccxt')
sys.modules.setdefault('ccxt', ccxt)

dotenv = types.ModuleType('dotenv')
dotenv.load_dotenv = lambda: None
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

pandas = types.ModuleType('pandas')
pandas.DataFrame = type('DataFrame', (), {})
sys.modules.setdefault('pandas', pandas)

from account_monitor import _ms

def test_ms_converts_seconds_to_ms():
    assert _ms(1700000000) == 1700000000 * 1000


def test_ms_accepts_milliseconds():
    assert _ms(1700000000000) == 1700000000000

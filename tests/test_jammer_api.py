import importlib.util
import sys
import types
from pathlib import Path


ROOT = Path(__file__).parents[1]


class FakeBase:
    AMPLITUDE_KEY = "amplitude"
    TX_FREQ_KEY = "tx_freq"


fake_uhd = types.ModuleType("gnuradio.uhd")
fake_uhd.uhd_siggen_base = FakeBase
fake_gnuradio = types.ModuleType("gnuradio")
fake_gnuradio.uhd = fake_uhd
sys.modules.setdefault("gnuradio", fake_gnuradio)
sys.modules.setdefault("gnuradio.uhd", fake_uhd)

spec = importlib.util.spec_from_file_location(
    "jammer_api", ROOT / "ansible/scripts/uhd_siggen_rest.py"
)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


class FakeTopBlock:
    gain_type = "gain"

    def __init__(self):
        self.values = {"amplitude": 0.7, "tx_freq": 3_600_000_000}
        self.gain = 12.0
        self.operations = []

    def lock(self):
        pass

    def unlock(self):
        pass

    def __getitem__(self, key):
        return self.values[key]

    def __setitem__(self, key, value):
        self.operations.append((key, value))
        self.values[key] = value

    def get_gain_or_power(self):
        return self.gain

    def set_gain_or_power(self, value):
        self.operations.append(("gain", value))
        self.gain = value


def test_off_and_on_change_amplitude_and_gain_in_safe_order():
    top_block = FakeTopBlock()
    jammer = api.JammerState(top_block)

    off_status = jammer.off()
    assert top_block.operations == [("amplitude", 0.0), ("gain", 0.0)]
    assert off_status["muted"] is True

    top_block.operations.clear()
    on_status = jammer.on(60)
    assert top_block.operations == [("gain", 60.0), ("amplitude", 0.7)]
    assert on_status["muted"] is False
    assert on_status["gain_or_power"] == 60


def test_off_is_idempotent_and_preserves_restore_amplitude():
    top_block = FakeTopBlock()
    jammer = api.JammerState(top_block)
    jammer.off()
    jammer.off()
    jammer.on(20)

    assert top_block.values["amplitude"] == 0.7
    assert top_block.gain == 20

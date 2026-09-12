import json

import pytest
from pydantic import ValidationError

from arbitrage.config import Settings, load_settings
from arbitrage.execution.live_venues import BinanceTrading
from arbitrage.monitor.controller import MonitorController

KEY, SECRET = "fixture-key-not-real", "fixture-secret-not-real"


def test_yaml_credentials_used_by_adapter_and_runtime_guard(tmp_path, monkeypatch):
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    monkeypatch.setenv("CONFIRM_LIVE_TRADING", "I_UNDERSTAND")
    path = tmp_path / "live.yaml"
    path.write_text(
        f"mode: live\nlive:\n  enabled: true\n"
        f"trading:\n  binance_underlying_per_qty: '1'\n"
        f"binance:\n  api_key: '{KEY}'\n  api_secret: '{SECRET}'\n",
        encoding="utf-8",
    )
    settings = load_settings(path)
    adapter = BinanceTrading(settings, None)
    assert adapter.key == KEY and adapter.secret == SECRET.encode()


def test_config_pair_takes_priority_over_environment(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", "environment-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "environment-secret")
    settings = Settings(binance={"api_key": KEY, "api_secret": SECRET})
    assert settings.binance_credentials() == (KEY, SECRET)


def test_blank_config_uses_environment_pair(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", KEY)
    monkeypatch.setenv("BINANCE_API_SECRET", SECRET)
    assert Settings().binance_credentials() == (KEY, SECRET)


@pytest.mark.parametrize("field", ["api_key", "api_secret"])
def test_partial_config_never_mixes_environment_credentials(monkeypatch, field):
    monkeypatch.setenv("BINANCE_API_KEY", "environment-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "environment-secret")
    with pytest.raises(ValueError):
        Settings(binance={field: KEY}).binance_credentials()


async def test_secrets_absent_from_repr_serialization_and_monitor():
    settings = Settings(binance={"api_key": KEY, "api_secret": SECRET})
    controller = MonitorController(settings)
    try:
        outputs = [
            repr(settings),
            repr(settings.binance),
            settings.model_dump_json(),
            str(settings.model_dump()),
            json.dumps(controller.snapshot()),
        ]
        for output in outputs:
            assert KEY not in output and SECRET not in output
        assert "binance" not in settings.model_dump()
    finally:
        await controller.close()


def test_config_validation_error_does_not_echo_credentials():
    with pytest.raises(ValidationError) as error:
        Settings.model_validate({"binance": {"api_key": KEY, "api_secret": [SECRET]}})
    assert KEY not in str(error.value) and SECRET not in str(error.value)


def test_yaml_error_does_not_echo_secret_line(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text(f'binance:\n  api_secret: "{SECRET}" broken\n', encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid YAML") as error:
        load_settings(path)
    assert SECRET not in str(error.value) and error.value.__cause__ is None

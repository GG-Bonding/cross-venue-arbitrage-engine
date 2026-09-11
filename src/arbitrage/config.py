import os
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from arbitrage.domain.enums import EntryMode

PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
FiniteDecimal = Annotated[Decimal, Field(allow_inf_nan=False, max_digits=18, decimal_places=8)]


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_default=True, frozen=True)


class ConfirmationConfig(ConfigModel):
    min_ticks: int = Field(default=3, ge=2)
    min_duration_ms: int = Field(default=200, ge=0)


class EntryConfig(ConfigModel):
    direction_mode: EntryMode = EntryMode.BOTH
    threshold: FiniteDecimal = Decimal("4.20")
    confirmation: ConfirmationConfig = Field(default_factory=ConfirmationConfig)


class MakerConfig(ConfigModel):
    cancel_threshold: FiniteDecimal = Decimal("4.00")
    cancel_confirm_ms: int = Field(default=50, ge=1)
    max_pending_ms: int = Field(default=2000, ge=1)
    paper_cancel_latency_ms: int = Field(default=20, ge=1)


class MarketConfig(ConfigModel):
    max_quote_age_ms: int = Field(default=300, ge=1)
    max_quote_skew_ms: int = Field(default=200, ge=0)
    mt5_poll_ms: int = Field(default=20, ge=1)
    watchdog_ms: int = Field(default=20, ge=1)
    stream_timeout_ms: int = Field(default=5000, ge=1)
    http_timeout_ms: int = Field(default=10000, ge=1)
    queue_size: int = Field(default=1000, ge=1)
    binance_rest_url: str = "https://fapi.binance.com"
    binance_ws_url: str = "wss://fstream.binance.com/public/ws"
    binance_proxy_url: str | None = Field(default=None, pattern=r"^https?://")


class SymbolConfig(ConfigModel):
    binance: str = Field(default="XAUUSDT", pattern=r"^[A-Z0-9_]+$")
    mt5: str = Field(default="XAUUSD", min_length=1)


class MT5Config(ConfigModel):
    terminal_path: str | None = None
    initialize_timeout_ms: int = Field(default=10000, ge=1)
    quote_startup_timeout_ms: int = Field(default=5000, ge=1)
    # Broker raw tick time minus UTC. Explicitly configured, never learned from quote age.
    tick_time_offset_minutes: int = Field(default=0, ge=-840, le=840)


class TradingConfig(ConfigModel):
    binance_qty: PositiveDecimal = Decimal("1")
    # No inferred contract multiplier: optional until verified against contract documentation.
    binance_underlying_per_qty: PositiveDecimal | None = None


class DatabaseConfig(ConfigModel):
    path: Path = Path("data/arbitrage.db")
    quote_sample_ms: int = Field(default=1000, ge=1)


class Settings(ConfigModel):
    mode: Literal["paper", "live"] = "paper"
    symbol: SymbolConfig = Field(default_factory=SymbolConfig)
    entry: EntryConfig = Field(default_factory=EntryConfig)
    maker: MakerConfig = Field(default_factory=MakerConfig)
    market: MarketConfig = Field(default_factory=MarketConfig)
    mt5: MT5Config = Field(default_factory=MT5Config)
    trading: TradingConfig = Field(default_factory=TradingConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    @model_validator(mode="after")
    def validate_hysteresis(self) -> Self:
        if self.maker.cancel_threshold > self.entry.threshold:
            raise ValueError("maker.cancel_threshold must not exceed entry.threshold")
        return self

    def require_paper(self) -> None:
        if self.mode == "live":
            if os.environ.get("CONFIRM_LIVE_TRADING") != "I_UNDERSTAND":
                raise ValueError("Live mode requires CONFIRM_LIVE_TRADING=I_UNDERSTAND")
            raise ValueError("Phase 1 does not implement live trading; use TRADING_MODE=paper")


def load_settings(path: Path) -> Settings:
    # Preserve YAML decimal scalars instead of converting through binary floats.
    class DecimalLoader(yaml.SafeLoader):
        pass

    DecimalLoader.add_constructor(
        "tag:yaml.org,2002:float", lambda loader, node: Decimal(loader.construct_scalar(node))
    )
    with path.open(encoding="utf-8") as file:
        data = yaml.load(file, Loader=DecimalLoader) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Configuration must be a mapping path={path}")
    if "TRADING_MODE" in os.environ:
        data["mode"] = os.environ["TRADING_MODE"]
    settings = Settings.model_validate(data)
    settings.require_paper()
    return settings

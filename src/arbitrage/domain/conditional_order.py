from dataclasses import dataclass
from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from arbitrage.config import ConfigModel, FiniteDecimal, PositiveDecimal
from arbitrage.domain.enums import Direction


class ConditionalRequest(ConfigModel):
    request_id: str
    direction: Direction
    entry_threshold: FiniteDecimal
    cancel_threshold: FiniteDecimal
    quantity: PositiveDecimal
    repeat: bool = Field(default=False, strict=True)
    exit_threshold: FiniteDecimal | None = None
    min_net_profit: FiniteDecimal | None = Field(default=None, ge=0)

    @field_validator("request_id")
    @classmethod
    def canonical_id(cls, value: str) -> str:
        return str(UUID(value))

    @model_validator(mode="after")
    def valid_thresholds(self) -> Self:
        if self.cancel_threshold > self.entry_threshold:
            raise ValueError("撤单价差不能高于入场价差")
        return self


@dataclass
class ConditionalOrder:
    request_id: str
    direction: Direction
    entry_threshold: Decimal
    cancel_threshold: Decimal
    quantity: Decimal
    repeat: bool
    queue_seq: int
    created_at_ms: int
    updated_at_ms: int
    state: str = "WAITING"
    execution_count: int = 0
    execution_order_id: str | None = None
    cancel_requested: bool = False
    last_result: str | None = None
    exit_threshold: Decimal | None = None
    close_requested: bool = False
    close_reason: str | None = None
    min_net_profit: Decimal | None = None

    @classmethod
    def from_payload(cls, payload: dict) -> Self:
        data = dict(payload)
        data["direction"] = Direction(data["direction"])
        for name in ("entry_threshold", "cancel_threshold", "quantity"):
            data[name] = Decimal(data[name])
        if data.get("exit_threshold") is not None:
            data["exit_threshold"] = Decimal(data["exit_threshold"])
        if data.get("min_net_profit") is not None:
            data["min_net_profit"] = Decimal(data["min_net_profit"])
        return cls(**data)

    def request_fields(self) -> dict:
        return {name: getattr(self, name) for name in ConditionalRequest.model_fields}

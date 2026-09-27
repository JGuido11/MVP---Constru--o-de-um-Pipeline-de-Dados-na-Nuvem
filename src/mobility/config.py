"""Validated identifiers and month contracts shared by the processing services."""
from dataclasses import dataclass
from datetime import date
import re


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def source_month(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError("source_month must be YYYY-MM")
    date.fromisoformat(value + "-01")
    return value


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"
    prefix: str = "mobility"

    def __post_init__(self):
        identifier(self.catalog)
        identifier(self.prefix)

    def schema(self, layer: str) -> str:
        if layer not in {"bronze", "silver", "gold", "ops"}:
            raise ValueError(f"Unknown layer: {layer}")
        return f"{self.catalog}.{self.prefix}_{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{identifier(name)}"

    @property
    def volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.prefix}_bronze/landing"

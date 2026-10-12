"""Load pipeline settings from config/pipeline.yaml with environment-variable overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"

# env var -> (section, key, parser)
_ENV_OVERRIDES = {
    "DBPIPE_MONTHS": ("dataset", "months", lambda v: [m.strip() for m in v.split(",") if m.strip()]),
    "DBPIPE_STORAGE_FORMAT": ("storage", "format", str),
    "DBPIPE_LAKEHOUSE_PATH": ("paths", "lakehouse", str),
    "DBPIPE_LANDING_PATH": ("paths", "landing", str),
    "DBPIPE_ARTIFACTS_PATH": ("paths", "artifacts", str),
    "DBPIPE_DOCS_PATH": ("paths", "docs", str),
    "DBPIPE_SPARK_MASTER": ("spark", "master", str),
    "DBPIPE_DRIVER_MEMORY": ("spark", "driver_memory", str),
    "DBPIPE_TEST_DAYS": ("ml", "test_days", int),
    "DBPIPE_WARMUP_DAYS": ("ml", "warmup_days", int),
}


@dataclass
class Settings:
    raw: dict[str, Any] = field(default_factory=dict)

    # --- convenience accessors -------------------------------------------------
    def section(self, name: str) -> dict[str, Any]:
        return self.raw.get(name, {})

    @property
    def months(self) -> list[str]:
        return list(self.raw["dataset"]["months"])

    @property
    def hf_repo_id(self) -> str:
        return self.raw["dataset"]["hf_repo_id"]

    @property
    def storage_format(self) -> str:
        fmt = self.raw["storage"]["format"].lower()
        if fmt not in {"delta", "parquet"}:
            raise ValueError(f"storage.format must be 'delta' or 'parquet', got {fmt!r}")
        return fmt

    def path(self, key: str) -> Path:
        """Absolute path for a key in the `paths` section (relative paths = project root)."""
        p = Path(self.raw["paths"][key])
        return p if p.is_absolute() else PROJECT_ROOT / p


def load_settings(config_path: str | Path | None = None) -> Settings:
    path = Path(config_path or os.environ.get("DBPIPE_CONFIG", DEFAULT_CONFIG))
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    for env_var, (section, key, parse) in _ENV_OVERRIDES.items():
        value = os.environ.get(env_var)
        if value:
            raw.setdefault(section, {})[key] = parse(value)

    settings = Settings(raw=raw)
    _validate(settings)
    return settings


def _validate(settings: Settings) -> None:
    for month in settings.months:
        parts = month.split("-")
        if len(parts) != 2 or len(parts[0]) != 4 or not (1 <= int(parts[1]) <= 12):
            raise ValueError(f"Invalid month {month!r}; expected format YYYY-MM")
    _ = settings.storage_format  # raises if invalid

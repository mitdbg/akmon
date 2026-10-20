"""Cost-table loader + estimator for agent sessions.

The default table embeds Anthropic public list prices (USD per 1M tokens)
for the model lineup current as of mid-2026. Deployments may override
per-model entries via ``modules_config.agent_sessions.cost_table_path``;
the override merges into the default table (per-model values win).

Pricing source: docs.anthropic.com/en/docs/about-claude/models — list
prices for opus-4-7, sonnet-4-6, haiku-4-5. Update the embedded table
when prices change; downstream nodes carry ``cost_table_version`` (a
hash of the resolved table) so re-costing is deterministic.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from okg.substrate import fastyaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelPrices:
    input_per_1m: float
    output_per_1m: float
    cache_read_per_1m: float = 0.0
    cache_write_per_1m: float = 0.0


# Embedded defaults — Anthropic list prices.
# Update these when prices change; the cost_table_version attr on each
# session captures which table was used so re-costing is possible.
_DEFAULT_PRICES: dict[str, ModelPrices] = {
    "claude-opus-4-7": ModelPrices(
        input_per_1m=15.00, output_per_1m=75.00,
        cache_read_per_1m=1.50, cache_write_per_1m=18.75,
    ),
    "claude-sonnet-4-6": ModelPrices(
        input_per_1m=3.00, output_per_1m=15.00,
        cache_read_per_1m=0.30, cache_write_per_1m=3.75,
    ),
    "claude-haiku-4-5": ModelPrices(
        input_per_1m=1.00, output_per_1m=5.00,
        cache_read_per_1m=0.10, cache_write_per_1m=1.25,
    ),
}

_UNKNOWN_MODELS_LOGGED: set[str] = set()


@dataclass
class CostTable:
    prices: dict[str, ModelPrices] = field(default_factory=lambda: dict(_DEFAULT_PRICES))

    def lookup(self, model_name: str | None) -> ModelPrices | None:
        if not model_name:
            return None
        # Try exact match, then strip version suffixes commonly seen
        # (e.g., "claude-opus-4-7-20260101" -> "claude-opus-4-7").
        if model_name in self.prices:
            return self.prices[model_name]
        for key in self.prices:
            if model_name.startswith(key):
                return self.prices[key]
        if model_name not in _UNKNOWN_MODELS_LOGGED:
            logger.info("No cost table entry for model %r — cost_usd_estimate will be null.", model_name)
            _UNKNOWN_MODELS_LOGGED.add(model_name)
        return None

    def version(self) -> str:
        """A short content hash so we can detect drift over time."""
        payload = json.dumps(
            {k: vars(v) for k, v in sorted(self.prices.items())},
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


def load_cost_table(path: str | Path | None = None) -> CostTable:
    """Build a CostTable. If ``path`` is given, merge its entries into the defaults."""
    table = CostTable()
    if not path:
        return table
    p = Path(path)
    if not p.is_file():
        logger.warning("cost_table_path %s does not exist; using defaults", p)
        return table
    try:
        raw = fastyaml.safe_load(p.read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        logger.warning("Could not load cost table %s: %s; using defaults", p, e)
        return table
    for name, entry in raw.get("models", {}).items():
        if not isinstance(entry, dict):
            continue
        try:
            table.prices[name] = ModelPrices(
                input_per_1m=float(entry["input_per_1m"]),
                output_per_1m=float(entry["output_per_1m"]),
                cache_read_per_1m=float(entry.get("cache_read_per_1m", 0.0)),
                cache_write_per_1m=float(entry.get("cache_write_per_1m", 0.0)),
            )
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed cost entry for %s: %s", name, e)
    return table


def compute_cost_usd(
    model_name: str | None,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    cache_creation_tokens: int,
    table: CostTable,
) -> tuple[float | None, str | None]:
    """Compute USD estimate for a token bundle. Returns (cost, table_version) or (None, None)."""
    prices = table.lookup(model_name)
    if prices is None:
        return None, None
    cost = (
        input_tokens / 1_000_000 * prices.input_per_1m
        + output_tokens / 1_000_000 * prices.output_per_1m
        + cache_read_tokens / 1_000_000 * prices.cache_read_per_1m
        + cache_creation_tokens / 1_000_000 * prices.cache_write_per_1m
    )
    return round(cost, 6), table.version()

"""Shipped enabled runtimes must be able to pass their boot budget check."""

from __future__ import annotations

from pathlib import Path
import re

import pytest
import yaml

from okg.substrate.db import concurrency_budget
from okg.substrate.runtime.config import RuntimeConfig


REPO_ROOT = Path(__file__).resolve().parents[2]
ROLE_SPLIT_SQL = (
    REPO_ROOT / "src" / "okg" / "substrate" / "runtime" / "sql" / "role_split.sql"
)
ROLE_LIMIT_MATCH = re.search(
    r"ALTER ROLE okg_substrate CONNECTION LIMIT (\d+);",
    ROLE_SPLIT_SQL.read_text(encoding="utf-8"),
)
assert ROLE_LIMIT_MATCH is not None, "okg_substrate role limit is not declared"
OKG_SUBSTRATE_ROLE_CONNECTION_LIMIT = int(ROLE_LIMIT_MATCH.group(1))
RUNTIME_TEMPLATES = tuple(
    sorted(
        path
        for root in (REPO_ROOT / "bench", REPO_ROOT / "src" / "okg" / "bench")
        for path in root.rglob("deployment.yaml.j2")
    )
)
assert RUNTIME_TEMPLATES, "no shipped deployment templates were discovered"


def _runtime_block_from_template(path: Path) -> dict[str, object]:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    start = lines.index("runtime:\n")
    block = [lines[start]]
    for line in lines[start + 1 :]:
        if line.strip() and not line.startswith((" ", "\t", "#")):
            break
        if not line.lstrip().startswith(("{%", "{#")):
            block.append(line)
    parseable = re.sub(r"{{.*?}}", "1", "".join(block))
    return yaml.safe_load(parseable)["runtime"]


def _enabled_runtime_blocks() -> list[tuple[str, dict[str, object]]]:
    blocks: list[tuple[str, dict[str, object]]] = []
    for path in sorted((REPO_ROOT / "deployments").glob("*/deployment.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        runtime = document.get("runtime") or {}
        if runtime.get("enabled") is True:
            blocks.append((str(path.relative_to(REPO_ROOT)), runtime))

    for path in sorted((REPO_ROOT / "ops" / "environments").glob("*/environment.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        runtime = document.get("runtime") or {}
        config = runtime.get("config") or {}
        if config.get("enabled") is True:
            blocks.append((str(path.relative_to(REPO_ROOT)), config))

    for path in RUNTIME_TEMPLATES:
        runtime = _runtime_block_from_template(path)
        if runtime.get("enabled") is True:
            blocks.append((str(path.relative_to(REPO_ROOT)), runtime))
    return blocks


@pytest.mark.parametrize(("manifest", "runtime_block"), _enabled_runtime_blocks())
def test_enabled_runtime_manifest_has_a_feasible_connection_budget(
    manifest: str,
    runtime_block: dict[str, object],
) -> None:
    concurrency_budget.clear_overrides()
    try:
        RuntimeConfig.from_manifest_block(runtime_block).apply_to_concurrency_budget()
        substrate_role_demand = sum(
            concurrency_budget.width(term)
            for term in (
                "db.shared_pool",
                "chronos.schema_index",
                "chronos.interval_gc",
            )
        )
        assert substrate_role_demand <= OKG_SUBSTRATE_ROLE_CONNECTION_LIMIT, (
            f"{manifest} declares {substrate_role_demand} connections that may use "
            f"okg_substrate, above its role limit "
            f"{OKG_SUBSTRATE_ROLE_CONNECTION_LIMIT}"
        )
    except concurrency_budget.BudgetError as exc:
        pytest.fail(f"{manifest} cannot boot with its declared runtime budget: {exc}")
    finally:
        concurrency_budget.clear_overrides()

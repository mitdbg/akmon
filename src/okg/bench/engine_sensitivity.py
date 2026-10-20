"""Planner for differential-engine sensitivity benchmark matrices.

The planner is intentionally pure: it reads a small YAML spec and emits
ordinary commit-replay experiment dictionaries that ``bench/orchestrator.py``
can validate, render, and run. Keeping planning separate from execution lets
developers inspect the sensitivity matrix before provisioning Postgres cells.
"""
from __future__ import annotations

import copy
import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


SCHEMA = "okg.bench.engine_sensitivity.v1"
PLAN_SCHEMA = "okg.bench.engine_sensitivity.plan.v1"
VALID_MODES = {"one_factor_at_a_time", "cartesian"}
WORKLOAD_AXES = ("repo", "source_set", "replay")
ENGINE_AXES = (
    "enrichers",
    "views",
    "chronos_continuation_pct",
    "embedding",
)
UNAVAILABLE_ENGINE_AXES = {
    "projection_batch": (
        "projection_batch is recognized by BenchConfig but is not rendered "
        "into bench deployment.yaml yet; measuring it would be a no-op"
    ),
    "branching": (
        "branching is measured by the explicit Chronos branching report, not "
        "as a linear commit-replay deployment config delta"
    ),
}
DBOS_EXPERIMENT_ID_MAX = 26

SOURCE_SET_TAGS = {
    "files-only": "fo",
    "history-only": "ho",
    "files+history": "fh",
    "files+code": "fc",
    "all": "all",
    "tier1": "t1",
    "tier1-enriched": "t1e",
}
ENRICHER_TAGS = {
    "default": "endef",
    "none": "en0",
    "declarative": "endecl",
    "code": "encode",
    "all": "enall",
}


@dataclass(frozen=True)
class SensitivityCell:
    id: str
    axis: str
    axis_value: Any
    experiment: dict[str, Any]
    baseline: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "axis": self.axis,
            "axis_value": self.axis_value,
            "baseline": self.baseline,
            "experiment": self.experiment,
            "run_command": f"python bench/orchestrator.py sensitivity --run --cell {self.id}",
        }


def load_sensitivity_spec(path: Path | str) -> dict[str, Any]:
    """Load a sensitivity spec YAML file and validate top-level shape."""
    p = Path(path)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{p}: sensitivity spec must be a mapping")
    return _validate_spec(raw, source=str(p))


def build_sensitivity_plan(path: Path | str) -> dict[str, Any]:
    return build_sensitivity_plan_from_spec(load_sensitivity_spec(path), source=str(path))


def build_sensitivity_plan_from_spec(
    spec: Mapping[str, Any],
    *,
    source: str = "<memory>",
) -> dict[str, Any]:
    """Expand a sensitivity spec into runnable commit-replay cells."""
    doc = _validate_spec(dict(spec), source=source)
    base_id = doc["id"]
    mode = doc["mode"]
    baseline_exp = _baseline_experiment(doc["baseline"])
    baseline_cell = SensitivityCell(
        id=f"{base_id}-base",
        axis="baseline",
        axis_value="baseline",
        experiment=copy.deepcopy(baseline_exp),
        baseline=True,
    )
    cells = (
        _one_factor_cells(doc, baseline_cell)
        if mode == "one_factor_at_a_time"
        else _cartesian_cells(doc, baseline_cell)
    )
    _validate_cell_ids(cells)
    reports = _report_commands(doc, baseline_cell)
    return {
        "schema": PLAN_SCHEMA,
        "spec": {"id": base_id, "mode": mode, "source": source},
        "baseline_cell": baseline_cell.id,
        "cell_count": len(cells),
        "cells": [cell.to_dict() for cell in cells],
        "reports": reports,
    }


def sensitivity_markdown(plan: Mapping[str, Any]) -> str:
    """Render a short human-readable plan summary."""
    spec = plan.get("spec", {})
    lines = [
        f"# Engine sensitivity plan: {spec.get('id')}",
        "",
        f"- mode: `{spec.get('mode')}`",
        f"- cells: `{plan.get('cell_count')}`",
        f"- baseline: `{plan.get('baseline_cell')}`",
        "",
        "| cell | axis | value | source_set | replay | engine config |",
        "|---|---|---|---|---|---|",
    ]
    for cell in plan.get("cells", []):
        exp = cell["experiment"]
        replay = _brief_replay(exp["replay"])
        cfg = exp.get("config") or {}
        lines.append(
            "| `{id}` | `{axis}` | `{axis_value}` | `{source}` | `{replay}` | "
            "`{config}` |".format(
                id=cell["id"],
                axis=cell["axis"],
                axis_value=_jsonish(cell["axis_value"]),
                source=exp["source_set"],
                replay=replay,
                config=_jsonish(cfg),
            )
        )
    reports = plan.get("reports") or {}
    if reports:
        lines += ["", "## Side reports", ""]
        for name, report in sorted(reports.items()):
            lines.append(f"- `{name}`: `{report['command']}`")
    return "\n".join(lines) + "\n"


def _validate_spec(doc: dict[str, Any], *, source: str) -> dict[str, Any]:
    schema = doc.get("schema")
    if schema != SCHEMA:
        raise ValueError(f"{source}: schema must be {SCHEMA!r}, got {schema!r}")
    base_id = doc.get("id")
    if not isinstance(base_id, str) or not base_id:
        raise ValueError(f"{source}: id must be a non-empty string")
    if len(f"{base_id}-base") > DBOS_EXPERIMENT_ID_MAX:
        raise ValueError(
            f"{source}: id {base_id!r} leaves no room for DBOS-safe cell tags"
        )
    mode = doc.get("mode", "one_factor_at_a_time")
    if mode not in VALID_MODES:
        raise ValueError(f"{source}: mode must be one of {sorted(VALID_MODES)}")
    doc["mode"] = mode
    baseline = doc.get("baseline")
    if not isinstance(baseline, dict):
        raise ValueError(f"{source}: baseline must be a mapping")
    _baseline_experiment(baseline)
    axes = doc.get("axes", {})
    if axes is None:
        axes = {}
    if not isinstance(axes, dict):
        raise ValueError(f"{source}: axes must be a mapping")
    workload = _axis_group(axes, "workload", WORKLOAD_AXES, source=source)
    engine = _axis_group(axes, "engine", ENGINE_AXES, source=source)
    for axis in axes.get("engine", {}) if isinstance(axes.get("engine"), dict) else ():
        if axis in UNAVAILABLE_ENGINE_AXES:
            raise ValueError(
                f"{source}: engine axis {axis!r} is unavailable: "
                f"{UNAVAILABLE_ENGINE_AXES[axis]}"
            )
    doc["axes"] = {"workload": workload, "engine": engine}
    reports = doc.get("reports", {})
    if reports is None:
        reports = {}
    if not isinstance(reports, dict):
        raise ValueError(f"{source}: reports must be a mapping")
    doc["reports"] = reports
    return doc


def _axis_group(
    axes: Mapping[str, Any],
    group: str,
    valid: tuple[str, ...],
    *,
    source: str,
) -> dict[str, list[Any]]:
    raw = axes.get(group, {})
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{source}: axes.{group} must be a mapping")
    out: dict[str, list[Any]] = {}
    for axis, values in raw.items():
        if axis not in valid:
            unavailable = UNAVAILABLE_ENGINE_AXES.get(axis)
            if unavailable:
                raise ValueError(
                    f"{source}: {group} axis {axis!r} is unavailable: {unavailable}"
                )
            raise ValueError(
                f"{source}: unknown {group} axis {axis!r}; expected {list(valid)}"
            )
        if not isinstance(values, list) or not values:
            raise ValueError(f"{source}: axes.{group}.{axis} must be a non-empty list")
        out[axis] = [copy.deepcopy(v) for v in values]
    return out


def _baseline_experiment(baseline: Mapping[str, Any]) -> dict[str, Any]:
    repo = baseline.get("repo")
    replay = baseline.get("replay")
    if not isinstance(repo, dict) or not repo.get("name") or not repo.get("url"):
        raise ValueError("baseline.repo must include name and url")
    if not isinstance(replay, dict) or not replay.get("strategy"):
        raise ValueError("baseline.replay must include strategy")
    source_set = baseline.get("source_set")
    if not isinstance(source_set, str) or not source_set:
        raise ValueError("baseline.source_set must be a non-empty string")
    config = baseline.get("config", {})
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ValueError("baseline.config must be a mapping when present")
    return {
        "id": "placeholder",
        "repo": {"name": str(repo["name"]), "url": str(repo["url"])},
        "source_set": source_set,
        "replay": copy.deepcopy(dict(replay)),
        "config": copy.deepcopy(dict(config)),
    }


def _one_factor_cells(
    doc: Mapping[str, Any],
    baseline: SensitivityCell,
) -> list[SensitivityCell]:
    cells = [_with_id(baseline, baseline.id)]
    for group in ("workload", "engine"):
        for axis, values in doc["axes"][group].items():
            for value in values:
                if _same_axis_value(baseline.experiment, group, axis, value):
                    continue
                exp = copy.deepcopy(baseline.experiment)
                _apply_axis(exp, group, axis, value)
                tag = _axis_tag(group, axis, value)
                cells.append(
                    SensitivityCell(
                        id=f"{doc['id']}-{tag}",
                        axis=f"{group}.{axis}",
                        axis_value=copy.deepcopy(value),
                        experiment=exp,
                    )
                )
    return _dedupe_cells(cells)


def _cartesian_cells(
    doc: Mapping[str, Any],
    baseline: SensitivityCell,
) -> list[SensitivityCell]:
    assignments: list[tuple[str, str, Any]] = []
    for group in ("workload", "engine"):
        for axis, values in doc["axes"][group].items():
            assignments.append((group, axis, values))
    if not assignments:
        return [_with_id(baseline, baseline.id)]
    cells: list[SensitivityCell] = [_with_id(baseline, baseline.id)]
    axes = [(g, a) for g, a, _values in assignments]
    for combo in itertools.product(*(values for _g, _a, values in assignments)):
        exp = copy.deepcopy(baseline.experiment)
        tags = []
        axis_value: dict[str, Any] = {}
        for (group, axis), value in zip(axes, combo, strict=True):
            _apply_axis(exp, group, axis, value)
            tags.append(_axis_tag(group, axis, value))
            axis_value[f"{group}.{axis}"] = copy.deepcopy(value)
        cells.append(
            SensitivityCell(
                id=f"{doc['id']}-" + "-".join(tags),
                axis="cartesian",
                axis_value=axis_value,
                experiment=exp,
            )
        )
    return _dedupe_cells(cells)


def _apply_axis(exp: dict[str, Any], group: str, axis: str, value: Any) -> None:
    if group == "workload":
        if axis == "repo":
            if not isinstance(value, dict) or not value.get("name") or not value.get("url"):
                raise ValueError("repo axis values must include name and url")
            exp["repo"] = {"name": str(value["name"]), "url": str(value["url"])}
        elif axis == "source_set":
            exp["source_set"] = str(value)
        elif axis == "replay":
            if not isinstance(value, dict) or not value.get("strategy"):
                raise ValueError("replay axis values must include strategy")
            exp["replay"] = copy.deepcopy(dict(value))
        else:  # pragma: no cover - guarded by validation
            raise ValueError(f"unknown workload axis {axis!r}")
        return
    cfg = dict(exp.get("config") or {})
    if axis == "enrichers":
        cfg["enrichers"] = str(value)
    elif axis in {"views", "embedding"}:
        if not isinstance(value, bool):
            raise ValueError(f"{axis} axis values must be booleans")
        cfg[axis] = value
    elif axis == "chronos_continuation_pct":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("chronos_continuation_pct values must be integers")
        cfg[axis] = value
    else:  # pragma: no cover - guarded by validation
        raise ValueError(f"unknown engine axis {axis!r}")
    exp["config"] = cfg


def _same_axis_value(exp: Mapping[str, Any], group: str, axis: str, value: Any) -> bool:
    if group == "workload":
        if axis == "repo":
            return _jsonish(exp["repo"]) == _jsonish(value)
        if axis == "source_set":
            return exp["source_set"] == value
        if axis == "replay":
            return _jsonish(exp["replay"]) == _jsonish(value)
    cfg = exp.get("config") or {}
    return cfg.get(axis) == value


def _axis_tag(group: str, axis: str, value: Any) -> str:
    if group == "workload":
        if axis == "repo":
            if not isinstance(value, Mapping) or not value.get("name"):
                raise ValueError("repo axis values must include name")
            return "repo-" + _short_token(str(value["name"]))
        if axis == "source_set":
            return "src-" + SOURCE_SET_TAGS.get(str(value), _short_token(str(value)))
        if axis == "replay":
            return "r-" + _replay_tag(value)
    if axis == "enrichers":
        return ENRICHER_TAGS.get(str(value), _short_token(str(value)))
    if axis == "views":
        return "vw1" if value else "vw0"
    if axis == "embedding":
        return "emb1" if value else "emb0"
    if axis == "chronos_continuation_pct":
        return f"chr{value}"
    raise ValueError(f"cannot tag axis {group}.{axis}")


def _replay_tag(value: Any) -> str:
    if not isinstance(value, Mapping):
        raise ValueError("replay axis values must be mappings")
    strategy = value.get("strategy")
    if strategy == "first_n":
        return f"fn{value.get('n')}"
    if strategy == "head_only":
        return f"hd{value.get('last_n')}"
    if strategy == "tail_window":
        return f"tw{value.get('last_n')}"
    if strategy == "full_history":
        return "full"
    if strategy == "hybrid_head_then_stride":
        return f"hy{value.get('head_n')}-{value.get('total')}"
    return _short_token(_brief_replay(value))


def _report_commands(
    doc: Mapping[str, Any],
    baseline: SensitivityCell,
) -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    branching = (doc.get("reports") or {}).get("branching")
    if isinstance(branching, Mapping) and branching.get("enabled", False):
        n_commits = int(branching.get("n_commits", 5))
        nodes_per_commit = int(branching.get("nodes_per_commit", 25))
        reports["branching"] = {
            "cell": baseline.id,
            "n_commits": n_commits,
            "nodes_per_commit": nodes_per_commit,
            "command": (
                "python bench/orchestrator.py sensitivity "
                f"--run-branching-report --cell {baseline.id} "
                f"--branching-n-commits {n_commits} "
                f"--branching-nodes-per-commit {nodes_per_commit}"
            ),
        }
    return reports


def _with_id(cell: SensitivityCell, cell_id: str) -> SensitivityCell:
    exp = copy.deepcopy(cell.experiment)
    exp["id"] = cell_id
    return SensitivityCell(
        id=cell_id,
        axis=cell.axis,
        axis_value=cell.axis_value,
        experiment=exp,
        baseline=cell.baseline,
    )


def _dedupe_cells(cells: list[SensitivityCell]) -> list[SensitivityCell]:
    out: list[SensitivityCell] = []
    seen: set[str] = set()
    fingerprints: set[str] = set()
    for cell in cells:
        c = _with_id(cell, cell.id)
        fingerprint = _experiment_fingerprint(c.experiment)
        if c.id in seen:
            raise ValueError(f"duplicate sensitivity cell id {c.id!r}")
        if fingerprint in fingerprints:
            continue
        seen.add(c.id)
        fingerprints.add(fingerprint)
        out.append(c)
    return out


def _validate_cell_ids(cells: list[SensitivityCell]) -> None:
    for cell in cells:
        if len(cell.id) > DBOS_EXPERIMENT_ID_MAX:
            raise ValueError(
                f"sensitivity cell id {cell.id!r} is {len(cell.id)} chars; "
                f"must be <= {DBOS_EXPERIMENT_ID_MAX} for DBOS app-name cap"
            )


def _experiment_fingerprint(exp: Mapping[str, Any]) -> str:
    return _jsonish(
        {
            "repo": exp.get("repo"),
            "source_set": exp.get("source_set"),
            "replay": exp.get("replay"),
            "config": exp.get("config") or {},
        }
    )


def _brief_replay(replay: Mapping[str, Any]) -> str:
    strategy = replay.get("strategy")
    if strategy == "first_n":
        return f"first_n(n={replay.get('n')})"
    if strategy == "head_only":
        return f"head_only(last_n={replay.get('last_n')})"
    if strategy == "tail_window":
        return f"tail_window(last_n={replay.get('last_n')})"
    if strategy == "hybrid_head_then_stride":
        return (
            "hybrid_head_then_stride("
            f"head_n={replay.get('head_n')},total={replay.get('total')})"
        )
    return str(strategy)


def _short_token(value: str) -> str:
    return "".join(c for c in value.lower() if c.isalnum())[:8] or "x"


def _jsonish(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))

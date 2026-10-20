"""Shared helpers for the Engram run-artifact source family.

Per `add-engram-run-artifact-sources` (Layer 3A). Centralizes:

  - canonical node id formation (`engram_<kind>:<run_id>:…`),
  - per-run manifest reading (problem_name + source_generation_id),
  - shared module-loaded probe (`reasoning` is hard-dep, `scholarly`
    / `bookkeeping` are soft-deps with stats-counter suppression),
  - configure-time validation (`runs_dir` resolves, `reasoning` is
    in `loaded_modules`).

The probe takes a `loaded_modules` set rather than reaching into
a global catalog object, so the sources are testable with hermetic
fixtures and don't depend on the runtime catalog at construction
time. Deployments pass the set via source_registry.yaml's `extras`
on the source entry; in tests the set is constructed directly.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from okg.substrate.errors import SubstrateError


__all__ = [
    "EngramSourceConfigError",
    "RunManifest",
    "content_hash_text",
    "content_hash_json",
    "research_digest_id",
    "experiment_result_id",
    "candidate_solution_id",
    "final_result_id",
    "agent_summary_id",
    "empirical_claim_id",
    "failed_approach_id",
    "project_id_for_run",
    "validate_reasoning_loaded",
    "scholarly_loaded",
    "bookkeeping_loaded",
    "iter_run_dirs",
    "read_run_manifest",
    "parse_journal_frontmatter",
]


# ---------- error class ----------


class EngramSourceConfigError(SubstrateError):
    """Raised at construct-time when a hard configuration constraint
    is violated — most commonly when `reasoning` isn't in the
    deployment's loaded modules (engram_runs's refined subtypes
    rely on `reasoning.Claim` / `Hypothesis` / `Evidence` existing
    in the catalog).
    """
    code = "engram_source_config_error"


# ---------- id helpers ----------


def _safe_slug(value: str) -> str:
    """Sanitize a value for inclusion in a node id. Replaces any
    character outside `[A-Za-z0-9_-]` with `-` so untrusted run
    names don't break the alias keyspace.
    """
    s = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value or "")).strip("-")
    return s or "unknown"


def research_digest_id(run_id: str) -> str:
    return f"engram_digest:{_safe_slug(run_id)}"


def experiment_result_id(run_id: str, agent_id: str, experiment_id: str) -> str:
    return (
        f"engram_exp:{_safe_slug(run_id)}:"
        f"{_safe_slug(agent_id)}:{_safe_slug(experiment_id)}"
    )


def candidate_solution_id(
    run_id: str, agent_id: str, experiment_id: str, language: str,
) -> str:
    return (
        f"engram_cand:{_safe_slug(run_id)}:"
        f"{_safe_slug(agent_id)}:{_safe_slug(experiment_id)}:"
        f"{_safe_slug(language)}"
    )


def final_result_id(run_id: str) -> str:
    return f"engram_final:{_safe_slug(run_id)}"


def agent_summary_id(run_id: str, agent_id: str) -> str:
    return f"engram_summary:{_safe_slug(run_id)}:{_safe_slug(agent_id)}"


def empirical_claim_id(run_id: str, agent_id: str, claim_index: int) -> str:
    return (
        f"engram_claim:{_safe_slug(run_id)}:"
        f"{_safe_slug(agent_id)}:{int(claim_index)}"
    )


def failed_approach_id(
    run_id: str, agent_id: str, approach_index: int,
) -> str:
    return (
        f"engram_failed:{_safe_slug(run_id)}:"
        f"{_safe_slug(agent_id)}:{int(approach_index)}"
    )


def project_id_for_run(run_id: str) -> str:
    """Identity for the bookkeeping.Project an Engram run
    materializes (when bookkeeping is loaded).
    """
    return f"proj:engram:{_safe_slug(run_id)}"


# ---------- content hashes ----------


def content_hash_text(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def content_hash_json(payload: Any) -> str:
    """Stable JSON hash — sort keys so two semantically-equal
    payloads hash identically across runs.
    """
    blob = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------- module-loaded probes ----------


def validate_reasoning_loaded(
    loaded_modules: Iterable[str] | None, *, source_name: str,
) -> None:
    """Raise at configure time when `reasoning` is not in
    `loaded_modules`. Engram_runs's EmpiricalClaim, FailedApproach,
    and ExperimentResult refine reasoning subtypes via LinkML
    `is_a:` — without reasoning in the catalog, the refinement
    closure can't populate and the FK on
    `okg.subtypes.parent_subtype` will fail at apply time.

    Sources accept `loaded_modules=None` to opt out of the check
    (legacy callers that don't pass the set; the test_conditional
    scenarios always pass it). When the check is bypassed, the
    catalog-level FK fires loudly during deployment compose, so
    the failure is preserved — just emitted later than the
    user-friendly configure-time message.
    """
    if loaded_modules is None:
        return
    modules = {str(m).strip() for m in loaded_modules if str(m).strip()}
    if "reasoning" not in modules:
        raise EngramSourceConfigError(
            f"{source_name}: the `reasoning` module SHALL be loaded "
            f"alongside `engram_runs`; engram_runs refines "
            f"reasoning's Claim / Hypothesis / Evidence subtypes "
            f"via LinkML `is_a:`. Without reasoning, the "
            f"refinement closure cannot populate and the catalog "
            f"compile will fail.",
            fix_hint=(
                "Add `- reasoning` to the deployment's "
                "`modules:` list in deployment.yaml (above "
                "`- engram_runs`)."
            ),
            data={
                "source_name": source_name,
                "loaded_modules": sorted(modules),
            },
        )


def scholarly_loaded(loaded_modules: Iterable[str] | None) -> bool:
    """Return True when `scholarly` is in `loaded_modules` (or when
    the caller passed None, meaning the check is opted out). The
    None case mirrors the legacy permissive behavior — if a
    deployment doesn't pass the set, assume scholarly is present
    and let the narrowing trigger reject edges if it isn't.
    """
    if loaded_modules is None:
        return True
    return "scholarly" in {str(m).strip() for m in loaded_modules}


def bookkeeping_loaded(loaded_modules: Iterable[str] | None) -> bool:
    """Same shape as `scholarly_loaded` for the `bookkeeping`
    module.
    """
    if loaded_modules is None:
        return True
    return "bookkeeping" in {str(m).strip() for m in loaded_modules}


# ---------- run-directory walking ----------


def iter_run_dirs(runs_dir: Path) -> list[Path]:
    """Return the sorted list of per-run subdirectories under
    `runs_dir`. A run directory is any direct child of `runs_dir`
    that itself is a directory (the run id is the directory name).
    Files at the top level are ignored.

    Returns an empty list when `runs_dir` doesn't exist; callers
    surface this via stats rather than a raise (a missing or empty
    runs_dir is a benign first-run state).
    """
    if not runs_dir.is_dir():
        return []
    return sorted(p for p in runs_dir.iterdir() if p.is_dir())


# ---------- run manifest reading ----------


@dataclass(frozen=True)
class RunManifest:
    """Per-run metadata sourced from `runs_dir/<run_id>/manifest.json`
    (if present). Carries the problem name and the OKG generation
    the run consumed when producing its context packet.

    Both fields are optional — a run without a manifest still
    ingests; `problem_name` falls back to the journal frontmatter
    parser, and `source_generation_id` is left absent.
    """
    run_id: str
    problem_name: Optional[str] = None
    source_generation_id: Optional[str] = None
    extras: dict[str, Any] = field(default_factory=dict)


def read_run_manifest(run_dir: Path) -> RunManifest:
    """Read `<run_dir>/manifest.json` when present. Returns an
    empty manifest (run_id only) when the file is absent or
    unreadable; the source's own per-file YELLOW finding handles
    explicit corruption upstream.
    """
    run_id = run_dir.name
    path = run_dir / "manifest.json"
    if not path.is_file():
        return RunManifest(run_id=run_id)
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return RunManifest(run_id=run_id)
    if not isinstance(payload, dict):
        return RunManifest(run_id=run_id)
    problem_name = payload.get("problem_name")
    if not isinstance(problem_name, str):
        problem_name = None
    source_gen = payload.get("source_generation_id")
    if not isinstance(source_gen, str):
        source_gen = None
    extras = {
        k: v for k, v in payload.items()
        if k not in {"problem_name", "source_generation_id"}
    }
    return RunManifest(
        run_id=run_id,
        problem_name=problem_name,
        source_generation_id=source_gen,
        extras=extras,
    )


# ---------- journal-frontmatter helpers ----------


_FRONTMATTER_RE = re.compile(
    r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL,
)


def parse_journal_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Parse YAML frontmatter from a `research_journal.md` body.

    Returns `(frontmatter_dict, body_text)`. The journal body
    (sans frontmatter block) is returned even when the
    frontmatter parse fails — the source falls back to using the
    full body as the digest text in that case.
    """
    import yaml

    if not text:
        return ({}, "")
    m = _FRONTMATTER_RE.match(text)
    if not m:
        return ({}, text)
    raw = m.group(1)
    body = text[m.end():]
    try:
        data = fastyaml.safe_load(raw) or {}
    except yaml.YAMLError:
        return ({}, text)
    if not isinstance(data, dict):
        return ({}, text)
    return (data, body)

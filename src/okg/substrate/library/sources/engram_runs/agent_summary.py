"""EngramAgentSummarySource — per-agent summary ingestion + decomposition.

Reads `<runs_dir>/<run_id>/agent_<N>_summary.json` and emits:

  - One `AgentSummary` per file.
  - One `EmpiricalClaim` per entry in `summary.empirical_claims[]`.
    Each carries `statement`, `observed_score_delta`, run_id,
    agent_id, and (when the claim entry references a `paper_key`)
    a `cite_key_ref` so the `EmpiricalClaim builds_on Paper`
    cross-link rule resolves.
  - One `FailedApproach` per entry in `summary.failed_approaches[]`.
    Each carries `statement`, `failure_mode`, run_id, agent_id,
    and (when the entry carries `experiment_id`) a
    `refuted_by_experiment_ref` so the
    `FailedApproach refuted_by ExperimentResult` rule resolves.
  - `surfaces` edges from AgentSummary → EmpiricalClaim and
    AgentSummary → FailedApproach.
  - `produced_during` edge from AgentSummary → ResearchDigest.
  - `cites: scholarly.Paper` edge from AgentSummary when the
    summary's `cite_key_ref` (parsed from a top-level `paper_key`
    or from a `[@key]` mention in `summary_text`) resolves; the
    edge is suppressed when scholarly isn't loaded, with the
    suppressed count recorded in
    `paper_citations_suppressed_count`.

Per `add-engram-run-artifact-sources` (Layer 3A).

YELLOW findings:

  - Malformed agent-summary JSON — YELLOW finding, skip file,
    continue with siblings.
  - Run directory missing agent summaries — increment
    `runs_missing_summary` stat; no finding (it's expected for
    runs that don't have agent-summary files).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)

from . import _common
from ._common import (
    agent_summary_id,
    bookkeeping_loaded,
    content_hash_json,
    empirical_claim_id,
    experiment_result_id,
    failed_approach_id,
    iter_run_dirs,
    read_run_manifest,
    research_digest_id,
    scholarly_loaded,
    validate_reasoning_loaded,
)


__all__ = ["EngramAgentSummarySource"]


_SUMMARY_RE = re.compile(r"^agent_([A-Za-z0-9_-]+)_summary\.json$")
# Matches `[@paper_key]` or `@paper_key` citations in summary_text.
# Conservative: word-boundary keys, no slashes.
_PAPER_KEY_RE = re.compile(r"\[?@([A-Za-z0-9_-]+)\]?")


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_paper_key(key: str) -> str:
    """Map a raw cite key to the canonical `paper:<key>` form the
    scholarly bib_references source emits as `cite_key_ref`."""
    if key.startswith("paper:"):
        return key
    return f"paper:{key}"


@dataclass
class EngramAgentSummarySource:
    """Read per-agent summary JSON + decompose claims / failures."""

    runs_dir: str | Path | None = None
    loaded_modules: Optional[tuple[str, ...]] = None
    target_subtypes: tuple[str, ...] = (
        "agent_summary",
        "empirical_claim",
        "failed_approach",
    )
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    last_run_findings: list[dict[str, Any]] = field(
        default_factory=list, init=False,
    )

    name: str = "engram_agent_summary"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323). EMISSION here is a pure function
    # of bytes-on-disk under the resolved ``runs_dir``: for every run directory
    # the source reads its ``agent_<N>_summary.json`` files (one AgentSummary +
    # decomposed EmpiricalClaim / FailedApproach nodes + edges per file) AND that
    # run's ``manifest.json`` (``problem_name`` and ``source_generation_id`` from
    # the manifest land on emitted node attrs). There is no git head, append
    # offset, or upstream API — just files — so the sound floor is the
    # O(content) ContentHashProbe over the source's OWN per-run enumeration.
    #
    # The content scope is membership-sensitive: an add / edit / delete of any
    # agent-summary file OR of a run's manifest.json busts the token, and the
    # digest counts entries so a delete drops a ``(key, sha)`` pair. It is
    # content-based, so a worktree-checkout mtime bump does NOT bust it. The
    # resolved ``runs_dir`` is folded into the content keys (rooted absolute
    # paths) AND into ``config``, so a runs_dir flip busts the token even when
    # both roots happen to be empty. No ``now()``/clock value reaches an emitted
    # attr — only ``run_id`` lands on ``source_revision`` lineage — so the content
    # hash is a complete superset of the emission-determining content.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not self.runs_dir:
            raise SubstrateError(
                f"{self.name}: runs_dir is required",
                fix_hint=(
                    "Set `runs_dir: <path>` in the deployment's "
                    "source_registry.yaml entry."
                ),
            )
        validate_reasoning_loaded(
            self.loaded_modules, source_name=self.name,
        )
        self.change_probe = self._build_change_probe()

    def _runs_dir(self) -> Path:
        return Path(str(self.runs_dir)).expanduser()

    def _probe_content_items(self) -> list[tuple[str, Path]]:
        """Re-enumerate the CURRENT emission-determining files on disk.

        For each run directory under the resolved ``runs_dir`` this returns
        every top-level ``agent_<N>_summary.json`` (the source's own
        ``_SUMMARY_RE`` membership rule) plus that run's ``manifest.json`` when
        present. Keys are the absolute paths, so a ``runs_dir`` flip changes the
        keyspace and the membership count reflects every add / delete. The probe
        reads live state each call; a deleted file is simply absent next call.
        """
        runs_dir = self._runs_dir()
        items: list[tuple[str, Path]] = []
        for run_dir in iter_run_dirs(runs_dir):
            for entry in sorted(run_dir.iterdir()):
                if not entry.is_file():
                    continue
                if _SUMMARY_RE.match(entry.name):
                    items.append((str(entry), entry))
            manifest_path = run_dir / "manifest.json"
            if manifest_path.is_file():
                items.append((str(manifest_path), manifest_path))
        return items

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        return ContentHashProbe(
            content_items=self._probe_content_items,
            config={
                "runs_dir": str(self._runs_dir()),
                "name": self.name,
                "profile": self.profile,
                "target_subtypes": list(self.target_subtypes),
                # ``loaded_modules`` GATES emission: ``scholarly_loaded`` decides
                # whether ``cite_key_ref`` attrs are emitted (vs. counted as
                # suppressed). A flip of the loaded-module set therefore changes
                # the emitted attrs for the same bytes, so it must bust the token.
                "loaded_modules": (
                    sorted(self.loaded_modules)
                    if self.loaded_modules is not None
                    else None
                ),
            },
            # ``_common`` shapes every emitted id (``agent_summary_id``,
            # ``empirical_claim_id``, ``failed_approach_id``,
            # ``experiment_result_id``, ``research_digest_id``) and the content
            # hash carried on attrs (``content_hash_json``). A normalization
            # change there (e.g. ``_safe_slug``) alters emitted ids without
            # touching the source's own module, so fold it into emit_version to
            # avoid a false-unchanged after that code change.
            emit_targets=[sys.modules[__name__], _common],
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        stats: dict[str, Any] = {
            "runs_seen": 0,
            "runs_missing_summary": 0,
            "summaries_emitted": 0,
            "empirical_claims_emitted": 0,
            "failed_approaches_emitted": 0,
            "surfaces_edges_emitted": 0,
            "produced_during_edges_emitted": 0,
            "refuted_by_refs_stamped": 0,
            "summary_malformed_json": 0,
            "paper_citations_emitted_count": 0,
            "paper_citations_suppressed_count": 0,
        }
        findings: list[dict[str, Any]] = []
        runs_dir = self._runs_dir()
        run_dirs = iter_run_dirs(runs_dir)
        scholarly_on = scholarly_loaded(self.loaded_modules)
        # bookkeeping not directly used here — it's the digest
        # source's job to materialize the Project — but a future
        # extension might tag claims with project provenance.
        _ = bookkeeping_loaded(self.loaded_modules)

        def _facts() -> Iterator[Any]:
            for run_dir in run_dirs:
                stats["runs_seen"] += 1
                this_run_id = run_dir.name
                # Discover all `agent_<N>_summary.json` files in
                # the run directory's top level.
                summary_files: list[tuple[str, Path]] = []
                for entry in sorted(run_dir.iterdir()):
                    if not entry.is_file():
                        continue
                    m = _SUMMARY_RE.match(entry.name)
                    if not m:
                        continue
                    raw_agent_tag = m.group(1)
                    # Stored agent_id mirrors the directory
                    # convention `agent_<tag>` so it matches
                    # ExperimentResult.agent_id at refuted_by
                    # resolution time.
                    agent_id = f"agent_{raw_agent_tag}"
                    summary_files.append((agent_id, entry))
                if not summary_files:
                    stats["runs_missing_summary"] += 1
                    continue

                manifest = read_run_manifest(run_dir)
                problem_name = manifest.problem_name or "unknown"
                digest_node_id = research_digest_id(this_run_id)

                for agent_id, summary_path in summary_files:
                    try:
                        raw = summary_path.read_text(encoding="utf-8")
                        payload = json.loads(raw)
                    except (OSError, json.JSONDecodeError) as exc:
                        stats["summary_malformed_json"] += 1
                        findings.append({
                            "severity": "YELLOW",
                            "code": "agent_summary_malformed",
                            "run_id": this_run_id,
                            "agent_id": agent_id,
                            "path": str(summary_path),
                            "message": str(exc),
                        })
                        continue
                    if not isinstance(payload, dict):
                        stats["summary_malformed_json"] += 1
                        findings.append({
                            "severity": "YELLOW",
                            "code": "agent_summary_not_a_mapping",
                            "run_id": this_run_id,
                            "agent_id": agent_id,
                            "path": str(summary_path),
                            "message": (
                                f"top-level must be a mapping; "
                                f"got {type(payload).__name__}"
                            ),
                        })
                        continue

                    summary_text = payload.get("summary_text") or ""
                    summary_text = str(summary_text)
                    summary_hash = content_hash_json(payload)
                    summary_node_id = agent_summary_id(
                        this_run_id, agent_id,
                    )

                    # Resolve summary-level cite_key_ref:
                    # explicit `paper_key` field wins; else first
                    # `@key` mention in `summary_text`.
                    raw_cite_key = payload.get("paper_key")
                    if not isinstance(raw_cite_key, str) or not raw_cite_key:
                        m = _PAPER_KEY_RE.search(summary_text)
                        raw_cite_key = m.group(1) if m else None
                    summary_cite_ref = (
                        _normalize_paper_key(raw_cite_key)
                        if raw_cite_key else None
                    )

                    artifact_path = str(
                        summary_path.relative_to(runs_dir),
                    )
                    summary_source_record = {
                        "source": "engram_agent_summary",
                        "engram_run_id": this_run_id,
                        "problem_name": problem_name,
                        "agent_id": agent_id,
                        "artifact_path": artifact_path,
                        "content_hash": summary_hash,
                    }
                    if manifest.source_generation_id:
                        summary_source_record["source_generation_id"] = (
                            manifest.source_generation_id
                        )

                    summary_attrs: dict[str, Any] = {
                        "agent_summary_id": summary_node_id,
                        "run_id": this_run_id,
                        "agent_id": agent_id,
                        "summary_text": summary_text,
                        "content_hash": summary_hash,
                    }
                    if summary_cite_ref:
                        if scholarly_on:
                            summary_attrs["cite_key_ref"] = (
                                summary_cite_ref
                            )
                            stats["paper_citations_emitted_count"] += 1
                        else:
                            stats["paper_citations_suppressed_count"] += 1
                    if manifest.source_generation_id:
                        summary_attrs["source_generation_id"] = (
                            manifest.source_generation_id
                        )

                    yield NodeFact(
                        node_id=summary_node_id,
                        subtype="agent_summary",
                        attrs=summary_attrs,
                        source_record_id=summary_source_record,
                        source_revision={"run_id": run_id},
                        op="I",
                    )
                    stats["summaries_emitted"] += 1

                    # `produced_during` edge to the run's digest.
                    yield EdgeFact(
                        src=summary_node_id,
                        dst=digest_node_id,
                        edge_type="produced_during",
                        source_record_id={
                            "source": "engram_agent_summary",
                            "engram_run_id": this_run_id,
                            "agent_id": agent_id,
                        },
                        source_revision={"run_id": run_id},
                    )
                    stats["produced_during_edges_emitted"] += 1

                    # Decompose empirical_claims.
                    empirical_entries = payload.get(
                        "empirical_claims",
                    )
                    if isinstance(empirical_entries, list):
                        for idx, entry in enumerate(empirical_entries):
                            if not isinstance(entry, dict):
                                continue
                            statement = entry.get("statement")
                            if not isinstance(statement, str) or not statement:
                                continue
                            ec_node_id = empirical_claim_id(
                                this_run_id, agent_id, idx,
                            )
                            ec_hash = content_hash_json(entry)
                            score_delta = _coerce_float(
                                entry.get("observed_score_delta"),
                            )
                            entry_paper_key = entry.get("paper_key")
                            if not isinstance(entry_paper_key, str) or not entry_paper_key:
                                m = _PAPER_KEY_RE.search(statement)
                                entry_paper_key = m.group(1) if m else None
                            entry_cite_ref = (
                                _normalize_paper_key(entry_paper_key)
                                if entry_paper_key else None
                            )

                            ec_attrs: dict[str, Any] = {
                                "empirical_claim_id": ec_node_id,
                                "statement": statement,
                                "run_id": this_run_id,
                                "agent_id": agent_id,
                                "content_hash": ec_hash,
                            }
                            if score_delta is not None:
                                ec_attrs["observed_score_delta"] = (
                                    score_delta
                                )
                            if entry_cite_ref:
                                if scholarly_on:
                                    ec_attrs["cite_key_ref"] = (
                                        entry_cite_ref
                                    )
                                    stats["paper_citations_emitted_count"] += 1
                                else:
                                    stats["paper_citations_suppressed_count"] += 1
                            if manifest.source_generation_id:
                                ec_attrs["source_generation_id"] = (
                                    manifest.source_generation_id
                                )

                            yield NodeFact(
                                node_id=ec_node_id,
                                subtype="empirical_claim",
                                attrs=ec_attrs,
                                source_record_id={
                                    "source": "engram_agent_summary",
                                    "engram_run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "artifact_path": artifact_path,
                                    "claim_index": idx,
                                    "content_hash": ec_hash,
                                },
                                source_revision={"run_id": run_id},
                                op="I",
                            )
                            stats["empirical_claims_emitted"] += 1

                            yield EdgeFact(
                                src=summary_node_id,
                                dst=ec_node_id,
                                edge_type="surfaces",
                                source_record_id={
                                    "source": "engram_agent_summary",
                                    "engram_run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "claim_index": idx,
                                },
                                source_revision={"run_id": run_id},
                            )
                            stats["surfaces_edges_emitted"] += 1

                    # Decompose failed_approaches.
                    failed_entries = payload.get(
                        "failed_approaches",
                    )
                    if isinstance(failed_entries, list):
                        for idx, entry in enumerate(failed_entries):
                            if not isinstance(entry, dict):
                                continue
                            statement = entry.get("statement")
                            if not isinstance(statement, str) or not statement:
                                continue
                            fa_node_id = failed_approach_id(
                                this_run_id, agent_id, idx,
                            )
                            fa_hash = content_hash_json(entry)
                            failure_mode = entry.get("failure_mode")
                            if failure_mode is not None:
                                failure_mode = str(failure_mode)

                            fa_attrs: dict[str, Any] = {
                                "failed_approach_id": fa_node_id,
                                "statement": statement,
                                "run_id": this_run_id,
                                "agent_id": agent_id,
                                "content_hash": fa_hash,
                            }
                            if failure_mode:
                                fa_attrs["failure_mode"] = failure_mode

                            # Stamp refuted_by_experiment_ref when
                            # the entry references an experiment_id.
                            entry_exp_id = entry.get("experiment_id")
                            if isinstance(entry_exp_id, str) and entry_exp_id:
                                fa_attrs["refuted_by_experiment_ref"] = (
                                    experiment_result_id(
                                        this_run_id,
                                        agent_id,
                                        entry_exp_id,
                                    )
                                )
                                stats["refuted_by_refs_stamped"] += 1
                            if manifest.source_generation_id:
                                fa_attrs["source_generation_id"] = (
                                    manifest.source_generation_id
                                )

                            yield NodeFact(
                                node_id=fa_node_id,
                                subtype="failed_approach",
                                attrs=fa_attrs,
                                source_record_id={
                                    "source": "engram_agent_summary",
                                    "engram_run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "artifact_path": artifact_path,
                                    "approach_index": idx,
                                    "content_hash": fa_hash,
                                },
                                source_revision={"run_id": run_id},
                                op="I",
                            )
                            stats["failed_approaches_emitted"] += 1

                            yield EdgeFact(
                                src=summary_node_id,
                                dst=fa_node_id,
                                edge_type="surfaces",
                                source_record_id={
                                    "source": "engram_agent_summary",
                                    "engram_run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "approach_index": idx,
                                },
                                source_revision={"run_id": run_id},
                            )
                            stats["surfaces_edges_emitted"] += 1

        self.last_run_stats = stats
        self.last_run_findings = findings
        return SourceRun(
            facts=_facts(),
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            run_mode=mode,
        )

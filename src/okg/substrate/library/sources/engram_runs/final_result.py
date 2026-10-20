"""EngramFinalResultSource — per-run summary ingestion.

Reads `<runs_dir>/<run_id>/final_result.json` and emits one
FinalResult per run. Parses `best_score`, `total_simulations`,
`total_agents`, and `convergence_reason` from known JSON keys.

When the JSON carries a `best_snapshot` pointer (with
`agent_id`, `experiment_id`, and `language`), the source stamps
the resolved CandidateSolution id into the FinalResult's
`best_candidate_ref` attr — the intra-module cross-link rule
`final_result_best_solution_candidate_solution` then emits the
`best_solution` edge.

Also emits a `produced_during` edge from FinalResult to the
run's ResearchDigest, and (when bookkeeping is loaded) stamps
the corresponding Project's `produced_engram_final_ref` attr by
emitting a thin Project update — the digest source emits the
Project node itself, but the final-result source needs to
populate the secondary ref so the cross-link rule for the
final-result side of `produced_by` fires.

Per `add-engram-run-artifact-sources` (Layer 3A).

YELLOW findings:

  - Malformed `final_result.json` — YELLOW finding, skip file.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)

from ._common import (
    bookkeeping_loaded,
    candidate_solution_id,
    content_hash_json,
    final_result_id,
    iter_run_dirs,
    project_id_for_run,
    read_run_manifest,
    research_digest_id,
    validate_reasoning_loaded,
)


__all__ = ["EngramFinalResultSource"]


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class EngramFinalResultSource:
    """Read per-run `final_result.json` files."""

    runs_dir: str | Path | None = None
    loaded_modules: Optional[tuple[str, ...]] = None
    target_subtypes: tuple[str, ...] = ("final_result", "project")
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    last_run_findings: list[dict[str, Any]] = field(
        default_factory=list, init=False,
    )

    name: str = "engram_final_result"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): the scope is a set of staged
    # run-artifact JSON files on disk (`<runs_dir>/<run_id>/final_result.json`
    # plus the sibling `manifest.json` whose `problem_name` /
    # `source_generation_id` reach emitted attrs), with no git head and no
    # mutable upstream API — so the probe is the O(content) content-hash floor.
    # The probe re-runs the SAME `iter_run_dirs()` enumeration `run()` uses and
    # hashes each artifact's bytes, so an edit (a content sha shifts), an add (a
    # new `(key, sha)` entry + count bump — a new run dir, or a manifest added
    # to an existing run), and a delete (a dropped entry + count drop — a run
    # dir removed, or a `final_result.json` deleted) all bust the token, while a
    # worktree-checkout mtime bump does not. The RESOLVED `runs_dir` folds into
    # `config` so a runs_dir flip busts even if two roots shared file bytes;
    # `loaded_modules` folds in because `bookkeeping_loaded()` gates whether the
    # Project node is emitted at all, and `target_subtypes` folds in for the
    # same superset reason. No `now()`-derived value reaches an emitted attr —
    # `content_hash` is the artifact bytes and ids derive from the run id — so
    # the content floor is sound. The id-shaping helpers live in the sibling
    # `_common` module, so it is included in `emit_targets`: a change to
    # `final_result_id` / `candidate_solution_id` / `project_id_for_run` /
    # `content_hash_json` there must bust the token, else it would be a silent
    # false-unchanged after that code change. A vanished file reads as `absent`
    # inside the probe rather than raising, so a delete is a membership change,
    # not a swallowed error.
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

    def _build_change_probe(self) -> Any:
        import sys

        from . import _common
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            # Reuse the source's OWN run-dir enumeration so the probe scope is
            # byte-for-byte the set `run()` reads: the per-run `final_result.json`
            # AND the sibling `manifest.json` (its fields reach emitted attrs).
            # A file removed since last cycle is simply absent from this list,
            # which the membership digest reads as a delete.
            out: list[tuple[str, Path]] = []
            for run_dir in iter_run_dirs(self._runs_dir()):
                for fname in ("final_result.json", "manifest.json"):
                    path = run_dir / fname
                    if path.is_file():
                        out.append((str(path), path))
            return out

        return ContentHashProbe(
            content_items=_items,
            config={
                # Fold the RESOLVED root so a runs_dir flip busts even if two
                # roots happened to share file bytes.
                "runs_dir": str(self._runs_dir()),
                # `bookkeeping_loaded()` gates whether the Project node is
                # emitted, so the loaded-module set is emission-determining.
                "loaded_modules": (
                    sorted(self.loaded_modules)
                    if self.loaded_modules is not None
                    else None
                ),
                "target_subtypes": list(self.target_subtypes),
            },
            # Include the sibling `_common` module: it shapes every emitted id
            # and the content_hash, so a normalization change there must bust
            # the token (else false-unchanged after that edit).
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
            "finals_emitted": 0,
            "runs_missing_final": 0,
            "final_malformed_json": 0,
            "best_solution_refs_stamped": 0,
            "project_refs_stamped": 0,
            "project_refs_suppressed": 0,
            "produced_during_edges_emitted": 0,
        }
        findings: list[dict[str, Any]] = []
        runs_dir = self._runs_dir()
        run_dirs = iter_run_dirs(runs_dir)
        bookkeeping_on = bookkeeping_loaded(self.loaded_modules)

        def _facts() -> Iterator[Any]:
            for run_dir in run_dirs:
                stats["runs_seen"] += 1
                this_run_id = run_dir.name
                final_path = run_dir / "final_result.json"
                if not final_path.is_file():
                    stats["runs_missing_final"] += 1
                    continue
                try:
                    raw = final_path.read_text(encoding="utf-8")
                    payload = json.loads(raw)
                except (OSError, json.JSONDecodeError) as exc:
                    stats["final_malformed_json"] += 1
                    findings.append({
                        "severity": "YELLOW",
                        "code": "final_result_malformed",
                        "run_id": this_run_id,
                        "path": str(final_path),
                        "message": str(exc),
                    })
                    continue
                if not isinstance(payload, dict):
                    stats["final_malformed_json"] += 1
                    findings.append({
                        "severity": "YELLOW",
                        "code": "final_result_not_a_mapping",
                        "run_id": this_run_id,
                        "path": str(final_path),
                        "message": (
                            f"final_result.json top-level must be "
                            f"a mapping; got {type(payload).__name__}"
                        ),
                    })
                    continue

                manifest = read_run_manifest(run_dir)
                problem_name = (
                    manifest.problem_name
                    or payload.get("problem_name")
                    or "unknown"
                )
                problem_name = str(problem_name).strip() or "unknown"
                best_score = _coerce_float(payload.get("best_score"))
                total_simulations = _coerce_int(
                    payload.get("total_simulations"),
                )
                total_agents = _coerce_int(
                    payload.get("total_agents"),
                )
                convergence_reason = payload.get("convergence_reason")
                if convergence_reason is not None:
                    convergence_reason = str(convergence_reason)

                final_node_id = final_result_id(this_run_id)
                digest_node_id = research_digest_id(this_run_id)
                final_hash = content_hash_json(payload)

                attrs: dict[str, Any] = {
                    "final_result_id": final_node_id,
                    "run_id": this_run_id,
                    "problem_name": problem_name,
                    "content_hash": final_hash,
                }
                if best_score is not None:
                    attrs["best_score"] = best_score
                if total_simulations is not None:
                    attrs["total_simulations"] = total_simulations
                if total_agents is not None:
                    attrs["total_agents"] = total_agents
                if convergence_reason:
                    attrs["convergence_reason"] = convergence_reason
                if manifest.source_generation_id:
                    attrs["source_generation_id"] = (
                        manifest.source_generation_id
                    )

                # Stamp `best_candidate_ref` for the deferred-
                # resolution cross-link rule.
                best_snapshot = payload.get("best_snapshot")
                if isinstance(best_snapshot, dict):
                    bs_agent = best_snapshot.get("agent_id")
                    bs_exp = best_snapshot.get("experiment_id")
                    bs_lang = best_snapshot.get("language")
                    if (
                        isinstance(bs_agent, str)
                        and isinstance(bs_exp, str)
                        and isinstance(bs_lang, str)
                    ):
                        cand_id = candidate_solution_id(
                            this_run_id, bs_agent, bs_exp, bs_lang,
                        )
                        attrs["best_candidate_ref"] = cand_id
                        stats["best_solution_refs_stamped"] += 1

                artifact_path = str(
                    final_path.relative_to(runs_dir),
                )
                source_record_id = {
                    "source": "engram_final_result",
                    "engram_run_id": this_run_id,
                    "problem_name": problem_name,
                    "artifact_path": artifact_path,
                    "content_hash": final_hash,
                }
                if manifest.source_generation_id:
                    source_record_id["source_generation_id"] = (
                        manifest.source_generation_id
                    )

                yield NodeFact(
                    node_id=final_node_id,
                    subtype="final_result",
                    attrs=attrs,
                    source_record_id=source_record_id,
                    source_revision={"run_id": run_id},
                    op="I",
                )
                stats["finals_emitted"] += 1

                yield EdgeFact(
                    src=final_node_id,
                    dst=digest_node_id,
                    edge_type="produced_during",
                    source_record_id={
                        "source": "engram_final_result",
                        "engram_run_id": this_run_id,
                    },
                    source_revision={"run_id": run_id},
                )
                stats["produced_during_edges_emitted"] += 1

                # When bookkeeping is loaded, emit a Project
                # NodeFact carrying `produced_engram_final_ref`
                # so the cross-link rule for the final-result
                # side of `produced_by` resolves. The digest
                # source also emits this Project node with the
                # digest_ref; the substrate's per-source-record
                # dedup collapses identical projections, and the
                # eventual nodes_live row carries both refs
                # (substrate's projector merges attrs across
                # node_facts from different sources).
                if bookkeeping_on:
                    pid = project_id_for_run(this_run_id)
                    project_attrs: dict[str, Any] = {
                        "project_id": pid,
                        "slug": pid.split(":", 1)[1],
                        "title": (
                            f"Engram run {this_run_id} — "
                            f"{problem_name}"
                        ),
                        "status": "done",
                        "produced_engram_final_ref": (
                            final_node_id
                        ),
                    }
                    yield NodeFact(
                        node_id=pid,
                        subtype="project",
                        attrs=project_attrs,
                        source_record_id={
                            "source": "engram_final_result",
                            "engram_run_id": this_run_id,
                            "materialized_from": final_node_id,
                        },
                        source_revision={"run_id": run_id},
                        op="I",
                    )
                    stats["project_refs_stamped"] += 1
                else:
                    stats["project_refs_suppressed"] += 1

        self.last_run_stats = stats
        self.last_run_findings = findings
        return SourceRun(
            facts=_facts(),
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            run_mode=mode,
        )

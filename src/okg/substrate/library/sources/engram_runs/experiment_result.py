"""EngramExperimentResultSource — per-experiment score ingestion.

Walks `<runs_dir>/<run_id>/knowledgebase/agent_<N>/experiments/
exp_<M>/score.txt` and emits one ExperimentResult per file. Reads
the score as float from score.txt, optionally pulls
`score_metric` from co-located metric.txt and `simulator` from
co-located simulator.txt.

Emits intra-module `evaluates` (deferred to the candidate-solution
source since this source can't know whether a snapshot exists; the
candidate-solution source emits `evaluates` when it walks a
snapshot whose sibling score.txt also exists — actually, the
cleanest design: the experiment_result source emits the
`evaluates` edge against the language-keyed CandidateSolution it
expects to coexist; if the candidate isn't yet projected, the
substrate's standard cross-link re-eval pattern picks it up next
cycle). For Layer 3A we keep the edge emission on this source:
emit `evaluates` for every snapshot.* file present in the same
experiment directory, language-keyed via the file extension; the
substrate's narrowing trigger accepts these because the
narrowing `experiment_result -- evaluates -- candidate_solution`
is in the catalog. If the matching CandidateSolution hasn't been
projected this cycle (e.g., the candidate-solution source runs
later), the edge fact still lands as `op='I'` and links once the
dst node exists.

Per `add-engram-run-artifact-sources` (Layer 3A).

YELLOW findings:

  - Malformed `score.txt` (non-float body) — emit YELLOW finding,
    skip the file, continue with siblings.
"""
from __future__ import annotations

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

from ._common import (
    candidate_solution_id,
    content_hash_text,
    experiment_result_id,
    iter_run_dirs,
    read_run_manifest,
    research_digest_id,
    validate_reasoning_loaded,
)


__all__ = ["EngramExperimentResultSource"]


# Extension → language tag mapping (kept aligned with
# `candidate_solution.py`).
_LANG_BY_EXT: dict[str, str] = {
    ".py": "python",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".c": "c",
    ".rs": "rust",
    ".go": "go",
    ".java": "java",
    ".ts": "typescript",
    ".js": "javascript",
}


_AGENT_DIR_RE = re.compile(r"^agent_[A-Za-z0-9_-]+$")
_EXP_DIR_RE = re.compile(r"^exp_[A-Za-z0-9_-]+$")


@dataclass
class EngramExperimentResultSource:
    """Read per-experiment `score.txt` files."""

    runs_dir: str | Path | None = None
    loaded_modules: Optional[tuple[str, ...]] = None
    target_subtypes: tuple[str, ...] = (
        "experiment_result", "candidate_solution",
    )
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    last_run_findings: list[dict[str, Any]] = field(
        default_factory=list, init=False,
    )

    name: str = "engram_experiment_result"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323). Emission is a pure function
    # of staged run-artifact files on disk under the resolved `runs_dir`:
    # per-experiment `score.txt` (drives the ExperimentResult node + score),
    # co-located `metric.txt` / `simulator.txt` sidecars (emitted attrs),
    # `snapshot.*` files (drive `evaluates` edges, language-keyed), and each
    # run's `manifest.json` (problem_name + source_generation_id reach the
    # emitted attrs and source_record_id). No git scope, no live API, no
    # `datetime.now()` reaches an emitted attribute -- every emitted value is
    # content-derived -- so the O(content) ContentHashProbe floor is the sound
    # probe. The membership digest over the full file enumeration is
    # delete-sensitive (removing a score.txt drops its node; removing a
    # snapshot.* drops an edge), and content-based so it is mtime-immune. The
    # resolved root is folded into the config component, so a `runs_dir` flip
    # busts the token even when the two trees happen to hash identically.
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
        # Built at the END of __post_init__ so `runs_dir` is validated and
        # `_runs_dir()` resolves. See `change_probe_kind` above.
        self.change_probe = self._build_change_probe()

    def _runs_dir(self) -> Path:
        return Path(str(self.runs_dir)).expanduser()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
            file_content_items,
        )

        # Enumerate EVERY file under the resolved `runs_dir` -- a strict
        # superset of the emission-determining files (score.txt, metric.txt,
        # simulator.txt, snapshot.*, manifest.json). A superset is sound: an
        # unrelated sibling edit can only over-fire (changed=true), never
        # under-fire. The enumeration re-walks live state on each call, so a
        # deleted artifact is simply absent next cycle -- which is exactly how
        # the membership digest detects the delete.
        def _content_items() -> list[tuple[str, Path]]:
            return file_content_items(self._runs_dir())

        # emit_targets: THIS module (the walker, the `_LANG_BY_EXT` /
        # `_AGENT_DIR_RE` / `_EXP_DIR_RE` constants, and the node/edge shaping)
        # PLUS the sibling `_common` module, whose id helpers
        # (`experiment_result_id`, `candidate_solution_id`,
        # `research_digest_id`), manifest reader, and run-dir walker shape the
        # emitted ids/attrs. A normalization change in `_common` that is NOT in
        # emit_targets would be a false-unchanged after that code change.
        from okg.substrate.library.sources.engram_runs import _common

        return ContentHashProbe(
            content_items=_content_items,
            config={"runs_dir": str(self._runs_dir())},
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
            "experiments_seen": 0,
            "experiments_emitted": 0,
            "score_parse_failed": 0,
            "evaluates_edges_emitted": 0,
            "produced_during_edges_emitted": 0,
        }
        findings: list[dict[str, Any]] = []
        runs_dir = self._runs_dir()
        run_dirs = iter_run_dirs(runs_dir)

        def _read_sidecar(path: Path) -> Optional[str]:
            if not path.is_file():
                return None
            try:
                return path.read_text(encoding="utf-8").strip() or None
            except OSError:
                return None

        def _facts() -> Iterator[Any]:
            for run_dir in run_dirs:
                stats["runs_seen"] += 1
                this_run_id = run_dir.name
                manifest = read_run_manifest(run_dir)
                problem_name = (
                    manifest.problem_name or "unknown"
                )
                kb_dir = run_dir / "knowledgebase"
                if not kb_dir.is_dir():
                    continue
                digest_node_id = research_digest_id(this_run_id)
                for agent_dir in sorted(kb_dir.iterdir()):
                    if not agent_dir.is_dir() or not _AGENT_DIR_RE.match(
                        agent_dir.name,
                    ):
                        continue
                    agent_id = agent_dir.name
                    experiments_dir = agent_dir / "experiments"
                    if not experiments_dir.is_dir():
                        continue
                    for exp_dir in sorted(experiments_dir.iterdir()):
                        if not exp_dir.is_dir() or not _EXP_DIR_RE.match(
                            exp_dir.name,
                        ):
                            continue
                        stats["experiments_seen"] += 1
                        exp_id = exp_dir.name
                        score_path = exp_dir / "score.txt"
                        if not score_path.is_file():
                            # No score → skip (a snapshot-only
                            # experiment is captured by the
                            # candidate-solution source).
                            continue
                        try:
                            raw_body = score_path.read_text(
                                encoding="utf-8",
                            )
                        except OSError as exc:
                            findings.append({
                                "severity": "YELLOW",
                                "code": "score_file_unreadable",
                                "run_id": this_run_id,
                                "agent_id": agent_id,
                                "experiment_id": exp_id,
                                "path": str(score_path),
                                "message": str(exc),
                            })
                            continue
                        body = raw_body.strip()
                        try:
                            score_value = float(body.split()[0])
                        except (ValueError, IndexError):
                            stats["score_parse_failed"] += 1
                            findings.append({
                                "severity": "YELLOW",
                                "code": "score_not_a_float",
                                "run_id": this_run_id,
                                "agent_id": agent_id,
                                "experiment_id": exp_id,
                                "path": str(score_path),
                                "message": (
                                    f"score.txt body {body[:80]!r} "
                                    f"could not parse as float"
                                ),
                            })
                            continue

                        score_metric = _read_sidecar(
                            exp_dir / "metric.txt",
                        )
                        simulator = _read_sidecar(
                            exp_dir / "simulator.txt",
                        )

                        exp_node_id = experiment_result_id(
                            this_run_id, agent_id, exp_id,
                        )
                        exp_hash = content_hash_text(body)

                        attrs: dict[str, Any] = {
                            "experiment_result_id": exp_node_id,
                            "run_id": this_run_id,
                            "agent_id": agent_id,
                            "experiment_id": exp_id,
                            "score": score_value,
                            "content_hash": exp_hash,
                        }
                        if score_metric:
                            attrs["score_metric"] = score_metric
                        if simulator:
                            attrs["simulator"] = simulator
                        if manifest.source_generation_id:
                            attrs["source_generation_id"] = (
                                manifest.source_generation_id
                            )

                        artifact_path = str(
                            score_path.relative_to(runs_dir),
                        )
                        source_record_id = {
                            "source": "engram_experiment_result",
                            "engram_run_id": this_run_id,
                            "problem_name": problem_name,
                            "agent_id": agent_id,
                            "experiment_id": exp_id,
                            "artifact_path": artifact_path,
                            "content_hash": exp_hash,
                        }
                        if manifest.source_generation_id:
                            source_record_id["source_generation_id"] = (
                                manifest.source_generation_id
                            )

                        yield NodeFact(
                            node_id=exp_node_id,
                            subtype="experiment_result",
                            attrs=attrs,
                            source_record_id=source_record_id,
                            source_revision={"run_id": run_id},
                            op="I",
                        )
                        stats["experiments_emitted"] += 1

                        # `produced_during` edge to the run's
                        # ResearchDigest. The digest may not be
                        # projected yet (this source is decoupled
                        # from the digest source); the substrate's
                        # standard cross-link re-eval handles the
                        # deferred case.
                        yield EdgeFact(
                            src=exp_node_id,
                            dst=digest_node_id,
                            edge_type="produced_during",
                            source_record_id={
                                "source": "engram_experiment_result",
                                "engram_run_id": this_run_id,
                                "agent_id": agent_id,
                                "experiment_id": exp_id,
                            },
                            source_revision={"run_id": run_id},
                        )
                        stats["produced_during_edges_emitted"] += 1

                        # `evaluates` edges for every snapshot.*
                        # file in the same experiment dir. Each
                        # snapshot file's language is keyed from
                        # the extension; the CandidateSolution
                        # node id is constructed deterministically
                        # so the edge resolves once that source
                        # projects.
                        for snap_path in sorted(exp_dir.iterdir()):
                            if not snap_path.is_file():
                                continue
                            if snap_path.stem != "snapshot":
                                continue
                            lang = _LANG_BY_EXT.get(
                                snap_path.suffix.lower(),
                            )
                            if not lang:
                                # Unknown snapshot extension — log
                                # but don't error; the candidate
                                # source has the same blindspot.
                                continue
                            cand_node_id = candidate_solution_id(
                                this_run_id, agent_id, exp_id, lang,
                            )
                            yield EdgeFact(
                                src=exp_node_id,
                                dst=cand_node_id,
                                edge_type="evaluates",
                                source_record_id={
                                    "source": "engram_experiment_result",
                                    "engram_run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "experiment_id": exp_id,
                                    "candidate_language": lang,
                                },
                                source_revision={"run_id": run_id},
                            )
                            stats["evaluates_edges_emitted"] += 1

        self.last_run_stats = stats
        self.last_run_findings = findings
        return SourceRun(
            facts=_facts(),
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            run_mode=mode,
        )

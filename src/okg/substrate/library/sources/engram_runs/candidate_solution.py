"""EngramCandidateSolutionSource — per-snapshot file ingestion.

Walks `<runs_dir>/<run_id>/knowledgebase/agent_<N>/experiments/
exp_<M>/snapshot.{py,cpp,…}` and emits one CandidateSolution per
file. The language is keyed from the file extension; the content
hash is the SHA-256 of the file body.

Also emits a `produced_during` edge from each CandidateSolution
to the run's ResearchDigest. The `evaluates` edge from the
sibling ExperimentResult is emitted by the experiment-result
source (so both sides converge on the same edge attrs).

Per `add-engram-run-artifact-sources` (Layer 3A).

YELLOW findings:

  - Unreadable snapshot file — YELLOW finding, skip file.
  - Unknown extension — silently skip (the file isn't a snapshot
    the source knows how to type).
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

from . import _common
from ._common import (
    candidate_solution_id,
    content_hash_text,
    iter_run_dirs,
    read_run_manifest,
    research_digest_id,
    validate_reasoning_loaded,
)


__all__ = ["EngramCandidateSolutionSource"]


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
class EngramCandidateSolutionSource:
    """Read snapshot.{py,cpp,…} files from each experiment dir."""

    runs_dir: str | Path | None = None
    loaded_modules: Optional[tuple[str, ...]] = None
    # Cap on per-file body size in chars; bodies larger than this
    # are still emitted (the substrate doesn't enforce a node-attr
    # size cap), but the source records the count for ops
    # visibility. Default 1 MiB is generous for an algorithm
    # snapshot.
    max_body_chars: int = 1_048_576
    target_subtypes: tuple[str, ...] = ("candidate_solution",)
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    last_run_findings: list[dict[str, Any]] = field(
        default_factory=list, init=False,
    )

    name: str = "engram_candidate_solution"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): the emission is a set of
    # `snapshot.{py,cpp,…}` bodies on disk plus the per-run `manifest.json`
    # (problem_name reaches the source_record_id; source_generation_id reaches
    # the emitted node attrs). There is no git head, append offset, or upstream
    # API — just bytes under `runs_dir` — so the sound floor is the O(content)
    # ContentHashProbe over the source's OWN walk (`iter_run_dirs` + the same
    # agent/exp/snapshot traversal + each run's manifest.json). It is
    # membership-sensitive, so an add / edit / delete of any snapshot or manifest
    # busts the token, and content-based, so a worktree-checkout mtime bump does
    # NOT. No `now()`/clock value reaches an emitted attr (only `run_id` lands on
    # `source_revision` lineage), so the content hash is a complete superset of
    # the emission-determining bytes. The `runs_dir` root, `_LANG_BY_EXT`
    # extension→language map, and the `_common` id-shaping helpers
    # (`candidate_solution_id` / `research_digest_id` / `_safe_slug` /
    # `content_hash_text`) all shape emitted ids/hashes, so they are folded into
    # the token: `runs_dir` into config, the rest via emit_targets (this module
    # plus the sibling `_common`).
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

    def _iter_emission_files(self) -> Iterator[Path]:
        """Yield every file whose bytes reach an emitted fact, using the
        source's OWN walk so the probe scope tracks the reader exactly.

        Per run dir: the `manifest.json` (problem_name → source_record_id;
        source_generation_id → node attrs) and every `snapshot.{ext}` under
        `knowledgebase/agent_*/experiments/exp_*/` whose extension is a known
        language. Re-enumerated live on each call, so a deletion drops the
        entry (membership-sensitive bust).
        """
        runs_dir = self._runs_dir()
        for run_dir in iter_run_dirs(runs_dir):
            manifest_path = run_dir / "manifest.json"
            if manifest_path.is_file():
                yield manifest_path
            kb_dir = run_dir / "knowledgebase"
            if not kb_dir.is_dir():
                continue
            for agent_dir in sorted(kb_dir.iterdir()):
                if not agent_dir.is_dir() or not _AGENT_DIR_RE.match(
                    agent_dir.name,
                ):
                    continue
                experiments_dir = agent_dir / "experiments"
                if not experiments_dir.is_dir():
                    continue
                for exp_dir in sorted(experiments_dir.iterdir()):
                    if not exp_dir.is_dir() or not _EXP_DIR_RE.match(
                        exp_dir.name,
                    ):
                        continue
                    for snap_path in sorted(exp_dir.iterdir()):
                        if not snap_path.is_file():
                            continue
                        if snap_path.stem != "snapshot":
                            continue
                        if not _LANG_BY_EXT.get(snap_path.suffix.lower()):
                            continue
                        yield snap_path

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        runs_dir = self._runs_dir()

        def _items() -> list[tuple[str, Path]]:
            # Re-enumerate the live emission scope each call (delete = dropped
            # entry). Key on the path relative to the resolved root so the
            # membership digest is stable across absolute-path moves but still
            # delete-sensitive within a fixed root.
            out: list[tuple[str, Path]] = []
            for p in self._iter_emission_files():
                try:
                    key = str(p.relative_to(runs_dir))
                except ValueError:
                    key = str(p)
                out.append((key, p))
            return out

        return ContentHashProbe(
            content_items=_items,
            config={
                "runs_dir": str(runs_dir),
                "name": self.name,
                "profile": self.profile,
                # max_body_chars only affects ops stats/findings (oversize
                # bodies are still emitted), but fold it in conservatively: a
                # false-changed on a knob flip is permitted, a false-unchanged
                # is not.
                "max_body_chars": self.max_body_chars,
            },
            # `_common` shapes emitted ids (`candidate_solution_id` /
            # `research_digest_id` / `_safe_slug`) and the content hash
            # (`content_hash_text`), and reads the manifest fields that land on
            # emitted attrs — so a normalization change there must bust the
            # token even when the scope bytes are unchanged. Include both this
            # module's emission logic and the `_common` sibling.
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
            "snapshots_seen": 0,
            "candidates_emitted": 0,
            "snapshots_unreadable": 0,
            "snapshots_oversize": 0,
            "produced_during_edges_emitted": 0,
        }
        findings: list[dict[str, Any]] = []
        runs_dir = self._runs_dir()
        run_dirs = iter_run_dirs(runs_dir)

        def _facts() -> Iterator[Any]:
            for run_dir in run_dirs:
                stats["runs_seen"] += 1
                this_run_id = run_dir.name
                manifest = read_run_manifest(run_dir)
                problem_name = manifest.problem_name or "unknown"
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
                        exp_id = exp_dir.name
                        for snap_path in sorted(exp_dir.iterdir()):
                            if not snap_path.is_file():
                                continue
                            if snap_path.stem != "snapshot":
                                continue
                            lang = _LANG_BY_EXT.get(
                                snap_path.suffix.lower(),
                            )
                            if not lang:
                                continue
                            stats["snapshots_seen"] += 1
                            try:
                                source_text = snap_path.read_text(
                                    encoding="utf-8",
                                )
                            except (OSError, UnicodeDecodeError) as exc:
                                stats["snapshots_unreadable"] += 1
                                findings.append({
                                    "severity": "YELLOW",
                                    "code": "snapshot_unreadable",
                                    "run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "experiment_id": exp_id,
                                    "path": str(snap_path),
                                    "message": str(exc),
                                })
                                continue
                            body_hash = content_hash_text(source_text)
                            if len(source_text) > self.max_body_chars:
                                stats["snapshots_oversize"] += 1
                                findings.append({
                                    "severity": "YELLOW",
                                    "code": "snapshot_oversize",
                                    "run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "experiment_id": exp_id,
                                    "path": str(snap_path),
                                    "message": (
                                        f"snapshot body length "
                                        f"{len(source_text)} > "
                                        f"max_body_chars "
                                        f"{self.max_body_chars}"
                                    ),
                                })

                            cand_node_id = candidate_solution_id(
                                this_run_id, agent_id, exp_id, lang,
                            )
                            attrs: dict[str, Any] = {
                                "candidate_solution_id": cand_node_id,
                                "run_id": this_run_id,
                                "agent_id": agent_id,
                                "experiment_id": exp_id,
                                "language": lang,
                                "content_hash": body_hash,
                                "source_text": source_text,
                            }
                            if manifest.source_generation_id:
                                attrs["source_generation_id"] = (
                                    manifest.source_generation_id
                                )

                            artifact_path = str(
                                snap_path.relative_to(runs_dir),
                            )
                            source_record_id = {
                                "source": "engram_candidate_solution",
                                "engram_run_id": this_run_id,
                                "problem_name": problem_name,
                                "agent_id": agent_id,
                                "experiment_id": exp_id,
                                "artifact_path": artifact_path,
                                "content_hash": body_hash,
                                "language": lang,
                            }
                            if manifest.source_generation_id:
                                source_record_id["source_generation_id"] = (
                                    manifest.source_generation_id
                                )

                            yield NodeFact(
                                node_id=cand_node_id,
                                subtype="candidate_solution",
                                attrs=attrs,
                                source_record_id=source_record_id,
                                source_revision={"run_id": run_id},
                                op="I",
                            )
                            stats["candidates_emitted"] += 1

                            yield EdgeFact(
                                src=cand_node_id,
                                dst=digest_node_id,
                                edge_type="produced_during",
                                source_record_id={
                                    "source": "engram_candidate_solution",
                                    "engram_run_id": this_run_id,
                                    "agent_id": agent_id,
                                    "experiment_id": exp_id,
                                    "language": lang,
                                },
                                source_revision={"run_id": run_id},
                            )
                            stats["produced_during_edges_emitted"] += 1

        self.last_run_stats = stats
        self.last_run_findings = findings
        return SourceRun(
            facts=_facts(),
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            run_mode=mode,
        )

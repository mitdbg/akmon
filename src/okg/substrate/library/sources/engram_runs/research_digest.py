"""EngramResearchDigestSource — per-run digest ingestion.

Walks `<runs_dir>/<run_id>/research_journal.md` and emits one
ResearchDigest per run. Optionally materializes the
`bookkeeping.Project` for the run (when `bookkeeping` is in
`loaded_modules`) so the cross-link rule `Project produced_by
ResearchDigest` resolves on the same publish cycle.

Per `add-engram-run-artifact-sources` (Layer 3A).

YELLOW findings (per spec `Malformed Run Directories Surfaced,
Not Fatal`):

  - Run directory missing `research_journal.md` — emit one
    YELLOW finding, increment `runs_missing_journal` stat,
    SKIP the run.

A run with a journal that has no frontmatter still ingests; the
source falls back to the directory name for the run id and
`<unknown>` for the problem name.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    NodeFact,
    SourceRun,
)

from ._common import (
    bookkeeping_loaded,
    content_hash_text,
    iter_run_dirs,
    parse_journal_frontmatter,
    project_id_for_run,
    read_run_manifest,
    research_digest_id,
    validate_reasoning_loaded,
)


__all__ = ["EngramResearchDigestSource"]


@dataclass
class EngramResearchDigestSource:
    """Read `research_journal.md` from each run directory."""

    runs_dir: str | Path | None = None
    # The deployment's loaded module set, used for the conditional
    # cross-module emission gate. `None` opts out of the check;
    # the catalog FK / narrowing trigger still rejects edges if
    # the target module isn't actually loaded.
    loaded_modules: Optional[tuple[str, ...]] = None
    target_subtypes: tuple[str, ...] = ("research_digest", "project")
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    last_run_findings: list[dict[str, Any]] = field(
        default_factory=list, init=False,
    )

    name: str = "engram_research_digest"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): the scope is a tree of local
    # run-artifact files under ``runs_dir`` — one ``research_journal.md`` (the
    # digest body / frontmatter) plus an optional ``manifest.json`` (problem
    # name + source_generation_id) per run directory. There is no git head,
    # append offset, or upstream API — just bytes on disk — so the sound floor
    # is the O(content) ContentHashProbe over the source's OWN run-dir
    # enumeration (``iter_run_dirs``). It is membership-sensitive, so adding or
    # deleting a run directory (or its journal / manifest) busts the token, and
    # content-based, so a worktree-checkout mtime bump does NOT.
    #
    # Both per-run files feed emitted attrs: the journal body becomes
    # ``digest_text`` / ``content_hash`` and the manifest supplies
    # ``problem_name`` / ``source_generation_id`` — so BOTH are folded into the
    # content scope (a manifest-only edit must bust the token). No
    # ``now()``/clock value reaches an emitted attr (``run_id`` lands only on
    # ``source_revision`` lineage), so the content hash is a complete superset
    # of the emission-determining content. ``loaded_modules`` GATES the
    # conditional ``bookkeeping.Project`` materialization, so it is folded into
    # config (a flip of bookkeeping-loaded changes which nodes are emitted).
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if not self.runs_dir:
            raise SubstrateError(
                f"{self.name}: runs_dir is required",
                fix_hint=(
                    "Set `runs_dir: <path>` in the deployment's "
                    "source_registry.yaml entry for engram_runs."
                ),
            )
        # `reasoning` is a hard dependency — fail at configure
        # time with a clear message rather than letting the
        # catalog compile fail later with an FK violation.
        validate_reasoning_loaded(
            self.loaded_modules, source_name=self.name,
        )
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        from . import _common

        def _items() -> list[tuple[str, Path]]:
            # Re-enumerate live scope each call using the source's OWN run-dir
            # walk (a deleted run dir is simply absent next call -> the entry
            # drops, busting membership). For each run dir, fold in BOTH the
            # journal and the manifest: a vanished file hashes to "absent" via
            # the probe's per-item read, which is exactly how the
            # journal-presence gate and manifest-edit sensitivity are captured.
            root = self._runs_dir()
            out: list[tuple[str, Path]] = []
            for run_dir in iter_run_dirs(root):
                out.append(
                    (f"{run_dir.name}/research_journal.md",
                     run_dir / "research_journal.md"),
                )
                out.append(
                    (f"{run_dir.name}/manifest.json",
                     run_dir / "manifest.json"),
                )
            return out

        return ContentHashProbe(
            content_items=_items,
            config={
                # Fold the RESOLVED runs_dir root into config so a flip of the
                # source's root busts the token even if both roots happened to
                # hold identical bytes.
                "runs_dir": str(self._runs_dir()),
                # `loaded_modules` gates Project materialization (emission), so
                # a flip of bookkeeping-loaded must bust the token. Sort for a
                # stable hash.
                "loaded_modules": sorted(
                    str(m) for m in (self.loaded_modules or ())
                ),
                "target_subtypes": list(self.target_subtypes),
                "status": self.status,
                "name": self.name,
                "profile": self.profile,
            },
            # `_common` shapes emitted ids and content: node-id formation
            # (`research_digest_id` / `project_id_for_run`), content hashing
            # (`content_hash_text`), frontmatter parsing
            # (`parse_journal_frontmatter`), manifest reading
            # (`read_run_manifest`), the run-dir walk (`iter_run_dirs`), and the
            # bookkeeping gate (`bookkeeping_loaded`) all live there. A
            # normalization change in `_common` (e.g. an id-slug or
            # frontmatter-parse tweak) reshapes emission with UNCHANGED bytes,
            # so it must be in emit_targets or the token is a false-unchanged
            # after that code change.
            emit_targets=[sys.modules[__name__], _common],
        )

    def _runs_dir(self) -> Path:
        return Path(str(self.runs_dir)).expanduser()

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
            "digests_emitted": 0,
            "runs_missing_journal": 0,
            "projects_emitted": 0,
            "project_materialization_suppressed": 0,
        }
        findings: list[dict[str, Any]] = []
        runs_dir = self._runs_dir()
        run_dirs = iter_run_dirs(runs_dir)
        bookkeeping_on = bookkeeping_loaded(self.loaded_modules)

        def _facts() -> Iterator[Any]:
            for run_dir in run_dirs:
                stats["runs_seen"] += 1
                this_run_id = run_dir.name
                journal_path = run_dir / "research_journal.md"
                if not journal_path.is_file():
                    stats["runs_missing_journal"] += 1
                    findings.append({
                        "severity": "YELLOW",
                        "code": "research_journal_missing",
                        "run_id": this_run_id,
                        "path": str(journal_path),
                        "message": (
                            f"run {this_run_id}: missing "
                            f"research_journal.md; skipping run"
                        ),
                    })
                    continue
                try:
                    journal_text = journal_path.read_text(
                        encoding="utf-8",
                    )
                except OSError as exc:
                    findings.append({
                        "severity": "YELLOW",
                        "code": "research_journal_unreadable",
                        "run_id": this_run_id,
                        "path": str(journal_path),
                        "message": str(exc),
                    })
                    continue

                frontmatter, body = parse_journal_frontmatter(
                    journal_text,
                )
                manifest = read_run_manifest(run_dir)
                problem_name = (
                    manifest.problem_name
                    or frontmatter.get("problem_name")
                    or frontmatter.get("problem")
                    or "unknown"
                )
                problem_name = str(problem_name).strip() or "unknown"
                digest_text = (
                    body.strip() if body.strip() else journal_text
                )
                digest_hash = content_hash_text(digest_text)
                digest_node_id = research_digest_id(this_run_id)

                attrs: dict[str, Any] = {
                    "research_digest_id": digest_node_id,
                    "run_id": this_run_id,
                    "problem_name": problem_name,
                    "digest_text": digest_text,
                    "content_hash": digest_hash,
                }
                if manifest.source_generation_id:
                    attrs["source_generation_id"] = (
                        manifest.source_generation_id
                    )

                artifact_path = str(
                    journal_path.relative_to(runs_dir)
                )
                source_record_id = {
                    "source": "engram_research_digest",
                    "engram_run_id": this_run_id,
                    "problem_name": problem_name,
                    "artifact_path": artifact_path,
                    "content_hash": digest_hash,
                }
                if manifest.source_generation_id:
                    source_record_id["source_generation_id"] = (
                        manifest.source_generation_id
                    )

                yield NodeFact(
                    node_id=digest_node_id,
                    subtype="research_digest",
                    attrs=attrs,
                    source_record_id=source_record_id,
                    source_revision={"run_id": run_id},
                    op="I",
                )
                stats["digests_emitted"] += 1

                # Conditional bookkeeping materialization. When
                # bookkeeping is loaded, mint a Project node for
                # the run and stamp `produced_engram_digest_ref`
                # so the cross-link rule fires this cycle.
                if bookkeeping_on:
                    pid = project_id_for_run(this_run_id)
                    project_attrs: dict[str, Any] = {
                        "project_id": pid,
                        "slug": pid.split(":", 1)[1],
                        "title": (
                            f"Engram run {this_run_id} — "
                            f"{problem_name}"
                        ),
                        "purpose": (
                            f"Engram research run targeting "
                            f"problem {problem_name!r}; ingested "
                            f"from runs_dir."
                        ),
                        "status": "done",
                        "produced_engram_digest_ref": (
                            digest_node_id
                        ),
                    }
                    yield NodeFact(
                        node_id=pid,
                        subtype="project",
                        attrs=project_attrs,
                        source_record_id={
                            "source": "engram_research_digest",
                            "engram_run_id": this_run_id,
                            "materialized_from": digest_node_id,
                        },
                        source_revision={"run_id": run_id},
                        op="I",
                    )
                    stats["projects_emitted"] += 1
                else:
                    stats["project_materialization_suppressed"] += 1

        self.last_run_stats = stats
        self.last_run_findings = findings
        return SourceRun(
            facts=_facts(),
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            run_mode=mode,
        )

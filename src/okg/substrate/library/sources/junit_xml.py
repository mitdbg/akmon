"""JUnit-XML source adapter — language-agnostic test results.

Reads JUnit XML files from a configurable drop directory.
Emits one TestRun per file plus N TestCase children. SHA is
derived either from explicit `sha=` config or from
`git rev-parse HEAD` at run time (best-effort; the operator
should pin the SHA via the runner that produces the XML).

Per `add-ci-test-source-adapter` (2026-04-26).
"""
from __future__ import annotations

import shutil
import subprocess  # nosec B404
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._path_scope import (
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources.base import SourceRun


__all__ = ["JUnitXmlSource"]


@dataclass
class JUnitXmlSource:
    """Read JUnit XML files from a directory.

    Each XML file → one `test_run` node + N `test_case`
    children + `test_run affects git_commit` edge if SHA is
    resolvable.
    """

    name: str = "junit_xml"
    profile: str = "discovery_crawl"
    drop_dir: Path | str = ""
    repo_root: Optional[Path | str] = None
    repo: str = "okg"
    sha: Optional[str] = None
    suite_name: str = "pytest"

    # Change-probe contract (#323): JUnit XML lands in a non-git drop dir, so
    # this falls to the O(content) floor over the *.xml files. When the SHA is
    # git-derived (repo_root set, sha unpinned) a HEAD move re-scopes the
    # emitted node ids, so the live HEAD is folded into the digest; with a
    # pinned sha it is omitted so a commit does not spuriously bust the token.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Any]]:
            drop = Path(self.drop_dir).expanduser().resolve()
            out: list[tuple[str, Any]] = []
            if drop.is_dir():
                out.extend((str(p), p) for p in sorted(drop.glob("*.xml")))
            if self.repo_root and not self.sha:
                out.append(
                    ("__head_sha__", (self._head_sha() or "").encode("utf-8"))
                )
            return out

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "drop_dir": str(self.drop_dir),
                "repo_root": str(self.repo_root) if self.repo_root else None,
                "sha": self.sha,
                "suite_name": self.suite_name,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        drop = Path(self.drop_dir).expanduser().resolve()
        if not drop.is_dir():
            raise SubstrateError(
                f"source {self.name!r}: drop_dir {drop} not found",
                data={"adapter": self.name, "drop_dir": str(drop)},
                fix_hint="point drop_dir at a directory containing "
                         "JUnit XML files",
            )
        scoped = path_scope_requested(sync_scope)
        xml_files = (
            self._scoped_xml_files(sync_scope, drop=drop)
            if scoped else sorted(drop.glob("*.xml"))
        )
        sha = self.sha or (self._head_sha() if self.repo_root else None)
        from okg.substrate.library.sources.test_reports import (
            TestReportSource,
        )

        repo_root = self.repo_root or Path.cwd()
        # Pass this source's name so an error raised by the wrapped reader
        # (for example SourceRootMissing) names the operator's junit_xml
        # source rather than the wrapper's default "test_reports".
        wrapped = TestReportSource(
            name=self.name,
            repo=self.repo,
            repo_root=repo_root,
            artifact_paths=xml_files,
            sha=sha,
            infer_sha=bool(self.repo_root),
            suite_name=self.suite_name,
        )
        report_run = wrapped.run(run_id, mode=mode, cursor=cursor)
        return SourceRun(
            facts=report_run.facts,
            completed_scope=(
                not scoped
                and mode in {"reconcile", "scope_complete", "release_new"}
            ),
            next_cursor=report_run.next_cursor,
            run_mode=mode,
        )

    def _scoped_xml_files(
        self,
        sync_scope: Optional[dict[str, Any]],
        *,
        drop: Path,
    ) -> list[Path]:
        roots = [drop]
        if self.repo_root is not None:
            roots.append(Path(self.repo_root).expanduser().resolve())
        candidates = resolve_scoped_paths(
            sync_scope,
            roots=roots,
            suffixes=[".xml"],
        )
        out = []
        for path in candidates:
            try:
                path.relative_to(drop)
            except ValueError:
                continue
            out.append(path)
        return sorted(set(out))

    def _head_sha(self) -> Optional[str]:
        if not self.repo_root:
            return None
        git = shutil.which("git")
        if git is None:
            return None
        try:
            proc = subprocess.run(
                [git, "-C", str(self.repo_root), "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=5,
            )  # nosec B603
            if proc.returncode == 0:
                return proc.stdout.strip() or None
        except (subprocess.TimeoutExpired, OSError):
            # Git metadata is optional for imported reports.
            pass
        return None

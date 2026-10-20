"""GitHub Actions and self-hosted runner source adapters.

The adapters use the GitHub CLI for live reads, matching
``github.GitHubSource``. Tests and air-gapped runs can pass fixture JSON
instead, so CI coverage does not depend on GitHub availability.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess  # nosec B404
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence
from urllib.parse import urlencode

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.github import GhCliMissing
from okg.substrate.support import redact_text


__all__ = ["GitHubActionsSource", "GitHubRunnerInventorySource"]


#: Payload key under which a live read reports how much of the run listing it
#: covered (issue #2390). Fixture payloads never carry it.
_LISTING_KEY = "_run_listing"


@dataclass
class GitHubActionsSource:
    """Import GitHub Actions run, job, check-run, and artifact metadata."""

    repo: str
    fixture_path: Optional[Path] = None
    branch: Optional[str] = None
    event: Optional[str] = None
    status: Optional[str] = None
    run_ids: Sequence[int | str] = ()
    run_limit: int = 20
    commit_repo: Optional[str] = None
    forge_host: str = "github"
    include_jobs: bool = True
    include_artifacts: bool = True
    include_check_runs: bool = True

    name: str = "github_actions"
    profile: str = "mutable_api"

    # Incremental-source contract (issue #323): a live repo's Actions feed is a
    # mutable upstream API, so the probe is a cheap high-water over the runs feed
    # (one `gh api .../actions/runs?per_page=1` call instead of the full crawl);
    # every new run / re-run / status transition bumps `updated_at`, the run id,
    # or the attempt. A fixture-backed source is local bytes -> the content-hash
    # floor (mirrors github.GitHubSource exactly). A `gh` failure / missing repo
    # RAISES -> run_probe forces a full read rather than a silent skip.
    change_probe_kind = "mutable_api"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Only fixture bytes provide complete live/deletion identity."""
        return self.fixture_path is not None

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
        )
        if self.fixture_path is not None:
            from okg.substrate.library.sources.content_hash_probe import (
                ContentHashProbe,
            )

            fixture = Path(self.fixture_path)

            def _items() -> list[tuple[str, Path]]:
                return [(str(fixture), fixture)] if fixture.is_file() else []

            return ContentHashProbe(
                content_items=_items,
                config={
                    "fixture_path": str(self.fixture_path),
                    "repo": self.repo,
                    "branch": self.branch,
                    "event": self.event,
                    "status": self.status,
                    "run_ids": list(self.run_ids),
                    "run_limit": self.run_limit,
                    "commit_repo": self.commit_repo,
                    "forge_host": self.forge_host,
                    "include_jobs": self.include_jobs,
                    "include_artifacts": self.include_artifacts,
                    "include_check_runs": self.include_check_runs,
                },
                emit_targets=emit_targets,
            )

        from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe

        return MutableApiProbe(
            version_fn=self._runs_updated_watermark,
            config={
                "repo": self.repo,
                "branch": self.branch,
                "event": self.event,
                "status": self.status,
                "run_ids": list(self.run_ids),
                "run_limit": self.run_limit,
                "commit_repo": self.commit_repo,
                "forge_host": self.forge_host,
                "include_jobs": self.include_jobs,
                "include_artifacts": self.include_artifacts,
                "include_check_runs": self.include_check_runs,
            },
            emit_targets=emit_targets,
            content_kind="updated_at",
        )

    def _runs_updated_watermark(self) -> str:
        """Cheap high-water over the Actions runs feed.

        One non-paginated `gh api` call (most-recently-updated run) instead of
        the full crawl. Raises (no repo / `gh` missing / API error) so the
        run_probe boundary forces a full read rather than a silent skip.
        """
        if self.repo is None:
            raise SubstrateError(
                f"source {self.name!r}: change-probe needs repo or fixture_path",
                fix_hint="repo='owner/name' for live, fixture_path=Path(...) for tests",
            )
        doc = _gh_api_json(f"/repos/{self.repo}/actions/runs?per_page=1")
        runs = _list_from_doc(doc, "workflow_runs")
        if not runs:
            return "empty"
        top = runs[0]
        # `updated_at` catches status/conclusion transitions; the run id catches a
        # brand-new run, and the attempt catches a re-run that re-uses the id.
        return f"{top.get('updated_at')}:{top.get('id')}:{top.get('run_attempt')}"

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        del cursor
        applied_raw_token: dict[str, Any] | None = None
        listing: dict[str, Any] | None = None
        if self.fixture_path:
            fixture = Path(self.fixture_path)
            raw_bytes = fixture.read_bytes()
            payload = self._load_fixture(raw_bytes=raw_bytes)
            applied_raw_token = self.change_probe.token_from_item_hashes(
                {
                    str(fixture): hashlib.sha256(raw_bytes).hexdigest(),
                }
            )
            # A fixture is the whole configured scope by construction.
            read_whole_scope = True
        else:
            payload = self._fetch_live()
            reported = payload.get(_LISTING_KEY)
            # A live payload that does not say how much of the listing it
            # covered proves nothing about the rest: read it as incomplete.
            listing = (
                dict(reported)
                if isinstance(reported, Mapping)
                else {"complete": False, "reason": "listing_unreported"}
            )
            read_whole_scope = listing.get("complete") is True
        facts, latest_updated_at = self._normalize(payload)
        # Issue #2390. A live read lists only the newest `run_limit` runs.
        # Claiming a completed scope for that window let every reconcile
        # retract each run older than it. Only a read that reached the end of
        # the listing may claim the whole scope; a truncated read carries no
        # deletion authority, and the runner keeps the prior footprint.
        completed_scope = read_whole_scope and mode in {
            "reconcile",
            "scope_complete",
            "release_new",
        }
        record_set = (
            _record_set_from_facts(facts)
            if completed_scope and mode in {"reconcile", "scope_complete"}
            else None
        )
        next_cursor: dict[str, Any] = {
            "run_id": run_id,
            "latest_updated_at": latest_updated_at,
            "run_count": sum(
                1
                for fact in facts
                if isinstance(fact, NodeFact) and fact.subtype == "ci_workflow"
            ),
        }
        if listing is not None:
            next_cursor["run_listing"] = listing

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from facts

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            applied_raw_token=applied_raw_token,
            applied_token_scope=(
                "global" if applied_raw_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_raw_token is not None else "unavailable"
            ),
            record_set=record_set,
            run_mode=mode,
        )

    def _load_fixture(
        self,
        *,
        raw_bytes: bytes | None = None,
    ) -> dict[str, Any]:
        path = Path(self.fixture_path)  # type: ignore[arg-type]
        if not path.is_file():
            raise SubstrateError(
                f"source {self.name!r}: fixture_path {path} not found",
                data={"adapter": self.name, "fixture_path": str(path)},
            )
        doc = json.loads(
            raw_bytes.decode("utf-8")
            if raw_bytes is not None
            else path.read_text(encoding="utf-8")
        )
        if not isinstance(doc, dict):
            raise SubstrateError(
                f"source {self.name!r}: fixture_path must contain an object",
                data={"adapter": self.name},
            )
        return doc

    def _fetch_live(self) -> dict[str, Any]:
        run_ids = [str(item) for item in self.run_ids]
        listing: dict[str, Any]
        if run_ids:
            runs = [
                _gh_api_json(f"/repos/{self.repo}/actions/runs/{run_id}")
                for run_id in run_ids
            ]
            # The configured scope is exactly these run ids, all read.
            listing = {"kind": "run_ids", "complete": True}
        else:
            query: dict[str, int | str] = {
                "per_page": max(1, min(int(self.run_limit or 20), 100)),
            }
            if self.branch:
                query["branch"] = self.branch
            if self.event:
                query["event"] = self.event
            if self.status:
                query["status"] = self.status
            doc = _gh_api_json(f"/repos/{self.repo}/actions/runs?{urlencode(query)}")
            runs = _list_from_doc(doc, "workflow_runs")
            if self.run_limit:
                runs = runs[: max(1, int(self.run_limit))]
            listing = _run_listing_state(doc, read_count=len(runs))

        jobs_by_run_id: dict[str, list[dict[str, Any]]] = {}
        artifacts_by_run_id: dict[str, list[dict[str, Any]]] = {}
        check_runs_by_sha: dict[str, list[dict[str, Any]]] = {}
        for run in runs:
            rid = str(run.get("id") or "")
            if not rid:
                continue
            if self.include_jobs:
                jobs_doc = _gh_api_json(
                    f"/repos/{self.repo}/actions/runs/{rid}/jobs?per_page=100"
                )
                jobs_by_run_id[rid] = _list_from_doc(jobs_doc, "jobs")
            if self.include_artifacts:
                artifacts_doc = _gh_api_json(
                    f"/repos/{self.repo}/actions/runs/{rid}/artifacts?per_page=100"
                )
                artifacts_by_run_id[rid] = _list_from_doc(artifacts_doc, "artifacts")
            sha = str(run.get("head_sha") or "")
            if sha and self.include_check_runs and sha not in check_runs_by_sha:
                checks_doc = _gh_api_json(
                    f"/repos/{self.repo}/commits/{sha}/check-runs?per_page=100"
                )
                check_runs_by_sha[sha] = _list_from_doc(checks_doc, "check_runs")

        return {
            "workflow_runs": runs,
            "jobs_by_run_id": jobs_by_run_id,
            "artifacts_by_run_id": artifacts_by_run_id,
            "check_runs_by_sha": check_runs_by_sha,
            _LISTING_KEY: listing,
        }

    def _normalize(
        self,
        payload: Mapping[str, Any],
    ) -> tuple[list[NodeFact | EdgeFact], str | None]:
        repo_node = _repo_node(self.forge_host, self.repo)
        commit_repo = self.commit_repo or self.repo.rsplit("/", 1)[-1]
        runs = _list_from_payload(payload, "workflow_runs", "runs")
        jobs_by_run = _mapping_of_lists(payload.get("jobs_by_run_id"))
        artifacts_by_run = _mapping_of_lists(payload.get("artifacts_by_run_id"))
        checks_by_sha = _mapping_of_lists(payload.get("check_runs_by_sha"))
        facts: list[NodeFact | EdgeFact] = [
            _repo_fact(repo_node, self.forge_host, self.repo)
        ]
        latest_updated_at: str | None = None

        for run in runs:
            workflow_id = _workflow_node_id(self.forge_host, self.repo, run)
            if not workflow_id:
                continue
            rid = str(run.get("id"))
            attempt = _int_or_none(run.get("run_attempt")) or 1
            sha = str(run.get("head_sha") or "")
            updated_at = _str_or_none(run.get("updated_at"))
            workflow_observed_at = updated_at or _str_or_none(run.get("created_at"))
            latest_updated_at = _max_iso(latest_updated_at, updated_at)
            workflow_revision = {
                "repo": self.repo,
                "run_id": rid,
                "run_attempt": attempt,
                "updated_at": updated_at or "",
                "head_sha": sha,
            }
            workflow = NodeFact(
                node_id=workflow_id,
                subtype="ci_workflow",
                attrs={
                    "workflow_id": workflow_id,
                    "run_id": _int_or_none(run.get("id")),
                    "run_attempt": attempt,
                    "name": _redacted(
                        run.get("name") or run.get("display_title") or "GitHub Actions"
                    ),
                    "display_title": _redacted(run.get("display_title") or ""),
                    "status": str(run.get("status") or ""),
                    "conclusion": str(run.get("conclusion") or ""),
                    "event": str(run.get("event") or ""),
                    "head_branch": str(run.get("head_branch") or ""),
                    "sha": sha,
                    "url": str(run.get("html_url") or run.get("url") or ""),
                    "created_at": _str_or_none(run.get("created_at")),
                    "updated_at": updated_at,
                    **(
                        {"observed_at": workflow_observed_at}
                        if workflow_observed_at
                        else {}
                    ),
                    "forge": self.forge_host,
                    "repo": self.repo,
                    "text": " ".join(
                        part
                        for part in [
                            _redacted(run.get("name") or ""),
                            _redacted(run.get("display_title") or ""),
                            str(run.get("status") or ""),
                            str(run.get("conclusion") or ""),
                        ]
                        if part
                    ),
                },
                source_record_id={
                    "repo": self.repo,
                    "run_id": rid,
                    "run_attempt": attempt,
                },
                source_revision=workflow_revision,
            )
            facts.append(workflow)
            facts.append(
                _edge(
                    repo_node,
                    workflow_id,
                    "contains",
                    {"repo": self.repo, "run_id": rid},
                    workflow.source_record_id,
                    workflow_revision,
                )
            )
            if sha:
                facts.append(
                    _edge(
                        workflow_id,
                        f"commit:{commit_repo}:{sha}",
                        "affects",
                        {"sha": sha, "run_id": rid},
                        workflow.source_record_id,
                        workflow_revision,
                    )
                )
            for pr_number in _pull_request_numbers(run):
                facts.append(
                    _edge(
                        workflow_id,
                        f"pr:{self.forge_host}:{self.repo}:#{pr_number}",
                        "references",
                        {"repo": self.repo, "number": pr_number, "run_id": rid},
                        {
                            "repo": self.repo,
                            "run_id": rid,
                            "pull_request": pr_number,
                        },
                        workflow_revision,
                    )
                )

            jobs = jobs_by_run.get(rid, [])
            checks = checks_by_sha.get(sha, [])
            checks_by_id = {
                str(check.get("id")): check
                for check in checks
                if check.get("id") is not None
            }
            job_nodes_by_normalized_name: dict[str, str] = {}
            for job in jobs:
                job_node = _job_node_id(self.forge_host, self.repo, job, attempt)
                if not job_node:
                    continue
                job_revision = {
                    "repo": self.repo,
                    "run_id": rid,
                    "job_id": str(job.get("id") or ""),
                    "run_attempt": _int_or_none(job.get("run_attempt")) or attempt,
                    "completed_at": _str_or_none(job.get("completed_at")) or "",
                }
                runner_id = _int_or_none(job.get("runner_id"))
                labels = _labels(job.get("labels"))
                started = _str_or_none(job.get("started_at"))
                completed = _str_or_none(job.get("completed_at"))
                job_observed_at = completed or started
                job_fact = NodeFact(
                    node_id=job_node,
                    subtype="ci_job",
                    attrs={
                        "job_id": job_node,
                        "github_job_id": _int_or_none(job.get("id")),
                        "workflow_id": workflow_id,
                        "run_id": _int_or_none(job.get("run_id")) or _int_or_none(rid),
                        "run_attempt": _int_or_none(job.get("run_attempt")) or attempt,
                        "name": _redacted(job.get("name") or "GitHub Actions job"),
                        "status": str(job.get("status") or ""),
                        "conclusion": str(job.get("conclusion") or ""),
                        "duration_s": _duration_seconds(started, completed),
                        "sha": str(job.get("head_sha") or sha),
                        "url": str(job.get("html_url") or ""),
                        "started_at": started,
                        "finished_at": completed,
                        "created_at": started,
                        **({"observed_at": job_observed_at} if job_observed_at else {}),
                        "runner_id": runner_id,
                        "runner_name": _redacted(job.get("runner_name") or ""),
                        "runner_group_name": _redacted(
                            job.get("runner_group_name") or ""
                        ),
                        "labels": labels,
                        "text": " ".join(
                            part
                            for part in [
                                _redacted(job.get("name") or ""),
                                str(job.get("status") or ""),
                                str(job.get("conclusion") or ""),
                                " ".join(labels),
                            ]
                            if part
                        ),
                    },
                    source_record_id={
                        "repo": self.repo,
                        "run_id": rid,
                        "job_id": str(job.get("id") or ""),
                    },
                    source_revision=job_revision,
                )
                facts.append(job_fact)
                facts.append(
                    _edge(
                        workflow_id,
                        job_node,
                        "contains",
                        {"run_id": rid, "job_id": job.get("id")},
                        job_fact.source_record_id,
                        job_revision,
                    )
                )
                normalized_name = _normalize_artifact_or_job_name(job.get("name"))
                if normalized_name:
                    job_nodes_by_normalized_name[normalized_name] = job_node
                check_id = _check_id_from_job(job)
                if check_id and check_id in checks_by_id:
                    facts.append(
                        _edge(
                            job_node,
                            _check_node_id(
                                self.forge_host, self.repo, checks_by_id[check_id]
                            ),
                            "references",
                            {"relation": "github_check_run"},
                            {
                                "repo": self.repo,
                                "job_id": str(job.get("id") or ""),
                                "check_run_id": check_id,
                            },
                            job_revision,
                        )
                    )
                if runner_id is not None:
                    facts.append(
                        _edge(
                            job_node,
                            _runner_node_id(
                                self.forge_host, "repo", self.repo, runner_id
                            ),
                            "references",
                            {
                                "relation": "runner",
                                "runner_name": job_fact.attrs["runner_name"],
                            },
                            job_fact.source_record_id,
                            job_revision,
                        )
                    )

            for check in checks:
                check_node = _check_node_id(self.forge_host, self.repo, check)
                if not check_node:
                    continue
                check_run_id = _int_or_none(check.get("id"))
                check_revision = {
                    "repo": self.repo,
                    "check_run_id": str(check.get("id") or ""),
                    "completed_at": _str_or_none(check.get("completed_at")) or "",
                    "head_sha": sha,
                }
                check_fact = NodeFact(
                    node_id=check_node,
                    subtype="ci_check_run",
                    attrs={
                        "check_run_id": check_run_id,
                        "repo": self.repo,
                        "name": _redacted(check.get("name") or "GitHub check run"),
                        "status": str(check.get("status") or ""),
                        "conclusion": str(check.get("conclusion") or ""),
                        "head_sha": sha,
                        "started_at": _str_or_none(check.get("started_at")),
                        "completed_at": _str_or_none(check.get("completed_at")),
                        "url": str(check.get("url") or ""),
                        "html_url": str(check.get("html_url") or ""),
                        "details_url": str(check.get("details_url") or ""),
                        "app_slug": str(
                            ((check.get("app") or {}) or {}).get("slug") or ""
                        ),
                        "workflow_run_id": _int_or_none(rid),
                        "job_id": _job_id_from_check(check),
                        "text": " ".join(
                            part
                            for part in [
                                _redacted(check.get("name") or ""),
                                str(check.get("status") or ""),
                                str(check.get("conclusion") or ""),
                            ]
                            if part
                        ),
                    },
                    source_record_id={
                        "repo": self.repo,
                        "check_run_id": str(check.get("id") or ""),
                    },
                    source_revision=check_revision,
                )
                facts.append(check_fact)
                facts.append(
                    _edge(
                        workflow_id,
                        check_node,
                        "references",
                        {"relation": "github_check_run"},
                        check_fact.source_record_id,
                        check_revision,
                    )
                )

            for artifact in artifacts_by_run.get(rid, []):
                artifact_node = _artifact_node_id(self.forge_host, self.repo, artifact)
                if not artifact_node:
                    continue
                artifact_revision = {
                    "repo": self.repo,
                    "artifact_id": str(artifact.get("id") or ""),
                    "updated_at": _str_or_none(artifact.get("updated_at")) or "",
                    "run_id": rid,
                }
                content_kind = _artifact_content_kind(artifact.get("name"))
                artifact_observed_at = _str_or_none(
                    artifact.get("updated_at")
                ) or _str_or_none(artifact.get("created_at"))
                artifact_fact = NodeFact(
                    node_id=artifact_node,
                    subtype="ci_artifact",
                    attrs={
                        "artifact_id": _int_or_none(artifact.get("id")),
                        "repo": self.repo,
                        "run_id": _int_or_none(rid),
                        "job_id": None,
                        "name": _redacted(artifact.get("name") or "GitHub artifact"),
                        "path": str(artifact.get("name") or ""),
                        "size_in_bytes": _int_or_none(artifact.get("size_in_bytes")),
                        "archive_download_url": str(
                            artifact.get("archive_download_url") or ""
                        ),
                        "url": str(artifact.get("url") or ""),
                        "expired": bool(artifact.get("expired")),
                        "expires_at": _str_or_none(artifact.get("expires_at")),
                        "artifact_hash": str(artifact.get("node_id") or ""),
                        "content_kind": content_kind,
                        **(
                            {"observed_at": artifact_observed_at}
                            if artifact_observed_at
                            else {}
                        ),
                        "created_at": _str_or_none(artifact.get("created_at")),
                        "updated_at": _str_or_none(artifact.get("updated_at")),
                        "text": f"{_redacted(artifact.get('name') or '')} {content_kind}".strip(),
                    },
                    source_record_id={
                        "repo": self.repo,
                        "artifact_id": str(artifact.get("id") or ""),
                    },
                    source_revision=artifact_revision,
                )
                facts.append(artifact_fact)
                facts.append(
                    _edge(
                        artifact_node,
                        workflow_id,
                        "references",
                        {"relation": "workflow_run"},
                        artifact_fact.source_record_id,
                        artifact_revision,
                    )
                )
                matched_job = job_nodes_by_normalized_name.get(
                    _normalize_artifact_or_job_name(artifact.get("name"))
                )
                if matched_job:
                    facts.append(
                        _edge(
                            matched_job,
                            artifact_node,
                            "produces",
                            {"content_kind": content_kind},
                            artifact_fact.source_record_id,
                            artifact_revision,
                        )
                    )

        return _dedupe_facts(facts), latest_updated_at


@dataclass
class GitHubRunnerInventorySource:
    """Import the current GitHub self-hosted runner inventory snapshot."""

    repo: Optional[str] = None
    org: Optional[str] = None
    fixture_path: Optional[Path] = None
    scope: str = "repo"
    forge_host: str = "github"
    observed_at: Optional[str] = None

    name: str = "github_runner_inventory"
    profile: str = "mutable_api"
    probe_short_circuit_safe = True

    # Incremental-source contract (issue #323): the runner inventory is a mutable
    # upstream API snapshot. The probe digests the CURRENT runner set — a sorted
    # list of (runner_id, status, busy, version, sorted(labels)) PLUS the count —
    # so a registration, DEREGISTRATION, or status/busy flip ALL bust the token
    # (a max-id watermark would MISS a deregistration: a silent false-unchanged).
    # A fixture-backed source is local bytes -> the content-hash floor. A `gh`
    # failure / missing scope RAISES -> run_probe forces a full read.
    change_probe_kind = "mutable_api"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
        )
        if self.fixture_path is not None:
            from okg.substrate.library.sources.content_hash_probe import (
                ContentHashProbe,
            )

            fixture = Path(self.fixture_path)

            def _items() -> list[tuple[str, Path]]:
                return [(str(fixture), fixture)] if fixture.is_file() else []

            return ContentHashProbe(
                content_items=_items,
                config={
                    "fixture_path": str(self.fixture_path),
                    "repo": self.repo,
                    "org": self.org,
                    "scope": self.scope,
                    "forge_host": self.forge_host,
                },
                emit_targets=emit_targets,
            )

        from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe

        return MutableApiProbe(
            version_fn=self._runner_state_digest,
            config={
                "repo": self.repo,
                "org": self.org,
                "scope": self.scope,
                "forge_host": self.forge_host,
            },
            emit_targets=emit_targets,
            content_kind="version_token",
        )

    def _runner_state_digest(
        self,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> str:
        """Membership-and-state digest over the CURRENT runner inventory.

        Hashes a canonical sorted list of (runner_id, status, busy, version,
        sorted(labels)) PLUS the count, so a runner ADD, DELETE, or status/busy
        flip ALL move the token. A max-id watermark would miss a deregistration
        (the runner that vanished still has the lower id) — a silent
        false-unchanged. ``_fetch_live`` raises (no scope / `gh` missing / API
        error) so the run_probe boundary forces a full read.
        """
        payload = payload if payload is not None else self._fetch_live()
        runners = _list_from_payload(payload, "runners")
        rows = sorted(
            (
                _runner_identity_projection(runner)
                for runner in runners
                if _int_or_none(runner.get("id")) is not None
            ),
            key=lambda row: json.dumps(row, sort_keys=True),
        )
        digest = hashlib.sha256()
        digest.update(f"n={len(rows)}".encode("utf-8"))
        for row in rows:
            digest.update(b"\x00")
            digest.update(json.dumps(row, sort_keys=True).encode("utf-8"))
        return digest.hexdigest()

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        del cursor
        if self.fixture_path:
            fixture = Path(self.fixture_path)
            raw_bytes = fixture.read_bytes()
            payload = self._load_fixture(raw_bytes=raw_bytes)
            applied_raw_token = self.change_probe.token_from_item_hashes(
                {
                    str(fixture): hashlib.sha256(raw_bytes).hexdigest(),
                }
            )
        else:
            payload = self._fetch_live()
            applied_raw_token = self.change_probe.token_for_version(
                self._runner_state_digest(payload)
            )
        facts = self._normalize(payload)
        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = (
            _record_set_from_facts(facts)
            if mode in {"reconcile", "scope_complete"}
            else None
        )

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from facts

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            next_cursor={
                "run_id": run_id,
                "observed_at": self.observed_at or _iso_now(),
                "runner_count": sum(
                    1
                    for fact in facts
                    if isinstance(fact, NodeFact) and fact.subtype == "ci_runner"
                ),
            },
            applied_raw_token=applied_raw_token,
            applied_token_scope="global",
            applied_token_authority="exact",
            record_set=record_set,
            run_mode=mode,
        )

    def _load_fixture(
        self,
        *,
        raw_bytes: bytes | None = None,
    ) -> dict[str, Any]:
        path = Path(self.fixture_path)  # type: ignore[arg-type]
        if not path.is_file():
            raise SubstrateError(
                f"source {self.name!r}: fixture_path {path} not found",
                data={"adapter": self.name, "fixture_path": str(path)},
            )
        doc = json.loads(
            raw_bytes.decode("utf-8")
            if raw_bytes is not None
            else path.read_text(encoding="utf-8")
        )
        if isinstance(doc, list):
            return {"runners": doc}
        if isinstance(doc, dict):
            return doc
        raise SubstrateError(
            f"source {self.name!r}: fixture_path must contain runner JSON",
            data={"adapter": self.name},
        )

    def _fetch_live(self) -> dict[str, Any]:
        if self.scope == "org" or (self.org and not self.repo):
            if not self.org:
                raise SubstrateError(
                    "GitHubRunnerInventorySource scope='org' requires org",
                    data={"adapter": self.name},
                )
            path = f"/orgs/{self.org}/actions/runners?per_page=100"
            return _gh_api_paginated_list(path, "runners")
        if not self.repo:
            raise SubstrateError(
                "GitHubRunnerInventorySource requires repo or org",
                data={"adapter": self.name},
            )
        path = f"/repos/{self.repo}/actions/runners?per_page=100"
        return _gh_api_paginated_list(path, "runners")

    def _normalize(self, payload: Mapping[str, Any]) -> list[NodeFact | EdgeFact]:
        runners = _list_from_payload(payload, "runners")
        scope = "org" if self.scope == "org" or (self.org and not self.repo) else "repo"
        scope_value = self.org if scope == "org" else self.repo
        if not scope_value:
            scope_value = "unknown"
        facts: list[NodeFact | EdgeFact] = []
        repo_node = _repo_node(self.forge_host, self.repo) if self.repo else None
        if repo_node and self.repo:
            facts.append(_repo_fact(repo_node, self.forge_host, self.repo))
        for runner in runners:
            runner_id = _int_or_none(runner.get("id"))
            if runner_id is None:
                continue
            projection = _runner_identity_projection(runner)
            labels = list(projection["labels"])
            runner_node = _runner_node_id(
                self.forge_host,
                scope,
                str(scope_value),
                runner_id,
            )
            revision = {
                "scope": scope,
                "scope_value": str(scope_value),
                "runner_id": str(runner_id),
                "state_sha256": hashlib.sha256(
                    json.dumps(
                        projection,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            }
            provider = str(projection["provider"])
            runner_fact = NodeFact(
                node_id=runner_node,
                subtype="ci_runner",
                attrs={
                    "runner_id": runner_id,
                    "runner_name": projection["name"],
                    "scope": scope,
                    "scope_value": str(scope_value),
                    "runner_group_id": projection["group_id"],
                    "runner_group_name": projection["group_name"],
                    "os": projection["os"],
                    "arch": projection["arch"],
                    "status": projection["status"],
                    "busy": projection["busy"],
                    "ephemeral": projection["ephemeral"],
                    "labels": labels,
                    "provider": provider,
                    "pool": projection["pool"],
                    "image_id": projection["image_id"],
                    "version": projection["version"],
                    "text": " ".join(
                        part
                        for part in [
                            str(projection["name"]),
                            str(projection["os"]),
                            str(projection["status"]),
                            " ".join(labels),
                        ]
                        if part
                    ),
                },
                source_record_id={
                    "scope": scope,
                    "scope_value": str(scope_value),
                    "runner_id": runner_id,
                },
                source_revision=revision,
            )
            facts.append(runner_fact)
            if repo_node:
                facts.append(
                    _edge(
                        repo_node,
                        runner_node,
                        "contains",
                        {"scope": scope, "provider": provider},
                        runner_fact.source_record_id,
                        revision,
                    )
                )
        return _dedupe_facts(facts)


def _runner_identity_projection(runner: Mapping[str, Any]) -> dict[str, Any]:
    """Canonical projection shared by runner facts and their change token."""
    labels = sorted(_labels(runner.get("labels")))
    return {
        "id": _int_or_none(runner.get("id")),
        "name": _redacted(runner.get("name") or ""),
        "group_id": _int_or_none(runner.get("runner_group_id")),
        "group_name": _redacted(runner.get("runner_group_name") or ""),
        "status": str(runner.get("status") or ""),
        "busy": bool(runner.get("busy")),
        "version": str(runner.get("version") or ""),
        "labels": labels,
        "os": str(runner.get("os") or ""),
        "arch": _arch_from_labels(labels),
        "ephemeral": "ephemeral" in {label.lower() for label in labels},
        "provider": _provider_from_labels(labels),
        "pool": _pool_from_labels(labels),
        "image_id": str(runner.get("image_id") or ""),
    }


def _gh_api_json(path: str) -> dict[str, Any]:
    gh = shutil.which("gh")
    if gh is None:
        raise GhCliMissing(
            "gh binary not on PATH",
            fix_hint="install `gh`, then run `gh auth login` or configure a token",
        )
    proc = subprocess.run(
        [gh, "api", "-H", "Accept: application/vnd.github+json", path],
        capture_output=True,
        text=True,
    )  # nosec B603
    if proc.returncode != 0:
        raise SubstrateError(
            f"`gh api` failed for {path!r}: {proc.stderr.strip()[:300]}",
            data={"returncode": proc.returncode, "path": path},
            fix_hint="check `gh auth status`, repository permissions, and rate limits",
        )
    parsed = json.loads(proc.stdout or "{}")
    if not isinstance(parsed, dict):
        raise SubstrateError(
            f"`gh api` returned non-object JSON for {path!r}",
            data={"path": path},
        )
    return parsed


def _gh_api_paginated_list(path: str, list_key: str) -> dict[str, Any]:
    """Fetch every REST page and fail unless GitHub's total count matches."""
    gh = shutil.which("gh")
    if gh is None:
        raise GhCliMissing(
            "gh binary not on PATH",
            fix_hint="install `gh`, then run `gh auth login` or configure a token",
        )
    proc = subprocess.run(
        [
            gh,
            "api",
            "--paginate",
            "--slurp",
            "-H",
            "Accept: application/vnd.github+json",
            path,
        ],
        capture_output=True,
        text=True,
    )  # nosec B603
    if proc.returncode != 0:
        raise SubstrateError(
            f"paginated `gh api` failed for {path!r}: {proc.stderr.strip()[:300]}",
            data={"returncode": proc.returncode, "path": path},
        )
    try:
        pages = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise SubstrateError(
            f"paginated `gh api` returned invalid JSON for {path!r}",
            data={"path": path},
        ) from exc
    if (
        not isinstance(pages, list)
        or not pages
        or any(not isinstance(page, Mapping) for page in pages)
    ):
        raise SubstrateError(
            f"paginated `gh api` returned invalid pages for {path!r}",
            data={"path": path},
        )
    expected = pages[0].get("total_count")
    items = [
        dict(item)
        for page in pages
        for item in page.get(list_key, [])
        if isinstance(item, Mapping)
    ]
    if not isinstance(expected, int) or len(items) != expected:
        raise SubstrateError(
            f"paginated `gh api` was incomplete for {path!r}",
            data={"path": path, "expected": expected, "observed": len(items)},
        )
    return {"total_count": expected, list_key: items}


def _repo_node(forge_host: str, repo: Optional[str]) -> str:
    return f"repo:{forge_host}:{repo or 'unknown/unknown'}"


def _repo_fact(node_id: str, forge_host: str, repo: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="repo",
        attrs={
            "repo_id": node_id,
            "forge": forge_host,
            "slug": repo,
            "url": f"https://{forge_host}.com/{repo}",
        },
        source_record_id={"repo": repo},
        source_revision={"forge": forge_host, "repo": repo},
    )


def _workflow_node_id(forge_host: str, repo: str, run: Mapping[str, Any]) -> str:
    rid = run.get("id")
    if rid is None:
        return ""
    attempt = _int_or_none(run.get("run_attempt")) or 1
    return f"ci-workflow:{forge_host}:{repo}:run:{rid}:attempt:{attempt}"


def _job_node_id(
    forge_host: str,
    repo: str,
    job: Mapping[str, Any],
    run_attempt: int,
) -> str:
    jid = job.get("id")
    if jid is None:
        return ""
    attempt = _int_or_none(job.get("run_attempt")) or run_attempt
    return f"ci-job:{forge_host}:{repo}:job:{jid}:attempt:{attempt}"


def _check_node_id(forge_host: str, repo: str, check: Mapping[str, Any]) -> str:
    cid = check.get("id")
    if cid is None:
        return ""
    return f"ci-check-run:{forge_host}:{repo}:{cid}"


def _artifact_node_id(forge_host: str, repo: str, artifact: Mapping[str, Any]) -> str:
    aid = artifact.get("id")
    if aid is None:
        return ""
    return f"ci-artifact:{forge_host}:{repo}:{aid}"


def _runner_node_id(
    forge_host: str,
    scope: str,
    scope_value: str,
    runner_id: int,
) -> str:
    return f"ci-runner:{forge_host}:{scope}:{scope_value}:{runner_id}"


def _edge(
    src: str,
    dst: str,
    edge_type: str,
    attrs: dict[str, Any],
    source_record_id: dict[str, Any],
    source_revision: dict[str, Any],
) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        attrs=attrs,
        source_record_id=source_record_id,
        source_revision=source_revision,
    )


def _record_set_from_facts(facts: Sequence[NodeFact | EdgeFact]) -> RecordSet:
    records: dict[str, RecordEmission] = {}
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}
    for node in nodes.values():
        records[node.node_id] = RecordEmission(
            primary=RecordNode(
                node_id=node.node_id,
                subtype=node.subtype,
                source_record_id=node.source_record_id,
                source_revision=node.source_revision,
            )
        )
    for edge in (fact for fact in facts if isinstance(fact, EdgeFact)):
        record_key = edge.src if edge.src in nodes else edge.dst
        records.setdefault(record_key, RecordEmission()).edges.append(
            EdgeKey(
                src=edge.src,
                dst=edge.dst,
                edge_type=edge.edge_type,
                attrs=edge.attrs,
                provenance=edge.provenance,
                confidence=edge.confidence,
                source_record_id=edge.source_record_id,
                source_revision=edge.source_revision,
            )
        )
    return RecordSet(records=records)


def _run_listing_state(doc: Mapping[str, Any], *, read_count: int) -> dict[str, Any]:
    """Report whether one page of the runs listing covered the whole listing.

    Issue #2390. The read fetches a single page of at most ``run_limit``
    runs, newest first. It covers the whole configured scope only when
    GitHub's ``total_count`` for the same filters is no larger than the runs
    read. A missing or malformed ``total_count`` proves nothing, so the read
    is reported incomplete and carries no deletion authority.
    """
    total = doc.get("total_count")
    if isinstance(total, bool) or not isinstance(total, int):
        return {
            "kind": "newest_runs",
            "complete": False,
            "read_count": read_count,
            "total_count": None,
            "reason": "total_count_unavailable",
        }
    complete = total <= read_count
    return {
        "kind": "newest_runs",
        "complete": complete,
        "read_count": read_count,
        "total_count": total,
        "reason": "listing_exhausted" if complete else "listing_truncated",
    }


def _list_from_doc(doc: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    return [item for item in (doc.get(key) or []) if isinstance(item, dict)]


def _list_from_payload(
    payload: Mapping[str, Any],
    *keys: str,
) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _mapping_of_lists(raw: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(raw, Mapping):
        return {}
    return {
        str(key): [item for item in (value or []) if isinstance(item, dict)]
        for key, value in raw.items()
        if isinstance(value, list)
    }


def _labels(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    labels: list[str] = []
    for item in raw:
        if isinstance(item, Mapping):
            name = item.get("name")
        else:
            name = item
        if name:
            labels.append(str(name))
    return labels


def _pull_request_numbers(run: Mapping[str, Any]) -> list[int]:
    out: list[int] = []
    for item in run.get("pull_requests") or []:
        if not isinstance(item, Mapping):
            continue
        number = _int_or_none(item.get("number"))
        if number is not None:
            out.append(number)
    return out


def _check_id_from_job(job: Mapping[str, Any]) -> str | None:
    url = str(job.get("check_run_url") or "")
    match = re.search(r"/check-runs/(\d+)", url)
    return match.group(1) if match else None


def _job_id_from_check(check: Mapping[str, Any]) -> int | None:
    url = str(check.get("details_url") or check.get("html_url") or "")
    match = re.search(r"/job/(\d+)", url)
    return _int_or_none(match.group(1)) if match else None


def _artifact_content_kind(name: Any) -> str:
    normalized = str(name or "").lower()
    if "coverage" in normalized:
        return "coverage"
    if "junit" in normalized or "pytest" in normalized:
        return "junit"
    if "runner" in normalized:
        return "runner_metadata"
    if "playwright" in normalized or "viz" in normalized or "console" in normalized:
        return "ui_report"
    return "ci_artifact"


def _normalize_artifact_or_job_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("ci-"):
        text = text[3:]
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def _provider_from_labels(labels: Sequence[str]) -> str:
    lowered = {label.lower() for label in labels}
    if "openstack" in lowered:
        return "openstack"
    if "github-hosted" in lowered:
        return "github-hosted"
    return ""


def _pool_from_labels(labels: Sequence[str]) -> str:
    lowered = {label.lower() for label in labels}
    if {"openstack", "linux", "pr-safe"} <= lowered:
        return "okg-openstack-linux-pr"
    if {"openstack", "windows", "pr-safe"} <= lowered:
        return "okg-openstack-windows-pr"
    return ""


def _arch_from_labels(labels: Sequence[str]) -> str:
    lowered = {label.lower() for label in labels}
    if "x64" in lowered:
        return "x64"
    if "arm64" in lowered:
        return "arm64"
    return ""


def _duration_seconds(started: str | None, completed: str | None) -> float | None:
    if not started or not completed:
        return None
    try:
        start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
        done_dt = datetime.fromisoformat(completed.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0.0, (done_dt - start_dt).total_seconds())


def _iso_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _max_iso(left: str | None, right: str | None) -> str | None:
    if not right:
        return left
    if not left:
        return right
    return max(left, right)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _redacted(value: Any) -> str:
    if value is None:
        return ""
    redacted, _markers = redact_text(str(value))
    return redacted


def _dedupe_facts(facts: Sequence[NodeFact | EdgeFact]) -> list[NodeFact | EdgeFact]:
    seen: set[tuple[Any, ...]] = set()
    out: list[NodeFact | EdgeFact] = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            key = ("node", fact.node_id, fact.subtype, fact.op)
        else:
            key = ("edge", fact.src, fact.edge_type, fact.dst, fact.op)
        if key in seen:
            continue
        seen.add(key)
        out.append(fact)
    return out

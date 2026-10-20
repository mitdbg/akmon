"""Full-retraction guard and failed-repository reporting (issue #2818).

A completed-scope run that would retract everything a source (or one of its
per-record sub-scopes) published while emitting nothing for it is refused
before any write, unless the operator passes
``okg ingest --allow-full-retraction <source>`` for that run.

The refusal tests import nothing the guard adds, so on a checkout without it
they fail on behavior (the run does not raise, and the rows are retracted).
A failed external repository sub-run is reported as a failed run, never as
an empty repository; the guard is the backstop behind that.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.base import (
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

REFUSED = "source_run_full_retraction_refused"
_TICKETS = 3


# ---------------------------------------------------------------- helpers


def _ticket(index: int) -> NodeFact:
    return NodeFact(
        node_id=f"t:{index}",
        subtype="concept",
        attrs={"text": f"ticket {index}"},
        source_record_id={"id": f"r-{index}"},
        source_revision={"v": 1},
    )


def _tickets(indexes) -> list[NodeFact]:
    return [_ticket(index) for index in indexes]


def _ticket_record_set(indexes) -> RecordSet:
    return RecordSet(records={
        f"r-{index}": RecordEmission(
            primary=RecordNode(node_id=f"t:{index}", subtype="concept"),
        )
        for index in indexes
    })


class _ScriptAdapter:
    """One scripted `SourceRun` per call."""

    name = "guard-script"
    profile = "discovery_crawl"

    def __init__(self, script: list[dict[str, Any]]) -> None:
        self.script = list(script)

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
        sync_scope: dict | None = None,
        **_kwargs: Any,
    ) -> SourceRun:
        del run_id, cursor, sync_scope, _kwargs
        step = self.script.pop(0)
        extra = {"asserts_empty_scope": True} if step.get("asserts") else {}
        return SourceRun(
            facts=iter(list(step.get("facts", ()))),
            completed_scope=step.get("completed", True),
            next_cursor={"step": len(self.script)},
            run_mode=mode,
            record_set=step.get("record_set"),
            **extra,
        )


def _runner(dsn: str, source: str, **kwargs: Any) -> SourceRunner:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
    return SourceRunner(dsn, source_name=source, **kwargs)


def _override(source: str):
    from okg.substrate.ingest.full_retraction_guard import (
        FullRetractionOverride,
    )

    return FullRetractionOverride(
        source=source,
        requested_by="guard-test-operator",
        requested_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
    )


def _live_nodes(dsn: str, source: str, *, like: str = "%") -> int:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
            " WHERE source = :source AND node_id LIKE :pattern",
            {"source": source, "pattern": like},
        )
    finally:
        store.close()
    return int(rows[0]["n"]) if rows else 0


def _live_node_ids(dsn: str, source: str) -> set[str]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout().query(
            "SELECT node_id FROM okg.graph_nodes WHERE source = :source",
            {"source": source},
        )
    finally:
        store.close()
    return {str(row["node_id"]) for row in rows}


def _audit_row(dsn: str, deployment: str, source: str, run_id: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT value FROM okg.source_progress"
            " WHERE deployment = %s AND source_name = %s AND key = %s",
            (deployment, source, f"sync.run.{run_id}"),
        )
        row = cur.fetchone()
    assert row is not None, "the run wrote no sync.run audit row"
    return json.loads(row[0])


def _assert_refused(excinfo: pytest.ExceptionInfo, **data: Any) -> dict:
    err = excinfo.value
    assert getattr(err, "code", None) == REFUSED, err
    for key, value in data.items():
        assert err.data.get(key) == value, (key, err.data)
    assert err.data["override_flag"].startswith("--allow-full-retraction ")
    return err.data


# ------------------------------------------------------- whole source


def test_empty_record_set_with_live_rows_is_refused(clean_runtime: str) -> None:
    source = "guard.whole-refused"
    adapter = _ScriptAdapter([
        {"facts": _tickets(range(_TICKETS)),
         "record_set": _ticket_record_set(range(_TICKETS))},
        {"facts": [], "record_set": RecordSet()},
    ])
    runner = _runner(clean_runtime, source)
    runner.run(adapter, mode="scope_complete")
    assert _live_nodes(clean_runtime, source) == _TICKETS

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete")

    data = _assert_refused(
        excinfo, source_name=source, kind="whole_source", live_rows=_TICKETS,
    )
    assert data["override_flag"] == f"--allow-full-retraction {source}"
    assert _live_nodes(clean_runtime, source) == _TICKETS


def test_empty_record_set_with_override_retracts_and_is_audited(
    clean_runtime: str,
) -> None:
    source = "guard.whole-allowed"
    deployment = "guard-audit"
    adapter = _ScriptAdapter([
        {"facts": _tickets(range(_TICKETS)),
         "record_set": _ticket_record_set(range(_TICKETS))},
        {"facts": [], "record_set": RecordSet()},
    ])
    runner = _runner(
        clean_runtime,
        source,
        deployment_name=deployment,
        full_retraction_override=_override(source),
    )
    runner.run(adapter, mode="scope_complete")

    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == _TICKETS
    assert _live_nodes(clean_runtime, source) == 0
    record = result.as_dict()["full_retraction_override"]
    assert record["requested_by"] == "guard-test-operator"
    assert record["source"] == source
    assert record["exercised"] is True
    assert [e["kind"] for e in record["exercises"]] == ["whole_source"]
    assert record["exercises"][0]["live_rows"] == _TICKETS
    audit = _audit_row(clean_runtime, deployment, source, result.run_id)
    assert audit["full_retraction_override"] == record


def test_first_run_and_ordinary_runs_pass(clean_runtime: str) -> None:
    source = "guard.ordinary"
    adapter = _ScriptAdapter([
        # A first run with nothing live: an empty set retracts nothing.
        {"facts": [], "record_set": RecordSet()},
        {"facts": _tickets(range(_TICKETS)),
         "record_set": _ticket_record_set(range(_TICKETS))},
        # An ordinary partial deletion.
        {"facts": _tickets(range(_TICKETS - 1)),
         "record_set": _ticket_record_set(range(_TICKETS - 1))},
    ])
    runner = _runner(clean_runtime, source)

    first = runner.run(adapter, mode="scope_complete")
    runner.run(adapter, mode="scope_complete")
    third = runner.run(adapter, mode="scope_complete")

    assert first.retractions_node == 0
    assert third.retractions_node == 1
    assert _live_nodes(clean_runtime, source) == _TICKETS - 1
    assert third.as_dict().get("full_retraction_override") is None


def test_reset_cursor_stray_pass_is_refused(clean_runtime: str) -> None:
    source = "guard.reset-cursor"
    adapter = _ScriptAdapter([
        {"facts": _tickets(range(_TICKETS)),
         "record_set": _ticket_record_set(range(_TICKETS))},
        {"facts": [], "record_set": RecordSet()},
    ])
    runner = _runner(clean_runtime, source)
    runner.run(adapter, mode="scope_complete")

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete", reset_cursor=True)

    _assert_refused(excinfo, kind="whole_source", live_rows=_TICKETS)
    assert _live_nodes(clean_runtime, source) == _TICKETS


# ------------------------------------------------ empty-scope assertion


def _stream_runner(dsn: str, source: str, **kwargs: Any) -> SourceRunner:
    return _runner(
        dsn,
        source,
        deletion_semantics="missing_from_completed_scope",
        **kwargs,
    )


def test_asserted_empty_scope_with_live_rows_is_refused(
    clean_runtime: str,
) -> None:
    source = "guard.asserts-refused"
    adapter = _ScriptAdapter([
        {"facts": _tickets(range(_TICKETS))},
        {"facts": [], "asserts": True},
    ])
    runner = _stream_runner(clean_runtime, source)
    runner.run(adapter, mode="scope_complete")

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete")

    _assert_refused(
        excinfo, kind="empty_scope_assertion", live_rows=_TICKETS,
    )
    assert _live_nodes(clean_runtime, source) == _TICKETS


def test_asserted_empty_scope_with_override_retracts(
    clean_runtime: str,
) -> None:
    source = "guard.asserts-allowed"
    adapter = _ScriptAdapter([
        {"facts": _tickets(range(_TICKETS))},
        {"facts": [], "asserts": True},
    ])
    runner = _stream_runner(
        clean_runtime, source, full_retraction_override=_override(source),
    )
    runner.run(adapter, mode="scope_complete")

    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == _TICKETS
    assert _live_nodes(clean_runtime, source) == 0
    exercises = result.as_dict()["full_retraction_override"]["exercises"]
    assert [e["kind"] for e in exercises] == ["empty_scope_assertion"]


def test_asserted_empty_scope_on_a_first_run_passes(clean_runtime: str) -> None:
    source = "guard.asserts-first"
    adapter = _ScriptAdapter([{"facts": [], "asserts": True}])
    result = _stream_runner(clean_runtime, source).run(
        adapter, mode="scope_complete",
    )
    assert result.retractions_node == 0


# ------------------------------------ per-repository sub-scopes (#2818)
#
# The real ExternalRepoFactory cannot finish a completed run against a
# Chronos graph on dev today: its record set declares no edges, so the
# runner's record-set coverage check refuses its member_of edges on the first
# run. The runner tests therefore drive a repository-shaped adapter with the
# same record shape (one record per repository: the repository primary plus
# its files as descendants) and the same class attribute, and
# `test_factory_failed_sub_run_empties_its_repository_record` proves the real
# factory produces exactly the record the guard refuses.


def _repo_facts_and_record(
    repo: str, files: tuple[str, ...],
) -> tuple[list[NodeFact], RecordEmission]:
    repo_id = f"external_repo:{repo}"
    facts = [NodeFact(
        node_id=repo_id,
        subtype="concept",
        attrs={"name": repo},
        source_record_id={"repo_name": repo},
        source_revision={"v": 1},
    )]
    descendants = []
    for name in files:
        node_id = f"file:{repo}:{name}"
        facts.append(NodeFact(
            node_id=node_id,
            subtype="concept",
            attrs={"path": name},
            source_record_id={"repo_name": repo, "path": name},
            source_revision={"v": 1},
        ))
        descendants.append(RecordNode(node_id=node_id, subtype="concept"))
    return facts, RecordEmission(
        primary=RecordNode(node_id=repo_id, subtype="concept"),
        descendants=descendants,
    )


class _RepositoryAdapter:
    """Repository-shaped reader: one record per repository."""

    name = "guard-repos"
    profile = "discovery_crawl"
    record_keys_are_sub_scopes = True

    def __init__(self, repos: dict[str, tuple[str, ...]]) -> None:
        self.repos = dict(repos)

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
        sync_scope: dict | None = None,
        **_kwargs: Any,
    ) -> SourceRun:
        del run_id, cursor, sync_scope, _kwargs
        facts: list[NodeFact] = []
        records: dict[str, RecordEmission] = {}
        for repo, files in self.repos.items():
            repo_facts, record = _repo_facts_and_record(repo, files)
            facts.extend(repo_facts)
            records[f"external_repo:{repo}"] = record
        return SourceRun(
            facts=iter(facts),
            completed_scope=True,
            next_cursor={"repos": sorted(self.repos)},
            run_mode=mode,
            record_set=RecordSet(records=records),
        )


_B_FILES = ("beta.py", "gamma.py")


def _two_repo_adapter() -> _RepositoryAdapter:
    return _RepositoryAdapter({"a": ("alpha.py",), "b": _B_FILES})


def test_repository_that_lost_all_its_files_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2818: a failed sub-run reads as an empty repository."""
    source = "guard.repos-refused"
    adapter = _two_repo_adapter()
    runner = _runner(clean_runtime, source)
    runner.run(adapter, mode="scope_complete")
    assert _live_nodes(clean_runtime, source, like="file:b:%") == 2

    adapter.repos["b"] = ()  # the repository record, with no files
    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete")

    data = _assert_refused(excinfo, kind="sub_scope", live_rows=2)
    assert data["record_keys"] == ["external_repo:b"]
    assert _live_nodes(clean_runtime, source, like="file:b:%") == 2


def test_repository_that_lost_all_its_files_under_reset_cursor_is_refused(
    clean_runtime: str,
) -> None:
    source = "guard.repos-reset"
    adapter = _two_repo_adapter()
    runner = _runner(clean_runtime, source)
    runner.run(adapter, mode="scope_complete")

    adapter.repos["b"] = ()
    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete", reset_cursor=True)

    data = _assert_refused(excinfo, kind="sub_scope", live_rows=2)
    assert data["record_keys"] == ["external_repo:b"]
    assert _live_nodes(clean_runtime, source, like="file:b:%") == 2


def test_repository_that_lost_all_its_files_with_override_retracts(
    clean_runtime: str,
) -> None:
    source = "guard.repos-allowed"
    adapter = _two_repo_adapter()
    runner = _runner(
        clean_runtime, source, full_retraction_override=_override(source),
    )
    runner.run(adapter, mode="scope_complete")

    adapter.repos["b"] = ()
    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == 2
    assert _live_nodes(clean_runtime, source, like="file:b:%") == 0
    exercises = result.as_dict()["full_retraction_override"]["exercises"]
    assert [e["kind"] for e in exercises] == ["sub_scope"]
    assert exercises[0]["record_keys"] == ["external_repo:b"]


def test_repository_removed_from_config_deletes_normally(
    clean_runtime: str,
) -> None:
    source = "guard.repos-removed"
    adapter = _two_repo_adapter()
    runner = _runner(clean_runtime, source)
    runner.run(adapter, mode="scope_complete")

    del adapter.repos["b"]
    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == 3  # the repository and its two files
    assert _live_nodes(clean_runtime, source, like="file:b:%") == 0
    assert _live_nodes(clean_runtime, source, like="file:a:%") == 1


def test_partial_loss_inside_a_repository_passes(clean_runtime: str) -> None:
    source = "guard.repos-partial"
    adapter = _two_repo_adapter()
    runner = _runner(clean_runtime, source)
    first = runner.run(adapter, mode="scope_complete")
    assert first.retractions_node == 0

    adapter.repos["b"] = ("beta.py",)
    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == 1
    assert _live_nodes(clean_runtime, source, like="file:b:%") == 1


def _vcs(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(repo: Path, files: dict[str, str]) -> None:
    repo.mkdir(parents=True)
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(repo)],
        check=True,
    )
    _vcs(repo, "config", "user.email", "fixture@example.invalid")
    _vcs(repo, "config", "user.name", "fixture")
    for rel, content in files.items():
        (repo / rel).write_text(content)
    _vcs(repo, "add", "-A")
    _vcs(repo, "commit", "--quiet", "-m", "seed")


_FAILURE = "simulated clone failure for b"


def _two_real_repos(tmp_path: Path) -> Path:
    repo_a = tmp_path / "a"
    repo_b = tmp_path / "b"
    _init_repo(repo_a, {"alpha.py": "def alpha():\n    return 1\n"})
    _init_repo(repo_b, {"beta.py": "def beta():\n    return 2\n"})
    cfg = tmp_path / "repos.yaml"
    cfg.write_text(
        "repos:\n"
        + "".join(
            f"  - name: {name}\n"
            f"    url: https://example.invalid/{name}.git\n"
            f"    local_path: {path}\n"
            for name, path in (("a", repo_a), ("b", repo_b))
        )
    )
    return cfg


def _fail_repository_b(monkeypatch: pytest.MonkeyPatch, factory: Any) -> None:
    original = factory._run_sub_sources

    def _fail_b(repo, *args, **kwargs):  # type: ignore[no-untyped-def]
        if repo.name == "b":
            raise RuntimeError(_FAILURE)
        return original(repo, *args, **kwargs)

    monkeypatch.setattr(factory, "_run_sub_sources", _fail_b)


def test_factory_reports_a_failed_repository_as_failed_not_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2818 (c): a failed repository sub-run is a failure, not an
    empty repository (no database needed).

    The factory's run names the repository and the cause in degraded
    health and claims neither a completed scope nor a record set, so no
    record says "repository b now has no files".
    """
    from okg.substrate.ingest.full_retraction_guard import SUB_SCOPE_ATTRIBUTE
    from okg.substrate.library.sources.external_repos import (
        ExternalRepoFactory,
    )

    factory = ExternalRepoFactory(repos_config_path=str(_two_real_repos(tmp_path)))
    assert getattr(factory, SUB_SCOPE_ATTRIBUTE) is True
    first = factory.run("run-1", mode="scope_complete")
    assert first.health is None
    assert first.record_set is not None
    assert first.record_set.records["external_repo:b"].descendants

    _fail_repository_b(monkeypatch, factory)
    second = factory.run(
        "run-2", mode="scope_complete", cursor=first.next_cursor,
    )

    assert second.health is not None
    assert second.health.status == "partial"
    assert "b: " + _FAILURE in (second.health.reason or "")
    assert second.completed_scope is False
    assert second.record_set is None
    assert second.next_cursor["errors"] == {"b": _FAILURE}
    # The failed repository keeps its prior cursor for the retry.
    assert second.next_cursor["repos"]["b"] == first.next_cursor["repos"]["b"]


def test_failed_repository_sub_run_fails_the_run_and_keeps_its_rows(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2818 (c) through a registered crawl runner, as deployments run
    the factory: the run fails with an error naming the repository and the
    cause, and nothing of that repository is retracted. The full-retraction
    guard is not what stops it."""
    from okg.substrate.library.sources.external_repos import (
        ExternalRepoFactory,
    )

    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    source = "guard.factory-failed-repo"
    factory = ExternalRepoFactory(repos_config_path=str(_two_real_repos(tmp_path)))
    runner = _runner(
        clean_runtime,
        source,
        write_fact_log=True,
        executable_profile="discovery_crawl",
        registry_name=source,
        ownership_id=source,
        registry_binding_fingerprint="sha256:guard-factory-failed-repo",
    )
    runner.run(factory, mode="scope_complete")
    b_files = _live_nodes(clean_runtime, source, like="file:b:%")
    every_row = _live_nodes(clean_runtime, source)
    assert b_files > 0

    _fail_repository_b(monkeypatch, factory)
    with pytest.raises(SubstrateError) as excinfo:
        runner.run(factory, mode="scope_complete")

    error = excinfo.value
    assert getattr(error, "code", None) != REFUSED, error
    assert "b: " + _FAILURE in error.message
    assert error.data["source_health_status"] == "partial"
    assert "b: " + _FAILURE in error.data["source_health_reason"]
    assert "fix or remove the failing repository" in (error.fix_hint or "")
    assert _live_nodes(clean_runtime, source, like="file:b:%") == b_files
    assert _live_nodes(clean_runtime, source) == every_row


def _external_repos_deployment(tmp_path: Path, repos_config: Path) -> Path:
    """A one-source deployment that runs the real factory, as `okg ingest`
    does."""
    import yaml

    from tests.substrate.test_ingest_service import (
        _fixture_source_privacy_policy,
        _source,
    )

    dep = tmp_path / "deployments" / "svc"
    (dep / "schemas").mkdir(parents=True)
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "svc",
        "description": "external repos through okg ingest",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": _EXTERNAL_REPOS_MODULES,
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["no_export"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }))
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "repos": {
                **_source("svc.repos"),
                "module": "okg.substrate.library.sources.external_repos.factory",
                "class": "ExternalRepoFactory",
                "params": {"repos_config_path": str(repos_config)},
                "source_policy": _fixture_source_privacy_policy(),
            },
        },
    }))
    return dep


# The factory's own subtypes plus the modules their narrowings name.
_EXTERNAL_REPOS_MODULES = [
    "external_repos", "git_graph", "code_graph", "agent_sessions", "ci",
    "openspec", "compute_env",
]


def test_failed_repository_through_okg_ingest_is_rejected_and_keeps_its_rows(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2818 (c) through the ingest service (`okg ingest`): a published
    repository that can no longer be read fails the run at admission with
    its name and cause in the reported health, and keeps every row.

    The source runs in a spawned child process, so the failure is real
    damage to the repository (its only branch ref deleted), not a patch.
    Both runs publish, as `okg ingest` does by default.
    """
    from okg.substrate.ingest.service import IngestOptions, run_ingest
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
    )

    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    cfg = _two_real_repos(tmp_path / "repos")
    dep = _external_repos_deployment(tmp_path, cfg)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    source = "svc.repos"

    def _ingest():
        return run_ingest(IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            publish=True,
        ))

    first = _ingest()
    assert first.any_failed is False, first.report
    b_files = _live_nodes(clean_runtime, source, like="file:b:%")
    every_node = _live_node_ids(clean_runtime, source)
    assert b_files > 0

    (tmp_path / "repos" / "b" / ".git" / "refs" / "heads" / "main").unlink()
    second = _ingest()

    assert second.any_failed is True
    records = [r for r in second.report["results"] if r["name"] == "repos"]
    assert len(records) == 1, second.report
    record = records[0]
    assert record["status"] == "FAIL", record
    assert record["admission_reject_reason"] == "source_health_not_healthy"
    policy = record["admission_verdict"]["producer_policy"]
    assert policy["reason"] == "failed_or_degraded", policy
    health = record["source_health"]
    assert health["status"] == "partial", health
    assert "external repo sub-run failed for b: " in health["reason"], health
    assert "refs/heads/main" in health["reason"], health
    assert _live_nodes(clean_runtime, source, like="file:b:%") == b_files
    assert _live_node_ids(clean_runtime, source) == every_node


# ------------------------------------- override through the source child


def _layered_deployment_with_first_emptied(dsn: str, tmp_path: Path) -> Path:
    """The layered `svc` fixture, published once, then `first` emptied.

    Found in merge train 29: the ingest service runs each source in a
    spawned child, and the child's options dropped the override.
    """
    from okg.substrate.ingest.service import IngestOptions, run_ingest
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _commit_layered_fixture_deployment,
        _write_layered_fixture_deployment,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    first = run_ingest(IngestOptions(
        dsn=dsn, deployment="svc", deployment_dir=dep, publish=True,
    ))
    assert first.any_failed is False, first.report
    assert _live_nodes(dsn, "svc.first") == 1
    (dep / "first.json").write_text(json.dumps({"nodes": [], "edges": []}))
    _commit_layered_fixture_deployment(dep, message="first source emptied")
    return dep


def _first_result(report: dict) -> dict:
    rows = [row for row in report["results"] if row["name"] == "first"]
    assert len(rows) == 1, report
    return rows[0]


def test_override_through_okg_ingest_reaches_the_source_child(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest.service import IngestOptions, run_ingest

    dep = _layered_deployment_with_first_emptied(clean_runtime, tmp_path)

    def _ingest(**extra: Any):
        return run_ingest(IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
            **extra,
        ))

    refused = _ingest()
    assert refused.any_failed is True
    row = _first_result(refused.report)
    assert "source_process" in row, row
    assert row["error_data"]["kind"] == "whole_source", row
    assert _live_nodes(clean_runtime, "svc.first") == 1

    allowed = _ingest(
        allow_full_retraction=("first",), owner_id="okg-ingest-operator",
    )

    assert allowed.any_failed is False, allowed.report
    row = _first_result(allowed.report)
    assert "source_process" in row, row
    assert row["retract_n"] == 1, row
    record = row["full_retraction_override"]
    assert record["requested_by"] == "okg-ingest-operator"
    assert record["source"] == "first"
    assert record["exercised"] is True
    assert [e["kind"] for e in record["exercises"]] == ["whole_source"]
    assert _live_nodes(clean_runtime, "svc.first") == 0
    audit = _audit_row(clean_runtime, "svc", "svc.first", row["run_id"])
    assert audit["full_retraction_override"] == record


def test_override_through_the_okg_ingest_command_reaches_the_source_child(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import okg.substrate.cli.main as cli

    dep = _layered_deployment_with_first_emptied(clean_runtime, tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(dep.parent))
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda name: False)
    capsys.readouterr()

    rc = cli.main([
        "--json", "ingest", "--deployment", "svc", "--dsn", clean_runtime,
        "--inline", "--include", "first", "--no-publish",
        "--allow-full-retraction", "first", "--owner-id", "cli-operator",
    ])

    captured = capsys.readouterr()
    out = captured.out
    assert rc == 0, (out, captured.err[-4000:])
    report = json.loads(out)
    report = report.get("data", report)
    row = _first_result(report)
    assert "source_process" in row, row
    record = row["full_retraction_override"]
    assert record["requested_by"] == "cli-operator"
    assert record["exercised"] is True
    assert _live_nodes(clean_runtime, "svc.first") == 0


# ------------------------------------------------ service-side plumbing


def test_override_matches_registry_name_or_ownership_id() -> None:
    from types import SimpleNamespace

    from okg.substrate.ingest.full_retraction_guard import (
        full_retraction_override_for,
        full_retraction_report_fields,
    )

    options = SimpleNamespace(
        allow_full_retraction=("repos",), owner_id="jason",
    )
    by_name = full_retraction_override_for(options, "repos", "own.repos")
    by_owner = full_retraction_override_for(
        SimpleNamespace(allow_full_retraction=("own.repos",), owner_id=None),
        "repos",
        "own.repos",
    )
    assert by_name is not None and by_name.requested_by == "jason"
    assert by_owner is not None and by_owner.source == "own.repos"
    assert full_retraction_override_for(options, "other", "own.other") is None
    assert full_retraction_report_fields(SimpleNamespace()) == {}


def test_override_naming_no_selected_source_is_refused() -> None:
    from types import SimpleNamespace

    from okg.substrate.ingest.full_retraction_guard import (
        validate_full_retraction_override_names,
    )

    entries = {"repos": SimpleNamespace(ownership_id="own.repos")}
    validate_full_retraction_override_names(
        ("own.repos",), entries, ["repos"],
    )
    with pytest.raises(SubstrateError) as excinfo:
        validate_full_retraction_override_names(
            ("repso",), entries, ["repos"],
        )
    assert excinfo.value.data["unknown_sources"] == ["repso"]


def test_emptied_sub_scopes_ignores_removed_and_partial_records() -> None:
    from okg.substrate.ingest.full_retraction_guard import emptied_sub_scopes

    def _rec(children: int) -> RecordEmission:
        return RecordEmission(
            primary=RecordNode(node_id="repo", subtype="concept"),
            descendants=[
                RecordNode(node_id=f"f:{i}", subtype="concept")
                for i in range(children)
            ],
        )

    prior = RecordSet(records={
        "emptied": _rec(3), "partial": _rec(3), "removed": _rec(2),
        "was-empty": _rec(0),
    })
    current = RecordSet(records={
        "emptied": _rec(0), "partial": _rec(1), "was-empty": _rec(0),
    })
    assert emptied_sub_scopes(prior, current) == [("emptied", 3)]

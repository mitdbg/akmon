from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.cli.deployment import (
    _ready_add_generation_count_integrity_check,
    _ready_add_runtime_process_authority,
    _ready_contract_evidence,
    _ready_finalize_payload_state,
    _ready_postgres_capacity_check_level,
    _ready_postgres_capacity_status,
)
from okg.substrate.cli.deployment_readiness import (
    _ready_timescaledb_database_demand,
)
from okg.substrate.cli.main import main


def _write_ready_fixture(root: Path) -> Path:
    deployment_dir = root / "ready-demo"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    manifest = {
        "name": "ready-demo",
        "description": "Ready evidence fixture",
        "postgres": {"dsn": "${READY_DEMO_DSN}"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
    }
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=False),
        encoding="utf-8",
    )
    return deployment_dir


class _PostgresCapacityCursor:
    def __init__(
        self,
        *,
        settings: dict[str, int],
        scheduler_row: tuple[object, ...],
    ) -> None:
        self._settings = settings
        self._scheduler_row = scheduler_row
        self._row: tuple[object, ...] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, query: str, *_args) -> None:
        normalized = " ".join(str(query).split())
        if normalized.startswith("SHOW "):
            setting = normalized.removeprefix("SHOW ")
            self._row = (str(self._settings[setting]),)
        else:
            self._row = self._scheduler_row

    def fetchone(self) -> tuple[object, ...]:
        assert self._row is not None
        return self._row


class _PostgresCapacityConnection:
    def __init__(
        self,
        *,
        settings: dict[str, int],
        scheduler_row: tuple[object, ...],
    ) -> None:
        self._settings = settings
        self._scheduler_row = scheduler_row

    def cursor(self) -> _PostgresCapacityCursor:
        return _PostgresCapacityCursor(
            settings=self._settings,
            scheduler_row=self._scheduler_row,
        )


def test_postgres_capacity_readiness_requires_declared_settings_and_scheduler(
    tmp_path: Path,
    monkeypatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )
    authority = {
        "schema": "okg.postgres_capacity_authority/v1",
        "environment_id": "ready-production",
        "deployment": "ready-demo",
        "authority_source": "sealed_postgres_capacity_sidecar",
        "max_worker_processes": 40,
        "max_parallel_workers": 8,
        "timescaledb_max_background_workers": 16,
        "timescaledb_job_worker_reserve": 4,
        "configured_headroom": 16,
        "minimum_headroom": 4,
    }
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.resolve_postgres_capacity_authority",
        lambda **_kwargs: authority,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment_readiness._ready_timescaledb_database_demand",
        lambda _conn, **_kwargs: {
            "schema": "okg.timescaledb_database_demand/v1",
            "database_count_probed": 40,
            "loaded_database_count": 8,
            "active_scheduler_count": 8,
            "missing_scheduler_count": 0,
            "extra_scheduler_count": 0,
            "probe_error_count": 0,
            "current_database_timescaledb_loaded": True,
            "current_database_scheduler_running": True,
            "loaded_database_identity_digest": "sha256:redacted",
        },
    )
    conn = _PostgresCapacityConnection(
        settings={
            "max_worker_processes": 40,
            "max_parallel_workers": 8,
            "timescaledb.max_background_workers": 16,
        },
        scheduler_row=("ready_demo", 1),
    )

    status = _ready_postgres_capacity_status(
        conn,
        deployment="ready-demo",
        deployment_dir=tmp_path,
    )

    assert status["ok"] is True
    assert status["configured_headroom"] == 16
    assert status["required_timescaledb_workers"] == 12
    assert status["timescaledb_slot_headroom"] == 4
    assert status["timescaledb_demand"]["missing_scheduler_count"] == 0
    assert "descriptor_path" not in status["authority"]
    assert all("postgres://" not in command for command in status["repair_commands"])


def test_postgres_capacity_readiness_fails_closed_when_slots_are_exhausted(
    tmp_path: Path,
    monkeypatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.resolve_postgres_capacity_authority",
        lambda **_kwargs: {
            "schema": "okg.postgres_capacity_authority/v1",
            "environment_id": "ready-production",
            "deployment": "ready-demo",
            "authority_source": "sealed_postgres_capacity_sidecar",
            "max_worker_processes": 40,
            "max_parallel_workers": 8,
            "timescaledb_max_background_workers": 16,
            "timescaledb_job_worker_reserve": 4,
            "configured_headroom": 16,
            "minimum_headroom": 4,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment_readiness._ready_timescaledb_database_demand",
        lambda _conn, **_kwargs: {
            "schema": "okg.timescaledb_database_demand/v1",
            "database_count_probed": 40,
            "loaded_database_count": 8,
            "active_scheduler_count": 7,
            "missing_scheduler_count": 1,
            "extra_scheduler_count": 0,
            "probe_error_count": 0,
            "current_database_timescaledb_loaded": True,
            "current_database_scheduler_running": False,
            "loaded_database_identity_digest": "sha256:redacted",
        },
    )
    conn = _PostgresCapacityConnection(
        settings={
            "max_worker_processes": 32,
            "max_parallel_workers": 8,
            "timescaledb.max_background_workers": 8,
        },
        scheduler_row=("ready_demo", 1),
    )

    status = _ready_postgres_capacity_status(
        conn,
        deployment="ready-demo",
        deployment_dir=tmp_path,
    )

    assert status["ok"] is False
    assert status["status"] == "unavailable"
    assert status["timescaledb_slot_headroom"] == -4
    assert status["timescaledb_demand"]["missing_scheduler_count"] == 1
    assert "live PostgreSQL settings differ from sealed authority" in status["problems"]
    assert (
        "TimescaleDB scheduler and job-worker demand has no positive headroom"
        in status["problems"]
    )
    assert (
        "one or more TimescaleDB database schedulers are not running"
        in status["problems"]
    )
    assert (
        "deployment database TimescaleDB scheduler is not running"
        in status["problems"]
    )


def test_timescaledb_demand_inventory_detects_missing_noncanonical_scheduler(
    monkeypatch,
    record_property,
) -> None:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )

    class PrimaryCursor:
        def __init__(self) -> None:
            self.rows: list[tuple[object, ...]] = []

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def execute(self, query: str) -> None:
            normalized = " ".join(str(query).split())
            if "FROM pg_database" in normalized:
                self.rows = [("canonical",), ("other",), ("plain",)]
            elif "FROM pg_stat_activity" in normalized:
                self.rows = [("canonical",)]
            else:
                self.rows = [("canonical",)]

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.rows

        def fetchone(self) -> tuple[object, ...]:
            return self.rows[0]

    class PrimaryConnection:
        info = SimpleNamespace(
            dsn="postgresql://postgres@127.0.0.1:55434/canonical"
        )

        def cursor(self) -> PrimaryCursor:
            return PrimaryCursor()

    class ProbeCursor:
        def __init__(self, loaded: bool) -> None:
            self.loaded = loaded

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def execute(self, _query: str) -> None:
            return None

        def fetchone(self) -> tuple[bool]:
            return (self.loaded,)

    class ProbeConnection:
        def __init__(self, loaded: bool) -> None:
            self.loaded = loaded

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def cursor(self) -> ProbeCursor:
            return ProbeCursor(self.loaded)

    def connect_probe(dsn: str, **_kwargs) -> ProbeConnection:
        conninfo = conninfo_to_dict(dsn)
        assert conninfo["password"] == "test"
        database = conninfo["dbname"]
        return ProbeConnection(database in {"canonical", "other"})

    monkeypatch.setattr(psycopg, "connect", connect_probe)

    demand = _ready_timescaledb_database_demand(
        PrimaryConnection(),
        connection_dsn=(
            "postgresql://postgres:test@127.0.0.1:55434/canonical"
        ),
    )

    assert demand["loaded_database_count"] == 2
    assert demand["active_scheduler_count"] == 1
    assert demand["missing_scheduler_count"] == 1
    assert demand["current_database_timescaledb_loaded"] is True
    assert demand["current_database_scheduler_running"] is True
    assert "canonical" not in repr(demand)
    assert "other" not in repr(demand)


@pytest.mark.parametrize("profile", ["dogfood", "ci", "general"])
def test_postgres_capacity_failure_blocks_every_applicable_readiness_profile(
    profile: str,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.postgres-capacity "
        "test.runtime-authority.postgres-capacity",
    )

    assert _ready_postgres_capacity_check_level(
        {"applicable": True, "ok": False},
        readiness_profile=profile,
    ) == "blocker"


def test_ready_json_includes_contract_readiness_evidence_for_missing_env(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    monkeypatch.delenv("READY_DEMO_DSN", raising=False)
    deployment_dir = _write_ready_fixture(tmp_path)

    rc = main(["deployment", "ready", str(deployment_dir), "--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    evidence = payload["readiness_evidence"]
    assert rc == 1
    assert evidence["schema"] == "okg.deployment_readiness_evidence.v1"
    assert evidence["status"] == "unavailable"
    assert evidence["provenance"]["command"] == "okg deployment ready"
    assert evidence["slots"]["environment"]["status"] == "unavailable"
    assert evidence["slots"]["migrations"]["status"] == "unavailable"
    assert evidence["slots"]["mcp_read_alignment"]["status"] == "unavailable"


def _ready_payload_with_generation(
    generation: dict[str, object],
) -> dict[str, object]:
    return {
        "ok": True,
        "deployment_name": "ready-demo",
        "profile": {"max_source_age_seconds": 3600},
        "environment": {"ok": True},
        "database": {
            "required_relations": ["okg.graph_nodes"],
            "present_relations": ["okg.graph_nodes"],
            "missing_relations": [],
        },
        "generation": generation,
        "source_freshness": {"ok": True, "missing": [], "stale": []},
        "queues": {
            "active_queue_depth": 0,
            "max_active_queue_depth": 0,
            "dlq_count": 0,
        },
        "doctor": {"ok": True, "status": "pass"},
        "mcp": {"ok": True, "pinned_generation_id": "generation:1"},
    }


def _clean_count_integrity(generation_id: str) -> dict[str, object]:
    return {
        "ok": True,
        "status": "clean",
        "generation_id": generation_id,
        "checkpoint": {"node_count": 1, "edge_count": 2},
        "pinned": {"node_count": 1, "edge_count": 2},
        "mismatches": [],
    }


def test_ready_contract_evidence_normalizes_clean_live_slots() -> None:
    evidence = _ready_contract_evidence(
        _ready_payload_with_generation(
            {
                "status": "published",
                "generation_id": "generation:1",
                "count_integrity": _clean_count_integrity("generation:1"),
            }
        ),
        readiness_profile="dogfood",
    )

    assert evidence["status"] == "clean"
    assert evidence["ttl_seconds"] == 3600
    assert {
        slot["status"] for slot in evidence["slots"].values()
    } == {"clean"}


def test_ready_rejects_checkpoint_pinned_count_mismatch() -> None:
    payload = {"checks": [], "blockers": [], "warnings": []}
    generation = {
        "status": "published",
        "generation_id": "generation:mismatch",
        "node_count": 10,
        "edge_count": 20,
        "count_integrity": {
            "schema": "okg.generation_count_integrity.v1",
            "ok": False,
            "status": "mismatch",
            "generation_id": "generation:mismatch",
            "checkpoint": {"node_count": 10, "edge_count": 20},
            "pinned": {"node_count": 10, "edge_count": 19},
            "mismatches": ["edge_count"],
        },
    }

    _ready_add_generation_count_integrity_check(payload, generation)
    _ready_finalize_payload_state(payload)

    assert payload["ok"] is False
    assert payload["status"] == "stale"
    assert payload["blockers"] == [{
        "id": "generation.count_integrity",
        "status": "blocker",
        "message": "checkpoint counts do not match the pinned published generation",
        "ok": False,
        "details": generation["count_integrity"],
    }]
    details = payload["blockers"][0]["details"]
    assert details["generation_id"] == "generation:mismatch"
    assert details["checkpoint"]["edge_count"] == 20
    assert details["pinned"]["edge_count"] == 19

    clean_payload = {"checks": [], "blockers": [], "warnings": []}
    clean_generation = {
        **generation,
        "count_integrity": {
            **generation["count_integrity"],
            "ok": True,
            "status": "clean",
            "pinned": {"node_count": 10, "edge_count": 20},
            "mismatches": [],
        },
    }
    _ready_add_generation_count_integrity_check(clean_payload, clean_generation)
    _ready_finalize_payload_state(clean_payload)
    assert clean_payload["ok"] is True

    unavailable_payload = {"checks": [], "blockers": [], "warnings": []}
    _ready_add_generation_count_integrity_check(
        unavailable_payload,
        {"status": "published", "generation_id": "generation:unavailable"},
    )
    _ready_finalize_payload_state(unavailable_payload)
    assert unavailable_payload["ok"] is False
    unavailable = unavailable_payload["blockers"][0]["details"]
    assert unavailable["status"] == "unavailable"
    assert unavailable["mismatches"] == ["count_integrity_evidence_missing"]


def test_ready_classifies_runtime_identity_and_upstream_drift_consistently(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )

    runtime_identity = {
        "checks": [],
        "warnings": [],
        "blockers": [{
            "id": "deployment.apply",
            "status": "blocker",
            "ok": False,
            "message": "worker authority differs from desired state",
            "details": {
                "status": "blocked",
                "problems": ["stage_needed:worker_reload"],
            },
        }],
    }
    _ready_finalize_payload_state(runtime_identity)
    assert runtime_identity["ok"] is False
    assert runtime_identity["status"] == "unavailable"

    upstream_drift = {
        "checks": [],
        "warnings": [],
        "blockers": [{
            "id": "deployment.code_drift",
            "status": "blocker",
            "ok": False,
            "message": "deployed revision is one commit behind origin/dev",
            "details": {
                "ok": True,
                "status": "ok",
                "commits_behind": 1,
            },
        }],
    }
    _ready_finalize_payload_state(upstream_drift)
    assert upstream_drift["ok"] is False
    assert upstream_drift["status"] == "stale"

    unmeasurable_drift = {
        "checks": [],
        "warnings": [],
        "blockers": [{
            "id": "deployment.code_drift",
            "status": "blocker",
            "ok": False,
            "message": "base ref is unavailable",
            "details": {"ok": False, "status": "error"},
        }],
    }
    _ready_finalize_payload_state(unmeasurable_drift)
    assert unmeasurable_drift["ok"] is False
    assert unmeasurable_drift["status"] == "unavailable"


def test_runtime_authority_code_drift_probe_failure_is_unavailable(
    tmp_path: Path,
    monkeypatch,
    record_property,
) -> None:
    import psycopg
    from okg.substrate.cli import deployment as deployment_cli

    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.deployment_drift.check_deployment_code_drift",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("probe exploded")),
    )
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: (
            _ for _ in ()
        ).throw(psycopg.OperationalError("database unavailable")),
    )

    payload = deployment_cli._deployment_ready_live_checks(
        deployment="okg-workspace",
        deployment_dir=tmp_path,
        dsn="postgres://redacted",
        max_active_queue_depth=0,
        max_source_age_seconds=3600,
        readiness_profile="dogfood",
        skip_doctor=True,
    )

    drift = next(
        blocker for blocker in payload["blockers"]
        if blocker["id"] == "deployment.code_drift"
    )
    assert drift["details"] == {
        "ok": False,
        "status": "error",
        "code": "code_drift_probe_failed",
        "message": "probe exploded",
    }
    _ready_finalize_payload_state(payload)
    assert payload["status"] == "unavailable"

    general = deployment_cli._deployment_ready_live_checks(
        deployment="okg-workspace",
        deployment_dir=tmp_path,
        dsn="postgres://redacted",
        max_active_queue_depth=0,
        max_source_age_seconds=3600,
        readiness_profile="general",
        skip_doctor=True,
    )
    assert any(
        warning["id"] == "deployment.code_drift"
        for warning in general["warnings"]
    )
    assert all(
        blocker["id"] != "deployment.code_drift"
        for blocker in general["blockers"]
    )


def test_deployment_ready_runtime_authority_requires_one_process_root(
    monkeypatch,
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: {
            "ok": False,
            "status": "not_observed",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "roots": [],
            "processes": [],
        },
    )
    payload = {"checks": [], "blockers": [], "warnings": []}

    _ready_add_runtime_process_authority(
        payload,
        deployment="okg-workspace",
    )
    _ready_finalize_payload_state(payload)

    assert payload["status"] == "unavailable"
    assert payload["blockers"][0]["id"] == "runtime.worker_processes"
    assert payload["runtime_processes"]["status"] == "not_observed"


def _process_audit(status: str, *, roots: int = 0) -> dict:
    return {
        "ok": status == "single_executor",
        "status": status,
        "process_count": roots,
        "root_count": roots,
        "split_executor": status == "split_executor",
        "roots": [],
        "processes": [],
    }


def test_ready_off_host_probe_defers_worker_processes_to_pod_readiness(
    monkeypatch,
) -> None:
    """Issue #1164: the Helm readiness test runs in its own pod, where no
    worker process can ever be counted. With the pod-readiness authority the
    check is reported skipped -- never pass -- and names who measures it."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _process_audit("not_observed"),
    )
    payload = {"checks": [], "blockers": [], "warnings": []}

    _ready_add_runtime_process_authority(
        payload,
        deployment="okg-workspace",
        authority="kubernetes-pod-readiness",
    )
    _ready_finalize_payload_state(payload)

    (check,) = payload["checks"]
    assert check["id"] == "runtime.worker_processes"
    assert check["status"] == "skipped"
    assert check["ok"] is None
    assert check["details"]["applicable"] is False
    assert check["details"]["authority"] == "kubernetes-pod-readiness"
    assert check["details"]["deferred_to"] == "runtime StatefulSet pod readiness probe"
    assert check["details"]["local_observation"]["status"] == "not_observed"
    assert "readiness probe" in check["message"]
    assert payload["blockers"] == []
    assert payload["ok"] is True


@pytest.mark.parametrize(
    ("status", "roots", "expected"),
    [
        ("split_executor", 2, "blocker"),
        ("unavailable", 0, "blocker"),
        ("single_executor", 1, "pass"),
    ],
)
def test_ready_off_host_probe_still_judges_what_it_can_see(
    monkeypatch, status: str, roots: int, expected: str,
) -> None:
    """The authority only covers 'no worker here'. A worker this host does
    see is judged as before, so a split executor stays a blocker."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _process_audit(status, roots=roots),
    )
    payload = {"checks": [], "blockers": [], "warnings": []}

    _ready_add_runtime_process_authority(
        payload,
        deployment="okg-workspace",
        authority="kubernetes-pod-readiness",
    )

    assert payload["checks"][0]["status"] == expected


def test_ready_pod_readiness_authority_is_ignored_outside_a_pod(monkeypatch) -> None:
    """On a host deployment (no Kubernetes pod) the authority cannot hide a
    stopped worker: the check is measured here and says why."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(
        "okg.substrate.cli.dogfood._runtime_worker_process_audit",
        lambda _deployment: _process_audit("not_observed"),
    )
    payload = {"checks": [], "blockers": [], "warnings": []}

    _ready_add_runtime_process_authority(
        payload,
        deployment="okg-workspace",
        authority="kubernetes-pod-readiness",
    )
    _ready_finalize_payload_state(payload)

    assert payload["blockers"][0]["id"] == "runtime.worker_processes"
    assert payload["ok"] is False
    assert "KUBERNETES_SERVICE_HOST" in payload["runtime_processes"]["authority_ignored"]


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        ([], "local"),
        (["--worker-process-authority", "kubernetes-pod-readiness"], "kubernetes-pod-readiness"),
    ],
)
def test_ready_worker_process_authority_reaches_the_command(
    monkeypatch, argv: list[str], expected: str,
) -> None:
    """`okg deployment ready` hands the parsed flag to the readiness command
    under the attribute it reads."""
    seen = {}

    def capture(args, *, as_json):
        seen["authority"] = args.worker_process_authority
        return 0

    monkeypatch.setattr("okg.substrate.cli.deployment._cmd_deployment_ready", capture)
    assert main(["deployment", "ready", "okg-workspace", "--checks", "dogfood", *argv]) == 0
    assert seen["authority"] == expected


def test_ready_worker_process_authority_refuses_an_unknown_value(capsys) -> None:
    with pytest.raises(SystemExit) as raised:
        main([
            "deployment", "ready", "okg-workspace", "--checks", "dogfood",
            "--worker-process-authority", "anywhere",
        ])
    assert raised.value.code == 2
    assert "--worker-process-authority" in capsys.readouterr().err


def test_helm_readiness_test_passes_the_pod_readiness_authority() -> None:
    """The chart's off-host readiness probe names its authority explicitly;
    the worker container's own probes keep the local default."""
    root = Path(__file__).resolve().parents[3] / "ops" / "kubernetes" / "okg" / "templates"
    test_pod = (root / "tests" / "readiness.yaml").read_text(encoding="utf-8")
    worker = (root / "runtime-statefulset.yaml").read_text(encoding="utf-8")
    assert "--worker-process-authority kubernetes-pod-readiness" in test_pod
    assert "worker-process-authority" not in worker


def test_ready_generation_slot_is_not_clean_without_status() -> None:
    """A generation id alone must not report the facet clean (#1329).

    The id is minted before the publication gate runs, so a payload that
    carries ``generation_id`` but no ``status`` states nothing about the
    outcome. Count integrity is ok here so the integrity gate above the
    fallback does not mask the branch under test. This test fails on the
    pre-fix code with ``assert 'clean' != 'clean'``.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with_generation(
            {
                "generation_id": "generation:no-status",
                "count_integrity": _clean_count_integrity("generation:no-status"),
            }
        ),
        readiness_profile="dogfood",
    )

    slot = evidence["slots"]["latest_published_generation"]
    assert slot["status"] != "clean"
    assert slot["status"] == "unavailable"


def test_ready_generation_slot_reports_blocked_status() -> None:
    """An explicit blocked generation status stays blocked (#1329).

    This one is a characterization guard, not a regression for the removed
    fallback: it passes on the pre-fix code too. It exists so that a future
    edit which hoists the statusless ``unavailable`` return above the
    explicit-status handling is caught.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with_generation(
            {
                "status": "blocked",
                "generation_id": "generation:blocked",
                "count_integrity": _clean_count_integrity("generation:blocked"),
            }
        ),
        readiness_profile="dogfood",
    )

    slot = evidence["slots"]["latest_published_generation"]
    assert slot["status"] == "blocked"


def _ready_payload_with(**overrides: object) -> dict[str, object]:
    """A clean readiness payload with individual facets overridden.

    ``None`` removes the facet entirely so a genuinely absent payload can be
    distinguished from a present-but-empty one.
    """

    payload = _ready_payload_with_generation(
        {
            "status": "published",
            "generation_id": "generation:1",
            "count_integrity": _clean_count_integrity("generation:1"),
        }
    )
    for key, value in overrides.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    return payload


@pytest.mark.parametrize("slot_name", ["queues_runtime", "dlq"])
def test_ready_queue_slots_are_not_clean_from_an_empty_payload(
    slot_name: str,
) -> None:
    """An empty ``queues`` payload measures nothing (#1515).

    Before the fix ``int(value.get("dlq_count") or 0)`` and both depth reads
    defaulted to zero, so ``{}`` satisfied ``dlq_count == 0`` and
    ``depth <= max_depth`` and BOTH slots reported ``clean``. This test fails
    on the pre-fix code with ``assert 'clean' != 'clean'``.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(queues={}),
        readiness_profile="dogfood",
    )

    slot = evidence["slots"][slot_name]
    assert slot["status"] != "clean"
    assert slot["status"] == "unavailable"


def test_ready_dlq_slot_is_unavailable_when_the_scan_did_not_finish() -> None:
    """An unread dead-letter count is not a count of zero (#1515).

    ``_ready_queue_status`` reports ``dlq_count: None`` with
    ``dlq_status: "unknown"`` when the dead-letter scan runs past its
    deadline. Queue depth was still measured, so ``queues_runtime`` keeps
    reporting; only the ``dlq`` slot is unmeasured.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            queues={
                "ok": None,
                "status": "unknown",
                "dlq_status": "unknown",
                "dlq_count": None,
                "active_queue_depth": 0,
                "max_active_queue_depth": 0,
            }
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["dlq"]["status"] == "unavailable"
    # Queue depth WAS measured, so that slot still reports the summary status
    # the probe produced rather than going dark with the dlq. Since #1572 that
    # word, "unknown", maps onto the contract vocabulary as "unavailable".
    assert evidence["slots"]["queues_runtime"]["status"] == "unavailable"


def test_ready_queues_runtime_slot_is_unavailable_without_a_depth_bound() -> None:
    """A missing depth bound is not a bound of zero (#1515)."""

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            queues={"active_queue_depth": 12, "dlq_count": 0},
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["queues_runtime"]["status"] == "unavailable"
    assert evidence["slots"]["dlq"]["status"] == "clean"


def test_ready_dlq_slot_blocks_on_a_measured_dead_letter() -> None:
    """A measured dead letter blocks the dlq slot (#1515).

    ``summarize_runtime_queue_health`` reports this as ``status:
    "dead_letter"``, which is outside the readiness-contract vocabulary and
    so could not anchor a blocker. The slot answers from its own measured
    count instead.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            queues={
                "ok": False,
                "status": "dead_letter",
                "dlq_status": "measured",
                "dlq_count": 3,
                "active_queue_depth": 0,
                "max_active_queue_depth": 0,
            },
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["dlq"]["status"] == "blocked"


def test_ready_queues_runtime_slot_blocks_on_a_measured_depth_breach() -> None:
    """A measured depth breach blocks even under a mild summary status (#1515).

    ``summarize_runtime_queue_health`` derives ``status`` from row
    availability and staleness only -- it never looks at the configured
    depth bound. Before the fix the summary status was read first, so a
    queue 900 deep against a bound of 25 reported ``available``. This test
    fails on the pre-fix code with ``assert 'available' == 'blocked'``.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            queues={
                "ok": True,
                "status": "available",
                "dlq_status": "measured",
                "dlq_count": 0,
                "active_queue_depth": 900,
                "max_active_queue_depth": 25,
            },
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["queues_runtime"]["status"] == "blocked"
    # The dlq half was measured and is empty, so it keeps reporting the probe
    # summary. Issue #1572 closed the vocabulary gap this line used to note:
    # "available" now normalizes to "clean".
    assert evidence["slots"]["dlq"]["status"] == "clean"


@pytest.mark.parametrize("payload_key", ["queues", "database"])
def test_ready_absent_probed_payload_is_unavailable(payload_key: str) -> None:
    """An unconditionally probed facet that is absent was never measured.

    When the database probe finds missing relations the readiness path
    returns early, leaving later facets absent from the payload entirely.
    Those slots used to report ``not_required``, asserting the facet was not
    needed rather than that it was never read. This test fails on the
    pre-fix code with ``assert 'not_required' == 'unavailable'``.
    """

    payload = _ready_payload_with()
    payload.pop(payload_key)

    evidence = _ready_contract_evidence(payload, readiness_profile="dogfood")

    slots = ["migrations"] if payload_key == "database" else ["queues_runtime", "dlq"]
    for slot_name in slots:
        assert evidence["slots"][slot_name]["status"] == "unavailable"


def test_ready_optional_absent_facet_still_reports_not_required() -> None:
    """Absent OPTIONAL facets keep reporting not_required (#1515).

    Only the unconditionally probed facets were reclassified; a facet a
    profile can legitimately skip must not start reading as a failed
    measurement.
    """

    payload = _ready_payload_with()
    payload.pop("mcp")

    evidence = _ready_contract_evidence(payload, readiness_profile="dogfood")

    assert evidence["slots"]["mcp_read_alignment"]["status"] == "not_required"


def test_ready_dlq_slot_is_unavailable_for_a_non_integer_count() -> None:
    """A count that is not an integer is malformed, not a measurement (#1515)."""

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            queues={
                "dlq_status": "measured",
                "dlq_count": "0",
                "active_queue_depth": 0,
                "max_active_queue_depth": 0,
            },
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["dlq"]["status"] == "unavailable"


def test_ready_migrations_slot_is_not_clean_from_an_empty_payload() -> None:
    """An empty ``database`` payload is an unrun probe (#1515).

    Before the fix the ``missing_relations: []`` branch returned ``clean``
    without consulting ``required_relations``, so ``{"missing_relations": []}``
    reported the migrations slot clean while nothing had been checked. This
    test fails on the pre-fix code with ``assert 'clean' != 'clean'``.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(database={"missing_relations": []}),
        readiness_profile="dogfood",
    )

    slot = evidence["slots"]["migrations"]
    assert slot["status"] != "clean"
    assert slot["status"] == "unavailable"


def test_ready_migrations_slot_is_unavailable_without_a_missing_list() -> None:
    """A required-relation list with no result is unmeasured (#1515)."""

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            database={"required_relations": ["okg.graph_nodes"]},
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["migrations"]["status"] == "unavailable"


def test_ready_migrations_slot_ignores_a_status_over_its_measurement() -> None:
    """A summary status cannot vouch for relations nobody checked (#1515).

    The database probe emits no ``status`` today, so a payload carrying one
    is malformed. Reading it before the relation lists is the same
    asymmetry that produced this bug, so the measurement decides first.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(database={"status": "pass"}),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["migrations"]["status"] == "unavailable"


def test_ready_migrations_slot_blocks_on_measured_missing_relations() -> None:
    """Measured missing relations block the migrations slot (#1515)."""

    evidence = _ready_contract_evidence(
        _ready_payload_with(
            database={
                "required_relations": ["okg.graph_nodes", "okg.graph_edges"],
                "present_relations": ["okg.graph_nodes"],
                "missing_relations": ["okg.graph_edges"],
            },
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["migrations"]["status"] == "blocked"


def test_ready_slot_is_not_clean_for_a_non_mapping_payload() -> None:
    """A payload that is not a mapping carries no measurement (#1515).

    ``_ready_contract_slot`` hardcoded ``clean`` for any non-mapping value,
    so a scalar or a string in a facet slot reported health. This test fails
    on the pre-fix code with ``assert 'clean' != 'clean'``.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with(doctor="pass"),
        readiness_profile="dogfood",
    )

    slot = evidence["slots"]["doctor"]
    assert slot["status"] != "clean"
    assert slot["status"] == "unavailable"
    assert slot["provenance"]["reason"] == "payload_not_a_mapping"


def test_ready_published_generation_status_still_normalizes_to_clean() -> None:
    """Removing the dead ``published -> clean`` branch changes nothing (#1515).

    ``_normalize_ready_slot_status`` already maps ``published`` to ``clean``,
    so the special case below it could never be reached. This guards the
    behaviour the removed branch claimed to provide.
    """

    evidence = _ready_contract_evidence(
        _ready_payload_with_generation(
            {
                "status": "published",
                "generation_id": "generation:1",
                "count_integrity": _clean_count_integrity("generation:1"),
            }
        ),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["latest_published_generation"]["status"] == "clean"


# --- issue #1572: readiness slot vocabulary -------------------------------
#
# `summarize_runtime_queue_health` emits "available", "dead_letter" and
# "unknown". None of the three was defined in the readiness contract
# vocabulary, and `_normalize_ready_slot_status` passed an unrecognized word
# straight through, so a HEALTHY production deployment published
# `queues_runtime: "available"` -- outside `_READY_ALLOWED`, tripping
# `deployment_contract.readiness.slot_not_clean` -- and an unfinished
# dead-letter scan published "unknown", which is in no allowed set and in no
# blocked set, so it could anchor neither.


class _FakeRuntimeQueueCursor:
    """Replay `okg.v_runtime_queue_health` rows for the real summarizer.

    Only the two statements `runtime_queue_health_rows` issues are answered;
    anything else raises so a new read cannot be silently satisfied with an
    invented result. This stands in for the DATABASE, not for the readiness
    path: the dead-letter probe and `_ready_queue_status` are not exercised
    here, and `workflow_name` is reported present so the view's legacy
    `NULL::text` fallback is not covered either. Those run in the real
    Postgres check recorded on the pull request.
    """

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self._pending: str | None = None

    def execute(self, sql: str, *args: object) -> None:
        if "information_schema.columns" in sql:
            self._pending = "workflow_name_present"
        elif "okg.v_runtime_queue_health" in sql:
            self._pending = "rows"
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def fetchone(self) -> object:
        assert self._pending == "workflow_name_present"
        return {"present": True}

    def fetchall(self) -> list[dict[str, object]]:
        assert self._pending == "rows"
        return list(self._rows)


def _healthy_queue_row() -> dict[str, object]:
    """One row shaped exactly as the view emits for an idle, healthy queue."""

    return {
        "queue_name": None,
        "status": "idle",
        "availability": "available",
        "active_count": 0,
        "pending_count": 0,
        "waiting_count": 0,
        "delayed_count": 0,
        "running_count": 0,
        "failed_count": 0,
        "cancelled_count": 0,
        "dlq_count": 0,
        "terminal_history_count": 0,
        "unknown_active_age_count": 0,
        "oldest_active_age_seconds": None,
        "newest_active_age_seconds": None,
        "stale_after_seconds": 900,
        "stale_active_work_present": False,
        "generated_at": "2026-09-03T00:00:00+00:00",
        "reason": None,
        "workflow_name": None,
    }


def _queues_payload_from_the_real_summarizer(
    *,
    rows: list[dict[str, object]] | None = None,
    current_failures: list[dict[str, object]] | None = None,
    dlq_unknown: dict[str, object] | None = None,
    max_active_queue_depth: int = 25,
) -> dict[str, object]:
    """Build the readiness `queues` payload from the shipping summariser.

    The payload is not written by hand: it is whatever
    `summarize_runtime_queue_health` derives from `okg.v_runtime_queue_health`
    rows, plus the `max_active_queue_depth` bound `_ready_queue_status` adds.
    A hand-made dict here would prove only that the test author agrees with
    the test author about the producer's status words. `_ready_queue_status`
    itself needs a live connection, so it is not in this path; it can only
    narrow the status word further (to "dead_letter", "stale" or
    "available"), all three of which the contract defines.
    """

    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    cursor = _FakeRuntimeQueueCursor(
        [_healthy_queue_row()] if rows is None else rows
    )
    summary = summarize_runtime_queue_health(
        cursor,
        current_dlq={"current_failures": list(current_failures or [])},
        dlq_unknown=dlq_unknown,
    )
    return {**summary, "max_active_queue_depth": max_active_queue_depth}


def _ready_payload_with_queues(queues: dict[str, object]) -> dict[str, object]:
    payload = _ready_payload_with_generation(
        {
            "status": "published",
            "generation_id": "generation:1",
            "count_integrity": _clean_count_integrity("generation:1"),
        }
    )
    payload["queues"] = queues
    return payload


def test_ready_queues_slot_is_clean_for_a_healthy_producer_payload() -> None:
    """A healthy queue reports "available"; readiness must read that as clean.

    Fails before the fix with `assert 'available' == 'clean'`: the word passed
    through unmapped and every live deployment tripped `slot_not_clean`.
    """

    queues = _queues_payload_from_the_real_summarizer()
    assert queues["status"] == "available", "producer word changed"

    evidence = _ready_contract_evidence(
        _ready_payload_with_queues(queues),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["queues_runtime"]["status"] == "clean"
    assert evidence["slots"]["dlq"]["status"] == "clean"
    provenance = evidence["slots"]["queues_runtime"]["provenance"]
    assert "reported_status" not in provenance


def test_ready_queues_slot_blocks_for_a_dead_letter_producer_payload() -> None:
    """A measured dead letter reports "dead_letter"; readiness must block.

    Fails before the fix with `assert 'dead_letter' == 'blocked'`, so the slot
    sat in neither `_READY_ALLOWED` nor `_BLOCKED_READY_STATUSES` and a blocked
    deployment could not anchor its blocker scope on it.
    """

    queues = _queues_payload_from_the_real_summarizer(
        # `workflow_name` is the key `summarize_runtime_queue_health` filters
        # on, and `okg_publisher_cycle` is not in the set deployment
        # readiness delegates to another gate -- so this failure counts on
        # the shipping path too.
        current_failures=[
            {"workflow_uuid": "wf-1", "workflow_name": "okg_publisher_cycle"}
        ],
    )
    assert queues["status"] == "dead_letter", "producer word changed"

    evidence = _ready_contract_evidence(
        _ready_payload_with_queues(queues),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["dlq"]["status"] == "blocked"
    assert evidence["slots"]["queues_runtime"]["status"] == "blocked"


def test_ready_queues_slot_is_unavailable_for_an_unfinished_dlq_scan() -> None:
    """An unfinished dead-letter scan reports "unknown": that is unavailable.

    Fails before the fix with `assert 'unknown' == 'unavailable'`.
    """

    queues = _queues_payload_from_the_real_summarizer(
        dlq_unknown={"reason": "statement_timeout"},
    )
    assert queues["status"] == "unknown", "producer word changed"

    evidence = _ready_contract_evidence(
        _ready_payload_with_queues(queues),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["queues_runtime"]["status"] == "unavailable"


def test_ready_slot_fails_closed_on_a_word_outside_the_vocabulary() -> None:
    """An undefined status word reports unavailable, never itself.

    Fails before the fix with `assert 'brownout' == 'unavailable'`: the raw
    word was returned, so the auditor saw a status it could not classify.
    """

    payload = _ready_payload_with_generation(
        {
            "status": "published",
            "generation_id": "generation:1",
            "count_integrity": _clean_count_integrity("generation:1"),
        }
    )
    payload["doctor"] = {"ok": True, "status": "brownout"}

    evidence = _ready_contract_evidence(payload, readiness_profile="dogfood")

    slot = evidence["slots"]["doctor"]
    assert slot["status"] == "unavailable"
    # The raw word is recorded, not discarded: the operator still learns what
    # the probe said.
    assert slot["provenance"]["reported_status"] == "brownout"


def _status_literals(
    node: object,
    constants: dict[str, str] | None = None,
) -> set[str]:
    """String literals an expression that produces a status can evaluate TO.

    Recurses through conditionals, boolean operators and calls so
    `"a" if x else "b"`, `str(x or "c")` and `dict(status="d")` all report
    their words. A bare name is resolved through `constants`, the producer
    module's string globals, so `status = READINESS_UNKNOWN_STATUS` reports
    "unknown" instead of nothing. Two shapes are excluded because they NAME a
    field rather than produce one: the key argument of a `.get(...)` lookup,
    and a subscript key. `row.get("status", "quiesced")` therefore reports
    "quiesced" and not "status".
    """

    import ast

    resolved = constants or {}
    if isinstance(node, ast.Constant):
        return {node.value} if isinstance(node.value, str) else set()
    if isinstance(node, ast.Name):
        value = resolved.get(node.id)
        return {value} if value is not None else set()
    words: set[str] = set()
    if isinstance(node, ast.Call):
        skip_key = (
            isinstance(node.func, ast.Attribute) and node.func.attr == "get"
        )
        for index, arg in enumerate(node.args):
            if skip_key and index == 0:
                continue
            words |= _status_literals(arg, resolved)
        for keyword in node.keywords:
            words |= _status_literals(keyword.value, resolved)
        return words
    if isinstance(node, ast.Subscript):
        # `mapping["status"]` reads a field; only the object can carry a word.
        return _status_literals(node.value, resolved)
    for child in ast.iter_child_nodes(node):
        words |= _status_literals(child, resolved)
    return words


def _string_globals(producer: object) -> dict[str, str]:
    """Module-level string constants visible to `producer`."""

    import inspect

    module = inspect.getmodule(producer)
    return {
        name: value
        for name, value in vars(module).items()
        if isinstance(value, str) and not name.startswith("__")
    }


def _assigns_a_status(node: object) -> bool:
    """True when this statement assigns to something named `status`.

    Covers `status = ...`, `status: str = ...` and `payload["status"] = ...`
    -- the last is how `_ready_queue_status` overrides the word the queue
    summariser produced, so missing it would leave the final writer of the
    queue status unread.
    """

    import ast

    if isinstance(node, ast.Assign):
        targets: list[object] = list(node.targets)
    elif isinstance(node, ast.AnnAssign):
        targets = [node.target]
    else:
        return False
    for target in targets:
        if isinstance(target, ast.Name) and target.id == "status":
            return True
        if (
            isinstance(target, ast.Subscript)
            and isinstance(target.slice, ast.Constant)
            and target.slice.value == "status"
        ):
            return True
    return False


def _readiness_status_producers() -> list[object]:
    """The functions that write a `status` word into a readiness facet.

    `_ready_contract_slot` reads `payload[<facet>]["status"]` for all eight
    slots, so this is the full set of producers whose vocabulary the contract
    has to define. `_ready_required_dependency` covers the timeout stub every
    probe falls back to.
    """

    from okg.substrate import runtime_queue_health
    from okg.substrate.cli import deployment_readiness

    return [
        runtime_queue_health.summarize_runtime_queue_health,
        deployment_readiness._ready_queue_status,
        deployment_readiness._ready_doctor_status,
        deployment_readiness._ready_mcp_status,
        deployment_readiness._ready_database_status,
        deployment_readiness._ready_generation_status,
        deployment_readiness._ready_source_freshness_status,
        deployment_readiness._ready_required_dependency,
    ]


def _producer_status_words() -> dict[str, set[str]]:
    """Every literal each readiness producer can write as a `status`.

    Read out of the producers' own source with `ast` rather than restated
    here, so adding a status word to a producer without adding it to the
    contract vocabulary reds this test instead of escaping silently. Both
    `status = "x"` statements and `{"status": "x"}` literals are read: the
    producers use both shapes.
    """

    import ast
    import inspect
    import textwrap

    words: dict[str, set[str]] = {}
    for producer in _readiness_status_producers():
        tree = ast.parse(textwrap.dedent(inspect.getsource(producer)))
        constants = _string_globals(producer)
        found: set[str] = set()
        for node in ast.walk(tree):
            if _assigns_a_status(node) and node.value is not None:
                found |= _status_literals(node.value, constants)
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if (
                        isinstance(key, ast.Constant)
                        and key.value == "status"
                    ):
                        found |= _status_literals(value, constants)
        words[producer.__name__] = found
    return words


def test_every_producer_status_word_is_defined_by_the_contract() -> None:
    """Every producer's vocabulary must be a subset of the consumer's.

    Fails before the fix on {'available', 'dead_letter', 'unknown'} from the
    queue producers alone.
    """

    from okg.substrate.deployment_contract import (
        READY_STATUS_ALIASES,
        READY_STATUS_VOCABULARY,
        normalize_ready_slot_status,
    )

    by_producer = _producer_status_words()
    # Anchor the walker against the words these producers are known to emit.
    # Without this the enumeration could silently degrade to an empty set and
    # the subset check below would pass vacuously.
    assert {"available", "dead_letter", "unknown", "stale"} <= by_producer[
        "summarize_runtime_queue_health"
    ]
    assert {"available", "dead_letter", "stale"} <= by_producer[
        "_ready_queue_status"
    ]
    assert {"pass", "command_failed", "check_load_error", "skipped"} <= (
        by_producer["_ready_doctor_status"]
    )
    assert "unknown" in by_producer["_ready_required_dependency"]

    undefined = sorted({
        f"{name}:{word}"
        for name, found in by_producer.items()
        for word in found
        if word not in READY_STATUS_ALIASES
    })
    assert undefined == [], (
        "readiness producers emit status words the contract does not define: "
        f"{undefined}"
    )
    for found in by_producer.values():
        for word in sorted(found):
            normalized, unmapped = normalize_ready_slot_status(word)
            assert unmapped is None
            assert normalized in READY_STATUS_VOCABULARY


def test_status_literal_walker_reads_the_shapes_producers_use() -> None:
    """The enumeration must not miss a word behind a common Python shape.

    A walker that silently returns nothing for `status["status"] = "x"` or
    for a conditional would make the census above pass by finding no words to
    object to. Each case here is a shape a readiness producer actually uses.
    """

    import ast
    import textwrap

    cases = {
        'status = "degraded" if rows else "available"': {
            "degraded",
            "available",
        },
        'out["status"] = "backlogged" if rows else "available"': {
            "backlogged",
            "available",
        },
        'status = str(row.get("status") or "quiesced")': {"quiesced"},
        'status = row.get("status", "throttled")': {"throttled"},
        'status = dict(status="flapping")["status"]': {"flapping"},
    }
    constant_case = ("status = _BACKLOG if rows else \"available\"", {"_BACKLOG": "backlogged"})
    for source, expected in cases.items():
        tree = ast.parse(textwrap.dedent(source))
        found: set[str] = set()
        for node in ast.walk(tree):
            if _assigns_a_status(node) and node.value is not None:
                found |= _status_literals(node.value)
        assert found == expected, source

    # A module-level constant is how `_ready_required_dependency` writes its
    # word, so the walker must resolve one.
    source, constants = constant_case
    tree = ast.parse(textwrap.dedent(source))
    found = set()
    for node in ast.walk(tree):
        if _assigns_a_status(node) and node.value is not None:
            found |= _status_literals(node.value, constants)
    assert found == {"backlogged", "available"}


def test_contract_vocabulary_maps_only_onto_allowed_statuses() -> None:
    """Every alias target is a status the auditor already accepts."""

    from okg.substrate.deployment_contract import (
        READY_STATUS_ALIASES,
        READY_STATUS_VOCABULARY,
        normalize_ready_slot_status,
    )

    unmapped_targets = sorted(
        set(READY_STATUS_ALIASES.values()) - set(READY_STATUS_VOCABULARY)
    )
    assert unmapped_targets == []
    # The three words issue #1572 names, pinned to the mapping the contract
    # chose rather than merely to "some allowed word".
    assert normalize_ready_slot_status("available") == ("clean", None)
    assert normalize_ready_slot_status("dead_letter") == ("blocked", None)
    assert normalize_ready_slot_status("unknown") == ("unavailable", None)
    # Fail closed on anything the table does not define, and say what it was.
    assert normalize_ready_slot_status("wormhole") == ("unavailable", "wormhole")
    assert normalize_ready_slot_status("") == ("unavailable", "")


def test_ready_provenance_never_blames_an_unread_status_word() -> None:
    """`reported_status` names a word only when the verdict READ that word.

    The generation slot answers from its count-integrity result before the
    status word is reached. Recording the word anyway would tell an operator
    that readiness downgraded the probe's verdict, when readiness never
    looked at it.
    """

    payload = _ready_payload_with_generation(
        {
            "status": "brownout",
            "generation_id": "generation:1",
            "count_integrity": {"ok": False, "status": "mismatch"},
        }
    )

    evidence = _ready_contract_evidence(payload, readiness_profile="dogfood")

    slot = evidence["slots"]["latest_published_generation"]
    assert slot["status"] == "blocked"
    assert "reported_status" not in slot["provenance"]


def test_ready_queues_slot_blocks_on_a_depth_breach_under_a_healthy_word() -> None:
    """A measured depth breach outranks the summary status word.

    `summarize_runtime_queue_health` derives its word from row availability
    and staleness only -- it never reads the configured depth bound. Now that
    "available" maps to clean, reading the word first would report a queue
    900 deep against a bound of 25 as healthy.
    """

    queues = _queues_payload_from_the_real_summarizer(max_active_queue_depth=25)
    assert queues["status"] == "available", "producer word changed"
    queues["active_queue_depth"] = 900

    evidence = _ready_contract_evidence(
        _ready_payload_with_queues(queues),
        readiness_profile="dogfood",
    )

    assert evidence["slots"]["queues_runtime"]["status"] == "blocked"

"""req.okg-workspace-resource-monitoring.schedule.

The collector is registered as a @deployment_scheduled workflow with a default
cron, listed in the manifest, and exposes a DBOS-free runner.
"""
from __future__ import annotations

import importlib.util
import inspect

import yaml

from okg.substrate.deployments import find_deployments_root


def _ws_dir():
    return find_deployments_root() / "okg-workspace"


def test_manifest_registers_workflow_and_schedule() -> None:
    manifest = yaml.safe_load((_ws_dir() / "deployment.yaml").read_text())
    runtime = manifest["runtime"]
    assert "workflows.resource_monitor" in runtime["workflows"]
    assert runtime["schedules"]["okg_workspace_resource_monitor"]


def test_workflow_module_registers_schedule() -> None:
    path = _ws_dir() / "workflows" / "resource_monitor.py"
    spec = importlib.util.spec_from_file_location("okgws_resource_monitor", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert mod._SCHEDULE_ID == "okg_workspace_resource_monitor"
    assert callable(mod.run_resource_monitor)  # DBOS-free runner

    fn = mod.okg_workspace_resource_monitor
    assert getattr(fn, "_okg_schedule_id", None) == "okg_workspace_resource_monitor"
    assert getattr(fn, "_okg_default_cron", None) == "*/5 * * * *"


def test_resource_monitor_records_database_size(monkeypatch) -> None:
    path = _ws_dir() / "workflows" / "resource_monitor.py"
    spec = importlib.util.spec_from_file_location("okgws_resource_monitor_db_size", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    recorded = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, query):
            assert query == "SELECT pg_database_size(current_database())"

        def fetchone(self):
            return (79_923_279_539,)

    class Connection:
        def cursor(self):
            return Cursor()

    monkeypatch.setattr(mod, "record_sample", lambda _conn, **kwargs: recorded.append(kwargs))

    assert mod._record_database_size(Connection()) == 79_923_279_539
    assert recorded == [{
        "deployment_name": "okg-workspace",
        "node_id": "database:okg-workspace",
        "metric": "db_size_bytes",
        "value": 79_923_279_539.0,
        "source": "resource_monitor:postgres",
    }]


def test_database_size_failure_is_contained_without_raw_details(monkeypatch) -> None:
    path = _ws_dir() / "workflows" / "resource_monitor.py"
    spec = importlib.util.spec_from_file_location("okgws_resource_monitor_db_failure", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(
        mod,
        "_record_database_size",
        lambda _conn: (_ for _ in ()).throw(RuntimeError("/private/tmp/secret-db-path")),
    )

    evidence = mod._database_size_evidence(object())

    assert evidence == {
        "database_size_bytes": None,
        "database_size_status": "unavailable",
        "database_size_error_type": "RuntimeError",
    }
    assert "secret-db-path" not in str(evidence)


def test_schedule_failure_is_contained_without_raw_details(monkeypatch, caplog) -> None:
    path = _ws_dir() / "workflows" / "resource_monitor.py"
    spec = importlib.util.spec_from_file_location("okgws_resource_monitor_failure", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    monkeypatch.setattr(
        mod,
        "run_resource_monitor",
        lambda _promotion_id=None, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("/private/tmp/secret-dsn-path")
        ),
    )
    # The body now takes the promotion-fence admission decision itself, before
    # the ssh probes, because holding the writer permit across them held a
    # Postgres transaction open for the whole collection (issue #1338). This
    # containment test has no deployment DSN and no business needing one; stub
    # both. The admission call's presence and placement are pinned by
    # tests/okg-workspace/test_resource_monitor_schedule.py.
    from okg.substrate.runtime import promotion_fence

    monkeypatch.setattr(
        mod, "_deployment_dsn", lambda: "postgresql://unused.invalid/x",
    )
    monkeypatch.setattr(
        promotion_fence, "assert_deployment_admitted", lambda **_kwargs: None,
    )

    evidence = inspect.unwrap(mod.okg_workspace_resource_monitor)(None, None)

    assert evidence == {
        "ok": False,
        "error": "Resource monitoring failed.",
        "error_type": "RuntimeError",
    }
    assert "secret-dsn-path" not in str(evidence)
    assert "secret-dsn-path" not in caplog.text
    assert "RuntimeError" in caplog.text

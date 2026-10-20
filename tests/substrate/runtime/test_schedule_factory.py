from __future__ import annotations

from datetime import datetime, timezone
from types import ModuleType
import sys


def test_enqueue_schedule_skips_when_workflow_already_active(
    monkeypatch,
) -> None:
    import dbos

    from okg.substrate.runtime._schedule_factory import make_enqueue_schedule

    module = ModuleType("tests.fake_runtime_workflow")
    module.WORKFLOW_NAME = "okg_fake_sweep"
    monkeypatch.setitem(sys.modules, module.__name__, module)

    decorated: dict[str, object] = {}

    def fake_workflow(*_args, **kwargs):
        def _decorator(fn):
            decorated[str(kwargs["name"])] = fn
            return fn

        return _decorator

    monkeypatch.setattr(dbos.DBOS, "workflow", staticmethod(fake_workflow))
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.active_workflow_id_for_name",
        lambda **kwargs: "wf-active",
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.enqueue_deployment_workflow",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("enqueue should be skipped")
        ),
    )

    register, _apply, _cache = make_enqueue_schedule(
        schedule_id="okg_fake_sweep",
        default_cron="*/1 * * * *",
        workflow_module=module.__name__,
        workflow_name_attr="WORKFLOW_NAME",
        log_label="fake sweep",
        deduplication_id_fn=lambda deployment, **_kwargs: (
            f"fake:{deployment}"
        ),
    )
    register(deployment="demo", dsn="postgres://unused")

    result = decorated["okg_fake_sweep_enqueue"](
        datetime(2026, 6, 2, 15, 0, tzinfo=timezone.utc),
        None,
    )

    assert result == "wf-active"

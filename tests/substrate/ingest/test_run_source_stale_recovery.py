"""`_run_source` recovers from a proven-stale adapter (issue #945).

The unit tests in ``test_source_signature_mismatch.py`` prove the
mechanism. This one proves the WIRING: that the recovery actually runs
where the 177 failures happened — inside ``_run_source`` — and that a
run which recovers reports the fact rather than looking clean.
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.ingest import service as ingest_service
from okg.substrate.ingest.source_construction import (
    SOURCE_ADAPTER_SIGNATURE_MISMATCH,
)


_WITHOUT_DSN = """
    class AliasSource:
        def __init__(self, source_name=None):
            self.source_name = source_name

        last_run_stats = {}
"""

_WITH_DSN = """
    class AliasSource:
        def __init__(self, source_name=None, dsn=None):
            self.source_name = source_name
            self.dsn = dsn

        last_run_stats = {}
"""


class _Result:
    nodes_appended = 3
    edges_appended = 1
    retractions_node = 0
    retractions_edge = 0
    rejected_fact_count = 0
    run_id = "run-1"
    mode = "scope_complete"
    completed_scope = True
    next_cursor = None
    terminal_progress = None
    clear_replay_required = False
    replay_target = None
    timings_ms: dict[str, float] = {}
    source_health = None
    source_delta_batch = None
    trace_id = None
    telemetry_span_id = None

    def graph_write_set_as_dict(self):
        return {}


class _Runner:
    """Stands in for SourceRunner; records the adapter it was handed."""

    handed: list[Any] = []

    def __init__(self, *args, **kwargs):
        pass

    def run(self, adapter, **kwargs):
        _Runner.handed.append(adapter)
        return _Result()


@pytest.fixture
def stale_adapter(tmp_path: Path, monkeypatch):
    """Import an adapter, then change its definition on disk."""
    root = tmp_path / "adapters"
    root.mkdir()
    module_path = root / "cooper_aliases.py"
    module_path.write_text(textwrap.dedent(_WITHOUT_DSN), encoding="utf-8")
    sys.path.insert(0, str(root))
    __import__("cooper_aliases")
    # The commit that landed 84 minutes after the worker booted.
    module_path.write_text(textwrap.dedent(_WITH_DSN), encoding="utf-8")

    _Runner.handed = []
    yield module_path

    for name in list(sys.modules):
        if name == "cooper_aliases":
            sys.modules.pop(name, None)
    sys.path.remove(str(root))


def _wire(monkeypatch) -> None:
    """Stub the collaborators `_run_source` needs that touch a database."""

    class _Context:
        admission_id = "adm-1"
        run_id = "run-1"
        observed_at = None

    monkeypatch.setattr(
        ingest_service,
        "_prepare_source_admission",
        lambda **kwargs: (_Context(), {"workflow_id": "w"}, "accepted"),
    )
    monkeypatch.setattr(
        ingest_service, "_snapshot_source_state", lambda *a, **k: None,
    )
    monkeypatch.setattr(ingest_service, "SourceRunner", _Runner)
    monkeypatch.setattr(
        ingest_service,
        "_coerce_admission_finalize_result",
        lambda value: value,
    )
    monkeypatch.setattr(
        ingest_service,
        "_finalize_source_admission",
        lambda **kwargs: type(
            "Finalize", (), {
                "status": "published",
                "refs": [],
                "rejected": False,
                "reason": None,
                "verdict": {},
            },
        )(),
    )
    monkeypatch.setattr(
        ingest_service, "_fail_source_admission", lambda **kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service, "source_sync_scope_payload", lambda name, events: {},
    )
    monkeypatch.setattr(
        ingest_service,
        "_run_mode_for_source",
        lambda *a, **k: "scope_complete",
    )
    monkeypatch.setattr(
        ingest_service, "source_adapter_graph_branch", lambda entry: "main",
    )
    monkeypatch.setattr(
        ingest_service,
        "import_source_adapter_class",
        lambda entry: getattr(
            sys.modules[entry.raw["module"]], entry.raw["class"],
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "source_adapter_init_params",
        # Mirror the real injector's contract: fill `dsn` when the TARGET
        # class declares it. The recovery path must recompute this
        # against the re-imported class, which is what this exercises.
        lambda entry, *, dsn, deployment=None, graph_branch, adapter_class: (
            _init_params(entry, dsn, adapter_class)
        ),
    )
    monkeypatch.setattr(
        ingest_service, "bind_source_adapter_identity",
        lambda adapter, entry: "fingerprint",
    )
    monkeypatch.setattr(
        ingest_service, "_retract_chronos_run", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        ingest_service, "_restore_source_state", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        ingest_service, "_sanitize_report_fields", lambda result: {},
    )
    # The at-rest privacy posture is resolved on the run stage from the
    # deployment's Nomos policy; this test is about the construct stage.
    monkeypatch.setattr(
        ingest_service,
        "resolve_deployment_source_pii_posture",
        lambda *a, **k: None,
    )


def _init_params(entry, dsn: str, adapter_class: Any) -> dict[str, Any]:
    """The registry's own params, as in the #945 incident.

    `source_registry.yaml` passed `dsn` explicitly there; the adapter the
    worker had loaded did not accept it.
    """
    return dict(entry.raw.get("params") or {})


def _entry(params: dict[str, Any]):
    return type("Entry", (), {
        "raw": {
            "module": "cooper_aliases",
            "class": "AliasSource",
            "params": params,
        },
        "ownership_id": "cooper-aliases",
        "admission_policy": None,
    })()


def _options(tmp_path: Path):
    # The run stage reads the deployment manifest to resolve the source's
    # at-rest privacy posture; a minimal one keeps this test on the
    # construct path it is actually about.
    manifest = tmp_path / "deployment.yaml"
    if not manifest.exists():
        manifest.write_text("name: cooper\n", encoding="utf-8")
    return ingest_service.IngestOptions(
        dsn="postgresql:///unused",
        deployment="cooper",
        deployment_dir=tmp_path,
    )


def test_run_source_recovers_from_stale_adapter(
    stale_adapter, tmp_path, monkeypatch,
) -> None:
    """The cycle succeeds instead of failing on code that no longer exists."""
    _wire(monkeypatch)

    result = ingest_service._run_source(
        _options(tmp_path),
        _entry({"dsn": "postgresql:///cooper"}),
        "cooper-aliases",
    )

    assert result["status"] == "OK", result.get("error")
    # An OK result must NOT carry `error_kind` — a consumer treating its
    # presence as failure would misread a recovered run.
    assert "error_kind" not in result
    recovery = result["stale_module_recovery"]
    assert recovery["error_kind"] == SOURCE_ADAPTER_SIGNATURE_MISMATCH
    assert recovery["cause"] == "stale_loaded_code"
    assert recovery["recovery"]["recovered"] is True
    # The adapter handed to the runner is the FRESH definition.
    adapter = _Runner.handed[-1]
    assert adapter.dsn == "postgresql:///cooper"


def test_recovery_is_what_makes_the_run_succeed(
    stale_adapter, tmp_path, monkeypatch,
) -> None:
    """Vacuity guard for the test above.

    With the retry neutralised, the identical input must FAIL at
    `construct` — i.e. the previous test passes because of the recovery
    path, not because the fixture would have worked anyway.
    """
    _wire(monkeypatch)
    monkeypatch.setattr(
        ingest_service,
        "retry_construction_after_eviction",
        lambda **kwargs: {"recovered": False, "reason": "disabled for test"},
    )

    result = ingest_service._run_source(
        _options(tmp_path),
        _entry({"dsn": "postgresql:///cooper"}),
        "cooper-aliases",
    )

    assert result["status"] == "FAIL"
    assert result["stage"] == "construct"
    assert result["error_kind"] == SOURCE_ADAPTER_SIGNATURE_MISMATCH
    assert result["error_diagnosis"]["cause"] == "stale_loaded_code"
    assert result["error_diagnosis"]["recovery"]["recovered"] is False


def test_run_source_reports_a_genuine_mismatch_without_recovery(
    tmp_path, monkeypatch,
) -> None:
    """No recovery is claimed when the disk definition also rejects it."""
    root = tmp_path / "adapters2"
    root.mkdir()
    (root / "plain_source.py").write_text(
        textwrap.dedent(_WITHOUT_DSN), encoding="utf-8",
    )
    sys.path.insert(0, str(root))
    __import__("plain_source")
    _wire(monkeypatch)
    captured: dict = {}
    monkeypatch.setattr(
        ingest_service,
        "_fail_source_admission",
        lambda **kwargs: captured.update(kwargs),
    )
    try:
        entry = _entry({"unexpected_param": 1})
        entry.raw["module"] = "plain_source"
        result = ingest_service._run_source(
            _options(tmp_path), entry, "plain-source",
        )
    finally:
        sys.modules.pop("plain_source", None)
        sys.path.remove(str(root))

    assert result["status"] == "FAIL"
    assert result["stage"] == "construct"
    assert result["error_kind"] == SOURCE_ADAPTER_SIGNATURE_MISMATCH
    assert result["error_diagnosis"]["cause"] == "registry_adapter_mismatch"
    # And the operator-visible admission record carries the same kind.
    assert captured["diagnosis"]["error_kind"] == (
        SOURCE_ADAPTER_SIGNATURE_MISMATCH
    )
    assert SOURCE_ADAPTER_SIGNATURE_MISMATCH in captured["error"]



def test_a_genuine_mismatch_leads_with_the_shared_param_check(
    tmp_path, monkeypatch,
) -> None:
    """#2571 item 2: legacy ingest names the key the way lint and doctor do.

    Construction still runs first, so the stale-class recovery above keeps
    working; only once the loaded class is proven current does the shared
    check explain the failure.
    """
    root = tmp_path / "adapters3"
    root.mkdir()
    (root / "typo_source.py").write_text(
        textwrap.dedent(_WITHOUT_DSN), encoding="utf-8",
    )
    sys.path.insert(0, str(root))
    __import__("typo_source")
    _wire(monkeypatch)
    captured: dict = {}
    monkeypatch.setattr(
        ingest_service,
        "_fail_source_admission",
        lambda **kwargs: captured.update(kwargs),
    )
    try:
        entry = _entry({"source_nam": "x"})
        entry.raw["module"] = "typo_source"
        result = ingest_service._run_source(
            _options(tmp_path), entry, "typo-source",
        )
    finally:
        sys.modules.pop("typo_source", None)
        sys.path.remove(str(root))

    assert result["status"] == "FAIL"
    assert result["error_kind"] == SOURCE_ADAPTER_SIGNATURE_MISMATCH
    (problem,) = result["error_diagnosis"]["source_param_problems"]
    assert (problem["code"], problem["key"]) == (
        "source_param_unconsumed", "source_nam",
    )
    assert captured["error"].startswith(
        "source_param_unconsumed: source 'typo-source' at "
        "/sources/typo-source/params/source_nam: params key 'source_nam' "
        "is not a parameter of typo_source.AliasSource."
    )
    assert SOURCE_ADAPTER_SIGNATURE_MISMATCH in captured["error"]


def test_admission_failure_is_not_masked_by_uninitialized_diagnostics(
    tmp_path, monkeypatch,
) -> None:
    """An admission error happens before adapter construction metadata exists.

    The failure report must preserve that original error instead of raising an
    ``UnboundLocalError`` while attempting construction-failure diagnosis.
    """
    _wire(monkeypatch)
    monkeypatch.setattr(
        ingest_service,
        "_prepare_source_admission",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("producer signature rejected"),
        ),
    )

    result = ingest_service._run_source(
        _options(tmp_path),
        _entry({}),
        "cooper-aliases",
    )

    assert result["status"] == "FAIL"
    assert result["stage"] == "admission"
    assert result["error"] == "RuntimeError: producer signature rejected"
    assert "error_kind" not in result

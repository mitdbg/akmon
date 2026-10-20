from __future__ import annotations

import builtins
import sys

from okg.substrate.publisher import publisher


def test_emit_phase_event_does_not_import_dbos_when_absent(
    monkeypatch,
) -> None:
    monkeypatch.delitem(sys.modules, "dbos", raising=False)
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "dbos":
            raise AssertionError("inline phase event imported dbos")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    publisher._emit_phase_event("probe", {"ok": True})


def test_emit_phase_event_skips_when_dbos_has_no_workflow(
    monkeypatch,
) -> None:
    calls: list[tuple[str, dict]] = []

    class FakeDBOS:
        workflow_id = None

        @staticmethod
        def set_event(name: str, payload: dict) -> None:
            calls.append((name, payload))

    monkeypatch.setitem(sys.modules, "dbos", type(
        "FakeDbosModule",
        (),
        {"DBOS": FakeDBOS},
    )())

    publisher._emit_phase_event("probe", {"ok": True})

    assert calls == []


def test_emit_phase_event_uses_loaded_dbos_workflow(
    monkeypatch,
) -> None:
    calls: list[tuple[str, dict]] = []

    class FakeDBOS:
        workflow_id = "wf-1"

        @staticmethod
        def set_event(name: str, payload: dict) -> None:
            calls.append((name, payload))

    monkeypatch.setitem(sys.modules, "dbos", type(
        "FakeDbosModule",
        (),
        {"DBOS": FakeDBOS},
    )())

    publisher._emit_phase_event("probe", {"ok": True})

    assert calls == [("probe", {"ok": True})]

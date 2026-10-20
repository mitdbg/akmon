"""The probe gate decides as the caller, with no default subject (#2361).

`build_probe_gate` used to evaluate ``principal or "service:okg-runtime"``, so
a probe reached with no principal was authorized as the runtime service. Now
an empty principal is refused before the bundle is asked, and a named caller
is decided as itself. No database: a bundle on disk and the gate function.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.external_backed import gating
from okg.substrate.external_backed.gating import build_probe_gate
from okg.substrate.external_backed.handle import HANDLE_ATTR, parse_handle

_DEPLOYMENT = "probe-gate-principal"


def _field():
    handle = parse_handle({HANDLE_ATTR: {"status": {
        "current": "probe", "adapter": {"kind": "sql", "config": {"query": "SELECT 1"}},
        "ttl_seconds": 60, "cache_policy": "never-cache",
        "access": {"permission": "external.probe.invoke", "secret_refs": []},
    }}})
    assert handle is not None
    return handle.fields["status"]


def _root_allowing(tmp_path: Path, subject: str) -> Path:
    deployment = tmp_path / _DEPLOYMENT
    deployment.mkdir()
    (deployment / gating.BUNDLE_FILENAME).write_text(json.dumps({
        "policy_bundle_id": "probe-gate-principal",
        "policy_version": "1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow-one-subject",
            "effect": "allow",
            "priority": 100,
            "actions": [gating.PROBE_ACTION],
            "subjects": [subject],
            "match": {"deployment": _DEPLOYMENT},
        }],
    }), encoding="utf-8")
    return tmp_path


def test_the_default_subject_is_gone() -> None:
    assert not hasattr(gating, "DEFAULT_PRINCIPAL")


@pytest.mark.parametrize("principal", [None, "", "   "])
def test_an_empty_principal_is_refused_even_when_the_runtime_is_allowed(
    tmp_path: Path, principal, monkeypatch,
) -> None:
    import okg.substrate.nomos.core as core

    root = _root_allowing(tmp_path, "service:okg-runtime")
    evaluated: list[object] = []
    real = core.evaluate
    # Installed BEFORE the gate is built: build_probe_gate binds `evaluate`
    # into the gate's closure when it runs.
    monkeypatch.setattr(core, "evaluate", lambda *a, **k: evaluated.append(a) or real(*a, **k))
    gate = build_probe_gate(_DEPLOYMENT, deployments_root=root)
    assert gate(_field(), principal) is False
    assert evaluated == []  # refused before the bundle was asked
    # Positive control: a named caller does reach the bundle, exactly once.
    gate(_field(), "subject:probe-caller")
    assert len(evaluated) == 1


def test_a_named_caller_is_decided_as_itself(tmp_path: Path) -> None:
    root = _root_allowing(tmp_path, "service:okg-runtime")
    gate = build_probe_gate(_DEPLOYMENT, deployments_root=root)
    assert gate(_field(), "subject:probe-caller") is False
    assert gate(_field(), "service:okg-runtime") is True

"""The operator UI boundary builds its policy attributes from fixed fields (#2115).

These cases need no database: the Nomos evaluator is stubbed to capture the
request it would have judged. The defect they guard against is a request
parameter that becomes a policy attribute -- one that overwrites
``resource_kind`` or ``read``, or collides with a fact the policy reads and
changes every decision (a ``source`` key once made every console search a
``nomos.invalid_identity_fact`` denial).
"""
from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

import okg.substrate.nomos as nomos
from okg.substrate.operator_read import (
    LOCAL_OPERATOR_BOUNDARY_KIND,
    LOCAL_OPERATOR_RESOURCE_ATTRIBUTES,
    RESERVED_OPERATOR_REQUEST_KEYS,
    OperatorReadRequestKeyReserved,
    OperatorReadRequestUnencodable,
    authorize_local_operator_read,
)
from okg.viz import record_endpoints

VIZ = Path(__file__).resolve().parents[3] / "src" / "okg" / "viz"
ROUTE_MODULES = (VIZ / "record_endpoints.py", VIZ / "graph_endpoints.py")


class _Session:
    generation_id = "okg:demo:branch:main:gen:1"
    branch_name = "main"
    catalog_version_id = "cat:1"


def _capture(monkeypatch: pytest.MonkeyPatch, *, required: bool = True) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _evaluate(_conn: Any, **kwargs: Any) -> None:
        calls.append(kwargs)
        return None

    monkeypatch.setattr(nomos, "deployment_requires_nomos_runtime", lambda _d: required)
    monkeypatch.setattr(nomos, "evaluate_runtime_boundary_with_active_policy", _evaluate)
    return calls


def _authorize(request: dict[str, Any], *, read: str = "search") -> Any:
    return authorize_local_operator_read(
        object(),
        deployment="demo",
        session=_Session(),
        read=read,
        request=request,
        subject="subject:human_operator",
    )


def test_request_keys_never_become_policy_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _capture(monkeypatch)
    request = {"query": "alpha", "type": None, "source_filter": "docs", "limit": 5, "offset": 0}

    assert _authorize(request) is None

    (call,) = calls
    resource = dict(call["request"].resource)
    assert tuple(resource) == LOCAL_OPERATOR_RESOURCE_ATTRIBUTES
    assert resource["resource_kind"] == LOCAL_OPERATOR_BOUNDARY_KIND
    assert resource["read"] == "search"
    assert not set(request) & set(resource)
    assert call["target"]["request"] == request
    assert call["target"]["read"] == "search"


def test_the_issue_repro_cannot_spoof_resource_kind_or_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #2115 repro: before the fix the evaluator saw resource_kind=spoofed."""
    calls = _capture(monkeypatch)

    with pytest.raises(OperatorReadRequestKeyReserved) as excinfo:
        _authorize({"source": "x", "resource_kind": "spoofed"})

    assert calls == []
    assert excinfo.value.code == "operator_read_request_key_reserved"
    assert excinfo.value.data == {"read": "search", "reserved_keys": ["resource_kind", "source"]}


@pytest.mark.parametrize("key", sorted(RESERVED_OPERATOR_REQUEST_KEYS))
@pytest.mark.parametrize("required", [True, False])
def test_every_reserved_key_is_refused_before_the_policy_runs(
    monkeypatch: pytest.MonkeyPatch, key: str, required: bool,
) -> None:
    """Fail closed even where the runtime is off: the route bug shows everywhere."""
    calls = _capture(monkeypatch, required=required)

    with pytest.raises(OperatorReadRequestKeyReserved, match=key):
        _authorize({"query": "alpha", key: "spoofed"}, read="list")

    assert calls == []


def test_distinct_requests_stay_distinct_decisions(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _capture(monkeypatch)

    _authorize({"query": "alpha", "limit": 5})
    _authorize({"limit": 5, "query": "alpha"})
    _authorize({"query": "beta", "limit": 5})

    digests = [call["request"].resource["request_digest"] for call in calls]
    assert digests[0] == digests[1]
    assert digests[0] != digests[2]


def test_a_request_value_that_cannot_be_recorded_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _capture(monkeypatch)

    with pytest.raises(OperatorReadRequestUnencodable) as excinfo:
        _authorize({"query": object(), "limit": 5})

    assert calls == []
    assert excinfo.value.code == "operator_read_request_unencodable"
    assert excinfo.value.data == {
        "read": "search", "unencodable": {"query": "object"}, "non_string_keys": [],
    }


@pytest.mark.parametrize(
    ("request_params", "code"),
    [
        ({"query": "alpha", "resource_kind": "spoofed"}, "operator_read_request_key_reserved"),
        ({"query": object()}, "operator_read_request_unencodable"),
    ],
)
def test_a_route_bug_is_a_server_fault_not_missing_authority(
    monkeypatch: pytest.MonkeyPatch, request_params: dict[str, Any], code: str,
) -> None:
    """A 503 tells the operator to check the publish; this needs a code fix."""
    calls = _capture(monkeypatch)

    with pytest.raises(HTTPException) as excinfo:
        record_endpoints._authorize_or_raise(
            object(),
            deployment="demo",
            session=_Session(),
            read="search",
            request=request_params,
            subject="subject:human_operator",
            subject_meta=None,
            version=_Session.generation_id,
        )

    assert calls == []
    assert excinfo.value.status_code == 500
    detail = excinfo.value.detail
    assert detail["reason"] == code
    assert detail["version"] == _Session.generation_id
    assert "next_check" not in detail
    assert "object at 0x" not in detail["error"]


def test_no_console_route_sends_a_reserved_request_key() -> None:
    sent: set[str] = set()
    for module in ROUTE_MODULES:
        tree = ast.parse(module.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg in {"request", "request_facts"} and isinstance(
                    keyword.value, ast.Dict,
                ):
                    sent.update(
                        key.value for key in keyword.value.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    )

    assert {"query", "source_filter", "record_id", "branch"} <= sent
    assert not sent & RESERVED_OPERATOR_REQUEST_KEYS

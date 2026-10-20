"""req.external-backed-nodes.handle-contract.

The handle is an in-attrs typed block validated by generic code with no
deployment-name branch: it accepts well-formed handles and rejects missing
backing, bad size/cache policy, and inline secret values (references only).
"""
from __future__ import annotations

import pytest

from okg.substrate.external_backed import (
    HANDLE_ATTR,
    ExternalHandleError,
    backing_fingerprint,
    parse_handle,
)

# Pure validation — no DB.


def _field(**over) -> dict:
    base = {
        "current": "latest_from_series",
        "series": {"store": "timescaledb", "metric": "gpu_util"},
        "ttl_seconds": 60,
        "cache_policy": "cache-first",
        "size": "inline",
        "access": {"secret_refs": ["env:PROBE_TOKEN"]},
        "default_hydration": "opt_in",
    }
    base.update(over)
    return base


def _attrs(field_spec: dict, name: str = "metrics") -> dict:
    return {"label": "gpu07", HANDLE_ATTR: {name: field_spec}}


def test_no_handle_returns_none() -> None:
    assert parse_handle({"label": "x"}) is None
    assert parse_handle(None) is None


def test_valid_latest_from_series_handle() -> None:
    handle = parse_handle(_attrs(_field()))
    assert handle is not None
    f = handle.fields["metrics"]
    assert f.current == "latest_from_series"
    assert f.series["metric"] == "gpu_util"
    assert f.requires_privilege is False


def test_valid_probe_handle() -> None:
    handle = parse_handle(_attrs(_field(
        current="probe",
        adapter={"kind": "sql", "config": {"query": "SELECT 1"}},
        access={"permission": "external.probe.invoke", "secret_refs": []},
    )))
    f = handle.fields["metrics"]
    assert f.current == "probe"
    assert f.requires_privilege is True
    assert f.permission == "external.probe.invoke"


def test_latest_from_series_requires_metric() -> None:
    with pytest.raises(ExternalHandleError) as exc:
        parse_handle(_attrs(_field(series={"store": "timescaledb"})))
    assert exc.value.field_name == "metrics"


def test_probe_requires_adapter_kind() -> None:
    with pytest.raises(ExternalHandleError):
        parse_handle(_attrs(_field(current="probe", adapter={})))


@pytest.mark.parametrize("bad", [
    {"current": "guessing"},
    {"cache_policy": "whenever"},
    {"size": "enormous"},
    {"default_hydration": "always_maybe"},
    {"ttl_seconds": 0},
])
def test_bad_field_values_rejected(bad: dict) -> None:
    with pytest.raises(ExternalHandleError):
        parse_handle(_attrs(_field(**bad)))


def test_inline_secret_value_rejected_mapping() -> None:
    with pytest.raises(ExternalHandleError) as exc:
        parse_handle(_attrs(_field(
            access={"secret_refs": [{"name": "TOK", "value": "hunter2"}]}
        )))
    assert "secret" in str(exc.value).lower()


def test_inline_secret_value_rejected_inline_string() -> None:
    with pytest.raises(ExternalHandleError):
        parse_handle(_attrs(_field(access={"secret_refs": ["TOK=hunter2"]})))


def test_secret_reference_names_accepted() -> None:
    handle = parse_handle(_attrs(_field(
        access={"secret_refs": ["env:TOK", "secret:vault/probe"]}
    )))
    assert handle.fields["metrics"].secret_refs == ("env:TOK", "secret:vault/probe")


def test_empty_handle_block_rejected() -> None:
    with pytest.raises(ExternalHandleError):
        parse_handle({HANDLE_ATTR: {}})


def test_no_deployment_name_branch() -> None:
    # Same attrs validate identically regardless of any deployment context —
    # parse_handle takes no deployment argument at all.
    a = parse_handle(_attrs(_field()))
    b = parse_handle(_attrs(_field()))
    assert a.fields["metrics"].current == b.fields["metrics"].current


def test_backing_fingerprint_distinguishes_divergent_handles() -> None:
    a = parse_handle(_attrs(_field(adapter={}))).fields["metrics"]
    b = parse_handle(_attrs(_field(
        current="probe", adapter={"kind": "sql", "config": {"query": "SELECT 1"}},
        access={"permission": "p", "secret_refs": []},
    ))).fields["metrics"]
    assert backing_fingerprint(a) != backing_fingerprint(b)
    # Stable for the same backing.
    assert backing_fingerprint(a) == backing_fingerprint(
        parse_handle(_attrs(_field(adapter={}))).fields["metrics"]
    )

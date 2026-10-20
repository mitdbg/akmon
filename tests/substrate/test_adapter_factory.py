from __future__ import annotations

from dataclasses import replace
import hashlib
import inspect
import json
from typing import Any

import pytest

from okg.substrate.ingest.adapter_factory import (
    construct_source_adapter,
    plan_source_adapter,
    source_adapter_binding_fingerprint,
    source_adapter_executable_profile,
    source_adapter_init_params,
)
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    SourceEntry,
)


def _entry(*, name: str) -> SourceEntry:
    return SourceEntry(
        name=name,
        ownership_id="source:shared",
        source_class=None,
        record_identity_kind=None,
        record_identity_fields=[],
        source_revision_kind=None,
        deletion_semantics=None,
        publication_mode=None,
        credential_refs=[],
        credential_aliases={},
        required_for_baseline=False,
        raw={
            "module": "example.sources",
            "class": "ExampleSource",
            "params": {"root": "/same/root"},
        },
    )


def test_registry_source_name_is_part_of_adapter_binding_identity() -> None:
    first = _entry(name="first-source")
    renamed = replace(first, name="renamed-source")

    assert source_adapter_binding_fingerprint(
        first
    ) != source_adapter_binding_fingerprint(renamed)


def test_worker_deployment_identity_cannot_be_overridden_by_registry_params() -> None:
    class Adapter:
        def __init__(self, *, deployment: str, root: str) -> None:
            self.deployment = deployment
            self.root = root

    entry = replace(
        _entry(name="source"),
        raw={
            "module": "example.sources",
            "class": "ExampleSource",
            "params": {"root": "/same/root", "deployment": "redirected"},
        },
    )
    with pytest.raises(ValueError, match="worker-owned"):
        source_adapter_init_params(
            entry,
            dsn="postgresql://worker/db",
            deployment="fixture",
            adapter_class=Adapter,
        )

    with pytest.raises(ValueError, match="requires worker authority"):
        source_adapter_init_params(
            entry,
            dsn="postgresql://worker/db",
            deployment=None,
            adapter_class=Adapter,
        )

    entry = replace(
        entry,
        raw={
            **entry.raw,
            "params": {"root": "/same/root", "deployment": "fixture"},
        },
    )
    assert source_adapter_init_params(
        entry,
        dsn="postgresql://worker/db",
        deployment="fixture",
        adapter_class=Adapter,
    ) == {"root": "/same/root", "deployment": "fixture"}


def test_unmarked_binding_fingerprint_preserves_legacy_v1_bytes() -> None:
    entry = _entry(name="first-source")
    payload = {
        "schema": "okg.source_adapter.binding.v1",
        "source_name": entry.name,
        "ownership_id": entry.ownership_id,
        "module": entry.raw["module"],
        "class": entry.raw["class"],
        "params": entry.raw["params"],
    }
    expected = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()

    assert source_adapter_binding_fingerprint(entry) == expected


@pytest.mark.parametrize(
    "raw",
    [
        {"module": "example.sources", "class": "ExampleSource"},
        {
            "module": "example.sources",
            "class": "ExampleSource",
            "params": None,
        },
        {
            "module": "example.sources",
            "class": "ExampleSource",
            "params": {},
        },
    ],
    ids=["absent", "null", "empty"],
)
def test_unmarked_empty_params_preserve_legacy_v1_bytes(
    raw: dict[str, Any],
) -> None:
    entry = replace(_entry(name="first-source"), raw=raw)
    payload = {
        "schema": "okg.source_adapter.binding.v1",
        "source_name": entry.name,
        "ownership_id": entry.ownership_id,
        "module": raw["module"],
        "class": raw["class"],
        "params": {},
    }
    expected = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()

    assert source_adapter_binding_fingerprint(entry) == expected


def test_completed_scope_deletion_policy_changes_legacy_binding() -> None:
    append_only = _entry(name="first-source")
    completed_scope = replace(
        append_only,
        deletion_semantics="missing_from_completed_scope",
    )
    explicit_delete = replace(
        append_only,
        deletion_semantics="explicit_delete",
    )

    append_fingerprint = source_adapter_binding_fingerprint(append_only)
    assert source_adapter_binding_fingerprint(completed_scope) != (
        append_fingerprint
    )
    assert source_adapter_binding_fingerprint(explicit_delete) not in {
        append_fingerprint,
        source_adapter_binding_fingerprint(completed_scope),
    }


def _strict_entry(*, params: object = None) -> SourceEntry:
    entry = _entry(name="strict-source")
    raw = dict(entry.raw)
    if params is not None:
        raw["params"] = params
    else:
        raw.pop("params", None)
    return replace(
        entry,
        source_class="mutable_api",
        record_identity_kind="remote_id",
        record_identity_fields=("remote_id",),
        source_revision_kind="updated_at",
        deletion_semantics="explicit_delete",
        publication_mode="published_generation",
        raw=raw,
        admission_contract=STRICT_ADMISSION_CONTRACT,
    )


class _StrictAdapter:
    profile = "mutable_api"

    def __init__(
        self,
        *,
        root: str = "/default",
        dsn: str = "",
        source_name: str = "",
        graph_branch: str = "",
    ) -> None:
        self.root = root
        self.dsn = dsn
        self.source_name = source_name
        self.graph_branch = graph_branch


@pytest.mark.parametrize(
    "reserved",
    [
        "dsn",
        "deployment",
        "source_name",
        "graph_branch",
        "name",
        "profile",
        "change_probe_kind",
        "run_id",
        "mode",
        "sync_scope",
    ],
)
def test_strict_authored_authority_is_rejected(reserved: str) -> None:
    with pytest.raises(ValueError, match=rf"source_param_reserved.*{reserved}"):
        source_adapter_init_params(
            _strict_entry(params={reserved: "operator"}),
            dsn="postgresql://deployment/db",
            adapter_class=_StrictAdapter,
        )


def test_strict_plan_injects_worker_deployment_identity() -> None:
    class DeploymentAdapter:
        profile = "mutable_api"

        def __init__(self, *, deployment: str) -> None:
            self.deployment = deployment

    plan = plan_source_adapter(
        _strict_entry(),
        dsn="postgresql://deployment/db",
        deployment="fixture",
        adapter_class=DeploymentAdapter,
    )
    assert dict(plan.init_params) == {"deployment": "fixture"}

    with pytest.raises(ValueError, match="requires worker authority"):
        plan_source_adapter(
            _strict_entry(),
            dsn="postgresql://deployment/db",
            adapter_class=DeploymentAdapter,
        )


@pytest.mark.parametrize("params", [None, [], "root=/tmp"])
def test_strict_explicit_non_mapping_params_are_rejected(
    params: object,
) -> None:
    entry = _strict_entry(params={})
    entry = replace(entry, raw={**entry.raw, "params": params})
    with pytest.raises(ValueError, match="source_params_not_mapping"):
        source_adapter_init_params(
            entry,
            dsn="postgresql://deployment/db",
            adapter_class=_StrictAdapter,
        )


def test_strict_top_level_param_keys_must_be_strings() -> None:
    with pytest.raises(ValueError, match="source_param_key_invalid"):
        source_adapter_init_params(
            _strict_entry(params={1: "not-a-constructor-name"}),
            dsn="postgresql://deployment/db",
            adapter_class=_StrictAdapter,
        )


def test_strict_kwargs_is_not_consumption_proof() -> None:
    class KwargsOnly:
        profile = "mutable_api"

        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    with pytest.raises(ValueError, match="source_param_unconsumed.*root"):
        source_adapter_init_params(
            _strict_entry(params={"root": "/tmp"}),
            dsn="postgresql://deployment/db",
            adapter_class=KwargsOnly,
        )


def test_strict_plan_rejects_missing_required_constructor_argument() -> None:
    class RequiredArgumentAdapter:
        profile = "mutable_api"

        def __init__(self, *, required_domain_value: str) -> None:
            self.required_domain_value = required_domain_value

    with pytest.raises(
        ValueError,
        match=r"source_param_missing: .*'required_domain_value'",
    ):
        plan_source_adapter(
            _strict_entry(),
            dsn="postgresql://deployment/db",
            adapter_class=RequiredArgumentAdapter,
        )


def test_strict_plan_rejects_uninspectable_constructor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = inspect.signature

    def fail_for_strict_adapter(value: object):
        if value is _StrictAdapter:
            raise ValueError("uninspectable")
        return original(value)

    monkeypatch.setattr(
        "okg.substrate.ingest.adapter_factory.inspect.signature",
        fail_for_strict_adapter,
    )
    with pytest.raises(ValueError, match="source_constructor_uninspectable"):
        plan_source_adapter(
            _strict_entry(),
            dsn="postgresql://deployment/db",
            adapter_class=_StrictAdapter,
        )


def test_strict_binding_fingerprint_preserves_value_and_scalar_type() -> None:
    first = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": "abcdefghijklmnopqrstuvwx"}),
        adapter_class=_StrictAdapter,
    )
    different_value = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": "zyxwvutsrqponmlkjihgfedc"}),
        adapter_class=_StrictAdapter,
    )
    boolean_value = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": True}),
        adapter_class=_StrictAdapter,
    )
    string_value = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": "True"}),
        adapter_class=_StrictAdapter,
    )

    assert first != different_value
    assert boolean_value != string_value


def test_strict_binding_fingerprint_excludes_secret_values() -> None:
    first = _strict_entry(params={
        "api_token": "first-short-secret",
        "nested": {"password": "first-password", "region": "east"},
    })
    rotated = _strict_entry(params={
        "api_token": "second-short-secret",
        "nested": {"password": "second-password", "region": "east"},
    })
    changed_config = _strict_entry(params={
        "api_token": "second-short-secret",
        "nested": {"password": "second-password", "region": "west"},
    })

    first_fingerprint = source_adapter_binding_fingerprint(first)
    assert first_fingerprint == source_adapter_binding_fingerprint(rotated)
    assert first_fingerprint != source_adapter_binding_fingerprint(
        changed_config
    )


def test_strict_binding_fingerprint_preserves_nested_mapping_key_types() -> None:
    integer_key = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": {1: "value"}})
    )
    string_key = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": {"1": "value"}})
    )
    both_keys = source_adapter_binding_fingerprint(
        _strict_entry(params={"root": {1: "value", "1": "value"}})
    )

    assert integer_key != string_key
    assert both_keys not in {integer_key, string_key}


def test_strict_binding_uses_registry_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.ingest.adapter_factory.import_source_adapter_class",
        lambda _entry: _StrictAdapter,
    )
    constructed = construct_source_adapter(
        _strict_entry(params={"root": "/operator"}),
        dsn="postgresql://deployment/db",
    )

    constructed.adapter.profile = "append_feed"
    assert constructed.executable_profile == "mutable_api"
    assert constructed.registry_name == "strict-source"
    assert constructed.ownership_id == "source:shared"
    assert constructed.adapter.dsn == "postgresql://deployment/db"
    assert constructed.adapter.source_name == "source:shared"
    assert constructed.registry_binding_fingerprint


def test_strict_registry_and_class_profile_must_match() -> None:
    with pytest.raises(ValueError, match="source_class_profile_mismatch"):
        source_adapter_executable_profile(
            replace(_strict_entry(), source_class="discovery_crawl"),
            _StrictAdapter,
        )

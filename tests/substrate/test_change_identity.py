from __future__ import annotations

import json
import types
from dataclasses import dataclass

import pytest

from okg.substrate.ingest.change_identity import (
    BOOTSTRAP_AUTHORITY_CURSOR_KEY,
    EFFECTIVE_TOKEN_SCHEMA,
    PipelineIdentitySnapshot,
    bootstrap_authority_from_cursor,
    build_bootstrap_authority,
    build_effective_token,
    effective_token_from_cursor,
    raw_token_cursor,
)
from okg.substrate.ingest.pii_posture import PiiPosture
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    compose_token,
)


@dataclass(frozen=True)
class _Canonicalizer:
    alias: str


@dataclass
class _MutableCanonicalizer:
    aliases: dict[str, str]
    id_family: str = "repo"

    def canonicalize(self, raw_id: str) -> str:
        return self.aliases.get(raw_id, raw_id)


@dataclass
class _AlternateCanonicalizer:
    aliases: dict[str, str]
    id_family: str = "repo"

    def canonicalize(self, raw_id: str) -> str:
        return self.aliases.get(raw_id.lower(), raw_id)


_GLOBAL_CLASSIFICATIONS = {"email": "non_pii"}
_METHOD_GLOBALS = {
    "canonical_prefix": "canonical-v1",
    "pseudonym_prefix": "pseudonym-v1",
    "detector_label": "detector-v1",
}
_UNFREEZABLE_GLOBAL = object()
_MODULE_HELPER_STATE = {"email": "non_pii"}


def _module_classify(key: str) -> str | None:
    return _MODULE_HELPER_STATE.get(key)


_HELPER_MODULE = types.ModuleType("test_pipeline_helper")
_HELPER_MODULE.classify = _module_classify


def _global_classification_helper(key: str) -> str | None:
    return _GLOBAL_CLASSIFICATIONS.get(key)


def _global_helper_classifier(key: str) -> str | None:
    return _global_classification_helper(key)


def _module_classifier(key: str) -> str | None:
    return _HELPER_MODULE.classify(key)


def _json_classifier(key: str) -> str | None:
    return json.dumps({"key": key}, sort_keys=True)


class _GlobalCanonicalizer:
    id_family = "repo"

    def canonicalize(self, raw_id: str) -> str:
        return f"{_METHOD_GLOBALS['canonical_prefix']}:{raw_id}"


class _GlobalPseudonymService:
    def pseudonym(
        self,
        *,
        canonical: str,
        entity_type: str | None = None,
    ) -> str:
        del entity_type
        return f"{_METHOD_GLOBALS['pseudonym_prefix']}:{canonical}"


class _GlobalDetector:
    def detect(self, text: str) -> list[str]:
        return [f"{_METHOD_GLOBALS['detector_label']}:{text}"]


def _snapshot(**overrides) -> PipelineIdentitySnapshot:
    values = {
        "canonicalizers": (_Canonicalizer("repo-v1"),),
        "volatile_fields": (("Document", ("observed_at",)),),
        "nomos_policy_fingerprint": "nomos-v1",
        "pii_posture": {
            "store_raw": False,
            "epoch": "e1",
            "label_width": 32,
        },
        "registry_binding_fingerprint": "registry-v1",
        "transform_identity": {"runner_transform": "v1"},
    }
    values.update(overrides)
    return PipelineIdentitySnapshot.build(**values)


@pytest.mark.parametrize(
    "mutation",
    [
        {"canonicalizers": (_Canonicalizer("repo-v2"),)},
        {"volatile_fields": (("Document", ("observed_at", "run_id")),)},
        {"nomos_policy_fingerprint": "nomos-v2"},
        {
            "pii_posture": {
                "store_raw": False,
                "epoch": "e2",
                "label_width": 32,
            }
        },
        {
            "pii_posture": {
                "store_raw": False,
                "epoch": "e1",
                "label_width": 40,
            }
        },
        {"registry_binding_fingerprint": "registry-v2"},
        {"transform_identity": {"runner_transform": "v2"}},
    ],
)
def test_each_pipeline_dependency_changes_effective_identity(
    mutation: dict,
) -> None:
    assert _snapshot(**mutation).fingerprint != _snapshot().fingerprint


def test_effective_token_retains_exact_raw_adapter_token() -> None:
    raw = {
        "schema": "okg.source.change_probe.token.v1",
        "components": {"content": {"kind": "content_hash", "value": "a" * 64}},
    }
    effective = build_effective_token(raw, _snapshot())
    cursor = {CURSOR_TOKEN_KEY: effective, "adapter_cursor": "opaque"}

    assert effective["schema"] == EFFECTIVE_TOKEN_SCHEMA
    assert effective["raw_token"] == raw
    assert effective_token_from_cursor(cursor) == effective
    assert raw_token_cursor(cursor) == {
        CURSOR_TOKEN_KEY: raw,
        "adapter_cursor": "opaque",
    }


def test_snapshot_detaches_mutable_canonicalizer_state() -> None:
    canonicalizer = _MutableCanonicalizer({"repo:raw": "repo:canonical"})
    snapshot = _snapshot(canonicalizers=(canonicalizer,))

    canonicalizer.aliases["repo:raw"] = "repo:changed"

    assert (
        snapshot.canonicalizers[0].canonicalize("repo:raw")
        == "repo:canonical"
    )
    assert snapshot.is_intact()


def test_canonicalizer_implementation_changes_pipeline_identity() -> None:
    aliases = {"repo:raw": "repo:canonical"}

    assert _snapshot(
        canonicalizers=(_MutableCanonicalizer(aliases),)
    ).fingerprint != _snapshot(
        canonicalizers=(_AlternateCanonicalizer(aliases),)
    ).fingerprint


def test_snapshot_detaches_classifier_closure_state() -> None:
    declared = {"email": "non_pii"}

    def classifier(key: str) -> str | None:
        return declared.get(key)

    posture = PiiPosture(
        store_raw=False,
        classifier=classifier,
        pseudonym_service=None,
        detector=None,
    )
    snapshot = _snapshot(pii_posture=posture)

    declared["email"] = "sensitive_attribute"

    assert snapshot.pii_posture.classifier("email") == "non_pii"
    assert snapshot.is_intact()


def test_snapshot_detaches_classifier_global_and_nested_helper_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_global_helper_classifier,
        pseudonym_service=None,
        detector=None,
    )
    snapshot = _snapshot(pii_posture=posture)

    monkeypatch.setitem(
        _GLOBAL_CLASSIFICATIONS,
        "email",
        "sensitive_attribute",
    )
    monkeypatch.setitem(
        globals(),
        "_global_classification_helper",
        lambda _key: "direct_identifier",
    )

    assert snapshot.pii_posture.classifier("email") == "non_pii"
    assert snapshot.is_intact()


def test_snapshot_detaches_nested_closure_callable_state() -> None:
    declared = {"email": "non_pii"}

    def helper(key: str) -> str | None:
        return declared.get(key)

    def classifier(key: str) -> str | None:
        return helper(key)

    posture = PiiPosture(
        store_raw=False,
        classifier=classifier,
        pseudonym_service=None,
        detector=None,
    )
    snapshot = _snapshot(pii_posture=posture)

    declared["email"] = "sensitive_attribute"

    assert snapshot.pii_posture.classifier("email") == "non_pii"
    assert snapshot.is_intact()


def test_snapshot_detaches_module_helper_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_module_classifier,
        pseudonym_service=None,
        detector=None,
    )
    snapshot = _snapshot(pii_posture=posture)

    monkeypatch.setitem(
        _MODULE_HELPER_STATE,
        "email",
        "sensitive_attribute",
    )
    monkeypatch.setattr(
        _HELPER_MODULE,
        "classify",
        lambda _key: "direct_identifier",
    )

    assert snapshot.pii_posture.classifier("email") == "non_pii"
    assert snapshot.is_intact()


def test_snapshot_captures_trusted_runtime_module_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_json_classifier,
        pseudonym_service=None,
        detector=None,
    )
    snapshot = _snapshot(pii_posture=posture)

    original_dumps = json.dumps
    monkeypatch.setattr(json, "dumps", lambda *_args, **_kwargs: "changed")

    assert snapshot.pii_posture.classifier("email") == '{"key": "email"}'
    monkeypatch.setattr(json, "dumps", original_dumps)
    assert snapshot.is_intact()


def test_global_classifier_state_changes_pipeline_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_global_helper_classifier,
        pseudonym_service=None,
        detector=None,
    )
    original = _snapshot(pii_posture=posture)

    monkeypatch.setitem(
        _GLOBAL_CLASSIFICATIONS,
        "email",
        "sensitive_attribute",
    )
    changed = _snapshot(pii_posture=posture)

    assert original.fingerprint != changed.fingerprint


def test_snapshot_integrity_detects_frozen_classifier_global_mutation() -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_global_helper_classifier,
        pseudonym_service=None,
        detector=None,
    )
    snapshot = _snapshot(pii_posture=posture)
    frozen_helper = snapshot.pii_posture.classifier.__globals__[
        "_global_classification_helper"
    ]

    frozen_helper.__globals__["_GLOBAL_CLASSIFICATIONS"]["email"] = (
        "sensitive_attribute"
    )

    assert not snapshot.is_intact()


def test_snapshot_detaches_all_bound_method_globals_across_aba(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_global_helper_classifier,
        pseudonym_service=_GlobalPseudonymService(),
        detector=_GlobalDetector(),
    )
    snapshot = _snapshot(
        canonicalizers=(_GlobalCanonicalizer(),),
        pii_posture=posture,
    )

    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "canonical_prefix",
        "canonical-v2",
    )
    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "pseudonym_prefix",
        "pseudonym-v2",
    )
    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "detector_label",
        "detector-v2",
    )

    assert snapshot.canonicalizers[0].canonicalize("raw") == (
        "canonical-v1:raw"
    )
    assert snapshot.pii_posture.pseudonym_service.pseudonym(
        canonical="raw"
    ) == "pseudonym-v1:raw"
    assert snapshot.pii_posture.detector.detect("raw") == [
        "detector-v1:raw"
    ]
    assert snapshot.is_intact()

    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "canonical_prefix",
        "canonical-v1",
    )
    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "pseudonym_prefix",
        "pseudonym-v1",
    )
    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "detector_label",
        "detector-v1",
    )

    assert snapshot.canonicalizers[0].canonicalize("raw") == (
        "canonical-v1:raw"
    )
    assert snapshot.pii_posture.pseudonym_service.pseudonym(
        canonical="raw"
    ) == "pseudonym-v1:raw"
    assert snapshot.pii_posture.detector.detect("raw") == [
        "detector-v1:raw"
    ]
    assert snapshot.is_intact()


def test_bound_method_global_state_changes_pipeline_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posture = PiiPosture(
        store_raw=False,
        classifier=_global_helper_classifier,
        pseudonym_service=_GlobalPseudonymService(),
        detector=_GlobalDetector(),
    )
    original = _snapshot(
        canonicalizers=(_GlobalCanonicalizer(),),
        pii_posture=posture,
    )

    monkeypatch.setitem(
        _METHOD_GLOBALS,
        "canonical_prefix",
        "canonical-v2",
    )
    changed = _snapshot(
        canonicalizers=(_GlobalCanonicalizer(),),
        pii_posture=posture,
    )

    assert original.fingerprint != changed.fingerprint


def test_classifier_with_unfreezable_global_fails_closed() -> None:
    def classifier(_key: str) -> str | None:
        return str(_UNFREEZABLE_GLOBAL)

    posture = PiiPosture(
        store_raw=False,
        classifier=classifier,
        pseudonym_service=None,
        detector=None,
    )

    with pytest.raises(ValueError, match="cannot be frozen"):
        _snapshot(pii_posture=posture)


def _bootstrap_raw_token(seed: str = "a") -> dict:
    return compose_token(
        content={"kind": "content_hash", "value": seed * 64},
        config="b" * 64,
        emit_version="c" * 64,
    )


def test_bootstrap_authority_binds_consumed_slice_without_global_probe_token() -> None:
    cursor = {
        "offset": 4,
        "outstanding_backlog": ["next"],
    }
    authority = build_bootstrap_authority(
        raw_token=_bootstrap_raw_token(),
        bindings={"source_name": "sessions", "scope": "global:none"},
        cursor_semantics="sessions.cursor.v1",
        resume_cursor=cursor,
        input_frontier_fingerprint="input-1",
        transition_identity={
            "admission_id": "admission-1",
            "run_id": "run-1",
            "idempotency_key": "idempotency-1",
        },
        generation=1,
        nonce="nonce-1",
    )
    cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY] = authority

    assert bootstrap_authority_from_cursor(cursor) == authority
    assert CURSOR_TOKEN_KEY not in raw_token_cursor(cursor)


def test_bootstrap_authority_refuses_token_tampering_and_cursor_replay() -> None:
    cursor = {
        "offset": 4,
        "outstanding_backlog": ["next"],
    }
    authority = build_bootstrap_authority(
        raw_token=_bootstrap_raw_token(),
        bindings={"source_name": "sessions", "scope": "global:none"},
        cursor_semantics="sessions.cursor.v1",
        resume_cursor=cursor,
        input_frontier_fingerprint="input-3",
        transition_identity={
            "admission_id": "admission-3",
            "run_id": "run-3",
            "idempotency_key": "idempotency-3",
        },
        generation=3,
        nonce="nonce-3",
    )
    cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY] = authority

    tampered = json.loads(json.dumps(cursor))
    tampered[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["raw_token"] = (
        _bootstrap_raw_token("d")
    )
    with pytest.raises(ValueError, match="fingerprint does not match"):
        bootstrap_authority_from_cursor(tampered)

    replayed = json.loads(json.dumps(cursor))
    replayed["offset"] = 9
    with pytest.raises(ValueError, match="resume frontier does not match"):
        bootstrap_authority_from_cursor(replayed)


def test_classifier_with_dynamic_module_access_fails_closed() -> None:
    def classifier(key: str) -> str | None:
        helper = getattr(_HELPER_MODULE, "classify")
        return helper(key)

    posture = PiiPosture(
        store_raw=False,
        classifier=classifier,
        pseudonym_service=None,
        detector=None,
    )

    with pytest.raises(ValueError, match="cannot be frozen"):
        _snapshot(pii_posture=posture)

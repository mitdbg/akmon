"""Refinement tests for strict source-registry document admission."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from types import MappingProxyType

import pytest
from jsonschema import Draft202012Validator

from okg.substrate import fastyaml
from okg.substrate.ingest import adapter_factory
from okg.substrate.ingest.adapter_factory import plan_source_registry_bindings
from okg.substrate.sources.registry import (
    SOURCE_ENTRY_CONTROL_KEYS,
    SOURCE_REGISTRY_DOCUMENT_KEYS,
    admit_source_registry_document,
)
from okg.substrate.sources.registry import _deep_freeze


_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / (
    "source_registry.schema.json"
)


def _source(**overrides: object) -> dict[str, object]:
    source: dict[str, object] = {
        "module": "tests.strict_sources",
        "class": "ReferenceAdapter",
        "ownership_id": "proof.reference",
        "source_class": "reference_catalog",
        "record_identity_kind": "domain_key",
        "record_identity_fields": ["id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "params": {"fixture_path": "/tmp/proof.json"},
    }
    source.update(overrides)
    return source


class _ReferenceAdapter:
    profile = "reference_catalog"

    def __init__(self, *, fixture_path: str) -> None:
        self.fixture_path = fixture_path


def test_strict_marker_rejects_unknown_source_key_with_move_hint() -> None:
    source = _source()
    source["fixture_path"] = source["params"]["fixture_path"]  # type: ignore[index]

    with pytest.raises(ValueError, match=r"fixture_path.*params"):
        admit_source_registry_document(
            {
                "admission_contract": "strict_v1",
                "sources": {"reference": source},
            },
            registry_path=Path("source_registry.yaml"),
        )


def test_present_unknown_marker_never_falls_back_to_legacy() -> None:
    with pytest.raises(ValueError, match="admission_contract"):
        admit_source_registry_document(
            {
                "admission_contract": "strict_v2",
                "sources": {"legacy": {"ownership_id": "legacy"}},
            },
            registry_path=Path("source_registry.yaml"),
        )


def test_unmarked_registry_preserves_legacy_policy_normalization() -> None:
    admitted = admit_source_registry_document(
        {
            "sources": {
                "legacy": {
                    "ownership_id": "proof.legacy",
                    "admission_policy": {
                        "trust_label": "legacy",
                        "output_signature": {"nodes": None, "edges": None},
                        "policy_clauses": [
                            {
                                "id": 7,
                                "record_kind": "node",
                                "subtype": "concept",
                            }
                        ],
                        "write_envelope": {
                            "nodes": None,
                            "edges": None,
                            "claim_keys": None,
                            "value_vocabularies": None,
                            "unbounded": None,
                        },
                    },
                }
            }
        },
        registry_path=Path("source_registry.yaml"),
    )

    policy = admitted.entries["legacy"].admission_policy
    assert policy is not None
    assert policy.output_signature == {}
    assert policy.policy_clauses[0]["id"] == 7
    assert policy.write_envelope == {
        "nodes": [],
        "edges": [],
        "claim_keys": [],
        "value_vocabularies": {},
        "unbounded": [],
        "nondeterministic": False,
    }


def test_strict_document_is_deeply_frozen() -> None:
    raw = _source()
    admitted = admit_source_registry_document(
        {
            "admission_contract": "strict_v1",
            "sources": {"reference": raw},
        },
        registry_path=Path("source_registry.yaml"),
    )

    raw_params = raw["params"]
    assert isinstance(raw_params, dict)
    raw_params["fixture_path"] = "/tmp/mutated.json"

    entry = admitted.entries["reference"]
    assert isinstance(entry.raw, MappingProxyType)
    assert entry.raw["params"]["fixture_path"] == "/tmp/proof.json"
    with pytest.raises(TypeError):
        entry.raw["params"]["fixture_path"] = "/tmp/second.json"


def test_record_authority_format_defaults_legacy_and_requires_explicit_v2() -> None:
    default = admit_source_registry_document(
        {
            "admission_contract": "strict_v1",
            "sources": {"reference": _source()},
        },
        registry_path=Path("source_registry.yaml"),
    ).entries["reference"]
    opted_in = admit_source_registry_document(
        {
            "admission_contract": "strict_v1",
            "sources": {
                "reference": _source(record_authority_format="indexed_v2")
            },
        },
        registry_path=Path("source_registry.yaml"),
    ).entries["reference"]

    assert default.record_authority_format == "legacy_v1"
    assert opted_in.record_authority_format == "indexed_v2"


def test_record_authority_format_rejects_unknown_without_fallback() -> None:
    with pytest.raises(ValueError, match="record_authority_format"):
        admit_source_registry_document(
            {
                "admission_contract": "strict_v1",
                "sources": {
                    "reference": _source(record_authority_format="indexed_v3")
                },
            },
            registry_path=Path("source_registry.yaml"),
        )


@pytest.mark.parametrize("malformed", [[], {}, 1, True, None])
def test_record_authority_format_rejects_non_string_without_type_error(
    malformed: object,
) -> None:
    with pytest.raises(ValueError, match="record_authority_format"):
        admit_source_registry_document(
            {
                "admission_contract": "strict_v1",
                "sources": {
                    "reference": _source(record_authority_format=malformed)
                },
            },
            registry_path=Path("source_registry.yaml"),
        )


def test_strict_document_rejects_yaml_date_with_policy_path() -> None:
    # Import the custom external-action loader first to prove its resolver
    # changes cannot leak into the shared registry YAML loader.
    __import__("okg.substrate.external_actions.config")
    parsed = fastyaml.safe_load("reviewed_on: 2026-08-29\n")
    source = _source(admission_policy={
        "trust_label": "review-trusted",
        "output_scope_summary": {
            "reviewed_on": parsed["reviewed_on"],
        },
    })

    with pytest.raises(
        ValueError,
        match=(
            r"source_registry_strict_json: .*"
            r"path=sources\.reference\.admission_policy\."
            r"output_scope_summary\.reviewed_on: .*date.*quote"
        ),
    ):
        admit_source_registry_document(
            {
                "admission_contract": "strict_v1",
                "sources": {"reference": source},
            },
            registry_path=Path("source_registry.yaml"),
        )


@pytest.mark.parametrize(
    ("yaml_scalar", "rendered_value"),
    [
        (".nan", "nan"),
        (".inf", "inf"),
        ("-.inf", "-inf"),
    ],
)
def test_strict_document_rejects_non_finite_yaml_number_with_policy_path(
    yaml_scalar: str,
    rendered_value: str,
) -> None:
    parsed = fastyaml.safe_load(f"confidence: {yaml_scalar}\n")
    source = _source(admission_policy={
        "trust_label": "review-trusted",
        "output_scope_summary": {
            "confidence": parsed["confidence"],
        },
    })

    with pytest.raises(ValueError) as exc_info:
        admit_source_registry_document(
            {
                "admission_contract": "strict_v1",
                "sources": {"reference": source},
            },
            registry_path=Path("source_registry.yaml"),
        )

    message = str(exc_info.value)
    assert (
        "path=sources.reference.admission_policy."
        "output_scope_summary.confidence"
    ) in message
    assert f"non-finite JSON number {rendered_value}" in message


def test_deep_freeze_preserves_distinct_nested_mapping_key_types() -> None:
    frozen = _deep_freeze({"nested": {1: "integer", "1": "string"}})

    assert len(frozen["nested"]) == 2
    assert frozen["nested"][1] == "integer"
    assert frozen["nested"]["1"] == "string"


def test_all_entries_plan_before_any_constructor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructor_calls: list[str] = []

    class RecordingAdapter(_ReferenceAdapter):
        def __init__(self, *, fixture_path: str) -> None:
            constructor_calls.append(fixture_path)
            super().__init__(fixture_path=fixture_path)

    class MissingRequiredAdapter(RecordingAdapter):
        def __init__(
            self,
            *,
            fixture_path: str,
            required_domain_value: str,
        ) -> None:
            del required_domain_value
            super().__init__(fixture_path=fixture_path)

    classes = {
        "ReferenceAdapter": RecordingAdapter,
        "BrokenAdapter": MissingRequiredAdapter,
    }
    monkeypatch.setattr(
        adapter_factory,
        "import_source_adapter_class",
        lambda entry: classes[entry.raw["class"]],
    )
    admitted = admit_source_registry_document(
        {
            "admission_contract": "strict_v1",
            "sources": {
                "valid": _source(ownership_id="proof.valid"),
                "invalid": _source(
                    **{
                        "class": "BrokenAdapter",
                        "ownership_id": "proof.invalid",
                        "params": {"fixture_path": "/tmp/invalid.json"},
                    }
                ),
            },
        },
        registry_path=Path("source_registry.yaml"),
    )

    with pytest.raises(
        ValueError, match=r"source_param_missing: source 'invalid'",
    ):
        plan_source_registry_bindings(admitted, dsn="postgresql://redacted")

    assert constructor_calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("params", None),
        ("credential_refs", None),
        ("record_identity_fields", ["id", "id"]),
        ("source_policy", None),
        ("sync", None),
    ],
)
def test_strict_presence_decisions_fail_closed(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=field):
        admit_source_registry_document(
            {
                "admission_contract": "strict_v1",
                "sources": {"reference": _source(**{field: value})},
            },
            registry_path=Path("source_registry.yaml"),
        )


def test_strict_schema_and_loader_key_sets_are_identical() -> None:
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))

    assert set(schema["$defs"]["strict_document"]["properties"]) == set(
        SOURCE_REGISTRY_DOCUMENT_KEYS
    )
    assert set(schema["$defs"]["strict_source"]["properties"]) == set(
        SOURCE_ENTRY_CONTROL_KEYS
    )


def _strict_payload() -> dict[str, object]:
    return {
        "admission_contract": "strict_v1",
        "sources": {"reference": _source()},
    }


def test_strict_policy_summary_is_json_shaped_and_detached() -> None:
    payload = _strict_payload()
    source = payload["sources"]["reference"]  # type: ignore[index]
    source["admission_policy"] = {
        "trust_label": "review-trusted",
        "authority_scope": {"path_globs": ["docs/**/*"]},
        "allowed_record_kinds": ["node"],
        "output_signature": {
            "nodes": [
                {
                    "subtype": "document",
                    "attrs": {"source": "proof"},
                }
            ]
        },
        "policy_clauses": [
            {
                "id": "proof.document",
                "record_kind": "node",
                "subtype": "document",
            }
        ],
        "output_scope_summary": {"families": ["document"]},
    }
    admitted = admit_source_registry_document(
        payload,
        registry_path=Path("source_registry.yaml"),
    )
    policy = admitted.entries["reference"].admission_policy
    assert policy is not None

    summary = policy.as_dict()
    raw = policy.raw_dict()
    json.dumps({"summary": summary, "raw": raw})
    summary["output_signature"]["nodes"][0]["attrs"]["source"] = "changed"
    raw["authority_scope"]["path_globs"].append("private/**/*")

    assert policy.output_signature["nodes"][0]["attrs"]["source"] == "proof"
    assert policy.raw["authority_scope"]["path_globs"] == ("docs/**/*",)


def test_strict_signature_accepts_exact_attribute_key_contract() -> None:
    payload = _strict_payload()
    source = payload["sources"]["reference"]  # type: ignore[index]
    source["admission_policy"] = {
        "trust_label": "review-trusted",
        "allowed_record_kinds": ["edge"],
        "output_signature": {
            "edges": [
                {
                    "src_subtype": "document",
                    "edge_type": "references",
                    "dst_subtype": "source_file",
                    "attrs": {"role": "authority_reference"},
                    "attrs_exact": True,
                    "attr_keys": ["role", "target_id"],
                }
            ]
        },
    }
    admitted = admit_source_registry_document(
        payload,
        registry_path=Path("source_registry.yaml"),
    )
    policy = admitted.entries["reference"].admission_policy
    assert policy is not None
    rule = policy.output_signature["edges"][0]
    assert rule["attrs_exact"] is True
    assert rule["attr_keys"] == ("role", "target_id")


def test_strict_signature_rejects_attr_keys_without_exact_mode() -> None:
    payload = _strict_payload()
    source = payload["sources"]["reference"]  # type: ignore[index]
    source["admission_policy"] = {
        "trust_label": "review-trusted",
        "output_signature": {
            "nodes": [
                {
                    "subtype": "document",
                    "attr_keys": ["role"],
                }
            ]
        },
    }
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema).iter_errors(payload))
    with pytest.raises(ValueError, match="attrs_exact"):
        admit_source_registry_document(
            payload,
            registry_path=Path("source_registry.yaml"),
        )


def test_strict_signature_rejects_attr_keys_with_false_exact_mode() -> None:
    payload = _strict_payload()
    source = payload["sources"]["reference"]  # type: ignore[index]
    source["admission_policy"] = {
        "trust_label": "review-trusted",
        "output_signature": {
            "edges": [
                {
                    "src_subtype": "document",
                    "edge_type": "references",
                    "dst_subtype": "source_file",
                    "attrs_exact": False,
                    "attr_keys": ["role"],
                }
            ]
        },
    }
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema).iter_errors(payload))
    with pytest.raises(ValueError, match="attrs_exact"):
        admit_source_registry_document(
            payload,
            registry_path=Path("source_registry.yaml"),
        )


def _strict_parity_cases() -> list[tuple[str, object, bool]]:
    valid_policy = {
        "sensitivity": "internal",
        "data_classification": "internal",
        "credential_ref_policy": "none",
        "live_call_allowed": False,
        "exportability": "internal",
        "retention": "normal",
        "provenance": "declared",
        "privacy_obligations": ["audit"],
    }
    valid_sync = {
        "watch": {
            "roots": ["src"],
            "include_globs": ["**/*.py"],
            "exclude_globs": [],
        },
        "triggers": ["fs_event", "manual"],
        "default_event_mode": "scope_incomplete",
        "scope": {"accepts": [], "unsupported_fallback": None},
        "compaction": None,
        "webhook": None,
        "poll": None,
    }
    valid_admission = {
        "trust_label": "review-trusted",
        "authority_scope": {},
        "allowed_record_kinds": [],
        "output_signature": {"nodes": [], "edges": []},
        "policy_clauses": [],
        "output_scope_summary": {},
        "write_envelope": {"nodes": [], "edges": []},
    }

    def changed(**fields: object) -> dict[str, object]:
        payload = _strict_payload()
        source = copy.deepcopy(payload["sources"]["reference"])  # type: ignore[index]
        source.update(fields)
        payload["sources"] = {"reference": source}
        return payload

    return [
        ("minimal", _strict_payload(), True),
        ("source-policy", changed(source_policy=valid_policy), True),
        ("sync", changed(sync=valid_sync), True),
        ("admission", changed(admission_policy=valid_admission), True),
        (
            "empty-code-repos",
            {**_strict_payload(), "code_repos": {"repos": [], "expand": []}},
            True,
        ),
        ("unknown-root", {**_strict_payload(), "mystery": True}, False),
        ("null-params", changed(params=None), False),
        ("unknown-source", changed(schedule={}), False),
        ("top-level-fixture", changed(fixture_path="fixture.json"), False),
        ("duplicate-identities", changed(record_identity_fields=["id", "id"]), False),
        ("null-source-policy", changed(source_policy=None), False),
        ("empty-source-policy", changed(source_policy={}), False),
        (
            "missing-policy-field",
            changed(source_policy={key: value for key, value in valid_policy.items() if key != "retention"}),
            False,
        ),
        ("null-sync", changed(sync=None), False),
        (
            "unknown-sync",
            changed(sync={"mystery": True}),
            False,
        ),
        (
            "bool-poll-interval",
            changed(sync={"poll": {"interval_seconds": True}}),
            False,
        ),
        (
            "null-signature-list",
            changed(admission_policy={**valid_admission, "output_signature": {"nodes": None}}),
            False,
        ),
    ]


@pytest.mark.parametrize(
    ("case_name", "payload", "expected"),
    _strict_parity_cases(),
    ids=[case[0] for case in _strict_parity_cases()],
)
def test_strict_schema_and_semantic_loader_parity(
    case_name: str,
    payload: object,
    expected: bool,
) -> None:
    del case_name
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    schema_accepts = not list(Draft202012Validator(schema).iter_errors(payload))
    try:
        admit_source_registry_document(
            payload,
            registry_path=Path("source_registry.yaml"),
        )
    except (TypeError, ValueError):
        loader_accepts = False
    else:
        loader_accepts = True

    assert schema_accepts is expected
    assert loader_accepts is expected


def _strict_normative_matrix() -> list[tuple[str, object, bool, bool]]:
    """Enumerate every strict required/optional/null/empty decision."""
    cases: list[tuple[str, object, bool, bool]] = []

    def changed(**fields: object) -> dict[str, object]:
        source = copy.deepcopy(_source())
        source.update(fields)
        return {
            "admission_contract": "strict_v1",
            "sources": {"reference": source},
        }

    def add(
        label: str,
        payload: object,
        expected: bool,
        loader_expected: bool | None = None,
    ) -> None:
        cases.append((
            label,
            payload,
            expected,
            expected if loader_expected is None else loader_expected,
        ))

    add("root-null-sources", {"admission_contract": "strict_v1", "sources": None}, False)
    add("root-blank-source-name", {"admission_contract": "strict_v1", "sources": {" ": _source()}}, False)
    add("root-null-source-body", {"admission_contract": "strict_v1", "sources": {"reference": None}}, False)

    profile_sources = {
        "reference": _source(),
        "mutable": {
            **_source(),
            "source_class": "mutable_api",
            "record_identity_kind": "remote_id",
            "source_revision_kind": "updated_at",
            "deletion_semantics": "explicit_delete",
        },
        "discovery": {
            **_source(),
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
        },
        "append": {
            **_source(),
            "source_class": "append_feed",
            "record_identity_kind": "event_id",
            "source_revision_kind": "sequence_number",
            "deletion_semantics": "append_only",
        },
        "overlay": {
            "module": "tests.strict_sources",
            "class": "OverlayAdapter",
            "ownership_id": "proof.overlay",
            "source_class": "live_overlay",
            "deletion_semantics": "external_overlay",
            "publication_mode": "live_overlay",
        },
        "registry-only": {
            "ownership_id": "proof.registry",
            "source_class": "registry_only",
        },
    }
    for label, source in profile_sources.items():
        add(
            f"profile-{label}",
            {
                "admission_contract": "strict_v1",
                "sources": {label: source},
            },
            True,
        )
    add("source-missing-ownership", changed(ownership_id=None), False)
    add("source-missing-classification", changed(source_class=None), False)
    add("source-runnable-missing-module", changed(module=None), False)
    add("source-runnable-blank-class", changed(**{"class": " "}), False)
    add("source-wrong-publication", changed(publication_mode="live_overlay"), False)
    add("source-identities-empty", changed(record_identity_fields=[]), False)
    add("source-identities-blank", changed(record_identity_fields=[" "]), False)

    source_field_cases = [
        ("params-empty", {"params": {}}, True),
        ("params-null", {"params": None}, False),
        ("credential-refs-empty", {"credential_refs": []}, True),
        ("credential-refs-null", {"credential_refs": None}, False),
        ("credential-refs-duplicate", {"credential_refs": ["a", "a"]}, False),
        ("credential-aliases-empty", {"credential_aliases": {}}, True),
        ("credential-aliases-null", {"credential_aliases": None}, False),
        ("credential-aliases-blank-key", {"credential_aliases": {" ": ["a"]}}, False),
        ("credential-aliases-duplicate", {"credential_aliases": {"a": ["b", "b"]}}, False),
        ("required-false", {"required_for_baseline": False}, True),
        ("required-null", {"required_for_baseline": None}, False),
        ("enabled-false", {"enabled_by_default": False}, True),
        ("enabled-null", {"enabled_by_default": None}, False),
        ("predecessor-string", {"predecessor_source": "upstream"}, True),
        ("predecessor-list", {"predecessor_source": ["upstream"]}, True),
        ("predecessor-empty", {"predecessor_source": []}, False),
        ("predecessor-duplicate", {"predecessor_source": ["upstream", "upstream"]}, False),
        ("description", {"description": "proof"}, True),
        ("description-blank", {"description": " "}, False),
    ]
    for label, fields, expected in source_field_cases:
        add(label, changed(**fields), expected)
    for field in (
        "configuration_hints",
        "lab_boundary",
        "mit_boundary",
        "microbench",
    ):
        add(f"{field}-empty", changed(**{field: {}}), True)
        add(f"{field}-null", changed(**{field: None}), False)

    admission_minimal = {"trust_label": "review-trusted"}
    admission_cases = [
        ("minimal", admission_minimal, True),
        ("null", None, False),
        ("empty", {}, False),
        ("unknown", {**admission_minimal, "unknown": True}, False),
        ("trust-blank", {"trust_label": " "}, False),
        ("producer-id", {**admission_minimal, "producer_id": "proof"}, True),
        ("producer-id-null", {**admission_minimal, "producer_id": None}, False),
        ("producer-kind", {**admission_minimal, "producer_kind": "source"}, True),
        ("producer-kind-invalid", {**admission_minimal, "producer_kind": "other"}, False),
        ("mode", {**admission_minimal, "admission_mode": "review"}, True),
        ("mode-null", {**admission_minimal, "admission_mode": None}, False),
        ("authority-empty", {**admission_minimal, "authority_scope": {}}, True),
        ("authority-null", {**admission_minimal, "authority_scope": None}, False),
        ("kinds-empty", {**admission_minimal, "allowed_record_kinds": []}, True),
        ("kinds-null", {**admission_minimal, "allowed_record_kinds": None}, False),
        ("kinds-duplicate", {**admission_minimal, "allowed_record_kinds": ["node", "node"]}, False),
        ("signature-empty", {**admission_minimal, "output_signature": {}}, True),
        ("signature-null", {**admission_minimal, "output_signature": None}, False),
        ("clauses-empty", {**admission_minimal, "policy_clauses": []}, True),
        ("clauses-null", {**admission_minimal, "policy_clauses": None}, False),
        ("summary-empty", {**admission_minimal, "output_scope_summary": {}}, True),
        ("summary-string", {**admission_minimal, "output_scope_summary": "proof"}, True),
        ("summary-null", {**admission_minimal, "output_scope_summary": None}, False),
        ("envelope-empty", {**admission_minimal, "write_envelope": {}}, True),
        ("envelope-null", {**admission_minimal, "write_envelope": None}, False),
        ("envelope-nodes-null", {**admission_minimal, "write_envelope": {"nodes": None}}, False),
        ("node-clause", {**admission_minimal, "policy_clauses": [{"id": "n", "record_kind": "node", "subtype": "document"}]}, True),
        ("node-clause-no-subtype", {**admission_minimal, "policy_clauses": [{"id": "n", "record_kind": "node"}]}, False),
        ("edge-clause", {**admission_minimal, "policy_clauses": [{"id": "e", "record_kind": "edge", "triple": ["a", "references", "b"]}]}, True),
        ("edge-clause-short", {**admission_minimal, "policy_clauses": [{"id": "e", "record_kind": "edge", "triple": ["a", "b"]}]}, False),
    ]
    for label, policy, expected in admission_cases:
        add(
            f"admission-{label}",
            changed(admission_policy=policy),
            expected,
        )

    valid_policy = {
        "sensitivity": "internal",
        "data_classification": "internal",
        "credential_ref_policy": "none",
        "live_call_allowed": False,
        "exportability": "internal",
        "retention": "normal",
        "provenance": "declared",
        "privacy_obligations": ["audit"],
    }
    for field in valid_policy:
        missing = {
            key: value for key, value in valid_policy.items() if key != field
        }
        add(
            f"source-policy-missing-{field}",
            changed(source_policy=missing),
            False,
        )
    source_policy_cases = [
        ("valid", valid_policy, True),
        ("unknown", {**valid_policy, "unknown": True}, False),
        ("store-raw", {**valid_policy, "store_raw": False}, True),
        ("store-raw-null", {**valid_policy, "store_raw": None}, False),
        ("pii", {**valid_policy, "pii_classes": {"email": "direct_identifier"}}, True),
        ("pii-empty", {**valid_policy, "pii_classes": {}}, False),
        ("pii-null", {**valid_policy, "pii_classes": None}, False),
        ("obligations-empty", {**valid_policy, "privacy_obligations": []}, False),
        ("obligations-duplicate", {**valid_policy, "privacy_obligations": ["audit", "audit"]}, False),
        ("live-call-null", {**valid_policy, "live_call_allowed": None}, False),
        ("enum-invalid", {**valid_policy, "retention": "forever"}, False),
    ]
    for label, policy, expected in source_policy_cases:
        add(
            f"source-policy-{label}",
            changed(source_policy=policy),
            expected,
        )

    sync_cases = [
        ("empty", {}, True),
        ("triggers-null", {"triggers": None}, True),
        ("triggers-empty", {"triggers": []}, True),
        ("triggers-duplicate", {"triggers": ["manual", "manual"]}, False),
        ("default-null", {"default_event_mode": None}, False),
        ("delete-flag-null", {"deletes_require_reconcile": None}, False),
        ("watch-null", {"watch": None}, True),
        ("watch-string-root", {"watch": {"roots": "src"}}, True),
        ("watch-empty-root", {"watch": {"roots": []}}, False),
        ("watch-empty-include", {"watch": {"roots": ["src"], "include_globs": []}}, False),
        ("watch-empty-exclude", {"watch": {"roots": ["src"], "exclude_globs": []}}, True),
        ("scope-null", {"scope": None}, True),
        ("scope-accepts-null", {"scope": {"accepts": None}}, True),
        ("scope-accepts-empty", {"scope": {"accepts": []}}, True),
        ("dependents-null", {"dependents": None}, True),
        ("compaction-null", {"compaction": None}, True),
        ("compaction-zero", {"compaction": {"window_seconds": 0}}, True),
        ("compaction-negative", {"compaction": {"window_seconds": -1}}, False),
        ("compaction-bool", {"compaction": {"max_events": True}}, False),
        ("webhook-null", {"webhook": None}, True),
        ("webhook-path-null", {"webhook": {"path": None}}, True),
        ("webhook-secret-null", {"webhook": {"secret_ref": None}}, True),
        ("webhook-header-blank", {"webhook": {"signature_header": " "}}, False),
        ("poll-null", {"poll": None}, True),
        ("poll-probe-null", {"poll": {"probe": None}}, True),
        ("poll-interval-zero", {"poll": {"interval_seconds": 0}}, False),
        ("poll-interval-bool", {"poll": {"interval_seconds": True}}, False),
        ("reconcile-null", {"reconcile_mode": None}, True),
        ("scoped-delete-null", {"scoped_delete_mode": None}, True),
        ("fanout-null", {"scoped_delete_dependent_fanout": None}, True),
        ("cheap-probe-null", {"cheap_probe": None}, True),
    ]
    for label, sync, expected in sync_cases:
        add(f"sync-{label}", changed(sync=sync), expected)

    valid_code_repos = {
        "base": "/tmp/okg-code-repos",
        "repos": [{"slug": "okg", "dir": "okg"}],
        "expand": [],
    }
    code_repo_cases = [
        ("null", None, False),
        ("empty", {}, True),
        ("valid", valid_code_repos, True),
        ("unknown", {**valid_code_repos, "unknown": True}, False),
        ("repos-null", {"repos": None}, False),
        ("expand-null", {"expand": None}, False),
        ("missing-base", {"repos": [{"slug": "okg"}], "expand": []}, False),
        ("relative-base", {**valid_code_repos, "base": "repos"}, False),
        ("row-scalar", {"base": "/tmp", "repos": ["okg"], "expand": []}, False),
        ("missing-slug", {"base": "/tmp", "repos": [{"dir": "okg"}], "expand": []}, False),
        ("blank-slug", {"base": "/tmp", "repos": [{"slug": " "}], "expand": []}, False),
        ("dir-escape", {"base": "/tmp", "repos": [{"slug": "okg", "dir": "../okg"}], "expand": []}, False),
        ("url-blank", {"base": "/tmp", "repos": [{"slug": "okg", "url": " "}], "expand": []}, False),
        ("branch-blank", {"base": "/tmp", "repos": [{"slug": "okg", "tracked_branch": " "}], "expand": []}, False),
        ("manage-null", {"base": "/tmp", "repos": [{"slug": "okg", "manage_checkout": None}], "expand": []}, False),
        ("expand-duplicate", {**valid_code_repos, "expand": ["git-files", "git-files"]}, False),
        ("expand-unknown", {**valid_code_repos, "expand": ["unknown"]}, False),
        (
            "params-valid",
            {
                **valid_code_repos,
                "expand": ["git-files", "doc-corpus"],
                "params": {"doc-corpus": {"include_globs": ["**/*"], "text_fallback": True}},
            },
            True,
        ),
        ("params-null", {**valid_code_repos, "params": None}, False),
        ("params-false", {**valid_code_repos, "params": False}, False),
        (
            "params-block-scalar",
            {**valid_code_repos, "expand": ["doc-corpus"], "params": {"doc-corpus": ""}},
            False,
        ),
        ("params-block-unknown", {**valid_code_repos, "params": {"unknown": {}}}, False),
        (
            "params-per-repo-key",
            {**valid_code_repos, "expand": ["doc-corpus"], "params": {"doc-corpus": {"url": "x"}}},
            False,
        ),
    ]
    for label, code_repos, expected in code_repo_cases:
        add(
            f"code-repos-{label}",
            {**_strict_payload(), "code_repos": code_repos},
            expected,
        )
    # Structurally valid, but the loader requires a params block to be expanded.
    add(
        "code-repos-params-block-not-expanded",
        {
            **_strict_payload(),
            "code_repos": {**valid_code_repos, "params": {"doc-corpus": {"max_files": 10}}},
        },
        True,
        loader_expected=False,
    )
    return cases


@pytest.mark.parametrize(
    ("case_name", "payload", "schema_expected", "loader_expected"),
    _strict_normative_matrix(),
    ids=[case[0] for case in _strict_normative_matrix()],
)
def test_strict_normative_schema_and_loader_matrix(
    case_name: str,
    payload: object,
    schema_expected: bool,
    loader_expected: bool,
) -> None:
    del case_name
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    schema_accepts = not list(
        Draft202012Validator(schema).iter_errors(payload)
    )
    try:
        admit_source_registry_document(
            payload,
            registry_path=Path("source_registry.yaml"),
        )
    except (TypeError, ValueError):
        loader_accepts = False
    else:
        loader_accepts = True

    assert schema_accepts is schema_expected
    assert loader_accepts is loader_expected


def test_strict_semantics_reject_duplicate_policy_ids_after_schema() -> None:
    payload = _strict_payload()
    source = payload["sources"]["reference"]  # type: ignore[index]
    source["admission_policy"] = {  # type: ignore[index]
        "trust_label": "review-trusted",
        "policy_clauses": [
            {"id": "same", "record_kind": "node", "subtype": "concept"},
            {"id": "same", "record_kind": "node", "subtype": "document"},
        ],
    }
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert not list(Draft202012Validator(schema).iter_errors(payload))
    with pytest.raises(ValueError, match="duplicate.*clause"):
        admit_source_registry_document(
            payload,
            registry_path=Path("source_registry.yaml"),
        )


@pytest.mark.parametrize(
    ("case_name", "payload", "message"),
    [
        (
            "ownership-id",
            {
                "admission_contract": "strict_v1",
                "sources": {
                    "first": _source(),
                    "second": {
                        **_source(),
                        "ownership_id": "proof.reference",
                    },
                },
            },
            "duplicate ownership_id",
        ),
        (
            "producer-id",
            {
                "admission_contract": "strict_v1",
                "sources": {
                    "first": {
                        **_source(),
                        "admission_policy": {
                            "trust_label": "review-trusted",
                            "producer_id": "proof.shared-producer",
                        },
                    },
                    "second": {
                        **_source(),
                        "ownership_id": "proof.second",
                        "admission_policy": {
                            "trust_label": "review-trusted",
                            "producer_id": "proof.shared-producer",
                        },
                    },
                },
            },
            "duplicate producer_id",
        ),
        (
            "code-repo-slug",
            {
                **_strict_payload(),
                "code_repos": {
                    "base": "/tmp/okg-code-repos",
                    "repos": [
                        {"slug": "same", "dir": "first"},
                        {"slug": "same", "dir": "second"},
                    ],
                    "expand": [],
                },
            },
            "duplicate code_repos slug",
        ),
    ],
)
def test_strict_semantic_uniqueness_after_structural_schema(
    case_name: str,
    payload: object,
    message: str,
) -> None:
    del case_name
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert not list(Draft202012Validator(schema).iter_errors(payload))

    with pytest.raises(ValueError, match=message):
        admit_source_registry_document(
            payload,
            registry_path=Path("source_registry.yaml"),
        )

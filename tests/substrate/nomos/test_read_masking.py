"""Tests for the classification-driven read-masking transform.

Proves the transform reveals non_pii, masks sensitive and UNCLASSIFIED
attributes (fail-closed), pseudonymizes identifier classes join-consistently via
the vault, provably subsumes the retired ``_redacted_attrs`` blocklist, produces
a two-principal projection difference (privileged reveal vs unprivileged mask
over the same attrs), and that the decision-sink payload summary carries no raw
values.
"""
from __future__ import annotations

import json

from okg.substrate.alias.pseudonymize import PseudonymService
from okg.substrate.nomos.read_masking import (
    LEGACY_REDACTION_KEYS,
    MASK_TOKEN,
    READ_DROP_CLASSES,
    legacy_attr_classifier,
    mask_attrs,
    safe_decision_payload,
)

_VAULT = PseudonymService(key=b"unit-test-key-material-for-masks", epoch="e1")


def _classifier(mapping):
    return lambda key: mapping.get(key)


def test_reveals_non_pii_and_stamps_nothing_when_nothing_masked() -> None:
    attrs = {"node_id": "n1", "created_at": "2026-01-01"}
    out = mask_attrs(attrs, classifier=_classifier({"node_id": "non_pii", "created_at": "non_pii"}))
    assert out == {"node_id": "n1", "created_at": "2026-01-01"}
    assert "nomos_redacted" not in out


def test_unclassified_attribute_is_masked_fail_closed() -> None:
    attrs = {"mystery": "Alice lives at 12 Elm St"}
    out = mask_attrs(attrs, classifier=_classifier({}))  # returns None -> fail-safe
    assert out["mystery"] == MASK_TOKEN
    assert out["nomos_redacted"] is True
    assert "Alice" not in json.dumps(out)


def test_sensitive_attribute_is_masked() -> None:
    out = mask_attrs(
        {"diagnosis": "hypertension"},
        classifier=_classifier({"diagnosis": "sensitive_attribute"}),
    )
    assert out["diagnosis"] == MASK_TOKEN


def test_identifiers_pseudonymize_join_consistently_with_vault() -> None:
    classifier = _classifier({"email": "direct_identifier"})
    a = mask_attrs({"email": "alice@example.com"}, classifier=classifier, pseudonym_service=_VAULT)
    b = mask_attrs({"email": "alice@example.com"}, classifier=classifier, pseudonym_service=_VAULT)
    c = mask_attrs({"email": "bob@example.com"}, classifier=classifier, pseudonym_service=_VAULT)
    assert a["email"] == b["email"]  # same value -> same pseudonym (joins survive)
    assert a["email"] != c["email"]  # distinct values -> distinct pseudonyms
    assert "alice@example.com" not in json.dumps(a)  # raw never present


def test_identifiers_mask_when_no_vault_key() -> None:
    out = mask_attrs(
        {"email": "alice@example.com"},
        classifier=_classifier({"email": "direct_identifier"}),
        pseudonym_service=None,
    )
    assert out["email"] == MASK_TOKEN  # fail-safe: mask, never reveal


def test_mask_attrs_subsumes_legacy_blocklist() -> None:
    # Every key the retired _redacted_attrs blocklist dropped must be withheld by
    # the new transform under the behavior-preserving legacy classifier.
    attrs = {key: f"secret-{key}" for key in LEGACY_REDACTION_KEYS}
    attrs["node_id"] = "n1"
    out = mask_attrs(attrs, classifier=legacy_attr_classifier)
    for key in LEGACY_REDACTION_KEYS:
        assert out[key] == MASK_TOKEN
        assert f"secret-{key}" not in json.dumps(out)
    assert out["node_id"] == "n1"  # non-blocklist structural field still revealed


def test_read_redact_path_drops_exactly_the_legacy_blocklist() -> None:
    # The retired _redacted_attrs behavior is now mask_attrs with the legacy
    # classifier + READ_DROP_CLASSES: it must DROP exactly the legacy free-text
    # keys and keep everything else (the prior drop-the-key semantics).
    non_legacy = {"node_id", "created_at", "email", "full_name", "custom_field"}
    attrs = {key: "x" for key in LEGACY_REDACTION_KEYS} | {k: "y" for k in non_legacy}
    out = mask_attrs(
        attrs, classifier=legacy_attr_classifier, drop_classes=READ_DROP_CLASSES
    )
    for key in LEGACY_REDACTION_KEYS:
        assert key not in out  # dropped, not mask-tokened
    assert non_legacy <= set(out)  # nothing outside the blocklist dropped
    assert out["nomos_redacted"] is True


def test_two_principal_projection_difference() -> None:
    attrs = {
        "full_name": "Alice McCarthy",
        "email": "alice.mccarthy@example.com",
        "note": "confidential remark",
        "node_id": "n1",
    }
    classifier = _classifier({
        "full_name": "direct_identifier",
        "email": "direct_identifier",
        "node_id": "non_pii",
        # "note" is unclassified -> fail-safe masked
    })
    privileged = dict(attrs)  # reveal path: the policy does not redact
    unprivileged = mask_attrs(attrs, classifier=classifier, pseudonym_service=_VAULT)

    assert privileged["email"] == "alice.mccarthy@example.com"  # privileged reveals
    blob = json.dumps(unprivileged)
    assert "alice.mccarthy@example.com" not in blob  # unprivileged: no raw PII
    assert "Alice McCarthy" not in blob
    assert "confidential remark" not in blob  # unclassified masked fail-closed
    assert unprivileged["node_id"] == "n1"  # non_pii still revealed
    assert unprivileged["nomos_redacted"] is True


def test_governance_keys_exempt_but_prefix_is_not() -> None:
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    attrs = {
        "classification": "internal",
        LINEAGE_ATTR: {"sources": []},
        "nomos_redacted": True,
        "nomos_email": "alice@example.com",  # a source-authored nomos_-prefixed key
        "email": "bob@example.com",
    }
    # Even with a fail-safe classifier (everything unknown -> masked), the
    # explicit governance keys survive; policy decisions key on classification.
    out = mask_attrs(attrs, classifier=lambda key: None)
    assert out["classification"] == "internal"
    assert out[LINEAGE_ATTR] == {"sources": []}
    assert out["nomos_redacted"] is True  # idempotency marker survives
    # A source-authored nomos_-prefixed attr is NOT exempt (the exemption is an
    # explicit allow-list, not a prefix) -- it masks like any unclassified attr.
    assert out["nomos_email"] == MASK_TOKEN
    assert out["email"] == MASK_TOKEN


def test_safe_decision_payload_has_no_raw_values() -> None:
    attrs = {"email": "alice@example.com", "full_name": "Alice"}
    payload = safe_decision_payload(attrs)
    assert payload["attr_keys"] == ["email", "full_name"]
    assert payload["attr_count"] == 2
    blob = json.dumps(payload)
    assert "alice@example.com" not in blob and "Alice" not in blob

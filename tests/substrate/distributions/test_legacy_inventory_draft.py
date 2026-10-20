"""The drafted Archi legacy-input inventory is internally consistent.

Database-free.  These tests read the two committed artifacts and check the four
things a consumer needs before confirming counts: the digest reproduces through
the production helper, nothing is unresolved, the declared entry count matches
the file, and every evidence reference is the pinned opaque token shape.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from okg.substrate.distributions.lifecycle_receipts import (
    LEGACY_INPUT_INVENTORY_SCHEMA,
    LEGACY_INPUT_KINDS,
    LifecycleReceiptError,
    canonical_legacy_inventory_entries,
    legacy_authority_evidence,
    legacy_input_inventory_declaration,
    legacy_input_inventory_digest,
)

_EVIDENCE = (
    Path(__file__).resolve().parents[3]
    / "pact"
    / "changes"
    / "s14-1794-legacy-inventory-draft"
    / "evidence"
)
_INVENTORY_PATH = _EVIDENCE / "archi-legacy-inventory-draft.json"
_DECLARATION_PATH = _EVIDENCE / "archi-legacy-inventory-declaration.json"

# archi@<40-hex>:<path>#L<line>, whitespace-free, as the draft mints it.
_EVIDENCE_REF_RE = re.compile(r"^archi@[0-9a-f]{40}:[^\s:#]+#L[1-9][0-9]*$")


@pytest.fixture(scope="module")
def inventory() -> dict:
    return json.loads(_INVENTORY_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def declaration() -> dict:
    return json.loads(_DECLARATION_PATH.read_text(encoding="utf-8"))


def test_draft_digest_reproduces_the_declaration(inventory, declaration) -> None:
    """The readable file alone reproduces the digest the declaration carries."""

    entries = inventory["entries"]
    assert inventory["schema_version"] == LEGACY_INPUT_INVENTORY_SCHEMA
    assert legacy_input_inventory_digest(entries) == declaration["inventory_digest"]
    rebuilt = legacy_input_inventory_declaration(inventory["inventory_id"], entries)
    assert rebuilt == declaration


def test_draft_has_no_unresolved_entries(inventory) -> None:
    """Every entry carries a resolved kind and disposition, and none repeats.

    The audit's own ``unresolved_inputs`` is hardcoded to zero, so asserting it
    would prove nothing about this file. The property is read out of the draft
    instead: every entry resolves to one of the two dispositions, and the audit's
    two counts add up to the entries the file actually carries.
    """

    raw = inventory["entries"]
    resolved = {"migrated", "refused"}
    unresolved = [
        entry["input_id"] for entry in raw if entry.get("disposition") not in resolved
    ]
    assert unresolved == []

    entries = canonical_legacy_inventory_entries(raw)
    audit = legacy_authority_evidence(entries)
    assert audit["declared_inputs"] == len(raw)
    assert audit["migrated_inputs"] == sum(
        1 for entry in raw if entry["disposition"] == "migrated"
    )
    assert audit["refused_inputs"] == sum(
        1 for entry in raw if entry["disposition"] == "refused"
    )
    assert audit["migrated_inputs"] + audit["refused_inputs"] == len(raw)
    assert {entry["kind"] for entry in raw} <= LEGACY_INPUT_KINDS
    assert len({entry["input_id"] for entry in raw}) == len(raw)


def test_declared_entry_count_matches_the_file(inventory, declaration) -> None:
    assert declaration["entry_count"] == len(inventory["entries"])
    assert declaration["empty"] is False
    assert declaration["inventory_id"] == inventory["inventory_id"]


def test_every_evidence_ref_is_the_pinned_token_shape(inventory) -> None:
    """Each reference names the pinned Archi commit, one path and one line."""

    revision = inventory["consumer_source_revision"]
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    for entry in inventory["entries"]:
        ref = entry["evidence_ref"]
        assert _EVIDENCE_REF_RE.fullmatch(ref), ref
        assert ref.startswith(f"archi@{revision}:")


def test_an_unresolved_disposition_refuses() -> None:
    """The failure case: a disposition the audit cannot account for refuses.

    Synthetic entries, not the partner draft above, so the public copy (which
    has no change records) runs it too.
    """

    entries = [
        {
            "input_id": "example.checkout-path",
            "kind": "authoring_checkout_path",
            "disposition": "migrated",
            "evidence_ref": "example@0000000000000000000000000000000000000000:install.sh#L1",
        },
        {
            "input_id": "example.second-input",
            "kind": "authoring_checkout_path",
            "disposition": "refused",
            "evidence_ref": "example@0000000000000000000000000000000000000000:install.sh#L2",
        },
    ]
    # The control: as written the entries are valid, so the refusal below is
    # the disposition's and not a malformed entry's.
    assert len(canonical_legacy_inventory_entries(entries)) == 2
    entries[0]["disposition"] = "unresolved"
    with pytest.raises(LifecycleReceiptError) as caught:
        canonical_legacy_inventory_entries(entries)
    assert caught.value.data["reason"] == "inventory_entry_invalid"

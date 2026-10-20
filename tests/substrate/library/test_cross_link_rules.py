"""Unit tests for the cross-link rule loader (no DB required).

Exercises CrossLinkRule parsing, per-matcher field validation,
and the strict matcher-name whitelist.

Per `declarative-cross-source-linker` (2026-04-26).
"""
from __future__ import annotations

import pytest

from okg.substrate.library.linkers.rules import (
    MATCHER_NAMES,
    RuleParseError,
    load_rules,
)


def _doc(*rules):
    return {"rules": list(rules)}


def test_loads_subject_prefix_rule():
    rules = load_rules(
        _doc({
            "id": "r1",
            "src": {"subtype": "git_commit", "field": "message_subject"},
            "dst": {"subtype": "open_spec_change", "field": "change_id"},
            "matcher": "subject_prefix",
        }),
        source="t",
    )
    assert len(rules) == 1
    r = rules[0]
    assert r.id == "r1"
    assert r.src.subtypes == ("git_commit",)
    assert r.dst.subtypes == ("open_spec_change",)
    assert r.matcher == "subject_prefix"
    assert r.bidirectional is True
    assert r.edge == "references"


def test_loads_alias_table_rule():
    rules = load_rules(
        _doc({
            "id": "r2",
            "src": {"subtype": "entity_mention", "field": "entity_value"},
            "dst": {"via": "alias_table"},
            "matcher": "alias_table",
        }),
        source="t",
    )
    assert rules[0].dst.via == "alias_table"
    assert rules[0].dst.subtypes == ()


def test_loads_derive_from_path_with_via_edge():
    rules = load_rules(
        _doc({
            "id": "r3",
            "src": {
                "subtype": ["agent_session", "sub_agent_session"],
                "via_edge": {
                    "edge_type": "affects",
                    "dst_subtype": "source_file",
                },
            },
            "dst": {
                "subtype": "open_spec_change",
                "field": "change_id",
                "derive_from_path": {
                    "attr": "path",
                    "regex": r"^foo/(\w+)/",
                    "capture_field": "change_id",
                },
            },
            "matcher": "derive_from_path",
        }),
        source="t",
    )
    r = rules[0]
    assert r.src.subtypes == ("agent_session", "sub_agent_session")
    assert r.src.via_edge is not None
    assert r.src.via_edge.edge_type == "affects"
    assert r.dst.derive_from_path is not None
    assert r.dst.derive_from_path.regex == r"^foo/(\w+)/"


def test_loads_word_boundary_with_fields_list():
    rules = load_rules(
        _doc({
            "id": "r4",
            "src": {
                "subtype": ["issue", "pull_request"],
                "fields": ["title", "body"],
            },
            "dst": {"subtype": "open_spec_change", "field": "change_id"},
            "matcher": "word_boundary",
        }),
        source="t",
    )
    r = rules[0]
    assert r.src.fields == ("title", "body")
    assert r.src.field is None


def test_rejects_unknown_matcher():
    with pytest.raises(RuleParseError) as exc:
        load_rules(
            _doc({
                "id": "bad",
                "src": {"subtype": "x", "field": "f"},
                "dst": {"subtype": "y", "field": "g"},
                "matcher": "something_made_up",
            }),
            source="t",
        )
    assert "something_made_up" in str(exc.value)
    for m in MATCHER_NAMES:
        # The error message lists valid matchers.
        assert m in str(exc.value)


def test_rejects_missing_field_for_subject_prefix():
    with pytest.raises(RuleParseError) as exc:
        load_rules(
            _doc({
                "id": "bad",
                "src": {"subtype": "git_commit"},  # missing field
                "dst": {"subtype": "open_spec_change", "field": "change_id"},
                "matcher": "subject_prefix",
            }),
            source="t",
        )
    assert "src.field" in str(exc.value)


def test_rejects_alias_table_without_via_alias_table():
    with pytest.raises(RuleParseError):
        load_rules(
            _doc({
                "id": "bad",
                "src": {"subtype": "entity_mention", "field": "entity_value"},
                "dst": {"subtype": "open_spec_change", "field": "change_id"},
                "matcher": "alias_table",
            }),
            source="t",
        )


def test_rejects_duplicate_rule_ids():
    with pytest.raises(RuleParseError) as exc:
        load_rules(
            _doc(
                {
                    "id": "dup",
                    "src": {"subtype": "a", "field": "f"},
                    "dst": {"subtype": "b", "field": "g"},
                    "matcher": "exact",
                },
                {
                    "id": "dup",
                    "src": {"subtype": "a", "field": "f"},
                    "dst": {"subtype": "b", "field": "g"},
                    "matcher": "exact",
                },
            ),
            source="t",
        )
    assert "duplicate rule id" in str(exc.value)


def test_substrate_shipped_yamls_parse():
    """Every cross-link YAML shipped under an ontology module's
    `cross_links/` subdir parses, and rule IDs are globally
    unique across all modules.

    Per `add-module-manifests-and-composition` (2026-05-19): the
    legacy `library/cross_links/` directory was deleted; each
    YAML now lives next to its owning module's `module.yaml`.
    """
    from pathlib import Path

    from okg.substrate.library.linkers.rules import load_rules_file

    root = (
        Path(__file__).resolve().parents[3]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
    )
    files = sorted(root.glob("*/cross_links/*.yaml"))
    assert len(files) >= 4, (
        f"expected >=4 cross-link YAMLs under {root}, got {len(files)}"
    )
    seen_ids: set[str] = set()
    for path in files:
        rules = load_rules_file(path)
        for r in rules:
            assert r.id not in seen_ids, (
                f"duplicate rule id {r.id} across files"
            )
            seen_ids.add(r.id)
            assert r.matcher in MATCHER_NAMES


def test_entity_mention_alias_rule_uses_mentions_edge():
    from pathlib import Path

    from okg.substrate.library.linkers.rules import load_rules_file

    # mentions_to_canonical.yaml is an intra-extraction rule
    # (entity_mention → alias_table → any canonical subtype).
    rule_path = (
        Path(__file__).resolve().parents[3]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
        / "extraction" / "cross_links" / "mentions_to_canonical.yaml"
    )
    rules = load_rules_file(rule_path)
    rule = next(
        r for r in rules
        if r.id == "entity_mention_resolves_via_alias_table"
    )

    assert rule.edge == "mentions"
    # Issue #1747: a bare code-symbol name links only when it is unique.
    fallback = rule.dst.unique_field_fallback
    assert fallback is not None
    assert (fallback.subtype, fallback.field) == ("code_symbol", "qualified_name")


def _fallback_rule(dst):
    return _doc({
        "id": "fb",
        "src": {"subtype": "entity_mention", "field": "entity_value"},
        "dst": dst,
        "matcher": "alias_table",
    })


def test_unique_field_fallback_requires_alias_table_dst():
    with pytest.raises(RuleParseError, match="needs `via: alias_table`"):
        load_rules(
            _fallback_rule({
                "subtype": "code_symbol",
                "field": "qualified_name",
                "unique_field_fallback": {
                    "subtype": "code_symbol", "field": "qualified_name",
                },
            }),
            source="t",
        )


@pytest.mark.parametrize("value", [
    "code_symbol",
    {"subtype": "code_symbol"},
    {"subtype": "code_symbol", "field": ""},
    {"subtype": "code_symbol", "field": "qualified_name", "pick": "first"},
])
def test_unique_field_fallback_rejects_malformed_block(value):
    with pytest.raises(RuleParseError, match="exactly `subtype` and `field`"):
        load_rules(
            _fallback_rule({"via": "alias_table", "unique_field_fallback": value}),
            source="t",
        )


def test_linker_built_from_shipped_mentions_rule_reads_fallback_subtype():
    """okg-workspace runs the mentions rule alone in `declarative_alias`.

    The publisher hands a linker only the changed nodes of the subtypes it
    reads, so the fallback subtype must be one of them; otherwise a newly
    published code symbol never reaches the fallback.
    """
    from pathlib import Path

    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import load_rules_file

    rule_path = (
        Path(__file__).resolve().parents[3]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
        / "extraction" / "cross_links" / "mentions_to_canonical.yaml"
    )
    linker = DeclarativeLinker(
        rules=load_rules_file(rule_path),
        include_rule_ids=["entity_mention_resolves_via_alias_table"],
    )

    assert [rule.id for rule in linker.rules] == [
        "entity_mention_resolves_via_alias_table",
    ]
    assert linker.reads_subtypes == ("code_symbol", "entity_mention")

"""PACT lane-h-nomos-sql-read-filter, slice 0 (Refs #3002): the read-class path.

``read_class_filter`` / ``filter_rows_by_read_class`` must return exactly what
the per-row filter (``filter_node_rows_for_session`` /
``filter_edge_rows_for_session``) returns: the same rows, in the same order,
with the same attrs (redaction included), in audit and enforce modes, for
every rule shape the policy engine evaluates. Only one representative row per
decision class is evaluated; the rest follow it.

These tests run the real policy engine and runtime boundary behind
``evaluate_with_active_policy``; storage is replaced as in
``test_read_filter_decision_class.py`` (bundle from memory, decisions captured).
The last test runs on PostgreSQL with nothing stubbed.

Lane C's review conditions, each pinned below:
1. keys and representatives inside one pass scope; a representative decided
   under another policy fingerprint refuses the read;
2. representatives are recorded as reads (``_read`` kinds, no read purpose);
3. members map to representatives by position, never by id;
4. a subject or identity-claim argument takes the per-row path;
5. the source set is always in the class key;
6. members get the per-row attrs byte for byte, masked with their own subtype
   classifier;
7. unclassifiable rows go through the per-row filter.
"""
from __future__ import annotations

import json
import random
import uuid
from pathlib import Path
from typing import Any

import pytest

from okg.substrate import fastyaml
from okg.substrate.errors import SubstrateError
from okg.substrate.nomos import read_filter, store
from okg.substrate.nomos.core import CompiledPolicyBundle, compile_policy_graph
from okg.substrate.nomos.read_filter import (
    READ_CLASS_SAFE_RULE_KEYS,
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
    filter_rows_by_read_class,
    read_class_filter,
)
from okg.substrate.nomos.read_masking import REDACTION_MARKER, ReadRedactedAttrs
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    combine_source_lineages,
    normalize_source_lineage,
    source_record_ref,
)

import test_read_filter_decision_class as _decision_class
from test_read_filter_decision_class import (
    DEPLOYMENT,
    RECORD_KEYS,
    SOURCES,
    SUBJECT,
    _bundle,
    _Conn,
    _Engine,
    _row,
    _rows,
    _session,
)

#: The decision-class tests' engine fixture, shared by these tests. Bound by
#: assignment so a test's ``engine`` parameter does not shadow an import.
engine = _decision_class.engine

REPO_ROOT = Path(__file__).resolve().parents[3]
#: The real catalog lookup, kept before the autouse fixture replaces it.
_REAL_PII_LOADER = read_filter._load_subtype_pii_classes

#: Per-subtype PII declarations, so a member's OWN classifier matters: a
#: ``doc`` masks ``name`` and drops ``body``; a ``ticket`` declares nothing
#: (legacy masking); a ``page`` declares an empty map (classifies nothing).
_SUBTYPE_PII = {
    "doc": {"name": "direct_identifier", "body": "free_text"},
    "page": {},
}


@pytest.fixture(autouse=True)
def _subtype_pii(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_filter, "_load_subtype_pii_classes", lambda conn, cv: _SUBTYPE_PII)


def _canon(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """Rows in order, each with its attrs' type and a byte-exact rendering."""
    out = []
    for row in rows:
        try:
            text = json.dumps(row, sort_keys=True, default=repr)
        except (TypeError, ValueError):
            text = repr(sorted((str(k), repr(v)) for k, v in row.items()))
        out.append((type(row.get("attrs")).__name__, text))
    return out


def _per_row(kind: str, session: Any, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exact = filter_node_rows_for_session if kind == "node" else filter_edge_rows_for_session
    return exact(_Conn(), session, [dict(r) for r in rows])


def _by_class(kind: str, session: Any, rows: list[dict[str, Any]]) -> tuple[list[dict], Any]:
    return read_class_filter(_Conn(), session, [dict(r) for r in rows], row_kind=kind)


def _with_payload(rng: random.Random, row: dict[str, Any]) -> dict[str, Any]:
    """Attrs a redaction can touch, and subtypes with different classifiers."""
    row = dict(row)
    row["attrs"] = {**row["attrs"], "body": f"text {rng.randrange(9)}", "email": "a@b.example"}
    if "subtype" in row:
        row["subtype"] = rng.choice(("doc", "ticket", "page"))
    return row


def _redacting_bundle(rng: random.Random, kind: str, rows: list[dict[str, Any]]) -> Any:
    """A random bundle, with a class-safe redact rule in front half the time."""
    bundle = _bundle(rng, kind, rows)
    if rng.random() < 0.5:
        return bundle
    graph = {
        "policy_bundle_id": "nomos:test:read-class-redact",
        "policy_version": "v1",
        "default_effect": bundle.default_effect,
        "rules": [
            {
                "rule_id": "redact_front",
                "effect": "redact",
                "actions": ["*"],
                "subjects": ["*"],
                "match": rng.choice((
                    {"source_id": rng.choice(SOURCES)},
                    {"source_ids": rng.choice(SOURCES)},
                    {"classification": rng.choice(("internal", "restricted"))},
                    {"resource_kind": "source_record"},
                )),
                "priority": 100,  # the random rules use 0-5
                "obligations": [{"type": "redaction"}],
            },
            *(rule.to_dict() for rule in bundle.rules),
        ],
    }
    return compile_policy_graph(graph)


# --------------------------------------------------------------------------- #
# T1: randomized equivalence, every rule shape                                 #
# --------------------------------------------------------------------------- #
def _new_stats() -> dict[str, int]:
    return {"class_path": 0, "per_row_path": 0, "redacted": 0, "hidden": 0, "mismatch": 0}


def _compare(stats: dict[str, int], kind: str, session: Any, rows: list[dict[str, Any]]) -> None:
    """One read through both filters under the engine's current bundle."""
    expected = _canon(_per_row(kind, session, rows))
    got, path = _by_class(kind, session, rows)
    if _canon(got) != expected:
        stats["mismatch"] += 1
        return
    stats["class_path" if path.path == "read_class" else "per_row_path"] += 1
    stats["redacted"] += any(t == "ReadRedactedAttrs" for t, _ in expected)
    stats["hidden"] += len(expected) < len(rows)


def _equivalence_sweep(engine: _Engine, kind: str, mode: str, seeds: int) -> dict[str, int]:
    engine.mode = mode
    session = _session()
    stats = _new_stats()
    for seed in range(seeds):
        rng = random.Random(f"read-class:{kind}:{mode}:{seed}")
        rows = [_with_payload(rng, r) for r in _rows(rng, kind, rng.randint(1, 40))]
        engine.bundle = _redacting_bundle(rng, kind, rows)
        _compare(stats, kind, session, rows)
    return stats


def _role_rows(kind: str) -> list[dict[str, Any]]:
    """30 fixed rows: three sources, mixed classifications, every fifth row
    carrying a two-source lineage, node subtypes with different classifiers,
    and a redactable payload. Rows of one classification but different
    sources get different decisions under the role bundles below."""
    rows = []
    for i in range(30):
        attrs: dict[str, Any] = {"name": f"Author {i}", "body": "text", "email": "a@b.example"}
        classification = ("internal", "restricted", None, "public")[i % 4]
        if classification is not None:
            attrs["classification"] = classification
        if i % 5 == 0:
            attrs[LINEAGE_ATTR] = combine_source_lineages([
                normalize_source_lineage(source_id=s, source_record_id={"key": "a"},
                                         source_revision={"v": 1})
                for s in (SOURCES[0], SOURCES[2])
            ])
        row: dict[str, Any] = {
            f"{kind}_id": f"{kind}:{i:02d}", "attrs": attrs, "source": SOURCES[i % 3],
            "source_record_id": {"key": "ab"[i % 2]}, "source_revision": {"v": 1},
        }
        if kind == "node":
            row["subtype"] = ("doc", "ticket", "page")[i % 3]
        else:
            row.update(src=f"n:{i}", dst="n:0", edge_type="member_of")
        rows.append(row)
    return rows


def _role_bundle(role: str) -> Any:
    """``class_safe`` names only read-class keys (a source deny, a
    classification deny, a source redaction), so the read-class path decides
    it; ``blocking`` adds a rule on a record id, a key outside them, so the
    read falls back to the per-row filter."""
    rules: list[dict[str, Any]] = [
        {"rule_id": "deny_jira", "effect": "deny", "actions": ["*"], "subjects": ["*"],
         "match": {"source_id": "jira"}, "priority": 3},
        {"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"], "subjects": ["*"],
         "match": {"classification": "restricted"}, "priority": 2},
        {"rule_id": "redact_docs", "effect": "redact", "actions": ["*"], "subjects": ["*"],
         "match": {"source_ids": "docs"}, "priority": 1, "obligations": [{"type": "redaction"}]},
    ]
    if role == "blocking":
        rules.append({"rule_id": "deny_record", "effect": "deny", "actions": ["*"],
                      "subjects": ["*"], "match": {"source_record_id": {"key": "b"}},
                      "priority": 9})
    return compile_policy_graph({
        "policy_bundle_id": f"nomos:test:read-class-{role}", "policy_version": "v1",
        "default_effect": "allow", "rules": rules,
    })


_ROLES = ("class_safe", "blocking")


def _role_stats(engine: _Engine, kind: str, mode: str) -> dict[str, int]:
    engine.mode = mode
    session = _session()
    stats = _new_stats()
    for role in _ROLES:
        engine.bundle = _role_bundle(role)
        _compare(stats, kind, session, _role_rows(kind))
    return stats


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_the_read_class_rows_equal_the_per_row_rows_for_random_rules_and_rows(
    engine: _Engine, kind: str, mode: str,
) -> None:
    # Fixed roles first: one read the read-class path decides, with rows
    # redacted (and hidden under enforce), and one forced onto the per-row
    # path. They reach both paths whatever the random draws do.
    roles = _role_stats(engine, kind, mode)
    assert roles["mismatch"] == 0, roles
    assert roles["class_path"] == 1 and roles["per_row_path"] == 1, roles
    assert roles["redacted"] == 2, roles
    assert roles["hidden"] == (2 if mode == "enforce" else 0), roles
    # Then random rules over random rows: every match shape the engine
    # evaluates. 60 seeds (was 300); the fixed roles above carry the mutation
    # detection the larger sweep reached by chance.
    stats = _equivalence_sweep(engine, kind, mode, seeds=60)

    assert stats["mismatch"] == 0, stats
    # Both paths, redaction and hiding were all exercised, not one trivially.
    assert stats["class_path"] > 12 and stats["per_row_path"] > 6, stats
    assert stats["redacted"] > 4 and stats["hidden"] > 4, stats


# --------------------------------------------------------------------------- #
# T2: okg-workspace-shaped rows under the real okg-workspace baseline bundle   #
# --------------------------------------------------------------------------- #
def _workspace_bundle(extra_rules: list[dict[str, Any]] = ()) -> tuple[CompiledPolicyBundle, list[str], str]:
    deployment_dir = REPO_ROOT / "deployments" / "okg-workspace"
    manifest = fastyaml.safe_load((deployment_dir / "deployment.yaml").read_text()) or {}
    registry = fastyaml.safe_load((deployment_dir / "source_registry.yaml").read_text()) or {}
    sources = sorted((registry.get("sources") or {}).keys())
    graph = store.deployment_baseline_policy_graph(
        manifest,
        deployment_name=DEPLOYMENT,
        scope=f"deployment:{DEPLOYMENT}",
        activated_by="subject:test",
        source_names=sources,
    )
    reader = next(
        rule for rule in graph["rules"]
        if rule["rule_id"] == "rollout_allow_source_lineage_filtered_reads"
    )["subjects"][0]
    graph["rules"] = [*extra_rules, *graph["rules"]]
    return compile_policy_graph(graph), sources, reader


def _workspace_rows(
    rng: random.Random, kind: str, sources: list[str], n: int, always: tuple[str, ...] = (),
) -> list[dict]:
    """Rows shaped like okg-workspace: many sources, derived rows whose lineage
    names several, chunks with text, a source no manifest registers, rows with
    no lineage, classification set per row. ``always`` are among the six
    sources every read uses (the sources a test's extra rules name)."""
    rest = [s for s in sources if s not in always]
    used = [*always, *rng.sample(rest, 6 - len(always))] + ["unregistered-source"]
    rows = []
    for i in range(n):
        attrs: dict[str, Any] = {"name": f"item {i}", "body": "x" * rng.randrange(1, 40)}
        if rng.random() < 0.3:
            attrs["classification"] = rng.choice(("internal", "restricted", "public"))
        source: str | None = rng.choice(used)
        shape = rng.random()
        if shape < 0.2:
            attrs[LINEAGE_ATTR] = combine_source_lineages([
                normalize_source_lineage(
                    source_id=s, source_record_id={"path": f"f{rng.randrange(20)}.py"},
                    source_revision={"sha": f"{rng.randrange(4):040x}"},
                )
                for s in rng.sample(used[:-1], rng.randint(2, 3))
            ])
            source = None
        elif shape < 0.25:
            source = None  # no lineage: hidden by the per-row filter
        row: dict[str, Any] = {
            f"{kind}_id": f"{kind}:{i}",
            "attrs": attrs,
            "source": source,
            "source_record_id": {"path": f"f{rng.randrange(20)}.py"} if source else None,
            "source_revision": {"sha": f"{rng.randrange(4):040x}"} if source else None,
        }
        if kind == "node":
            row["subtype"] = rng.choice(("doc", "document_chunk", "ticket"))
        else:
            row.update(src=f"node:{i}", dst="node:0", edge_type=rng.choice(("member_of", "contains")))
        rows.append(row)
    return rows


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("variant", ["baseline", "deny_one_redact_another"])
def test_okg_workspace_shaped_rows_under_its_real_baseline_bundle(
    engine: _Engine, kind: str, mode: str, variant: str,
) -> None:
    engine.mode = mode
    probe, sources, _ = _workspace_bundle()
    extra: list[dict[str, Any]] = []
    if variant == "deny_one_redact_another":
        extra = [
            {"rule_id": "deny_one", "effect": "deny", "actions": ["*"], "subjects": ["*"],
             "match": {"source_id": sources[0]}, "priority": 900},
            {"rule_id": "redact_other", "effect": "redact", "actions": ["*"], "subjects": ["*"],
             "match": {"source_ids": sources[1]}, "priority": 900,
             "obligations": [{"type": "redaction"}]},
        ]
    bundle, sources, reader = _workspace_bundle(extra)
    # The denied and the redacted source are in every read, so each of the
    # fewer reads below exercises them (40 random reads used to, by chance).
    named = (sources[0], sources[1]) if extra else ()
    engine.bundle = bundle
    session = _session(nomos_subject=reader, nomos_read_action=None, nomos_read_surface=None)
    shown = hidden = redacted = 0
    # 12 reads of 60 rows each (was 40); every read takes the read-class path.
    for seed in range(12):
        rng = random.Random(f"workspace:{kind}:{mode}:{variant}:{seed}")
        rows = _workspace_rows(rng, kind, sources, 60, always=named)
        expected = _canon(_per_row(kind, session, rows))
        got, path = _by_class(kind, session, rows)

        assert path.path == "read_class", path
        assert _canon(got) == expected, seed
        assert path.classes < path.class_rows, path  # classes are shared
        shown += len(expected)
        hidden += len(rows) - len(expected)
        redacted += sum(t == "ReadRedactedAttrs" for t, _ in expected)
    assert shown and hidden
    if variant == "deny_one_redact_another" or mode == "audit":
        assert redacted or variant == "baseline"


# --------------------------------------------------------------------------- #
# Whole-read gates: the per-row path                                           #
# --------------------------------------------------------------------------- #
def _plain_rows(kind: str, n: int = 8) -> list[dict[str, Any]]:
    rows = []
    for i in range(n):
        row = {f"{kind}_id": f"{kind}:{i}", "source": SOURCES[i % 3],
               "source_record_id": {"key": RECORD_KEYS[i % 4]}, "source_revision": {"v": 1},
               "attrs": {"classification": "internal", "name": f"n{i}", "body": "b"}}
        if kind == "node":
            row["subtype"] = "doc"
        else:
            row.update(src="n:1", dst="n:2", edge_type="links")
        rows.append(row)
    return rows


def _one_rule_bundle(match: dict[str, Any], effect: str = "deny", **rule: Any) -> Any:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:read-class-gate", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "r", "effect": effect, "actions": ["*"], "subjects": ["*"],
                   "match": match, **rule}],
    })


@pytest.mark.parametrize(
    "match",
    [
        {"node_id": "node:3"},
        {"graph_ref": "graph:node:node:3"},
        {"node_ref": "graph:node:node:3"},
        {"source_record_ref": source_record_ref("git", {"key": "a"})},
        {"source_record_refs": [source_record_ref("git", {"key": "a"})]},
        {"source_record_id": {"key": "b"}},
        {"source_revision": {"v": 1}},
        {"source_lineage": {}},
        {"is_hidden_ref": True},
        {"context_id": "ctx:1"},
        {"subtype": "doc"},
        {"a_key_no_resource_has": 1},
    ],
)
def test_a_rule_naming_a_key_outside_the_allow_list_sends_the_whole_read_per_row(
    engine: _Engine, match: dict[str, Any],
) -> None:
    rows = _plain_rows("node")
    engine.bundle = _one_rule_bundle(match)

    got, path = _by_class("node", _session(), rows)

    assert path.path == "per_row" and path.reason.startswith("rule_names_key:"), path
    assert _canon(got) == _canon(_per_row("node", _session(), rows))


def test_the_allow_list_holds_only_constant_and_class_facts() -> None:
    assert READ_CLASS_SAFE_RULE_KEYS == {
        "deployment", "scope", "kind", "audit", "subject", "action", "principal_kind",
        "nomos_surface", "nomos_boundary", "nomos_read_purpose", "source_ids", "source_id",
        "resource_kind", "classification",
    }


@pytest.mark.parametrize(
    "rule",
    [
        {"actions": ["export.bundle"]},  # an action this read never evaluates
        {"subjects": ["subject:someone-else"]},  # a subject this read is not
    ],
)
def test_a_rule_that_cannot_match_this_read_does_not_force_the_per_row_path(
    engine: _Engine, rule: dict[str, Any],
) -> None:
    rows = _plain_rows("node")
    engine.bundle = _one_rule_bundle({"node_id": "node:3"}, **rule)

    got, path = _by_class("node", _session(), rows)

    assert path.path == "read_class", path
    assert _canon(got) == _canon(_per_row("node", _session(), rows))


@pytest.mark.parametrize(
    ("session_overrides", "call", "reason"),
    [
        ({}, {"subject": SUBJECT}, "subject_argument"),
        ({}, {"identity_claim": {"iss": "t", "sub": "r"}}, "identity_claim_argument"),
        ({"nomos_identity_claim": {"iss": "t", "sub": "r"}}, {}, "identity_claim_session"),
        ({"nomos_access_context": {"context_id": "ctx:1"}}, {}, "access_context_session"),
        ({"nomos_access_context": {}}, {}, "access_context_session"),
    ],
)
def test_identity_and_access_context_reads_take_the_per_row_path(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
    session_overrides: dict[str, Any], call: dict[str, Any], reason: str,
) -> None:
    from types import SimpleNamespace

    from okg.substrate.nomos import identity_store

    def claim(conn: Any, *, resource: Any, **_: Any) -> Any:
        effect = "deny" if resource["source_id"] == "jira" else "allow"
        return SimpleNamespace(
            decision=SimpleNamespace(effect=effect, decision_id=f"claim:{effect}"),
            resolved_subject=SUBJECT,
        )

    monkeypatch.setattr(identity_store, "evaluate_identity_claim_from_postgres", claim)
    rows = _plain_rows("node", 9)
    engine.bundle = _one_rule_bundle({"classification": "restricted"})
    session = _session(**session_overrides)
    exact = filter_node_rows_for_session(_Conn(), session, [dict(r) for r in rows], **call)

    got, path = read_class_filter(_Conn(), session, [dict(r) for r in rows], row_kind="node", **call)

    assert (path.path, path.reason) == ("per_row", reason)
    assert _canon(got) == _canon(exact)


def test_an_access_context_with_hidden_refs_is_decided_per_row(engine: _Engine) -> None:
    rows = _plain_rows("node")
    hidden = source_record_ref("git", {"key": "a"})
    engine.bundle = _one_rule_bundle({"is_hidden_ref": True})
    session = _session(nomos_access_context={"context_id": "ctx:1", "hidden_refs": [hidden]})

    got, path = _by_class("node", session, rows)

    assert path.path == "per_row"
    assert _canon(got) == _canon(_per_row("node", session, rows))
    assert "node:0" not in {r["node_id"] for r in got}


def test_without_the_runtime_rows_come_back_unfiltered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    rows = [{"node_id": "node:1"}, {"node_id": "node:2"}]

    got, path = read_class_filter(_Conn(), _session(), rows, row_kind="node")

    assert got == rows and path.path == "off"


def test_an_unknown_row_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="row_kind"):
        filter_rows_by_read_class(_Conn(), _session(), [], row_kind="graph")


def test_a_multi_user_read_with_no_subject_is_refused_even_with_nomos_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    with pytest.raises(Exception, match="request-scoped subject"):
        read_class_filter(
            _Conn(), _session(nomos_multi_user=True, nomos_subject=None),
            [{"node_id": "node:1"}], row_kind="node",
        )


# --------------------------------------------------------------------------- #
# Conditions 2, 3, 5, 6, 7                                                     #
# --------------------------------------------------------------------------- #
def test_only_representatives_are_recorded_as_reads_tagged_with_their_class(
    engine: _Engine,
) -> None:
    rows = [
        {"node_id": f"node:{i:02d}", "subtype": "doc", "source": SOURCES[i % 2],
         "source_record_id": {"key": "a"}, "source_revision": None,
         "attrs": {"classification": "restricted" if i % 3 == 0 else "internal"}}
        for i in range(12)
    ]
    engine.bundle = _one_rule_bundle({"classification": "restricted"})

    got, path = _by_class("node", _session(), rows)
    decisions = list(engine.decisions)

    assert _canon(got) == _canon(_per_row("node", _session(), rows))
    # 2 sources x 2 classifications = 4 classes; the representative of each is
    # its first member in id order.
    assert path.classes == 4 and path.class_rows == 12
    decided = {target["node_id"] for _, target in decisions}
    assert decided == {"node:00", "node:01", "node:02", "node:03"}
    for decision, target in decisions:
        assert target["kind"].endswith("_read"), target  # not a count
        assert "nomos_read_purpose" not in decision.context
        assert target["decided_by"] == "read_class"
        assert len(target["read_class"]) == 32
    members = {
        (target["node_id"], target["class_member_count"]) for _, target in decisions
    }
    assert members == {("node:00", 2), ("node:01", 4), ("node:02", 4), ("node:03", 2)}


def test_the_source_set_is_in_the_class_key_even_when_no_rule_names_it(
    engine: _Engine,
) -> None:
    """Condition 5: rows from different sources never share a class, so each
    class decision names its members' sources."""
    rows = _plain_rows("node", 9)
    engine.bundle = _one_rule_bundle({"deployment": DEPLOYMENT}, effect="allow")

    _, path = _by_class("node", _session(), rows)

    assert path.path == "read_class" and path.classes == 3
    sources = {target.get("source_id") for _, target in engine.decisions if "source_id" in target}
    assert sources == set(SOURCES)


def test_duplicate_ids_map_by_position_not_by_id(engine: _Engine) -> None:
    """Condition 3: two rows share an id; one is hidden, one is shown. Mapping
    by id would leak the hidden one or drop the shown one."""
    shown = {"node_id": "node:dup", "subtype": "doc", "source": "git",
             "source_record_id": {"key": "a"}, "source_revision": {"v": 1},
             "attrs": {"classification": "internal", "name": "shown"}}
    hidden = {**shown, "source": "jira", "attrs": {"classification": "internal", "name": "hidden"}}
    rows = [hidden, shown, dict(hidden), dict(shown)]
    engine.bundle = _one_rule_bundle({"source_id": "jira"})

    got, path = _by_class("node", _session(), rows)

    assert path.path == "read_class"
    assert [r["attrs"]["name"] for r in got] == ["shown", "shown"]
    assert _canon(got) == _canon(_per_row("node", _session(), rows))


def test_members_are_masked_with_their_own_subtype_classifier(engine: _Engine) -> None:
    """Condition 6: one class, three subtypes; each member masked as the
    per-row filter masks it, with the redaction marker set."""
    rows = [
        {"node_id": f"node:{i}", "subtype": ("doc", "ticket", "page")[i % 3], "source": "git",
         "source_record_id": {"key": "a"}, "source_revision": {"v": 1},
         "attrs": {"classification": "internal", "name": f"Person {i}", "body": "secret text",
                   "email": "someone@example.org"}}
        for i in range(6)
    ]
    engine.bundle = _one_rule_bundle({"source_id": "git"}, effect="redact",
                                     obligations=[{"type": "redaction"}])

    got, path = _by_class("node", _session(), rows)

    assert path.path == "read_class" and path.classes == 1
    assert all(isinstance(r["attrs"], ReadRedactedAttrs) for r in got)
    assert all(r["attrs"][REDACTION_MARKER] is True for r in got)
    assert _canon(got) == _canon(_per_row("node", _session(), rows))
    by_subtype = {r["subtype"]: dict(r["attrs"]) for r in got}
    assert by_subtype["doc"] != by_subtype["page"]  # the classifier differed


def test_a_classification_override_splits_the_class(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The class key reads the EFFECTIVE classification: an accepted overlay
    on one row puts it in its own class."""

    def overlay(conn: Any, rows: list[dict[str, Any]], **_: Any) -> list[dict[str, Any]]:
        return [
            {**r, "attrs": {**r["attrs"], "classification": "restricted"}}
            if r.get("node_id") == "node:4" else r
            for r in rows
        ]

    monkeypatch.setattr(read_filter, "apply_effective_attrs_to_rows", overlay)
    rows = [{**r, "source": "git"} for r in _plain_rows("node")]
    engine.bundle = _one_rule_bundle({"classification": "restricted"})

    got, path = _by_class("node", _session(), rows)

    assert path.classes == 2
    assert "node:4" not in {r["node_id"] for r in got}
    assert _canon(got) == _canon(_per_row("node", _session(), rows))


def test_rows_missing_class_facts_go_through_the_per_row_filter(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Condition 7 and missing class facts: a row with no lineage is hidden with
    no decision; an unclassifiable row is decided by the per-row filter.

    On the read-class path every projected fact is an allow-listed key whose
    value is canonical JSON, so no real row is unclassifiable today; the
    projection's canonical-JSON writer is made to refuse one row's source to
    reach the branch."""
    real_fact_text = read_filter._fact_text

    def refuse_odd(value: Any) -> str:
        if "odd-source" in repr(value):
            raise TypeError("not canonical JSON")
        return real_fact_text(value)

    monkeypatch.setattr(read_filter, "_fact_text", refuse_odd)
    good = _plain_rows("node", 4)
    no_lineage = {"node_id": "node:none", "subtype": "doc", "source": None, "attrs": {}}
    odd = {**good[0], "node_id": "node:odd", "source": "odd-source"}
    rows = [good[0], no_lineage, odd, *good[1:]]
    engine.bundle = _one_rule_bundle({"source_id": "jira"})

    got, path = _by_class("node", _session(), rows)

    assert path.path == "read_class" and path.per_row_rows == 2
    assert _canon(got) == _canon(_per_row("node", _session(), rows))
    assert "node:none" not in {r["node_id"] for r in got}
    assert not any(t.get("node_id") == "node:none" for _, t in engine.decisions)
    assert any(t.get("node_id") == "node:odd" and "read_class" not in t
               for _, t in engine.decisions)


def test_no_active_bundle_hides_every_row_in_enforce(engine: _Engine) -> None:
    rows = [_row(random.Random(i), "edge", f"edge:{i}") for i in range(10)]
    engine.bundle = None

    got, path = _by_class("edge", _session(), rows)

    assert got == []
    # PACT lane-h-nomos-a-part1 (decision 123a): one decision for the call.
    assert path.path == "no_active_bundle"
    assert len(engine.decisions) == 1
    assert _per_row("edge", _session(), rows) == []


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_no_active_bundle_returns_the_per_row_rows_with_one_decision(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch, kind: str, mode: str,
) -> None:
    """Decision 123a: with no active bundle the call returns exactly the
    per-row filter's rows -- same order, attrs byte-equal (lineage stamped) --
    under audit and none under enforce, records ONE ``read`` decision, and
    runs no class projection or per-row evaluation."""
    engine.mode = mode
    engine.bundle = None
    rng = random.Random(f"read-class-no-bundle:{kind}:{mode}")
    rows = [_with_payload(rng, row) for row in _rows(rng, kind, 200)]
    expected = _per_row(kind, _session(), rows)
    engine.decisions.clear()
    _decision_class._refuse_per_row_work(monkeypatch)

    got, path = _by_class(kind, _session(), rows)

    assert _canon(got) == _canon(expected)
    assert (len(got) > 0) == (mode == "audit")
    assert path.path == "no_active_bundle" and path.classes == 0
    assert len(engine.decisions) == 1
    decision, target = engine.decisions[0]
    assert decision.reason_code == "nomos.default_deny.no_active_bundle"
    assert read_filter.READ_PURPOSE_FACT not in decision.context
    assert target["kind"] == f"local_operator_{kind}_no_active_bundle_read"
    assert target["enforcement_mode"] == mode
    assert target["rows_decided"] == path.class_rows


def test_no_active_bundle_still_leaves_a_subject_argument_on_the_per_row_path(
    engine: _Engine,
) -> None:
    """The per-row refusals are checked before the bundle: a caller that names
    a subject is decided row by row, as before."""
    engine.mode = "audit"
    engine.bundle = None
    rows = [_row(random.Random(i), "node", f"node:{i}") for i in range(6)]

    got, path = read_class_filter(
        _Conn(), _session(), [dict(r) for r in rows], row_kind="node", subject=SUBJECT,
    )

    assert path.path == "per_row" and path.reason == "subject_argument"
    assert len(engine.decisions) > 1


# --------------------------------------------------------------------------- #
# Fail closed: a class decision under another policy refuses the read          #
# --------------------------------------------------------------------------- #
def test_a_policy_activated_mid_read_refuses_the_read(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Condition 1: the keys come from bundle A; the representatives are then
    evaluated under bundle B (as if the memo did not hold). The read is
    refused; no row is returned and nothing falls back to allow."""
    old = _one_rule_bundle({"source_id": "jira"})
    new = _one_rule_bundle({"source_id": "git"})
    served = iter([old])

    def load(conn: Any, **_: Any) -> Any:
        return next(served, new)

    monkeypatch.setattr(read_filter, "load_active_policy_bundle", load)
    monkeypatch.setattr(store, "load_active_policy_bundle", load)

    with pytest.raises(SubstrateError, match="different policy"):
        filter_rows_by_read_class(_Conn(), _session(), _plain_rows("node"), row_kind="node")


def test_a_decision_from_an_older_policy_refuses_the_read(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A representative's decision that carries another fingerprint (a stale
    cached decision) refuses the read, in audit mode too."""
    engine.mode = "audit"
    engine.bundle = _one_rule_bundle({"source_id": "jira"})
    import dataclasses

    real = read_filter.evaluate_with_active_policy

    def stale(*args: Any, **kwargs: Any) -> Any:
        decision = real(*args, **kwargs)
        return dataclasses.replace(decision, policy_fingerprint="sha256:older")

    monkeypatch.setattr(read_filter, "evaluate_with_active_policy", stale)

    with pytest.raises(SubstrateError, match="different policy"):
        filter_rows_by_read_class(_Conn(), _session(), _plain_rows("node"), row_kind="node")


def test_a_class_returned_with_no_decision_refuses_the_read(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine.bundle = _one_rule_bundle({"source_id": "jira"})
    monkeypatch.setattr(
        read_filter, "_filter_rows",
        lambda conn, session, rows, **_: [dict(r) for r in rows],
    )

    with pytest.raises(SubstrateError, match="no recorded decision"):
        filter_rows_by_read_class(_Conn(), _session(), _plain_rows("node"), row_kind="node")


# --------------------------------------------------------------------------- #
# Mutations: each must break the equivalence (kill check)                      #
# --------------------------------------------------------------------------- #
def _cases_with_wrong_answers(engine: _Engine) -> int:
    """How many of the four kind and mode cases answer wrongly on the fixed
    roles. The class-safe role holds rows of one classification from sources
    with different decisions (denied, redacted, allowed), so a class key that
    drops the source facts, or members returned unmasked, answers wrongly in
    every case, audit included."""
    return sum(
        _role_stats(engine, kind, mode)["mismatch"] > 0
        for kind in ("node", "edge") for mode in ("enforce", "audit")
    )


def test_mutation_a_class_key_that_drops_a_rule_named_key_is_caught(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = read_filter._read_class_keys

    def dropped(bundle: Any) -> frozenset[str]:
        return real(bundle) - {"source_id", "source_ids"}

    monkeypatch.setattr(read_filter, "_read_class_keys", dropped)

    assert _cases_with_wrong_answers(engine) == 4


def test_mutation_skipping_member_redaction_is_caught(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = read_filter._read_class_member_row
    monkeypatch.setattr(
        read_filter, "_read_class_member_row",
        lambda row, attrs, *, redact_with: real(row, attrs, redact_with=None),
    )

    assert _cases_with_wrong_answers(engine) == 4


# Not a mutation test: taking the class path for an access-context session is
# NOT caught by equivalence, because the projection keys on every rule-named
# fact, access-context facts included, so the answer stays exact. The gate is a
# second guard, pinned by test_identity_and_access_context_reads_take_the_per_row_path.


# --------------------------------------------------------------------------- #
# PostgreSQL: nothing stubbed                                                  #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_on_postgres_read_class_rows_and_trail_match_the_per_row_filter(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The active bundle, accepted overlays, subtype metadata lookup and the
    decision rows are real. Nodes and edges, audit and enforce: the read-class
    rows equal the per-row rows, and the trail holds one tagged decision per
    class and target under the representative only."""
    import psycopg

    from okg.substrate.nomos import activate_policy_bundle

    monkeypatch.setattr(read_filter, "_load_subtype_pii_classes", _REAL_PII_LOADER)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    run = uuid.uuid4().hex[:12]
    deployment = f"read-class-{run}"
    generation = f"gen:read-class:{run}"
    nodes = [
        {"node_id": f"node:{i:02d}", "subtype": "doc", "source": SOURCES[i % 3],
         "source_record_id": {"key": RECORD_KEYS[i % 4]}, "source_revision": {"v": 1},
         "attrs": {"classification": "internal", "name": f"Person {i}", "body": "text"}}
        for i in range(24)
    ]
    edges = [
        {"edge_id": f"edge:{i:02d}", "src": f"node:{i:02d}", "dst": "node:00",
         "edge_type": "member_of", "source": SOURCES[i % 3],
         "source_record_id": {"key": RECORD_KEYS[i % 4]}, "source_revision": {"v": 1},
         "attrs": {"chunk_ordinal": i}}
        for i in range(24)
    ]
    results: dict[str, Any] = {}
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=compile_policy_graph({
                "policy_bundle_id": "nomos:test:read-class-db", "policy_version": "v1",
                "default_effect": "allow",
                "rules": [
                    {"rule_id": "deny_jira", "effect": "deny", "actions": ["*"],
                     "subjects": ["*"], "match": {"source_id": "jira"}, "priority": 3},
                    {"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"],
                     "subjects": ["*"], "match": {"classification": "restricted"},
                     "priority": 2},
                    {"rule_id": "redact_docs", "effect": "redact", "actions": ["*"],
                     "subjects": ["*"], "match": {"source_ids": "docs"}, "priority": 1,
                     "obligations": [{"type": "redaction"}]},
                ],
            }),
            scope=f"deployment:{deployment}",
            activated_by="test:operator",
            bootstrap=True,
        )
        for i in (3, 9):  # git rows reclassified by an accepted overlay
            conn.execute(
                "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id,"
                " generation_id, attr_key, observation_id, value, value_hash, producer)"
                " VALUES ('node', %s, %s, 'classification', %s, %s::jsonb, 'h', 'test')",
                (f"node:{i:02d}", generation, f"obs:{i}", '"restricted"'),
            )
        conn.commit()
        session = _session(deployment_name=deployment, generation_id=generation)
        for mode in ("enforce", "audit"):
            monkeypatch.setattr(store, "deployment_nomos_enforcement_mode", lambda name, m=mode: m)
            for kind, rows in (("node", nodes), ("edge", edges)):
                exact = filter_node_rows_for_session if kind == "node" else filter_edge_rows_for_session
                # The class read first: the trail is insert-once, so a per-row
                # read of the same representative first would keep its row.
                got, path = read_class_filter(conn, session, [dict(r) for r in rows], row_kind=kind)
                results[f"{mode}:{kind}:class"] = _canon(got)
                results[f"{mode}:{kind}:path"] = path
                results[f"{mode}:{kind}:per_row"] = _canon(exact(conn, session, [dict(r) for r in rows]))
        conn.commit()
        trail = conn.execute(
            "SELECT target FROM okg.nomos_decisions"
            " WHERE deployment_name = %s AND target ? 'read_class'",
            (deployment,),
        ).fetchall()

    for mode in ("enforce", "audit"):
        for kind in ("node", "edge"):
            assert results[f"{mode}:{kind}:path"].path == "read_class"
            assert results[f"{mode}:{kind}:class"] == results[f"{mode}:{kind}:per_row"], (mode, kind)
    targets = [row[0] for row in trail]
    assert targets, "the class decisions were recorded"
    assert all(t["decided_by"] == "read_class" for t in targets)
    assert all(t["kind"].endswith("_read") for t in targets)
    # Node classes: (git, internal), (git, restricted), (jira), (docs); each
    # representative is the first member in id order.
    node_reps = {t["node_id"] for t in targets if "node_id" in t}
    assert node_reps == {"node:00", "node:01", "node:02", "node:03"}
    counts = {t["node_id"]: t["class_member_count"] for t in targets if "node_id" in t}
    assert counts == {"node:00": 6, "node:01": 8, "node:02": 8, "node:03": 2}


# --------------------------------------------------------------------------- #
# Record text end to end: member_of + contains, contains-only (example-kb), and    #
# member_of-only graphs, every page and a chunk's parent list                  #
# --------------------------------------------------------------------------- #
class _RecordGraph:
    """Answers record text's queries from in-memory nodes and edges."""

    def __init__(self, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
        self.nodes = {n["node_id"]: n for n in nodes}
        self.edges = edges

    def _chunk(self, node_id: Any) -> bool:
        node = self.nodes.get(node_id)
        return bool(node) and node["subtype"] == "document_chunk"

    def query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        limit = params.get("raw_limit", 10**9)

        def edges(pred: Any) -> list[dict[str, Any]]:
            rows = sorted((e for e in self.edges if pred(e)), key=lambda e: e["edge_id"])
            return [dict(e) for e in rows[:limit]]

        if "WHERE node_id = :node_id LIMIT 1" in sql:
            node = self.nodes.get(params["node_id"])
            return [dict(node)] if node else []
        if "e.dst = :node_id AND e.edge_type = 'member_of'" in sql:
            return edges(lambda e: e["dst"] == params["node_id"]
                         and e["edge_type"] == "member_of" and self._chunk(e["src"]))
        if "e.src = :node_id AND e.edge_type = 'contains'" in sql:
            return edges(lambda e: e["src"] == params["node_id"]
                         and e["edge_type"] == "contains" and self._chunk(e["dst"]))
        if "e.src = :chunk_id AND e.edge_type = 'member_of'" in sql:
            return edges(lambda e: e["src"] == params["chunk_id"] and e["edge_type"] == "member_of")
        if "e.dst = :chunk_id AND e.edge_type = 'contains'" in sql:
            return edges(lambda e: e["dst"] == params["chunk_id"] and e["edge_type"] == "contains")
        if "node_id = ANY(:ids)" in sql:
            want = [i for i in params["ids"] if i in self.nodes]
            if "subtype = :chunk_subtype" in sql:
                want = [i for i in want if self._chunk(i)]
            return [dict(self.nodes[i]) for i in want]
        raise AssertionError(f"unexpected query: {sql}")


def _record_corpus(rng: random.Random, shape: str) -> tuple[list[dict], list[dict], list[str]]:
    """Records with overlapping text windows, from two sources, with mixed
    classifications and a few multi-source chunks. The first record has 40
    chunks, so every corpus has a record that reads in several pages; the rest
    have 1-20, which keeps the six cases fast."""
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    records = []
    for r in range(4):
        rec = f"doc:{r}"
        records.append(rec)
        src = SOURCES[r % 2]
        nodes.append({"node_id": rec, "subtype": "document", "source": src,
                      "source_record_id": {"key": f"file{r}"}, "source_revision": {"v": 1},
                      "attrs": {"classification": "internal", "title": f"Doc {r}"}})
        total = 40 if r == 0 else rng.randint(1, 20)
        text = "".join(chr(97 + (i % 26)) for i in range(total * 60 + 40))
        for k in range(total):
            cid = f"chunk:{r}:{k:03d}"
            attrs: dict[str, Any] = {"text": text[k * 60: k * 60 + 100], "name": f"Author {r}"}
            if rng.random() < 0.3:
                attrs["classification"] = rng.choice(("internal", "restricted", "public"))
            chunk_src = src if rng.random() < 0.85 else SOURCES[2]
            if rng.random() < 0.1:
                attrs[LINEAGE_ATTR] = combine_source_lineages([
                    normalize_source_lineage(source_id=s, source_record_id={"key": f"file{r}"},
                                             source_revision={"v": 1})
                    for s in (src, SOURCES[2])
                ])
            nodes.append({"node_id": cid, "subtype": "document_chunk", "source": chunk_src,
                          "source_record_id": {"key": f"file{r}"}, "source_revision": {"v": 1},
                          "attrs": attrs})
            position = {"chunk_ordinal": k, "chunk_total": total,
                        "char_offset": k * 60, "char_end": k * 60 + 100}
            edge_base = {"source": chunk_src, "source_record_id": {"key": f"file{r}"},
                         "source_revision": {"v": 1}}
            if shape in ("both", "member_only"):
                edges.append({"edge_id": f"m:{r}:{k:03d}", "src": cid, "dst": rec,
                              "edge_type": "member_of", "attrs": dict(position), **edge_base})
            if shape in ("both", "contains_only"):
                edges.append({"edge_id": f"c:{r}:{k:03d}", "src": rec, "dst": cid,
                              "edge_type": "contains",
                              "attrs": dict(position) if shape == "contains_only" else {},
                              **edge_base})
    return nodes, edges, records


def _read_pages(node_id: str) -> list[dict[str, Any]]:
    from okg.substrate.errors import NodeNotFound
    from okg.substrate.mcp import record_text

    pages: list[dict[str, Any]] = []
    token = None
    for _ in range(200):
        try:
            page = record_text.read_record_text(
                _Conn(), _session(nomos_read_action=None, nomos_read_surface=None),
                node_id, continuation_token=token, max_bytes=6000, reserve_bytes=500,
            )
        except NodeNotFound:
            return [{"hidden": True}]
        token = page.pop("continuation_token", None)
        pages.append(page)
        if token is None:
            return pages
    raise AssertionError("record text did not finish")


def _record_text_bundle(role: str, rng: random.Random, nodes: list[dict[str, Any]]) -> Any:
    """The policy for one corpus. ``class_safe`` names only read-class keys, so
    the read-class path decides it and hides (deny) and redacts members;
    ``blocking`` adds a rule on a key outside them, so the call falls back to
    the per-row filter; ``random`` is a random bundle, as the sweep tests use."""
    if role == "random":
        return _redacting_bundle(rng, "node", nodes)
    rules: list[dict[str, Any]] = [
        {"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"], "subjects": ["*"],
         "match": {"classification": "restricted"}, "priority": 2},
        {"rule_id": "redact_source", "effect": "redact", "actions": ["*"], "subjects": ["*"],
         "match": {"source_id": SOURCES[1]}, "priority": 1,
         "obligations": [{"type": "redaction"}]},
    ]
    if role == "blocking":
        rules.append({"rule_id": "deny_record", "effect": "deny", "actions": ["*"],
                      "subjects": ["*"], "match": {"source_record_id": {"key": "file2"}},
                      "priority": 3})
    return compile_policy_graph({
        "policy_bundle_id": f"nomos:test:record-text-{role}",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": rules,
    })


@pytest.mark.parametrize("shape", ["both", "contains_only", "member_only"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_record_text_pages_equal_through_both_paths(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch, shape: str, mode: str,
) -> None:
    """example-kb links chunks by contains only; okg-workspace and lane B's corpus
    by both. Every page of every record, and a few chunks' parent lists, are
    identical through the read-class path and the per-row filter."""
    from okg.substrate.mcp import record_text

    engine.mode = mode
    monkeypatch.setattr(record_text, "apply_effective_attrs_to_rows", lambda conn, rows, **_: rows)
    monkeypatch.setattr(record_text, "assert_session_matches", lambda conn, session: None)
    monkeypatch.setattr(
        record_text, "continuation_binding",
        lambda conn, session, **kw: {"kind": kw["kind"], "projection": kw["projection"]},
    )
    monkeypatch.setattr(
        record_text, "classify_sources",
        lambda conn, deployment_name, sources: {s: "trusted" for s in sources},
    )
    # The no-text pointer (#3467) reads the real graph through tools' own
    # filtered reads, the same on both paths compared here; this fake
    # connection cannot serve them, so it keeps the plain pointer.
    monkeypatch.setattr(
        record_text, "_text_twin_move",
        lambda conn, session, node_id, **_: record_text.NO_TEXT_NODE_MOVE,
    )

    def per_row_shim(conn: Any, session: Any, rows: Any, *, row_kind: str) -> list[dict]:
        exact = filter_node_rows_for_session if row_kind == "node" else filter_edge_rows_for_session
        return exact(conn, session, rows)

    paths: list[str] = []
    class_hidden = class_redacted = 0
    real_class_filter = read_filter.read_class_filter

    def counting(conn: Any, session: Any, rows: Any, *, row_kind: str) -> list[dict]:
        nonlocal class_hidden, class_redacted
        rows = list(rows)
        got, path = real_class_filter(conn, session, rows, row_kind=row_kind)
        paths.append(path.path)
        if path.path == "read_class":
            class_hidden += len(rows) - len(got)
            class_redacted += sum(isinstance(row.get("attrs"), ReadRedactedAttrs) for row in got)
        return got

    redacted_pages = multi_page = 0
    for role in ("class_safe", "blocking", "random"):
        rng = random.Random(f"record-text:{shape}:{mode}:{role}")
        nodes, edges, records = _record_corpus(rng, shape)
        engine.bundle = _record_text_bundle(role, rng, nodes)
        graph = _RecordGraph(nodes, edges)
        monkeypatch.setattr(record_text, "_checkout", lambda conn, session, g=graph: g)
        chunks = [n["node_id"] for n in nodes if n["subtype"] == "document_chunk"]
        for target in records + rng.sample(chunks, min(4, len(chunks))):
            monkeypatch.setattr(record_text, "filter_rows_by_read_class", per_row_shim)
            want = _read_pages(target)
            monkeypatch.setattr(record_text, "filter_rows_by_read_class", counting)
            got = _read_pages(target)
            assert got == want, (role, target)
            multi_page += len(want) > 1
            redacted_pages += any(
                isinstance(item, dict) and item.get("redacted")
                for page in want for item in page.get("items") or ()
            )
    # Each role must reach what it is there for, or the comparison above proved
    # less than it claims: the read-class path deciding, hiding (enforce only;
    # audit returns denied rows) and redacting members, the per-row fallback,
    # redacted text on a page, and a record read in several pages.
    assert paths.count("read_class") >= 10
    assert class_redacted > 0
    assert class_hidden > 0 if mode == "enforce" else class_hidden == 0
    assert paths.count("per_row") > 0
    assert redacted_pages > 0
    assert multi_page > 0

"""PACT lane-h-nomos-read-class-stored (slice 1): stored read classes.

``read_class_filter`` keys a row by its stored ``nomos_read_class`` when the
class is present and its ``nomos_read_class_version`` is the running
``CLASS_VOCABULARY_VERSION``, and by slice 0's read-time projection otherwise.
Every answer must equal the per-row filter's: rows, order, attrs byte for byte
(redaction included), in audit and enforce.

The harness is slice 0's (``test_read_filter_decision_class``): the real policy
engine and runtime boundary, with the bundle served from memory and decisions
captured. PostgreSQL behavior (columns, writers, Chronos) is in
``test_read_class_storage.py``.
"""
from __future__ import annotations

import json
import math
import random
import re
import uuid
from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos import read_class as rc
from okg.substrate.nomos import read_filter
from okg.substrate.nomos.core import compile_policy_graph
from okg.substrate.nomos.read_class import (
    CLASS_VOCABULARY_DIGEST,
    CLASS_VOCABULARY_VERSION,
    READ_CLASS_COLUMN,
    READ_CLASS_VERSION_COLUMN,
    read_class,
    stamp_row,
    vocabulary_digest,
)
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
    read_class_filter,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    combine_source_lineages,
    normalize_source_lineage,
    source_record_ref,
)

import test_read_filter_decision_class as _decision_class
from test_read_filter_decision_class import SOURCES, _Conn, _Engine, _rows, _session
from test_read_filter_read_class import _canon, _redacting_bundle, _with_payload

engine = _decision_class.engine

_SUBTYPE_PII = {"doc": {"name": "direct_identifier", "body": "free_text"}, "page": {}}
#: A class value stamped under an older vocabulary: shared by every "old" row,
#: so a read that trusted it would group rows with different decisions.
_OLD_CLASS = uuid.UUID(int=7)


@pytest.fixture(autouse=True)
def _subtype_pii(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_filter, "_load_subtype_pii_classes", lambda conn, cv: _SUBTYPE_PII)


def _with_stamp_states(rng: random.Random, kind: str, rows: list[dict]) -> list[dict]:
    """Rows as a database holds them across an upgrade: most stamped, some
    NULL (written before the column or by a writer that does not stamp), some
    from a database without the column (no key at all), and some stamped
    under an older vocabulary with a class that is NOT their content's."""
    out = []
    for row in rows:
        row = dict(row)
        state = rng.random()
        if state < 0.6:
            stamp_row(row, kind)
        elif state < 0.75:
            row[READ_CLASS_COLUMN] = None
            row[READ_CLASS_VERSION_COLUMN] = None
        elif state < 0.85:
            pass
        else:
            row[READ_CLASS_COLUMN] = _OLD_CLASS
            row[READ_CLASS_VERSION_COLUMN] = CLASS_VOCABULARY_VERSION - 1
        out.append(row)
    return out


def _per_row(kind: str, session: Any, rows: list[dict]) -> list[dict]:
    exact = filter_node_rows_for_session if kind == "node" else filter_edge_rows_for_session
    return exact(_Conn(), session, [dict(r) for r in rows])


def _by_class(kind: str, session: Any, rows: list[dict], *, verify: int = 0) -> tuple[list, Any]:
    return read_class_filter(
        _Conn(), session, [dict(r) for r in rows], row_kind=kind, verify_sample=verify,
    )


def _new_stats() -> dict[str, int]:
    return {"class_path": 0, "per_row_path": 0, "stored_rows": 0, "verified": 0,
            "redacted": 0, "hidden": 0, "mismatch": 0}


def _compare(
    stats: dict[str, int], kind: str, session: Any, rows: list[dict], *, verify: int,
) -> bool:
    """One read through both filters under the engine's current bundle; True
    when the answers differ."""
    expected = _canon(_per_row(kind, session, rows))
    got, path = _by_class(kind, session, rows, verify=verify)
    if _canon(got) != expected:
        stats["mismatch"] += 1
        return True
    stats["class_path" if path.path == "read_class" else "per_row_path"] += 1
    stats["stored_rows"] += path.stored_class_rows
    stats["verified"] += path.verified_rows
    stats["redacted"] += any(t == "ReadRedactedAttrs" for t, _ in expected)
    stats["hidden"] += len(expected) < len(rows)
    return False


def _sweep(engine: _Engine, kind: str, mode: str, seeds: int, *, verify: int = 0) -> dict[str, int]:
    """Random rows in every stamp state under random bundles."""
    engine.mode = mode
    session = _session()
    stats = _new_stats()
    for seed in range(seeds):
        rng = random.Random(f"stored-class:{kind}:{mode}:{seed}")
        rows = _with_stamp_states(
            rng, kind, [_with_payload(rng, r) for r in _rows(rng, kind, rng.randint(1, 40))],
        )
        engine.bundle = _redacting_bundle(rng, kind, rows)
        _compare(stats, kind, session, rows, verify=verify)
    return stats


# --------------------------------------------------------------------------- #
# T1: randomized equivalence on stored classes                                 #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_stored_class_rows_equal_the_per_row_rows_for_random_rules_and_rows(
    engine: _Engine, kind: str, mode: str,
) -> None:
    # Fixed roles first: one record's rows in each stamp state under a
    # class-safe deny and redact policy, and once under a rule that sends the
    # read per row. They reach every path whatever the random draws do.
    roles = _role_stats(engine, kind, mode, verify=3)
    assert roles["mismatch"] == 0, roles
    assert roles["class_path"] == len(_STAMP_ROLES) and roles["per_row_path"] == 1, roles
    assert roles["stored_rows"] == 30 and roles["verified"] == 3, roles
    assert roles["redacted"] == len(_STAMP_ROLES) + 1, roles
    if mode == "enforce":
        assert roles["hidden"] == len(_STAMP_ROLES) + 1, roles
    # Then random rules over random rows in every stamp state: every match
    # shape the engine evaluates. 60 seeds (was 300); the fixed roles above
    # carry the mutation detection the larger sweep only reached by chance.
    stats = _sweep(engine, kind, mode, seeds=60, verify=3)

    assert stats["mismatch"] == 0, stats
    assert stats["class_path"] > 12 and stats["per_row_path"] > 6, stats
    # Stored classes decided rows, and the verification sample ran on them.
    assert stats["stored_rows"] > 150 and stats["verified"] > 20, stats
    assert stats["redacted"] > 4 and stats["hidden"] > 4, stats


def _record_rows(kind: str) -> list[dict[str, Any]]:
    """Rows shaped like one record's chunks: a few sources, records, a NaN
    revision and multi-source lineage, each stamped."""
    rows = []
    for i in range(30):
        source = SOURCES[i % 3]
        attrs: dict[str, Any] = {"name": f"Author {i}", "body": "text"}
        revision: Any = {"v": 1 if i % 7 else float("nan")}
        if i % 5 == 0:
            attrs[LINEAGE_ATTR] = combine_source_lineages([
                normalize_source_lineage(source_id=s, source_record_id={"key": "a"},
                                         source_revision={"v": 1})
                for s in SOURCES[:2]
            ])
        row: dict[str, Any] = {
            f"{kind}_id": f"{kind}:{i:02d}", "attrs": attrs, "source": source,
            "source_record_id": {"key": "ab"[i % 2]}, "source_revision": revision,
        }
        if kind == "node":
            row["subtype"] = "doc"
        else:
            row.update(src=f"n:{i}", dst="n:0", edge_type="member_of")
        rows.append(stamp_row(row, kind))
    return rows


def _redact_and_deny_bundle(extra: list[dict[str, Any]] = ()) -> Any:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:stored-class", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            *extra,
            {"rule_id": "deny_jira", "effect": "deny", "actions": ["*"], "subjects": ["*"],
             "match": {"source_id": "jira"}, "priority": 3},
            {"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"], "subjects": ["*"],
             "match": {"classification": "restricted"}, "priority": 2},
            {"rule_id": "redact_docs", "effect": "redact", "actions": ["*"], "subjects": ["*"],
             "match": {"source_ids": "docs"}, "priority": 1,
             "obligations": [{"type": "redaction"}]},
        ],
    })


#: The stamp states a record's rows can be in: all stamped under the running
#: vocabulary, NULL, without the columns, or stamped under an older one.
_STAMP_ROLES = ("stamped", "null", "absent", "old_version")


def _in_stamp_state(rows: list[dict], state: str) -> list[dict]:
    out = []
    for row in rows:
        row = dict(row)
        if state == "null":
            row[READ_CLASS_COLUMN] = row[READ_CLASS_VERSION_COLUMN] = None
        elif state == "absent":
            del row[READ_CLASS_COLUMN], row[READ_CLASS_VERSION_COLUMN]
        elif state == "old_version":
            row[READ_CLASS_COLUMN] = _OLD_CLASS
            row[READ_CLASS_VERSION_COLUMN] = CLASS_VOCABULARY_VERSION - 1
        out.append(row)
    return out


def _role_stats(engine: _Engine, kind: str, mode: str, *, verify: int = 0) -> dict[str, int]:
    """The record's rows in each stamp state under the class-safe deny and
    redact policy (read-class path), then stamped under a rule on a record id
    (per-row path). Each stamp state holds rows that share source and
    classification but get different decisions (a NaN revision is hidden), so
    a read that grouped them by a stamp it should not trust answers wrongly."""
    engine.mode = mode
    session = _session()
    stats = _new_stats()
    engine.bundle = _redact_and_deny_bundle()
    for state in _STAMP_ROLES:
        _compare(stats, kind, session, _in_stamp_state(_record_rows(kind), state), verify=verify)
    engine.bundle = _redact_and_deny_bundle([
        {"rule_id": "deny_record", "effect": "deny", "actions": ["*"], "subjects": ["*"],
         "match": {"source_record_id": {"key": "b"}}, "priority": 9},
    ])
    _compare(stats, kind, session, _record_rows(kind), verify=verify)
    return stats


@pytest.mark.parametrize("kind", ["node", "edge"])
def test_a_stamped_read_is_decided_entirely_by_stored_classes(engine: _Engine, kind: str) -> None:
    engine.bundle = _redact_and_deny_bundle()
    rows = _record_rows(kind)

    got, path = _by_class(kind, _session(), rows, verify=len(rows))

    assert _canon(got) == _canon(_per_row(kind, _session(), rows))
    assert path.path == "read_class"
    assert path.stored_class_rows == len(rows) and path.per_row_rows == 0
    assert path.verified_rows == len(got) > 0
    # Far fewer classes than rows: one per source (the record key is not a
    # class fact), one per source with a NaN revision, one multi-source
    # lineage. One set of decisions per class.
    assert path.classes == 7


# --------------------------------------------------------------------------- #
# T3: fail-closed cases                                                        #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("state", ["null", "absent", "old_version"])
def test_rows_without_a_current_stored_class_use_the_projection(
    engine: _Engine, state: str,
) -> None:
    engine.bundle = _redact_and_deny_bundle()
    rows = _in_stamp_state(_record_rows("node"), state)

    got, path = _by_class("node", _session(), rows, verify=5)

    assert path.path == "read_class" and path.stored_class_rows == 0
    assert path.class_rows > 0
    assert _canon(got) == _canon(_per_row("node", _session(), rows))


def test_a_wrong_stamp_is_refused_by_the_verification_sample(engine: _Engine) -> None:
    engine.bundle = _redact_and_deny_bundle()
    rows = _record_rows("node")
    # A writer bug: a git row with a malformed (NaN) revision, which the
    # per-row filter hides, stamped with a readable git row's class. Its
    # source set matches, so only the verification sample can catch it.
    plain = [r for r in rows if r["source"] == "git" and LINEAGE_ATTR not in r["attrs"]]
    git = next(r for r in plain if not math.isnan(r["source_revision"]["v"]))
    wrong = next(r for r in plain if math.isnan(r["source_revision"]["v"]))
    wrong[READ_CLASS_COLUMN] = git[READ_CLASS_COLUMN]

    with pytest.raises(SubstrateError, match="does not match its content") as exc:
        _by_class("node", _session(), rows, verify=len(rows))
    assert exc.value.data["row_id"] == wrong["node_id"]
    # Without the sample the wrong stamp is not detected: the row is returned.
    got, _ = _by_class("node", _session(), rows, verify=0)
    assert wrong["node_id"] in {r["node_id"] for r in got}


def test_the_verification_sample_is_read_from_the_environment(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine.bundle = _redact_and_deny_bundle()
    rows = _record_rows("node")
    monkeypatch.setenv(read_filter.READ_CLASS_VERIFY_SAMPLE_ENV, "4")
    _, path = read_class_filter(_Conn(), _session(), rows, row_kind="node")
    assert path.verified_rows == 4
    monkeypatch.setenv(read_filter.READ_CLASS_VERIFY_SAMPLE_ENV, "four")
    with pytest.raises(SubstrateError, match="must be an integer"):
        read_class_filter(_Conn(), _session(), rows, row_kind="node")


@pytest.mark.parametrize("match", [
    {"graph_ref": "node:node:03"},
    {"source_record_ref": source_record_ref("git", {"key": "a"})},
    {"fact_no_row_has": "x"},
    {"source_lineage": {}},
])
def test_a_rule_on_a_row_specific_or_unknown_key_sends_stamped_rows_per_row(
    engine: _Engine, match: dict[str, Any],
) -> None:
    engine.bundle = _redact_and_deny_bundle([
        {"rule_id": "row_specific", "effect": "deny", "actions": ["*"], "subjects": ["*"],
         "match": match, "priority": 9},
    ])
    rows = _record_rows("node")

    got, path = _by_class("node", _session(), rows)

    assert path.path == "per_row" and path.reason.startswith("rule_names_key:")
    assert _canon(got) == _canon(_per_row("node", _session(), rows))


@pytest.mark.parametrize("override", [
    {"nomos_identity_claim": {"issuer": "https://idp.example", "subject": "u"}},
    {"nomos_access_context": {"context_id": "ctx", "hidden_refs": []}},
])
def test_identity_and_access_context_sessions_take_the_per_row_path(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch, override: dict[str, Any],
) -> None:
    engine.bundle = _redact_and_deny_bundle()
    seen: list[Any] = []
    monkeypatch.setattr(
        read_filter, "filter_node_rows_for_session",
        lambda conn, session, rows, **kw: seen.append(session) or [],
    )

    _, path = _by_class("node", _session(**override), _record_rows("node"))

    assert path.path == "per_row" and seen


def test_a_classification_overlay_splits_a_stored_class(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classification is not in the stored class: an accepted overlay changes
    it after the write, and the read keys on the effective value."""
    engine.bundle = _redact_and_deny_bundle()
    rows = [r for r in _record_rows("node") if r["source"] == "git"]

    def overlay(conn: Any, rows: list[dict], **_: Any) -> list[dict]:
        out = []
        for row in rows:
            row = dict(row)
            if row["node_id"] in {"node:03", "node:09"}:
                row["attrs"] = {**row["attrs"], "classification": "restricted"}
            out.append(row)
        return out

    monkeypatch.setattr(read_filter, "apply_effective_attrs_to_rows", overlay)
    got, path = _by_class("node", _session(), rows)

    ids = {r["node_id"] for r in got}
    assert path.stored_class_rows == len(rows)
    assert "node:03" not in ids and "node:09" not in ids
    assert ids == {r["node_id"] for r in filter_node_rows_for_session(_Conn(), _session(), rows)}


# --------------------------------------------------------------------------- #
# The class itself                                                             #
# --------------------------------------------------------------------------- #
def _node(**kw: Any) -> dict[str, Any]:
    row = {"node_id": "n:1", "attrs": {}, "source": "git",
           "source_record_id": {"key": "a"}, "source_revision": {"v": 1}}
    row.update(kw)
    return row


def test_rows_differing_only_in_id_or_payload_share_a_class() -> None:
    assert read_class(_node(), "node") == read_class(
        _node(node_id="n:2", attrs={"body": "other"}), "node"
    )


@pytest.mark.parametrize("change", [
    {"source": "jira"},
    {"source_revision": {"v": float("nan")}},
    {"node_id": None},
    {"source_record_id": None},
])
def test_a_fact_a_decision_reads_changes_the_class(change: dict[str, Any]) -> None:
    assert read_class(_node(**change), "node") != read_class(_node(), "node")


def test_classification_is_not_in_the_class() -> None:
    assert read_class(_node(attrs={"classification": "restricted"}), "node") == read_class(
        _node(), "node"
    )


def test_a_row_without_lineage_has_no_class_and_stamps_null() -> None:
    row = stamp_row(_node(source=None, source_record_id=None), "node")
    assert row[READ_CLASS_COLUMN] is None and row[READ_CLASS_VERSION_COLUMN] is None


def test_the_class_is_computed_on_the_stored_json_form() -> None:
    """A writer's tuple or int key reads back as a list or a string key."""
    lineage = normalize_source_lineage(
        source_id="git", source_record_id={"key": ("a", "b")}, source_revision={1: "x"},
    )
    written = _node(attrs={LINEAGE_ATTR: lineage})
    read_back = json.loads(json.dumps(written, default=str))
    assert read_class(written, "node") == read_class(read_back, "node")
    assert isinstance(read_class(written, "node"), uuid.UUID)


def test_the_stored_class_round_trips_through_chronos_json_load_paths() -> None:
    """The splice paths serialize with json.dumps(default=str); a UUID becomes
    its text, which PostgreSQL's uuid input reads back as the same value."""
    value = read_class(_node(), "node")
    text = json.loads(json.dumps({"c": value}, default=str))["c"]
    assert uuid.UUID(text) == value


# --------------------------------------------------------------------------- #
# Classes computed ahead of the write (issue #2941)                            #
# --------------------------------------------------------------------------- #
def _stamped(rows: list[dict[str, Any]], kind: str, **kw: Any) -> tuple[list, dict]:
    copies = [dict(row) for row in rows]
    classes = rc.stamp_rows(copies, kind, **kw)
    stamps = [(r[READ_CLASS_COLUMN], r[READ_CLASS_VERSION_COLUMN]) for r in copies]
    return stamps, {k: rc.canonical_facts_text(v) for k, v in classes.items()}


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("seed", range(8))
def test_classes_computed_ahead_stamp_what_the_write_computes(kind: str, seed: int) -> None:
    rows = _decision_class._rows(random.Random(seed), kind, 30) + [
        row for row_kind, row in _pinned_rows().values() if row_kind == kind
    ]
    known = rc.known_read_classes(rows, kind)
    assert known
    assert _stamped(rows, kind, known=known) == _stamped(rows, kind)


def test_a_known_class_is_reused_without_recomputing(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [_node(node_id=f"n:{i}", source_record_id={"key": str(i)}) for i in range(5)]
    known = rc.known_read_classes(rows, "node")
    computed: list[Any] = []
    real = rc._facts_of_stored
    monkeypatch.setattr(
        rc, "_facts_of_stored", lambda stored, kind: computed.append(stored) or real(stored, kind),
    )
    rc.stamp_rows([dict(row) for row in rows], "node", known=known)
    assert computed == []


@pytest.mark.parametrize("change", [
    {"source": "jira"},
    {"node_id": None},
    {"source_record_id": None},
    {"attrs": {LINEAGE_ATTR: normalize_source_lineage(source_id="jira", source_record_id={"k": 1})}},
])
def test_a_row_changed_after_its_class_was_computed_ahead_is_recomputed(
    change: dict[str, Any],
) -> None:
    """The write half may rewrite a prepared row (a declarer handoff, a
    lineage stamp): its class is its final content's, never the prepared one."""
    row = _node()
    known = rc.known_read_classes([row], "node")
    row.update(change)
    stamped = rc.stamp_rows([row], "node", known=known)
    assert row[READ_CLASS_COLUMN] == read_class(row, "node")
    assert row[READ_CLASS_COLUMN] != read_class(_node(), "node")
    assert set(stamped) == ({row[READ_CLASS_COLUMN]} - {None})


def _edge(**kw: Any) -> dict[str, Any]:
    row = {"edge_id": 42, "attrs": {}, "source": "git",
           "source_record_id": {"key": "a"}, "source_revision": {"v": 1}}
    row.update(kw)
    return row


@pytest.mark.parametrize("kind,row", [
    ("node", _node(source_revision={"v": float("nan")})),
    ("edge", _edge(edge_id=object())),
    # JSON writes {1: "a"} and {"1": "a"} the same; their classes differ.
    ("edge", _edge(edge_id={1: "a"})),
    ("edge", _edge(edge_id={"1": "a"})),
])
def test_rows_without_an_exact_key_are_computed_at_the_write(
    kind: str, row: dict[str, Any],
) -> None:
    """No key, never shared: computed at the write even when other rows of
    the batch have known classes."""
    assert rc._stored_and_key(row, kind)[1] is None
    keyable = _node() if kind == "node" else _edge()
    known = rc.known_read_classes([keyable, row], kind)
    assert len(known) == 1
    assert _stamped([keyable, row], kind, known=known) == _stamped([keyable, row], kind)


def test_a_row_whose_stored_view_fails_stamps_null_with_known_classes() -> None:
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    row = _node(source_record_id=cyclic)
    known = rc.known_read_classes([_node(), row], "node")
    stamps, _ = _stamped([_node(), row], "node", known=known)
    assert stamps == _stamped([_node(), row], "node")[0]
    assert stamps[1] == (None, None)


def test_rows_of_one_class_share_one_facts_object() -> None:
    rows = [_node(node_id=f"n:{i}", source_record_id={"key": str(i)}) for i in range(20)]
    known = rc.known_read_classes(rows, "node")
    assert len(known) == 20
    assert len({id(facts) for _, facts in known.values()}) == 1


# --------------------------------------------------------------------------- #
# T5: vocabulary pin                                                           #
# --------------------------------------------------------------------------- #
def test_the_vocabulary_digest_is_pinned_to_the_running_version() -> None:
    assert vocabulary_digest() == CLASS_VOCABULARY_DIGEST, (
        "the read filter's resource keys or the class facts changed: bump "
        "CLASS_VOCABULARY_VERSION and record the new CLASS_VOCABULARY_DIGEST"
    )


def test_a_resource_builder_change_without_a_version_bump_fails_the_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = read_filter._row_resource

    def with_new_fact(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {**real(*args, **kwargs), "producer_kind": "agent"}

    monkeypatch.setattr(read_filter, "_row_resource", with_new_fact)
    assert vocabulary_digest() != CLASS_VOCABULARY_DIGEST


def _pinned_rows() -> dict[str, tuple[str, dict[str, Any]]]:
    """One row per lineage shape the class facts derive from: plain columns,
    a lineage attr, a combined (derived) lineage, a fine-grained source
    record, a JSON-awkward revision, a numeric edge id and no lineage."""
    combined = combine_source_lineages([
        normalize_source_lineage(source_id="git", source_record_id={"key": "a"},
                                 source_revision={"v": 1}),
        normalize_source_lineage(source_id="jira", source_record_id={"issue": 7},
                                 source_revision={"v": 2}),
    ])
    return {
        "plain_node": ("node", _node()),
        "lineage_attr_node": ("node", _node(attrs={LINEAGE_ATTR: normalize_source_lineage(
            source_id="git", source_record_id={"key": ("a", "b")}, source_revision={1: "x"},
        )})),
        "combined_node": ("node", _node(source=None, source_record_id=None,
                                        source_revision=None,
                                        attrs={LINEAGE_ATTR: combined})),
        "nan_revision_node": ("node", _node(source_revision={"v": float("nan")})),
        "plain_edge": ("edge", {"edge_id": "42", "attrs": {}, "source": "git",
                                "source_record_id": {"key": "a"},
                                "source_revision": {"v": 1}}),
        "combined_edge": ("edge", {"edge_id": 43, "attrs": {LINEAGE_ATTR: combined},
                                   "source": None, "source_record_id": None,
                                   "source_revision": None}),
        "no_lineage_node": ("node", _node(source=None, source_record_id=None)),
    }


#: The class each pinned row had when CLASS_VOCABULARY_VERSION 1 was set.
#: The two plain/lineage-attr nodes share one: record ids and revisions are
#: not class facts (a rule on them sends the read per row).
_PINNED_CLASSES = {
    "plain_node": "7f54fd7e-3873-3f95-2ce6-27f43d88c1c3",
    "lineage_attr_node": "7f54fd7e-3873-3f95-2ce6-27f43d88c1c3",
    "combined_node": "541a41c8-36cb-9c1d-f39c-5646f104a72b",
    "nan_revision_node": "2015502f-0e77-523a-394e-732c671297f8",
    "plain_edge": "5f500051-bb39-42c9-02e7-3c25b8159d23",
    "combined_edge": "0825520b-cf91-4b0f-2844-6854cee3a18a",
    "no_lineage_node": None,
}


def test_the_class_of_pinned_sample_rows_is_unchanged() -> None:
    """The digest pins key NAMES; this pins BEHAVIOUR. A change to how the
    facts are derived (lineage contributors, fine-grained targets, the JSON
    flag, fact text) moves a stamped row to a different class while old
    stamps keep the old one, so it must bump CLASS_VOCABULARY_VERSION."""
    got = {
        name: (None if (value := read_class(row, kind)) is None else str(value))
        for name, (kind, row) in _pinned_rows().items()
    }
    assert CLASS_VOCABULARY_VERSION == 1, "re-pin _PINNED_CLASSES for the new version"
    assert got == _PINNED_CLASSES, (
        "a pinned row's read class changed: bump CLASS_VOCABULARY_VERSION, "
        "record the new CLASS_VOCABULARY_DIGEST and re-pin these classes"
    )


def test_a_fine_grained_target_change_without_a_version_bump_fails_the_pin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The review's case: lineage starts emitting one more source-record
    target. Key names are unchanged, so only the behaviour pin sees it."""
    real = read_filter._fine_grained_targets

    def with_extra_target(base: Any, **kwargs: Any) -> Any:
        yield from real(base, **kwargs)
        yield {**base, "resource_kind": "source_record", "source_id": "extra"}, "extra", False

    monkeypatch.setattr(read_filter, "_fine_grained_targets", with_extra_target)
    assert vocabulary_digest() == CLASS_VOCABULARY_DIGEST
    changed = {
        name for name, (kind, row) in _pinned_rows().items()
        if _PINNED_CLASSES[name] is not None
        and str(read_class(row, kind)) != _PINNED_CLASSES[name]
    }
    assert changed == {name for name, value in _PINNED_CLASSES.items() if value}


# --------------------------------------------------------------------------- #
# Mutations: each must make a test above fail                                  #
# --------------------------------------------------------------------------- #
def _cases_with_wrong_answers(engine: _Engine) -> int:
    """How many of the four kind and mode cases answer wrongly on the
    fixed-role corpora. Each stamp state's corpus holds rows that share source
    and classification but get different decisions, so a read that keys on a
    stamp it should not trust, or a class that drops a fact, answers wrongly
    in every case, audit included."""
    return sum(
        _role_stats(engine, kind, mode)["mismatch"] > 0
        for kind in ("node", "edge") for mode in ("enforce", "audit")
    )


def test_mutation_m2_dropping_the_sources_from_the_class_is_caught(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = rc.class_facts

    def without_sources(row: Any, kind: str) -> Any:
        facts = real(row, kind)
        if facts is None:
            return None
        facts.pop("source_ids")
        facts["by_source"] = [len(facts["by_source"])]
        facts["fine_grained"] = sorted(
            re.sub(r"git|jira|docs", "?", text) for text in facts["fine_grained"]
        )
        return facts

    monkeypatch.setattr(rc, "class_facts", without_sources)
    with pytest.raises(AssertionError):
        test_a_fact_a_decision_reads_changes_the_class({"source": "jira"})


def test_mutation_m3_dropping_the_json_shape_flag_is_caught(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rc, "_is_json_compatible", lambda value: True)
    with pytest.raises(AssertionError):
        test_a_fact_a_decision_reads_changes_the_class({"source_revision": {"v": math.nan}})
    # The stamped NaN row now shares its class with readable rows: the read
    # returns a row the per-row filter hides.
    assert _cases_with_wrong_answers(engine) == 4


def test_mutation_m4_treating_null_as_a_class_is_caught(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = read_filter.stored_read_class
    null_class = uuid.UUID(int=0)

    def null_is_a_class(row: Any) -> Any:
        if READ_CLASS_COLUMN in row and row[READ_CLASS_COLUMN] is None:
            return null_class
        return real(row)

    monkeypatch.setattr(read_filter, "stored_read_class", null_is_a_class)
    assert _cases_with_wrong_answers(engine) == 4


def test_mutation_m6_classification_in_the_stored_class_is_caught(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mutant stores the write-time classification in the class and keys
    on the class alone; an overlay that reclassifies after the write is then
    missed."""
    real_facts = rc.class_facts
    real_key = read_filter._stored_class_key_and_attrs

    def with_classification(row: Any, kind: str) -> Any:
        facts = real_facts(row, kind)
        if facts is not None:
            facts["classification"] = str((row.get("attrs") or {}).get("classification") or "")
        return facts

    def key_without_effective(row: Any, stored: Any) -> Any:
        key, attrs = real_key(row, stored)
        return (None if key is None else key[:3]), attrs

    monkeypatch.setattr(rc, "class_facts", with_classification)
    monkeypatch.setattr(read_filter, "_stored_class_key_and_attrs", key_without_effective)
    with pytest.raises(AssertionError):
        test_a_classification_overlay_splits_a_stored_class(engine, monkeypatch)


def test_mutation_m7_ignoring_the_class_version_is_caught(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def any_version(row: Any) -> Any:
        value = row.get(READ_CLASS_COLUMN)
        return None if value is None else uuid.UUID(str(value))

    monkeypatch.setattr(read_filter, "stored_read_class", any_version)
    assert _cases_with_wrong_answers(engine) == 4

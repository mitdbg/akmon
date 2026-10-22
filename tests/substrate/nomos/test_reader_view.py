"""PACT lane-h-nomos-read-class-sql (slice 2): the reader's view, in memory.

The harness is slice 0's (``test_read_filter_decision_class``): the real policy
engine and runtime boundary, the bundle served from memory and every decision
captured. The catalog is built from the rows' own stamps. The SQL predicate is
evaluated on PostgreSQL in ``test_reader_view_postgres.py``; this file covers
what needs no database:

- T3b: a class decided from its catalog facts decides like the row it came
  from, for every effect and classification probe, audit and enforce;
- every mode and reason, the fingerprint, ``masked_attrs`` against
  ``mask_attrs``, the scan-site recording, and the mutations that need no SQL.
"""
from __future__ import annotations

import dataclasses
import random
import uuid
from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos import read_class as rc
from okg.substrate.nomos import read_filter, reader_view
from okg.substrate.nomos.core import compile_policy_graph
from okg.substrate.nomos.read_class import stamp_rows
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.read_masking import (
    READ_DROP_CLASSES,
    ReadRedactedAttrs,
    mask_attrs,
    subtype_attr_classifier,
)
from okg.substrate.nomos.reader_view import (
    BLANK_LABEL,
    ClassOutcome,
    classification_label,
    named_classifications,
)
from okg.substrate.operator_read import (
    LOCAL_OPERATOR_READ_ACTION,
    LOCAL_OPERATOR_READ_SURFACE,
)

import test_read_filter_decision_class as _decision_class
from test_read_filter_decision_class import SOURCES, _Conn, _Engine, _rows, _session
from test_read_filter_read_class import _canon, _redacting_bundle, _with_payload

engine = _decision_class.engine

_SUBTYPE_PII = {
    "doc": {"name": "direct_identifier", "body": "free_text", "title": "non_pii"},
    "page": {},
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Database lookups the view makes, answered from memory. ``catalog`` is
    filled per test from stamped rows."""
    state: dict[str, Any] = {"catalog": {"node": {}, "edge": {}}, "overlay": False,
                             "columns": True, "catalog_present": True, "bundle_loads": 0}
    monkeypatch.setattr(read_filter, "_load_subtype_pii_classes", lambda conn, cv: _SUBTYPE_PII)
    monkeypatch.setattr(
        reader_view, "_subtype_pii_classes", lambda conn, cv: _SUBTYPE_PII, raising=False,
    )
    monkeypatch.setattr(reader_view, "_catalog_present", lambda conn: state["catalog_present"])
    monkeypatch.setattr(reader_view, "_columns_present", lambda conn, kind: state["columns"])
    monkeypatch.setattr(reader_view, "_load_catalog", lambda conn: state["catalog"])
    monkeypatch.setattr(reader_view, "_overlay_present",
                        lambda conn, session, kind: state["overlay"])
    real_load = read_filter._load_read_bundle

    def counting_load(conn: Any, session: Any, *, scope: str) -> Any:
        state["bundle_loads"] += 1
        return real_load(conn, session, scope=scope)

    monkeypatch.setattr(read_filter, "_load_read_bundle", counting_load)
    reader_view.clear_reader_view_cache()
    return state


def _catalog_of(kind: str, rows: list[dict[str, Any]]) -> dict[uuid.UUID, dict[str, Any]]:
    stamped = [dict(r) for r in rows]
    return stamp_rows(stamped, kind)


def _per_row(kind: str, session: Any, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exact = filter_node_rows_for_session if kind == "node" else filter_edge_rows_for_session
    return exact(_Conn(), session, [dict(r) for r in rows])


def _effective_classification(row: dict[str, Any]) -> str:
    return str((row.get("attrs") or {}).get("classification") or "internal")


def _probe_label(classification: str, view: reader_view.ReaderView) -> str | None:
    """The probe that stands for this classification: the SQL label, or the
    blank probe for a blank value no rule names."""
    label = classification_label(classification, view._named_labels)
    if label is None and not classification.strip():
        return BLANK_LABEL
    return label


# --------------------------------------------------------------------------- #
# T3b: catalog decisions equal representative-row decisions                    #
# --------------------------------------------------------------------------- #
def _t3b_sweep(engine: _Engine, state: dict, kind: str, mode: str, seeds: int) -> dict[str, int]:
    engine.mode = mode
    session = _session()
    stats = {"compared": 0, "hidden": 0, "redacted": 0, "visible": 0, "blank": 0, "other": 0,
             "class_mode": 0, "mismatch": 0, "skipped_unusual": 0}
    for seed in range(seeds):
        rng = random.Random(f"t3b:{kind}:{mode}:{seed}")
        rows = [_with_payload(rng, r) for r in _rows(rng, kind, rng.randint(1, 30))]
        engine.bundle = _redacting_bundle(rng, kind, rows)
        state["catalog"] = {"node": {}, "edge": {}, kind: _catalog_of(kind, rows)}
        view = reader_view.open(_Conn(), session, row_kind=kind, action=None, surface=None)
        if view.mode != "class":
            continue
        stats["class_mode"] += 1
        for row in rows:
            class_id = rc.read_class(row, kind)
            if class_id is None:
                continue
            label = _probe_label(_effective_classification(row), view)
            if label is None:
                stats["skipped_unusual"] += 1
                continue
            out = _per_row(kind, session, [row])
            per_row = ClassOutcome(
                visible=bool(out),
                redacted=bool(out) and isinstance(out[0]["attrs"], ReadRedactedAttrs),
            )
            got = view._state.decisions[class_id][label]
            stats["compared"] += 1
            stats["blank"] += label == BLANK_LABEL
            stats["other"] += label == reader_view.OTHER_LABEL
            stats["hidden" if not per_row.visible else
                  "redacted" if per_row.redacted else "visible"] += 1
            if got != per_row:
                stats["mismatch"] += 1
    return stats


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_t3b_a_class_decided_from_its_facts_decides_like_its_row(
    engine: _Engine, _offline: dict, kind: str, mode: str,
) -> None:
    stats = _t3b_sweep(engine, _offline, kind, mode, seeds=200)

    assert stats["mismatch"] == 0, stats
    assert stats["class_mode"] > 60 and stats["compared"] > 500, stats
    assert stats["redacted"] > 50 and stats["visible"] > 50, stats
    if mode == "enforce":
        assert stats["hidden"] > 50, stats
    # The blank and the unnamed probes were each compared on real rows.
    assert stats["blank"] > 20 and stats["other"] > 20, stats


def test_t3b_mutation_dropping_the_json_shape_flag_is_caught(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resource rebuilt from facts that ignores a target's JSON-shape verdict
    decides a malformed row as if it were well formed."""
    real = reader_view.class_resources

    def no_json_flag(facts: Any, **kw: Any) -> Any:
        by_source, fine = real(facts, **kw)
        strip = lambda r: {k: v for k, v in r.items() if k != reader_view._NOT_JSON_KEY}  # noqa: E731
        return [strip(r) for r in by_source], [(strip(r), fc) for r, fc in fine]

    monkeypatch.setattr(reader_view, "class_resources", no_json_flag)
    stats = _t3b_sweep(engine, _offline, "node", "enforce", seeds=200)
    assert stats["mismatch"] > 0, stats


def test_t3b_mutation_deciding_only_the_named_classifications_is_caught(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the blank probe a blank classification (an invalid identity fact,
    denied) would be read off another probe's answer."""
    real = reader_view._probes
    monkeypatch.setattr(
        reader_view, "_probes",
        lambda named: tuple(p if p.label != BLANK_LABEL else
                            reader_view._Probe(BLANK_LABEL, "internal") for p in real(named)),
    )
    stats = _t3b_sweep(engine, _offline, "node", "enforce", seeds=200)
    assert stats["mismatch"] > 0, stats


# --------------------------------------------------------------------------- #
# Modes and reasons                                                            #
# --------------------------------------------------------------------------- #
def _plain_rows(kind: str, n: int = 6) -> list[dict[str, Any]]:
    rows = []
    for i in range(n):
        row: dict[str, Any] = {
            f"{kind}_id": f"{kind}:{i}", "source": SOURCES[i % 3],
            "source_record_id": {"key": "ab"[i % 2]}, "source_revision": {"v": 1},
            "attrs": {"classification": ("internal", "restricted")[i % 2],
                      "name": f"P{i}", "body": "b", "title": "t"},
        }
        if kind == "node":
            row["subtype"] = "doc"
        else:
            row.update(src="n:1", dst="n:2", edge_type="links")
        rows.append(row)
    return rows


def _bundle(rules: list[dict[str, Any]], *, default: str = "allow", version: str = "v1") -> Any:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:reader-view", "policy_version": version,
        "default_effect": default,
        "rules": [{"actions": ["*"], "subjects": ["*"], "priority": i, **rule}
                  for i, rule in enumerate(rules)],
    })


_RULES = [
    {"rule_id": "deny_jira", "effect": "deny", "match": {"source_id": "jira"}},
    {"rule_id": "deny_restricted", "effect": "deny", "match": {"classification": "restricted"}},
    {"rule_id": "redact_docs", "effect": "redact", "match": {"source_ids": "docs"},
     "obligations": [{"type": "redaction"}]},
]


def _open(kind: str = "node", session: Any = None, **kw: Any) -> reader_view.ReaderView:
    return reader_view.open(_Conn(), session or _session(), row_kind=kind,
                            action=None, surface=None, **kw)


@pytest.mark.parametrize("kind", ["node", "edge"])
def test_class_mode_fields(engine: _Engine, _offline: dict, kind: str) -> None:
    rows = _plain_rows(kind)
    engine.bundle = _bundle(_RULES)
    _offline["catalog"][kind] = _catalog_of(kind, rows)

    view = _open(kind)

    assert (view.mode, view.reason) == ("class", "read_class_catalog")
    assert view.class_version == rc.CLASS_VOCABULARY_VERSION
    assert view.row_kind == kind and view.force is None
    by_source = {
        facts["source_ids"][0]: class_id for class_id, facts in _offline["catalog"][kind].items()
    }
    # jira is denied for every classification; git and docs are visible for
    # some classification (internal), so their classes are in the set.
    assert view.visible_classes == {by_source["git"], by_source["docs"]}
    assert all(isinstance(c, uuid.UUID) for c in view.visible_classes)
    internal = reader_view._md5("internal")
    restricted = reader_view._md5("restricted")
    decisions = view._state.decisions
    assert decisions[by_source["git"]][internal] == ClassOutcome(True, False)
    assert decisions[by_source["git"]][restricted] == ClassOutcome(False, False)
    assert decisions[by_source["docs"]][internal] == ClassOutcome(True, True)
    assert not any(o.visible for o in decisions[by_source["jira"]].values())
    assert "AND" in view.predicate("n").as_string(None)
    assert view.pseudonymized("doc") == frozenset()
    # finish is the per-row filter.
    assert _canon(view.finish(rows)) == _canon(_per_row(kind, _session(), rows))


def test_off_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    rows = _plain_rows("node")
    view = _open("node")
    assert (view.mode, view.reason) == ("off", "nomos_off")
    assert view.predicate("n").as_string(None) == "TRUE"
    assert view.readable("n", "body").as_string(None) == "TRUE"
    assert view.finish(rows) == rows
    assert view.sql_exact is True
    assert view.visible_classes == frozenset() and view.class_version is None
    assert not view.masked_attrs("doc").masked("body")


@pytest.mark.parametrize(
    ("session_overrides", "force", "reason"),
    [
        ({"nomos_identity_claim": {"issuer": "x"}}, None, "identity_claim_session"),
        ({"nomos_access_context": {"context_id": "c1"}}, None, "access_context_session"),
        ({}, "per_row", "forced_per_row"),
    ],
)
def test_per_row_reasons_from_the_session_and_the_caller(
    engine: _Engine, _offline: dict, session_overrides: dict, force: Any, reason: str,
) -> None:
    rows = _plain_rows("node")
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", rows)
    session = _session(**session_overrides)

    view = _open("node", session, force=force)

    assert (view.mode, view.reason) == ("per_row", reason)
    assert view.predicate("n").as_string(None) == "TRUE"
    assert view.visible_classes == frozenset()
    assert view.sql_exact is False
    assert view.exactness()["reason"] == f"per_row:{reason}"
    # No class was decided: nothing at the scan site.
    assert engine.decisions == []


@pytest.mark.parametrize(
    ("setup", "reason"),
    [
        ("row_rule", "rule_names_key:graph_ref"),
        ("no_catalog", "catalog_absent"),
        ("no_columns", "read_class_columns_absent"),
    ],
)
def test_per_row_reasons_from_the_policy_and_the_database(
    engine: _Engine, _offline: dict, setup: str, reason: str,
) -> None:
    rows = _plain_rows("node")
    _offline["catalog"]["node"] = _catalog_of("node", rows)
    engine.bundle = _bundle(_RULES)
    if setup == "row_rule":
        engine.bundle = _bundle([
            {"rule_id": "deny_one", "effect": "deny", "match": {"graph_ref": "graph:node:node:1"}},
            *_RULES,
        ])
    elif setup == "no_catalog":
        _offline["catalog_present"] = False
    elif setup == "no_columns":
        _offline["columns"] = False

    view = _open("node")

    assert (view.mode, view.reason) == ("per_row", reason)
    assert view.predicate("n").as_string(None) == "TRUE"
    # The served rows are still exactly the per-row filter's.
    assert _canon(view.finish(rows)) == _canon(_per_row("node", _session(), rows))


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_no_active_bundle_is_its_own_mode(
    engine: _Engine, _offline: dict, mode: str, kind: str,
) -> None:
    """Read-model slice 7a: with no active bundle the view takes the
    no-bundle default decision (unrecorded): audit serves every row with
    lineage unredacted (predicate TRUE, every key readable, nothing
    redacted), enforce serves nothing (predicate FALSE). The served rows are
    still exactly the per-row filter's, which redacts none of them; forcing
    per_row keeps the per-row path."""
    rows = _plain_rows(kind)
    engine.bundle = None
    engine.mode = mode
    view = _open(kind)
    assert (view.mode, view.reason) == ("no_bundle", "no_active_bundle")
    assert view.serves_unredacted is (mode == "audit")
    assert view.predicate("n").as_string(None) == ("TRUE" if mode == "audit" else "FALSE")
    # Audit: every key is readable and nothing is redacted, as with Nomos
    # off. Enforce: undecided (the FALSE predicate already drops every row).
    assert view.readable("n", "assignee").as_string(None) == (
        "TRUE" if mode == "audit" else "CAST(NULL AS boolean)")
    assert view.redacts("n").as_string(None) == (
        "FALSE" if mode == "audit" else "CAST(NULL AS boolean)")
    assert view.exactness() == {"sql_exact": False, "reason": "per_row:no_active_bundle"}
    assert engine.decisions == []  # open records nothing
    served = view.finish(rows)
    assert _canon(served) == _canon(_per_row(kind, _session(), rows))
    if mode == "audit":
        assert served and not any(isinstance(r["attrs"], ReadRedactedAttrs) for r in served)
    else:
        assert served == []
    forced = _open(kind, force="per_row")
    assert (forced.mode, forced.reason) == ("per_row", "forced_per_row")


def test_force_and_kind_are_validated() -> None:
    with pytest.raises(ValueError):
        _open("node", force="sql")
    with pytest.raises(ValueError):
        _open("chunk")
    with pytest.raises(ValueError):
        reader_view.open_many(_Conn(), _session(), action=None, surface=None, kinds=("x",))


def test_a_multi_user_view_with_no_subject_is_refused_even_with_nomos_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    with pytest.raises(SubstrateError):
        _open("node", _session(nomos_subject=None, nomos_multi_user=True))


def test_a_class_decided_under_another_policy_refuses_the_view(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _plain_rows("node")
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", rows)
    other = _bundle(_RULES, version="v2")
    real = reader_view._evaluate_unrecorded

    def stale(conn: Any, **kw: Any) -> Any:
        return dataclasses.replace(real(conn, **kw), policy_fingerprint=other.policy_fingerprint)

    monkeypatch.setattr(reader_view, "_evaluate_unrecorded", stale)
    with pytest.raises(SubstrateError, match="different policy"):
        _open("node")


# --------------------------------------------------------------------------- #
# open_many, fingerprint                                                       #
# --------------------------------------------------------------------------- #
def test_open_many_decides_nodes_and_edges_in_one_pass_with_one_fingerprint(
    engine: _Engine, _offline: dict,
) -> None:
    engine.bundle = _bundle(_RULES)
    _offline["catalog"] = {"node": _catalog_of("node", _plain_rows("node")),
                           "edge": _catalog_of("edge", _plain_rows("edge"))}

    views = reader_view.open_many(_Conn(), _session(), action=None, surface=None)

    assert set(views) == {"node", "edge"} and len(views) == 2
    assert _offline["bundle_loads"] == 1
    assert views["node"].mode == views["edge"].mode == "class"
    assert views["node"].fingerprint == views["edge"].fingerprint == views.fingerprint
    assert views["node"]._state.decisions and views["edge"]._state.decisions
    assert engine.decisions == []  # decided, not recorded


def test_the_fingerprint_is_stable_and_changes_with_bundle_version_and_generation(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _plain_rows("node")
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", rows)

    first = _open("node").fingerprint
    assert _open("node").fingerprint == first

    engine.bundle = _bundle(_RULES, version="v2")
    assert _open("node").fingerprint != first
    engine.bundle = _bundle(_RULES)
    assert _open("node").fingerprint == first

    assert _open("node", _session(generation_id="gen:other")).fingerprint != first
    assert _open("node", _session(branch_name="fork")).fingerprint != first
    assert _open("node", force="per_row").fingerprint != first

    monkeypatch.setattr(rc, "CLASS_VOCABULARY_VERSION", rc.CLASS_VOCABULARY_VERSION + 1)
    assert _open("node").fingerprint != first


def test_the_fingerprint_serializes_classes_as_sorted_lowercase_uuid_strings(
    engine: _Engine, _offline: dict,
) -> None:
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", _plain_rows("node"))
    view = _open("node")
    listed = view._fingerprint_inputs["kinds"]["node"]["visible_classes"]
    assert listed == sorted(str(c) for c in view.visible_classes)
    assert all(text == text.lower() and str(uuid.UUID(text)) == text for text in listed)
    assert view.fingerprint == reader_view.fingerprint_of(view._fingerprint_inputs)


# --------------------------------------------------------------------------- #
# Masks                                                                        #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("subtype", ["doc", "page", "ticket", ""])
def test_masked_attrs_predicts_mask_attrs_key_by_key(
    engine: _Engine, _offline: dict, subtype: str,
) -> None:
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", _plain_rows("node"))
    view = _open("node")
    mask = view.masked_attrs(subtype)
    keys = ["name", "body", "title", "text", "searchable_text", "classification",
            "okg_source_lineage", "email", "path", "raw"]
    from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR

    keys.append(PRODUCER_TEXT_ATTR)
    attrs = {key: f"v-{key}" for key in keys}
    out = mask_attrs(attrs, classifier=subtype_attr_classifier(_SUBTYPE_PII.get(subtype)),
                     drop_classes=READ_DROP_CLASSES)
    for key in keys:
        action = mask.action(key)
        if action == "drop":
            assert key not in out, key
        elif action == "mask":
            assert out[key] == "[nomos:masked]", key
        else:
            assert out[key] == attrs[key], key
    assert mask.declared is (subtype in _SUBTYPE_PII)
    # A declared identifier is masked on reads (no pseudonym service).
    if subtype == "doc":
        assert mask.action("name") == "mask"
        assert view.pseudonymized("doc") == frozenset()


def test_the_named_classifications_and_labels() -> None:
    bundle = _bundle([
        {"rule_id": "a", "effect": "deny", "match": {"classification": "restricted"}},
        {"rule_id": "b", "effect": "deny", "match": {"classification": ["pii", 5, "a b"]}},
    ])
    named = named_classifications(bundle)
    assert named == {"internal", "restricted", "pii", "a b"}
    labels = frozenset(reader_view._md5(n) for n in named)
    assert classification_label("restricted", labels) == reader_view._md5("restricted")
    assert classification_label("a b", labels) == reader_view._md5("a b")
    assert classification_label("public", labels) == "other"
    assert classification_label("x y", labels) is None
    assert classification_label("  ", labels) is None
    assert classification_label("p:q", labels) is None


# --------------------------------------------------------------------------- #
# Recording: today's per-row read decisions only (Jason, 2026-10-02)           #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_open_records_nothing_and_finish_records_per_row_reads_for_served_rows(
    engine: _Engine, _offline: dict, mode: str,
) -> None:
    engine.mode = mode
    rows = _plain_rows("node")
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", rows)

    view = _open("node")
    assert view.mode == "class" and view.visible_classes
    # Every class was decided, and nothing was recorded.
    assert engine.decisions == []

    page = rows[:2]
    served = view.finish(page)
    expected_trail: list = []
    engine_trail = list(engine.decisions)
    engine.decisions.clear()
    _per_row("node", _session(), page)
    expected_trail = [(d.decision_id, t) for d, t in engine.decisions]
    # Exactly what the per-row filter records for the same rows, nothing more.
    assert [(d.decision_id, t) for d, t in engine_trail] == expected_trail
    # As today: the per-row filter records the read decisions of every row
    # it decides (the ones finish was given), served or hidden.
    reads = [t for _d, t in engine_trail if str(t["kind"]).endswith("_read")]
    assert {t["node_id"] for t in reads} == {r["node_id"] for r in page}
    assert {r["node_id"] for r in served} <= {r["node_id"] for r in page}
    assert all(not str(t["kind"]).endswith("_scan") for _d, t in engine_trail)


# --------------------------------------------------------------------------- #
# sql_exact without a scan (Jason, 2026-10-02)                                 #
# --------------------------------------------------------------------------- #
def test_sql_exact_is_true_only_with_nomos_off(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _plain_rows("node")
    _offline["catalog"]["node"] = _catalog_of("node", rows)
    engine.bundle = _bundle(_RULES)
    view = _open("node")
    assert view.exactness() == {"sql_exact": False, "reason": "nomos_on"}
    assert reader_view.exactness_cause(view) == "rule_can_hide_or_mask"
    assert not hasattr(view, "unclassed_counts") and not hasattr(view, "sql_exact_for")
    # An allow-everything bundle still is not exact: the per-row filter hides
    # a row with no source lineage whatever the policy says.
    engine.bundle = _bundle([{"rule_id": "allow_all", "effect": "allow", "match": {}}])
    allow_all = _open("node")
    assert allow_all.mode == "class"
    # In enforce a blank classification is an invalid identity fact: denied.
    assert allow_all.exactness() == {"sql_exact": False, "reason": "nomos_on"}
    assert reader_view.exactness_cause(allow_all) == "blank_classification_denied"
    engine.mode = "audit"
    assert reader_view.exactness_cause(_open("node")) == (
        "per_row_filter_hides_rows_without_lineage"
    )
    engine.mode = "enforce"
    bare = {"node_id": "node:bare", "subtype": "doc", "attrs": {"name": "x"},
            "source": None, "source_record_id": {}, "source_revision": {}}
    assert allow_all.finish([bare]) == []
    for mode in ("audit", "enforce"):
        engine.mode = mode
        assert _open("node").finish([bare]) == [], mode
    # Per-row mode is never exact.
    assert _open("node", force="per_row").sql_exact is False
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    off = _open("node")
    assert off.exactness() == {"sql_exact": True, "reason": "nomos_off"}
    assert off.finish([bare, *rows]) == [bare, *rows]


def test_audit_mode_still_masks_on_redact(engine: _Engine, _offline: dict) -> None:
    """Why audit is not exact either: audit turns only hide effects into
    allow; a redact still masks."""
    engine.mode = "audit"
    engine.bundle = _bundle(_RULES)
    rows = [r for r in _plain_rows("node") if r["source"] == "docs"]
    out = _per_row("node", _session(), rows)
    assert out and all(isinstance(r["attrs"], ReadRedactedAttrs) for r in out)


def test_the_catalog_has_a_clone_disposition() -> None:
    from okg.substrate.environment_clone import clone_table_disposition

    # It describes the graph rows a clone purges, so it purges with them.
    assert clone_table_disposition("okg", "nomos_read_classes") == "purge"


@pytest.mark.parametrize("force", [None, "per_row"])
def test_finish_refuses_node_rows_without_their_subtype(
    engine: _Engine, _offline: dict, force: Any,
) -> None:
    """Failure case (LOW-1): masks key on the row's own subtype, so a projected
    node row without it is refused, never masked as subtype ""."""
    rows = _plain_rows("node")
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", rows)
    view = _open("node", force=force)
    projected = [{k: v for k, v in row.items() if k != "subtype"} for row in rows]
    with pytest.raises(reader_view.ReaderViewRowShapeError, match="subtype"):
        view.finish(projected)
    # Edges carry no subtype; their rows are accepted.
    assert _open("edge").finish(_plain_rows("edge")) is not None


def test_the_exactness_reason_does_not_reveal_a_hidden_class(
    engine: _Engine, _offline: dict,
) -> None:
    """MED-1: the catalog holds classes of hidden rows (every generation), so
    nothing a reader sees may depend on whether such a class exists."""
    engine.bundle = _bundle([
        {"rule_id": "deny_jira", "effect": "deny", "match": {"source_id": "jira"}},
    ])  # default effect: allow
    engine.mode = "enforce"
    visible = [r for r in _plain_rows("node") if r["source"] == "git"]
    hidden = [r for r in _plain_rows("node") if r["source"] == "jira"]
    _offline["catalog"]["node"] = _catalog_of("node", visible)
    without = _open("node")
    _offline["catalog"]["node"] = {**_catalog_of("node", visible), **_catalog_of("node", hidden)}
    with_hidden = _open("node")
    # The cause differs (only the catalog with the jira class can hide) ...
    assert reader_view.exactness_cause(without) != reader_view.exactness_cause(with_hidden)
    # ... but what the view reports does not.
    assert without.exactness() == with_hidden.exactness() == {
        "sql_exact": False, "reason": "nomos_on",
    }
    assert without.reason == with_hidden.reason == "read_class_catalog"


def test_finish_after_nomos_is_turned_on_fails_closed(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LOW-3: a view opened with Nomos off re-checks the mode in finish."""
    rows = _plain_rows("node")
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = _catalog_of("node", rows)
    nomos_on = {"value": False}
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime",
                        lambda name: nomos_on["value"])
    view = _open("node")
    assert view.mode == "off"
    nomos_on["value"] = True
    got = view.finish(rows)
    assert (view.mode, view.reason) == ("per_row", "nomos_turned_on_after_open")
    assert _canon(got) == _canon(_per_row("node", _session(), rows))
    assert len(got) < len(rows)  # jira and restricted rows were removed
    assert view.sql_exact is False


# --------------------------------------------------------------------------- #
# The class-decision cache (PACT reader-view-decision-cache-3560)              #
# --------------------------------------------------------------------------- #
@pytest.fixture
def decided(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """One entry per call of the policy evaluation step (``decide_classes``),
    holding the row kind it decided. The cache stands in front of it, so a
    cache hit adds nothing."""
    calls: list[str] = []
    real = reader_view.decide_classes

    def counting(conn: Any, session: Any, catalog: Any, **kw: Any) -> Any:
        calls.append(kw["kind"])
        return real(conn, session, catalog, **kw)

    monkeypatch.setattr(reader_view, "decide_classes", counting)
    return calls


_HIDES_NOTHING = [{"rule_id": "deny_nobody", "effect": "deny", "match": {"source_id": "nowhere"}}]


def _catalog_for(_offline: dict, kind: str, rows: list[dict[str, Any]]) -> dict[str, uuid.UUID]:
    """Set the catalog to the classes of ``rows``; returns source -> class id."""
    catalog = _catalog_of(kind, rows)
    _offline["catalog"][kind] = catalog
    return {facts["source_ids"][0]: class_id for class_id, facts in catalog.items()}


def test_the_second_open_serves_the_class_decisions_from_the_cache(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))

    first = _open("node")
    second = _open("node")

    assert decided == ["node"], "the policy was evaluated again for an unchanged key"
    assert second._state.decisions == first._state.decisions
    assert second.visible_classes == first.visible_classes
    assert second.fingerprint == first.fingerprint
    assert reader_view.decision_cache_info()["hits"] == 1


def test_a_policy_change_is_a_new_key_even_under_the_same_policy_version(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    """Must fail if the key ignores the policy: the three bundles share one
    ``policy_version``; only their rules differ. (Neither names a
    classification, so the probes, which the key also holds, are the same.)"""
    jira = _catalog_for(_offline, "node", _plain_rows("node"))["jira"]

    engine.bundle = _bundle([_RULES[0]])  # denies jira
    denied = _open("node")
    engine.bundle = _bundle(_HIDES_NOTHING)  # same version, rules differ
    allowed = _open("node")
    engine.bundle = _bundle([_RULES[0]])  # back to the first policy
    denied_again = _open("node")

    assert jira not in denied.visible_classes
    assert jira in allowed.visible_classes, "a cached decision outlived the policy change"
    assert denied_again.visible_classes == denied.visible_classes
    # Two policies decided once each; returning to the first is a hit.
    assert decided == ["node", "node"]


def test_a_new_catalog_class_is_decided_not_read_from_the_old_set(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    """Must fail if the key ignores the catalog: the new class would be missing
    from the cached set, and the view would not have decided it."""
    engine.bundle = _bundle(_RULES)  # denies jira
    git_rows = [r for r in _plain_rows("node") if r["source"] == "git"]
    jira_rows = [r for r in _plain_rows("node") if r["source"] == "jira"]
    _catalog_for(_offline, "node", git_rows)
    before = _open("node")
    by_source = _catalog_for(_offline, "node", git_rows + jira_rows)
    after = _open("node")
    _catalog_for(_offline, "node", git_rows)
    before_again = _open("node")

    assert by_source["jira"] not in before.decided_classes
    assert by_source["jira"] in after.decided_classes
    assert by_source["jira"] not in after.visible_classes  # decided, and denied
    assert before_again.decided_classes == before.decided_classes
    assert decided == ["node", "node"]


def test_the_pinned_catalog_version_is_part_of_the_key(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))

    _open("node", _session(catalog_version_id="catalog-v1"))
    _open("node", _session(catalog_version_id="catalog-v1"))
    _open("node", _session(catalog_version_id="catalog-v2"))

    assert decided == ["node", "node"]


def test_the_reader_is_part_of_the_key(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    """Must fail if the key ignores the subject: reader b would be served the
    decisions made for reader a, who may not read jira."""
    engine.bundle = _bundle([
        {"rule_id": "deny_jira_for_a", "effect": "deny", "match": {"source_id": "jira"},
         "subjects": ["subject:a"]},
    ])
    jira = _catalog_for(_offline, "node", _plain_rows("node"))["jira"]

    a = _open("node", _session(nomos_subject="subject:a"))
    b = _open("node", _session(nomos_subject="subject:b"))
    a_again = _open("node", _session(nomos_subject="subject:a"))

    assert jira not in a.visible_classes
    assert jira in b.visible_classes, "reader b was served reader a's decisions"
    assert a_again.visible_classes == a.visible_classes
    assert decided == ["node", "node"]


def test_the_surface_is_part_of_the_key(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    """Same action, another surface: decided again. (The surface names the
    enforcement point the policy is evaluated at, so it is an input of the
    evaluation even where today's policies decide the same.)"""
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))

    _open("node", _session(nomos_read_action="graph.read.mcp", nomos_read_surface="mcp"))
    _open("node", _session(nomos_read_action="graph.read.mcp", nomos_read_surface="mcp"))
    _open("node", _session(
        nomos_read_action="graph.read.mcp", nomos_read_surface=LOCAL_OPERATOR_READ_SURFACE,
    ))

    assert decided == ["node", "node"]


def test_the_action_is_part_of_the_key(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    """Same surface, another action: decided again (the action is an input of
    the policy evaluation, whatever the mapping makes of it)."""
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))

    _open("node", _session(nomos_read_action="graph.read.mcp", nomos_read_surface="mcp"))
    _open("node", _session(nomos_read_action="graph.read.mcp", nomos_read_surface="mcp"))
    _open("node", _session(nomos_read_action=LOCAL_OPERATOR_READ_ACTION,
                           nomos_read_surface="mcp"))

    assert decided == ["node", "node"]


def test_the_enforcement_mode_is_part_of_the_key(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    """Audit lets a denied class through; enforce does not. Same policy."""
    engine.bundle = _bundle(_RULES)
    jira = _catalog_for(_offline, "node", _plain_rows("node"))["jira"]

    engine.mode = "enforce"
    enforced = _open("node")
    engine.mode = "audit"
    audited = _open("node")
    engine.mode = "enforce"
    enforced_again = _open("node")

    assert jira not in enforced.visible_classes
    assert jira in audited.visible_classes
    assert enforced_again.visible_classes == enforced.visible_classes
    assert decided == ["node", "node"]


@pytest.mark.parametrize(("keyed", "flipped_to"), [("enforce", "audit"), ("audit", "enforce")])
def test_a_mode_switch_during_a_fill_never_changes_the_keyed_decision(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
    keyed: str, flipped_to: str,
) -> None:
    """Must fail if a cache miss re-reads the enforcement mode while it decides:
    a manifest switched during the fill would store audit decisions under the
    enforce key (or the reverse), and every later open with that key would be
    served them."""
    engine.mode = keyed
    engine.bundle = _bundle([_RULES[0]])  # deny jira
    jira = _catalog_for(_offline, "node", _plain_rows("node"))["jira"]
    real = reader_view.decide_classes

    def switching(conn: Any, session: Any, catalog: Any, **kw: Any) -> Any:
        engine.mode = flipped_to  # the manifest is switched while the miss is filled
        try:
            return real(conn, session, catalog, **kw)
        finally:
            engine.mode = keyed

    monkeypatch.setattr(reader_view, "decide_classes", switching)
    _open("node")
    monkeypatch.setattr(reader_view, "decide_classes", real)
    later = _open("node")  # a genuine open under the keyed mode; nothing flips
    assert reader_view.decision_cache_info()["hits"] == 1  # it was served from the fill

    reader_view.clear_reader_view_cache()
    fresh = _open("node")  # the same open, decided from scratch
    assert later.visible_classes == fresh.visible_classes
    assert later.admits_every_row == fresh.admits_every_row
    assert (jira in later.visible_classes) == (keyed == "audit")
    if keyed == "enforce":
        assert not later.admits_every_row, "the reader clause was dropped under an enforce key"


def test_a_reader_change_during_a_fill_never_changes_the_keyed_decision(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Must fail if a cache miss re-reads the reader's subject from the
    environment while it decides: reader a's key would hold what reader b may
    see, and a would read a class denied to a."""
    engine.mode = "enforce"
    engine.bundle = _bundle([
        {"rule_id": "deny_jira_for_a", "effect": "deny", "match": {"source_id": "jira"},
         "subjects": ["subject:a"]},
    ])
    jira = _catalog_for(_offline, "node", _plain_rows("node"))["jira"]
    who = {"subject": "subject:a"}
    monkeypatch.setattr(read_filter, "nomos_subject_from_env", lambda **_: who["subject"])
    real = reader_view.decide_classes

    def switching(conn: Any, session: Any, catalog: Any, **kw: Any) -> Any:
        who["subject"] = "subject:b"  # the environment changes while the miss is filled
        try:
            return real(conn, session, catalog, **kw)
        finally:
            who["subject"] = "subject:a"

    session = _session(nomos_subject=None)  # the subject comes from the environment
    monkeypatch.setattr(reader_view, "decide_classes", switching)
    _open("node", session)
    monkeypatch.setattr(reader_view, "decide_classes", real)
    later = _open("node", session)
    assert reader_view.decision_cache_info()["hits"] == 1
    assert jira not in later.visible_classes, "reader a was served a class denied to a"

    reader_view.clear_reader_view_cache()
    assert _open("node", session).visible_classes == later.visible_classes


def test_the_row_kind_is_part_of_the_key(
    engine: _Engine, _offline: dict, decided: list[str],
) -> None:
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))
    _catalog_for(_offline, "edge", _plain_rows("edge"))

    _open("node")
    _open("edge")
    _open("node")
    _open("edge")

    assert decided == ["node", "edge"]


def test_what_a_caller_does_to_a_returned_decision_never_reaches_the_cache(
    engine: _Engine, _offline: dict,
) -> None:
    engine.bundle = _bundle(_RULES)
    jira = _catalog_for(_offline, "node", _plain_rows("node"))["jira"]
    internal = reader_view._md5("internal")

    first = _open("node")  # decided
    first._state.decisions[jira][internal] = ClassOutcome(True, False)  # a caller edits it
    second = _open("node")  # from the cache
    second._state.decisions[jira][internal] = ClassOutcome(True, False)  # and edits this one
    third = _open("node")

    assert third._state.decisions[jira][internal] == ClassOutcome(False, False)
    assert not any(o.visible for o in third._state.decisions[jira].values())


def test_the_cache_is_bounded_and_drops_the_least_recently_used(
    engine: _Engine, _offline: dict, decided: list[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(reader_view, "DECISION_CACHE_MAX", 3)
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))

    for n in range(8):
        _open("node", _session(nomos_subject=f"subject:r{n}"))
    info = reader_view.decision_cache_info()
    assert (info["entries"], info["max_entries"], info["evictions"]) == (3, 3, 5)
    assert len(decided) == 8

    _open("node", _session(nomos_subject="subject:r7"))  # most recent: still cached
    assert len(decided) == 8
    _open("node", _session(nomos_subject="subject:r0"))  # evicted long ago: decided again
    assert len(decided) == 9
    assert reader_view.decision_cache_info()["entries"] == 3


# ---- admits_every_row: the clause may be left out -------------------------- #
@pytest.mark.parametrize(
    ("rules", "mode", "admits"),
    [
        (_HIDES_NOTHING, "enforce", True),
        (_HIDES_NOTHING, "audit", True),
        (_RULES, "enforce", False),  # jira and restricted rows are denied
        ([_RULES[0]], "enforce", False),  # a deny alone
        ([_RULES[0]], "audit", True),  # audit lets a deny through and masks nothing
        (_RULES, "audit", False),  # the redaction still masks in audit
        ([_RULES[2]], "enforce", False),  # a redaction alone
    ],
)
def test_admits_every_row_follows_the_decisions(
    engine: _Engine, _offline: dict, rules: list, mode: str, admits: bool,
) -> None:
    engine.mode = mode
    engine.bundle = _bundle(rules)
    _catalog_for(_offline, "node", _plain_rows("node"))

    assert _open("node").admits_every_row is admits


def test_admits_every_row_is_true_with_no_class_decided_and_in_the_non_class_modes(
    engine: _Engine, _offline: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine.bundle = _bundle(_RULES)
    _offline["catalog"]["node"] = {}
    assert _open("node").admits_every_row is True  # no class: no row is known
    _catalog_for(_offline, "node", _plain_rows("node"))
    assert _open("node", force="per_row").admits_every_row is True
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    assert _open("node").admits_every_row is True


def test_admits_every_row_with_no_bundle_follows_the_default_decision(
    engine: _Engine, _offline: dict,
) -> None:
    engine.bundle = None
    engine.mode = "audit"
    served = _open("node")
    engine.mode = "enforce"
    refused = _open("node")
    assert (served.mode, served.admits_every_row) == ("no_bundle", True)
    assert (refused.mode, refused.admits_every_row) == ("no_bundle", False)


# ---- keep_sql: the clause lists what is hidden, not what is visible --------- #
def _many_classes(n: int) -> list[dict[str, Any]]:
    return [
        {"node_id": f"node:{i}", "source": f"src{i:03d}", "source_record_id": {"key": "a"},
         "source_revision": {"v": 1}, "subtype": "doc",
         "attrs": {"classification": "internal", "name": "n", "body": "b", "title": "t"}}
        for i in range(n)
    ]


@pytest.mark.parametrize(("denied", "ratio"), [(19, 4), (0, 0)])
def test_keep_sql_is_a_fraction_of_the_text_when_most_classes_are_visible(
    engine: _Engine, _offline: dict, denied: int, ratio: int,
) -> None:
    """187 classes, 19 denied (about 10 percent, as in the large-copy cost run):
    the clause that lists the hidden classes is under a quarter of the length
    of the one that lists the visible ones. With none denied there is nothing
    to list."""
    engine.bundle = _bundle([
        {"rule_id": f"deny_{i}", "effect": "deny", "match": {"source_id": f"src{i:03d}"}}
        for i in range(denied)
    ] or _HIDES_NOTHING)
    _catalog_for(_offline, "node", _many_classes(187))
    view = _open("node")

    old = f"({view.predicate('n').as_string(None)}) AND (({view.readable('n', 'text').as_string(None)}) IS NOT FALSE)"
    new = view.keep_sql("n", "text").as_string(None)

    assert len(view.decided_classes) == 187
    if denied:
        assert len(new) * ratio < len(old), (len(new), len(old))
        assert new.count("ANY(") <= 3, "the clause lists hidden classes once, not repeatedly"
    else:
        assert new == "TRUE"


def test_keep_sql_outside_class_mode_is_the_old_composition(
    engine: _Engine, _offline: dict,
) -> None:
    engine.bundle = _bundle(_RULES)
    _catalog_for(_offline, "node", _plain_rows("node"))
    view = _open("node", force="per_row")
    assert view.keep_sql("n", "text").as_string(None) == (
        f"({view.predicate('n').as_string(None)}) AND (({view.readable('n', 'text').as_string(None)}) IS NOT FALSE)"
    )

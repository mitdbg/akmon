"""PACT nomos-decision-class-filter: the class-based visibility answer.

``visible_ids_by_decision_class`` must answer exactly what the per-row filter
answers (``filter_node_rows_for_session`` / ``filter_edge_rows_for_session``)
for every rule shape the policy engine evaluates. These tests run the real
policy engine and runtime boundary (``nomos.core.evaluate`` behind
``evaluate_with_active_policy``) over randomized rows and randomized bundles;
only storage is replaced: the active bundle is served from memory and each
persisted decision is captured in a list, so the audit trail can be counted.
"""
from __future__ import annotations

import contextlib
import random
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.nomos import read_filter, store
from okg.substrate.nomos.core import CompiledPolicyBundle, compile_policy_graph
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
    visible_ids_by_decision_class,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    RECORD_DETAIL_KEY,
    RECORD_DETAIL_TRUNCATED,
    combine_source_lineages,
    graph_object_ref,
    normalize_source_lineage,
    source_record_ref,
)
from okg.substrate.operator_read import (
    LOCAL_OPERATOR_READ_ACTION,
    LOCAL_OPERATOR_READ_SURFACE,
)

DEPLOYMENT = "decision-class-test"
SUBJECT = "subject:reader"
SOURCES = ("git", "jira", "docs")
CLASSIFICATIONS = (None, "internal", "restricted", "public", "  ")
RECORD_KEYS = ("a", "b", "c", "d")
#: Record ids the boundary sanitizes before any rule sees them: a local path,
#: an SSN-shaped value, a secret reference, a string over 512 characters.
REDACTED_KEYS = ("/Users/someone/private.txt", "123-45-6789", "secret://vault/x", "z" * 600)
EFFECTS = ("allow", "deny", "review", "quarantine", "redact", "declassify")


class _Conn:
    """Stands in for the connection: the filter commits after it records."""

    def commit(self) -> None:
        pass


class _Engine:
    def __init__(self) -> None:
        self.bundle: CompiledPolicyBundle | None = None
        self.mode = "enforce"
        self.decisions: list[tuple[Any, dict[str, Any]]] = []


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch) -> _Engine:
    state = _Engine()

    def load_bundle(conn: Any, **_: Any) -> CompiledPolicyBundle | None:
        return state.bundle

    def persist(conn: Any, *, deployment_name: str, decision: Any, target: Any, **_: Any) -> None:
        state.decisions.append((decision, dict(target)))

    @contextlib.contextmanager
    def no_batch(conn: Any) -> Iterator[None]:
        yield None

    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: True)
    monkeypatch.setattr(read_filter, "apply_effective_attrs_to_rows", lambda conn, rows, **_: rows)
    monkeypatch.setattr(read_filter, "batched_decision_persist", no_batch)
    monkeypatch.setattr(read_filter, "load_active_policy_bundle", load_bundle)
    monkeypatch.setattr(store, "load_active_policy_bundle", load_bundle)
    monkeypatch.setattr(store, "persist_nomos_decision", persist)
    monkeypatch.setattr(store, "deployment_nomos_enforcement_mode", lambda name: state.mode)
    # The uncached activation re-read the no-bundle path makes before its one
    # decision (its real query is proven on PostgreSQL below).
    monkeypatch.setattr(
        read_filter, "_active_policy_fingerprint_now",
        lambda conn, **_: state.bundle.policy_fingerprint if state.bundle is not None else None,
    )
    return state


def _session(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "deployment_name": DEPLOYMENT,
        "generation_id": "gen:test",
        "branch_name": "default",
        "catalog_version_id": None,
        "nomos_subject": SUBJECT,
        "nomos_read_action": LOCAL_OPERATOR_READ_ACTION,
        "nomos_read_surface": LOCAL_OPERATOR_READ_SURFACE,
        "nomos_multi_user": False,
        "nomos_identity_claim": None,
        "nomos_access_context": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


# --------------------------------------------------------------------------- #
# Random rows                                                                  #
# --------------------------------------------------------------------------- #
def _row(rng: random.Random, kind: str, row_id: str) -> dict[str, Any]:
    attrs: dict[str, Any] = {"name": f"n{rng.randrange(5)}"}
    classification = rng.choice(CLASSIFICATIONS)
    if classification is not None:
        attrs["classification"] = classification
    source: str | None = rng.choice((*SOURCES, None)) if rng.random() < 0.9 else None
    record: Any = {"key": rng.choice(RECORD_KEYS)}
    revision: Any = {"v": rng.choice((1, 2))}
    if rng.random() < 0.15:
        record = {"key": rng.choice(REDACTED_KEYS)}
    if rng.random() < 0.05:
        # Not JSON: the boundary refuses the resource as malformed.
        revision = {"v": float("nan")}
    shape = rng.random()
    if shape < 0.1:
        # A lineage envelope whose per-record detail was truncated.
        lineage = normalize_source_lineage(
            source_id=rng.choice(SOURCES), source_record_id=record, source_revision=revision,
        )
        lineage[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
        attrs[LINEAGE_ATTR] = lineage
    elif shape < 0.25:
        # Several sources, carried in the attrs lineage envelope.
        picked = rng.sample(SOURCES, rng.randint(1, 3))
        attrs[LINEAGE_ATTR] = combine_source_lineages([
            normalize_source_lineage(
                source_id=s,
                source_record_id={"key": rng.choice(RECORD_KEYS)},
                source_revision={"v": rng.choice((1, 2))},
            )
            for s in picked
        ])
    elif shape < 0.3:
        record = None  # a source with no record id: no fine-grained target
    row: dict[str, Any] = {
        f"{kind}_id": row_id,
        "attrs": attrs,
        "source": source,
        "source_record_id": record,
        "source_revision": revision,
    }
    if kind == "node":
        row["subtype"] = rng.choice(("doc", "ticket"))
    else:
        row.update(src="n:1", dst="n:2", edge_type="links")
    return row


def _rows(rng: random.Random, kind: str, n: int) -> list[dict[str, Any]]:
    ids = [f"{kind}:{i}" for i in range(n)]
    rows = [_row(rng, kind, row_id) for row_id in ids]
    # Now and then two rows share an id; the answers must still agree.
    if n > 2 and rng.random() < 0.3:
        rows.append(_row(rng, kind, rng.choice(ids)))
    return rows


# --------------------------------------------------------------------------- #
# Random bundles: every match shape the engine evaluates                       #
# --------------------------------------------------------------------------- #
def _match(rng: random.Random, kind: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    some_id = str(rng.choice(rows)[f"{kind}_id"])
    some_source = rng.choice(SOURCES)
    some_ref = source_record_ref(some_source, {"key": rng.choice(RECORD_KEYS)})
    shapes: list[dict[str, Any]] = [
        {},
        {"classification": rng.choice(("internal", "restricted", "public"))},
        {"classification": rng.sample(["internal", "restricted", "public"], 2)},
        {"source_id": some_source},
        {"source_id": rng.sample(list(SOURCES), 2)},
        {"source_ids": rng.sample(list(SOURCES), 2)},
        {"source_ids": some_source},
        {"resource_kind": rng.choice(("graph_object", "source_record", "local_operator_boundary"))},
        {f"{kind}_id": some_id},
        {f"{kind}_id": [some_id, f"{kind}:0"]},
        {"graph_ref": graph_object_ref(kind, some_id)},
        {f"{kind}_ref": graph_object_ref(kind, some_id)},
        {"source_record_ref": some_ref},
        {"source_record_refs": [some_ref]},
        {"source_record_id": {"key": rng.choice(RECORD_KEYS)}},
        {"source_record_id": {"key": rng.choice(REDACTED_KEYS)}},
        {"source_record_observed_at": "2026-01-01T00:00:00+00:00"},
        {"is_after_as_of": True},
        {"resource_kind": "source_record", "source_revision": {"v": 2}},
        {"source_revision": {"v": rng.choice((1, 2))}},
        {"kind": kind},
        {"audit": False},
        {"deployment": DEPLOYMENT},
        {"scope": f"deployment:{DEPLOYMENT}"},
        {"subject": SUBJECT},
        {"action": LOCAL_OPERATOR_READ_ACTION},
        {"fact_no_row_has": "x"},
    ]
    match = dict(rng.choice(shapes))
    if rng.random() < 0.3:
        match.update(rng.choice(shapes))
    return match


def _bundle(rng: random.Random, kind: str, rows: list[dict[str, Any]]) -> CompiledPolicyBundle:
    rules = []
    for i in range(rng.randint(1, 6)):
        effect = rng.choice(EFFECTS)
        obligations: list[dict[str, Any]] = []
        if effect == "redact" or rng.random() < 0.3:
            obligations.append({"type": "redaction"})
        if effect == "declassify" and rng.random() < 0.5:
            obligations.append({"type": "declassification"})
        if effect == "quarantine" and rng.random() < 0.5:
            obligations.append({"type": "quarantine"})
        rules.append({
            "rule_id": f"r{i}",
            "effect": effect,
            "actions": rng.choice(([LOCAL_OPERATOR_READ_ACTION], ["*"], ["graph.read.mcp"])),
            "subjects": rng.choice(([SUBJECT], ["*"], ["subject:someone-else"])),
            "match": _match(rng, kind, rows),
            "priority": i,
            "reason_code": f"nomos.test.r{i}",
            "obligations": obligations,
        })
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:decision-class",
        "policy_version": "v1",
        "default_effect": rng.choice(("allow", "deny")),
        "rules": rules,
    })


def _per_row(kind: str, session: Any, rows: list[dict[str, Any]]) -> set[str]:
    exact = filter_node_rows_for_session if kind == "node" else filter_edge_rows_for_session
    return {str(row[f"{kind}_id"]) for row in exact(_Conn(), session, [dict(r) for r in rows])}


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_the_class_answer_equals_the_per_row_answer_for_random_rules_and_rows(
    engine: _Engine, kind: str, mode: str,
) -> None:
    engine.mode = mode
    session = _session()
    hidden_somewhere = shown_somewhere = 0
    for seed in range(150):
        rng = random.Random(f"{kind}:{mode}:{seed}")
        rows = _rows(rng, kind, rng.randint(1, 40))
        engine.bundle = _bundle(rng, kind, rows)

        by_row = _per_row(kind, session, rows)
        by_class = visible_ids_by_decision_class(_Conn(), session, rows, row_kind=kind)

        assert by_class == by_row, (seed, engine.bundle.rules)
        hidden_somewhere += len({str(r[f"{kind}_id"]) for r in rows} - by_row) > 0
        shown_somewhere += bool(by_row)
    # The random bundles exercised both answers, not one trivially.
    assert hidden_somewhere > 20 and shown_somewhere > 20


@pytest.mark.parametrize("kind", ["node", "edge"])
def test_a_rule_naming_one_record_hides_only_that_record(engine: _Engine, kind: str) -> None:
    rng = random.Random(7)
    rows = [
        {**_row(rng, kind, f"{kind}:{i}"), "source": "git", "source_record_id": {"key": "a"},
         "source_revision": {"v": 1}, "attrs": {"classification": "internal"}}
        for i in range(30)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:one-record",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {"rule_id": "allow_all", "effect": "allow", "actions": ["*"], "subjects": ["*"],
             "match": {}, "priority": 1},
            {"rule_id": "deny_one", "effect": "deny", "actions": ["*"], "subjects": ["*"],
             "match": {f"{kind}_id": f"{kind}:3"}, "priority": 2},
        ],
    })

    visible = visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind=kind)

    assert visible == {f"{kind}:{i}" for i in range(30)} - {f"{kind}:3"}
    assert visible == _per_row(kind, _session(), rows)


def test_an_invalid_protected_identity_fact_is_decided_as_the_per_row_filter_does(
    engine: _Engine,
) -> None:
    """A blank classification fails the protected-identity check: denied."""
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": "git",
         "source_record_id": {"key": "a"}, "source_revision": {"v": 1},
         "attrs": {"classification": "  " if i % 2 else "internal"}}
        for i in range(10)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:identity", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "noop", "effect": "allow", "actions": ["*"], "subjects": ["*"],
                   "match": {"fact_no_row_has": 1}}],
    })

    visible = visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node")

    assert visible == {f"node:{i}" for i in range(0, 10, 2)}
    assert visible == _per_row("node", _session(), rows)


def test_only_each_class_representative_gets_decision_rows_marked_as_counts(
    engine: _Engine,
) -> None:
    """The stated audit semantics: counted rows share their class's decisions,
    which are recorded as counts, apart from the reads of returned rows."""
    rows = [
        {"node_id": f"node:{i:02d}", "subtype": "doc", "source": "git",
         "source_record_id": {"key": "a"}, "source_revision": None,
         "attrs": {"classification": "restricted" if i % 3 == 0 else "internal"}}
        for i in range(12)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:audit", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"],
                   "subjects": ["*"], "match": {"classification": "restricted"}}],
    })

    visible = visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node")

    assert visible == {f"node:{i:02d}" for i in range(12) if i % 3}
    counted = list(engine.decisions)
    # One representative per class, the first in id order: node:00 and node:01.
    assert {target["node_id"] for _, target in counted} == {"node:00", "node:01"}
    kinds = {target["kind"] for _, target in counted}
    assert "local_operator_node_source_count" in kinds
    assert "local_operator_node_source_record_count" in kinds
    assert all(kind.endswith("_count") for kind in kinds)
    assert all(d.context.get(read_filter.READ_PURPOSE_FACT) == "count" for d, _ in counted)

    engine.decisions.clear()
    returned = filter_node_rows_for_session(_Conn(), _session(), rows)
    read = list(engine.decisions)
    # A caller that RETURNS rows filters them row by row: every row is decided,
    # as a read, and no read decision shares an identity with a count decision,
    # so the insert-once trail keeps both.
    assert {target["node_id"] for _, target in read} == {f"node:{i:02d}" for i in range(12)}
    assert all(target["kind"].endswith("_read") for _, target in read)
    assert read_filter.READ_PURPOSE_FACT not in {k for d, _ in read for k in d.context}
    assert {d.decision_id for d, _ in read}.isdisjoint({d.decision_id for d, _ in counted})
    assert {row["node_id"] for row in returned} == visible


def test_the_representative_does_not_depend_on_input_order(engine: _Engine) -> None:
    rows = [
        {"node_id": f"node:{i:02d}", "subtype": "doc", "source": "git",
         "source_record_id": None, "source_revision": None,
         "attrs": {"classification": ("internal", "restricted", "public")[i % 3]}}
        for i in range(15)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:order", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"],
                   "subjects": ["*"], "match": {"classification": "restricted"}}],
    })
    decided_sets = []
    for seed in range(5):
        shuffled = list(rows)
        random.Random(seed).shuffle(shuffled)
        engine.decisions.clear()
        visible_ids_by_decision_class(_Conn(), _session(), shuffled, row_kind="node")
        decided_sets.append({target["node_id"] for _, target in engine.decisions})
    assert decided_sets == [{"node:00", "node:01", "node:02"}] * 5


def test_an_identity_claim_session_is_filtered_row_by_row(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The identity-claim path evaluates facts outside the class projection:
    one source is denied by the claim, and the class answer still matches."""
    from okg.substrate.nomos import identity_store

    def claim(conn: Any, *, resource: Any, **_: Any) -> Any:
        effect = "deny" if resource["source_id"] == "jira" else "allow"
        return SimpleNamespace(
            decision=SimpleNamespace(effect=effect, decision_id=f"claim:{effect}"),
            resolved_subject=SUBJECT,
        )

    monkeypatch.setattr(identity_store, "evaluate_identity_claim_from_postgres", claim)
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": SOURCES[i % 3],
         "source_record_id": {"key": "a"}, "source_revision": None,
         "attrs": {"classification": "internal"}}
        for i in range(9)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:claim", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "allow", "effect": "allow", "actions": ["*"], "subjects": ["*"],
                   "match": {}}],
    })
    session = _session(nomos_identity_claim={"iss": "test", "sub": "reader"})

    visible = visible_ids_by_decision_class(_Conn(), session, rows, row_kind="node")

    assert visible == _per_row("node", session, rows)
    assert visible == {f"node:{i}" for i in range(9) if SOURCES[i % 3] != "jira"}


@pytest.mark.parametrize(
    ("match", "expected_hidden"),
    [
        # Facts present only on the fine-grained source-record target.
        ({"resource_kind": "source_record", "source_revision": {"v": 2}}, {"node:1", "node:3"}),
        ({"source_record_id": {"key": "b"}}, {"node:1"}),
        ({"source_record_ref": source_record_ref("git", {"key": "c"})}, {"node:2"}),
    ],
)
def test_rules_on_facts_only_fine_grained_targets_carry(
    engine: _Engine, match: dict[str, Any], expected_hidden: set[str],
) -> None:
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": "git",
         "source_record_id": {"key": RECORD_KEYS[i]}, "source_revision": {"v": 1 + i % 2},
         "attrs": {"classification": "internal"}}
        for i in range(4)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:fine", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "deny_fine", "effect": "deny", "actions": ["*"],
                   "subjects": ["*"], "match": match}],
    })

    visible = visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node")

    assert visible == {f"node:{i}" for i in range(4)} - expected_hidden
    assert visible == _per_row("node", _session(), rows)


def test_an_observed_at_rule_under_an_as_of_context_matches_the_per_row_answer(
    engine: _Engine,
) -> None:
    """``source_record_observed_at`` exists only on fine-grained targets and only
    under an as-of access context, which the class path filters row by row."""
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": "git",
         "source_record_id": {"key": RECORD_KEYS[i]},
         "source_revision": {"updated_at": f"2026-0{i + 1}-01T00:00:00Z"},
         "attrs": {"classification": "internal"}}
        for i in range(4)
    ]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:asof", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "deny_future", "effect": "deny", "actions": ["*"],
                   "subjects": ["*"], "match": {"is_after_as_of": True}}],
    })
    session = _session(nomos_access_context={"context_id": "ctx:asof", "as_of": "2026-02-15T00:00:00Z"})

    visible = visible_ids_by_decision_class(_Conn(), session, rows, row_kind="node")

    assert visible == _per_row("node", session, rows)
    assert visible == {"node:0", "node:1"}


def test_an_unserialisable_fact_is_decided_as_the_per_row_filter_decides_it(
    engine: _Engine,
) -> None:
    """Lineage facts are normalised to JSON, but the row id reaches the policy
    facts as given. An id the canonical JSON cannot write (a mapping with mixed
    key types) must not raise: the row is its own class, the per-row filter
    decides it, and the boundary hides it as malformed, as it does on a read."""
    odd_id = {1: "mixed", "k": "keys"}
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": "git",
         "source_record_id": {"key": "a"}, "source_revision": None,
         "attrs": {"classification": "internal"}}
        for i in range(3)
    ]
    rows.append({**rows[0], "node_id": odd_id})
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:odd", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "deny_one", "effect": "deny", "actions": ["*"],
                   "subjects": ["*"], "match": {"node_id": "node:1"}}],
    })
    with pytest.raises(TypeError):
        read_filter._fact_text(odd_id)  # the projection cannot write it

    visible = visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node")

    assert visible == _per_row("node", _session(), rows) == {"node:0", "node:2"}
    # The odd row was decided on its own, as its own class.
    assert any(target["node_id"] == odd_id for _, target in engine.decisions)


def test_a_session_with_an_access_context_is_filtered_row_by_row(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": "git",
         "source_record_id": {"key": RECORD_KEYS[i % 4]}, "source_revision": {"v": 1},
         "attrs": {"classification": "internal"}}
        for i in range(8)
    ]
    hidden_ref = source_record_ref("git", {"key": "b"})
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:context", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "deny_hidden", "effect": "deny", "actions": ["*"],
                   "subjects": ["*"], "match": {"is_hidden_ref": True}}],
    })
    session = _session(nomos_access_context={"context_id": "ctx:1", "hidden_refs": [hidden_ref]})

    visible = visible_ids_by_decision_class(_Conn(), session, rows, row_kind="node")

    assert visible == _per_row("node", session, rows)
    assert visible == {f"node:{i}" for i in range(8) if RECORD_KEYS[i % 4] != "b"}


def test_rows_without_source_lineage_are_hidden_without_a_decision(engine: _Engine) -> None:
    rows = [{"node_id": "node:1", "subtype": "doc", "source": None, "attrs": {}}]
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:nolineage", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "a", "effect": "allow", "actions": ["*"], "subjects": ["*"],
                   "match": {}}],
    })

    assert visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node") == set()
    assert engine.decisions == []


def test_no_active_bundle_hides_every_row_as_the_per_row_filter_does(engine: _Engine) -> None:
    rows = [_row(random.Random(i), "edge", f"edge:{i}") for i in range(10)]
    engine.bundle = None

    assert visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="edge") == set()
    # PACT lane-h-nomos-a-part1 (decision 123a): one decision for the call.
    assert len(engine.decisions) == 1
    assert _per_row("edge", _session(), rows) == set()


def _refuse_per_row_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every per-row step of the class path raise if it runs."""
    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("per-row class work ran with no active bundle")

    for name in (
        "_decision_class_key", "_decision_class_key_and_attrs", "_projected_facts",
        "_stable_row_order", "_stable_order_key", "_row_resource", "_fine_grained_targets",
        "_filter_rows",
    ):
        monkeypatch.setattr(read_filter, name, refuse)


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_no_active_bundle_records_one_decision_and_does_no_per_row_work(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch, kind: str, mode: str,
) -> None:
    """PACT lane-h-nomos-a-part1 (decision 123a): with no active bundle the
    call records exactly ONE decision and answers what the per-row filter
    answers -- every row with lineage under audit, none under enforce --
    without projecting, ordering or evaluating any row."""
    engine.mode = mode
    engine.bundle = None
    rng = random.Random(f"no-bundle:{kind}:{mode}")
    rows = _rows(rng, kind, 200)
    expected = _per_row(kind, _session(), rows)
    engine.decisions.clear()
    _refuse_per_row_work(monkeypatch)

    visible = visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind=kind)

    assert visible == expected
    if mode == "audit":
        # Some rows have no lineage: they stay hidden, as on the per-row path.
        assert 0 < len(visible) < len({str(r[f"{kind}_id"]) for r in rows})
    else:
        assert visible == set()
    assert len(engine.decisions) == 1
    decision, target = engine.decisions[0]
    assert decision.reason_code == "nomos.default_deny.no_active_bundle"
    assert decision.effect == "deny"
    assert decision.policy_fingerprint is None
    assert decision.context.get(read_filter.READ_PURPOSE_FACT) == "count"
    assert target["kind"] == f"local_operator_{kind}_no_active_bundle_count"
    assert target["decided_by"] == "no_active_bundle"
    assert target["enforcement_mode"] == mode
    with_lineage = [r for r in rows if read_filter._row_source_ids(r)]
    assert target["rows_decided"] == len(with_lineage)
    assert decision.resource["deployment"] == DEPLOYMENT
    assert decision.resource["kind"] == kind
    # The decision's identity names the mode and this call, so the next call
    # records its own row instead of meeting this one in the insert-once trail.
    assert decision.context[read_filter.NO_BUNDLE_MODE_FACT] == mode
    request = decision.context[read_filter.NO_BUNDLE_REQUEST_FACT]
    assert request.startswith("read-request:") and target["read_request"] == request
    engine.decisions.clear()
    visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind=kind)
    (again, _), = engine.decisions
    assert again.decision_id != decision.decision_id


def test_no_active_bundle_and_no_lineage_records_nothing(engine: _Engine) -> None:
    engine.mode = "audit"
    engine.bundle = None
    rows = [{"node_id": f"node:{i}", "subtype": "doc", "source": None, "attrs": {}}
            for i in range(3)]

    assert visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node") == set()
    assert engine.decisions == []


def test_a_bundle_activated_after_the_no_bundle_read_refuses_it(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The read saw no bundle, but one is active by the time it decides:
    refuse rather than answer the rows from a policy no longer active. (The
    stubbed loaders stand in for the memo; the PostgreSQL test below
    activates a real bundle from a second connection.)"""
    from okg.substrate.errors import SubstrateError

    engine.mode = "audit"
    engine.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:late", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{"rule_id": "a", "effect": "allow", "actions": ["*"], "subjects": ["*"],
                   "match": {}}],
    })
    monkeypatch.setattr(read_filter, "load_active_policy_bundle", lambda conn, **_: None)
    rows = [{"node_id": f"node:{i}", "subtype": "doc", "source": "git",
             "source_record_id": {"key": "a"}, "source_revision": None,
             "attrs": {"classification": "internal"}} for i in range(5)]

    with pytest.raises(SubstrateError, match="no policy bundle was active"):
        visible_ids_by_decision_class(_Conn(), _session(), rows, row_kind="node")


def test_fact_text_writes_the_canonical_json_text() -> None:
    """The projection's scalar shortcut writes exactly what ``json.dumps``
    writes, so class keys (and the read-class ids built from them) are
    unchanged (PACT lane-h-nomos-a-part1)."""
    import enum
    import json

    class Text(str):
        pass

    class Number(enum.IntEnum):
        ONE = 1

    values: list[Any] = [
        "", "plain", 'quote " and \\ backslash', "café ☃ \U0001f600", "\x00\n\t",
        Text("sub"), None, True, False, 0, -7, 10**30, Number.ONE, 1.0, float("nan"),
        float("inf"), [1, "a", None], {"b": 1, "a": [True]}, ("t", 1), object,
    ]
    for value in values:
        expected = json.dumps(value, sort_keys=True, default=read_filter._fact_default)
        assert read_filter._fact_text(value) == expected, value
    with pytest.raises(TypeError):
        read_filter._fact_text({1: "mixed", "k": "keys"})


def test_the_stable_order_key_orders_rows_as_the_whole_row_key() -> None:
    """Writing a row's content only to break an id tie keeps the old order,
    duplicate ids included (PACT lane-h-nomos-a-part1)."""
    for seed in range(40):
        rng = random.Random(f"order:{seed}")
        kind = rng.choice(("node", "edge"))
        rows = _rows(rng, kind, rng.randint(1, 30))
        for _ in range(rng.randint(0, 4)):
            rows.append(_row(rng, kind, str(rng.choice(rows)[f"{kind}_id"])))
        rng.shuffle(rows)
        key = read_filter._stable_order_key(rows)
        new = sorted(range(len(rows)), key=key)
        old = sorted(range(len(rows)), key=lambda p: read_filter._stable_row_order(rows[p]))
        assert new == old, seed
        for positions in (list(range(len(rows))), rng.sample(range(len(rows)), len(rows))):
            assert min(positions, key=key) == min(
                positions, key=lambda p: read_filter._stable_row_order(rows[p])
            )


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_with_a_bundle_the_answer_and_trail_equal_the_unoptimised_class_path(
    engine: _Engine, monkeypatch: pytest.MonkeyPatch, kind: str, mode: str,
) -> None:
    """The cheaper projection (scalar JSON shortcut, id-first order, no attrs
    stamping when only the key is needed) changes neither the visible ids nor
    a single recorded decision or target, against the path as it was."""
    import json

    engine.mode = mode
    session = _session()
    fast = read_filter._fact_text
    fast_order = read_filter._stable_order_key
    fast_key = read_filter._decision_class_key

    def old_fact_text(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=read_filter._fact_default)

    def old_order(rows: list[Any]) -> Any:
        return lambda p: read_filter._stable_row_order(rows[p])

    def old_key(*args: Any, **kwargs: Any) -> Any:
        return read_filter._decision_class_key_and_attrs(*args, **kwargs, stamp_attrs=True)[0]

    for seed in range(60):
        rng = random.Random(f"unoptimised:{kind}:{mode}:{seed}")
        rows = _rows(rng, kind, rng.randint(1, 40))
        engine.bundle = _bundle(rng, kind, rows)
        answers = []
        for fact_text, order, key in ((fast, fast_order, fast_key),
                                      (old_fact_text, old_order, old_key)):
            monkeypatch.setattr(read_filter, "_fact_text", fact_text)
            monkeypatch.setattr(read_filter, "_stable_order_key", order)
            monkeypatch.setattr(read_filter, "_decision_class_key", key)
            engine.decisions.clear()
            visible = visible_ids_by_decision_class(_Conn(), session, rows, row_kind=kind)
            trail = [(d.decision_id, d.effect, json.dumps(t, sort_keys=True, default=repr))
                     for d, t in engine.decisions]
            answers.append((visible, trail))
        assert answers[0] == answers[1], seed


def test_without_the_runtime_every_id_is_visible(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    rows = [{"node_id": "node:1"}, {"node_id": "node:2"}]

    assert visible_ids_by_decision_class(
        _Conn(), _session(), rows, row_kind="node",
    ) == {"node:1", "node:2"}


def test_an_unknown_row_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="row_kind"):
        visible_ids_by_decision_class(_Conn(), _session(), [], row_kind="graph")


# --------------------------------------------------------------------------- #
# Real storage: accepted attribute overlays and the recorded decision trail    #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_on_postgres_overlays_count_and_the_trail_keeps_counts_beside_reads(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing stubbed: the active bundle, apply_effective_attrs_to_rows and the
    decision rows are the real ones. An accepted overlay that reclassifies two
    rows as restricted hides them from both answers, and the trail records the
    count decisions beside the read decisions for the same rows."""
    import psycopg

    from okg.substrate.nomos import activate_policy_bundle

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    # The runtime reset clears neither overlays nor decisions, and a decision
    # row is insert-once by identity: a fresh deployment and generation keep
    # this run's rows apart from any earlier run's.
    run = uuid.uuid4().hex[:12]
    deployment = f"decision-class-{run}"
    generation = f"gen:decision-class:{run}"
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": SOURCES[0],
         "source_record_id": {"key": "a"}, "source_revision": {"v": 1},
         "attrs": {"classification": "internal"}}
        for i in range(8)
    ]
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=compile_policy_graph({
                "policy_bundle_id": "nomos:test:decision-class-db", "policy_version": "v1",
                "default_effect": "deny",
                "rules": [
                    {"rule_id": "allow_rows", "effect": "allow", "actions": ["*"],
                     "subjects": ["*"], "match": {}, "priority": 1},
                    {"rule_id": "deny_restricted", "effect": "deny", "actions": ["*"],
                     "subjects": ["*"], "match": {"classification": "restricted"},
                     "priority": 2},
                ],
            }),
            scope=f"deployment:{deployment}",
            activated_by="test:operator",
            bootstrap=True,
        )
        for i in (2, 5):
            conn.execute(
                "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id,"
                " generation_id, attr_key, observation_id, value, value_hash, producer)"
                " VALUES ('node', %s, %s, 'classification', %s, %s::jsonb, 'h', 'test')",
                (f"node:{i}", generation, f"obs:{i}", '"restricted"'),
            )
        conn.commit()
        session = _session(deployment_name=deployment, generation_id=generation)

        by_class = visible_ids_by_decision_class(conn, session, rows, row_kind="node")
        by_row = {
            str(row["node_id"]) for row in filter_node_rows_for_session(conn, session, rows)
        }
        conn.commit()
        trail = conn.execute(
            "SELECT target->>'node_id', target->>'kind' FROM okg.nomos_decisions"
            " WHERE deployment_name = %s AND target ? 'node_id'"
            " AND target->>'generation_id' = %s",
            (deployment, generation),
        ).fetchall()

    assert by_class == by_row == {f"node:{i}" for i in range(8)} - {"node:2", "node:5"}
    counted = {node for node, kind in trail if kind.endswith("_count")}
    read = {node for node, kind in trail if kind.endswith("_read")}
    # One representative per class, first in id order: node:0 (internal) and
    # node:2 (restricted by the overlay).
    assert counted == {"node:0", "node:2"}
    # Every row was read and decided, the representatives included: the count
    # decision did not take the read decision's place in the insert-once trail.
    assert read == {f"node:{i}" for i in range(8)}


@pytest.mark.postgres
def test_on_postgres_no_active_bundle_writes_one_decision_per_call(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PACT lane-h-nomos-a-part1 (decision 123a), nothing stubbed: with no
    active bundle under audit, a count over many rows and a read of the same
    rows each write ONE decision row, and both answer what the per-row filter
    answers (which writes one decision per row and target)."""
    import psycopg

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    run = uuid.uuid4().hex[:12]
    deployment = f"no-bundle-{run}"
    generation = f"gen:no-bundle:{run}"
    rows = [
        {"node_id": f"node:{i}", "subtype": "doc", "source": SOURCES[i % 3],
         "source_record_id": {"key": f"r{i}"}, "source_revision": {"v": 1},
         "attrs": {"classification": "internal"}}
        for i in range(40)
    ] + [{"node_id": "node:no-lineage", "subtype": "doc", "source": None, "attrs": {}}]
    with psycopg.connect(clean_runtime) as conn:
        session = _session(deployment_name=deployment, generation_id=generation)

        def trail() -> list[tuple[str, str]]:
            conn.commit()
            return conn.execute(
                "SELECT target->>'kind', reason_code FROM okg.nomos_decisions"
                " WHERE deployment_name = %s AND target->>'generation_id' = %s",
                (deployment, generation),
            ).fetchall()

        counted = visible_ids_by_decision_class(conn, session, rows, row_kind="node")
        after_count = trail()
        returned = read_filter.filter_rows_by_read_class(conn, session, rows, row_kind="node")
        after_read = trail()
        by_row = filter_node_rows_for_session(conn, session, rows)
        after_per_row = trail()

    expected = {f"node:{i}" for i in range(40)}
    assert counted == {str(r["node_id"]) for r in by_row} == expected
    assert [r["node_id"] for r in returned] == [r["node_id"] for r in by_row]
    assert [dict(r["attrs"]) for r in returned] == [dict(r["attrs"]) for r in by_row]
    assert after_count == [
        ("local_operator_node_no_active_bundle_count", "nomos.default_deny.no_active_bundle"),
    ]
    assert sorted(after_read) == sorted([
        *after_count,
        ("local_operator_node_no_active_bundle_read", "nomos.default_deny.no_active_bundle"),
    ])
    # The per-row filter, unchanged, still decides every row and target.
    assert len(after_per_row) - len(after_read) >= 40


def _no_bundle_rows(n: int) -> list[dict[str, Any]]:
    return [
        {"node_id": f"node:{i}", "subtype": "doc", "source": SOURCES[i % 3],
         "source_record_id": {"key": f"r{i}"}, "source_revision": {"v": 1},
         "attrs": {"classification": "internal"}}
        for i in range(n)
    ]


def _set_mode(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    from okg.substrate.nomos.runtime_enforcement import _clear_nomos_enforcement_mode_cache

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    _clear_nomos_enforcement_mode_cache()


@pytest.mark.postgres
def test_on_postgres_each_no_bundle_call_records_its_own_mode_and_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 1, finding 1: three calls with no active bundle -- enforce
    over 40 rows, audit over 7, audit over 3 -- write THREE decision rows,
    each naming the mode its rows were decided under and how many it decided.
    (Before the fix the decision identity named neither, so the insert-once
    trail kept only the first: enforce, 40 rows.)"""
    import psycopg

    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    run = uuid.uuid4().hex[:12]
    deployment = f"no-bundle-calls-{run}"
    generation = f"gen:no-bundle-calls:{run}"
    with psycopg.connect(clean_runtime) as conn:
        session = _session(deployment_name=deployment, generation_id=generation)
        answers = []
        for mode, n in (("enforce", 40), ("audit", 7), ("audit", 3)):
            _set_mode(monkeypatch, mode)
            answers.append(visible_ids_by_decision_class(
                conn, session, _no_bundle_rows(n), row_kind="node",
            ))
        conn.commit()
        trail = conn.execute(
            "SELECT target->>'enforcement_mode', (target->>'rows_decided')::int,"
            " context->>%s, context->>%s, reason_code, decision_id"
            " FROM okg.nomos_decisions WHERE deployment_name = %s ORDER BY created_at",
            (read_filter.NO_BUNDLE_MODE_FACT, read_filter.NO_BUNDLE_REQUEST_FACT, deployment),
        ).fetchall()

    assert answers == [set(), {f"node:{i}" for i in range(7)}, {f"node:{i}" for i in range(3)}]
    assert sorted((mode, n, ctx_mode) for mode, n, ctx_mode, *_ in trail) == sorted([
        ("enforce", 40, "enforce"), ("audit", 7, "audit"), ("audit", 3, "audit"),
    ])
    assert {reason for *_, reason, _ in trail} == {"nomos.default_deny.no_active_bundle"}
    assert len({request for *_, request, _, _ in trail}) == 3
    assert len({decision_id for *_, decision_id in trail}) == 3


@pytest.mark.postgres
def test_on_postgres_a_bundle_activated_mid_call_refuses_the_read(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 1, finding 2, nothing stubbed: the call reads no active
    bundle (memoized for its pass), then a second connection activates a
    deny-all bundle and commits before the call decides. The call refuses
    with SubstrateError and records no no-bundle decision."""
    import psycopg

    from okg.substrate.errors import SubstrateError
    from okg.substrate.nomos import activate_policy_bundle

    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    _set_mode(monkeypatch, "audit")
    run = uuid.uuid4().hex[:12]
    deployment = f"no-bundle-late-{run}"
    generation = f"gen:no-bundle-late:{run}"
    activated: list[str] = []
    real_source_ids = read_filter._row_source_ids

    def activate_once_then_read(row: Any) -> Any:
        # Runs after the call loaded (and memoized) "no bundle", before it decides.
        if not activated:
            with psycopg.connect(clean_runtime) as other:
                activated.append(activate_policy_bundle(
                    other,
                    deployment_name=deployment,
                    bundle=compile_policy_graph({
                        "policy_bundle_id": "nomos:test:late-deny-all", "policy_version": "v1",
                        "default_effect": "deny",
                        "rules": [{"rule_id": "deny_all", "effect": "deny", "actions": ["*"],
                                   "subjects": ["*"], "match": {}}],
                    }),
                    scope=f"deployment:{deployment}",
                    activated_by="test:operator",
                    bootstrap=True,
                ))
                other.commit()
        return real_source_ids(row)

    monkeypatch.setattr(read_filter, "_row_source_ids", activate_once_then_read)
    with psycopg.connect(clean_runtime) as conn:
        session = _session(deployment_name=deployment, generation_id=generation)
        with pytest.raises(SubstrateError, match="one is active now"):
            visible_ids_by_decision_class(conn, session, _no_bundle_rows(5), row_kind="node")
        conn.rollback()
        no_bundle_rows = conn.execute(
            "SELECT count(*) FROM okg.nomos_decisions WHERE deployment_name = %s"
            " AND reason_code = 'nomos.default_deny.no_active_bundle'",
            (deployment,),
        ).fetchone()[0]

    assert activated
    assert no_bundle_rows == 0

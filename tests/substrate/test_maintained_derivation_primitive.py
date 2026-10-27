"""Governance-kit tests for the maintained-derivation primitive (#444 slice 1).

Pure (no DB): exercises the consumer-agnostic core -- the parity oracle (incl.
drift), the self-heal cadence arithmetic, the forcing-reason precedence (all five
reasons), the sentinel round-trip, and the loud-fail-on-absent-sentinel read. The
byte-identical proof against the real ontology rollup is the Postgres golden-fixture
replay (step 7); this file pins the kit's contract in isolation.
"""
from __future__ import annotations

import pytest

from okg.substrate.derived_artifacts.maintained_derivation import (
    CorrectnessOracle,
    ForceInputs,
    ForcePredicate,
    MaintainedDerivationUnavailable,
    ProjectionParityOracle,
    SentinelSpec,
    cadence_due,
    decide_maintain_mode,
    is_sentinel_row,
    partition_sentinel_rows,
    read_self_heal_cadence,
    sentinel_row_dict,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    SENTINEL_TOKEN,
)


class _FakeCheckout:
    """A checkout whose ``query`` returns canned rows for the oracle's recompute."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def query(self, _sql: str, _params: dict | None = None) -> list[dict]:
        return list(self._rows)


# --------------------------------------------------------------------------- #
# Forcing-reason precedence (all five reasons, exact order)                   #
# --------------------------------------------------------------------------- #
def _inputs(*, force_full=False, scope_available=True, change=None) -> ForceInputs:
    return ForceInputs(change_inputs=change, force_full=force_full, scope_available=scope_available)


_RETYPE = ForcePredicate("node_subtype_changed", "node_retype", lambda i: bool(i.change_inputs))


def test_maintained_derivation_primitive_force_reason_each():
    # 1. forced
    m = decide_maintain_mode(has_prior=True, inputs=_inputs(force_full=True),
                             force_full_predicates=[_RETYPE], elapsed=0, self_heal_every=25)
    assert m == (True, "forced")
    # 2. scope_unavailable
    m = decide_maintain_mode(has_prior=True, inputs=_inputs(scope_available=False),
                             force_full_predicates=[_RETYPE], elapsed=0, self_heal_every=25)
    assert m == (True, "scope_unavailable")
    # 3. first_publish
    m = decide_maintain_mode(has_prior=False, inputs=_inputs(),
                             force_full_predicates=[_RETYPE], elapsed=None, self_heal_every=25)
    assert m == (True, "first_publish")
    # 4. a force predicate (node_retype)
    m = decide_maintain_mode(has_prior=True, inputs=_inputs(change="retype"),
                             force_full_predicates=[_RETYPE], elapsed=0, self_heal_every=25)
    assert m == (True, "node_retype")
    # 5. periodic_self_heal
    m = decide_maintain_mode(has_prior=True, inputs=_inputs(),
                             force_full_predicates=[_RETYPE], elapsed=24, self_heal_every=25)
    assert m == (True, "periodic_self_heal")
    # else: incremental
    m = decide_maintain_mode(has_prior=True, inputs=_inputs(),
                             force_full_predicates=[_RETYPE], elapsed=0, self_heal_every=25)
    assert m == (False, None)


def test_maintained_derivation_primitive_force_reason_precedence():
    # forced beats everything (even with every other trigger active)
    m = decide_maintain_mode(has_prior=False, inputs=_inputs(force_full=True, scope_available=False, change="retype"),
                             force_full_predicates=[_RETYPE], elapsed=None, self_heal_every=25)
    assert m.reason == "forced"
    # scope_unavailable beats first_publish + predicate + cadence
    m = decide_maintain_mode(has_prior=False, inputs=_inputs(scope_available=False, change="retype"),
                             force_full_predicates=[_RETYPE], elapsed=None, self_heal_every=25)
    assert m.reason == "scope_unavailable"
    # first_publish beats predicate + cadence
    m = decide_maintain_mode(has_prior=False, inputs=_inputs(change="retype"),
                             force_full_predicates=[_RETYPE], elapsed=24, self_heal_every=25)
    assert m.reason == "first_publish"
    # predicate beats cadence; FIRST matching predicate wins
    p1 = ForcePredicate("p1", "reason_one", lambda i: True)
    p2 = ForcePredicate("p2", "reason_two", lambda i: True)
    m = decide_maintain_mode(has_prior=True, inputs=_inputs(change="x"),
                             force_full_predicates=[p1, p2], elapsed=24, self_heal_every=25)
    assert m.reason == "reason_one"


# --------------------------------------------------------------------------- #
# Cadence arithmetic                                                          #
# --------------------------------------------------------------------------- #
def test_maintained_derivation_primitive_cadence_due():
    assert cadence_due(None, 25) is True          # no counter -> due
    assert cadence_due(24, 25) is True            # (24+1) >= 25
    assert cadence_due(23, 25) is False           # (23+1) = 24 < 25
    assert cadence_due(0, 25) is False
    assert cadence_due(5, 0) is False             # non-positive cadence disabled
    assert cadence_due(None, 0) is False


# --------------------------------------------------------------------------- #
# Large change set bound (issue 3955)                                         #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("changed", "prior_total", "percent", "expected"),
    [
        (9_999, 0, 10, False),        # one below the floor, even on an empty prior
        (10_000, 0, 10, True),        # at the floor, prior total 0
        (10_000, -5, 10, True),       # a negative total counts as 0
        (10_000, 100_000, 10, False), # exactly 10 % is not large
        (10_001, 100_000, 10, True),  # just over 10 %
        (10_000, 99_999, 10, True),   # just over 10 % from the other side
        (10_000, 100_001, 10, False), # just under 10 %
        (15_000, 100_000, 15, False), # the ontology bound, exactly
        (15_001, 100_000, 15, True),
        (10_000, 200_000, 5, False),  # the console bound, exactly
        (10_001, 200_000, 5, True),
        (9_999, 10, 5, False),        # below the floor however small the graph
    ],
)
def test_is_large_change_set_edges(changed, prior_total, percent, expected):
    from okg.substrate.derived_artifacts.maintained_derivation import (
        LARGE_CHANGE_SET_MIN_IDS,
        is_large_change_set,
    )

    assert LARGE_CHANGE_SET_MIN_IDS == 10_000
    assert is_large_change_set(changed, prior_total, percent=percent) is expected


def test_rollup_large_change_bounds_are_the_measured_ones():
    """The per-rollup bounds the code comments justify (issue 3955): ontology
    15 % (crossover 10-20 % measured), console 5 % (crossover just under 5 %)."""
    from okg.substrate.publisher import console_count_rollup, ontology_rollup

    assert ontology_rollup.LARGE_CHANGE_SET_PERCENT == 15
    assert console_count_rollup.LARGE_CHANGE_SET_PERCENT == 5


# --------------------------------------------------------------------------- #
# Self-heal cadence resolution (warn-and-default, never silently disabled)    #
# --------------------------------------------------------------------------- #
def test_maintained_derivation_primitive_self_heal_cadence(monkeypatch):
    monkeypatch.delenv("OKG_TEST_CADENCE", raising=False)
    assert read_self_heal_cadence("OKG_TEST_CADENCE", default=25) == 25
    monkeypatch.setenv("OKG_TEST_CADENCE", "7")
    assert read_self_heal_cadence("OKG_TEST_CADENCE", default=25) == 7
    monkeypatch.setenv("OKG_TEST_CADENCE", "nope")          # non-integer -> default
    assert read_self_heal_cadence("OKG_TEST_CADENCE", default=25) == 25
    monkeypatch.setenv("OKG_TEST_CADENCE", "0")             # non-positive -> default
    assert read_self_heal_cadence("OKG_TEST_CADENCE", default=25) == 25
    monkeypatch.setenv("OKG_TEST_CADENCE", "-3")
    assert read_self_heal_cadence("OKG_TEST_CADENCE", default=25) == 25


# --------------------------------------------------------------------------- #
# Correctness oracle: parity + drift injection                                #
# --------------------------------------------------------------------------- #
def test_maintained_derivation_primitive_correctness_oracle_and_drift():
    oracle = CorrectnessOracle("SELECT k, v FROM t", key_columns=["k"], value_columns=["v"])
    ground = oracle.recompute(_FakeCheckout([{"k": "a", "v": 1}, {"k": "b", "v": 2}]))
    assert ground == {("a",): (1,), ("b",): (2,)}

    # identical kept -> ok
    res = oracle.diff(dict(ground), ground)
    assert res.ok and not (res.missing_in_kept or res.extra_in_kept or res.mismatched)

    # drift: a corrupted kept value (sentinel present elsewhere, value wrong) is caught
    drifted = {("a",): (999,), ("b",): (2,)}
    res = oracle.diff(drifted, ground)
    assert not res.ok and res.mismatched == [("a",)]

    # kept missing a key / has an extra key
    res = oracle.diff({("a",): (1,)}, ground)
    assert not res.ok and res.missing_in_kept == [("b",)]
    res = oracle.diff({**ground, ("c",): (3,)}, ground)
    assert not res.ok and res.extra_in_kept == [("c",)]


def test_maintained_derivation_primitive_projection_parity_oracle_shape():
    # The judgment-class oracle is callable on a synthetic frozen output: it
    # reprojects (never re-runs the producer) and diffs against the kept rows.
    def reproject(frozen, _checkout):
        return {(r["k"],): (r["v"],) for r in frozen}

    oracle = ProjectionParityOracle(reproject, key_columns=["k"])
    frozen = [{"k": "a", "v": 1}, {"k": "b", "v": 2}]
    kept = {("a",): (1,), ("b",): (2,)}
    assert oracle.diff(kept, frozen, checkout=None).ok
    assert not oracle.diff({("a",): (1,)}, frozen, checkout=None).ok


# --------------------------------------------------------------------------- #
# Sentinel round-trip (schema-agnostic) + loud-fail read partition            #
# --------------------------------------------------------------------------- #
def test_maintained_derivation_primitive_sentinel_roundtrip():
    spec2 = SentinelSpec(pk_columns=["dimension_kind", "dimension_value"], cadence_column="item_count")
    row = sentinel_row_dict(spec2, 3)
    assert row == {"dimension_kind": SENTINEL_TOKEN, "dimension_value": SENTINEL_TOKEN, "item_count": 3}
    assert is_sentinel_row(row, spec2)
    assert not is_sentinel_row({"dimension_kind": SENTINEL_TOKEN, "dimension_value": "x", "item_count": 0}, spec2)

    # the 4-column rollup PK works through the same schema-agnostic helpers
    spec4 = SentinelSpec(pk_columns=["src_subtype", "edge_type", "dst_subtype", "producer"], cadence_column="edge_count")
    row4 = sentinel_row_dict(spec4, 0)
    assert is_sentinel_row(row4, spec4)


def test_maintained_derivation_primitive_read_partition_and_loud_fail():
    spec = SentinelSpec(pk_columns=["k"], cadence_column="cnt")
    sentinel = {"k": SENTINEL_TOKEN, "cnt": 4}
    data = {"k": "a", "cnt": 5}

    # sentinel present + data rows -> mapped data rows
    out = partition_sentinel_rows([sentinel, data], sentinel_spec=spec, row_mapper=lambda r: r["k"], generation_id=7)
    assert out == ["a"]

    # maintained-but-empty (sentinel only) -> []
    assert partition_sentinel_rows([sentinel], sentinel_spec=spec, row_mapper=lambda r: r["k"], generation_id=7) == []

    # NO sentinel -> loud failure, never a silent empty result
    with pytest.raises(MaintainedDerivationUnavailable) as exc:
        partition_sentinel_rows([data], sentinel_spec=spec, row_mapper=lambda r: r["k"], generation_id=7)
    assert exc.value.reason == "no_sentinel"
    assert exc.value.generation_id == 7


# --------------------------------------------------------------------------- #
# Dedup: ONE definition of the sentinel + producer constants                  #
# --------------------------------------------------------------------------- #
def test_maintained_derivation_primitive_constants_single_definition():
    """The write path, the read path, and the import-light constants module share the
    SAME object for every shared constant -- one definition, so they cannot drift (the
    de-dup that replaced the hand-duplicated read-side copy + its sync test)."""
    from okg.substrate.console_views import ontology_rollup_read as r
    from okg.substrate.derived_artifacts import maintained_derivation_constants as c
    from okg.substrate.publisher import ontology_rollup as w

    assert w.UNATTRIBUTED_DERIVED_PRODUCER is c.UNATTRIBUTED_DERIVED_PRODUCER
    assert r.UNATTRIBUTED_DERIVED_PRODUCER is c.UNATTRIBUTED_DERIVED_PRODUCER
    assert w.PUBLISHER_DERIVED_SOURCE is c.PUBLISHER_DERIVED_SOURCE
    assert r._ENRICHER_PREFIX is c.ENRICHER_PREFIX
    # The write path still references the sentinel token (its _current_rollup_rows
    # filter); the read path no longer carries one at all -- it delegates sentinel
    # detection to the kit -- so only the write side is asserted here.
    assert w._SENTINEL_TOKEN is c.SENTINEL_TOKEN
    assert not hasattr(r, "_SENTINEL_TOKEN")


def test_maintained_derivation_primitive_constants_read_path_import_light():
    """The read path must not import graph_store at MODULE level -- it shares the
    constants via the import-light module and defers its own graph_store import into the
    function body. Parsed from source so a new top-level graph_store import fails loud."""
    import ast
    import inspect

    from okg.substrate.console_views import ontology_rollup_read

    tree = ast.parse(inspect.getsource(ontology_rollup_read))
    offenders: list[str] = []
    for node in tree.body:  # module-level statements only
        if isinstance(node, ast.Import):
            offenders += [n.name for n in node.names if "graph_store" in n.name]
        elif isinstance(node, ast.ImportFrom) and node.module and "graph_store" in node.module:
            offenders.append(node.module)
    assert not offenders, f"read path has a module-level graph_store import: {offenders}"


def test_maintained_derivation_primitive_kit_module_import_light():
    """The KIT module itself must not import graph_store at MODULE level -- it defers
    its two graph_store imports (read_maintained_rows, maintain) into function bodies.
    The read-path guard above is only sound if THIS holds: the read path imports the kit,
    so a hoisted graph_store import in the kit would transitively pollute the read path's
    module-level imports while the read-path-only guard still passed."""
    import ast
    import inspect

    from okg.substrate.derived_artifacts import maintained_derivation

    tree = ast.parse(inspect.getsource(maintained_derivation))
    offenders: list[str] = []
    for node in tree.body:  # module-level statements only
        if isinstance(node, ast.Import):
            offenders += [n.name for n in node.names if "graph_store" in n.name]
        elif isinstance(node, ast.ImportFrom) and node.module and "graph_store" in node.module:
            offenders.append(node.module)
    assert not offenders, f"kit module has a module-level graph_store import: {offenders}"

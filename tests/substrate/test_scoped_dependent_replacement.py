"""Issue #1539: the rule for an out-of-scope replacement key, database-free.

A narrowed run may replace a record outside its sync_scope only when the
runner can verify, against the durable prior, that the replacement keeps
the record's nodes and adds or removes only `references`/`calls` edges whose
target symbol lives in a file the scope names. These tests pin the rule
itself; the runner tests in test_source_runner_record_set.py pin where it
is enforced.
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.runner import (
    SCOPED_DEPENDENT_EDGE_TYPES,
    SourceRunner,
    review_scoped_dependent_replacements,
    scoped_dependent_edge_qualifies,
)
from okg.substrate.library.sources.base import (
    EdgeKey,
    RecordEmission,
    RecordNode,
    RecordSet,
)


SCOPE = ["pkg/utils.py"]
OLD_TARGET = "symbol:tinydb:pkg/utils.py:pkg.utils.freeze:10"
NEW_TARGET = "symbol:tinydb:pkg/utils.py:pkg.utils.freeze:14"
REF = "reference:abc123"
CALLER = "symbol:tinydb:pkg/queries.py:pkg.queries.Query.test:40"
OTHER_TARGET = "symbol:tinydb:pkg/storages.py:pkg.storages.Storage:5"
# pkg/table.py imports pkg/utils.py; pkg/queries.py imports pkg/table.py.
TWO_HOP_TARGET = "symbol:tinydb:pkg/table.py:pkg.table.Table:20"


def _edges(*triples: tuple[str, str, str]) -> list[EdgeKey]:
    return [
        EdgeKey(src=src, edge_type=edge_type, dst=dst)
        for src, edge_type, dst in triples
    ]


def _records(**by_key: RecordEmission) -> RecordSet:
    return RecordSet(records={
        key.replace("__", "/") + ".py": emission
        for key, emission in by_key.items()
    })


def _dependent_prior() -> RecordSet:
    return _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", OLD_TARGET),
        (CALLER, "calls", OLD_TARGET),
        (REF, "references", OTHER_TARGET),
    )))


def _review(prior: RecordSet, current: RecordSet, *keys: str):
    return review_scoped_dependent_replacements(
        prior=prior,
        current=current,
        keys=keys or ["pkg/queries.py"],
        scope_keys=SCOPE,
    )


def test_dependent_repointing_its_edges_into_scope_is_verified() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", NEW_TARGET),
        (CALLER, "calls", NEW_TARGET),
        (REF, "references", OTHER_TARGET),
    )))

    review = _review(_dependent_prior(), current)

    assert review.unverified == {}
    assert review.dependents == {"pkg/queries.py"}


def test_attribute_change_on_a_kept_edge_is_verified() -> None:
    prior = _records(pkg__queries=RecordEmission(edges=[
        EdgeKey(src=REF, edge_type="references", dst=OTHER_TARGET,
                attrs={"source_frontier": "old"}),
    ]))
    current = _records(pkg__queries=RecordEmission(edges=[
        EdgeKey(src=REF, edge_type="references", dst=OTHER_TARGET,
                attrs={"source_frontier": "new"}),
    ]))

    assert _review(prior, current).unverified == {}


def test_unchanged_dependent_whose_edge_was_already_removed_is_a_noop() -> None:
    record = RecordEmission(edges=_edges((REF, "references", OTHER_TARGET)))
    review = _review(
        _records(pkg__queries=record),
        _records(pkg__queries=record),
    )

    assert review.unverified == {}
    assert review.dependents == frozenset()


def test_edge_into_a_file_deleted_in_the_same_run_may_be_retired() -> None:
    """The deleted file is still a scope key, so its edges qualify."""
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", OTHER_TARGET),
    )))

    assert _review(_dependent_prior(), current).unverified == {}


def test_unrelated_record_emptied_is_refused() -> None:
    prior = _records(pkg__storages=RecordEmission(edges=_edges(
        (REF, "references", OTHER_TARGET),
    )))

    review = _review(prior, RecordSet(), "pkg/storages.py")

    assert list(review.unverified) == ["pkg/storages.py"]
    assert review.dependents == frozenset()


def test_qualifying_edge_types_are_exactly_references_and_calls() -> None:
    """Widening this set widens what a scoped run may delete."""
    assert SCOPED_DEPENDENT_EDGE_TYPES == frozenset({"references", "calls"})


@pytest.mark.parametrize(
    "edge_type",
    ["contains", "imports", "defines", "member_of"],
)
def test_non_qualifying_edge_type_into_scope_is_refused(edge_type: str) -> None:
    prior = _records(pkg__queries=RecordEmission(edges=_edges(
        ("file:pkg/queries.py", edge_type, OLD_TARGET),
    )))

    review = _review(prior, RecordSet())

    assert list(review.unverified) == ["pkg/queries.py"]
    assert f"-{edge_type}->" in review.unverified["pkg/queries.py"][0]
    assert review.dependents == frozenset()


def test_missing_prior_with_a_new_node_is_refused() -> None:
    current = _records(pkg__queries=RecordEmission(
        primary=RecordNode("file:pkg/queries.py", "code_file"),
        edges=_edges((REF, "references", NEW_TARGET)),
    ))

    review = _review(RecordSet(), current)

    assert review.unverified["pkg/queries.py"] == ["record nodes change"]


def test_missing_prior_with_a_non_qualifying_edge_is_refused() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", NEW_TARGET),
        (REF, "references", OTHER_TARGET),
    )))

    review = _review(RecordSet(), current)

    assert list(review.unverified) == ["pkg/queries.py"]
    assert review.unverified["pkg/queries.py"][0].startswith("adds edge")
    assert OTHER_TARGET in review.unverified["pkg/queries.py"][0]


def test_missing_prior_with_only_qualifying_edges_is_verified() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", NEW_TARGET),
        (CALLER, "calls", NEW_TARGET),
    )))

    review = _review(RecordSet(), current)

    assert review.unverified == {}
    assert review.dependents == frozenset()


def test_claim_two_hops_away_is_refused() -> None:
    prior = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", TWO_HOP_TARGET),
    )))

    review = _review(prior, RecordSet())

    assert list(review.unverified) == ["pkg/queries.py"]
    assert review.dependents == frozenset()


def test_dependent_that_also_drops_an_out_of_scope_edge_is_refused() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", NEW_TARGET),
        (CALLER, "calls", NEW_TARGET),
    )))

    review = _review(_dependent_prior(), current)

    assert list(review.unverified) == ["pkg/queries.py"]
    assert review.dependents == {"pkg/queries.py"}
    assert OTHER_TARGET in review.unverified["pkg/queries.py"][0]


def test_added_edge_outside_scope_is_refused() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", OLD_TARGET),
        (CALLER, "calls", OLD_TARGET),
        (REF, "references", OTHER_TARGET),
        (REF, "references", TWO_HOP_TARGET),
    )))

    review = _review(_dependent_prior(), current)

    assert list(review.unverified) == ["pkg/queries.py"]
    assert review.unverified["pkg/queries.py"][0].startswith("adds edge")


def test_node_change_is_refused() -> None:
    prior = _records(pkg__queries=RecordEmission(
        primary=RecordNode("file:pkg/queries.py", "code_file"),
        edges=_edges((REF, "references", OLD_TARGET)),
    ))
    current = _records(pkg__queries=RecordEmission(
        edges=_edges((REF, "references", NEW_TARGET)),
    ))

    review = _review(prior, current)

    assert review.unverified["pkg/queries.py"] == ["record nodes change"]


@pytest.mark.parametrize(
    "dst",
    [
        # Not a symbol id at all.
        "file:pkg/utils.py",
        # No line number.
        "symbol:tinydb:pkg/utils.py:pkg.utils.freeze",
        # A colon inside the path splits it; the path read is "pkg".
        "symbol:tinydb:pkg:utils.py:pkg.utils.freeze:3",
        # Scope-key prefix without a path separator.
        "symbol:tinydb:pkg/utils.pyc:pkg.utils.freeze:3",
    ],
)
def test_malformed_or_near_miss_symbol_id_does_not_qualify(dst: str) -> None:
    edge = EdgeKey(src=REF, edge_type="references", dst=dst)

    assert not scoped_dependent_edge_qualifies(edge, SCOPE)


def test_directory_scope_key_governs_symbols_beneath_it() -> None:
    edge = EdgeKey(src=REF, edge_type="calls", dst=NEW_TARGET)

    assert scoped_dependent_edge_qualifies(edge, ["pkg"])
    assert not scoped_dependent_edge_qualifies(edge, ["pk"])


def _verify(prior: RecordSet, current: RecordSet, *keys: str) -> None:
    runner = SourceRunner.__new__(SourceRunner)
    runner.source_name = "code_lsp_resolution"
    runner._verify_scoped_replacement_keys_within_scope(
        sync_scope={"scope_keys": SCOPE},
        outside_keys=list(keys),
        prior_record_set=prior,
        declared_record_set=current,
    )


def test_runner_verifier_accepts_a_verified_dependent() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", NEW_TARGET),
        (CALLER, "calls", NEW_TARGET),
        (REF, "references", OTHER_TARGET),
    )))

    _verify(_dependent_prior(), current, "pkg/queries.py")


def test_runner_verifier_keeps_the_0922_code_for_unrelated_keys() -> None:
    prior = _records(pkg__storages=RecordEmission(edges=_edges(
        (REF, "references", OTHER_TARGET),
    )))

    with pytest.raises(SubstrateError) as excinfo:
        _verify(prior, RecordSet(), "pkg/storages.py")

    assert excinfo.value.data["reason_code"] == (
        "scoped_replacement_keys_outside_scope"
    )
    assert excinfo.value.data["out_of_scope_replacement_keys"] == [
        "pkg/storages.py",
    ]


def test_runner_verifier_names_an_overreaching_dependent() -> None:
    current = _records(pkg__queries=RecordEmission(edges=_edges(
        (REF, "references", NEW_TARGET),
    )))

    with pytest.raises(SubstrateError) as excinfo:
        _verify(_dependent_prior(), current, "pkg/queries.py")

    assert excinfo.value.data["reason_code"] == (
        "scoped_dependent_replacement_unverified"
    )
    assert excinfo.value.data["unverified_dependent_keys"] == [
        "pkg/queries.py",
    ]

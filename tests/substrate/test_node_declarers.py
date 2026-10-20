"""Shared-node declarer decisions without a database (issue #1901).

The PostgreSQL rows in ``test_chronos_graph_store.py`` prove the writer wiring,
the fence and the races; these rows pin the decision table itself.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.graph_store_internal.node_declarers import (
    choose_survivor,
    declaration_from_upsert,
    declaration_unchanged,
    load_declarations,
    node_row_from_declaration,
    reconcile_node_declarers,
)

NODE = "author:x@example.org"


def _upsert(source: str, *, seen_in: str | None = None) -> dict[str, Any]:
    return {
        "node_id": NODE,
        "subtype": "git_author",
        "attrs": {"email": "x@example.org", "seen_in": seen_in or source},
        "text_hash": None,
        "source": source,
        "source_record_id": {"author": "x@example.org", "repo": source},
        "source_revision": {"commit": f"{source}-head"},
        "run_id": f"run-{source}",
        "observed_at": None,
        "_ownership_mode": "substitute",
    }


def _declaration(source: str, **kwargs: Any) -> dict[str, Any]:
    return declaration_from_upsert(_upsert(source, **kwargs))


def _row(source: str) -> dict[str, Any]:
    return {"node_id": NODE, "source": source, "subtype": "git_author"}


def _declaration_as(source: str, subtype: str) -> dict[str, Any]:
    return dict(_declaration(source), subtype=subtype)


def test_survivor_is_chosen_among_copies_with_the_live_subtype() -> None:
    """The lowest-named declarer holds a copy under another subtype; the next
    one matches the live row, so the node passes to it instead of being
    deleted."""
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={
            NODE: [
                _declaration_as("repo.beta", "legacy_author"),
                _declaration("repo.gamma"),
            ],
        },
    )
    assert [row["source"] for row in outcome.handoffs] == ["repo.gamma"]
    assert outcome.node_deletes == []
    assert outcome.refused_subtype == []


def test_no_copy_with_the_live_subtype_deletes_and_counts_the_refusal() -> None:
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration_as("repo.beta", "legacy_author")]},
    )
    assert outcome.handoffs == []
    assert outcome.node_deletes == [{"node_id": NODE}]
    assert outcome.refused_subtype == [NODE]


def test_owner_retraction_hands_the_row_to_the_surviving_declarer() -> None:
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration("repo.alpha"), _declaration("repo.beta")]},
    )
    assert outcome.node_deletes == []
    assert [row["source"] for row in outcome.handoffs] == ["repo.beta"]
    handoff = outcome.handoffs[0]
    assert handoff["attrs"]["seen_in"] == "repo.beta"
    assert handoff["source_record_id"] == {
        "author": "x@example.org", "repo": "repo.beta",
    }
    assert outcome.declaration_deletes == [
        {"node_id": NODE, "source": "repo.alpha"},
    ]
    assert outcome.declaration_upserts == []


def test_owner_retraction_hands_off_even_when_its_own_declaration_was_never_recorded() -> None:
    """A declaration made before the table existed is simply absent."""
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration("repo.beta")]},
    )
    assert [row["source"] for row in outcome.handoffs] == ["repo.beta"]
    assert outcome.declaration_deletes == []


def test_last_declarer_retraction_deletes_the_row() -> None:
    outcome = reconcile_node_declarers(
        source="repo.beta",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.beta")},
        declarations={NODE: [_declaration("repo.beta")]},
    )
    assert outcome.node_deletes == [{"node_id": NODE}]
    assert outcome.handoffs == []
    assert outcome.declaration_deletes == [{"node_id": NODE, "source": "repo.beta"}]


def test_owner_with_no_recorded_declarers_deletes_as_before() -> None:
    """The disclosed limit: nothing recorded means nobody to pass it to."""
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={},
    )
    assert outcome.node_deletes == [{"node_id": NODE}]
    assert outcome.handoffs == []


def test_non_owner_retraction_only_withdraws_its_declaration() -> None:
    outcome = reconcile_node_declarers(
        source="repo.beta",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration("repo.alpha"), _declaration("repo.beta")]},
    )
    assert outcome.node_deletes == []
    assert outcome.handoffs == []
    assert outcome.declaration_deletes == [{"node_id": NODE, "source": "repo.beta"}]


def test_retraction_of_an_absent_row_changes_no_graph_row() -> None:
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={},
        declarations={NODE: [_declaration("repo.alpha")]},
    )
    assert outcome.node_deletes == []
    assert outcome.handoffs == []
    assert outcome.declaration_deletes == [{"node_id": NODE, "source": "repo.alpha"}]


def test_unattributed_retraction_deletes_the_row_and_every_declaration() -> None:
    outcome = reconcile_node_declarers(
        source=None,
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[_upsert("repo.alpha")],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration("repo.alpha"), _declaration("repo.beta")]},
    )
    assert outcome.node_deletes == [{"node_id": NODE}]
    assert outcome.handoffs == []
    assert sorted(row["source"] for row in outcome.declaration_deletes) == [
        "repo.alpha", "repo.beta",
    ]
    assert outcome.declaration_upserts == []


def test_purge_deletes_the_owned_row_and_every_declaration() -> None:
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration("repo.alpha"), _declaration("repo.beta")]},
        purge=True,
    )
    assert outcome.node_deletes == [{"node_id": NODE}]
    assert outcome.handoffs == []
    assert sorted(row["source"] for row in outcome.declaration_deletes) == [
        "repo.alpha", "repo.beta",
    ]


def test_same_write_retract_and_upsert_is_left_to_the_writer() -> None:
    outcome = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[NODE],
        substitute_rows=[_upsert("repo.alpha")],
        current_nodes={NODE: _row("repo.alpha")},
        declarations={NODE: [_declaration("repo.alpha")]},
    )
    assert outcome.node_deletes == []
    assert outcome.handoffs == []
    assert outcome.declaration_deletes == []
    assert outcome.declaration_upserts == []


def test_three_declarers_pass_the_node_in_source_order() -> None:
    declarations = {
        NODE: [
            _declaration("repo.gamma"),
            _declaration("repo.alpha"),
            _declaration("repo.beta"),
        ],
    }
    first = reconcile_node_declarers(
        source="repo.alpha",
        retracted_node_ids=[NODE],
        still_declared_node_ids=[],
        substitute_rows=[],
        current_nodes={NODE: _row("repo.alpha")},
        declarations=declarations,
    )
    assert [row["source"] for row in first.handoffs] == ["repo.beta"]


def test_substitute_declaration_is_recorded_once_and_replay_writes_nothing() -> None:
    first = reconcile_node_declarers(
        source="repo.beta",
        retracted_node_ids=[],
        still_declared_node_ids=[NODE],
        substitute_rows=[_upsert("repo.beta")],
        current_nodes={},
        declarations={},
    )
    assert first.declaration_upserts == [_declaration("repo.beta")]

    replay = reconcile_node_declarers(
        source="repo.beta",
        retracted_node_ids=[],
        still_declared_node_ids=[NODE],
        substitute_rows=[_upsert("repo.beta")],
        current_nodes={},
        declarations={NODE: [_declaration("repo.beta")]},
    )
    assert replay.declaration_upserts == []

    changed = reconcile_node_declarers(
        source="repo.beta",
        retracted_node_ids=[],
        still_declared_node_ids=[NODE],
        substitute_rows=[_upsert("repo.beta", seen_in="renamed")],
        current_nodes={},
        declarations={NODE: [_declaration("repo.beta")]},
    )
    assert [row["attrs"]["seen_in"] for row in changed.declaration_upserts] == [
        "renamed",
    ]


def test_declaration_change_ignores_only_the_source_revision() -> None:
    recorded = _declaration("repo.beta")
    moved_revision = dict(recorded, source_revision={"commit": "later"})
    assert declaration_unchanged(moved_revision, recorded)
    moved_record = dict(recorded, source_record_id={"author": "other"})
    assert not declaration_unchanged(moved_record, recorded)
    assert not declaration_unchanged(recorded, None)


def test_choose_survivor_excludes_the_leaving_source() -> None:
    declarations = [_declaration("repo.beta"), _declaration("repo.alpha")]
    assert choose_survivor(declarations, excluding=set())["source"] == "repo.alpha"
    assert choose_survivor(declarations, excluding={"repo.alpha"})["source"] == "repo.beta"
    assert choose_survivor(declarations, excluding={"repo.alpha", "repo.beta"}) is None


def test_node_row_round_trips_a_declaration() -> None:
    upsert = _upsert("repo.beta")
    row = node_row_from_declaration(declaration_from_upsert(upsert))
    for key in (
        "node_id", "subtype", "attrs", "text_hash", "source",
        "source_record_id", "source_revision", "run_id", "observed_at",
    ):
        assert row[key] == upsert[key], key
    assert row["embedding"] is None and row["embedding_model"] is None


class _FakeSession:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        wanted = set(params["node_ids"])
        return [row for row in self.rows if row["node_id"] in wanted]


def test_load_declarations_groups_by_node_and_skips_an_empty_lookup() -> None:
    session = _FakeSession([_declaration("repo.alpha"), _declaration("repo.beta")])
    assert load_declarations(session, []) == {}
    assert session.calls == []
    loaded = load_declarations(session, [NODE, NODE, "author:other"])
    assert sorted(row["source"] for row in loaded[NODE]) == ["repo.alpha", "repo.beta"]
    assert session.calls[0][1] == {"node_ids": ["author:other", NODE]}
    assert "okg.graph_node_declarers" in session.calls[0][0]


@pytest.mark.parametrize("missing", ["node_id", "source", "subtype"])
def test_declaration_from_upsert_refuses_a_row_without_identity(missing: str) -> None:
    row = _upsert("repo.alpha")
    del row[missing]
    with pytest.raises(KeyError):
        declaration_from_upsert(row)

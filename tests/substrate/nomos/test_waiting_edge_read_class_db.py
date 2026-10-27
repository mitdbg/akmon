"""A waiting cross-source edge keeps its Nomos read class through promotion.

SECURITY (cross-source-endpoint-wedge). An edge admitted as waiting sits in
``okg.graph_edge_deferrals`` and later moves into ``okg.graph_edges``. It must
come out with exactly the stored read class (and Nomos source lineage) a
direct write of the same edge would have, so a policy that restricts its
source hides it after promotion as it would have before: the class is neither
lost nor changed on the way. Both moves copy the whole row, class columns
included (``edge_row`` / ``waiting_row`` in
``graph_store_internal/edge_deferrals.py``); this test proves the result.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.nomos import read_class as rc
from okg.substrate.nomos.core import compile_policy_graph
from okg.substrate.nomos.read_class import (
    READ_CLASS_COLUMN,
    READ_CLASS_VERSION_COLUMN,
    stamp_row,
)
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    read_class_filter,
)
from tests.pytest import endpoint_wedge_fixture as fx
from tests.pytest.source_jobs_inline import use_inline_source_jobs

import test_read_filter_decision_class as _decision_class
from test_read_filter_decision_class import _Conn, _session

pytestmark = pytest.mark.postgres

engine = _decision_class.engine


def _graph_edge_row(dsn: str) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout().query("SELECT * FROM okg.graph_edges")
    assert len(rows) == 1
    return dict(rows[0])


def _restricting_bundle() -> Any:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:waiting-edge", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{
            "rule_id": "deny_b", "effect": "deny", "actions": ["*"],
            "subjects": ["*"], "match": {"source_id": "svc.b"}, "priority": 5,
        }],
    })


def test_promotion_keeps_the_class_a_direct_write_would_have(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    use_inline_source_jobs(monkeypatch)
    dep = fx.build_deployment(tmp_path, clean_runtime)

    # Written directly (the widget exists): the class the writer stamps.
    fx.write_fixture(
        dep, "a",
        [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")], [],
    )
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note"), fx.node("note:2", "note")],
        [fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    direct = _graph_edge_row(clean_runtime)
    assert direct[READ_CLASS_COLUMN] is not None, "a restricted source's edge"
    assert direct[READ_CLASS_VERSION_COLUMN] == rc.CLASS_VOCABULARY_VERSION

    # The same edge shape admitted as waiting (the widget does not exist yet).
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note"), fx.node("note:2", "note")],
        [fx.edge("note:1", "widget:1"), fx.edge("note:2", "widget:3")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (held,) = fx.waiting_rows(clean_runtime)
    assert held["dst"] == "widget:3"
    # The waiting row carries the class the edge had at admission.
    assert held[READ_CLASS_COLUMN] is not None
    assert held[READ_CLASS_VERSION_COLUMN] == rc.CLASS_VOCABULARY_VERSION
    lineage = held["attrs"]["nomos_source_lineage"]
    assert lineage["source_ids"] == ["svc.b"]

    fx.write_fixture(
        dep, "a",
        [fx.node(f"widget:{i}", "widget") for i in (1, 2, 3)], [],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.waiting_rows(clean_runtime) == []
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        promoted = next(
            dict(row) for row in ChronosGraphStore.from_connection(
                conn, ensure_schema=False,
            ).checkout().query("SELECT * FROM okg.graph_edges")
            if row["dst"] == "widget:3"
        )

    # Exactly the class, version and lineage the waiting row held ...
    assert promoted[READ_CLASS_COLUMN] == held[READ_CLASS_COLUMN]
    assert promoted[READ_CLASS_VERSION_COLUMN] == held[READ_CLASS_VERSION_COLUMN]
    assert promoted["attrs"]["nomos_source_lineage"] == lineage
    # ... and exactly what the writer's stamp computes for the promoted row's
    # content, i.e. what a direct write of it would have stored.
    recomputed = dict(promoted)
    recomputed[READ_CLASS_COLUMN] = None
    recomputed[READ_CLASS_VERSION_COLUMN] = None
    stamp_row(recomputed, "edge")
    assert recomputed[READ_CLASS_COLUMN] == promoted[READ_CLASS_COLUMN]
    # The same source and shape as the directly written edge share its class.
    assert promoted[READ_CLASS_COLUMN] == direct[READ_CLASS_COLUMN]

    # A policy that restricts the source hides the promoted edge from a reader
    # without access, by the stored class and by the per-row filter alike; a
    # control policy that allows shows it.
    engine_state = request.getfixturevalue("engine")
    session = _session()
    engine_state.mode = "enforce"
    engine_state.bundle = _restricting_bundle()
    by_class, path = read_class_filter(
        _Conn(), session, [dict(promoted)], row_kind="edge", verify_sample=1,
    )
    per_row = filter_edge_rows_for_session(_Conn(), session, [dict(promoted)])
    assert by_class == [] and per_row == []
    assert path.path == "read_class" and path.stored_class_rows == 1
    engine_state.bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:allow", "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{
            "rule_id": "deny_other", "effect": "deny", "actions": ["*"],
            "subjects": ["*"], "match": {"source_id": "svc.other"},
            "priority": 5,
        }],
    })
    visible = filter_edge_rows_for_session(_Conn(), session, [dict(promoted)])
    assert [row["edge_id"] for row in visible] == [promoted["edge_id"]]


def test_a_legacy_waiting_row_without_a_class_is_recomputed_by_the_reader(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """A waiting row written before read classes existed (NULL class) is
    promoted as it is. The reader does not trust a missing class: it judges
    the row by its content, so a restricted reader still cannot see it."""
    from okg.substrate.graph_store import ChronosGraphStore

    use_inline_source_jobs(monkeypatch)
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")], [fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (held,) = fx.waiting_rows(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        with session.transaction():
            session.upsert_rows("okg.graph_edge_deferrals", [{
                **held, READ_CLASS_COLUMN: None, READ_CLASS_VERSION_COLUMN: None,
            }])
    assert fx.waiting_rows(clean_runtime)[0][READ_CLASS_COLUMN] is None

    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    promoted = _graph_edge_row(clean_runtime)
    assert promoted[READ_CLASS_COLUMN] is None, "copied as the legacy row it is"
    assert promoted["attrs"]["nomos_source_lineage"]["source_ids"] == ["svc.b"]

    engine_state = request.getfixturevalue("engine")
    session = _session()
    engine_state.mode = "enforce"
    engine_state.bundle = _restricting_bundle()
    by_class, path = read_class_filter(
        _Conn(), session, [dict(promoted)], row_kind="edge", verify_sample=1,
    )
    assert by_class == [], "a restricted reader must not see the promoted edge"
    assert path.stored_class_rows == 0 and path.class_rows == 1, (
        "the missing class is not trusted: the reader projects the class "
        "from the row's content and decides on that"
    )
    assert filter_edge_rows_for_session(
        _Conn(), session, [dict(promoted)],
    ) == []

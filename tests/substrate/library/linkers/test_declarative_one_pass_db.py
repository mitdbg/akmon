"""`word_boundary` and `subject_prefix` rules through the linker, on Postgres.

The one-pass matchers must write the edges the old every-pair loops would
have, on a full run and on an incremental run that only names the changed
nodes. The expected edges are computed with the old loops (kept in
`_one_pass_reference`) over the same node values.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.enrichers.base import IncrementalContext
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.linkers.declarative import DeclarativeLinker
from okg.substrate.library.linkers.rules import CrossLinkRule, RuleEndpoint
from okg.substrate.library.sources.base import NodeFact
from tests.substrate._db_helpers import ensure_published_generation
from tests.substrate.library.linkers._one_pass_reference import (
    old_pairs_subject_prefix as _old_pairs_subject_prefix,
    old_pairs_word_boundary as _old_pairs_word_boundary,
)


pytestmark = pytest.mark.postgres

_SUBTYPES = ("ticket", "source_file", "commit", "change")

_WORD_RULE = CrossLinkRule(
    id="ticket_mentions_file",
    src=RuleEndpoint(subtypes=("ticket",), fields=("title", "body")),
    dst=RuleEndpoint(subtypes=("source_file",), field="path"),
    matcher="word_boundary",
    edge="references",
    bidirectional=False,
)
_SUBJECT_RULE = CrossLinkRule(
    id="commit_names_change",
    src=RuleEndpoint(subtypes=("commit",), field="subject"),
    dst=RuleEndpoint(subtypes=("change",), field="change_id"),
    matcher="subject_prefix",
    edge="references",
    bidirectional=False,
)

_FILES = {
    "file:a": "src/okg/x.py",
    "file:b": "okg/x.py",
    "file:c": "x.py",
    "file:d": "docs/a b/notes.md",
    "file:e": "src/okg/x.pyc",
}
_TICKETS = {
    "ticket:1": ("Crash in src/okg/x.py", "see okg/x.py and x.py."),
    "ticket:2": ("x.pyc is stale", "src/okg/x.pyc"),
    "ticket:3": ("notes", "read docs/a b/notes.md first"),
    # Near misses only: each slug touches a letter or a hyphen.
    "ticket:4": ("none", "srcokg/x.pyc and -x.py-"),
}
_CHANGES = {"change:1": "add-cache", "change:2": "add", "change:3": "fix bug"}
_COMMITS = {
    "commit:1": "add-cache: first cut",
    "commit:2": "add more tests",
    "commit:3": "fix bug in parser",
    "commit:4": "addition",
}


def _node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
    return NodeFact(node_id, subtype, attrs, {"id": node_id}, {"v": 1})


def _facts(
    files: dict[str, str],
    tickets: dict[str, tuple[str, str]],
    changes: dict[str, str],
    commits: dict[str, str],
) -> list[NodeFact]:
    return [
        *(_node(i, "source_file", {"path": p}) for i, p in files.items()),
        *(
            _node(i, "ticket", {"title": t, "body": b})
            for i, (t, b) in tickets.items()
        ),
        *(_node(i, "change", {"change_id": c}) for i, c in changes.items()),
        *(_node(i, "commit", {"subject": s}) for i, s in commits.items()),
    ]


def _seed(conn: psycopg.Connection, facts: list[NodeFact]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-one-pass-test",
            catalog_version_id=catalog_version,
        )
        for subtype in _SUBTYPES:
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, catalog_version),
            )
    ChronosGraphStore.from_connection(conn).apply_facts(
        facts, source="default", strict_endpoints=True,
    )


def _edges(conn: psycopg.Connection) -> set[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src, dst FROM okg.edge_facts "
            " WHERE edge_type = 'references' AND op = 'I'"
        )
        return {(str(src), str(dst)) for src, dst in cur.fetchall()}


def _expected(
    files: dict[str, str],
    tickets: dict[str, tuple[str, str]],
    changes: dict[str, str],
    commits: dict[str, str],
) -> set[tuple[str, str]]:
    ticket_texts = [(i, f"{t}\n{b}") for i, (t, b) in tickets.items()]
    return _old_pairs_word_boundary(ticket_texts, list(files.items())) | (
        _old_pairs_subject_prefix(list(commits.items()), list(changes.items()))
    )


def _linker() -> DeclarativeLinker:
    return DeclarativeLinker(rules=[_WORD_RULE, _SUBJECT_RULE])


def test_full_run_writes_the_edges_the_old_loops_found(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, _facts(_FILES, _TICKETS, _CHANGES, _COMMITS))
        result = _linker().enrich(conn, generation_id=2)
        edges = _edges(conn)

    expected = _expected(_FILES, _TICKETS, _CHANGES, _COMMITS)
    assert edges == expected
    assert result.n_edges_emitted == len(expected)
    # The cases that tell a whole-word match from a substring.
    assert {
        ("ticket:1", "file:a"), ("ticket:1", "file:b"), ("ticket:1", "file:c"),
        ("ticket:2", "file:e"), ("ticket:3", "file:d"),
        ("commit:1", "change:1"), ("commit:2", "change:2"),
        ("commit:3", "change:3"),
    } <= edges
    assert not {p for p in edges if p[0] in {"ticket:4", "commit:4"}}


def test_incremental_run_adds_the_edges_of_changed_nodes(clean_runtime: str) -> None:
    new_files = {"file:f": "src/new.py"}
    new_tickets = {"ticket:5": ("new", "touches src/new.py and x.py")}
    new_changes = {"change:4": "addition"}
    new_commits = {"commit:5": "add-cache tidy"}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, _facts(_FILES, _TICKETS, _CHANGES, _COMMITS))
        linker = _linker()
        linker.enrich(conn, generation_id=2)
        ChronosGraphStore.from_connection(conn).apply_facts(
            _facts(new_files, new_tickets, new_changes, new_commits),
            source="default",
            strict_endpoints=True,
        )
        dirty = frozenset({*new_files, *new_tickets, *new_changes, *new_commits})
        result = linker.enrich(
            conn,
            generation_id=3,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=dirty,
                is_first_run=False,
                last_status="success",
            ),
        )
        edges = _edges(conn)

    expected = _expected(
        {**_FILES, **new_files},
        {**_TICKETS, **new_tickets},
        {**_CHANGES, **new_changes},
        {**_COMMITS, **new_commits},
    )
    assert edges == expected
    new_pairs = {p for p in expected if p[0] in dirty or p[1] in dirty}
    assert new_pairs == {
        ("ticket:5", "file:f"), ("ticket:5", "file:c"),
        ("commit:4", "change:4"), ("commit:5", "change:1"),
    }
    assert result.n_edges_emitted == len(new_pairs)

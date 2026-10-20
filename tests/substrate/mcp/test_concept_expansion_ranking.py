"""Concept expansion through the real BM25 lexical path (issue #1218).

The hermetic half lives in `test_concept_expansion.py`. What needs a database
is the claim the whole change rests on: expansion REACHES a passage written in
the other vocabulary, and the passage it reaches still ranks BELOW every
passage carrying a term the caller typed -- on a corpus rigged so BM25 alone
would order them the other way round.

Neutral vocabulary on purpose: concept groups are deployment-declared, so no
domain term belongs in a substrate test.
"""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.concept_expansion import parse_concept_groups
from okg.substrate.mcp.search_profiles import ResolvedScope
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres


GROUPS = parse_concept_groups({"throughput": ["\\tput", "items/s"]})
SCOPE = ResolvedScope(concept_groups=GROUPS)

#: `exact` mentions the caller's own word ONCE, buried in filler. `variant`
#: repeats the notation, so on the expanded query BM25 scores it higher. Any
#: implementation that expanded by score alone would return `variant` first.
CORPUS = {
    "n:ce:exact": (
        "the ingest stage reported throughput once in a long paragraph of "
        "otherwise unrelated commentary about scheduling and retries"
    ),
    "n:ce:variant": "\\tput \\tput \\tput measured on the same stage",
    "n:ce:unrelated": "scheduling and retries commentary only",
}


def _seed_and_publish(dsn: str, conn: psycopg.Connection, corpus=None) -> None:
    seed_catalog_version(conn)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="pact_change",
                attrs={"text": text},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
            for node_id, text in (CORPUS if corpus is None else corpus).items()
        ],
        source="default.concept_expansion_test",
        strict_endpoints=True,
    )
    result = Publisher(
        dsn,
        "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert result["status"] == "published", result


def _marker(prefix: str, index: int) -> str:
    """A letters-only word unique to one row, so no tokenizer splits it."""
    return prefix + "".join("abcdefghij"[int(digit)] for digit in f"{index:03}")


def test_no_declared_groups_leaves_the_page_and_the_query_unchanged(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_and_publish(clean_runtime, conn)
        session = open_session(conn, deployment_name="default")
        page = search(conn, session, "throughput", method="lexical", limit=20)
    assert [row["node_id"] for row in page["results"]] == ["n:ce:exact"]
    assert page["query"] == "throughput"
    assert page["concept_expansion"] == {
        "applied": False,
        "groups": [],
        "expanded_query": None,
        "reason": "no_groups_declared",
    }


def test_expansion_reaches_the_notation_only_passage_and_reports_why(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_and_publish(clean_runtime, conn)
        session = open_session(conn, deployment_name="default")
        page = search(
            conn,
            session,
            "throughput",
            method="lexical",
            limit=20,
            scope=SCOPE,
        )
    found = {row["node_id"] for row in page["results"]}
    assert found == {"n:ce:exact", "n:ce:variant"}
    disclosure = page["concept_expansion"]
    assert disclosure["applied"] is True
    assert disclosure["groups"] == [
        {
            "canonical": "throughput",
            "matched_terms": ["throughput"],
            "added_terms": ["\\tput", "items/s"],
        }
    ]
    assert disclosure["expanded_query"] == "throughput \\tput items/s"


def test_an_exact_match_ranks_above_an_expanded_one(
    clean_runtime: str,
) -> None:
    """Expanded top-k must neither evict originals nor change their order.

    Each row ends in its own marker word, which matches no query term: a
    records-ranked lexical search folds rows with identical text into one
    (#3465), and every row here must stay its own row. Every row gains exactly
    one token, so the length-driven score order is unchanged.
    """
    corpus = {
        **{
            f"n:ce:original:{i:03}": "throughput "
            + "filler " * (40 + i % 13)
            + ("\\tput " * 3 if i % 5 == 0 else "")
            + _marker("o", i)
            for i in range(220)
        },
        **{
            f"n:ce:variant:{i:03}": "\\tput " * 12 + _marker("v", i) for i in range(220)
        },
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_and_publish(clean_runtime, conn, corpus)
        session = open_session(conn, deployment_name="default")
        original = search(conn, session, "throughput", method="lexical", limit=200)
        page = search(
            conn,
            session,
            "throughput",
            method="lexical",
            limit=200,
            scope=SCOPE,
        )
        additions = search(
            conn,
            session,
            "throughput",
            method="lexical",
            limit=200,
            offset=200,
            scope=SCOPE,
        )
        raw_expanded = search(
            conn,
            session,
            "throughput \\tput items/s",
            method="lexical",
            limit=200,
        )
    ordered = [row["node_id"] for row in page["results"]]
    original_ids = [row["node_id"] for row in original["results"]]
    assert len(original_ids) == 200
    # Positive control: expanded BM25 alone would change this top-k.
    assert [row["node_id"] for row in raw_expanded["results"]] != original_ids
    assert ordered == original_ids
    assert [row["score"] for row in page["results"]] == [
        row["score"] for row in original["results"]
    ]
    assert additions["results"]
    assert any(
        row["node_id"].startswith("n:ce:variant:") for row in additions["results"]
    )
    assert not set(original_ids) & {row["node_id"] for row in additions["results"]}
    assert page["rank_capped"] is True


def test_expand_concepts_false_disables_expansion_per_request(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_and_publish(clean_runtime, conn)
        session = open_session(conn, deployment_name="default")
        page = search(
            conn,
            session,
            "throughput",
            method="lexical",
            limit=20,
            scope=SCOPE,
            expand_concepts=False,
        )
    assert [row["node_id"] for row in page["results"]] == ["n:ce:exact"]
    assert page["concept_expansion"]["applied"] is False
    assert page["concept_expansion"]["reason"] == "disabled_by_request"

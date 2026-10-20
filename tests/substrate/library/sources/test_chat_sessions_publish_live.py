"""PACT chat-transcript-source — req.chat-transcript-source.published-substrate-output.

FLAGGED FOR THE SERIAL RUN. This is the live half of the PACT: it needs a real
Postgres runtime, it publishes a generation, and it must not share a database
with a parallel worker pool. Select it with::

    uv run --extra dev python -m pytest \
      tests/substrate/library/sources/test_chat_sessions_publish_live.py \
      -q -m "postgres and serial_db"

What it proves, in the order the requirement states it:

1. The narrowing catalog is REAL and NON-EMPTY: the composed
   ``chat_sessions`` + ``git_graph`` + ``openspec`` catalog is applied before
   anything is ingested. On a zero-narrowing catalog the publish-time gate
   passes vacuously, which would make every assertion below meaningless.
2. The cited nodes are created by a DIFFERENT source first, so the
   ``chat_message references X`` edges have real endpoints and commit rather
   than being pruned as dangling.
3. Ingest and one publish cycle run through ``SourceRunner`` and
   ``Publisher.cycle()`` -- the ordinary path, no bootstrap shortcut.
4. The per-subtype row delta is read through a GENERATION-PINNED CHRONOS
   CHECKOUT over ``okg.graph_nodes`` / ``okg.graph_edges``. The legacy
   ``nodes_live`` / ``edges_live`` relations are dropped on chronos-native
   deployments and their bare passthroughs read empty, so a zero from them
   would be a failed read path masquerading as an empty upstream -- this
   suite never touches them.
5. ``describe_generation`` reports ``published``, not ``blocked``.
6. Queue depth and DLQ are within bounds and ``database_doctor`` is clean.

Zero new rows is a FAILURE here, and the two ways of reaching zero are
distinguished. The INDEPENDENT upstream count is taken from the fixture
builder's own SQL over the app database -- a different reader than the
adapter's -- and both numbers are recorded, never a boolean.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest
import yaml

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.library.sources.chat_sessions import ChatSessionsSource
from okg.substrate.publisher import Publisher
from tests.substrate.library.sources import _chat_sessions_fixture as fx
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = [
    pytest.mark.postgres,
    pytest.mark.serial_db,
    pytest.mark.slow,
]


DEPLOYMENT = "chat-transcript-publish-proof"
OWNERSHIP_ID = f"{DEPLOYMENT}.chat-sessions"
CITED_OWNERSHIP_ID = f"{DEPLOYMENT}.cited-targets"
INSTANCE = "publish-proof"
PRESET = "workspace"
MODULES = ["chat_sessions", "git_graph", "openspec"]

_EXPECTED_SUBTYPES = (
    "chat_session",
    "chat_message",
    "chat_rating",
    "arena_preference",
    "chat_context_record",
)


class _CitedTargetsSource:
    """Creates the nodes the chat answer cites.

    Without it the ``references`` edges would have no endpoints and would be
    pruned or blocked, and the proof would silently be about ``contains``
    edges only.
    """

    name = "cited_targets"
    profile = "discovery_crawl"
    change_probe_kind = "content_hash"
    probe_short_circuit_safe = False

    def run(self, run_id: str, **_kwargs: Any) -> SourceRun:
        record = {"repo": "okg", "path": "cited"}
        revision = {"content_hash": "cited-v1"}
        facts = [
            NodeFact(
                node_id=fx.CITED_FILE,
                subtype="source_file",
                attrs={"path": "src/okg/substrate/chat/audit_filter.py",
                       "repo": "okg"},
                source_record_id=record,
                source_revision=revision,
            ),
            NodeFact(
                node_id=fx.CITED_PACT,
                subtype="pact_change",
                attrs={"change_id": "chat-transcript-source",
                       "title": "Chat sessions as a graph source"},
                source_record_id=record,
                source_revision=revision,
            ),
        ]
        return SourceRun(facts=iter(facts), completed_scope=True,
                         next_cursor={"v": 1}, run_mode="scope_complete")


def _write_deployment(root: Path) -> Path:
    dep = root / DEPLOYMENT
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({"name": DEPLOYMENT, "modules": MODULES}, sort_keys=True)
    )
    return dep


def _upstream_counts(app_db: Path) -> dict[str, int]:
    """The INDEPENDENT upstream count, on a different surface than the reader.

    The requirement names "the instance HTTP API with a different credential".
    That surface is not reachable here: this deployment is a fixture with no
    instance behind it, and obtaining an admin token from the operator's real
    instance would mean reading their credential store. The substitute keeps
    the property the requirement is for -- the number must not come from the
    adapter's own read of the file -- by taking it from the fixture builder's
    OWN CONSTRUCTION MANIFEST, which is what was written rather than what any
    reader reports. Both numbers are recorded, never a boolean.

    ``app_db`` is accepted and cross-checked so a fixture change that alters
    the row counts fails here instead of silently weakening the guard.
    """
    declared = fx.CONSTRUCTED_COUNTS
    conn = sqlite3.connect(f"file:{app_db}?mode=ro", uri=True)
    try:
        observed = {
            "conversations": int(
                conn.execute("SELECT count(*) FROM chat").fetchone()[0]
            ),
            "messages": int(
                conn.execute("SELECT count(*) FROM chat_message").fetchone()[0]
            ),
            "feedback": int(
                conn.execute("SELECT count(*) FROM feedback").fetchone()[0]
            ),
        }
    finally:
        conn.close()
    assert observed == declared, (
        "the fixture builder's construction manifest disagrees with the "
        f"database it wrote: declared {declared}, observed {observed}"
    )
    return declared


def _counts(store: Any) -> dict[str, int]:
    """Row counts through a chronos checkout — never nodes_live."""
    nodes = store.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    edges = store.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
    return {"nodes": int(nodes or 0), "edges": int(edges or 0)}


@pytest.fixture
def app_db(tmp_path: Path) -> Iterator[Path]:
    yield fx.build_app_db(tmp_path / "chat" / "app.db")


def test_chat_sessions_reach_a_published_generation(
    clean_runtime: str, tmp_path: Path, app_db: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path / "deployments")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )

    # 1 — a REAL narrowing catalog. A zero-narrowing catalog makes the
    # publish-time gate pass vacuously.
    composed = compose_catalog(dep_dir)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        # The LATEST applied version, not every applied version ever: counting
        # across versions would make the guard drift upward run after run and
        # stop being a statement about the catalog in force.
        cur.execute(
            "SELECT catalog_version_id FROM okg.catalog_versions "
            "WHERE status = 'applied' ORDER BY catalog_version_id DESC LIMIT 1"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            "SELECT count(*) FROM okg.narrowings WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        applied_narrowings = int(cur.fetchone()[0])
        cur.execute(
            "SELECT count(*) FROM okg.narrowings "
            "WHERE catalog_version_id = %s AND parent_subtype LIKE 'chat\\_%%'",
            (catalog_version_id,),
        )
        applied_chat_narrowings = int(cur.fetchone()[0])
    assert applied_narrowings > 0, (
        "the applied catalog declares zero narrowings, so the publish gate "
        "would pass vacuously and this proof would assert nothing"
    )
    # 4 intra-module triples plus the citation classes this module set loads
    # (source_file, git_commit, open_spec_change, pact_change, pact_task).
    assert applied_chat_narrowings == 9, applied_chat_narrowings
    assert applied_narrowings == len(composed.narrowing_triples()), (
        applied_narrowings, len(composed.narrowing_triples()),
    )

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = _counts(store)
    finally:
        store.close()

    # 2 — the independent upstream count, taken BEFORE any graph read.
    upstream = _upstream_counts(app_db)
    assert upstream["conversations"] > 0, (
        "the fixture app database holds no conversations, so a zero row "
        "delta below would be an honest empty ingest and prove nothing"
    )

    # 3 — the cited targets, then the chat source, both through SourceRunner.
    SourceRunner(clean_runtime, source_name=CITED_OWNERSHIP_ID).run(
        _CitedTargetsSource(), mode="scope_complete",
    )
    source = ChatSessionsSource(
        app_db_path=app_db,
        instance=INSTANCE,
        deployment=DEPLOYMENT,
        preset=PRESET,
        audit_state_home=tmp_path / "state",
        source_name=OWNERSHIP_ID,
    )
    ingest = SourceRunner(clean_runtime, source_name=OWNERSHIP_ID).run(
        source, mode="scope_complete",
    )
    assert ingest.nodes_appended > 0, ingest
    assert ingest.edges_appended > 0, ingest
    # The rejected-fact count for THIS source. `describe_generation` exposes
    # no such field (okg/substrate/exchange/harness.py), so the count is taken
    # from the ingest result that owns it. Zero here plus a published (not
    # blocked) generation is the pair the requirement asks for.
    rejected_facts = int(getattr(ingest, "rejected_fact_count", 0) or 0)
    assert rejected_facts == 0, ingest
    adapter_stats = dict(source.last_run_stats)

    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    generation_id = published["generation_id"]

    # 4 — generation describe reports published, not blocked.
    described = describe_generation(clean_runtime, generation_id)
    assert described["status"] == "published", described
    assert not described.get("blocked_reason"), described

    # 5 — generation-pinned chronos read, attributed by node id and subtype.
    ids = fx.node_ids(INSTANCE)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(generation_id)
        after = _counts(generation)
        by_subtype = {
            str(row["subtype"]): int(row["n"])
            for row in generation.query(
                "SELECT subtype, count(*)::int AS n FROM okg.graph_nodes "
                "WHERE source = :source GROUP BY subtype ORDER BY subtype",
                {"source": OWNERSHIP_ID},
            )
        }
        edge_rows = {
            (str(row["edge_type"]), int(row["n"]))
            for row in generation.query(
                "SELECT edge_type, count(*)::int AS n FROM okg.graph_edges "
                "WHERE source = :source GROUP BY edge_type",
                {"source": OWNERSHIP_ID},
            )
        }
        answer_rows = generation.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
            "WHERE node_id = :node_id",
            {"node_id": ids["graph_answer"]},
        )
        reference_rows = generation.query(
            "SELECT src, dst FROM okg.graph_edges "
            "WHERE source = :source AND edge_type = 'references' "
            "ORDER BY dst",
            {"source": OWNERSHIP_ID},
        )
    finally:
        store.close()

    assert answer_rows, (
        f"the chat answer produced no row under generation {generation_id}; "
        f"the app database held {upstream['conversations']} conversation(s) "
        f"and {upstream['messages']} message(s), so this is a zero-output "
        "failure, not an empty upstream"
    )
    for subtype in _EXPECTED_SUBTYPES:
        assert by_subtype.get(subtype, 0) > 0, (by_subtype, upstream)
    assert by_subtype["chat_session"] == upstream["conversations"]
    assert by_subtype["chat_message"] == upstream["messages"]
    assert (
        by_subtype["chat_rating"] + by_subtype["arena_preference"]
        == upstream["feedback"]
    )

    delta = {
        "nodes": after["nodes"] - before["nodes"],
        "edges": after["edges"] - before["edges"],
    }
    assert delta["nodes"] > 0 and delta["edges"] > 0, (delta, upstream)
    # THE TWO READINGS MUST AGREE. `describe_generation`'s `nodes_live` /
    # `edges_live` keys are its own generation-pinned chronos counts, not the
    # dropped legacy relations; if this test's checkout and describe's
    # disagreed, one of the two read paths would be wrong and neither number
    # could be trusted.
    assert (described["nodes_live"], described["edges_live"]) == (
        after["nodes"], after["edges"],
    ), (described, after)
    assert any(kind == "contains" for kind, _n in edge_rows), edge_rows
    # The cross-source citation edges landed with real endpoints, so the
    # closed allowlist is exercised end to end rather than only in the
    # hermetic tests.
    cited = sorted(str(row["dst"]) for row in reference_rows)
    assert cited == sorted([fx.CITED_FILE, fx.CITED_PACT]), cited
    assert fx.CITED_UNKNOWN not in cited

    # The credential a person pasted did not reach the published graph.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE source = %s AND attrs::text LIKE %s",
            (OWNERSHIP_ID, "%sk-liveKEYmaterial%"),
        )
        leaked = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.projection_pending")
        projection_queue = int(cur.fetchone()[0])
        cur.execute(
            "SELECT coalesce(sum(dlq_count), 0)::int "
            "FROM okg.v_runtime_queue_health"
        )
        dlq_count = int(cur.fetchone()[0])
    assert leaked == 0
    assert projection_queue == 0
    assert dlq_count == 0

    doctor = database_doctor(clean_runtime)
    assert doctor["ok"] is True, doctor

    report = {
        "pact": "chat-transcript-source",
        "requirement": "req.chat-transcript-source.published-substrate-output",
        "deployment": f"isolated-test:{DEPLOYMENT}",
        "source": OWNERSHIP_ID,
        "openwebui_version": adapter_stats["openwebui_version"],
        "app_db_revision": adapter_stats["app_db_revision"],
        "upstream_counts": upstream,
        "upstream_read_surface": (
            "the fixture builder's own construction manifest (what it WROTE), "
            "cross-checked against the file it wrote; NOT the adapter's "
            "reader. The requirement names the instance HTTP API with a "
            "different credential; that surface does not exist for a fixture "
            "deployment, and the operator's real instance would require "
            "reading their credential store"
        ),
        "catalog_version_id": catalog_version_id,
        "applied_narrowings": applied_narrowings,
        "applied_chat_narrowings": applied_chat_narrowings,
        "generation_id": generation_id,
        "generation_status": described["status"],
        "rejected_fact_count": rejected_facts,
        "describe_counts": {
            "nodes_live": described["nodes_live"],
            "edges_live": described["edges_live"],
        },
        "read_surface": (
            "generation-pinned chronos checkout over okg.graph_nodes / "
            "okg.graph_edges (never nodes_live)"
        ),
        "before": before,
        "after": after,
        "delta": delta,
        "nodes_by_subtype": by_subtype,
        "edges_by_type": sorted(edge_rows),
        "references_committed": cited,
        "adapter_stats": adapter_stats,
        "credential_leaks": leaked,
        "projection_queue": projection_queue,
        "dlq_count": dlq_count,
        "doctor_ok": doctor["ok"],
    }
    artifact = Path(".scratch/chat-transcript-source-publish-proof.json")
    artifact.parent.mkdir(exist_ok=True)
    artifact.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def test_deleted_conversation_retracts_in_the_next_generation(
    clean_runtime: str, tmp_path: Path, app_db: Path,
) -> None:
    """The retraction arm, proven against published rows rather than a diff.

    A deletion that only changes the adapter's record set proves nothing; what
    matters is that the row stops being live in the next generation.
    """
    dep_dir = _write_deployment(tmp_path / "deployments")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    composed = compose_catalog(dep_dir)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    def _source() -> ChatSessionsSource:
        return ChatSessionsSource(
            app_db_path=app_db,
            instance=INSTANCE,
            deployment=DEPLOYMENT,
            preset=PRESET,
            audit_state_home=tmp_path / "state",
            source_name=OWNERSHIP_ID,
        )

    SourceRunner(clean_runtime, source_name=CITED_OWNERSHIP_ID).run(
        _CitedTargetsSource(), mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name=OWNERSHIP_ID).run(
        _source(), mode="scope_complete",
    )
    first = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first["status"] == "published", first

    ids = fx.node_ids(INSTANCE)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(first["generation_id"])
        present = generation.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": ids["secret_session"]},
        )
    finally:
        store.close()
    assert present, "the conversation never published, so its removal is moot"

    fx.delete_conversation(app_db, fx.CHAT_SECRET)
    SourceRunner(clean_runtime, source_name=OWNERSHIP_ID).run(
        _source(), mode="scope_complete",
    )
    second = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second["status"] == "published", second

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(second["generation_id"])
        still_there = generation.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": ids["secret_session"]},
        )
        orphan_messages = generation.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = :node_id",
            {"node_id": ids["secret_message"]},
        )
        surviving = {
            str(row["subtype"]): int(row["n"])
            for row in generation.query(
                "SELECT subtype, count(*)::int AS n FROM okg.graph_nodes "
                "WHERE source = :source GROUP BY subtype",
                {"source": OWNERSHIP_ID},
            )
        }
    finally:
        store.close()

    assert not still_there, (
        "a conversation the user deleted upstream is still live in "
        f"generation {second['generation_id']}"
    )
    assert not orphan_messages, (
        "the conversation retracted but its message did not, leaving an "
        "orphan row under the new generation"
    )
    # The retraction is SCOPED: everything below the surviving conversations
    # is still there. Asserting only that one session node survived would pass
    # even if every message, rating, preference, and context record had been
    # taken with it.
    assert surviving == {
        "chat_session": fx.CONSTRUCTED_COUNTS["conversations"] - 1,
        "chat_message": fx.CONSTRUCTED_COUNTS["messages"] - 1,
        "chat_rating": 1,
        "arena_preference": 1,
        "chat_context_record": 1,
    }, surviving

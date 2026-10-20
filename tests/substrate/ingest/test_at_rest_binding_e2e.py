"""PACT bind-at-rest-anonymization -- published-generation E2E (req.chokepoint-binding).

Runs a not-store-raw fixture source through the LIVE SourceRunner against the test
DB and row-checks the published chronos graph: direct-identifier attributes hold
join-consistent pseudonyms, non_pii is revealed, no raw identifier survives, and a
store_raw source in the same graph stores its values unchanged. This is the
production-caller proof the at-rest transform lacked -- the transform runs at
admission (Step 0.5) before the graph write, so the stored graph never sees raw.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.alias.pseudonymize import load_pseudonym_service
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest.pii_posture import (
    PiiPosture,
    declared_pii_classifier,
    resolve_source_pii_posture,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.publisher.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_KEY = "0123456789abcdef0123456789abcdef"


class _PiiAdapter:
    """Emits two person concepts sharing one identifier (to prove join-consistency)."""

    profile = "discovery_crawl"

    def __init__(self, name: str) -> None:
        self.name = name

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        return SourceRun(
            facts=[
                NodeFact(
                    node_id=f"{self.name}:person:1",
                    subtype="concept",
                    attrs={"owner_email": "alice@example.com", "label": "Alice project"},
                    source_record_id={"id": "1", "email": "alice@example.com"},
                    source_revision={"v": 1, "rev": run_id, "email": "alice@example.com"},
                    op="I",
                ),
                NodeFact(
                    node_id=f"{self.name}:person:2",
                    subtype="concept",
                    attrs={"owner_email": "alice@example.com", "label": "second"},
                    source_record_id={"id": "2", "email": "alice@example.com"},
                    source_revision={"v": 1, "rev": run_id, "email": "alice@example.com"},
                    op="I",
                ),
                EdgeFact(
                    src=f"{self.name}:person:1",
                    dst=f"{self.name}:person:2",
                    edge_type="links_to",
                    attrs={"owner_email": "alice@example.com", "label": "same-owner"},
                    source_record_id={"id": "e1", "email": "alice@example.com"},
                    source_revision={"v": 1, "rev": run_id, "email": "alice@example.com"},
                    op="I",
                ),
            ],
            completed_scope=True,
            next_cursor=None,
            run_mode=mode,
        )


class _ProbedPiiAdapter:
    """Stable source snapshot with exact applied identity and deletion proof."""

    name = "probed-pii"
    profile = "discovery_crawl"
    change_probe_kind = "content_hash"
    probe_short_circuit_safe = True

    def __init__(self) -> None:
        self.snapshot = {
            "owner_email": "alice@example.com",
            "label": "Alice project",
        }
        self.run_calls = 0
        self.run_modes: list[str] = []
        self.run_cursors: list[dict | None] = []
        self.change_probe = ContentHashProbe(
            content_items=lambda: [
                (
                    "person:1",
                    json.dumps(
                        self.snapshot,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                ),
            ],
            config={"source": self.name},
            emit_targets=[type(self)],
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor=None,
        change_probe_verdict=None,
    ) -> SourceRun:
        del run_id, change_probe_verdict
        self.run_calls += 1
        self.run_modes.append(mode)
        self.run_cursors.append(cursor)
        record_id = {"id": "1"}
        revision = {"v": 1}
        return SourceRun(
            facts=[
                NodeFact(
                    node_id="probed-pii:person:1",
                    subtype="concept",
                    attrs=dict(self.snapshot),
                    source_record_id={
                        **record_id,
                        "email": self.snapshot["owner_email"],
                    },
                    source_revision={
                        **revision,
                        "email": self.snapshot["owner_email"],
                    },
                    op="I",
                ),
            ],
            completed_scope=True,
            next_cursor={"adapter_runs": self.run_calls},
            applied_raw_token=self.change_probe.build_token(),
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=RecordSet(records={
                "person:1": RecordEmission(
                    primary=RecordNode(
                        node_id="probed-pii:person:1",
                        subtype="concept",
                        source_record_id=record_id,
                        source_revision=revision,
                    ),
                ),
            }),
        )


def _not_store_raw_posture() -> PiiPosture:
    return PiiPosture(
        store_raw=False,
        classifier=declared_pii_classifier(
            {"owner_email": "direct_identifier", "label": "non_pii"}
        ),
        pseudonym_service=load_pseudonym_service(env={"OKG_PSEUDONYM_HMAC_KEY": _KEY}),
    )


def _nodes(graph, source: str) -> list[dict]:
    return graph.query(
        """
        SELECT node_id, attrs, source_record_id, source_revision
          FROM okg.graph_nodes
         WHERE source = :source
         ORDER BY node_id
        """,
        {"source": source},
    )


def _edges(graph, source: str) -> list[dict]:
    return graph.query(
        """
        SELECT src, dst, attrs, source_record_id, source_revision
          FROM okg.graph_edges
         WHERE source = :source
        """,
        {"source": source},
    )


def test_not_store_raw_source_publishes_pseudonyms(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    # Not-store-raw source: facts are pseudonymized/masked at admission.
    anon = SourceRunner(
        clean_runtime,
        source_name="test.anon-src",
        pii_posture=_not_store_raw_posture(),
        write_fact_log=True,
    ).run(_PiiAdapter("anon"), mode="scope_complete")
    # store_raw source (default posture): stored unchanged, in the same graph.
    raw = SourceRunner(
        clean_runtime,
        source_name="test.raw-src",
        pii_posture=resolve_source_pii_posture(source_policy={"store_raw": True}),
        write_fact_log=True,
    ).run(_PiiAdapter("raw"), mode="scope_complete")

    assert anon.nodes_appended == 2 and raw.nodes_appended == 2  # positive delta

    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    assert published["compat_generation_id"] > 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout_generation(published["generation_id"])
        anon_rows = _nodes(graph, "test.anon-src")
        raw_rows = _nodes(graph, "test.raw-src")
        assert len(anon_rows) == 2 and len(raw_rows) == 2

        # Not-store-raw: direct identifier pseudonymized, join-consistent, raw gone.
        e1 = anon_rows[0]["attrs"]["owner_email"]
        e2 = anon_rows[1]["attrs"]["owner_email"]
        assert e1.startswith("directidentifier_") and e1 == e2
        # non_pii revealed unchanged.
        assert anon_rows[0]["attrs"]["label"] == "Alice project"
        # No raw identifier anywhere in the not-store-raw source's published graph.
        assert "alice@example.com" not in json.dumps(anon_rows)

        # store_raw source: values stored unchanged (the raw is intentionally kept).
        assert raw_rows[0]["attrs"]["owner_email"] == "alice@example.com"

        # Edges transform at rest too, with a positive edges delta.
        anon_edges = _edges(graph, "test.anon-src")
        raw_edges = _edges(graph, "test.raw-src")
        assert len(anon_edges) == 1 and len(raw_edges) == 1
        edge_email = anon_edges[0]["attrs"]["owner_email"]
        assert edge_email.startswith("directidentifier_") and edge_email == e1
        assert "alice@example.com" not in json.dumps(anon_edges)
        assert raw_edges[0]["attrs"]["owner_email"] == "alice@example.com"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_record_id, source_revision, attrs "
                "FROM okg.node_facts WHERE source = %s",
                ("test.anon-src",),
            )
            anon_facts = cur.fetchall()
            cur.execute(
                "SELECT source_record_id, source_revision, attrs "
                "FROM okg.edge_facts WHERE source = %s",
                ("test.anon-src",),
            )
            anon_edge_facts = cur.fetchall()
        assert anon_facts and "alice@example.com" not in json.dumps(anon_facts, default=str)
        assert anon_edge_facts and "alice@example.com" not in json.dumps(
            anon_edge_facts, default=str
        )


def test_same_runner_pii_posture_transition_forces_authoritative_replay(
    clean_runtime: str,
) -> None:
    """An unchanged source is replayed when its at-rest policy tightens."""
    source = "test.pii-posture-transition"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _ProbedPiiAdapter()
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        pii_posture=resolve_source_pii_posture(
            source_policy={"store_raw": True},
        ),
        write_fact_log=True,
    )
    raw_token = adapter.change_probe.build_token()
    first = runner.run(adapter, mode="scope_complete")

    assert first.nodes_appended == 1
    assert adapter.run_calls == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        raw_rows = store.query(
            """
            SELECT attrs, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE source = :source
            """,
            {"source": source},
        )
        prior_cursor, prior_replay = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source,),
        ).fetchone()
    assert len(raw_rows) == 1
    assert raw_rows[0]["attrs"]["owner_email"] == "alice@example.com"
    assert prior_replay == {}
    prior_effective = prior_cursor[CURSOR_TOKEN_KEY]
    assert prior_effective["raw_token"] == raw_token

    # Model the live deployment policy changing while this worker retains the
    # same SourceRunner instance. The source bytes and raw probe token do not
    # change; only the runner's effective pipeline identity does.
    runner._configured_pii_posture = _not_store_raw_posture()
    assert adapter.change_probe.build_token() == raw_token
    poll = runner.evaluate_poll(adapter, mode="scope_complete")
    assert poll.enqueue is True
    assert poll.changed is True
    assert adapter.run_calls == 1

    replay = runner.run(adapter, mode="scope_complete")

    assert replay.nodes_appended == 1
    assert replay.clear_replay_required is True
    assert adapter.run_calls == 2
    assert adapter.run_modes == ["scope_complete", "scope_complete"]
    assert adapter.run_cursors[0] is None
    assert adapter.run_cursors[1] is not None
    assert "adapter_runs" not in adapter.run_cursors[1]
    assert "record_set" in adapter.run_cursors[1]
    assert adapter.change_probe.build_token() == raw_token

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        current_rows = store.query(
            """
            SELECT attrs, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE source = :source
            """,
            {"source": source},
        )
        latest_fact = conn.execute(
            """
            SELECT attrs, source_record_id, source_revision, op
              FROM okg.node_facts
             WHERE source = %s
               AND node_id = 'probed-pii:person:1'
             ORDER BY fact_id DESC
             LIMIT 1
            """,
            (source,),
        ).fetchone()
        next_cursor, next_replay = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source,),
        ).fetchone()

    assert len(current_rows) == 1
    current_email = current_rows[0]["attrs"]["owner_email"]
    assert current_email.startswith("directidentifier_")
    assert "alice@example.com" not in json.dumps(current_rows, default=str)
    assert latest_fact[3] == "I"
    assert "alice@example.com" not in json.dumps(latest_fact, default=str)
    next_effective = next_cursor[CURSOR_TOKEN_KEY]
    assert next_effective["raw_token"] == raw_token
    assert (
        next_effective["pipeline_fingerprint"]
        != prior_effective["pipeline_fingerprint"]
    )
    assert next_replay == {}

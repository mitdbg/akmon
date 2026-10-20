"""The opensearch-reference deployment, provisioned and published for real.

PACT `opensearch-reference-integration` (issue #2014), req.ori.source.

The unit tests prove what the adapter emits. This proves the graph: a real
provision against a real database, a published generation, and the counts
read back from live rows rather than from the adapter's own return value.
A source can emit six perfectly good facts and still have none of them
reach a published generation.

Two arms, deliberately different in what they need:

  * The DEFAULT arm replays the recorded response bodies under
    `tests/substrate/library/sources/opensearch_fixtures/baseline/`. It
    needs Postgres and nothing else, so it runs in hosted CI on every
    pull request.
  * The `live_opensearch` arm runs the documented command,
    `deployments/opensearch-reference/up.sh`, against the real container.
    It FAILS rather than skips when unconfigured -- a skipped arm reads as
    a passed one -- so it is excluded from automated lanes by marker and
    run on demand, and its counts are diffed against this arm's.

Both arms assert the same numbers. That diff is how a fixture that has
drifted from the real image gets caught.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main

from .._deployment_repo import commit_deployment_tree

pytestmark = pytest.mark.postgres

REPO_ROOT = Path(__file__).resolve().parents[3]
REFERENCE = REPO_ROOT / "deployments/opensearch-reference"
FIXTURES = (
    REPO_ROOT
    / "tests/substrate/library/sources/opensearch_fixtures/baseline"
)

#: What the seed data must become in the graph. Exact, both arms.
EXPECTED_NODES = {"search_cluster": 1, "search_index": 3, "service": 2}
EXPECTED_EDGES = {"contains": 3, "logs_to": 4}
EXPECTED_INDEX_NAMES = {
    "app-logs-2026.09", "ingest-events-2026.09", "audit-trail-2026.09",
}


def _json_out(capsys) -> dict:
    out = capsys.readouterr()
    text = (out.out or out.err).strip().splitlines()[-1]
    return json.loads(text)


def _stage(tmp_path: Path, name: str, *, live: bool) -> Path:
    """Copy the real deployment, renamed, pointed at fixtures or the cluster.

    The tree is copied rather than mocked so this exercises the manifest,
    the registry entry and the module list that ship in the repository. The
    only edit is the name (each run owns its own database) and, for the
    default arm, swapping `base_url` for `fixture_dir`.
    """
    target = tmp_path / "deployments" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(REFERENCE, target)

    manifest = yaml.safe_load((target / "deployment.yaml").read_text())
    manifest["name"] = name
    # No `database:` key. The test DSN names its own database, and a
    # declared name would collide with it -- `okg search --deployment`
    # refuses a deployment whose manifest disagrees with the DSN it was
    # handed, which is the right refusal and the wrong fixture.
    manifest["postgres"] = {"dsn": "${OKG_DSN}"}
    (target / "deployment.yaml").write_text(yaml.safe_dump(manifest))

    registry = yaml.safe_load((target / "source_registry.yaml").read_text())
    entry = registry["sources"]["opensearch-catalog"]
    entry["ownership_id"] = f"{name}.opensearch-catalog"
    entry["admission_policy"]["producer_id"] = f"{name}.opensearch-catalog"
    if not live:
        entry["params"].pop("base_url")
        entry["params"]["fixture_dir"] = str(FIXTURES)
    (target / "source_registry.yaml").write_text(yaml.safe_dump(registry))

    commit_deployment_tree(target)
    return target


def _provision(
    target: Path, dsn: str, name: str, capsys,
    monkeypatch: pytest.MonkeyPatch,
    *,
    expect_additions: bool = True,
) -> dict:
    """Provision once and return the envelope.

    `expect_additions=False` is for a re-provision where only ATTRIBUTES
    moved. `provision` reports ok false and exits non-zero when a publish
    adds no rows -- a sensible default, since a source that suddenly adds
    nothing is usually broken -- but an attribute-only update is the one
    case where zero added rows is the correct answer. The caller says so
    explicitly rather than the helper swallowing every non-zero exit.
    """
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target.parent))
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()
    rc = main(["provision", "--deployment", name, "--dsn", dsn, "--json"])
    out = _json_out(capsys)
    rendered = json.dumps(out, indent=2, default=str)
    if expect_additions:
        assert rc == 0, rendered
        return out

    proof = out["publish_proof"]
    # Still a real publish, still nothing wrong: a generation was cut, the
    # dead-letter queue is empty, and the only reason ok is false is that
    # no ROW was added.
    assert proof["generation_id"], rendered
    assert proof["nodes_added"] == 0, rendered
    assert proof["edges_added"] == 0, rendered
    assert proof["dlq_count"] == 0, rendered
    return out


def _with_database(dsn: str, database: str) -> str:
    """Same server and credentials, a different database name."""
    parts = urlsplit(dsn)
    return urlunsplit((
        parts.scheme, parts.netloc, f"/{database}", parts.query,
        parts.fragment,
    ))


def _recreate_database(dsn: str, database: str) -> None:
    """Drop and recreate the deployment's own database, clean.

    Recreated rather than reused because the catalog records an ownership
    claim bound to the manifest's fingerprint: a database left over from
    a run against an older source_registry.yaml refuses the next publish
    as manifest drift, correctly. A live proof that had to be repaired by
    hand between runs would not be a proof of the documented command.

    Guarded to the deployment's own name on a loopback host, so it can
    only ever drop the disposable database this test made.
    """
    parts = urlsplit(dsn)
    assert database == "okg_opensearch_reference", database
    assert parts.hostname in ("127.0.0.1", "localhost"), parts.hostname

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()", (database,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
        cur.execute(f'CREATE DATABASE "{database}"')


def _rows(dsn: str, query: str, params: dict | None = None) -> list[dict]:
    """Read through a Chronos checkout, not raw SQL.

    `okg.graph_nodes` and `okg.graph_edges` are generation-scoped views; a
    bare psycopg connection sees them empty, which would make every count
    assertion below pass or fail for the wrong reason.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return store.checkout().query(query, params or {})


def _node_counts(dsn: str) -> dict[str, int]:
    return {
        row["subtype"]: row["n"] for row in _rows(
            dsn,
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
            "GROUP BY subtype",
        )
    }


def _edge_counts(dsn: str) -> dict[str, int]:
    return {
        row["edge_type"]: row["n"] for row in _rows(
            dsn,
            "SELECT edge_type, count(*) AS n FROM okg.graph_edges "
            "GROUP BY edge_type",
        )
    }


def _assert_published_graph(dsn: str, capsys, name: str) -> dict:
    """Every assertion both arms share."""
    nodes = _node_counts(dsn)
    edges = _edge_counts(dsn)

    assert nodes == EXPECTED_NODES, nodes
    assert edges == EXPECTED_EDGES, edges

    index_rows = _rows(
        dsn,
        """
        SELECT attrs->>'name' AS name,
               attrs->>'retention' AS retention,
               attrs->>'retention_read' AS retention_read,
               attrs->>'field_count' AS field_count,
               attrs->'field_summary' AS field_summary
          FROM okg.graph_nodes WHERE subtype = 'search_index'
         ORDER BY attrs->>'name'
        """,
    )
    assert {row["name"] for row in index_rows} == EXPECTED_INDEX_NAMES
    by_name = {row["name"]: row for row in index_rows}

    # Every index's retention was READ; two carry a policy and the third
    # deliberately does not, so a null here means absent, never unknown.
    assert all(
        str(row["retention_read"]).lower() == "true" for row in index_rows
    ), index_rows
    assert by_name["audit-trail-2026.09"]["retention"] is None
    assert by_name["app-logs-2026.09"]["retention"] == "okg-logs-30d"
    assert by_name["ingest-events-2026.09"]["retention"] == "okg-events-90d"

    # The bounded mapping summary survived to a live node.
    assert int(by_name["app-logs-2026.09"]["field_count"]) == 9
    summary = by_name["app-logs-2026.09"]["field_summary"]
    if isinstance(summary, str):
        summary = json.loads(summary)
    names = [f["name"] for f in summary]
    assert "http.status" in names, names

    # The audit index is written by BOTH services -- the fan-in that a
    # one-service-per-index model would lose.
    writers = _rows(
        dsn,
        """
        SELECT s.attrs->>'declared_service_id' AS service
          FROM okg.graph_edges e
          JOIN okg.graph_nodes s ON s.node_id = e.src
          JOIN okg.graph_nodes i ON i.node_id = e.dst
         WHERE e.edge_type = 'logs_to'
           AND i.attrs->>'name' = 'audit-trail-2026.09'
         ORDER BY 1
        """,
    )
    assert [row["service"] for row in writers] == [
        "okg-api", "okg-ingest",
    ], writers

    # Nothing from inside an index leaked into the graph: the OKG models
    # the system, never its contents.
    leaked = _rows(
        dsn,
        "SELECT count(*) AS n FROM okg.graph_nodes "
        " WHERE attrs::text ILIKE :needle",
        {"needle": "%request 0 handled%"},
    )
    assert leaked[0]["n"] == 0

    # One successful read through the ordinary surface. The graph is
    # useful only if an ordinary reader can find the index by name.
    rc = main([
        "search", "--deployment", name, "--dsn", dsn,
        "--query", "app-logs", "--limit", "5",
    ])
    found = capsys.readouterr()
    assert rc == 0, found.err or found.out
    assert "app-logs-2026.09" in (found.out + found.err), found.out

    # Doctor clean against the provisioned database. The assertion IS the
    # check: a failing doctor raises here, so there is no doctor verdict to
    # hand back. An earlier shape returned `doctor_ok: rc == 0` beside it,
    # which could only ever be True by the time it was built -- a field that
    # reads as a result and carries none. No caller read it.
    rc = main(["--json", "doctor", "--dsn", dsn])
    doctor = _json_out(capsys)
    assert rc == 0, doctor

    return {"nodes": nodes, "edges": edges}


# ── the hosted-CI arm ─────────────────────────────────────────────────

def test_recorded_metadata_publishes_the_expected_graph(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """Provision, ingest and publish against recorded cluster responses.

    The counts are read back from live rows, not from the adapter. Six
    facts the adapter emitted but the publisher dropped would pass every
    unit test in this change and fail here.
    """
    name = f"osref{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage(tmp_path, name, live=False)
    out = _provision(target, clean_runtime, name, capsys, monkeypatch)

    assert out["ok"] is True
    proof = out["publish_proof"]
    assert proof["status"] == "published", proof
    assert proof["nodes_added"] == 6, proof
    assert proof["edges_added"] == 7, proof
    assert proof["dlq_count"] == 0, proof

    _assert_published_graph(clean_runtime, capsys, name)


def test_a_moved_document_count_reaches_the_published_node(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The other half of the volatile-metric decision.

    Keeping doc_count out of the content hash is only correct if the new
    figure still reaches the PUBLISHED node. A source that achieved a
    stable revision by dropping the metric, or a publisher that skipped
    the update because the revision had not moved, would both pass the
    unit tests and leave the graph serving a stale number while reporting
    itself current. Only a real publish can tell the difference.
    """
    name = f"osref{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage(tmp_path, name, live=False)
    _provision(target, clean_runtime, name, capsys, monkeypatch)

    before = _rows(
        clean_runtime,
        "SELECT (attrs->>'doc_count')::int AS docs FROM okg.graph_nodes "
        " WHERE subtype = 'search_index' AND attrs->>'name' = :name",
        {"name": "app-logs-2026.09"},
    )
    assert before[0]["docs"] == 40

    # A second fixture directory differing ONLY in the document count.
    moved = tmp_path / "fixtures-moved"
    shutil.copytree(FIXTURES, moved)
    rows = json.loads((moved / "cat_indices.json").read_text())
    for row in rows:
        if row["index"] == "app-logs-2026.09":
            row["docs.count"] = "93412"
    (moved / "cat_indices.json").write_text(json.dumps(rows, indent=2))

    registry = yaml.safe_load((target / "source_registry.yaml").read_text())
    registry["sources"]["opensearch-catalog"]["params"]["fixture_dir"] = str(
        moved,
    )
    (target / "source_registry.yaml").write_text(yaml.safe_dump(registry))
    commit_deployment_tree(target)
    _provision(
        target, clean_runtime, name, capsys, monkeypatch,
        expect_additions=False,
    )

    after = _rows(
        clean_runtime,
        "SELECT (attrs->>'doc_count')::int AS docs FROM okg.graph_nodes "
        " WHERE subtype = 'search_index' AND attrs->>'name' = :name",
        {"name": "app-logs-2026.09"},
    )
    assert after[0]["docs"] == 93412

    # And the node count did not move: this is an update, not a new node.
    assert _node_counts(clean_runtime) == EXPECTED_NODES


def test_an_unreadable_cluster_retracts_nothing(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The failure case that matters most at this layer.

    Under `missing_from_completed_scope`, a completed scope with nothing in
    it retracts every node this source owns. So a second run whose metadata
    read fails must leave the published graph exactly as it was rather than
    emptying it.
    """
    name = f"osref{tmp_path.name.lower().replace('-', '')[-8:]}"
    target = _stage(tmp_path, name, live=False)
    _provision(target, clean_runtime, name, capsys, monkeypatch)
    before = (_node_counts(clean_runtime), _edge_counts(clean_runtime))
    assert before[0] == EXPECTED_NODES

    # Point the source at a fixture directory that is not there: the read
    # fails, and the run must raise rather than complete an empty scope.
    registry = yaml.safe_load((target / "source_registry.yaml").read_text())
    registry["sources"]["opensearch-catalog"]["params"]["fixture_dir"] = str(
        tmp_path / "gone",
    )
    (target / "source_registry.yaml").write_text(yaml.safe_dump(registry))
    commit_deployment_tree(target)

    rc = main([
        "provision", "--deployment", name, "--dsn", clean_runtime, "--json",
    ])
    capsys.readouterr()
    assert rc != 0, "an unreadable cluster must not report a clean provision"

    after = (_node_counts(clean_runtime), _edge_counts(clean_runtime))
    assert after == before, (before, after)

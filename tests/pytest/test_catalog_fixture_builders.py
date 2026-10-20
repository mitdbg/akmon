from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    explicit_catalog_publisher,
    seed_catalog,
)

# Pull the substrate Postgres fixtures in by IMPORTING them (and their
# transitive deps) rather than `pytest_plugins = ("tests.substrate.conftest",)`.
# Referencing a conftest.py via pytest_plugins registers it as a named pluggy
# plugin; when a whole-tree collection (e.g. the perf lane) ALSO discovers that
# same conftest by path, pluggy raises "Plugin already registered under a
# different name" and the entire collection aborts. A plain import never
# pluggy-registers, so it sidesteps the collision while keeping the fixtures.
from tests.substrate.conftest import (
    clean_runtime,
    okg_dsn,
    okg_migrated,
)

pytestmark = pytest.mark.postgres

# Every name here is consumed by pytest BY NAME, and by nothing in this module:
# the three fixtures above are a deliberate re-export (see the import comment),
# and `pytestmark` is read off the module during collection. Neither a reader
# nor a static analyser can tell any of them is load-bearing, which is what
# `__all__` is for -- and matches `tests/okg-workspace/conftest.py` and
# `tests/af_pubs/conftest.py`, which re-export the same three fixtures this way.
# `okg_dsn` and `okg_migrated` are the transitive deps `clean_runtime` needs in
# scope.
__all__ = ["clean_runtime", "okg_dsn", "okg_migrated", "pytestmark"]


def test_catalog_helper_seeds_closure(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime) as conn:
        cv = seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("document"),
                SubtypeSpec("page", parent="document"),
                SubtypeSpec("concept"),
            ],
            narrowings=[("document", "references", "concept")],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT depth
                  FROM okg.subtype_ancestors
                 WHERE catalog_version_id = %s
                   AND subtype_name = 'page'
                   AND ancestor_name = 'document'
                """,
                (cv,),
            )
            assert cur.fetchone() == (1,)
        conn.commit()
        # Exercise the seeded catalog by writing a node/edge that the
        # `(document → references → concept)` narrowing accepts (page
        # narrows from document) into the chronos graph.
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("n:page", "page", {}, {"id": "n:page"}, {"v": 1}),
                NodeFact(
                    "n:concept",
                    "concept",
                    {},
                    {"id": "n:concept"},
                    {"v": 1},
                ),
                EdgeFact(
                    "n:page",
                    "n:concept",
                    "references",
                    source_record_id={"id": "n:page->n:concept"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        graph = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        node_count = graph.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
        )[0]["n"]
        edge_count = graph.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_edges"
        )[0]["n"]
        assert node_count == 2
        assert edge_count == 1


def test_catalog_helper_binds_explicit_publish_authority(
    clean_runtime: str,
) -> None:
    identity = bind_test_catalog_writer(clean_runtime, "fixture-publisher")

    assert identity.deployment_name == "fixture-publisher"
    assert identity.provenance["writer_scope"] == "deployment_root"
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT cv.writer_fingerprint, own.owner_fingerprint
              FROM okg.catalog_versions cv
              JOIN okg.deployment_write_ownership own
                ON own.deployment_name = cv.deployment_name
             WHERE cv.status = 'applied'
             ORDER BY cv.catalog_version_id DESC
             LIMIT 1
            """
        )
        assert cur.fetchone() == (
            identity.writer_fingerprint,
            identity.writer_fingerprint,
        )


def test_catalog_helper_preserves_distinct_deployment_bindings(
    clean_runtime: str,
) -> None:
    from okg.substrate.catalog.ownership import diagnose_catalog_ownership

    first = bind_test_catalog_writer(clean_runtime, "fixture-deployment-a")
    second = bind_test_catalog_writer(clean_runtime, "fixture-deployment-b")

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (deployment_name)
                       deployment_name, catalog_version_id, writer_fingerprint
                  FROM okg.catalog_versions
                 WHERE deployment_name = ANY(%s)
                   AND status = 'applied'
                 ORDER BY deployment_name, catalog_version_id DESC
                """,
                ([first.deployment_name, second.deployment_name],),
            )
            rows = cur.fetchall()
        reports = [
            diagnose_catalog_ownership(conn, identity=identity)
            for identity in (first, second)
        ]

    assert [row[0] for row in rows] == [
        first.deployment_name,
        second.deployment_name,
    ]
    assert rows[0][1] != rows[1][1]
    assert rows[0][2] == first.writer_fingerprint
    assert rows[1][2] == second.writer_fingerprint
    assert all(report["ok"] for report in reports)
    assert all(report["latest_compatible"] for report in reports)


def test_explicit_catalog_publisher_passes_bound_identity(
    clean_runtime: str,
) -> None:
    publisher = explicit_catalog_publisher(clean_runtime, "fixture-publisher")

    assert publisher.catalog_identity is not None
    assert publisher.catalog_identity.provenance["writer_scope"] == ("deployment_root")


def test_proof_catalog_writer_binds_the_exact_seeded_version(
    clean_runtime: str,
) -> None:
    from okg.substrate.publisher import Publisher
    from scripts.proof_catalog_authority import bind_proof_catalog_writer

    with psycopg.connect(clean_runtime) as conn:
        catalog_version_id = seed_catalog(
            conn,
            subtypes=["document"],
        )
        conn.commit()
    identity = bind_proof_catalog_writer(
        clean_runtime,
        deployment_name="standalone-proof-fixture",
        catalog_version_id=catalog_version_id,
        proof_name="catalog-authority-regression",
    )

    result = Publisher(
        clean_runtime,
        identity.deployment_name,
        catalog_identity=identity,
    ).cycle()

    assert result["status"] == "published"


def _bare_narrowing_insert_failures(
    source: str,
    *,
    rel: Path,
) -> list[str]:
    failures: list[str] = []
    for index, line in enumerate(source.splitlines(), start=1):
        if "INSERT INTO okg.narrowings" not in line:
            continue
        if "catalog-helper: allow-bare-narrowing-insert" in source:
            continue
        failures.append(
            f"{rel}:{index} inserts directly into okg.narrowings; "
            "use tests.pytest.catalog_fixture_builders.seed_catalog or "
            "document a catalog-helper exception"
        )
    return failures


def test_bare_narrowing_insert_lint_names_shared_helper() -> None:
    failures = _bare_narrowing_insert_failures(
        'cur.execute("INSERT INTO okg.narrowings VALUES (...)")\n',
        rel=Path("synthetic/test_direct_narrowing.py"),
    )

    assert failures == [
        "synthetic/test_direct_narrowing.py:1 inserts directly into "
        "okg.narrowings; use "
        "tests.pytest.catalog_fixture_builders.seed_catalog or document a "
        "catalog-helper exception"
    ]


def test_pact_named_db_tests_use_catalog_helper() -> None:
    repo = Path(__file__).resolve().parents[2]
    checked = [
        "tests/substrate/test_mcp_tools.py",
        "tests/substrate/test_narrowings.py",
        "tests/substrate/test_publisher.py",
        "tests/views/test_backbone.py",
    ]
    failures: list[str] = []
    for rel in checked:
        path = repo / rel
        source = path.read_text(encoding="utf-8")
        if "seed_catalog" not in source and "seed_narrowings" not in source:
            failures.append(f"{rel}: missing shared catalog helper")
        failures.extend(_bare_narrowing_insert_failures(source, rel=Path(rel)))

    assert failures == []

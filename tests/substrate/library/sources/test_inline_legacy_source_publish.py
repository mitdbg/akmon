from __future__ import annotations

import json
import sys
from pathlib import Path

import psycopg
import pytest
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


@pytest.fixture
def settings_path(tmp_path: Path) -> Path:
    path = tmp_path / "settings.json"
    path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "alpha": {"command": "node", "args": ["alpha.js"]},
                    "beta": {"url": "http://localhost:9000/mcp"},
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_representative_inlined_sources_publish_generation(
    clean_runtime: str,
    fixture_paths,
    settings_path: Path,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.compute_env_local import LocalEnvSource
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.publisher import Publisher

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    git_run = SourceRunner(
        clean_runtime,
        source_name="ilsa.git-files",
    ).run(
        GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root),
        mode="scope_complete",
    )
    local_env_run = SourceRunner(
        clean_runtime,
        source_name="ilsa.compute-env-local",
    ).run(
        LocalEnvSource(
            repo="okg",
            settings_path=settings_path,
            hostname_override="ilsa-host",
        ),
        mode="release_new",
    )

    assert git_run.completed_scope is True
    assert local_env_run.completed_scope is True
    assert git_run.nodes_appended > 0
    assert local_env_run.nodes_appended > 0

    published = Publisher(
        clean_runtime,
        "ilsa-proof",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "ilsa-proof"),
    ).cycle()

    assert published["status"] == "published", published
    assert published["node_count"] >= (
        git_run.nodes_appended + local_env_run.nodes_appended
    )
    assert published["edge_count"] >= (
        git_run.edges_appended + local_env_run.edges_appended
    )
    assert published["phase_b_blocked"] is False

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(
            published["generation_id"],
            branch="default",
        )
        source_counts = {
            row["source"]: int(row["n"])
            for row in checkout.query(
                """
                SELECT source, count(*) AS n
                  FROM okg.graph_nodes
                 WHERE source IN ('ilsa.git-files', 'ilsa.compute-env-local')
                 GROUP BY source
                """
            )
        }
    finally:
        store.close()

    assert source_counts["ilsa.git-files"] >= git_run.nodes_appended
    assert source_counts["ilsa.compute-env-local"] >= local_env_run.nodes_appended

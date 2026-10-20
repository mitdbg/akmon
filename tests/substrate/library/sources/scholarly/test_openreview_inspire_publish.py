from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.error import URLError

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.scholarly import (
    _json_fetch,
    inspire_hep,
    openreview,
)
from okg.substrate.mcp import inspect_node, open_session
from okg.substrate.publish_proof import dlq_count
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    bind_test_catalog_writer,
    seed_catalog,
)


pytestmark = pytest.mark.postgres


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def read(self) -> bytes:
        return self.payload


def _case(name: str) -> tuple[Any, type, dict[str, Any], bytes, str]:
    if name == "openreview":
        return (
            openreview,
            openreview.OpenReviewSource,
            {
                "slug": "test",
                "name": "TestConf",
                "year": 2026,
                "invitation": "Test/2026/-/Submission",
            },
            b'{"notes":[{"id":"note-1","forum":"or-1","content":{"title":{"value":"OpenReview One"}}}],"count":1}',
            "paper:openreview:or-1",
        )
    return (
        inspire_hep,
        inspire_hep.InspireHepSource,
        {
            "slug": "test",
            "name": "TestConf",
            "queries": ["conference TestConf"],
            "split_by_year": False,
        },
        b'{"hits":{"hits":[{"id":"inspire-1","metadata":{"control_number":1,"titles":[{"title":"INSPIRE One"}],"publication_info":[{"year":2026}]}}],"total":1}}',
        "paper:inspire:1",
    )


def _source(
    source_type: type,
    venue: dict[str, Any],
    cache_root: Path,
    *,
    max_retries: int = 0,
):
    return source_type(
        cache_root=cache_root,
        endpoint="https://203.0.113.34/api",
        requests_per_second=None,
        page_size=1,
        max_retries=max_retries,
        venues=[venue],
    )


def _seed_publication_catalog(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["paper", "conference", "conference_edition"],
            narrowings=[
                ("conference", "contains", "conference_edition"),
                ("conference_edition", "member_of", "conference"),
                ("paper", "published_in", "conference_edition"),
            ],
        )


@pytest.mark.parametrize("source_name", ["openreview", "inspire_hep"])
def test_controlled_network_source_publishes_with_health_and_provenance(
    source_name: str,
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _module, source_type, venue, payload, paper_id = _case(source_name)
    opened: list[str] = []

    def _open(request: Any, *, timeout: float):
        assert timeout == 60
        opened.append(request.full_url)
        return _Response(payload)

    monkeypatch.setattr(_json_fetch, "assert_public_http_url", lambda _url: None)
    monkeypatch.setattr(_json_fetch, "open_validated_url", _open)
    _seed_publication_catalog(clean_runtime)
    deployment = f"scholarly-retry-{source_name}"
    source = _source(source_type, venue, tmp_path)

    result = SourceRunner(
        clean_runtime,
        source_name=source_name,
        deployment_name=deployment,
        write_fact_log=True,
    ).run(source, mode="scope_complete")

    assert len(opened) == 1
    assert source.last_run_stats["records"] == 1
    assert source.last_run_stats["cache_misses"] == 1
    assert len(list((tmp_path / source_name).glob("*.json"))) == 1
    assert result.nodes_appended == 3
    assert result.edges_appended == 3
    assert result.completed_scope is True
    assert result.source_health is None

    published = Publisher(
        clean_runtime,
        deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published
    assert published["generation_id"]
    assert published["phase_a_converged"] is True
    assert published["phase_b_blocked"] is False

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(published["generation_id"])
        nodes = checkout.query(
            "SELECT node_id, source, attrs FROM okg.graph_nodes ORDER BY node_id"
        )
        edges = checkout.query(
            "SELECT source, attrs FROM okg.graph_edges ORDER BY edge_type, src, dst"
        )
    finally:
        store.close()
    assert len(nodes) == 3
    assert len(edges) == 3
    assert {row["source"] for row in [*nodes, *edges]} == {source_name}
    assert {row["attrs"]["source_provider"] for row in [*nodes, *edges]} == {
        source_name
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=deployment,
            requested_generation=published["generation_id"],
        )
        readback = inspect_node(conn, session, paper_id)
    assert session.generation_id == published["generation_id"]
    assert readback is not None
    assert readback["attrs"]["source_provider"] == (
        f"⟦UNTRUSTED_DATA source={source_name}⟧\n"
        f"{source_name}\n"
        "⟦/UNTRUSTED_DATA⟧"
    )

    dead_letters, queue_depth = dlq_count(clean_runtime)
    assert dead_letters == 0
    assert all(int(depth) == 0 for depth in queue_depth.values())


@pytest.mark.parametrize("source_name", ["openreview", "inspire_hep"])
def test_late_page_failure_does_not_commit_completion_state(
    source_name: str,
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _module, source_type, venue, first_payload, _paper_id = _case(source_name)
    if source_name == "openreview":
        first_payload = first_payload.replace(b'"count":1', b'"count":2')
    else:
        first_payload = first_payload.replace(b'"total":1', b'"total":2')
    actions: list[bytes | BaseException] = [
        first_payload,
        URLError("second page failed"),
    ]

    def _open(_request: Any, *, timeout: float):
        assert timeout == 60
        action = actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return _Response(action)

    monkeypatch.setattr(_json_fetch, "assert_public_http_url", lambda _url: None)
    monkeypatch.setattr(_json_fetch, "open_validated_url", _open)
    _seed_publication_catalog(clean_runtime)
    deployment = f"scholarly-retry-failed-{source_name}"
    source = _source(source_type, venue, tmp_path, max_retries=0)

    with pytest.raises(URLError, match="second page failed"):
        SourceRunner(
            clean_runtime,
            source_name=source_name,
            deployment_name=deployment,
            write_fact_log=True,
        ).run(source, mode="scope_complete")

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT key
              FROM okg.source_progress
             WHERE deployment = %s AND source_name = %s
             ORDER BY key
            """,
            (deployment, source_name),
        )
        progress_keys = [str(row[0]) for row in cur.fetchall()]
        cur.execute(
            "SELECT count(*) FROM okg.source_watermarks WHERE source = %s",
            (source_name,),
        )
        watermark_count = int(cur.fetchone()[0])

    assert actions == []
    assert progress_keys == []
    assert "sync.last_run" not in progress_keys
    assert not any(key.startswith("sync.run.") for key in progress_keys)
    assert watermark_count == 0
    assert source.last_run_stats == {}

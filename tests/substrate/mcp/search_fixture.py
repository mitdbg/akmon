from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import psycopg

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def make_search_deployment(root: Path, *, dsn: str) -> str:
    deployment = root / "analyst-search"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("{}\n", encoding="utf-8")
    manifest: dict[str, Any] = {
        "name": "analyst-search",
        "description": "analyst search test fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "postgres": {"dsn": dsn},
    }
    (deployment / "deployment.yaml").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return str(deployment)


def publish_nodes(
    dsn: str,
    deployment: str,
    *,
    subtypes: Iterable[str],
    nodes: list[NodeFact],
    aliases: list[tuple[str, str, str | None]] | None = None,
    narrowings: list[tuple[str, str, str]] | None = None,
    edges: list[Any] | None = None,
    node_groups: list[tuple[str, list[Any]]] | None = None,
) -> tuple[int, str]:
    """Seed a catalog and publish nodes plus optional declared edges.

    `node_groups` publishes additional facts under DIFFERENT producer ids. The
    `source` column is a property of the apply_facts BATCH, not of a NodeFact,
    so a corpus with several producers needs several batches; source-family
    tests need exactly that and cannot build it with `nodes` alone.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        catalog_version = seed_catalog(
            conn,
            subtypes=list(subtypes),
            narrowings=narrowings,
            aliases=aliases,
        )
        store = ChronosGraphStore.from_connection(conn)
        for producer, facts in (node_groups or []):
            store.apply_facts(
                list(facts), source=producer, strict_endpoints=True,
            )
        store.apply_facts(
            list(nodes) + list(edges or []),
            source="system:analyst_search_fixture",
            strict_endpoints=True,
        )
    result = Publisher(dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment)).cycle()
    return catalog_version, str(result["generation_id"])


def node(node_id: str, subtype: str, text: str, **attrs: Any) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"text": text, **attrs},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )

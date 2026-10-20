"""The console view-definition probe reads through a Chronos checkout.

Regression for `retire-dead-live-table-readers`: the probe behind
`/viz/api/console/view` previously queried the pre-chronos live table,
which the Chronos cutover emptied and `schema-canonical-read-path`
dropped — so the route errored on every request regardless of whether
the view was registered. `okg.viz.reads.view_definition_registered` is
the single-sited replacement; the endpoint calls it verbatim.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session
from okg.viz import reads


pytestmark = pytest.mark.postgres

_VIEW_DEF_ID = "okg_workspace.today_brief"
_DEPLOYMENT = "okg-workspace"


def _seed_view_definition(conn: psycopg.Connection) -> None:
    """Publish one generation carrying a registered view_definition node."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        NodeFact(
            node_id=f"view_def:{_VIEW_DEF_ID}",
            subtype="view_definition",
            attrs={
                "view_def_id": _VIEW_DEF_ID,
                "deployment": _DEPLOYMENT,
            },
            source_record_id={"node_id": f"view_def:{_VIEW_DEF_ID}"},
            source_revision={"rev": 1},
        )
    ])
    store.checkpoint_generation(
        deployment_name="default",
        catalog_version_id=int(cv),
    )


def test_view_definition_probe_reads_chronos_checkout(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_definition(conn)
        session = open_session(conn, deployment_name="default")
        assert reads.view_definition_registered(
            conn, session,
            view_def_id=_VIEW_DEF_ID, deployment=_DEPLOYMENT,
        )
        # Unregistered id and wrong deployment both miss.
        assert not reads.view_definition_registered(
            conn, session,
            view_def_id="okg_workspace.not_registered",
            deployment=_DEPLOYMENT,
        )
        assert not reads.view_definition_registered(
            conn, session,
            view_def_id=_VIEW_DEF_ID, deployment="other-deployment",
        )

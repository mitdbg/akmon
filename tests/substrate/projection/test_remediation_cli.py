"""Smoke coverage for the `okg remediate` CLI verbs
(fix-projection-alias-identity, ruling 4). Each verb is dry-run by default;
these exercise the argparse wiring + dispatch and the dry-run JSON envelope.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from tests.pytest.catalog_fixture_builders import seed_catalog

pytestmark = pytest.mark.postgres


def _run(capsys, *argv: str) -> dict:
    rc = main(list(argv))
    out = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(out)
    return {"rc": rc, **payload}


def test_remediate_stranded_link_cache_dry_run(clean_runtime, capsys) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[])
    res = _run(
        capsys,
        "remediate", "stranded-link-cache",
        "--subtype", "page", "--limit", "10",
        "--dsn", clean_runtime, "--json",
    )
    assert res["rc"] == 0
    assert res["applied"] is False
    assert res["pass_name"] == "stranded_link_cache"


def test_remediate_collapsed_aliases_dry_run(clean_runtime, capsys) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["author"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact("a", "author", {"name": "J. Smith"}, {"id": "a"}, {"v": 1}),
            NodeFact("b", "author", {"name": "J. Smith"}, {"id": "b"}, {"v": 1}),
        ])
        store.close()
    res = _run(
        capsys,
        "remediate", "collapsed-aliases",
        "--subtype", "author", "--field", "name",
        "--limit", "10",
        "--dsn", clean_runtime, "--json",
    )
    assert res["rc"] == 0
    assert res["applied"] is False
    assert res["affected_count"] == 1


def test_remediate_link_endpoints_dry_run_lists_waiting_records(
    clean_runtime, capsys,
) -> None:
    """Issue #3666: the dry run names the records holding unready misses of
    the entity type and queues nothing; a ready row and another type are
    left out."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.cursor().executemany(
            "INSERT INTO okg.link_retry (deployment_name, branch_name, node_id, "
            "entity_type, entity_value, ready_at) VALUES "
            "('default', 'default', %s, %s, %s, %s)",
            [
                ("commit:a", "cms_jira_key", "CMSTRANSF-452", None),
                ("commit:b", "cms_jira_key", "CMSTRANSF-453", "2026-01-01"),
                ("commit:c", "cms_site", "T9_XX_Nowhere", None),
            ],
        )
    res = _run(
        capsys,
        "remediate", "link-endpoints",
        "--entity-type", "cms_jira_key", "--limit", "10",
        "--deployment-name", "default",
        "--dsn", clean_runtime, "--json",
    )
    assert res["rc"] == 0
    assert res["applied"] is False
    assert res["pass_name"] == "link_endpoints"
    assert res["details"]["node_ids"] == ["commit:a"]
    assert res["details"]["queued_count"] == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.extractor_retry_queue").fetchone()[0] == 0


def test_remediate_link_endpoints_refuses_an_out_of_range_limit(clean_runtime, capsys) -> None:
    rc = main([
        "remediate", "link-endpoints",
        "--entity-type", "cms_jira_key", "--limit", "0",
        "--deployment-name", "default",
        "--dsn", clean_runtime, "--json",
    ])
    assert rc == 1
    assert "between 1 and 1000" in capsys.readouterr().err


def test_remediate_link_endpoints_needs_the_deployment_named(clean_runtime, capsys) -> None:
    """No default deployment: a wrong one would quietly page no rows."""
    argv = [
        "remediate", "link-endpoints",
        "--entity-type", "cms_jira_key", "--limit", "10",
        "--dsn", clean_runtime, "--json",
    ]
    try:
        rc = main(argv)
    except SystemExit as exit_:
        rc = exit_.code
    assert rc == 2
    assert "--deployment-name" in capsys.readouterr().err


def test_multi_target_report_rejects_invalid_limit(clean_runtime, capsys) -> None:
    rc = main([
        "remediate", "multi-target-report",
        "--limit", "-1",
        "--dsn", clean_runtime, "--json",
    ])
    assert rc == 1
    assert "between 1 and 1000" in capsys.readouterr().err


def test_remediate_multi_target_report(clean_runtime, capsys) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
    res = _run(
        capsys,
        "remediate", "multi-target-report",
        "--dsn", clean_runtime, "--json",
    )
    assert res["rc"] == 0
    assert res["subcmd"] == "multi-target-report"
    assert "groups" in res

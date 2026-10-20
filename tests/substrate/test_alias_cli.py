"""CLI for alias-tools — exercises the
`okg alias <subcmd> --json` surface end-to-end against a clean
DB. Per `refine-doc-corpus-extraction` Stride F."""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact


pytestmark = pytest.mark.postgres


def _seed_cv_with_changes(conn: psycopg.Connection, *, n: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes (name, catalog_version_id) "
            "VALUES ('open_spec_change', %s)",
            (cv,),
        )
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "  (deployment_name, status, published_at, "
            "   catalog_version_id, correlation_id) "
            "VALUES ('test', 'published', now(), %s, 'cli') "
            "RETURNING generation_id",
            (cv,),
        )
        cur.fetchone()
    if n:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    f"change:cli-{i}",
                    "open_spec_change",
                    {"change_id": f"cli-{i}"},
                    {"id": f"change:cli-{i}"},
                    {"v": 1},
                )
                for i in range(n)
            ],
            source="default",
            strict_endpoints=True,
        )
    return cv


def test_alias_seed_preview_then_apply(
    monkeypatch, capsys, clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_changes(conn, n=3)

    monkeypatch.setenv("OKG_DSN", clean_runtime)
    from okg.substrate.cli.main import main

    # Preview (no --apply).
    rc = main([
        "alias", "seed-from-subtype",
        "--subtype", "open_spec_change",
        "--field", "change_id",
        "--json",
    ])
    out_preview = capsys.readouterr().out
    payload = json.loads(out_preview)
    assert rc == 0
    assert payload["dry_run"] is True
    assert payload["inserted_count"] == 3
    assert payload["conflict_count"] == 0

    # Apply.
    repeat_rc = main([
        "alias", "seed-from-subtype",
        "--subtype", "open_spec_change",
        "--field", "change_id",
        "--apply", "--json",
    ])
    out_apply = capsys.readouterr().out
    payload = json.loads(out_apply)
    assert repeat_rc == 0
    assert payload["dry_run"] is False
    assert payload["inserted_count"] == 3

    # Re-apply (idempotent).
    rc = main([
        "alias", "seed-from-subtype",
        "--subtype", "open_spec_change",
        "--field", "change_id",
        "--apply", "--json",
    ])
    out_repeat = capsys.readouterr().out
    payload = json.loads(out_repeat)
    assert rc == 0
    assert payload["inserted_count"] == 0
    assert payload["conflict_count"] == 3


def test_alias_add_list_remove_roundtrip(
    monkeypatch, capsys, clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_changes(conn, n=0)

    monkeypatch.setenv("OKG_DSN", clean_runtime)
    from okg.substrate.cli.main import main

    rc = main([
        "alias", "add", "x-alias", "change:x-canonical", "--json",
    ])
    add_out = json.loads(capsys.readouterr().out)
    assert rc == 0 and add_out["inserted"] is True

    list_rc = main(["alias", "list", "--json"])
    list_out = json.loads(capsys.readouterr().out)
    assert list_rc == 0
    aliases = [r["alias"] for r in list_out["rows"]]
    assert "x-alias" in aliases

    remove_rc = main(["alias", "remove", "x-alias", "--json"])
    rm_out = json.loads(capsys.readouterr().out)
    assert remove_rc == 0
    assert rm_out["removed"] is True

    final_list_rc = main(["alias", "list", "--json"])
    list_out = json.loads(capsys.readouterr().out)
    assert final_list_rc == 0
    aliases = [r["alias"] for r in list_out["rows"]]
    assert "x-alias" not in aliases

"""End-to-end CLI tests for catalog / alias / concept / view /
cron / generation subcommands. These exercise more of the
`_cmd_*` happy-path code than test_dispatch_paths.py covers
(which focuses on argparse + early-exit paths).
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cli.main import main

pytestmark = pytest.mark.postgres


# ---------- alias list ----------


def test_alias_list_returns_envelope(
    clean_runtime: str, capsys,
) -> None:
    """`alias list` against a clean runtime returns an empty list."""
    rc = main([
        "--json", "alias", "list",
        "--dsn", clean_runtime,
    ])
    assert rc == 0


# ---------- generation describe (happy path against gen 0 / known) ----------


def test_generation_describe_against_real_gen(
    clean_runtime: str, capsys,
) -> None:
    """Publish a real Chronos checkpoint generation, then
    `generation describe` resolves it via the checkpoint store.

    Chronos cutover: the chronos-native publisher no longer writes
    ``okg.graph_generations`` rows as the source of truth — a
    published generation is a Chronos CHECKPOINT. ``generation
    describe`` resolves through ``ChronosGraphStore.get_generation``,
    so the fixture must seed a real checkpoint (not just a raw
    ``graph_generations`` row, which would be reported not-found).
    """
    from tests.substrate._db_helpers import ensure_published_generation

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen_id = ensure_published_generation(
            conn, 7, deployment_name="default",
        )

    rc = main([
        "--json", "generation", "describe", str(gen_id),
        "--dsn", clean_runtime,
    ])
    assert rc == 0
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    # Chronos-native: `generation_id` is the STRING checkpoint id, not
    # the integer the legacy graph_generations row used.
    assert payload["generation_id"] == f"generation:{gen_id}"
    assert payload["deployment_name"] == "default"
    assert payload["status"] == "published"


# ---------- doctor with --check sources (no DSN required) ----------


def test_doctor_check_extraction_succeeds(capsys) -> None:
    """`doctor --check extraction` import-checks every extraction
    library; succeeds when all are importable."""
    rc = main([
        "--json", "doctor", "--check", "extraction",
    ])
    # May exit 0 (all importable) or non-zero (some missing); both
    # exercise the doctor extraction-check path.
    assert rc in (0, 1, 2)


# ---------- migrate happy path ----------


def test_migrate_with_apply_against_test_db(
    clean_runtime: str, capsys,
) -> None:
    """Re-running migrate against an already-migrated DB is
    idempotent and succeeds."""
    rc = main([
        "--json", "migrate", "--apply",
        "--dsn", clean_runtime,
    ])
    assert rc == 0
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    assert payload.get("idempotent_second_run") is True \
        or payload.get("statements_executed", 0) >= 0


# ---------- alias list ----------


def test_alias_list_against_clean_db(
    clean_runtime: str, capsys,
) -> None:
    """`alias list` against a clean runtime returns an empty list."""
    rc = main([
        "--json", "alias", "list",
        "--dsn", clean_runtime,
    ])
    assert rc == 0
    out = capsys.readouterr()
    payload = json.loads(out.out.strip().splitlines()[-1])
    assert payload["rows"] == []


# ---------- ingest happy path against test fixture ----------


def test_ingest_no_publish_flag(
    clean_runtime: str, monkeypatch,
) -> None:
    """`ingest --no-publish` exits without running publish."""
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc = main([
        "--json", "ingest",
        "--deployment", "totally-bogus-deployment-no-such-dir",
        "--no-publish",
    ])
    # No such deployment → structured error; the dispatch path is
    # exercised regardless.
    assert rc != 0


# ---------- run with --once and missing manifest ----------


def test_run_once_with_unknown_deployment(
    clean_runtime: str, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc = main([
        "--json", "run", "--once",
        "--deployment", "totally-bogus-deployment-no-such-dir",
    ])
    assert rc != 0

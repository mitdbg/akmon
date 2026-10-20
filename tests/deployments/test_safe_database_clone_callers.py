from pathlib import Path


REPO = Path(__file__).resolve().parents[2]


def test_snapshot_restore_delegates_destructive_work_to_safe_clone_cli():
    script = (REPO / "deployments/okg-ci/snapshot_restore.sh").read_text()
    restore_section = script.split("sanctioned safe-clone boundary", 1)[1]

    assert 'CLONE_ARGS=( db clone --restore-dump "$DUMP_FILE" --apply )' in script
    assert "OKG_CLONE_SOURCE_DSN" in restore_section
    assert "OKG_CLONE_TARGET_DSN" in restore_section
    assert "OKG_CLONE_ADMIN_DSN" in restore_section
    assert "--admin-dsn" not in restore_section
    assert "with (force)" not in restore_section.lower()
    assert "pg_terminate_backend" not in restore_section
    assert "pg_restore --" not in restore_section
    assert "drop database" not in restore_section.lower()
    assert "create database" not in restore_section.lower()


def test_benchmark_setup_contains_no_inline_database_reset():
    source = (REPO / "src/okg/bench/orchestrator.py").read_text()
    setup = source.split("def setup_db", 1)[1].split("def ", 1)[0]

    assert "safe_recreate_database(" in setup
    assert "pg_terminate_backend" not in setup
    assert "DROP DATABASE" not in setup
    assert "CREATE DATABASE" not in setup


def test_postgres_image_uses_exact_release_tag_digest_and_version_labels():
    dockerfile = (REPO / "ops/pg/Dockerfile").read_text()

    assert (
        "FROM docker.io/timescale/timescaledb-ha:pg17.10-ts2.29.0-all@sha256:"
        "d3300a7bdbc77a4ec35289eda5083ba4580aa23ba13dfc6cf77826771e5338e6"
    ) in dockerfile
    assert "ARG POSTGRES_VERSION=17.10" in dockerfile
    assert "ARG TIMESCALEDB_VERSION=2.29.0" in dockerfile
    assert 'io.okg.timescaledb.version="${TIMESCALEDB_VERSION}"' in dockerfile
    assert 'test "$DEFAULT_VERSION" = "$TIMESCALEDB_VERSION"' in dockerfile

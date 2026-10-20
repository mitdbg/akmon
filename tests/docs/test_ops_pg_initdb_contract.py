"""The ops/pg init scripts keep the shape the contributor recipe promises.

Issue #1972. ``ops/pg/initdb/01-template1-extensions.sql`` creates the
substrate extensions in ``template1`` so that a database made with a bare
``CREATE DATABASE`` -- which clones ``template1`` -- inherits them. Two ways
that silently stops being true, neither of which fails a database test:

* the ``\\c template1`` goes missing, and the extensions land in ``okg``
  again while every statement still succeeds;
* ``pg_cron`` is added, which aborts container initialization, because
  PostgreSQL accepts it only in the database ``cron.database_name`` names.

No database, no marker: this runs in the fast lane on every pull request.
The live counterpart is ``tests/substrate/test_template1_extensions.py``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INITDB = REPO_ROOT / "ops" / "pg" / "initdb"
TEMPLATE1_INITDB = INITDB / "01-template1-extensions.sql"

#: Everything a freshly created database needs. ``pg_cron`` is
#: instance-scoped and deliberately absent.
TEMPLATE1_EXTENSIONS = frozenset(
    {
        "btree_gist",
        "fuzzystrmatch",
        "pg_textsearch",
        "pg_trgm",
        "timescaledb",
        "vector",
    }
)


def _statements(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    ]


def test_template1_script_switches_database_and_omits_pg_cron() -> None:
    statements = _statements(TEMPLATE1_INITDB)
    assert any(line == "\\c template1" for line in statements), (
        "01-template1-extensions.sql must switch to template1 before creating "
        "extensions; without it they land in the okg database again and every "
        "statement still succeeds"
    )
    created = {
        line.split()[-1].rstrip(";")
        for line in statements
        if line.startswith("CREATE EXTENSION")
    }
    assert created == set(TEMPLATE1_EXTENSIONS)
    assert "pg_cron" not in created, (
        "pg_cron can be created only in the database cron.database_name names; "
        "creating it in template1 aborts initdb"
    )


def test_template1_script_runs_after_the_okg_extensions() -> None:
    """The entrypoint runs these in lexical order, and order matters.

    ``00-extensions.sql`` ends connected to ``okg``; the template1 script
    switches away for the rest of its own file. Reverse them and the roles
    and extensions ``00`` creates would be created in ``template1`` instead.
    """

    scripts = sorted(p.name for p in INITDB.glob("*.sql"))
    assert scripts == ["00-extensions.sql", "01-template1-extensions.sql"]

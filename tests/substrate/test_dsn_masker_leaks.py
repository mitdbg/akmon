"""DSN maskers that leaked or crashed (#2285 items 3-5), and a delegate guard.

Every value here is a fake. Each masker is called the way its caller calls
it, and the secret must not appear in what it returns or raises.
"""

from __future__ import annotations

import ast
import logging
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from okg.substrate.db import connection as db_connection
from okg.substrate.db import dsn as shared_dsn

REPO_ROOT = Path(__file__).resolve().parents[2]
OKG_ROOT = REPO_ROOT / "src" / "okg"

#: A fake password, split so a raw delimiter can sit between its halves.
HEAD = "FakePwHead2285"
TAIL = "FakePwTail2285"


# ---------------------------------------------------------------------------
# Item 3: runtime/bootstrap logged passwords holding ":", ?password= and the
# keyword form.
# ---------------------------------------------------------------------------

BOOTSTRAP_DSNS = {
    "colon-in-password": (f"postgresql://u:{HEAD}:{TAIL}@127.0.0.1:5432/d", (HEAD, TAIL)),
    "query-password": (f"postgresql://u@127.0.0.1:5432/d?password={TAIL}", (TAIL,)),
    "keyword-password": (f"host=127.0.0.1 dbname=d password={TAIL}", (TAIL,)),
    "raw-at-in-password": (f"postgresql://u:{HEAD}@{TAIL}@127.0.0.1/d", (HEAD, TAIL)),
}


@pytest.mark.parametrize("case", sorted(BOOTSTRAP_DSNS))
def test_bootstrap_never_logs_a_password(
    case: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.runtime import bootstrap

    dsn, secrets = BOOTSTRAP_DSNS[case]

    class Stop(Exception):
        pass

    def refuse(*_args: Any, **_kwargs: Any) -> Any:
        raise Stop

    monkeypatch.setattr(bootstrap, "db_connect", refuse)
    caplog.set_level(logging.INFO, logger=bootstrap.log.name)
    with pytest.raises(Stop):
        bootstrap.bootstrap_dbos_schema(
            deployment="fixture", dsn=dsn, runtime=SimpleNamespace(dbos_schema="dbos"),
        )

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "bootstrap_dbos_schema: deployment=fixture" in logged
    assert shared_dsn.MASK in logged
    for secret in secrets:
        assert secret not in logged


# ---------------------------------------------------------------------------
# Item 4: deployment_env_doctor raised on a password holding "/".
# ---------------------------------------------------------------------------

ENV_DOCTOR_VALUES = {
    "slash-in-password": f"postgresql://u:{HEAD}/{TAIL}@127.0.0.1:5432/d",
    "hash-in-password": f"postgresql://u:{HEAD}#{TAIL}@127.0.0.1:5432/d",
    "question-in-password": f"postgresql://u:{HEAD}?{TAIL}@127.0.0.1:5432/d",
    # A password that starts with digits parses as a port, so only the `@`
    # after the authority shows the split is wrong; the tail would be shown
    # as the database, or the digits as the port.
    "digits-then-slash": f"postgresql://u:2285/{TAIL}@127.0.0.1:5432/d",
    "digits-then-question": f"postgresql://u:2285?{TAIL}@127.0.0.1:5432/d",
    "digits-then-hash": f"postgresql://u:2285#{TAIL}@127.0.0.1:5432/d",
    "letters-as-port": f"postgresql://u:{HEAD}@127.0.0.1:{TAIL}/d",
    "port-out-of-range": f"postgresql://u:{HEAD}@127.0.0.1:99999/d",
}


@pytest.mark.parametrize("case", sorted(ENV_DOCTOR_VALUES))
def test_env_doctor_withholds_a_dsn_it_cannot_read_instead_of_raising(case: str) -> None:
    from okg.substrate import deployment_env_doctor as doctor

    value = ENV_DOCTOR_VALUES[case]

    shown = doctor.redact_env_value(value, key="OKG_DSN")
    commands = doctor._recommended_commands(
        "fixture", unresolved=["OKG_DSN"], resolved=False, suggested_dsn=value,
    )

    assert shown == shared_dsn.REDACTED_DSN
    rendered = shown + "\n".join(commands)
    assert HEAD not in rendered and TAIL not in rendered


@pytest.mark.parametrize(("value", "shown"), [
    ("postgres://u:FakePw2285@localhost:5432/wrong_db", "postgres://<redacted>@localhost:5432/wrong_db"),
    ("postgresql://127.0.0.1/d", "postgresql://<redacted>@127.0.0.1/d"),
    ("postgresql://u:FakePw2285@[::1]:5432/d", "postgresql://<redacted>@[::1]:5432/d"),
    # Any scheme: a token in the user slot is hidden, not only a password.
    ("https://FakeToken2285@example.invalid/v1", "https://<redacted>@example.invalid/<redacted>"),
    # Only a database scheme's path is shown: a webhook's path is its token.
    ("https://hooks.example.invalid/services/T0FAKE/B0FAKE/FakeHookToken2285",
     "https://<redacted>@hooks.example.invalid/<redacted>"),
    ("redis://:FakePw2285@cache.invalid:6379/0", "redis://<redacted>@cache.invalid:6379/0"),
])
def test_env_doctor_still_shows_the_target_of_a_readable_url(value: str, shown: str) -> None:
    from okg.substrate import deployment_env_doctor as doctor

    assert doctor.redact_env_value(value, key="OKG_DSN") == shown


# ---------------------------------------------------------------------------
# Item 5: okg's connection factory repeated libpq's parse error, which quotes
# the password fragment it choked on; Chronos showed a password's tail.
# ---------------------------------------------------------------------------

UNPARSEABLE_DSNS = {
    "keyword-password-with-blank": f"host=127.0.0.1 dbname=d password={HEAD} {TAIL}",
    "bad-percent-escape": f"postgresql://u:{HEAD}%zz{TAIL}@127.0.0.1/d",
    "bare-word": f"{HEAD}{TAIL}",
}


@pytest.mark.parametrize("case", sorted(UNPARSEABLE_DSNS))
def test_connect_refuses_an_unparseable_dsn_without_quoting_it(
    case: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = UNPARSEABLE_DSNS[case]
    reached: list[str] = []
    monkeypatch.setattr(
        db_connection.psycopg, "connect",
        lambda conninfo, **_kw: reached.append(conninfo),
    )

    with pytest.raises(psycopg.ProgrammingError) as info:
        db_connection.connect(dsn)

    assert reached == []
    assert str(info.value).startswith("libpq could not parse the connection string")
    assert info.value.__cause__ is None and info.value.__context__ is None
    assert HEAD not in str(info.value) and TAIL not in str(info.value)


@pytest.mark.parametrize("dsn", [
    "postgresql://u:FakePw2285@127.0.0.1:5432/d?sslmode=disable",
    "host=127.0.0.1 dbname=d password='two words'",
    # libpq keywords that okg's form detector does not list still parse.
    "target_session_attrs=any",
    "",
])
def test_connect_passes_a_dsn_libpq_accepts_to_psycopg(
    dsn: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Reached(Exception):
        pass

    def reached(conninfo: str, **_kw: Any) -> Any:
        raise Reached(conninfo)

    monkeypatch.setattr(db_connection.psycopg, "connect", reached)

    with pytest.raises(Reached):
        db_connection.connect(dsn)


def test_chronos_does_not_show_a_password_tail_as_the_database() -> None:
    from chronos_core.branching.sql_adapters import connect_sql_database

    for delimiter in ("/", "?", "#"):
        with pytest.raises(ValueError) as info:
            connect_sql_database(f"mysql://u:{HEAD}{delimiter}{TAIL}@h/d")
        assert "scheme=mysql" in str(info.value)
        assert HEAD not in str(info.value) and TAIL not in str(info.value)


# ---------------------------------------------------------------------------
# Review round 2: the DSN in error text on three more paths.
# ---------------------------------------------------------------------------

MIGRATE_DSN = f"postgresql://u:{HEAD}@127.0.0.1:5432/d?password={TAIL}"


@pytest.mark.parametrize("dbos_binary", [True, False], ids=["dbos-binary", "python-c-fallback"])
def test_a_failed_dbos_migrate_never_shows_the_dsn(
    dbos_binary: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import shutil
    import subprocess
    import sys

    from okg.substrate.runtime import bootstrap

    python = tmp_path / "bin" / "python"
    python.parent.mkdir()
    if dbos_binary:
        (python.parent / "dbos").write_text("", encoding="utf-8")
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    # The CLI echoes the URL it was given, once whole and once cut by the
    # 2,000-character limit inside the password.
    cut = "x" * (2000 - len("postgresql://u:") - 4) + MIGRATE_DSN
    monkeypatch.setattr(subprocess, "run", lambda cmd, **_kw: subprocess.CompletedProcess(
        cmd, 1, stdout=cut, stderr=f"could not connect to {MIGRATE_DSN}",
    ))

    with pytest.raises(RuntimeError) as info:
        bootstrap.run_dbos_migrate(dsn=MIGRATE_DSN)

    message = str(info.value)
    assert "dbos migrate failed (exit 1)" in message
    assert HEAD[:4] not in message and TAIL not in message
    assert shared_dsn.MASK in message


def test_the_mcp_entry_never_prints_the_dsn(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.mcp import deployment_entry

    dsn = f"postgresql://u:{HEAD}@127.0.0.1:5432/d"

    def unreachable(conninfo: str, **_kw: Any) -> Any:
        raise psycopg.OperationalError(f"connection to {conninfo} failed")

    monkeypatch.setattr(db_connection, "connect", unreachable)
    with pytest.raises(SystemExit):
        deployment_entry._preflight(dsn, "fixture")
    unreachable_err = capsys.readouterr().err

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *_exc: Any) -> None:
            return None

        def execute(self, *_args: Any) -> None:
            return None

        def fetchone(self) -> tuple[int]:
            return (0,)

    class Conn(Cursor):
        def cursor(self) -> Cursor:
            return Cursor()

    monkeypatch.setattr(db_connection, "connect", lambda _dsn, **_kw: Conn())
    with pytest.raises(SystemExit):
        deployment_entry._preflight(dsn, "fixture")
    unpublished_err = capsys.readouterr().err

    assert "could not reach substrate DSN" in unreachable_err
    assert "no published generation" in unpublished_err
    for err in (unreachable_err, unpublished_err):
        assert HEAD not in err


def test_an_unreadable_outcome_source_never_shows_the_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.qa.complete_reference import IncompleteSnapshotPin, OutcomeReplaySource

    dsn = f"postgresql://u:{HEAD}@127.0.0.1:5432/d"

    def unreachable(conninfo: str, **_kw: Any) -> Any:
        raise psycopg.OperationalError(f"connection to {conninfo} failed")

    monkeypatch.setattr(db_connection, "connect", unreachable)
    with pytest.raises(IncompleteSnapshotPin) as info:
        OutcomeReplaySource.from_dsn(dsn, "fixture")

    assert "outcome source unreadable" in str(info.value)
    assert HEAD not in str(info.value)


def test_chronos_describes_a_bracket_in_a_password_without_raising() -> None:
    from chronos_core.branching.sql_adapters import _describe_dsn

    for bracket in ("[", "]"):
        described = _describe_dsn(f"mysql://u:{HEAD}{bracket}{TAIL}@h/d")
        assert HEAD not in described and TAIL not in described


# ---------------------------------------------------------------------------
# Guard: a masker named for DSNs outside the shared module may only hand its
# argument to the shared one. Seen: a def or an async def, and a lambda
# assigned to a name, whose name pairs a DSN word with a masking or logging
# verb in any order (`_redact_dsn`, `mask_conninfo`, `_dsn_for_log`,
# `scrubDatabaseUrl`).
# ---------------------------------------------------------------------------

#: Name words that say "this shows a DSN safely". Matched as the start of one
#: word of the name, so `catalog` is not `log`.
_MASKING_VERB = re.compile(r"^(redact|mask|scrub|hide|sanitiz|censor|log|display|shown)")
#: Name words, or joined word pairs, that say "this is a DSN".
_DSN_WORDS = frozenset({"dsn", "conninfo"})
_DSN_WORD_PAIRS = frozenset({
    ("connection", "string"), ("conn", "str"), ("database", "url"), ("db", "url"),
})
_SHARED_MODULES = ("okg.substrate.db.dsn", "okg.substrate.db.connection")
_NAME_WORD_RE = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|[0-9]+")


def _name_words(name: str) -> list[str]:
    return [
        word.lower()
        for chunk in name.split("_")
        for word in _NAME_WORD_RE.findall(chunk)
    ]


def _is_dsn_masker_name(name: str) -> bool:
    words = _name_words(name)
    names_dsn = bool(_DSN_WORDS.intersection(words)) or any(
        pair in _DSN_WORD_PAIRS for pair in zip(words, words[1:])
    )
    return names_dsn and any(_MASKING_VERB.match(word) for word in words)


def _shared_imports(nodes: list[ast.stmt]) -> set[str] | None:
    """Names imported from the shared modules by leading imports, or None
    when a leading import comes from anywhere else."""
    imported: set[str] = set()
    for node in nodes:
        if not isinstance(node, ast.ImportFrom):
            break
        if node.module not in _SHARED_MODULES:
            return None
        imported.update(alias.asname or alias.name for alias in node.names)
    return imported


def _module_shared_imports(tree: ast.Module) -> set[str]:
    return {
        alias.asname or alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module in _SHARED_MODULES
        for alias in node.names
    }


def _calls_shared(expr: ast.expr | None, imported: set[str]) -> bool:
    return (
        isinstance(expr, ast.Call)
        and isinstance(expr.func, ast.Name)
        and expr.func.id in imported
    )


def _delegates_to_shared(
    func: ast.FunctionDef | ast.AsyncFunctionDef, module_imports: set[str],
) -> bool:
    """True when ``func`` is a docstring, shared imports and one ``return call``."""
    body = list(func.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    local = _shared_imports(body)
    if local is None:
        return False
    body = [node for node in body if not isinstance(node, ast.ImportFrom)]
    return (
        len(body) == 1
        and isinstance(body[0], ast.Return)
        and _calls_shared(body[0].value, local | module_imports)
    )


def _private_dsn_maskers(tree: ast.Module) -> list[tuple[int, str]]:
    module_imports = _module_shared_imports(tree)
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_dsn_masker_name(node.name) and not _delegates_to_shared(node, module_imports):
                found.append((node.lineno, node.name))
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Lambda):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and _is_dsn_masker_name(target.id)
                    and not _calls_shared(node.value.body, module_imports)
                ):
                    found.append((node.lineno, target.id))
    return found


@pytest.mark.parametrize(("source", "flagged"), [
    ("def _redact_dsn(v):\n    return v.split('@')[-1]\n", ["_redact_dsn"]),
    ("async def _redact_dsn(v):\n    return v\n", ["_redact_dsn"]),
    ("_scrub_dsn = lambda v: v.replace(':', '')\n", ["_scrub_dsn"]),
    ("def _dsn_for_log(v):\n    return v\n", ["_dsn_for_log"]),
    ("def maskDatabaseUrl(v):\n    return v\n", ["maskDatabaseUrl"]),
    ("def _shown_conn_str(v):\n    return v\n", ["_shown_conn_str"]),
    # Delegates, and names that only look alike, are not flagged.
    ("from okg.substrate.db.dsn import redact\n" +
     "def _redact_dsn(v):\n    return redact(v)\n", []),
    ("def _redact_dsn(v):\n    from okg.substrate.db.dsn import redact\n    return redact(v)\n", []),
    ("from okg.substrate.db.dsn import redact\n_scrub_dsn = lambda v: redact(v)\n", []),
    ("def dsn_catalog(v):\n    return v\n", []),
    ("def _assert_scratch_dsn_named_safely(v):\n    return v\n", []),
])
def test_the_private_masker_scan_sees_each_shape(source: str, flagged: list[str]) -> None:
    assert [name for _, name in _private_dsn_maskers(ast.parse(source))] == flagged


def test_no_private_dsn_masker_function_remains() -> None:
    offenders: list[str] = []
    for path in sorted(OKG_ROOT.rglob("*.py")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel == "src/okg/substrate/db/dsn.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
        offenders.extend(f"{rel}:{line} {name}" for line, name in _private_dsn_maskers(tree))
    assert offenders == [], (
        "mask DSNs with okg.substrate.db.dsn (redact, redact_text, "
        "redact_target), not a private copy:\n" + "\n".join(offenders)
    )

"""`okg doctor` warns when pg_textsearch is older than 1.5.1 (#3480, #2436).

The default doctor reads each extension's installed version. An installed
pg_textsearch below 1.5.1 is a warning with a stable code; the doctor's `ok`
does not change. A fake database connection stands in for PostgreSQL: the
check under test is the comparison and the report shape, not the query.
"""
from __future__ import annotations

import argparse
from typing import Any

import pytest

import okg.substrate.cli.doctor as dmod
from okg.substrate.db.migrate import OPTIONAL_EXTENSIONS, REQUIRED_EXTENSIONS


class _FakeCursor:
    def __init__(self, extensions: dict[str, str]) -> None:
        self._extensions = extensions
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        if sql.startswith("SELECT version()"):
            self._rows = [("PostgreSQL 17.0 on fake, compiled by fake",)]
        elif "FROM pg_extension" in sql:
            self._rows = list(self._extensions.items())
        elif "FROM pg_roles WHERE rolname = %s" in sql:
            self._rows = [(1,)]
        elif "rolsuper" in sql:
            self._rows = [(role, False) for role in params[0]]
        else:
            raise AssertionError(f"unexpected doctor query: {sql}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _FakeConnection:
    def __init__(self, extensions: dict[str, str]) -> None:
        self._extensions = extensions

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._extensions)


def _all_extensions(**overrides: str | None) -> dict[str, str]:
    versions = {name: minimum for name, minimum in REQUIRED_EXTENSIONS.items()}
    versions.update(OPTIONAL_EXTENSIONS)
    versions["pg_textsearch"] = "1.5.1"
    for name, version in overrides.items():
        if version is None:
            versions.pop(name, None)
        else:
            versions[name] = version
    return versions


def _run_doctor(
    monkeypatch: pytest.MonkeyPatch, extensions: dict[str, str]
) -> tuple[int, dict[str, Any]]:
    import okg.substrate.db.connection as connection
    import okg.substrate.local_database_roles as roles

    dmod._load_main_helpers()
    captured: dict[str, Any] = {}

    def _capture(payload: dict[str, Any], *, as_json: bool) -> None:
        captured["payload"] = payload

    monkeypatch.setattr(dmod, "_emit_ok", _capture)
    monkeypatch.setattr(
        connection, "connect", lambda dsn, **kw: _FakeConnection(extensions)
    )
    monkeypatch.setattr(roles, "builtin_password_findings", lambda dsn: [])
    args = argparse.Namespace(
        dsn="postgresql://fake@127.0.0.1:1/fake", deployment=None, check=None
    )
    code = dmod._cmd_doctor(args, as_json=True)
    return code, captured["payload"]


def _codes(payload: dict[str, Any], key: str) -> list[str]:
    return [item.get("code") for item in payload.get(key, [])]


def test_old_pg_textsearch_warns_without_failing_doctor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, payload = _run_doctor(monkeypatch, _all_extensions(pg_textsearch="1.3.0"))

    assert code == 0
    assert payload["ok"] is True
    assert payload["errors"] == []
    assert payload["extensions"]["pg_textsearch"] == "1.3.0"
    [warning] = [
        w for w in payload["warnings"] if w["code"] == "extension_outdated"
    ]
    assert warning["extension"] == "pg_textsearch"
    assert warning["installed_version"] == "1.3.0"
    assert warning["recommended_version"] == "1.5.1"
    assert "pg_textsearch 1.3.0" in warning["message"]
    assert "dropped and re-cloned" in warning["message"]
    # User-facing text carries no issue numbers.
    assert "#" not in warning["message"]
    assert "ALTER EXTENSION pg_textsearch UPDATE;" in warning["fix_hint"]
    assert "one-way" in warning["fix_hint"]
    assert "0.2.0 okg PostgreSQL image" in warning["fix_hint"]
    docs = "docs/operate/lifecycle.md#move-search-to-pg_textsearch-151"
    assert warning["docs"] == docs
    assert docs in warning["fix_hint"]


@pytest.mark.parametrize("version", ["1.5.1", "1.6.0", "1.10", "2.0"])
def test_current_pg_textsearch_is_quiet(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    code, payload = _run_doctor(monkeypatch, _all_extensions(pg_textsearch=version))

    assert code == 0
    assert payload["ok"] is True
    assert "extension_outdated" not in _codes(payload, "warnings")
    assert "extension_version_unrecognized" not in _codes(payload, "warnings")


def test_missing_pg_textsearch_keeps_the_existing_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, payload = _run_doctor(monkeypatch, _all_extensions(pg_textsearch=None))

    assert code == 1
    assert payload["ok"] is False
    assert {"code": "extension_missing", "extension": "pg_textsearch"} in (
        payload["errors"]
    )
    assert "extension_outdated" not in _codes(payload, "warnings")


def test_unreadable_version_warns_and_does_not_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, payload = _run_doctor(
        monkeypatch, _all_extensions(pg_textsearch="unknown-build")
    )

    assert code == 0
    assert payload["ok"] is True
    [warning] = [
        w for w in payload["warnings"]
        if w["code"] == "extension_version_unrecognized"
    ]
    assert warning["installed_version"] == "unknown-build"
    assert "extension_outdated" not in _codes(payload, "warnings")


@pytest.mark.parametrize(
    ("version", "outdated"),
    [
        ("1.3.0", True),
        ("1.5", True),
        ("1.5.0", True),
        ("v1.4.9", True),
        ("1.5.1", False),
        ("1.5.1-dev", False),
        ("1.10", False),
        ("1.6.0", False),
    ],
)
def test_versions_compare_numerically(version: str, outdated: bool) -> None:
    codes = [
        w["code"]
        for w in dmod._extension_version_warnings({"pg_textsearch": version})
    ]
    assert codes == (["extension_outdated"] if outdated else [])


def test_version_key_reads_leading_number_only() -> None:
    assert dmod._version_key("1.10") > dmod._version_key("1.5")
    assert dmod._version_key("1.5.1-dev") == (1, 5, 1)
    assert dmod._version_key("") is None
    assert dmod._version_key("dev") is None


def test_text_mode_prints_the_warning_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Text output, through the real `_emit_ok`, carries the warning once.

    The warning prints as one raw dict line under `warnings:`, the shape
    every other doctor warning shares.
    """
    import okg.substrate.db.connection as connection
    import okg.substrate.local_database_roles as roles

    extensions = _all_extensions(pg_textsearch="1.3.0")
    dmod._load_main_helpers()
    monkeypatch.setattr(
        connection, "connect", lambda dsn, **kw: _FakeConnection(extensions)
    )
    monkeypatch.setattr(roles, "builtin_password_findings", lambda dsn: [])
    args = argparse.Namespace(
        dsn="postgresql://fake@127.0.0.1:1/fake", deployment=None, check=None
    )

    code = dmod._cmd_doctor(args, as_json=False)
    out = capsys.readouterr().out

    assert code == 0
    assert out.count("'code': 'extension_outdated'") == 1
    assert out.count("'installed_version': '1.3.0'") == 1
    assert out.count("pg_textsearch 1.3.0 can read stale") == 1
    assert out.count("ALTER EXTENSION pg_textsearch UPDATE;") == 1

"""Local, untracked environment profile support for substrate commands.

The profile is intentionally tiny: it lets operators keep deployment
connection details out of git while giving agents a stable place to recover
the live workspace DSN across sessions.
"""
from __future__ import annotations

import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


DEFAULT_LOCAL_ENV = ".okg/local.env"

_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DISALLOWED_KEYS = {
    "OKG_TEST_DSN",
    "OKG_TEST_DSN_TEMPLATE",
    "OKG_TEST_DSN_TRUST_AS_EPHEMERAL",
}
_ALLOWED_NON_OKG_KEYS = {
    "CHRONOS_INTERVAL_CONTINUATION_PERCENT",
}


@dataclass(frozen=True)
class LocalEnvLoad:
    path: Path | None
    exists: bool
    loaded: Mapping[str, str]
    skipped: Mapping[str, str]


def find_repo_root(start: Path | None = None) -> Path | None:
    """Return the nearest repo root found by walking up from ``start``."""
    cur = (start or Path.cwd()).resolve()
    for candidate in (cur, *cur.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
        if (candidate / ".git").exists():
            return candidate
    return None


def default_local_env_path(start: Path | None = None) -> Path:
    root = find_repo_root(start) or (start or Path.cwd()).resolve()
    return root / DEFAULT_LOCAL_ENV


def configured_local_env_path(start: Path | None = None) -> Path:
    override = os.environ.get("OKG_LOCAL_ENV")
    if override:
        return Path(override).expanduser().resolve()
    return default_local_env_path(start)


def parse_local_env(path: Path) -> dict[str, str]:
    """Parse a small shell-style KEY=VALUE env file.

    Supported syntax is comments, blank lines, ``KEY=value``, and
    ``export KEY=value``. Values may be single- or double-quoted. Shell
    expansion is deliberately not performed.
    """
    values: dict[str, str] = {}
    for lineno, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            raise ValueError(f"{path}:{lineno}: expected KEY=VALUE")
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not _KEY_RE.match(key):
            raise ValueError(f"{path}:{lineno}: invalid env key {key!r}")
        value = raw_value.strip()
        if value:
            try:
                parts = shlex.split(value, posix=True)
            except ValueError as exc:
                raise ValueError(
                    f"{path}:{lineno}: invalid quoted value for {key}"
                ) from exc
            if parts:
                value = parts[0]
        values[key] = value
    return values


def load_local_env(
    *,
    path: Path | None = None,
    override: bool = False,
) -> LocalEnvLoad:
    """Load allowed local profile values into ``os.environ``.

    Explicit process environment values win unless ``override`` is true.
    ``OKG_TEST_DSN*`` keys are ignored so live workspace configuration cannot
    silently become test database configuration.
    """
    env_path = path or configured_local_env_path()
    if not env_path.is_file():
        return LocalEnvLoad(env_path, False, {}, {})

    raw_values = parse_local_env(env_path)
    loaded: dict[str, str] = {}
    skipped: dict[str, str] = {}
    for key, value in raw_values.items():
        if key in _DISALLOWED_KEYS:
            skipped[key] = "test_db_key_not_loaded_from_local_profile"
            continue
        if not key.startswith("OKG_") and key not in _ALLOWED_NON_OKG_KEYS:
            skipped[key] = "not_an_okg_key"
            continue
        if value == "":
            skipped[key] = "empty_value"
            continue
        if os.environ.get(key) and not override:
            skipped[key] = "process_env_wins"
            continue
        os.environ[key] = value
        loaded[key] = value
    return LocalEnvLoad(env_path, True, loaded, skipped)


def load_local_env_for_cli() -> LocalEnvLoad:
    """Load local env for normal CLI runs without contaminating pytest.

    Developer machines may have ``.okg/local.env`` present. Unit tests that
    expect a missing env var should not change behavior just because the local
    checkout has a profile, so implicit loading is disabled under pytest unless
    the test supplies ``OKG_LOCAL_ENV`` explicitly.
    """
    if (
        os.environ.get("PYTEST_CURRENT_TEST")
        and not os.environ.get("OKG_LOCAL_ENV")
        and os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
    ):
        return LocalEnvLoad(None, False, {}, {})
    return load_local_env()

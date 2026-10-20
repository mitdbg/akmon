"""Every Postgres OKG starts for a local graph carries the graph WAL settings.

The values live in ``okg.substrate.db.wal_settings``. The two renderers read
them directly; the tracked okg-workspace descriptor and the academic-researcher
profile compose are static files that copy them, so these tests refuse drift.
``ops/pg/docker-compose.yaml``, which CI starts its test databases from,
carries only ``wal_compression=zstd`` by operator decision, and a test checks
it has none of the other settings.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.db.wal_settings import (
    GRAPH_WAL_SETTINGS,
    graph_wal_command_args,
    resolve_wal_settings,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
# (path, compose service that runs Postgres)
STATIC_COMPOSES = (
    ("deployments/okg-workspace/docker-compose.okg-workspace.dogfood.yaml", "postgres"),
    ("src/okg/substrate/library/profiles/academic-researcher/compose.yaml", "postgres"),
)
OPS_PG_COMPOSE = ("ops/pg/docker-compose.yaml", "db")
# The one graph WAL setting the CI-facing ops/pg compose carries.
OPS_PG_WAL_SETTINGS = {"wal_compression": "zstd"}


def _settings_in_command(command: list[str]) -> dict[str, str]:
    """Return the ``-c name=value`` pairs of a postgres command, in order."""
    settings: dict[str, str] = {}
    for flag, value in zip(command, command[1:]):
        if flag != "-c":
            continue
        name, _, setting = str(value).partition("=")
        settings[name] = setting
    return settings


def _wal_drift(command: list[str]) -> dict[str, tuple[str, str | None]]:
    """Map each WAL setting a command gets wrong to (expected, observed)."""
    observed = _settings_in_command(command)
    return {
        name: (expected, observed.get(name))
        for name, expected in GRAPH_WAL_SETTINGS.items()
        if observed.get(name) != expected
    }


def _postgres_command(path: Path, service: str = "postgres") -> list[str]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [str(part) for part in document["services"][service]["command"]]


def test_command_args_are_name_value_pairs() -> None:
    args = graph_wal_command_args()
    assert args[0::2] == ["-c"] * len(GRAPH_WAL_SETTINGS)
    assert _settings_in_command(args) == dict(GRAPH_WAL_SETTINGS)
    # The measured problem was the 1GB initdb cap forcing a checkpoint per
    # gigabyte of WAL; the shipped cap must be larger and compressed.
    assert GRAPH_WAL_SETTINGS["max_wal_size"] != "1GB"
    assert GRAPH_WAL_SETTINGS["wal_compression"] == "zstd"


@pytest.mark.parametrize(("relative", "service"), STATIC_COMPOSES)
def test_static_compose_carries_the_wal_settings(relative: str, service: str) -> None:
    command = _postgres_command(REPO_ROOT / relative, service)
    assert _wal_drift(command) == {}


def _ops_pg_wal_mismatch(command: list[str]) -> dict[str, tuple[str | None, str | None]]:
    """Map each graph WAL setting ops/pg gets wrong to (expected, observed)."""
    observed = _settings_in_command(command)
    return {
        name: (OPS_PG_WAL_SETTINGS.get(name), observed.get(name))
        for name in GRAPH_WAL_SETTINGS
        if observed.get(name) != OPS_PG_WAL_SETTINGS.get(name)
    }


def test_ops_pg_compose_carries_only_wal_compression() -> None:
    command = _postgres_command(REPO_ROOT / OPS_PG_COMPOSE[0], OPS_PG_COMPOSE[1])
    assert OPS_PG_WAL_SETTINGS["wal_compression"] == GRAPH_WAL_SETTINGS["wal_compression"]
    assert _ops_pg_wal_mismatch(command) == {}


def test_ops_pg_check_names_a_sizing_setting_that_comes_back() -> None:
    """The ops/pg check fails if checkpoint sizing reappears or zstd is lost."""
    command = _postgres_command(REPO_ROOT / OPS_PG_COMPOSE[0], OPS_PG_COMPOSE[1])
    index = command.index("wal_compression=zstd")
    regressed = [*command[: index - 1], *command[index + 1 :], "-c", "max_wal_size=4GB"]
    assert _ops_pg_wal_mismatch(regressed) == {
        "max_wal_size": (None, "4GB"),
        "wal_compression": ("zstd", None),
    }


def test_drift_check_names_a_missing_or_changed_setting() -> None:
    """The drift check fails loudly when a copy loses or changes a value."""
    command = _postgres_command(REPO_ROOT / STATIC_COMPOSES[0][0])
    index = command.index("max_wal_size=" + GRAPH_WAL_SETTINGS["max_wal_size"])
    stale = [*command[: index - 1], *command[index + 1 :]]
    stale = [
        "wal_compression=off" if part.startswith("wal_compression=") else part
        for part in stale
    ]
    assert _wal_drift(stale) == {
        "max_wal_size": (GRAPH_WAL_SETTINGS["max_wal_size"], None),
        "wal_compression": ("zstd", "off"),
    }


def test_rendered_dogfood_compose_matches_the_tracked_descriptor(capsys) -> None:
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "docker-compose",
        "--repo-root",
        "/tmp/okg",
        "--okg-bin",
        "uv run okg",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["postgres"]["wal_settings"] == dict(GRAPH_WAL_SETTINGS)
    rendered = yaml.safe_load(payload["content"])
    command = [str(part) for part in rendered["services"]["postgres"]["command"]]
    assert _wal_drift(command) == {}
    # The tracked descriptor is this renderer's output; its server command
    # must not fall behind the renderer.
    assert command == _postgres_command(REPO_ROOT / STATIC_COMPOSES[0][0])


def test_resolve_overlays_known_settings_and_keeps_the_rest() -> None:
    resolved = resolve_wal_settings({"max_wal_size": "2GB", "checkpoint_completion_target": 0.8})
    assert resolved["max_wal_size"] == "2GB"
    assert resolved["checkpoint_completion_target"] == "0.8"
    assert resolved["wal_compression"] == GRAPH_WAL_SETTINGS["wal_compression"]
    assert list(resolved) == list(GRAPH_WAL_SETTINGS)
    assert resolve_wal_settings(None) == GRAPH_WAL_SETTINGS


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"max_wal_sise": "2GB"}, "unknown setting(s) max_wal_sise"),
        ({"shared_buffers": "1GB"}, "unknown setting(s) shared_buffers"),
        ({"max_wal_size": "2GB -c fsync=off"}, "not a plain setting value"),
        ({"wal_compression": True}, "must be a string or number"),
        (["max_wal_size=2GB"], "must be a mapping"),
    ],
)
def test_resolve_refuses_unknown_names_and_unsafe_values(overrides, message) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        resolve_wal_settings(overrides)

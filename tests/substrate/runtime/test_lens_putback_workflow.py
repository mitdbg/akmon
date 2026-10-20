"""Tests for the lens-putback DBOS workflow + schedule.

Covers:
  * pure-function helpers (`_parse_iso8601_duration`,
    `_resolve_lens_putback_config`) without spinning a DBOS singleton.
  * exports / registration shape (no DBOS singleton — just imports).
  * the doctor probe `check_lens_putback_backlog` on the empty path.
    (Non-empty backlog coverage lives in `test_lens_putback_sweep.py`,
    which uses the proper `prepare_lens_putback` flow to seed rows.)

The end-to-end workflow firing is exercised by the cutover-time
verification step (see proposal's task 15), not here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.runtime.lens_putback_workflow import (
    LENS_PUTBACK_WORKFLOW_NAME,
    _parse_iso8601_duration,
    _resolve_lens_putback_config,
)
from okg.substrate.runtime.lens_putback_schedule import (
    LENS_PUTBACK_DEFAULT_CRON,
    LENS_PUTBACK_SCHEDULE_ID,
)
from okg.substrate.runtime.publisher_health import (
    LensPutbackBacklogVerdict,
    check_lens_putback_backlog,
)


# ---------------------------------------------------------------------------
# Pure-function: ISO-8601 duration parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("PT1H", timedelta(hours=1)),
        ("PT30M", timedelta(minutes=30)),
        ("PT45S", timedelta(seconds=45)),
        ("PT1H30M", timedelta(hours=1, minutes=30)),
        ("P1D", timedelta(days=1)),
        ("P1DT12H", timedelta(days=1, hours=12)),
    ],
)
def test_parse_iso8601_duration(value, expected):
    assert _parse_iso8601_duration(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "1H",         # missing P prefix
        "PT1Y",       # year unit not supported
        "PT1M30",     # trailing digits with no unit
        "P1Z",        # Z is not a valid unit
    ],
)
def test_parse_iso8601_duration_rejects_invalid(value):
    with pytest.raises(ValueError):
        _parse_iso8601_duration(value)


# ---------------------------------------------------------------------------
# Pure-function: manifest config resolution
# ---------------------------------------------------------------------------


@dataclass
class _StubRuntime:
    lens_putback: dict[str, Any] = field(default_factory=dict)


@dataclass
class _StubManifest:
    runtime: _StubRuntime = field(default_factory=_StubRuntime)


def test_resolve_config_defaults():
    """An empty lens_putback block yields the library defaults."""
    manifest = _StubManifest()
    config = _resolve_lens_putback_config(manifest)
    assert config["stale_after"] == timedelta(hours=1)
    assert config["policy"] == "sweep_to_failed"


def test_resolve_config_honors_overrides():
    manifest = _StubManifest(
        runtime=_StubRuntime(
            lens_putback={
                "stale_after": "PT30M",
                "policy": "auto_commit",
            },
        )
    )
    config = _resolve_lens_putback_config(manifest)
    assert config["stale_after"] == timedelta(minutes=30)
    assert config["policy"] == "auto_commit"


def test_resolve_config_rejects_unknown_policy():
    manifest = _StubManifest(
        runtime=_StubRuntime(
            lens_putback={"policy": "delete_everything"},
        )
    )
    with pytest.raises(ValueError, match="must be"):
        _resolve_lens_putback_config(manifest)


def test_resolve_config_tolerates_missing_runtime():
    """A manifest with no runtime attribute (e.g. legacy stub in tests)
    still resolves to defaults rather than raising."""

    class _Bare:
        pass

    config = _resolve_lens_putback_config(_Bare())
    assert config["stale_after"] == timedelta(hours=1)
    assert config["policy"] == "sweep_to_failed"


# ---------------------------------------------------------------------------
# Identity / export shape
# ---------------------------------------------------------------------------


def test_workflow_name_constant_is_stable():
    """Operators and other modules reference this name; pin it."""
    assert LENS_PUTBACK_WORKFLOW_NAME == "okg_lens_putback_sweep"


def test_schedule_constants_pinned():
    assert LENS_PUTBACK_SCHEDULE_ID == "okg_lens_putback_sweep"
    # 5-field cron, every 5 minutes.
    assert LENS_PUTBACK_DEFAULT_CRON == "*/5 * * * *"


# ---------------------------------------------------------------------------
# Doctor probe: empty-state + verdict shape
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.postgres


def _ensure_putback_schema(dsn: str) -> None:
    """Skip the test if `okg.lens_putback_requests` is missing from the
    DSN — the clean_runtime fixture handles migration, this is a guard
    for fixture order changes."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('okg.lens_putback_requests')"
            )
            if cur.fetchone()[0] is None:
                pytest.skip("okg.lens_putback_requests missing in test DSN")


def test_backlog_probe_returns_zero_on_empty(clean_runtime: str) -> None:
    """No putback rows for the deployment → zero count, healthy."""
    _ensure_putback_schema(clean_runtime)
    verdict = check_lens_putback_backlog(
        deployment=f"empty-{uuid4().hex[:8]}",
        dsn=clean_runtime,
        warn_threshold=10,
        runaway_threshold=100,
        age_floor_seconds=600,
    )
    assert isinstance(verdict, LensPutbackBacklogVerdict)
    assert verdict.ok is True
    assert verdict.code == "lens_putback_backlog_healthy"
    assert verdict.count == 0
    assert verdict.warn_threshold == 10
    assert verdict.runaway_threshold == 100


def test_backlog_verdict_to_dict_shape() -> None:
    """Verdict.to_dict serializes the expected keys (no DB needed)."""
    v = LensPutbackBacklogVerdict(
        deployment="test",
        ok=True,
        code="lens_putback_backlog_warn",
        message="50 prepared lens-putback requests older than 600s",
        count=50,
        warn_threshold=10,
        runaway_threshold=100,
        age_floor_seconds=600,
    )
    d = v.to_dict()
    assert d == {
        "deployment": "test",
        "ok": True,
        "code": "lens_putback_backlog_warn",
        "message": "50 prepared lens-putback requests older than 600s",
        "count": 50,
        "warn_threshold": 10,
        "runaway_threshold": 100,
        "age_floor_seconds": 600,
    }

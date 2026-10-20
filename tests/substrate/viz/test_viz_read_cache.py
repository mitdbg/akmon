"""Generation-keyed read cache + ETag revalidation on the console endpoints.

Drives the real ``build_app`` middleware with a fake context whose pinned
generation the test controls, and a call-counting stub for the health composer,
so a cache HIT is observable as "the composer ran ONCE" without timing. Covers:
a repeat read is a HIT, a generation advance busts the cache, ``If-None-Match``
returns 304, an advance never 304s a client into a stale generation, and an
explicit refresh bypasses the cache. (PACT operator-ui-cache-and-refresh)

``/console/health`` is the cacheable route these cases drive. They used to drive
``/console/graph`` and ``/console/graph-batch``; both routes left with the Graph
view in issue #1924, so the cache behaviour is pinned on a route that survives.
"""
from __future__ import annotations

from contextlib import contextmanager

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

import okg.substrate.console_views.health as health_views  # noqa: E402
from okg.viz.app import build_app  # noqa: E402
from okg.viz.read_cache import CACHEABLE_PATHS  # noqa: E402

# Mutable holders: _GEN is the currently-pinned generation; _LATEST is what an
# explicit refresh would re-resolve to (simulating the worker publishing newer).
_GEN = {"id": "gen-A"}
_LATEST = {"id": "gen-A"}

HEALTH_PATH = "/viz/api/console/health"


class _Sess:
    branch_name = "default"
    compat_generation_id = None

    @property
    def generation_id(self):
        return _GEN["id"]


class _Ctx:
    deployment_name = "demo"
    deployment_dir = None
    dsn = None
    session = _Sess()
    health_snapshot = {"node_count": 1, "edge_count": 2}

    @contextmanager
    def conn(self):
        yield None

    def refresh_to_latest(self):
        # Mirror VizContext.refresh_to_latest: re-pin to the latest published gen.
        _GEN["id"] = _LATEST["id"]
        return _GEN["id"]


@pytest.fixture(autouse=True)
def _no_producers_read(monkeypatch):
    """The health route also composes the producers read for its source
    breakdown (PACT console-archiv3-read-layer-fixes). These cases are about
    the cache, so that read answers with no sources and touches nothing."""
    import okg.substrate.console_views.source_diagnostics as source_diagnostics

    monkeypatch.setattr(
        source_diagnostics, "compose_source_diagnostics",
        lambda conn, **_: {"sources": []},
    )


@pytest.fixture
def client(monkeypatch):
    _GEN["id"] = "gen-A"
    _LATEST["id"] = "gen-A"
    calls = {"n": 0}

    def fake_compose_health(conn, *, deployment_name, session, **_producer_states):
        calls["n"] += 1
        return {"verdict": "ok", "checks": [], "deployment_name": deployment_name}

    monkeypatch.setattr(health_views, "compose_health", fake_compose_health)
    app = build_app(ctx_provider=_Ctx, is_ready=lambda: True)
    c = TestClient(app)
    c.calls = calls  # type: ignore[attr-defined]
    return c


def _get(c, **headers):
    return c.get(HEALTH_PATH, headers=headers)


def test_the_driven_route_is_actually_on_the_cacheable_allowlist():
    """Guards the rebase: if health ever leaves CACHEABLE_PATHS these cases
    would pass vacuously (every response a MISS with no store behind it)."""
    assert HEALTH_PATH in CACHEABLE_PATHS


def test_repeat_read_is_served_from_cache_composer_runs_once(client):
    r1 = _get(client)
    assert r1.status_code == 200
    assert r1.headers["X-Cache"] == "MISS"
    assert "ETag" in r1.headers
    r2 = _get(client)
    assert r2.status_code == 200
    assert r2.headers["X-Cache"] == "HIT"
    # The composer ran exactly once across two identical-generation reads.
    assert client.calls["n"] == 1
    assert r2.json() == r1.json()


def test_if_none_match_returns_304_without_recompute(client):
    r1 = _get(client)
    etag = r1.headers["ETag"]
    r304 = _get(client, **{"If-None-Match": etag})
    assert r304.status_code == 304
    assert r304.content == b""
    assert client.calls["n"] == 1  # no recompute


def test_generation_advance_busts_cache_and_never_serves_stale(client):
    r1 = _get(client)
    etag_a = r1.headers["ETag"]
    assert client.calls["n"] == 1
    # A new generation is published.
    _GEN["id"] = "gen-B"
    # The old ETag must NOT 304 the client into the stale generation.
    r2 = _get(client, **{"If-None-Match": etag_a})
    assert r2.status_code == 200
    assert r2.headers["X-Cache"] == "MISS"
    assert r2.headers["ETag"] != etag_a
    assert client.calls["n"] == 2  # recomputed for the new generation


def test_refresh_bypasses_cache(client):
    _get(client)
    assert client.calls["n"] == 1
    r = client.get(f"{HEALTH_PATH}?refresh=1")
    assert r.status_code == 200
    assert r.headers["X-Cache"] == "BYPASS"
    assert client.calls["n"] == 2  # bypass forced a recompute


def test_refresh_reresolves_latest_generation(client):
    r1 = _get(client)
    etag_a = r1.headers["ETag"]
    assert _GEN["id"] == "gen-A"
    # The worker publishes a newer generation.
    _LATEST["id"] = "gen-B"
    # A refresh re-resolves to it (relatest), bypasses the cache, returns fresh.
    r = client.get(f"{HEALTH_PATH}?refresh=1")
    assert r.status_code == 200
    assert r.headers["X-Cache"] == "BYPASS"
    assert r.headers["ETag"] != etag_a  # keyed on the new generation
    assert _GEN["id"] == "gen-B"
    # A subsequent normal read now hits the cache at the NEW generation.
    r2 = _get(client)
    assert r2.headers["X-Cache"] == "HIT"
    assert r2.headers["ETag"] == r.headers["ETag"]


def test_liveness_probe_is_not_cached(client):
    _LATEST["id"] = "gen-B"
    r = client.get("/viz/api/health")
    assert "X-Cache" not in r.headers
    assert r.status_code == 200
    assert r.json()["generation_id"] == "gen-B"
    assert _GEN["id"] == "gen-B"


def test_store_keys_on_body_generation_not_the_preread(monkeypatch):
    """Regression for the cross-generation poisoning race: the store must key on
    the generation in the RESPONSE BODY, not the generation the middleware read
    before the handler ran. We simulate a refresh swapping the shared session's
    generation between the middleware's read and the handler's read by returning
    gen-A on the first access and gen-B thereafter."""
    accesses = {"n": 0}

    class _RaceSess:
        branch_name = "default"
        compat_generation_id = None

        @property
        def generation_id(self):
            accesses["n"] += 1
            return "gen-A" if accesses["n"] == 1 else "gen-B"

    class _RaceCtx:
        deployment_name = "demo"
        deployment_dir = None
        dsn = None
        session = _RaceSess()

        @contextmanager
        def conn(self):
            yield None

    calls = {"n": 0}

    def fake_compose_health(conn, *, deployment_name, session, **_producer_states):
        calls["n"] += 1
        return {"verdict": "ok", "checks": []}

    monkeypatch.setattr(health_views, "compose_health", fake_compose_health)
    c = TestClient(build_app(ctx_provider=_RaceCtx, is_ready=lambda: True))

    # Request 1: middleware reads gen-A (key); handler labels the body gen-B.
    r1 = c.get(HEALTH_PATH)
    assert r1.status_code == 200
    assert r1.json()["generation_id"] == "gen-B"  # body's own generation
    # Request 2 (steady-state gen-B): a HIT proves the entry was stored under the
    # BODY's gen-B, not the pre-read gen-A (which would MISS and recompute).
    r2 = c.get(HEALTH_PATH)
    assert r2.headers["X-Cache"] == "HIT"
    assert calls["n"] == 1


def test_cache_hit_carries_cors_headers(client):
    origin = "http://localhost:8768"
    r1 = _get(client, Origin=origin)
    assert r1.headers["X-Cache"] == "MISS"
    assert r1.headers.get("access-control-allow-origin") == origin
    r2 = _get(client, Origin=origin)
    assert r2.headers["X-Cache"] == "HIT"
    # The short-circuit HIT must still carry CORS (the cross-origin console relies
    # on it) — proves CORS wraps the cache middleware.
    assert r2.headers.get("access-control-allow-origin") == origin


def test_cache_hit_and_304_still_carry_console_server_timing(client):
    """A cached console read must still report what the browser waited for.

    The perf gate measures ONE thing: the `console;dur` metric in
    `Server-Timing`. Registered before the read cache, the timing middleware sat
    INSIDE it, so a HIT or a 304 returned from the cache without ever reaching
    the timer and carried no `console;dur` -- the gate's ceilings for the two
    cacheable reads (`health`, `subtype`) measured nothing on the second read
    onward. Timing is registered AFTER the cache, so it wraps it and every
    outcome is measured (issue #1924).
    """
    r1 = _get(client)
    assert r1.status_code == 200
    assert r1.headers["X-Cache"] == "MISS"
    assert "console;dur=" in r1.headers.get("Server-Timing", "")

    r2 = _get(client)
    assert r2.status_code == 200
    assert r2.headers["X-Cache"] == "HIT"
    assert "console;dur=" in r2.headers.get("Server-Timing", ""), (
        "a cache HIT emitted no console;dur: the timing middleware is inside "
        "the read cache again, so the perf gate cannot measure a cached read"
    )

    r304 = _get(client, **{"If-None-Match": r1.headers["ETag"]})
    assert r304.status_code == 304
    assert "console;dur=" in r304.headers.get("Server-Timing", ""), (
        "a 304 revalidation emitted no console;dur"
    )
    # The composer ran once across all three: the cache is still a cache.
    assert client.calls["n"] == 1


def test_refresh_failure_fails_loud(monkeypatch):
    """A failed re-resolve on refresh must 503, never silently serve the stale
    boot generation (the repo's P0 no-silent-fallback rule)."""

    class _FailCtx:
        deployment_name = "demo"
        deployment_dir = None
        dsn = None
        session = _Sess()

        @contextmanager
        def conn(self):
            yield None

        def refresh_to_latest(self):
            raise RuntimeError("no published generation")

    monkeypatch.setattr(
        health_views,
        "compose_health",
        lambda conn, *, deployment_name, session, **_: {"verdict": "ok", "checks": []},
    )
    c = TestClient(build_app(ctx_provider=_FailCtx, is_ready=lambda: True))
    r = c.get(f"{HEALTH_PATH}?refresh=1")
    assert r.status_code == 503
    assert r.json()["ok"] is False

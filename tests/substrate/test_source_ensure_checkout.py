"""The runner invokes ensure_checkout() before the change-probe and never blocks.

Uses the migrated ephemeral test DB. A spy adapter records the order of
ensure_checkout / probe / run and lets each test choose the acquisition health.
Proves: ensure_checkout runs before the probe (so a freshly-materialised HEAD is
what the probe reads); a degraded acquisition health rides out on the normal
result AND on the probe short-circuit result; ensure_checkout raising is
contained (converted to acquire_failed, run still proceeds); a seam-less adapter
is unaffected.
"""
from __future__ import annotations

import hashlib

import pytest

from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.base import SourceHealth, SourceRun
from okg.substrate.library.sources.change_probe import ProbeVerdict, compose_token


pytestmark = pytest.mark.postgres


def _hex(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _token():
    return compose_token(
        content={"kind": "content_hash", "value": _hex("v1")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )


class _StubProbe:
    def __init__(self, token, events):
        self.changed = True
        self._token = token
        self._events = events

    def probe(self, *, cursor, sync_scope):
        self._events.append("probe")
        return ProbeVerdict(changed=self.changed, token=self._token)


class _SpyAdapter:
    name = "spy-acquire-source"
    profile = "discovery_crawl"

    def __init__(self, *, health=None, raise_on_ensure=False, with_probe=True):
        self.events: list[str] = []
        self.change_probe = _StubProbe(_token(), self.events) if with_probe else None
        self.run_calls = 0
        self.probe_verdicts: list[ProbeVerdict | None] = []
        self._health = health
        self._raise = raise_on_ensure

    def ensure_checkout(self):
        self.events.append("ensure")
        if self._raise:
            raise RuntimeError("boom")
        return self._health

    def run(
        self,
        run_id,
        *,
        mode="cursor",
        cursor=None,
        sync_scope=None,
        change_probe_verdict=None,
    ):
        self.events.append("run")
        self.run_calls += 1
        self.probe_verdicts.append(change_probe_verdict)
        return SourceRun(
            facts=iter(()),
            completed_scope=False,
            next_cursor={"spy_run": self.run_calls},
            run_mode=mode,
        )


class _NoSeamAdapter:
    """An adapter without ensure_checkout — must be unaffected."""

    name = "no-seam-source"
    profile = "discovery_crawl"

    def __init__(self):
        self.change_probe = _StubProbe(_token(), [])
        self.run_calls = 0

    def run(self, run_id, *, mode="cursor", cursor=None, sync_scope=None):
        self.run_calls += 1
        return SourceRun(facts=iter(()), completed_scope=False, next_cursor={})


@pytest.fixture
def runner(okg_migrated):
    return SourceRunner(
        okg_migrated,
        source_name="spy-acquire-source",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )


@pytest.fixture
def no_seam_runner(okg_migrated):
    return SourceRunner(
        okg_migrated,
        source_name="no-seam-source",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )


def test_ensure_checkout_before_probe_and_never_blocks(runner):
    adapter = _SpyAdapter(health=SourceHealth(status="acquire_stale", reason="no ff"))
    result = runner.run(adapter, mode="cursor")

    # Acquisition happens BEFORE the probe reads HEAD, and before the reader.
    assert adapter.events == ["ensure", "probe", "run"]
    assert adapter.run_calls == 1
    assert adapter.probe_verdicts == [
        ProbeVerdict(changed=True, token=_token())
    ]
    # Degraded acquisition health rides out on the normal result.
    assert result.source_health is not None
    assert result.source_health.status == "acquire_stale"


def test_degraded_health_forces_reader_and_remains_visible(runner):
    adapter = _SpyAdapter(health=SourceHealth(status="acquire_stale", reason="no ff"))

    # First run reads (changed=true) and stamps the probe token.
    runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1

    # This legacy test adapter has no exact applied-snapshot acknowledgement,
    # so even changed=false must keep reading. The degraded checkout remains
    # visible and cannot hide behind a probe-only short-circuit.
    adapter.change_probe.changed = False
    result = runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 2
    assert result.source_health is not None
    assert result.source_health.status == "acquire_stale"


def test_ensure_checkout_raise_is_contained(runner):
    # A buggy ensure_checkout that raises must NOT abort the run; the runner
    # converts it to acquire_failed and the reader still proceeds.
    adapter = _SpyAdapter(raise_on_ensure=True)
    result = runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1
    assert result.source_health is not None
    assert result.source_health.status == "acquire_failed"


def test_adapter_without_seam_unaffected(no_seam_runner):
    adapter = _NoSeamAdapter()
    result = no_seam_runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1
    assert result.source_health is None

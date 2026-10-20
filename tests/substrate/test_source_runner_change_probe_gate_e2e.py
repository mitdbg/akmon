"""End-to-end: the runner change-probe gate physically skips the reader (#323).

Uses a migrated ephemeral test DB (okg_migrated). A spy adapter records reader
invocations; a stub probe flips changed. Proves: changed=true invokes the
reader and stamps the token; a later changed=false skips the reader; a
reconcile run is never short-circuited.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.change_identity import (
    BOOTSTRAP_AUTHORITY_CURSOR_KEY,
    PipelineIdentitySnapshot,
    build_effective_token,
    identity_fingerprint,
)
from okg.substrate.ingest.runner import (
    BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
    _PROBE_SKIP_COUNT_KEY,
    _RECORD_SET_CHRONOS_PROOF_SCHEMA,
    _RECORD_SET_SANITIZATION_SCHEMA,
    SourceRunContext,
    SourceRunner,
    persist_source_watermark,
)
from okg.substrate.library.sources.base import (
    BOOTSTRAP_SNAPSHOT_MATERIALIZATION,
    BootstrapIdentityClaim,
    NodeFact,
    ProgressMarker,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    SourceHealth,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
)


pytestmark = pytest.mark.postgres


def _hex(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


class _StubProbe:
    def __init__(self, token):
        self.changed = True
        self._token = token

    def probe(self, *, cursor, sync_scope):
        return ProbeVerdict(changed=self.changed, token=self._token)


class _SpyAdapter:
    name = "spy-probe-source"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(self, probe):
        self.change_probe = probe
        self.run_calls = 0
        self.last_cursor = None
        self.applied_token = None
        self.outstanding_backlog = False
        self.raise_on_run: Exception | None = None

    def run(self, run_id, *, mode="cursor", cursor=None, sync_scope=None):
        self.run_calls += 1
        self.last_cursor = cursor
        if self.raise_on_run is not None:
            raise self.raise_on_run
        completed_scope = (
            mode in {"reconcile", "scope_complete"}
            and not self.outstanding_backlog
        )
        next_cursor = {"spy_run": self.run_calls}
        if self.outstanding_backlog:
            next_cursor["outstanding_backlog"] = [
                {"record_id": "pending-1"}
            ]
        return SourceRun(
            facts=iter(()),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            applied_raw_token=(
                None
                if self.outstanding_backlog
                else (
                    self.applied_token
                    if self.applied_token is not None
                    else self.change_probe._token
                )
            ),
            applied_token_scope=(
                "unavailable"
                if self.outstanding_backlog
                else "scoped" if sync_scope else "global"
            ),
            applied_token_authority=(
                "unavailable" if self.outstanding_backlog else "exact"
            ),
            run_mode=mode,
            record_set=(
                RecordSet() if completed_scope else None
            ),
        )


class _BootstrapAdapter:
    name = "bootstrap-spy"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True
    bootstrap_cursor_semantics = "bootstrap-spy.cursor.v1"

    def __init__(self, probe, *, terminal_after: int = 3):
        self.change_probe = probe
        self.terminal_after = terminal_after
        self.run_calls = 0
        self.received_cursors: list[dict | None] = []
        self.health_status: str | None = "ok"
        self.return_claim = True
        self.lazy_facts = False
        self.post_probe_token = None

    def run(
        self,
        run_id,
        *,
        mode="cursor",
        cursor=None,
        sync_scope=None,
        **_kwargs,
    ):
        del run_id, sync_scope
        self.run_calls += 1
        self.received_cursors.append(cursor)
        if self.post_probe_token is not None:
            self.change_probe._token = self.post_probe_token
        terminal = self.run_calls >= self.terminal_after
        node = NodeFact(
            node_id=f"bootstrap:{self.run_calls}",
            subtype="concept",
            attrs={"chunk": self.run_calls},
            source_record_id={"record_id": f"chunk-{self.run_calls}"},
            source_revision={"revision": f"chunk-{self.run_calls}"},
        )
        facts = (node, ProgressMarker(
            record_id=f"chunk-{self.run_calls}",
            fingerprint=_hex(f"chunk-{self.run_calls}"),
        ))
        if self.lazy_facts:
            facts = iter(facts)
        next_cursor = {"offset": self.run_calls}
        if not terminal:
            next_cursor["outstanding_backlog"] = [
                {"record_id": f"pending-{self.run_calls + 1}"}
            ]
        raw_token = self.change_probe._token
        return SourceRun(
            facts=facts,
            completed_scope=False if not terminal else mode == "scope_complete",
            next_cursor=next_cursor,
            applied_raw_token=raw_token if terminal else None,
            applied_token_scope="global" if terminal else "unavailable",
            applied_token_authority="exact" if terminal else "unavailable",
            bootstrap_identity=(
                BootstrapIdentityClaim(
                    raw_token=raw_token,
                    scope="consumed_slices",
                    cursor_semantics=self.bootstrap_cursor_semantics,
                    snapshot_materialization=(
                        BOOTSTRAP_SNAPSHOT_MATERIALIZATION
                    ),
                    input_cursor=dict(cursor or {}),
                )
                if not terminal and self.return_claim else None
            ),
            run_mode=mode,
            record_set=RecordSet() if terminal else None,
            health=(
                SourceHealth(status=self.health_status, mode="test")
                if self.health_status is not None else None
            ),
        )


class _BootstrapTestRunner(SourceRunner):
    """Supply the explicit transition identity production gets from admission."""

    _transition_index = 0

    def run(self, adapter, **kwargs):
        if kwargs.get("run_context") is None:
            self._transition_index += 1
            transition = f"{self.source_name}:{self._transition_index}"
            kwargs["run_context"] = SourceRunContext(
                run_id=str(uuid.uuid5(uuid.NAMESPACE_URL, transition)),
                observed_at=datetime.now(timezone.utc),
                admission_id=str(uuid.uuid5(uuid.NAMESPACE_OID, transition)),
                idempotency_key=f"test:{transition}",
            )
        result = super().run(adapter, **kwargs)
        if not result.idempotent_replay:
            with psycopg.connect(self.dsn, autocommit=True) as conn:
                persist_source_watermark(
                    conn,
                    source_name=self.source_name,
                    deployment_name=self.deployment_name,
                    next_cursor=result.next_cursor,
                    last_reconcile=(
                        kwargs["run_context"].observed_at
                        if result.completed_scope else None
                    ),
                    clear_replay_required=result.clear_replay_required,
                    replay_target_fingerprint=(
                        str(result.replay_target["target_fingerprint"])
                        if result.replay_target else None
                    ),
                    replay_target_run_id=(
                        str(result.replay_target["run_id"])
                        if result.replay_target else None
                    ),
                    expected_last_applied_rev=result.expected_last_applied_rev,
                    require_compare_and_swap=result.watermark_compare_and_swap,
                    expected_bootstrap_ledger=result.expected_bootstrap_ledger,
                    next_bootstrap_ledger=result.next_bootstrap_ledger,
                )
        return result


def _bootstrap_runner(dsn: str, *, source_name: str = "bootstrap-spy"):
    return _BootstrapTestRunner(
        dsn,
        source_name=source_name,
        deployment_name="bootstrap-test",
        executable_profile="discovery_crawl",
        registry_name="bootstrap-spy-registry",
        ownership_id=source_name,
        registry_binding_fingerprint="bootstrap-registry-v1",
        write_chronos_graph=False,
        write_fact_log=True,
    )


def _bootstrap_context(seed: str) -> SourceRunContext:
    return SourceRunContext(
        run_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"run:{seed}")),
        observed_at=datetime.now(timezone.utc),
        admission_id=str(uuid.uuid5(uuid.NAMESPACE_OID, f"admission:{seed}")),
        idempotency_key=f"bootstrap-test:{seed}",
    )


class _AppendFeedSpyAdapter:
    name = "append-feed-spy"
    profile = "append_feed"
    probe_short_circuit_safe = True

    def __init__(self, probe):
        self.change_probe = probe
        self.run_calls = 0

    def run(self, run_id, *, mode="append", **_kwargs):
        del run_id, mode
        self.run_calls += 1
        return SourceRun(
            facts=iter(()),
            completed_scope=True,
            next_cursor={},
            applied_raw_token=self.change_probe._token,
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode="scope_complete",
            record_set=RecordSet(),
        )


class _AppendFeedDedupAdapter:
    name = "append-feed-dedup"
    profile = "append_feed"
    probe_short_circuit_safe = True

    def __init__(self, probe):
        self.change_probe = probe
        self.run_calls = 0

    def run(self, run_id, *, mode="append", **_kwargs):
        del run_id, mode
        self.run_calls += 1
        complete = self.run_calls == 1
        node = NodeFact(
            node_id="agent:stable",
            subtype="concept",
            attrs={"value": "stable"},
            source_record_id={"record_id": "agent:stable"},
            source_revision={"revision": "stable"},
        )
        return SourceRun(
            facts=iter([node]),
            completed_scope=complete,
            next_cursor={},
            applied_raw_token=self.change_probe._token,
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode="scope_complete" if complete else "append",
            record_set=(
                RecordSet(records={
                    "agent:stable": RecordEmission(
                        primary=RecordNode("agent:stable", "concept"),
                    )
                })
                if complete else None
            ),
        )


def test_authoritative_replay_prepares_adapter_before_probe(okg_migrated):
    prepared = False
    normalized = False
    steady_token = compose_token(
        content={"kind": "content_hash", "value": _hex("steady")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )

    class _PreparedProbe(_StubProbe):
        def probe(self, *, cursor, sync_scope):
            assert prepared is True
            return super().probe(cursor=cursor, sync_scope=sync_scope)

    class _PreparedAdapter(_SpyAdapter):
        def prepare_authoritative_replay(self, *, record_set):
            nonlocal prepared
            assert record_set.records == {}
            prepared = True

        def normalize_authoritative_replay_probe(self, *, next_cursor):
            nonlocal normalized
            assert next_cursor is not None
            self.change_probe._token = steady_token
            normalized = True

    adapter = _PreparedAdapter(_PreparedProbe(_token()))
    prepared_runner = SourceRunner(
        okg_migrated,
        source_name="prepared-authoritative-replay",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )

    result = prepared_runner.run(adapter, mode="cursor")

    assert prepared is True
    assert normalized is True
    assert result.mode == "scope_complete"
    assert (
        result.next_cursor[CURSOR_TOKEN_KEY]["raw_token"]
        == steady_token
    )


@pytest.fixture
def runner(okg_migrated):
    return SourceRunner(
        okg_migrated,
        source_name="spy-probe-source",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )


def _token(value: str = "v1"):
    return compose_token(
        content={"kind": "content_hash", "value": _hex(value)},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )


def _pipeline_snapshot(**overrides) -> PipelineIdentitySnapshot:
    values = {
        "canonicalizers": (),
        "volatile_fields": (),
        "nomos_policy_fingerprint": "nomos-v1",
        "pii_posture": {"store_raw": True, "epoch": "e1"},
        "registry_binding_fingerprint": "registry-v1",
        "transform_identity": {"runner": "v1"},
    }
    values.update(overrides)
    return PipelineIdentitySnapshot.build(**values)


def _verified_external_cursor(source_name: str, token: dict) -> dict:
    return {
        CURSOR_TOKEN_KEY: token,
        "record_set_ref": {
            "schema": "okg.source_runner.record_set_ref.v1",
            "storage": "okg.source_record_set_blobs",
            "source": source_name,
            "sha256": _hex("record-set"),
        },
        "record_set_summary": {
            "schema": "okg.source_runner.record_set_summary.v1",
            "admission_sanitization": _RECORD_SET_SANITIZATION_SCHEMA,
            "source_name": source_name,
            "graph_branch": "default",
            "record_count": 0,
            "node_count": 0,
            "edge_count": 0,
            "chronos_graph_verified": True,
        },
        "record_set_chronos_proof": {
            "ok": True,
            "source_name": source_name,
            "graph_branch": "default",
            "schema": _RECORD_SET_CHRONOS_PROOF_SCHEMA,
        },
    }


def _seed_external_record_set(
    dsn: str,
    *,
    source_name: str,
    cursor: dict,
) -> None:
    runner = SourceRunner(
        dsn,
        source_name=source_name,
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )
    runner._ensure_source_record_sets_table()
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_record_set_blobs
              (source, sha256, record_set, record_set_summary, chronos_proof)
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
            ON CONFLICT (source, sha256) DO UPDATE SET
              record_set = EXCLUDED.record_set,
              record_set_summary = EXCLUDED.record_set_summary,
              chronos_proof = EXCLUDED.chronos_proof
            """,
            (
                source_name,
                cursor["record_set_ref"]["sha256"],
                json.dumps({"records": {}}),
                json.dumps(cursor["record_set_summary"]),
                json.dumps(cursor["record_set_chronos_proof"]),
            ),
        )


def test_first_backlog_mints_cursor_bound_authority_then_resumes_to_exact(
    okg_migrated,
):
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=3)
    bootstrap_runner = _bootstrap_runner(
        okg_migrated,
        source_name="bootstrap-first-resume-exact",
    )

    first = bootstrap_runner.run(adapter, mode="scope_complete")
    first_authority = first.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]
    assert first_authority["generation"] == 1
    assert CURSOR_TOKEN_KEY not in first.next_cursor
    assert first.next_cursor["offset"] == 1

    poll = bootstrap_runner.evaluate_poll(adapter, mode="scope_complete")
    assert poll.enqueue is True
    assert poll.reason == "bootstrap_identity_resume_required"

    second = bootstrap_runner.run(adapter, mode="scope_complete")
    second_authority = second.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]
    assert second_authority["generation"] == 2
    assert second_authority["nonce"] != first_authority["nonce"]
    assert adapter.received_cursors[1]["offset"] == 1
    assert BOOTSTRAP_AUTHORITY_CURSOR_KEY not in adapter.received_cursors[1]

    terminal = bootstrap_runner.run(adapter, mode="scope_complete")
    assert adapter.received_cursors[2]["offset"] == 2
    assert BOOTSTRAP_AUTHORITY_CURSOR_KEY not in terminal.next_cursor
    assert terminal.next_cursor[CURSOR_TOKEN_KEY]["raw_token"] == _token()
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        cursor, replay_required, fact_count, progress_count = conn.execute(
            """
            SELECT w.last_applied_rev,
                   w.replay_required,
                   (SELECT count(*) FROM okg.node_facts WHERE source = w.source),
                   (SELECT count(*) FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = w.source
                       AND key LIKE 'record:%%')
              FROM okg.source_watermarks AS w
             WHERE w.source = %s
            """,
            (bootstrap_runner.source_name,),
        ).fetchone()
    assert cursor == terminal.next_cursor
    assert replay_required == {}
    assert fact_count == 3
    assert progress_count == 3


def test_accepted_bootstrap_transition_retry_is_a_reader_free_noop(
    okg_migrated,
):
    source_name = "bootstrap-idempotent-retry"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=3)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    context = _bootstrap_context(source_name)

    first = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=context,
    )
    authority = first.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        before = conn.execute(
            """
            SELECT w.last_applied_rev,
                   (SELECT value FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = %s
                       AND key = %s),
                   (SELECT count(*) FROM okg.node_facts WHERE source = %s),
                   (SELECT count(*) FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = %s)
              FROM okg.source_watermarks AS w
             WHERE w.source = %s
            """,
            (
                source_name,
                BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                source_name,
                source_name,
                source_name,
            ),
        ).fetchone()

    retry = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=context,
    )

    assert retry.idempotent_replay is True
    assert retry.next_cursor == first.next_cursor
    assert adapter.run_calls == 1
    assert retry.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 1
    assert retry.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["nonce"] == (
        authority["nonce"]
    )
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        after = conn.execute(
            """
            SELECT w.last_applied_rev,
                   (SELECT value FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = %s
                       AND key = %s),
                   (SELECT count(*) FROM okg.node_facts WHERE source = %s),
                   (SELECT count(*) FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = %s)
              FROM okg.source_watermarks AS w
             WHERE w.source = %s
            """,
            (
                source_name,
                BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                source_name,
                source_name,
                source_name,
            ),
        ).fetchone()
    assert after == before


def test_exact_terminal_transition_retry_is_reader_free_and_cursor_tamper_fails(
    okg_migrated,
):
    source_name = "bootstrap-exact-idempotent-retry"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=2)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=_bootstrap_context(f"{source_name}:first"),
    )
    terminal_context = _bootstrap_context(f"{source_name}:terminal")
    terminal = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=terminal_context,
    )

    retry = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=terminal_context,
    )

    assert retry.idempotent_replay is True
    assert retry.next_cursor == terminal.next_cursor
    assert adapter.run_calls == 2

    tampered_cursor = json.loads(json.dumps(terminal.next_cursor))
    tampered_cursor[CURSOR_TOKEN_KEY]["raw_token"]["digest"] = "forged"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(tampered_cursor), source_name),
        )

    with pytest.raises(SubstrateError) as caught:
        bootstrap_runner.run(
            adapter,
            mode="scope_complete",
            run_context=terminal_context,
        )
    assert caught.value.code == "bootstrap_identity_exact_ledger_cursor_mismatch"
    assert adapter.run_calls == 2


def test_exact_terminal_transition_retry_after_new_frontier_fails_closed(
    okg_migrated,
):
    source_name = "bootstrap-exact-stale-retry"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=2)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=_bootstrap_context(f"{source_name}:first"),
    )
    terminal_context = _bootstrap_context(f"{source_name}:terminal")
    terminal = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=terminal_context,
    )

    advanced_cursor = json.loads(json.dumps(terminal.next_cursor))
    advanced_cursor["ordinary_exact_frontier"] = "later-accepted-transition"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(advanced_cursor), source_name),
        )

    with pytest.raises(SubstrateError) as caught:
        bootstrap_runner.run(
            adapter,
            mode="scope_complete",
            run_context=terminal_context,
        )
    assert caught.value.code == "bootstrap_identity_exact_ledger_cursor_mismatch"
    assert adapter.run_calls == 2


def test_distinct_transitions_continue_after_terminal_bootstrap_ledger(
    okg_migrated,
):
    source_name = "bootstrap-exact-future-transitions"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=2)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=_bootstrap_context(f"{source_name}:first"),
    )
    bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=_bootstrap_context(f"{source_name}:terminal"),
    )

    adapter.change_probe._token = _token("ordinary-v2")
    first_ordinary = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=_bootstrap_context(f"{source_name}:ordinary-v2"),
    )
    adapter.change_probe._token = _token("ordinary-v3")
    second_ordinary = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=_bootstrap_context(f"{source_name}:ordinary-v3"),
    )

    assert first_ordinary.idempotent_replay is False
    assert second_ordinary.idempotent_replay is False
    assert adapter.run_calls == 4
    assert first_ordinary.next_cursor[CURSOR_TOKEN_KEY]["raw_token"] == _token(
        "ordinary-v2"
    )
    assert second_ordinary.next_cursor[CURSOR_TOKEN_KEY]["raw_token"] == _token(
        "ordinary-v3"
    )


def test_conflicting_bootstrap_transition_identity_fails_before_reader(
    okg_migrated,
):
    source_name = "bootstrap-transition-conflict"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=3)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    accepted = _bootstrap_context(source_name)
    bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        run_context=accepted,
    )
    conflicting = SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        admission_id=str(uuid.uuid4()),
        idempotency_key=accepted.idempotency_key,
    )

    with pytest.raises(SubstrateError) as caught:
        bootstrap_runner.run(
            adapter,
            mode="scope_complete",
            run_context=conflicting,
        )

    assert caught.value.code == "bootstrap_identity_transition_identity_conflict"
    assert adapter.run_calls == 1


def test_first_backlog_without_deployment_scope_refuses_before_writes(
    okg_migrated,
):
    source_name = "bootstrap-missing-deployment"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=2)
    unscoped_runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=None,
        executable_profile="discovery_crawl",
        registry_name="bootstrap-spy-registry",
        ownership_id=source_name,
        registry_binding_fingerprint="bootstrap-registry-v1",
        write_chronos_graph=False,
        write_fact_log=True,
    )

    with pytest.raises(SubstrateError) as caught:
        unscoped_runner.run(adapter, mode="scope_complete")

    assert caught.value.code == "bootstrap_identity_frozen_binding_missing"
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        fact_count = conn.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            (source_name,),
        ).fetchone()[0]
        progress_count = conn.execute(
            """
            SELECT count(*)
              FROM okg.source_progress
             WHERE source_name = %s
            """,
            (source_name,),
        ).fetchone()[0]
    assert fact_count == 0
    assert progress_count == 0


@pytest.mark.parametrize(
    ("mutation", "error_code"),
    [
        ("missing_claim", "bootstrap_identity_claim_missing"),
        ("unknown_health", "bootstrap_identity_health_not_affirmative"),
        ("unhealthy", "bootstrap_identity_health_not_affirmative"),
        ("lazy_facts", "bootstrap_identity_facts_not_sealed"),
    ],
)
def test_first_backlog_refusals_precede_fact_progress_and_cursor_writes(
    okg_migrated,
    mutation: str,
    error_code: str,
):
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=2)
    if mutation == "missing_claim":
        adapter.return_claim = False
    elif mutation == "unknown_health":
        adapter.health_status = None
    elif mutation == "unhealthy":
        adapter.health_status = "acquire_failed"
    elif mutation == "lazy_facts":
        adapter.lazy_facts = True
    source_name = f"bootstrap-refusal-{mutation}"
    bootstrap_runner = _bootstrap_runner(
        okg_migrated,
        source_name=source_name,
    )

    with pytest.raises(SubstrateError) as caught:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert caught.value.code == error_code
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        cursor, fact_count, progress_count = conn.execute(
            """
            SELECT w.last_applied_rev,
                   (SELECT count(*) FROM okg.node_facts WHERE source = w.source),
                   (SELECT count(*) FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = w.source
                       AND key LIKE 'record:%%')
              FROM okg.source_watermarks AS w
             WHERE w.source = %s
            """,
            (source_name,),
        ).fetchone()
    assert cursor == {}
    assert fact_count == 0
    assert progress_count == 0


def test_bootstrap_binding_changes_refuse_but_source_append_advances(
    okg_migrated,
):
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=4)
    bootstrap_runner = _bootstrap_runner(
        okg_migrated,
        source_name="bootstrap-binding-change",
    )
    bootstrap_runner.run(adapter, mode="scope_complete")

    bootstrap_runner.registry_binding_fingerprint = "changed-registry"
    with pytest.raises(SubstrateError) as config_error:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert config_error.value.code == "bootstrap_identity_binding_mismatch"
    assert adapter.run_calls == 1

    bootstrap_runner.registry_binding_fingerprint = "bootstrap-registry-v1"
    bootstrap_runner.executable_profile = "mutable_api"
    with pytest.raises(SubstrateError) as profile_error:
        bootstrap_runner.run(adapter, mode="reconcile")
    assert profile_error.value.code == "bootstrap_identity_binding_mismatch"
    assert adapter.run_calls == 1

    bootstrap_runner.executable_profile = "discovery_crawl"
    adapter.bootstrap_cursor_semantics = "bootstrap-spy.cursor.v2"
    with pytest.raises(SubstrateError) as semantics_error:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert semantics_error.value.code == "bootstrap_identity_binding_mismatch"
    assert adapter.run_calls == 1

    adapter.bootstrap_cursor_semantics = "bootstrap-spy.cursor.v1"
    bootstrap_runner.graph_branch = "replacement-branch"
    with pytest.raises(SubstrateError) as branch_error:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert branch_error.value.code == "bootstrap_identity_binding_mismatch"
    assert adapter.run_calls == 1

    bootstrap_runner.graph_branch = "default"
    changed_token = compose_token(
        content={"kind": "content_hash", "value": _hex("changed-source")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    probe._token = changed_token
    advanced = bootstrap_runner.run(adapter, mode="scope_complete")
    assert advanced.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 2
    assert adapter.run_calls == 2

    with pytest.raises(SubstrateError) as scope_error:
        bootstrap_runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["slice-a"], "trigger_kinds": ["manual"]},
        )
    assert scope_error.value.code == "bootstrap_identity_scope_mismatch"
    assert adapter.run_calls == 2


@pytest.mark.parametrize(
    "sync_scope",
    [
        {},
        {"scope_keys": [], "revision_hints": [], "trigger_kinds": ["reconcile"]},
        {
            "scope_keys": [],
            "revision_hints": ["always_changed:fp"],
            "trigger_kinds": ["poll"],
        },
    ],
    ids=["empty", "reconcile", "poll"],
)
def test_worker_wakeup_naming_no_scope_key_drains_bootstrap_backlog(
    okg_migrated,
    sync_scope,
):
    """#3314: a wakeup that narrows nothing resumes the global backlog."""
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=3)
    bootstrap_runner = _bootstrap_runner(
        okg_migrated,
        source_name="bootstrap-worker-drain-" + (sync_scope.get("trigger_kinds") or ["none"])[0],
    )

    first = bootstrap_runner.run(adapter, mode="scope_complete")
    assert first.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 1

    second = bootstrap_runner.run(
        adapter, mode="scope_complete", sync_scope=sync_scope,
    )
    assert second.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 2
    assert adapter.received_cursors[1]["offset"] == 1

    terminal = bootstrap_runner.run(
        adapter, mode="scope_complete", sync_scope=sync_scope,
    )
    assert adapter.received_cursors[2]["offset"] == 2
    assert BOOTSTRAP_AUTHORITY_CURSOR_KEY not in terminal.next_cursor
    assert adapter.run_calls == 3


def test_terminal_bootstrap_promotion_requires_stable_global_identity(
    okg_migrated,
):
    source_name = "bootstrap-terminal-instability"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=2)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    bootstrap_runner.run(adapter, mode="scope_complete")
    adapter.post_probe_token = compose_token(
        content={"kind": "content_hash", "value": _hex("unstable-terminal")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )

    with pytest.raises(SubstrateError) as caught:
        bootstrap_runner.run(adapter, mode="scope_complete")

    assert caught.value.code == "bootstrap_identity_snapshot_unstable"


def test_adapter_progress_cannot_overwrite_bootstrap_identity_ledger(
    okg_migrated,
):
    source_name = "bootstrap-reserved-progress"
    adapter = _BootstrapAdapter(_StubProbe(_token()), terminal_after=4)
    adapter.progress_after_run = lambda: {
        BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY: "forged",
    }
    bootstrap_runner = _bootstrap_runner(
        okg_migrated,
        source_name=source_name,
    )

    with pytest.raises(SubstrateError) as caught:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert caught.value.code == "source_progress_reserved_key"

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        cursor, ledger_count = conn.execute(
            """
            SELECT last_applied_rev,
                   (SELECT count(*) FROM okg.source_progress
                     WHERE deployment = 'bootstrap-test'
                       AND source_name = %s
                       AND key = %s)
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (
                source_name,
                BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                source_name,
            ),
        ).fetchone()
    assert cursor == {}
    assert ledger_count == 0

    del adapter.progress_after_run
    recovered = bootstrap_runner.run(adapter, mode="scope_complete")
    assert recovered.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY][
        "generation"
    ] == 1


def test_copied_source_and_branch_incarnation_authority_refuse_before_reader(
    okg_migrated,
):
    original_source = "bootstrap-original-source"
    copied_source = "bootstrap-copied-source"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=4)
    original_runner = _bootstrap_runner(
        okg_migrated,
        source_name=original_source,
    )
    first = original_runner.run(adapter, mode="scope_complete")

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        persist_source_watermark(
            conn,
            source_name=copied_source,
            next_cursor=first.next_cursor,
            last_reconcile=None,
        )
    copied_adapter = _BootstrapAdapter(probe, terminal_after=4)
    with pytest.raises(SubstrateError) as copied_error:
        _bootstrap_runner(okg_migrated, source_name=copied_source).run(
            copied_adapter,
            mode="scope_complete",
        )
    assert copied_error.value.code == "bootstrap_identity_binding_mismatch"
    assert copied_adapter.run_calls == 0

    stale_branch_cursor = dict(first.next_cursor)
    stale_authority = dict(
        stale_branch_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]
    )
    stale_authority["bindings"] = dict(stale_authority["bindings"])
    stale_authority["bindings"]["graph_branch_incarnation"] = (
        "retired-branch-incarnation"
    )
    stale_authority["authority_fingerprint"] = identity_fingerprint({
        key: value
        for key, value in stale_authority.items()
        if key != "authority_fingerprint"
    })
    stale_branch_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY] = stale_authority
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(stale_branch_cursor), original_source),
        )
    with pytest.raises(SubstrateError) as incarnation_error:
        original_runner.run(adapter, mode="scope_complete")
    assert incarnation_error.value.code == "bootstrap_identity_binding_mismatch"
    assert adapter.run_calls == 1


def test_reset_explicitly_revokes_incompatible_bootstrap_progress(
    okg_migrated,
):
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=5)
    bootstrap_runner = _bootstrap_runner(
        okg_migrated,
        source_name="bootstrap-explicit-reset",
    )
    first = bootstrap_runner.run(adapter, mode="scope_complete")
    assert first.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 1

    bootstrap_runner.registry_binding_fingerprint = "replacement-registry"
    reset = bootstrap_runner.run(
        adapter,
        mode="scope_complete",
        reset_cursor=True,
    )

    assert adapter.received_cursors[1] is None
    assert reset.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 2
    assert reset.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["bindings"][
        "registry_binding_fingerprint"
    ] == "replacement-registry"


def test_bootstrap_ledger_refuses_restored_whole_cursor_before_reader(
    okg_migrated,
):
    source_name = "bootstrap-cas-replay"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=4)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    first = bootstrap_runner.run(adapter, mode="scope_complete")
    second = bootstrap_runner.run(adapter, mode="scope_complete")

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(first.next_cursor), source_name),
        )

    with pytest.raises(SubstrateError) as replay_error:
        bootstrap_runner.run(adapter, mode="scope_complete")

    assert replay_error.value.code == (
        "bootstrap_identity_authority_ledger_mismatch"
    )
    assert adapter.run_calls == 2
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        persisted, ledger_value, fact_count = conn.execute(
            """
            SELECT w.last_applied_rev, p.value,
                   (SELECT count(*) FROM okg.node_facts WHERE source = w.source)
              FROM okg.source_watermarks AS w
              JOIN okg.source_progress AS p
                ON p.deployment = 'bootstrap-test'
               AND p.source_name = w.source
               AND p.key = 'sync.bootstrap_identity_authority'
             WHERE w.source = %s
            """,
            (source_name,),
        ).fetchone()
    assert persisted == first.next_cursor
    assert json.loads(ledger_value)["generation"] == 2
    assert fact_count == 2
    assert second.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["generation"] == 2


def test_tampered_bootstrap_token_refuses_before_reader_or_writes(
    okg_migrated,
):
    source_name = "bootstrap-tampered-token"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=4)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    bootstrap_runner.run(adapter, mode="scope_complete")

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        persisted = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks WHERE source = %s",
            (source_name,),
        ).fetchone()[0]
        persisted[BOOTSTRAP_AUTHORITY_CURSOR_KEY]["raw_token"][
            "components"
        ]["content"]["value"] = _hex("tampered")
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(persisted), source_name),
        )

    with pytest.raises(SubstrateError) as tamper_error:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert tamper_error.value.code == "bootstrap_identity_authority_malformed"
    assert adapter.run_calls == 1
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        fact_count = conn.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            (source_name,),
        ).fetchone()[0]
    assert fact_count == 1


def test_tampered_bootstrap_resume_frontier_refuses_before_reader(
    okg_migrated,
):
    source_name = "bootstrap-tampered-frontier"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=4)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    bootstrap_runner.run(adapter, mode="scope_complete")

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        persisted = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks WHERE source = %s",
            (source_name,),
        ).fetchone()[0]
        persisted["offset"] = 999
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(persisted), source_name),
        )

    with pytest.raises(SubstrateError) as frontier_error:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert frontier_error.value.code == "bootstrap_identity_authority_malformed"
    assert adapter.run_calls == 1


def test_self_consistent_but_unsound_bootstrap_token_refuses_before_reader(
    okg_migrated,
):
    source_name = "bootstrap-unsound-token"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=4)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    first = bootstrap_runner.run(adapter, mode="scope_complete")
    cursor = dict(first.next_cursor)
    authority = dict(cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY])
    authority["raw_token"] = {
        "schema": "okg.source.change_probe.token.v1",
        "components": {},
    }
    authority["authority_fingerprint"] = identity_fingerprint({
        key: value
        for key, value in authority.items()
        if key != "authority_fingerprint"
    })
    cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY] = authority
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = %s::jsonb
             WHERE source = %s
            """,
            (json.dumps(cursor), source_name),
        )

    with pytest.raises(SubstrateError) as token_error:
        bootstrap_runner.run(adapter, mode="scope_complete")
    assert token_error.value.code == "bootstrap_identity_raw_token_malformed"
    assert adapter.run_calls == 1


def test_bootstrap_concurrent_consumer_refuses_before_reader(okg_migrated):
    source_name = "bootstrap-concurrent-consumer"
    probe = _StubProbe(_token())
    adapter = _BootstrapAdapter(probe, terminal_after=3)
    bootstrap_runner = _bootstrap_runner(okg_migrated, source_name=source_name)
    lock_key = f"okg.source.bootstrap_identity:{source_name}"

    with psycopg.connect(okg_migrated, autocommit=True) as lock_conn:
        lock_conn.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
            (lock_key,),
        )
        with pytest.raises(SubstrateError) as concurrent_error:
            bootstrap_runner.run(adapter, mode="scope_complete")

    assert concurrent_error.value.code == (
        "bootstrap_identity_authority_concurrent"
    )
    assert adapter.run_calls == 0


def test_admitted_bootstrap_lease_spans_terminal_watermark_cas(okg_migrated):
    source_name = "bootstrap-admission-lease"
    probe = _StubProbe(_token())
    first_adapter = _BootstrapAdapter(probe, terminal_after=3)
    first_runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name="bootstrap-test",
        executable_profile="discovery_crawl",
        registry_name="bootstrap-spy-registry",
        ownership_id=source_name,
        registry_binding_fingerprint="bootstrap-registry-v1",
        write_chronos_graph=False,
        write_fact_log=True,
        defer_bootstrap_lock_release=True,
    )
    context = SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        admission_id=str(uuid.uuid4()),
        idempotency_key="bootstrap-admission-lease",
    )

    try:
        first = first_runner.run(
            first_adapter,
            mode="scope_complete",
            run_context=context,
        )
        competing_adapter = _BootstrapAdapter(probe, terminal_after=3)
        competing_runner = _bootstrap_runner(
            okg_migrated,
            source_name=source_name,
        )
        with pytest.raises(SubstrateError) as concurrent_error:
            competing_runner.run(competing_adapter, mode="scope_complete")
        assert concurrent_error.value.code == (
            "bootstrap_identity_authority_concurrent"
        )
        assert competing_adapter.run_calls == 0

        with psycopg.connect(okg_migrated, autocommit=True) as conn:
            persist_source_watermark(
                conn,
                source_name=source_name,
                deployment_name="bootstrap-test",
                next_cursor=first.next_cursor,
                last_reconcile=None,
                expected_last_applied_rev=first.expected_last_applied_rev,
                require_compare_and_swap=True,
                expected_bootstrap_ledger=(
                    first.expected_bootstrap_ledger
                ),
                next_bootstrap_ledger=first.next_bootstrap_ledger,
            )
    finally:
        first_runner.release_deferred_bootstrap_lock()

    resumed = _bootstrap_runner(okg_migrated, source_name=source_name).run(
        _BootstrapAdapter(probe, terminal_after=3),
        mode="scope_complete",
    )
    assert resumed.next_cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY][
        "generation"
    ] == 2


def test_gate_skips_reader_on_unchanged(runner):
    probe = _StubProbe(_token())
    adapter = _SpyAdapter(probe)

    # First run: changed=true -> reader invoked, token stamped to the watermark.
    runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1

    # Second run: probe now unchanged -> reader NOT invoked (short-circuit).
    probe.changed = False
    result = runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1, "reader must not be invoked on changed=false"
    assert result.mode == "no_change"
    assert result.completed_scope is False
    assert result.nodes_appended == 0
    assert result.edges_appended == 0


def test_append_feed_unchanged_probe_is_complete(okg_migrated):
    probe = _StubProbe(_token())
    adapter = _AppendFeedSpyAdapter(probe)
    append_runner = SourceRunner(
        okg_migrated,
        source_name="append-feed-spy",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )

    first = append_runner.run(adapter, mode="append")
    assert first.mode == "append"
    assert first.effective_mode == "scope_complete"

    probe.changed = False
    result = append_runner.run(adapter, mode="append")

    assert adapter.run_calls == 1
    assert result.mode == "no_change"
    assert result.completed_scope is True


def test_append_feed_exact_changed_read_deduped_to_zero_is_no_change(
    okg_migrated,
):
    probe = _StubProbe(_token())
    adapter = _AppendFeedDedupAdapter(probe)
    append_runner = SourceRunner(
        okg_migrated,
        source_name="append-feed-dedup",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )

    first = append_runner.run(adapter, mode="append")
    assert first.nodes_appended == 1

    probe._token = compose_token(
        content={"kind": "content_hash", "value": _hex("v2")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    probe.changed = True
    result = append_runner.run(adapter, mode="append")

    assert adapter.run_calls == 2
    assert result.mode == "append"
    assert result.effective_mode == "no_change"
    assert result.runner_proven_no_change is True
    assert result.completed_scope is True
    assert result.nodes_appended == 0
    assert result.edges_appended == 0


def test_adapter_becoming_short_circuit_unsafe_forces_reader(runner):
    probe = _StubProbe(_token())
    adapter = _SpyAdapter(probe)

    runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1

    # Simulate a source whose old deployment/run acknowledged an exact token,
    # but whose current adapter now declares that its reader is not snapshot
    # bound. A persisted token must not keep the old skip authority alive.
    probe.changed = False
    adapter.probe_short_circuit_safe = False
    result = runner.run(adapter, mode="cursor")

    assert adapter.run_calls == 2
    assert result.mode == "cursor"


def test_reconcile_without_verified_record_set_is_not_short_circuited(runner):
    probe = _StubProbe(_token())
    adapter = _SpyAdapter(probe)

    runner.run(adapter, mode="cursor")  # seed the token
    assert adapter.run_calls == 1
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = last_applied_rev
                    - 'record_set' - 'record_set_summary'
             WHERE source = %s
            """,
            (runner.source_name,),
        )

    # Even with changed=false, an unverified reconcile run must invoke the
    # reader; it carries the deletion proof a cheap probe cannot see.
    probe.changed = False
    runner.run(adapter, mode="reconcile")
    assert adapter.run_calls == 2, "unverified reconcile must invoke the reader"


@pytest.mark.postgres
def test_completed_scope_with_verified_record_set_short_circuits_reader(okg_migrated):
    source_name = "spy-probe-source-completed"
    token = _token()
    probe = _StubProbe(token)
    adapter = _SpyAdapter(probe)
    completed_runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )
    completed_runner.run(adapter, mode="cursor")
    probe.changed = False
    result = completed_runner.run(adapter, mode="scope_complete")

    assert adapter.run_calls == 1, "verified completed scope must skip the reader"
    assert result.mode == "no_change"
    assert result.completed_scope is True
    assert result.nodes_appended == 0
    assert result.edges_appended == 0
    assert result.next_cursor is not None
    assert result.next_cursor[_PROBE_SKIP_COUNT_KEY] == 1
    assert "record_set" in result.next_cursor

    with psycopg.connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_reconcile_at IS NOT NULL,
                   last_applied_rev->>'record_set' IS NOT NULL,
                   (last_applied_rev->>%s)::int
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (_PROBE_SKIP_COUNT_KEY, source_name),
        )
        assert cur.fetchone() == (True, True, 1)


@pytest.mark.postgres
def test_completed_scope_with_unreachable_external_record_set_invokes_reader(
    okg_migrated,
):
    source_name = "spy-probe-source-missing-record-set"
    token = _token()
    cursor = _verified_external_cursor(source_name, token)
    with psycopg.connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (source, last_applied_rev)
            VALUES (%s, %s::jsonb)
            """,
            (source_name, json.dumps(cursor)),
        )

    probe = _StubProbe(token)
    probe.changed = False
    adapter = _SpyAdapter(probe)

    result = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    ).run(adapter, mode="scope_complete")

    assert adapter.run_calls == 1, "missing external record set must read"
    assert adapter.last_cursor is None
    assert result.completed_scope is True
    assert result.next_cursor is not None
    assert "record_set_ref" not in result.next_cursor


def test_path_scoped_sync_bypasses_gate_and_does_not_stamp(runner):
    token_v1 = _token()
    token_v2 = compose_token(
        content={"kind": "content_hash", "value": _hex("v2")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    probe = _StubProbe(token_v1)
    adapter = _SpyAdapter(probe)

    # Seed the global token at content v1.
    seed = runner.run(adapter, mode="cursor")
    assert adapter.run_calls == 1
    assert seed.next_cursor[CURSOR_TOKEN_KEY]["raw_token"] == token_v1

    # Global content has since moved to v2, but the operator issues a path-scoped
    # sync (reading only a subset). It must (a) invoke the reader despite
    # changed=false, and (b) NOT advance the global token to v2 -- the partial
    # read did not reconcile the global scope, so stamping v2 would let a later
    # full sync skip the non-scoped paths that changed at v2.
    probe.changed = False
    probe._token = token_v2
    scoped = runner.run(
        adapter, mode="cursor", sync_scope={"scope_keys": ["only/this.json"]}
    )
    assert adapter.run_calls == 2, "path-scoped sync must always invoke the reader"
    assert scoped.next_cursor[CURSOR_TOKEN_KEY]["raw_token"] == token_v1, (
        "a path-scoped run must preserve the global token, not stamp its own"
    )
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()[0]
    assert replay_required["obligation"] == "in_flight_delta"


def test_first_path_scoped_sync_is_not_widened_to_global_replay(runner):
    probe = _StubProbe(_token())
    adapter = _SpyAdapter(probe)
    scope = {"scope_keys": ["only/this.json"]}
    scoped_runner = SourceRunner(
        runner.dsn,
        source_name="spy-first-path-scoped",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )

    scoped = scoped_runner.run(
        adapter,
        mode="scope_incomplete",
        sync_scope=scope,
    )

    assert adapter.run_calls == 1
    assert adapter.last_cursor is None
    assert scoped.mode == "scope_incomplete"
    assert scoped.completed_scope is False
    assert CURSOR_TOKEN_KEY not in scoped.next_cursor
    with psycopg.connect(scoped_runner.dsn, autocommit=True) as conn:
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (scoped_runner.source_name,),
        ).fetchone()[0]
    assert replay_required["obligation"] == "in_flight_delta"


def test_path_scoped_sync_preserves_pending_authoritative_replay(runner):
    probe = _StubProbe(_token())
    adapter = _SpyAdapter(probe)
    runner.run(adapter, mode="cursor")
    pending = {
        "schema": "okg.source_runner.replay_required.v1",
        "target_fingerprint": "pending-global-repair",
        "pipeline_fingerprint": "prior-pipeline",
        "obligation": "authoritative_full_replay",
        "run_id": "failed-global-run",
    }
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET replay_required = %s::jsonb,
                   replay_required_at = now()
             WHERE source = %s
            """,
            (json.dumps(pending), runner.source_name),
        )

    scoped = runner.run(
        adapter,
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["only/this.json"]},
    )

    assert scoped.mode == "scope_incomplete"
    assert scoped.replay_target == pending
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()[0]
    assert replay_required == pending


def test_reader_crash_leaves_durable_latch_and_zero_net_repair_clears_it(
    runner,
):
    probe = _StubProbe(_token())
    crashing = _SpyAdapter(probe)
    crashing.raise_on_run = RuntimeError("simulated reader crash")

    with pytest.raises(RuntimeError, match="simulated reader crash"):
        runner.run(crashing, mode="cursor")

    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        row = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()
    assert row is not None
    assert row[1]["obligation"] == "authoritative_full_replay"

    repair = _SpyAdapter(probe)
    repaired = runner.run(repair, mode="cursor")

    assert repaired.mode == "scope_complete"
    assert repaired.completed_scope is True
    assert repaired.nodes_appended == 0
    assert repaired.edges_appended == 0
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()[0]
    assert replay_required == {}


def test_backlogged_replay_preserves_latch_and_prior_token_until_exact_terminal_run(
    runner,
):
    token_v1 = _token()
    token_v2 = compose_token(
        content={"kind": "content_hash", "value": _hex("v2")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    probe = _StubProbe(token_v1)
    adapter = _SpyAdapter(probe)

    seeded = runner.run(adapter, mode="cursor")
    seeded_token = seeded.next_cursor[CURSOR_TOKEN_KEY]
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        seeded_last_reconcile = conn.execute(
            """
            SELECT last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()[0]

    probe._token = token_v2
    adapter.outstanding_backlog = True
    partial = runner.run(adapter, mode="cursor")

    # The first changed run establishes the durable in-flight latch while
    # retaining bounded backlog progress. The next invocation observes that
    # latch and is promoted to the profile's authoritative replay mode.
    assert partial.mode == "cursor"
    assert partial.completed_scope is False
    assert partial.next_cursor[CURSOR_TOKEN_KEY] == seeded_token
    assert partial.next_cursor["outstanding_backlog"]
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        cursor, replay_required, last_reconcile = conn.execute(
            """
            SELECT last_applied_rev, replay_required, last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()
    assert cursor[CURSOR_TOKEN_KEY] == seeded_token
    assert replay_required["obligation"] == "in_flight_delta"
    assert last_reconcile == seeded_last_reconcile

    adapter.outstanding_backlog = False
    terminal = runner.run(adapter, mode="cursor")

    assert terminal.mode == "scope_complete"
    assert terminal.completed_scope is True
    assert terminal.next_cursor[CURSOR_TOKEN_KEY]["raw_token"] == token_v2
    assert "outstanding_backlog" not in terminal.next_cursor
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        replay_required, last_reconcile = conn.execute(
            """
            SELECT replay_required, last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()
    assert replay_required == {}
    assert last_reconcile is not None
    assert last_reconcile != seeded_last_reconcile


def test_aba_post_probe_mismatch_cannot_advance_cursor_or_clear_latch(runner):
    token_a = _token()
    token_b = compose_token(
        content={"kind": "content_hash", "value": _hex("v2")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    seed_probe = _StubProbe(token_a)
    runner.run(_SpyAdapter(seed_probe), mode="cursor")

    class _SequenceProbe:
        def __init__(self) -> None:
            self._tokens = [token_b, token_a]
            self._token = token_b

        def probe(self, *, cursor, sync_scope):
            del cursor, sync_scope
            self._token = self._tokens.pop(0)
            return ProbeVerdict(changed=True, token=self._token)

    aba_probe = _SequenceProbe()
    aba_adapter = _SpyAdapter(aba_probe)
    aba_adapter.applied_token = token_b

    with pytest.raises(
        SubstrateError,
        match="cannot acknowledge change identity",
    ):
        runner.run(aba_adapter, mode="cursor")

    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        cursor, replay_required = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()
    assert cursor[CURSOR_TOKEN_KEY]["raw_token"] == token_a
    assert replay_required["obligation"] == "in_flight_delta"


def test_pipeline_identity_change_forces_poll_wakeup_and_full_replay(runner):
    probe = _StubProbe(_token())
    adapter = _SpyAdapter(probe)
    runner.registry_binding_fingerprint = "registry-v1"

    runner.run(adapter, mode="cursor")
    probe.changed = False
    unchanged = runner.evaluate_poll(adapter)
    assert unchanged.enqueue is False
    assert unchanged.reason == "eligible_unchanged"

    runner.registry_binding_fingerprint = "registry-v2"
    changed = runner.evaluate_poll(adapter)
    assert changed.enqueue is True
    assert changed.changed is True

    replay = runner.run(adapter, mode="cursor")
    assert replay.mode == "scope_complete"
    assert replay.completed_scope is True
    assert adapter.run_calls == 2
    with psycopg.connect(runner.dsn, autocommit=True) as conn:
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (runner.source_name,),
        ).fetchone()[0]
    assert replay_required == {}


@pytest.mark.parametrize(
    "mutation",
    [
        {"canonicalizers": ({"alias": "repo-v2"},)},
        {"volatile_fields": (("Document", ("observed_at",)),)},
        {"nomos_policy_fingerprint": "nomos-v2"},
        {"pii_posture": {"store_raw": False, "epoch": "e2"}},
        {"transform_identity": {"runner": "v2"}},
    ],
    ids=[
        "canonicalizer",
        "catalog-volatile-fields",
        "nomos-policy",
        "pii-posture",
        "runner-transform",
    ],
)
def test_each_pipeline_identity_mutation_forces_reader(
    okg_migrated,
    monkeypatch: pytest.MonkeyPatch,
    mutation: dict,
):
    source_name = "spy-pipeline-" + next(iter(mutation)).replace("_", "-")
    raw_token = _token()
    baseline = _pipeline_snapshot()
    changed = _pipeline_snapshot(**mutation)
    prior_cursor = {
        CURSOR_TOKEN_KEY: build_effective_token(raw_token, baseline),
    }
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev)
            VALUES (%s, %s::jsonb)
            """,
            (source_name, json.dumps(prior_cursor)),
        )

    probe = _StubProbe(raw_token)
    probe.changed = False
    adapter = _SpyAdapter(probe)
    mutation_runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )
    monkeypatch.setattr(
        mutation_runner,
        "_resolve_pipeline_identity_snapshot",
        lambda _adapter: changed,
    )

    result = mutation_runner.run(adapter, mode="cursor")

    assert adapter.run_calls == 1
    assert result.mode == "scope_complete"
    assert result.completed_scope is True
    assert result.next_cursor[CURSOR_TOKEN_KEY]["pipeline_fingerprint"] == (
        changed.fingerprint
    )


def test_pipeline_identity_resolution_failure_precedes_all_writes(
    okg_migrated,
    monkeypatch: pytest.MonkeyPatch,
):
    source_name = "spy-pipeline-resolution-failure"
    adapter = _SpyAdapter(_StubProbe(_token()))
    failing_runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )

    def fail_resolution(_adapter):
        raise RuntimeError("catalog identity unavailable")

    monkeypatch.setattr(
        failing_runner,
        "_resolve_pipeline_identity_snapshot",
        fail_resolution,
    )

    with pytest.raises(RuntimeError, match="catalog identity unavailable"):
        failing_runner.run(adapter, mode="cursor")

    assert adapter.run_calls == 0
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        assert conn.execute(
            """
            SELECT count(*)
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE source = %s
            """,
            (source_name,),
        ).fetchone()[0] == 0


def test_stale_replay_target_cannot_clear_latch_or_advance_cursor(
    okg_migrated,
):
    source_name = "spy-stale-replay-target"
    current_cursor = {"accepted": "newer"}
    replay = {
        "schema": "okg.source_runner.replay_required.v1",
        "target_fingerprint": "newer-target",
        "obligation": "authoritative_full_replay",
        "run_id": "newer-run",
    }
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, replay_required, replay_required_at)
            VALUES (%s, %s::jsonb, %s::jsonb, now())
            """,
            (
                source_name,
                json.dumps(current_cursor),
                json.dumps(replay),
            ),
        )
        persist_source_watermark(
            conn,
            source_name=source_name,
            next_cursor={"accepted": "stale"},
            last_reconcile=None,
            clear_replay_required=True,
            replay_target_fingerprint="older-target",
            replay_target_run_id="older-run",
        )
        cursor, retained = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        ).fetchone()

    assert cursor == current_cursor
    assert retained == replay


def test_older_same_target_run_cannot_clear_newer_replay_owner(
    okg_migrated,
):
    source_name = "spy-same-target-newer-owner"
    replay = {
        "schema": "okg.source_runner.replay_required.v1",
        "target_fingerprint": "shared-target",
        "obligation": "authoritative_full_replay",
        "run_id": "newer-run",
    }
    current_cursor = {"accepted": "newer"}
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, replay_required, replay_required_at)
            VALUES (%s, %s::jsonb, %s::jsonb, now())
            """,
            (
                source_name,
                json.dumps(current_cursor),
                json.dumps(replay),
            ),
        )
        persist_source_watermark(
            conn,
            source_name=source_name,
            next_cursor={"accepted": "older"},
            last_reconcile=None,
            clear_replay_required=True,
            replay_target_fingerprint="shared-target",
            replay_target_run_id="older-run",
        )
        cursor, retained = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        ).fetchone()

    assert cursor == current_cursor
    assert retained == replay


def test_facts_and_marker_before_failed_ack_require_authoritative_reapply(
    okg_migrated,
):
    source_name = "spy-crash-after-facts"
    token_a = _token()
    token_b = compose_token(
        content={"kind": "content_hash", "value": _hex("v2")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )

    class _SequenceProbe:
        def __init__(self, tokens):
            self._tokens = list(tokens)
            self._token = self._tokens[0]

        def probe(self, *, cursor, sync_scope):
            del cursor, sync_scope
            self._token = self._tokens.pop(0)
            return ProbeVerdict(changed=True, token=self._token)

    class _MarkerAdapter:
        name = "marker-snapshot"
        profile = "discovery_crawl"
        probe_short_circuit_safe = True

        def __init__(self, probe, applied_token):
            self.change_probe = probe
            self.applied_token = applied_token
            self.seen_progress = []

        def run(
            self,
            run_id,
            *,
            mode="cursor",
            cursor=None,
            since_progress=None,
        ):
            del run_id, cursor
            self.seen_progress.append(since_progress)
            node_id = "document:marker-snapshot"
            return SourceRun(
                facts=iter([
                    NodeFact(
                        node_id=node_id,
                        subtype="concept",
                        attrs={"value": "stable"},
                        source_record_id={"id": "record-1"},
                        source_revision={"rev": "stable"},
                    ),
                    ProgressMarker(
                        record_id="record-1",
                        fingerprint="stable-fingerprint",
                    ),
                ]),
                completed_scope=True,
                next_cursor={},
                run_mode=mode,
                record_set=RecordSet(records={
                    "record-1": RecordEmission(
                        primary=RecordNode(node_id, "concept"),
                    )
                }),
                applied_raw_token=self.applied_token,
                applied_token_scope="global",
                applied_token_authority="exact",
            )

    runner = SourceRunner(
        okg_migrated,
        source_name=source_name,
        deployment_name="test",
        write_chronos_graph=False,
        write_fact_log=True,
    )
    failed = _MarkerAdapter(_SequenceProbe([token_b, token_a]), token_b)

    with pytest.raises(
        SubstrateError,
        match="cannot acknowledge change identity",
    ):
        runner.run(failed, mode="cursor")

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        fact_count = conn.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            (source_name,),
        ).fetchone()[0]
        marker_count = conn.execute(
            """
            SELECT count(*)
              FROM okg.source_progress
             WHERE deployment = 'test'
               AND source_name = %s
               AND key = 'record:record-1'
            """,
            (source_name,),
        ).fetchone()[0]
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        ).fetchone()[0]
    assert fact_count == 1
    assert marker_count == 1
    assert replay_required["obligation"] == "authoritative_full_replay"

    repair = _MarkerAdapter(_StubProbe(token_a), token_a)
    repaired = runner.run(repair, mode="cursor")

    assert repaired.mode == "scope_complete"
    assert repaired.nodes_appended == 1
    assert repair.seen_progress == [None]
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        replay_required = conn.execute(
            """
            SELECT replay_required
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        ).fetchone()[0]
    assert replay_required == {}


# --- Issue #2483: a file-selection config change retracts newly excluded files


def _git(repo_root, *args: str) -> None:
    import subprocess

    subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=True,
        capture_output=True,
    )


def _git_files_entry(repo_root, *, exclude_globs: list[str]):
    from okg.substrate.sources.registry import SourceEntry

    return SourceEntry(
        name="exclude-change.git-files",
        ownership_id="exclude-change.git-files",
        source_class=None,
        record_identity_kind=None,
        record_identity_fields=[],
        source_revision_kind=None,
        deletion_semantics=None,
        publication_mode=None,
        credential_refs=[],
        credential_aliases={},
        required_for_baseline=False,
        raw={
            "module": "okg.substrate.library.sources.git_graph_files",
            "class": "GitFilesSource",
            "params": {
                "repo": "demo",
                "repo_root": str(repo_root),
                "exclude_globs": list(exclude_globs),
            },
        },
    )


def _run_registered_git_files(dsn: str, repo_root, *, exclude_globs):
    """Build the adapter and its registry binding the way `okg ingest` does."""
    from okg.substrate.ingest.adapter_factory import construct_source_adapter

    constructed = construct_source_adapter(
        _git_files_entry(repo_root, exclude_globs=exclude_globs),
        dsn=dsn,
    )
    runner = SourceRunner(
        dsn,
        source_name=constructed.ownership_id,
        registry_binding_fingerprint=constructed.registry_binding_fingerprint,
    )
    return runner.run(constructed.adapter, mode="scope_complete")


def _source_file_rows(dsn: str) -> dict[str, str]:
    """Live source_file rows with the columns a rewrite would change."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT node_id, attrs::text AS attrs_text
              FROM okg.graph_nodes
             WHERE subtype = 'source_file'
            """,
        )
    return {row["node_id"]: row["attrs_text"] for row in rows}


def test_exclude_globs_change_retracts_newly_excluded_files(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Issue #2483: adding an exclude glob removes the already-indexed files
    it now matches on the next ordinary run -- no --reset-cursor -- from the
    working graph, and leaves every other row unrewritten. The search half
    is proven through a real `okg install` + `okg ingest` + `okg search`
    run (pact/changes/exclude-globs-replay-2483/evidence)."""
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-q")
    _git(repo_root, "config", "user.email", "test@example.com")
    _git(repo_root, "config", "user.name", "OKG Test")
    (repo_root / "src" / "demo").mkdir(parents=True)
    (repo_root / "README.md").write_text("# demo\n")
    (repo_root / "src" / "demo" / "keep.py").write_text("KEEP = 1\n")
    (repo_root / "src" / "demo" / "scratch_notes.py").write_text("N = 1\n")
    (repo_root / "src" / "demo" / "scratch_more.py").write_text("M = 1\n")
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-q", "-m", "initial")

    excluded = {
        "file:demo:src/demo/scratch_notes.py",
        "file:demo:src/demo/scratch_more.py",
    }
    kept = {"file:demo:README.md", "file:demo:src/demo/keep.py"}

    first = _run_registered_git_files(clean_runtime, repo_root, exclude_globs=[])
    assert first.completed_scope is True
    before = _source_file_rows(clean_runtime)
    assert set(before) == excluded | kept

    second = _run_registered_git_files(
        clean_runtime,
        repo_root,
        exclude_globs=["src/demo/scratch_*.py"],
    )

    assert second.mode == "scope_complete"
    assert second.completed_scope is True
    assert second.retractions_node == len(excluded)
    after = _source_file_rows(clean_runtime)
    assert set(after) == kept
    # Unchanged rows are compared against the live branch and not rewritten.
    assert second.nodes_appended == 0
    assert {node_id: after[node_id] for node_id in kept} == {
        node_id: before[node_id] for node_id in kept
    }

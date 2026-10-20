"""Runner change-probe gate (#323, incremental-source-probe-contract task 2).

DB-free unit tests for the gate's eligibility decision (reconcile bypass,
clean-authority predicate, skip budget, reset) plus the skip-counter / token
stamp helpers. The end-to-end reader-not-invoked behavior is covered by
``test_source_runner_change_probe_gate_e2e.py`` (needs a migrated test DB).

These methods are pure (they read the cursor dict; no connection), so we
construct a SourceRunner against an unused DSN and never open a connection.
"""
from __future__ import annotations

import hashlib

import pytest

from okg.substrate.ingest.change_identity import identity_fingerprint
from okg.substrate.ingest.runner import (
    _PROBE_SKIP_COUNT_KEY,
    _RECORD_SET_CHRONOS_PROOF_SCHEMA,
    SourceRunner,
    _adapter_run_kwargs,
    _probe_skip_budget,
    applied_identity_acknowledgeable,
    applied_identity_refusal,
)
from okg.substrate.library.sources.base import SourceRun
from okg.substrate.library.sources.change_probe import (
    CURSOR_OUTSTANDING_KEY,
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
)


def _hex(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _runner() -> SourceRunner:
    # __init__ does not open a connection (lazy via _acquire_conn), so an unused
    # DSN is safe for these pure-method tests.
    runner = SourceRunner(
        "postgresql://unused/none",
        source_name="spy",
        deployment_name=None,
    )
    runner._chronos_branch_incarnation = lambda: "current-lineage"
    return runner


def test_probe_short_circuit_authority_requires_explicit_opt_in() -> None:
    class LegacyAdapter:
        pass

    class ExactAdapter:
        probe_short_circuit_safe = True

    assert SourceRunner._adapter_probe_short_circuit_safe(
        LegacyAdapter()
    ) is False
    assert SourceRunner._adapter_probe_short_circuit_safe(
        ExactAdapter()
    ) is True


def _chronos_verified_cursor(runner: SourceRunner) -> dict:
    return {
        "record_set": {"records": {}},
        "record_set_summary": {
            "schema": "okg.source_runner.record_set_summary.v1",
            "source_name": runner.source_name,
            "graph_branch": runner.graph_branch,
            "record_count": 0,
            "node_count": 0,
            "edge_count": 0,
            "chronos_graph_verified": True,
        },
        "record_set_chronos_proof": {
            "ok": True,
            "source_name": runner.source_name,
            "graph_branch": runner.graph_branch,
            "schema": _RECORD_SET_CHRONOS_PROOF_SCHEMA,
            "graph_incarnation": "current-lineage",
        },
    }


def _chronos_verified_external_cursor(runner: SourceRunner) -> dict:
    return {
        "record_set_ref": {
            "schema": "okg.source_runner.record_set_ref.v1",
            "storage": "okg.source_record_set_blobs",
            "source": runner.source_name,
            "sha256": _hex("record-set"),
        },
        "record_set_summary": {
            "schema": "okg.source_runner.record_set_summary.v1",
            "source_name": runner.source_name,
            "graph_branch": runner.graph_branch,
            "record_count": 0,
            "node_count": 0,
            "edge_count": 0,
            "chronos_graph_verified": True,
        },
        "record_set_chronos_proof": {
            "ok": True,
            "source_name": runner.source_name,
            "graph_branch": runner.graph_branch,
            "schema": _RECORD_SET_CHRONOS_PROOF_SCHEMA,
            "graph_incarnation": "current-lineage",
        },
    }


# ---- clean-authority predicate ----

def test_authority_clean_when_no_record_set():
    r = _runner()
    assert r._probe_skip_authority_clean({}) is True
    assert r._probe_skip_authority_clean(None) is True
    assert r._probe_skip_authority_clean({"files": {"a": 1}}) is True


def test_authority_clean_when_chronos_verified():
    r = _runner()
    assert r._probe_skip_authority_clean(_chronos_verified_cursor(r)) is True


def test_authority_not_clean_when_record_set_unverified():
    r = _runner()
    # Record-set authority present but no chronos proof => a real read would run
    # the stray-retraction sweep; the skip would strand it. Not clean.
    assert r._probe_skip_authority_clean({"record_set": {"records": {}}}) is False


def test_authority_not_clean_when_external_summary_missing():
    r = _runner()
    cursor = _chronos_verified_external_cursor(r)
    cursor.pop("record_set_summary")

    assert r._probe_skip_authority_clean(cursor) is False


def test_authority_not_clean_when_external_summary_mismatched():
    r = _runner()
    cursor = _chronos_verified_external_cursor(r)
    cursor["record_set_summary"]["source_name"] = "other-source"

    assert r._probe_skip_authority_clean(cursor) is False


def test_authority_not_clean_when_external_ref_mismatched():
    r = _runner()
    cursor = _chronos_verified_external_cursor(r)
    cursor["record_set_ref"]["source"] = "other-source"

    assert r._probe_skip_authority_clean(cursor) is False


def test_record_set_proof_is_bound_to_graph_incarnation(monkeypatch):
    r = _runner()
    cursor = _chronos_verified_cursor(r)
    cursor["record_set_chronos_proof"]["graph_incarnation"] = "old-lineage"
    monkeypatch.setattr(
        r,
        "_chronos_branch_incarnation",
        lambda: "current-lineage",
    )

    assert r._prior_record_set_chronos_verified(cursor) is False


def test_legacy_record_set_proof_without_incarnation_is_unverified():
    r = _runner()
    cursor = _chronos_verified_cursor(r)
    cursor["record_set_chronos_proof"].pop("graph_incarnation")

    assert r._prior_record_set_chronos_verified(cursor) is False
    sanitized = r._prior_cursor_for_adapter(
        mode="scope_complete",
        prior_cursor=cursor,
    )
    assert sanitized is None


# ---- eligibility ----

def test_not_eligible_on_reset():
    r = _runner()
    assert r._probe_skip_eligible(mode="cursor", reset_cursor=True, prior_cursor={}) is False


def test_completed_scope_modes_not_eligible_without_record_set_authority():
    r = _runner()
    for mode in ("reconcile", "scope_complete", "release_new", "release_unchanged"):
        assert r._probe_skip_eligible(mode=mode, reset_cursor=False, prior_cursor={}) is False, mode


def test_completed_scope_modes_not_eligible_with_unverified_record_set_authority():
    r = _runner()
    cursor = {"record_set_ref": {"storage": "okg.source_record_set_blobs"}}
    for mode in ("reconcile", "scope_complete", "release_new", "release_unchanged"):
        assert r._probe_skip_eligible(mode=mode, reset_cursor=False, prior_cursor=cursor) is False, mode


def test_completed_scope_modes_eligible_with_verified_record_set_authority():
    r = _runner()
    cursor = _chronos_verified_cursor(r)
    for mode in ("reconcile", "scope_complete", "release_new", "release_unchanged"):
        assert r._probe_skip_eligible(mode=mode, reset_cursor=False, prior_cursor=cursor) is True, mode


def test_eligible_on_cursor_mode_clean_authority():
    r = _runner()
    assert r._probe_skip_eligible(mode="cursor", reset_cursor=False, prior_cursor={}) is True


def test_not_eligible_when_authority_unverified():
    r = _runner()
    assert r._probe_skip_eligible(
        mode="cursor", reset_cursor=False, prior_cursor={"record_set": {"records": {}}}
    ) is False


def test_not_eligible_when_skip_budget_exhausted():
    r = _runner()
    cursor = {_PROBE_SKIP_COUNT_KEY: _probe_skip_budget()}
    assert r._probe_skip_eligible(mode="cursor", reset_cursor=False, prior_cursor=cursor) is False


def test_not_eligible_with_outstanding_backlog():
    r = _runner()
    cursor = {CURSOR_OUTSTANDING_KEY: {"next": "page-2"}}

    assert r._probe_skip_eligible(
        mode="cursor",
        reset_cursor=False,
        prior_cursor=cursor,
    ) is False


def test_due_reconcile_trigger_bypasses_probe_skip():
    r = _runner()
    r._active_sync_scope = {"trigger_kinds": ["poll", "reconcile"]}

    assert r._probe_skip_eligible(
        mode="cursor",
        reset_cursor=False,
        prior_cursor={},
    ) is False


def test_eligible_just_below_budget():
    r = _runner()
    cursor = {_PROBE_SKIP_COUNT_KEY: _probe_skip_budget() - 1}
    assert r._probe_skip_eligible(mode="cursor", reset_cursor=False, prior_cursor=cursor) is True


# ---- skip counter + token stamp ----

def test_probe_skips_reads_counter():
    r = _runner()
    assert r._probe_skips(None) == 0
    assert r._probe_skips({}) == 0
    assert r._probe_skips({_PROBE_SKIP_COUNT_KEY: 5}) == 5
    assert r._probe_skips({_PROBE_SKIP_COUNT_KEY: -1}) == 0  # defensive


def test_stamp_probe_token_sets_token_and_resets_counter():
    r = _runner()
    token = {"schema": "okg.source.change_probe.token.v1", "components": {}}
    out = r._stamp_probe_token({_PROBE_SKIP_COUNT_KEY: 7, "files": {"a": 1}}, token)
    assert out[CURSOR_TOKEN_KEY] == token
    assert out[_PROBE_SKIP_COUNT_KEY] == 0  # a real read resets the budget
    assert out["files"] == {"a": 1}  # preserves other cursor keys


def test_stamp_probe_token_from_none_cursor():
    r = _runner()
    token = {"schema": "okg.source.change_probe.token.v1", "components": {}}
    out = r._stamp_probe_token(None, token)
    assert out[CURSOR_TOKEN_KEY] == token
    assert out[_PROBE_SKIP_COUNT_KEY] == 0


def test_adapter_run_kwargs_passes_current_probe_verdict_only_when_supported():
    verdict = ProbeVerdict(changed=True, token={"current": "token"})

    class _Aware:
        def run(
            self,
            run_id,
            *,
            mode="cursor",
            cursor=None,
            change_probe_verdict=None,
        ):
            raise AssertionError("not invoked")

    class _Legacy:
        def run(self, run_id, *, mode="cursor", cursor=None):
            raise AssertionError("not invoked")

    common = {
        "mode": "cursor",
        "cursor": {"prior": "cursor"},
        "since_progress": None,
        "sync_scope": None,
        "change_probe_verdict": verdict,
    }
    aware = _adapter_run_kwargs(_Aware(), **common)
    legacy = _adapter_run_kwargs(_Legacy(), **common)

    assert aware["change_probe_verdict"] is verdict
    assert "change_probe_verdict" not in legacy


# --- source_has_published_facts reaches only adapters that name it (#2892) --


class _PublishedFactsQuery:
    """Stand-in for the runner's graph query; counts evaluations."""

    def __init__(self, answer: bool) -> None:
        self.answer = answer
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        return self.answer


_SELECTOR_COMMON = {
    "mode": "scope_complete",
    "cursor": {"prior": "cursor"},
    "since_progress": None,
    "sync_scope": None,
}


def test_forwarding_wrapper_never_hands_a_strict_inner_run_the_published_flag():
    """A `**kwargs` wrapper around a strict `run` still runs.

    The codex chunked-ingest test failed exactly this way: its wrapper
    forwarded every runner kwarg to `CodexSessionsSource.run`, which does
    not name `source_has_published_facts`.
    """

    class _StrictInner:
        def run(
            self,
            run_id,
            *,
            mode="cursor",
            cursor=None,
            since_progress=None,
            sync_scope=None,
            change_probe_verdict=None,
            authoritative_replay=False,
        ):
            return SourceRun(facts=[], completed_scope=True, run_mode=mode)

    class _ForwardingWrapper:
        def __init__(self) -> None:
            self._inner = _StrictInner()

        def run(self, run_id, **kwargs):
            return self._inner.run(run_id, **kwargs)

    query = _PublishedFactsQuery(False)
    wrapper = _ForwardingWrapper()
    selected = _adapter_run_kwargs(
        wrapper, **_SELECTOR_COMMON, source_has_published_facts=query,
    )

    assert "source_has_published_facts" not in selected
    # The other kwargs still flow through `**kwargs` as before.
    assert selected["cursor"] == {"prior": "cursor"}
    assert "authoritative_replay" in selected
    assert query.calls == 0
    wrapper.run("run-1", **selected)


def test_uninspectable_run_does_not_receive_the_published_flag():
    class _Opaque:
        __signature__ = "not a signature"

        def __call__(self, *args, **kwargs):
            raise AssertionError("not invoked")

    class _Adapter:
        run = _Opaque()

    query = _PublishedFactsQuery(True)
    selected = _adapter_run_kwargs(
        _Adapter(), **_SELECTOR_COMMON, source_has_published_facts=query,
    )

    assert "source_has_published_facts" not in selected
    assert selected["cursor"] == {"prior": "cursor"}
    assert query.calls == 0


def test_openspec_names_the_published_flag_and_receives_it_once():
    from okg.substrate.library.sources.openspec import OpenSpecSource

    class _OpenSpecSignature:
        run = OpenSpecSource.run

    query = _PublishedFactsQuery(False)
    selected = _adapter_run_kwargs(
        _OpenSpecSignature(),
        **_SELECTOR_COMMON,
        source_has_published_facts=query,
    )

    assert selected["source_has_published_facts"] is False
    assert query.calls == 1


def test_published_flag_is_omitted_when_the_runner_offers_none():
    class _Named:
        def run(self, run_id, *, mode="cursor", source_has_published_facts=None):
            raise AssertionError("not invoked")

    selected = _adapter_run_kwargs(_Named(), **_SELECTOR_COMMON)

    assert selected == {"mode": "scope_complete"}


# --- Sealed-snapshot acknowledgement (PACT runner-ack-sealed-snapshot-token) -


def _probe_token(seed: str) -> dict:
    return compose_token(
        content={"kind": "content_hash", "value": _hex(seed)},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )


def _ack(**overrides):
    values = {
        "applied_raw_token": _probe_token("snapshot"),
        "applied_token_authority": "exact",
        "applied_token_scope": "global",
        "applied_token_binding": "sealed_snapshot",
        "pre_token": _probe_token("snapshot"),
        "post_token": _probe_token("advanced"),
        "pipeline_stable": True,
        "replay_proof": True,
        "normalizes_replay_probe": False,
    }
    values.update(overrides)
    return applied_identity_acknowledgeable(**values)


def test_sealed_snapshot_with_advanced_input_is_acknowledged() -> None:
    assert _ack() == (True, "sealed_snapshot_input_advanced")


def test_sealed_applied_token_refused_when_input_did_not_move() -> None:
    """Pre- and post-run probes agree, so the input never moved.

    An applied token that differs from both was not computed from that
    input; the sealed-snapshot binding must not excuse it.
    """
    unchanged = _probe_token("unchanged")
    assert _ack(pre_token=unchanged, post_token=dict(unchanged)) == (
        False,
        "applied_token_diverges_without_input_change",
    )
    # Without a pre-read token there is no evidence either way; the
    # sealed-snapshot rule still applies.
    assert _ack(pre_token=None, post_token=unchanged) == (
        True,
        "sealed_snapshot_input_advanced",
    )


def test_equal_post_probe_is_acknowledged_without_sealed_binding() -> None:
    token = _probe_token("same")
    assert _ack(
        applied_raw_token=token,
        post_token=dict(token),
        applied_token_binding="unverified",
    ) == (True, "post_probe_equal")


def test_unsealed_run_with_advanced_input_is_refused() -> None:
    assert _ack(applied_token_binding="unverified") == (
        False,
        "post_probe_differs_without_sealed_snapshot",
    )


def test_sealed_but_non_global_or_inexact_token_is_refused() -> None:
    assert _ack(applied_token_scope="scoped") == (
        False,
        "applied_token_not_exact_global",
    )
    assert _ack(applied_token_authority="unavailable") == (
        False,
        "applied_token_not_exact_global",
    )
    assert _ack(applied_raw_token=None) == (
        False,
        "applied_token_not_exact_global",
    )


def test_sealed_but_pipeline_unstable_is_refused() -> None:
    assert _ack(pipeline_stable=False) == (
        False,
        "pipeline_snapshot_unstable",
    )


def test_sealed_but_replay_unproven_is_refused() -> None:
    assert _ack(replay_proof=False) == (
        False,
        "authoritative_replay_unproven",
    )


def test_sealed_but_post_probe_failed_is_refused() -> None:
    assert _ack(post_token=None) == (False, "post_probe_unavailable")
    assert _ack(post_token={}) == (False, "post_probe_unavailable")
    assert _ack(post_token={"not": "a token"}) == (
        False,
        "post_probe_token_malformed",
    )


def test_sealed_replay_normalization_path_still_requires_equality() -> None:
    assert _ack(normalizes_replay_probe=True) == (
        False,
        "replay_normalization_requires_equal_post_probe",
    )
    token = _probe_token("same")
    assert _ack(
        normalizes_replay_probe=True,
        applied_raw_token=token,
        post_token=dict(token),
    ) == (True, "post_probe_equal")


def test_refusal_message_names_every_decision_input() -> None:
    applied = _probe_token("snapshot")
    post = _probe_token("advanced")
    error = applied_identity_refusal(
        source_name="git-history",
        reason="post_probe_differs_without_sealed_snapshot",
        exact_applied_snapshot=False,
        pipeline_snapshot_stable=True,
        authoritative_replay_proof=True,
        applied_token_binding="unverified",
        applied_raw_token=applied,
        post_token=post,
    )

    message = str(error)
    assert message.startswith("source run cannot acknowledge change identity")
    assert "exact_applied_snapshot=False" in message
    assert "pipeline_snapshot_stable=True" in message
    assert "authoritative_replay_proof=True" in message
    assert "applied_token_binding=unverified" in message
    assert f"applied_token={identity_fingerprint(applied)}" in message
    assert f"post_run_token={identity_fingerprint(post)}" in message
    assert error.data == {
        "source_name": "git-history",
        "reason": "post_probe_differs_without_sealed_snapshot",
        "exact_applied_snapshot": False,
        "pipeline_snapshot_stable": True,
        "authoritative_replay_proof": True,
        "applied_token_binding": "unverified",
        "applied_token_fingerprint": identity_fingerprint(applied),
        "post_run_token_fingerprint": identity_fingerprint(post),
    }


def test_source_run_rejects_unknown_applied_token_binding() -> None:
    with pytest.raises(ValueError, match="applied_token_binding"):
        SourceRun(facts=iter(()), applied_token_binding="trust_me")
    assert SourceRun(facts=iter(())).applied_token_binding == "unverified"

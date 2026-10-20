"""Tests for the ChangeProbe contract (issue #323, incremental-source-probe-contract).

Proves the token-superset composition, collision-resistance (both directions),
machine-derived emit-version, the banned stat-digest tier, and the
one-directional soundness of ``run_probe`` (probe error => changed=True; an
unsound token can never short-circuit).
"""
from __future__ import annotations

import hashlib

import pytest

from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    ProbeVerdict,
    compose_token,
    emit_version_for,
    run_probe,
    tokens_equal,
    validate_token_composition,
)


def _hex(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _git_content(tree_sha: str) -> dict:
    return {"kind": "git_tree", "value": tree_sha}


def _git_head(commit_sha: str) -> dict:
    return {"kind": "git_head", "value": commit_sha}


# ---- ProbeVerdict shape ----

def test_probe_verdict_defaults():
    v = ProbeVerdict(changed=True)
    assert v.changed is True
    assert v.token == {}
    assert v.delta_hint is None  # advisory-only, defaults absent
    assert v.reason == ""


# ---- token composition + validation ----

def test_compose_token_includes_all_components():
    tok = compose_token(
        content=_git_content(_hex("tree")),
        config=_hex("cfg"),
        emit_version=_hex("emit"),
        head=_git_head(_hex("commit")),
    )
    comps = tok["components"]
    assert set(comps) == {"content", "config", "emit_version", "head"}
    validate_token_composition(tok)  # round-trips clean


def test_validator_rejects_content_only_token():
    bad = {
        "schema": "okg.source.change_probe.token.v1",
        "components": {"content": _git_content(_hex("tree"))},
    }
    with pytest.raises(ChangeProbeError, match="missing required component"):
        validate_token_composition(bad)


def test_validator_rejects_banned_stat_digest_kind():
    for kind in ("mtime", "size", "stat", "mtime_size"):
        with pytest.raises(ChangeProbeError, match="banned stat-digest"):
            compose_token(
                content={"kind": kind, "value": 12345},
                config=_hex("cfg"),
                emit_version=_hex("emit"),
            )


def test_validator_rejects_truncated_hash():
    with pytest.raises(ChangeProbeError, match="truncated digest"):
        compose_token(
            content=_git_content(_hex("tree")),
            config=_hex("cfg")[:16],  # truncated digest (16 < 32 hex) => lossy
            emit_version=_hex("emit"),
        )


def test_validator_accepts_content_hash_floor():
    tok = compose_token(
        content={"kind": "content_hash", "value": _hex("payload")},
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    validate_token_composition(tok)


# ---- collision resistance, both directions ----

def test_equal_state_yields_equal_token():
    args = dict(
        content=_git_content(_hex("tree")),
        config=_hex("cfg"),
        emit_version=_hex("emit"),
        head=_git_head(_hex("commit")),
    )
    assert tokens_equal(compose_token(**args), compose_token(**args))


def test_head_move_to_identical_tree_changes_token():
    # Same tree (content), different HEAD commit — the identical-tree HEAD move.
    base = dict(
        content=_git_content(_hex("tree")),
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    t1 = compose_token(head=_git_head(_hex("commit-A")), **base)
    t2 = compose_token(head=_git_head(_hex("commit-B")), **base)
    assert not tokens_equal(t1, t2)


def test_distinct_config_yields_distinct_token():
    base = dict(
        content=_git_content(_hex("tree")),
        emit_version=_hex("emit"),
    )
    assert not tokens_equal(
        compose_token(config=_hex("cfg-A"), **base),
        compose_token(config=_hex("cfg-B"), **base),
    )


def test_extra_inputs_fold_into_config():
    base = dict(
        content=_git_content(_hex("tree")),
        config=_hex("cfg"),
        emit_version=_hex("emit"),
    )
    plain = compose_token(**base)
    with_extra = compose_token(extra={"policy_fingerprint": "pf-1"}, **base)
    assert not tokens_equal(plain, with_extra)


# ---- machine-derived emit-version ----

def _emit_a(x):
    return x + 1


def _emit_b(x):
    return x + 2  # different body


def test_emit_version_is_stable_for_same_code():
    assert emit_version_for(_emit_a) == emit_version_for(_emit_a)


def test_emit_version_changes_with_emission_logic():
    assert emit_version_for(_emit_a) != emit_version_for(_emit_b)


def test_emit_version_requires_a_target():
    with pytest.raises(ChangeProbeError):
        emit_version_for()


# ---- run_probe: one-directional soundness ----

class _RaisingProbe:
    def probe(self, *, cursor, sync_scope):
        raise RuntimeError("git rev-parse failed mid-rebase")


class _UnchangedUnsoundProbe:
    """Returns changed=False with a content-only (unsound) token."""

    def probe(self, *, cursor, sync_scope):
        return ProbeVerdict(
            changed=False,
            token={
                "schema": "okg.source.change_probe.token.v1",
                "components": {"content": _git_content(_hex("tree"))},
            },
        )


class _UnchangedSoundProbe:
    def probe(self, *, cursor, sync_scope):
        return ProbeVerdict(
            changed=False,
            token=compose_token(
                content=_git_content(_hex("tree")),
                config=_hex("cfg"),
                emit_version=_hex("emit"),
            ),
        )


def test_run_probe_error_yields_changed_true():
    v = run_probe(_RaisingProbe(), cursor=None, sync_scope=None)
    assert v.changed is True
    assert v.reason.startswith("probe_error:")


def test_run_probe_unsound_unchanged_token_forces_full_read():
    v = run_probe(_UnchangedUnsoundProbe(), cursor=None, sync_scope=None)
    assert v.changed is True  # an unsound token can NEVER short-circuit
    assert "unsound_token_forces_full_read" in v.reason


def test_run_probe_honors_sound_unchanged_verdict():
    v = run_probe(_UnchangedSoundProbe(), cursor=None, sync_scope=None)
    assert v.changed is False


def test_run_probe_rejects_non_verdict_return():
    class _Bad:
        def probe(self, *, cursor, sync_scope):
            return {"changed": False}  # wrong type

    v = run_probe(_Bad(), cursor=None, sync_scope=None)
    assert v.changed is True

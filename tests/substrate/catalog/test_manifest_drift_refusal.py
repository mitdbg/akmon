"""#1791: the manifest-drift refusal names its reason and its recovery.

No database: these exercise ``_enforce_manifest_continuity`` directly with a
synthetic owner row. They live in their own file so the identity-continuity
Postgres suite is not touched by a reporting change.
"""
from __future__ import annotations

import re

import pytest

from okg.substrate.catalog import ownership as ownership_mod
from okg.substrate.catalog.ownership import CatalogIdentity
from okg.substrate.errors import CatalogOwnershipMismatch
from tests.substrate._fake_credentials import fake_credential


def test_manifest_drift_refusal_names_its_reason_and_recovery() -> None:
    """#1791: the drift refusal must be machine-identifiable and say how
    to repair it.

    Before the fix it passed no ``reason`` to ``_raise_mismatch``, so the
    payload read ``catalog_ownership_unspecified`` -- the same generic
    label as an unclaimed database or a foreign writer -- and the hint
    never said that ``okg catalog load --apply`` will not clear it.
    """
    identity = CatalogIdentity(
        deployment_name="drift-fixture",
        catalog_fingerprint="sha256:catalog",
        schema_fingerprint="sha256:schema",
        writer_fingerprint="sha256:writer",
        deployment_root_hash="sha256:root",
        worktree_root_hash=None,
        provenance={
            "deployment_root": "/srv/deployments/drift-fixture",
            "git_commit": "live-commit",
            "writer_scope": "deployment_root",
        },
        manifest_fingerprint="sha256:live",
        manifest_block_hashes={"nomos": "sha256:live-nomos"},
    )
    owner = {
        "manifest_fingerprint": "sha256:recorded",
        "claimed_at": "2026-09-09T00:00:00Z",
        "claimed_by_command": "okg catalog ownership claim",
        "metadata": {
            # Claim-time provenance: written by `claim_catalog_ownership`.
            "git_commit": "claimed-commit",
            "git_branch": "claimed-branch",
            "manifest_block_hashes": {"nomos": "sha256:recorded-nomos"},
            # Refreshed by every owned `okg catalog load --apply`; equal to
            # the live commit here, exactly as after a post-edit --apply.
            "last_seen_provenance": {"git_commit": "live-commit"},
        },
    }

    with pytest.raises(CatalogOwnershipMismatch) as exc:
        ownership_mod._enforce_manifest_continuity(
            identity=identity, owner=owner,
        )

    assert exc.value.data["reason_code"] == "catalog_manifest_drifted"
    assert exc.value.code == "catalog_manifest_drifted"
    assert exc.value.data["observed"]["drifted_blocks"] == ["nomos"]
    hint = exc.value.fix_hint or ""
    assert "okg catalog ownership claim --deployment" in hint
    assert "/srv/deployments/drift-fixture" in hint
    assert "catalog load --apply" in hint
    assert "does NOT" in hint
    # The "recorded" commit is the CLAIM's, not the one --apply refreshed:
    # otherwise the message prints recorded == live and reads as already
    # re-claimed.
    observed = exc.value.data["observed"]
    assert observed["recorded_git_commit"] == "claimed-commit"
    assert observed["recorded_git_branch"] == "claimed-branch"
    assert observed["last_seen_git_commit"] == "live-commit"
    assert observed["live_git_commit"] == "live-commit"
    message = str(exc.value)
    assert "recorded_git_commit=claimed-commit" in message
    assert "last_seen_git_commit=live-commit" in message
    assert observed["recorded_provenance_source"] == "claim"


def test_manifest_drift_refusal_falls_back_to_last_seen_for_backfilled_rows() -> None:
    """A row backfilled by ``mark_catalog_version_seen`` has no claim-time
    provenance keys; the refusal must say so rather than print None."""
    identity = CatalogIdentity(
        deployment_name="drift-fixture",
        catalog_fingerprint="sha256:catalog",
        schema_fingerprint="sha256:schema",
        writer_fingerprint="sha256:writer",
        deployment_root_hash="sha256:root",
        worktree_root_hash=None,
        provenance={"git_commit": "live-commit"},
        manifest_fingerprint="sha256:live",
        manifest_block_hashes={"nomos": "sha256:live-nomos"},
    )
    owner = {
        "manifest_fingerprint": "sha256:recorded",
        "metadata": {
            "manifest_block_hashes": {},
            "last_seen_provenance": {"git_commit": "seen-commit"},
        },
    }
    with pytest.raises(CatalogOwnershipMismatch) as exc:
        ownership_mod._enforce_manifest_continuity(
            identity=identity, owner=owner,
        )
    observed = exc.value.data["observed"]
    assert observed["recorded_git_commit"] == "seen-commit"
    assert observed["recorded_provenance_source"] == "last_seen"


def test_recommended_commands_never_print_the_suggested_dsn_password() -> None:
    """`ownership claim --json` is pasted into tickets; it carried a password.

    `_recommended_commands` embedded `suggested_worktree_dsn` verbatim, so
    every diagnose/claim payload printed `postgres://postgres:<password>@...`
    in clear text (#2213). Only the redacted spelling may appear.
    """
    identity = CatalogIdentity(
        deployment_name="journey",
        catalog_fingerprint="sha256:" + "c" * 64,
        schema_fingerprint="sha256:" + "d" * 64,
        writer_fingerprint="sha256:" + "e" * 64,
        manifest_fingerprint="sha256:" + "f" * 64,
        manifest_block_hashes={},
        deployment_root_hash="sha256:" + "1" * 64,
        worktree_root_hash=None,
        provenance={"writer_scope": "deployment_root"},
    )
    commands = ownership_mod._recommended_commands(
        identity, "legacy_history", host="127.0.0.1", port=56100,
    )
    dsn_commands = [c for c in commands if "--dsn" in c]
    assert dsn_commands, commands
    for command in dsn_commands:
        assert "postgres://postgres:***@" in command, command
    joined = "\n".join(commands)
    # Any userinfo password that is not the redaction marker is a leak.
    assert re.search(r"://[^:/@\s]*:(?!\*\*\*)[^@\s]+@", joined) is None, joined


def test_redaction_keeps_content_digests_readable() -> None:
    """The drift refusal prints fingerprints; they must not read `<redacted>`.

    `redact_text` treated any 24+ character word as a credential, so the
    refusal an operator has to act on printed `recorded=sha256:<redacted>
    live=sha256:<redacted>` -- two identical, useless strings (#2213).
    """
    from okg.substrate.sources.redaction import redact_text

    recorded = "sha256:" + "a" * 64
    live = "sha256:" + "b" * 64
    text = redact_text(f"recorded={recorded} live={live}")
    assert text == f"recorded={recorded} live={live}"
    # Real credentials are still removed.
    assert redact_text(fake_credential("glpat-", "AbCdEfGhIjKlMnOpQrSt")) == "<redacted>"
    assert "secret" not in redact_text("postgres://u:secret@h:5432/db")

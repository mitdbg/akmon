from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from okg.substrate.external_actions.config import load_config
from okg.substrate.external_actions import restore_fence
from okg.substrate.external_actions.restore_fence import (
    CHAIN_SCHEMA,
    EXTERNAL_ACTION_ROLLOUT_BLOCKED,
    EXTERNAL_ACTION_ROLLOUT_DISABLED,
    EXTERNAL_ACTION_ROLLOUT_ENABLED,
    EXTERNAL_ACTION_RESTORE_REFUSAL_CODE,
    external_action_rollout_state,
    merkle_leaf,
    merkle_root,
    pre_bind_restore_fence,
    resolve_restore_reference,
    verify_restore_proof,
    verify_restore_envelope,
    verify_lease_chain,
)
from okg.substrate.external_actions.signing import (
    b64url_encode,
    build_detached_envelope,
    canonical_json_bytes,
    domain_digest,
    generate_keypair,
)


DEPLOYMENT = "support-prod"
KID = "support-prod/epoch/1"
NOW = datetime(2026, 8, 26, 14, 30, tzinfo=UTC)


class _ProbeConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[object, object | None]] = []
        self.transaction_entries = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def transaction(self):
        self.transaction_entries += 1
        return nullcontext()

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        return self

    def fetchall(self):
        return []


def test_database_rollout_probe_uses_sanctioned_read_only_connection(
    monkeypatch,
) -> None:
    connection = _ProbeConnection()
    observed: list[tuple[str, str | None]] = []

    def connect(dsn: str, *, application_name: str | None = None):
        observed.append((dsn, application_name))
        return connection

    monkeypatch.setattr("okg.substrate.db.connection.connect", connect)

    assert restore_fence._database_external_action_footprint(
        deployment=DEPLOYMENT,
        dsn="postgresql://rollout-proof",
    ) == EXTERNAL_ACTION_ROLLOUT_DISABLED
    assert observed == [
        (
            "postgresql://rollout-proof",
            "okg-external-action-rollout-guard",
        )
    ]
    assert connection.transaction_entries == 1
    assert connection.statements[0] == ("SET TRANSACTION READ ONLY", None)
    assert len(connection.statements) == 2
    assert str(connection.statements[1][0]).lstrip().startswith("SELECT")


def test_database_rollout_probe_fails_closed_when_connection_fails(
    monkeypatch,
) -> None:
    def fail_connect(_dsn: str, *, application_name: str | None = None):
        assert application_name == "okg-external-action-rollout-guard"
        raise RuntimeError("unavailable")

    monkeypatch.setattr("okg.substrate.db.connection.connect", fail_connect)

    assert restore_fence._database_external_action_footprint(
        deployment=DEPLOYMENT,
        dsn="postgresql://rollout-proof",
    ) == EXTERNAL_ACTION_ROLLOUT_BLOCKED


@pytest.mark.parametrize(
    ("database_state", "expected"),
    [
        (EXTERNAL_ACTION_ROLLOUT_DISABLED, EXTERNAL_ACTION_ROLLOUT_DISABLED),
        (EXTERNAL_ACTION_ROLLOUT_ENABLED, EXTERNAL_ACTION_ROLLOUT_ENABLED),
        (EXTERNAL_ACTION_ROLLOUT_BLOCKED, EXTERNAL_ACTION_ROLLOUT_BLOCKED),
    ],
)
def test_rollout_guard_requires_database_proof_when_authority_files_are_absent(
    tmp_path, monkeypatch, database_state: str, expected: str
) -> None:
    observed = []

    def probe(*, deployment: str, dsn: str) -> str:
        observed.append((deployment, dsn))
        return database_state

    monkeypatch.setattr(
        "okg.substrate.external_actions.restore_fence._database_external_action_footprint",
        probe,
    )

    assert external_action_rollout_state(
        deployment=DEPLOYMENT,
        deployment_root=tmp_path,
        dsn="postgresql://rollout-proof",
    ) == expected
    assert observed == [(DEPLOYMENT, "postgresql://rollout-proof")]


def test_rollout_guard_blocks_authority_created_during_database_probe(
    tmp_path, monkeypatch
) -> None:
    def probe(*, deployment: str, dsn: str) -> str:
        assert deployment == DEPLOYMENT
        assert dsn == "postgresql://rollout-proof"
        (tmp_path / "external-action-review.yaml").write_text(
            "review: authority\n", encoding="utf-8"
        )
        (tmp_path / "external-actions.yaml").write_text(
            "actions: []\n", encoding="utf-8"
        )
        return EXTERNAL_ACTION_ROLLOUT_DISABLED

    monkeypatch.setattr(
        "okg.substrate.external_actions.restore_fence._database_external_action_footprint",
        probe,
    )

    assert external_action_rollout_state(
        deployment=DEPLOYMENT,
        deployment_root=tmp_path,
        dsn="postgresql://rollout-proof",
    ) == EXTERNAL_ACTION_ROLLOUT_BLOCKED


def test_rollout_guard_recognizes_only_complete_valid_authority(
    tmp_path, monkeypatch
) -> None:
    review_path = tmp_path / "external-action-review.yaml"
    registry_path = tmp_path / "external-actions.yaml"
    review_path.write_text("review: authority\n", encoding="utf-8")
    registry_path.write_text("actions: []\n", encoding="utf-8")
    authority = object()
    config = SimpleNamespace(
        deployment=DEPLOYMENT,
        deployment_product_authority=authority,
        verifier_capability_env="OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY",
        action_registry_path="external-actions.yaml",
    )
    monkeypatch.setattr(
        "okg.substrate.external_actions.config.load_config", lambda _path: config
    )
    monkeypatch.setattr(
        "okg.substrate.external_actions.config.load_action_contracts",
        lambda _path: {"schema": "okg.external_action_contracts/v1"},
    )
    monkeypatch.setattr(
        "okg.substrate.external_actions.config.load_deployment_product_authority",
        lambda *_args, **_kwargs: (object(), object(), object()),
    )
    monkeypatch.setattr(
        "okg.substrate.external_actions.action_contracts.resolve_action_contracts",
        lambda *_args, **_kwargs: {"ticket.update": object()},
    )

    assert external_action_rollout_state(
        deployment=DEPLOYMENT,
        deployment_root=tmp_path,
        dsn="postgresql://unused",
    ) == EXTERNAL_ACTION_ROLLOUT_ENABLED


def test_rollout_guard_blocks_partial_or_malformed_authority(
    tmp_path, monkeypatch
) -> None:
    review_path = tmp_path / "external-action-review.yaml"
    review_path.write_text("not: a-valid-review-config\n", encoding="utf-8")
    assert external_action_rollout_state(
        deployment=DEPLOYMENT,
        deployment_root=tmp_path,
        dsn="postgresql://unused",
    ) == EXTERNAL_ACTION_ROLLOUT_BLOCKED

    (tmp_path / "external-actions.yaml").write_text(
        "not: a-valid-registry\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        "okg.substrate.external_actions.config.load_config",
        lambda _path: (_ for _ in ()).throw(ValueError("malformed")),
    )
    assert external_action_rollout_state(
        deployment=DEPLOYMENT,
        deployment_root=tmp_path,
        dsn="postgresql://unused",
    ) == EXTERNAL_ACTION_ROLLOUT_BLOCKED
    assert (
        EXTERNAL_ACTION_RESTORE_REFUSAL_CODE
        == "external_action_restore_requires_external_fence"
    )


class _FenceStore:
    def __init__(self) -> None:
        self.inserted = []
        self.fence = None

    def retained_leases(self, **_):
        return tuple(item["lease"] for item in self.inserted)

    def restore_fence(self, **_):
        return self.fence

    def insert_lease(self, **values):
        self.inserted.append(values)

    def set_restore_fence(self, **values):
        self.fence = values

    def transaction(self):
        return nullcontext()


def _lease_fixture(*, with_private_key=False):
    private_key, public_key = generate_keypair()
    config = load_config(
        {
            "schema": "okg.external_action_review.config/v1",
            "deployment": DEPLOYMENT,
            "authentication": {
                "audience": "deployment_scoped_review_api",
                "allowed_algorithms": ["EdDSA"],
            },
            "signing_keys": [
                {
                    "kid": KID,
                    "purpose": "execution_epoch_lease",
                    "algorithm": "EdDSA",
                    "public_key_b64": public_key,
                    "state": "current",
                }
            ],
        }
    )
    payload = {
        "deployment": DEPLOYMENT,
        "epoch": 1,
        "lease_sequence": 1,
        "previous_lease_digest": None,
        "previous_epoch_digest": None,
        "issued_at": "2026-08-26T14:00:00.000000Z",
        "expires_at": "2026-08-26T15:00:00.000000Z",
        "signer_separation_subject": "subject:lease-signer",
        "nonce": "lease-nonce-1",
    }
    envelope = build_detached_envelope(
        payload,
        private_key,
        kid=KID,
        typ="okg-execution-epoch+jws",
        domain="okg-external-action-execution-epoch-lease-envelope-v1",
    )
    chain = {"schema": CHAIN_SCHEMA, "deployment": DEPLOYMENT, "leases": [envelope]}
    if with_private_key:
        return config, chain, private_key
    return config, chain


def test_verified_lease_retains_domain_separated_signing_key_identity() -> None:
    config, chain = _lease_fixture()

    leases, _ = verify_lease_chain(chain, config=config, now=NOW)

    assert leases[0].signer_key_id_digest == domain_digest(
        "okg-external-action-execution-epoch-signing-key-id-v1",
        {"kid": KID, "purpose": "execution_epoch_lease"},
    )


def test_pre_bind_fence_persists_only_the_key_identity_selected_by_verification() -> None:
    config, chain = _lease_fixture()
    store = _FenceStore()

    result = pre_bind_restore_fence(
        chain,
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
    )

    verified = store.inserted[0]["lease"]
    assert store.inserted[0]["signer_key_id_digest"] == verified.signer_key_id_digest
    assert result.inserted_count == 1
    assert store.fence["lease_chain_digest"] == result.lease_chain_digest


def _successor_chain(chain, private_key):
    prior = chain["leases"][0]
    successor_payload = {
        "deployment": DEPLOYMENT,
        "epoch": 1,
        "lease_sequence": 2,
        "previous_lease_digest": prior["envelope_digest"],
        "previous_epoch_digest": None,
        "issued_at": "2026-08-26T14:10:00.000000Z",
        "expires_at": "2026-08-26T15:10:00.000000Z",
        "signer_separation_subject": "subject:lease-signer",
        "nonce": "lease-nonce-2",
    }
    successor = build_detached_envelope(
        successor_payload,
        private_key,
        kid=KID,
        typ="okg-execution-epoch+jws",
        domain="okg-external-action-execution-epoch-lease-envelope-v1",
    )
    return {**chain, "leases": [prior, successor]}


def test_exact_restart_rejects_verification_config_mutation() -> None:
    config, chain = _lease_fixture()
    store = _FenceStore()
    verification_config = {"binding_version": 1}
    login_binding = {"session_user": "okg_external_action_support_prod"}

    pre_bind_restore_fence(
        chain,
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
        verification_config=verification_config,
        login_binding=login_binding,
    )

    with pytest.raises(
        ValueError, match="verification config changed without a signed lease successor"
    ):
        pre_bind_restore_fence(
            chain,
            config=config,
            store=store,
            deployment=DEPLOYMENT,
            now=NOW,
            verification_config={"binding_version": 2},
            login_binding=login_binding,
        )


def test_exact_restart_rejects_login_binding_mutation() -> None:
    config, chain = _lease_fixture()
    store = _FenceStore()
    verification_config = {"binding_version": 1}
    login_binding = {"session_user": "okg_external_action_support_prod"}

    pre_bind_restore_fence(
        chain,
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
        verification_config=verification_config,
        login_binding=login_binding,
    )

    with pytest.raises(
        ValueError, match="login binding changed without a signed lease successor"
    ):
        pre_bind_restore_fence(
            chain,
            config=config,
            store=store,
            deployment=DEPLOYMENT,
            now=NOW,
            verification_config=verification_config,
            login_binding={"session_user": "other_login"},
        )


def test_exact_restart_with_same_context_is_idempotent() -> None:
    config, chain = _lease_fixture()
    store = _FenceStore()
    verification_config = {"binding_version": 1}
    login_binding = {"session_user": "okg_external_action_support_prod"}
    kwargs = {
        "config": config,
        "store": store,
        "deployment": DEPLOYMENT,
        "now": NOW,
        "verification_config": verification_config,
        "login_binding": login_binding,
    }

    first = pre_bind_restore_fence(chain, **kwargs)
    replay = pre_bind_restore_fence(chain, **kwargs)

    assert first.inserted_count == 1
    assert replay.inserted_count == 0
    assert replay.lease_chain_digest == first.lease_chain_digest


def test_context_mutation_is_accepted_only_with_a_signed_successor() -> None:
    config, chain, private_key = _lease_fixture(with_private_key=True)
    # A successor must be signed by the pinned key; changing only the trust
    # projection is never sufficient.
    successor_chain = _successor_chain(chain, private_key)
    store = _FenceStore()
    old_config = {"binding_version": 1}
    old_binding = {"session_user": "okg_external_action_support_prod"}

    pre_bind_restore_fence(
        chain,
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
        verification_config=old_config,
        login_binding=old_binding,
    )
    restarted = pre_bind_restore_fence(
        successor_chain,
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
        verification_config={"binding_version": 2},
        login_binding={"session_user": "other_login"},
    )

    assert restarted.inserted_count == 1
    assert restarted.verification_config_digest == domain_digest(
        "okg-external-action-startup-verification-config-v1", {"binding_version": 2}
    )
    assert restarted.verification_login_binding_digest == domain_digest(
        "okg-external-action-startup-login-binding-v1", {"session_user": "other_login"}
    )


def test_context_mutation_without_a_signed_successor_is_rejected() -> None:
    config, chain = _lease_fixture()
    store = _FenceStore()
    pre_bind_restore_fence(
        chain,
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
        verification_config={"binding_version": 1},
        login_binding={"session_user": "okg_external_action_support_prod"},
    )

    with pytest.raises(ValueError, match="signed lease successor"):
        pre_bind_restore_fence(
            chain,
            config=config,
            store=store,
            deployment=DEPLOYMENT,
            now=NOW,
            verification_config={"binding_version": 2},
            login_binding={"session_user": "other_login"},
        )


def test_empty_store_accepts_a_contiguous_multi_epoch_chain_and_quarantines() -> None:
    config, chain, private_key = _lease_fixture(with_private_key=True)
    prior = chain["leases"][0]
    successor_payload = {
        "deployment": DEPLOYMENT,
        "epoch": 2,
        "lease_sequence": 1,
        "previous_lease_digest": None,
        "previous_epoch_digest": prior["envelope_digest"],
        "issued_at": "2026-08-26T14:10:00.000000Z",
        "expires_at": "2026-08-26T15:10:00.000000Z",
        "signer_separation_subject": "subject:lease-signer",
        "nonce": "lease-epoch-2",
    }
    successor = build_detached_envelope(
        successor_payload,
        private_key,
        kid=KID,
        typ="okg-execution-epoch+jws",
        domain="okg-external-action-execution-epoch-lease-envelope-v1",
    )
    store = _FenceStore()
    result = pre_bind_restore_fence(
        {**chain, "leases": [prior, successor]},
        config=config,
        store=store,
        deployment=DEPLOYMENT,
        now=NOW,
    )
    assert result.highest_epoch == 2
    assert result.service_mode == "restore_only"
    assert result.quarantined is True
    assert len(store.inserted) == 2


def test_inline_jws_reference_round_trips_without_a_path_or_url_resolver() -> None:
    config, chain = _lease_fixture()
    envelope = chain["leases"][0]
    reference = "jws:" + b64url_encode(canonical_json_bytes(envelope))
    resolved = resolve_restore_reference(reference, resolver=None, purpose="execution_epoch_lease")
    verified = verify_restore_envelope(
        resolved,
        config=config,
        purpose="execution_epoch_lease",
        now=NOW,
    )
    assert verified.envelope_digest == envelope["envelope_digest"]


def test_restore_positive_proof_binds_directions_and_exact_tree_width() -> None:
    entries = [("sha256:" + "1" * 64, "sha256:" + "a" * 64), ("sha256:" + "2" * 64, "sha256:" + "b" * 64), ("sha256:" + "3" * 64, "sha256:" + "c" * 64)]
    entries.sort()
    root = merkle_root(entries)
    leaves = [merkle_leaf(*entry) for entry in entries]
    parent01 = domain_digest("okg-external-action-restore-merkle-node-v1", {"left_digest": leaves[0], "right_digest": leaves[1]})
    proof = {
        "effect_instance_key": entries[2][0],
        "ledger_entry_digest": entries[2][1],
        "leaf_index": 2,
        "leaf_count": 3,
        "sibling_steps": [
            {"side": "right", "digest": None},
            {"side": "left", "digest": parent01},
        ],
    }
    assert verify_restore_proof(proof, proof_kind="positive", effect_key=entries[2][0], root=root, count=3)
    with pytest.raises(ValueError, match="direction|extra levels"):
        verify_restore_proof(
            {**proof, "sibling_steps": [{"side": "left", "digest": None}, *proof["sibling_steps"][1:]]},
            proof_kind="positive", effect_key=entries[2][0], root=root, count=3,
        )
    with pytest.raises(ValueError, match="extra levels"):
        verify_restore_proof(
            {**proof, "sibling_steps": [*proof["sibling_steps"], {"side": "right", "digest": None}]},
            proof_kind="positive", effect_key=entries[2][0], root=root, count=3,
        )

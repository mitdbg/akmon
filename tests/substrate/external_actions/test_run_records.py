"""Closed run reporting and transaction-bound authority contracts."""

from copy import deepcopy

import pytest

from okg.substrate.external_actions.api import APIError, ExternalActionAPI
from okg.substrate.external_actions.run_records import (
    authorization_digest,
    registration_body,
)
from okg.substrate.external_actions.service import ExternalActionError


def registration():
    return {
        "run_id": "run-1",
        "automation_id": "triage",
        "automation_version": "1.0",
        "correlation_id": "correlation-1",
        "generation_id": "okg:support:branch:default:gen:published",
        "reported_distribution": {
            "name": "support",
            "version": "3",
            "package_digest": None,
        },
    }


def test_registration_is_reported_identity_not_execution():
    body = registration()
    assert registration_body(body) == body
    for field in (
        "status",
        "owner",
        "registered_at",
        "read_verified",
        "budget",
        "heartbeat",
    ):
        with pytest.raises(ExternalActionError, match="invalid run record"):
            registration_body({**body, field: "succeeded"})
    body["reported_distribution"]["verification"] = "verified"
    with pytest.raises(ExternalActionError):
        registration_body(body)


@pytest.mark.parametrize("bad", [None, True, 0, -1, 1.5, "", "a" * 1025, "bad\nref"])
def test_generation_requires_a_bounded_exact_checkpoint_string(bad):
    body = registration()
    body["generation_id"] = bad
    with pytest.raises(ExternalActionError):
        registration_body(body)


@pytest.mark.parametrize(
    "field", ["run_id", "automation_id", "automation_version", "correlation_id"]
)
def test_identity_rejects_unbounded_or_structured_values(field):
    for bad in ("", "a" * 129, {}, "has space", None):
        body = registration()
        body[field] = bad
        with pytest.raises(ExternalActionError):
            registration_body(body)


def test_distribution_requires_all_reported_fields_without_trust_claims():
    for bad in (
        {},
        {"name": "support", "version": "3"},
        {"name": "support", "version": "3", "package_digest": "bad"},
    ):
        body = registration()
        body["reported_distribution"] = bad
        with pytest.raises(ExternalActionError):
            registration_body(body)
    body = registration()
    body["reported_distribution"]["package_digest"] = "sha256:" + "a" * 64
    assert registration_body(body) == body


def test_each_authority_digest_binds_transaction_owner_and_exact_resource():
    args = dict(
        transaction_id="123",
        deployment="support",
        owner="service",
        record_id="run",
        proposal_id="proposal",
        request_hash="sha256:" + "a" * 64,
        operation="external_automation.link",
    )
    baseline, target = authorization_digest(**args)
    assert target == {
        "kind": "external_automation_run",
        "id": "run",
        "operation": "external_automation.link",
    }
    for key, replacement in (
        ("transaction_id", "124"),
        ("owner", "other"),
        ("deployment", "other"),
        ("record_id", "other"),
        ("proposal_id", "other"),
        ("operation", "external_action.read"),
    ):
        changed = deepcopy(args)
        changed[key] = replacement
        assert authorization_digest(**changed)[0] != baseline
    read_args = {**args, "operation": "external_action.read"}
    assert authorization_digest(**read_args)[1]["id"] == "proposal"


def test_run_routes_have_no_lifecycle_or_list_surface():
    api = ExternalActionAPI()
    base = "/v1/deployments/support/external-automation-runs"
    record = "01J63N8FV2P3XT8VZB4Z7NJ27A"
    assert api._route("POST", base).operation == "run_register"
    assert api._route("GET", base + "/" + record).operation == "run_read"
    assert (
        api._route("POST", base + "/" + record + "/proposals").operation == "run_link"
    )
    for method, path in (
        ("GET", base),
        ("POST", base + "/" + record + "/finish"),
        ("DELETE", base + "/" + record),
    ):
        with pytest.raises(APIError):
            api._route(method, path)


def test_sql_identity_timestamp_spelling_preserves_the_verified_instant():
    from datetime import datetime, timezone
    from okg.substrate.external_actions.store import PostgresExternalActionStore
    from okg.substrate.external_actions.auth import _coerce_time

    store = object.__new__(PostgresExternalActionStore)
    raw = {
        "provider_updated_at": "2026-09-10T08:00:00.123+02:00",
        "binding_updated_at": "2026-09-10T06:00:00+00:00",
        "unrelated": "unchanged",
    }
    store._verifier_call_in_transaction = lambda *args: raw
    result = store.resolve_principal("issuer", "subject")
    assert result == {
        "provider_updated_at": "2026-09-10T06:00:00.123000Z",
        "binding_updated_at": "2026-09-10T06:00:00.000000Z",
        "unrelated": "unchanged",
    }
    assert _coerce_time(result["provider_updated_at"]) == datetime(
        2026, 9, 10, 6, 0, 0, 123000, tzinfo=timezone.utc
    )
    assert raw["provider_updated_at"].endswith("+02:00")


def test_sql_identity_timestamp_refuses_naive_or_invalid_values_without_fallback():
    from datetime import datetime, tzinfo
    from okg.substrate.external_actions.store import (
        PostgresExternalActionStore,
        ExternalActionStoreError,
    )

    class UnknownOffset(tzinfo):
        def utcoffset(self, dt):
            return None

    store = object.__new__(PostgresExternalActionStore)
    for bad in (
        "2026-09-10T06:00:00",
        "not-a-date",
        1,
        {},
        datetime(2026, 9, 10),
        datetime(2026, 9, 10, tzinfo=UnknownOffset()),
    ):
        store._verifier_call_in_transaction = lambda *args: {"provider_updated_at": bad}
        with pytest.raises(
            ExternalActionStoreError, match="identity timestamp unavailable"
        ):
            store.resolve_principal("issuer", "subject")
    store._verifier_call_in_transaction = lambda *args: {"provider_updated_at": None}
    assert store.resolve_principal("issuer", "subject") == {"provider_updated_at": None}


def test_durable_bucket_truthiness_counts_only_retained_facts():
    from types import SimpleNamespace
    from okg.substrate.external_actions.store import _DurableBuckets
    from okg.substrate.external_actions.restore_fence import _has_external_action_state

    writes = []
    buckets = _DurableBuckets(
        SimpleNamespace(_append=lambda *args: writes.append(args)), "events"
    )
    store = SimpleNamespace(events=buckets)
    assert not buckets and not _has_external_action_state(store, "deployment")
    one = buckets["one"]
    two = buckets["two"]
    assert len(one) == len(two) == 0
    assert not buckets and not _has_external_action_state(store, "deployment")
    buckets._load("two", {"retained": True})
    assert buckets and _has_external_action_state(store, "deployment")
    assert writes == []


def test_payload_version_hydration_uses_actual_submit_or_replacement_hash():
    from okg.substrate.external_actions.models import PayloadVersion
    from okg.substrate.external_actions.store import (
        PostgresExternalActionStore,
        ExternalActionStoreError,
    )

    base = {"proposal_id": "proposal", "version": 1, "canonical_payload": b"{}"}

    def normalize(value):
        return PostgresExternalActionStore._coerce_record(value, PayloadVersion)

    original = {**base, "payload_hash": "retained"}
    assert normalize(original) == original
    assert (
        normalize(
            {**base, "current_payload_hash": "current", "approved_payload_hash": "old"}
        )["payload_hash"]
        == "current"
    )
    assert (
        normalize(
            {
                **base,
                "operation": "edit",
                "current_payload_hash": "old",
                "replacement_payload_hash": "new",
            }
        )["payload_hash"]
        == "new"
    )
    for value in (
        {**base, "payload_hash": "different", "current_payload_hash": "current"},
        {
            **base,
            "operation": "approve_with_edit",
            "payload_hash": "old",
            "replacement_payload_hash": "new",
        },
        {**base, "replacement_payload_hash": "unsupported"},
    ):
        with pytest.raises(ExternalActionStoreError):
            normalize(value)
    with pytest.raises(TypeError):
        PayloadVersion(**normalize({**base, "approved_payload_hash": "unrelated"}))
    assert normalize({**original, "operation": "edit"}) == original
    assert (
        normalize({**base, "operation": "submit", "current_payload_hash": "current"})[
            "payload_hash"
        ]
        == "current"
    )
    for operation in ("edit", "approve_with_edit", "approve", "unknown", None, [], {}):
        with pytest.raises(ExternalActionStoreError, match="invalid payload-version"):
            normalize({**base, "operation": operation, "current_payload_hash": "old"})
    for operation in (None, [], {}):
        with pytest.raises(ExternalActionStoreError, match="invalid payload-version"):
            normalize(
                {**base, "operation": operation, "replacement_payload_hash": "new"}
            )
    for alias in ("current_payload_hash", "replacement_payload_hash"):
        for malformed in (None, "", 7, {}):
            with pytest.raises(
                ExternalActionStoreError, match="invalid payload-version"
            ):
                normalize(
                    {
                        **base,
                        "operation": "edit"
                        if alias == "replacement_payload_hash"
                        else "submit",
                        alias: malformed,
                    }
                )
    with pytest.raises(ExternalActionStoreError, match="invalid payload-version"):
        normalize(
            {
                **base,
                "operation": "edit",
                "current_payload_hash": None,
                "replacement_payload_hash": "new",
            }
        )


def test_policy_denial_classification_excludes_runtime_failures():
    from dataclasses import replace
    from okg.substrate.nomos.core import DecisionInput, compile_policy_graph, evaluate
    from okg.substrate.nomos.runtime_enforcement import NomosRuntimeOutcome
    from okg.substrate.external_actions.store import _is_explicit_policy_denial

    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "classification",
            "policy_version": "1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "unmatched",
                    "effect": "allow",
                    "actions": ["external_automation.register"],
                    "subjects": ["service"],
                }
            ],
        }
    )
    decision = evaluate(
        bundle,
        DecisionInput(
            subject="service", action="external_automation.read", resource={}
        ),
    )
    outcome = NomosRuntimeOutcome(decision, False, "deny", (), (), (), (), {})
    fingerprint = bundle.policy_fingerprint
    assert _is_explicit_policy_denial(outcome, fingerprint)
    assert _is_explicit_policy_denial(
        replace(
            outcome,
            decision=replace(
                decision, matched_rule_ids=("deny-rule",), reason_code="policy-refusal"
            ),
        ),
        fingerprint,
    )
    for reason in (
        "nomos.runtime.obligation_missing",
        "nomos.runtime.obligation_blocked",
        "nomos.runtime.stale_active_state",
        "nomos.invalid_identity_fact",
        "nomos.default_deny.no_active_bundle",
    ):
        assert not _is_explicit_policy_denial(
            replace(outcome, decision=replace(decision, reason_code=reason)),
            fingerprint,
        )
    assert not _is_explicit_policy_denial(
        replace(outcome, missing_obligations=("approval",)), fingerprint
    )
    assert not _is_explicit_policy_denial(
        replace(outcome, required_obligations=("audit",)), fingerprint
    )
    assert not _is_explicit_policy_denial(outcome, "different")
    assert not _is_explicit_policy_denial(outcome, None)
    assert not _is_explicit_policy_denial(
        {"allowed": False, "final_effect": "deny"}, fingerprint
    )


def test_clone_physical_exclusions_use_catalog_capabilities_and_preserve_controls():
    from okg.substrate.environment_clone import _timescale_purge_relations

    class Catalog:
        def __init__(self, columns, rows, present=True):
            self.columns, self.rows, self.present = columns, rows, present
            self.queries = []

        def execute(self, query):
            self.queries.append(query)
            return self

        def fetchone(self):
            return {"relation": "hypertable" if self.present else None}

        def fetchall(self):
            if len(self.queries) == 2:
                return [
                    {"relation": table, "column": column}
                    for table, columns in self.columns.items()
                    for column in columns
                ]
            return self.rows

    def row(parent, **changes):
        return {
            "parent_schema": "okg",
            "parent_table": parent,
            "chunk_reference": 1,
            "chunk_schema": "internal",
            "chunk_table": "ordinary",
            "compressed_reference": 2,
            "compressed_schema": "internal",
            "compressed_table": "compressed",
            "compressed_parent_reference": None,
            "compressed_parent_schema": None,
            "compressed_parent_table": None,
            **changes,
        }

    inventory = [
        {"table": "okg.trace", "disposition": "purge"},
        {"table": "okg.facts", "disposition": "preserve"},
    ]
    current = {
        "hypertable": {"id", "schema_name", "table_name"},
        "chunk": {"hypertable_id", "relid"},
        "compression_settings": {"relid", "compress_relid"},
    }
    conn = Catalog(
        current,
        [
            row("trace"),
            row(
                "facts",
                chunk_table="preserved",
                compressed_table="preserved_compressed",
            ),
        ],
    )
    assert _timescale_purge_relations(conn, inventory) == [
        "internal.compressed",
        "internal.ordinary",
    ]
    assert "settings.compress_relid" in conn.queries[-1]
    legacy = {
        "hypertable": current["hypertable"] | {"compressed_hypertable_id"},
        "chunk": {
            "id",
            "hypertable_id",
            "schema_name",
            "table_name",
            "compressed_chunk_id",
        },
    }
    conn = Catalog(
        legacy,
        [
            row(
                "trace",
                compressed_parent_reference=3,
                compressed_parent_schema="internal",
                compressed_parent_table="parent",
            )
        ],
    )
    assert _timescale_purge_relations(conn, inventory) == [
        "internal.compressed",
        "internal.ordinary",
        "internal.parent",
    ]
    assert "compressed.id = chunk.compressed_chunk_id" in conn.queries[-1]
    assert _timescale_purge_relations(Catalog({}, [], present=False), inventory) == []
    assert _timescale_purge_relations(
        Catalog(
            current,
            [
                row(
                    "trace",
                    compressed_reference=None,
                    compressed_schema=None,
                    compressed_table=None,
                )
            ],
        ),
        inventory,
    ) == ["internal.ordinary"]
    for columns in (
        {},
        {**current, "compression_settings": {"relid"}},
        {**legacy, "chunk": legacy["chunk"] - {"compressed_chunk_id"}},
    ):
        with pytest.raises(Exception, match="unsupported Timescale"):
            _timescale_purge_relations(Catalog(columns, []), inventory)
    for prefix in ("chunk", "compressed", "compressed_parent"):
        broken = row(
            "trace",
            **{
                prefix + "_reference": 9,
                prefix + "_schema": None,
                prefix + "_table": None,
            },
        )
        with pytest.raises(Exception, match="unresolved Timescale relation"):
            _timescale_purge_relations(Catalog(current, [broken]), inventory)


def test_run_fixture_cleans_failed_initialization_only_for_its_created_role(
    monkeypatch,
):
    from tests.substrate.external_actions import test_run_records_postgres as proof

    class Admin:
        def __init__(self):
            self.oid = None
            self.collision = False
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, parameters=None):
            query = query.as_string() if hasattr(query, "as_string") else query
            self.statements.append(query)
            if query.startswith("CREATE ROLE"):
                if self.collision:
                    raise RuntimeError("existing role")
                self.oid = 123
            elif query.startswith("DROP ROLE"):
                self.oid = None
            return self

        def fetchone(self):
            return None if self.oid is None else (self.oid,)

    admin = Admin()
    monkeypatch.setattr(proof.psycopg, "connect", lambda *a, **k: admin)
    monkeypatch.setattr(proof, "_bind_verifier_role", lambda *a, **k: None)

    def failed_policy(self):
        raise RuntimeError("policy activation failed")

    monkeypatch.setattr(proof.RunFixture, "policy", failed_policy)
    with pytest.raises(RuntimeError, match="policy activation failed"):
        proof.RunFixture("dbname=unused", "generation")
    assert admin.oid is None
    assert sum(query.startswith("DROP ROLE") for query in admin.statements) == 1
    admin.oid, admin.collision = 456, True
    admin.statements.clear()
    with pytest.raises(RuntimeError, match="existing role"):
        proof.RunFixture("dbname=unused", "generation")
    assert admin.oid == 456 and not any("DROP" in query for query in admin.statements)
    fixture = proof.RunFixture.__new__(proof.RunFixture)
    fixture.dsn, fixture.role, fixture.role_oid = "dbname=unused", "owned", 123
    with pytest.raises(AssertionError, match="owned role identity changed"):
        fixture.close()
    assert admin.oid == 456 and not any("DROP" in query for query in admin.statements)

from __future__ import annotations

import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from okg.substrate.admission import FactRangeRef
from okg.substrate.fact_batch import (
    ApplicationReceipt,
    ApplicationReceiptConflict,
    ApplicationReceiptTransitionError,
    FactBatchAuthorityError,
    FactBatchFieldError,
    FactBatchHeader,
    FactBatchIntegrityError,
    FactBatchSummary,
    FactBatchValidator,
    FactBatchVersionError,
    FactChunk,
    MemoryApplicationReceiptStore,
    recover_prepared_receipt,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    RecordNode,
)
from okg.substrate.producer_authority import ProducerProfile


ROOT = Path(__file__).resolve().parents[3]


def test_header_round_trip_reuses_run_admission_and_idempotency() -> None:
    header = _header()

    restored = FactBatchHeader.from_dict(header.to_dict())

    assert restored == header
    assert restored.run_id == header.run_id
    assert restored.admission_id == header.admission_id
    assert restored.idempotency_key == "admission-replay-key"


def test_header_rejects_unknown_version_and_control_fields() -> None:
    payload = _header().to_dict()
    payload["schema_version"] = 99
    with pytest.raises(FactBatchVersionError, match="unsupported"):
        FactBatchHeader.from_dict(payload)

    payload = _header().to_dict()
    payload["credential"] = "should-never-enter-a-batch"
    with pytest.raises(FactBatchFieldError, match="unknown control fields"):
        FactBatchHeader.from_dict(payload)

    payload = _header().to_dict()
    payload["requested_scope"] = {"access_token": "secret"}
    with pytest.raises(FactBatchFieldError, match="forbidden"):
        FactBatchHeader.from_dict(payload)


def test_header_rejects_clients_callbacks_and_non_json_values() -> None:
    with pytest.raises(FactBatchFieldError, match="callback/client"):
        _header(requested_scope={"repo": lambda: "okg"})

    with pytest.raises(FactBatchFieldError, match="non-serializable"):
        _header(requested_scope={"repo": object()})


def test_header_authority_accepts_nested_structured_scope_lists() -> None:
    profile = _profile()
    profile = replace(
        profile,
        authority_scope={
            "graph_profiles": [
                {"graph_id": "one", "modes": ["fast_track", "review"]}
            ]
        },
    )
    header = _header(
        requested_scope={
            "graph_profiles": [{"graph_id": "one", "modes": ["review"]}]
        }
    )

    FactBatchValidator(header, profile=profile)


def test_chunk_round_trip_and_digest_cover_canonical_values() -> None:
    chunk = _chunk()

    restored = FactChunk.from_dict(chunk.to_dict())

    assert restored == chunk
    assert restored.digest.startswith("sha256:")
    assert restored.facts[0].attrs["title"] == "Canonical"
    with pytest.raises(TypeError):
        restored.facts[0].attrs["title"] = "mutated"


def test_chunk_round_trip_preserves_explicit_substitute_ownership() -> None:
    fact = NodeFact(
        node_id="document:substitute",
        subtype="document",
        attrs={"title": "Substitute"},
        source_record_id={"record_id": "substitute"},
        source_revision={"revision": "1"},
        ownership_mode="substitute",
    )
    chunk = FactChunk.create(
        run_id=_header().run_id,
        ordinal=0,
        facts=[fact],
    )

    payload = chunk.to_dict()
    assert payload["facts"][0]["ownership_mode"] == "substitute"
    restored = FactChunk.from_dict(payload)
    assert restored.facts[0].ownership_mode == "substitute"

    # Existing serialized batches omitted the field; omission has one strict
    # meaning and cannot accidentally opt a node into substitute ownership.
    legacy_payload = _chunk().to_dict()
    assert "ownership_mode" not in legacy_payload["facts"][0]
    restored_default = FactChunk.from_dict(legacy_payload)
    assert restored_default.facts[0].ownership_mode == "authoritative"

    with pytest.raises(ValueError, match="ownership_mode"):
        NodeFact(
            node_id="document:invalid",
            subtype="document",
            attrs={},
            source_record_id={},
            source_revision={},
            ownership_mode="",
        )

    invalid_cursor = {
        "node_id": "document:invalid-cursor",
        "subtype": "document",
        "source_record_id": {},
        "source_revision": {},
        "ownership_mode": None,
    }
    with pytest.raises(ValueError, match="ownership_mode"):
        RecordNode.from_cursor(invalid_cursor)


def test_chunk_rejects_digest_tampering_and_ordinal_errors() -> None:
    chunk = _chunk()
    with pytest.raises(FactBatchIntegrityError, match="digest mismatch"):
        FactChunk(
            run_id=chunk.run_id,
            ordinal=chunk.ordinal,
            facts=chunk.facts,
            progress_markers=chunk.progress_markers,
            digest="sha256:" + "0" * 64,
        )

    with pytest.raises(FactBatchFieldError, match="ordinal"):
        FactChunk.create(run_id=chunk.run_id, ordinal=-1)


def test_stream_validator_uses_constant_state_and_checks_digest_chain() -> None:
    header = _header()
    validator = FactBatchValidator(header, profile=_profile())
    store = MemoryApplicationReceiptStore()
    receipts: list[ApplicationReceipt] = []

    for ordinal in range(3):
        chunk = _chunk(ordinal=ordinal, title=f"Document {ordinal}")
        validator.accept(chunk)
        prepared = store.prepare(ApplicationReceipt.prepared(header, chunk))
        receipts.append(
            store.transition(
                prepared.key,
                "applied",
                graph_result={"nodes_upserted": 1},
                branch_token=f"branch-revision-{ordinal}",
            )
        )

    summary = FactBatchSummary.from_receipts(
        run_id=header.run_id,
        receipts=receipts,
        observed_scope={"repo": "okg"},
        completion="delta",
    )

    assert validator.finish(summary) == summary
    assert FactBatchSummary.from_dict(summary.to_dict()) == summary
    assert not any(
        name in validator.__dict__ for name in ("chunks", "facts", "receipts")
    )


def test_stream_validator_rejects_gap_wrong_run_and_summary_tampering() -> None:
    header = _header()
    validator = FactBatchValidator(header, profile=_profile())
    with pytest.raises(FactBatchIntegrityError, match="expected chunk ordinal"):
        validator.accept(_chunk(ordinal=1))

    wrong_run = FactChunk.create(run_id=uuid.uuid4(), ordinal=0)
    with pytest.raises(FactBatchIntegrityError, match="run_id"):
        validator.accept(wrong_run)

    validator.accept(_chunk())
    receipt = ApplicationReceipt.prepared(header, _chunk()).transition(
        "applied", graph_result={"nodes_upserted": 1}
    )
    summary = FactBatchSummary.from_receipts(
        run_id=header.run_id,
        receipts=[receipt],
        observed_scope=None,
        completion="delta",
    )
    tampered = FactBatchSummary(
        run_id=summary.run_id,
        terminal_chunk_count=summary.terminal_chunk_count,
        digest_chain="sha256:" + "0" * 64,
        application_receipts=summary.application_receipts,
        observed_scope=summary.observed_scope,
        completion=summary.completion,
    )
    with pytest.raises(FactBatchIntegrityError, match="digest chain"):
        validator.finish(tampered)


def test_zero_chunk_batch_has_a_valid_terminal_summary() -> None:
    header = _header()
    validator = FactBatchValidator(header, profile=_profile())
    summary = FactBatchSummary.from_receipts(
        run_id=header.run_id,
        receipts=(),
        observed_scope={"repo": "okg"},
        completion="delta",
        source_delta={"changed_record_ids": []},
    )

    assert validator.finish(summary).terminal_chunk_count == 0


def test_summary_rejects_secret_bearing_source_evidence() -> None:
    with pytest.raises(FactBatchFieldError, match="forbidden"):
        FactBatchSummary.from_receipts(
            run_id=_header().run_id,
            receipts=(),
            observed_scope=None,
            completion="partial",
            source_delta={"refresh_token": "secret"},
        )


def test_receipt_replay_is_idempotent_and_digest_mismatch_conflicts() -> None:
    header = _header()
    store = MemoryApplicationReceiptStore()
    first = ApplicationReceipt.prepared(header, _chunk())

    assert store.prepare(first) == first
    assert store.prepare(first) == first

    changed = ApplicationReceipt.prepared(
        header,
        _chunk(title="different canonical content"),
    )
    with pytest.raises(ApplicationReceiptConflict, match="immutable"):
        store.prepare(changed)


def test_receipt_state_machine_is_explicit_and_monotonic() -> None:
    header = _header()
    receipt = ApplicationReceipt.prepared(header, _chunk())
    applied = receipt.transition(
        "applied",
        graph_result={"nodes_upserted": 1},
        branch_token="revision-1",
        audit_fact_ranges=[_fact_range()],
    )
    compensating = applied.transition("compensating")
    compensated = compensating.transition("compensated")

    assert compensated.state == "compensated"
    assert applied.audit_fact_ranges == (_fact_range(),)
    assert applied.transition("applied") == applied
    with pytest.raises(ApplicationReceiptTransitionError, match="invalid"):
        receipt.transition("compensated")
    with pytest.raises(ApplicationReceiptConflict, match="changed"):
        applied.transition("applied", graph_result={"nodes_upserted": 2})


def test_prepared_receipt_recovery_reapplies_once_and_finalizes() -> None:
    header = _header()
    chunk = _chunk()
    store = MemoryApplicationReceiptStore()
    prepared = store.prepare(ApplicationReceipt.prepared(header, chunk))
    calls: list[str] = []

    applied = recover_prepared_receipt(
        store=store,
        receipt=prepared,
        chunk=chunk,
        apply_chunk=lambda value: calls.append(value.digest) or {"nodes_upserted": 1},
        branch_token="revision-1",
    )
    replayed = recover_prepared_receipt(
        store=store,
        receipt=applied,
        chunk=chunk,
        apply_chunk=lambda value: calls.append("unexpected") or {},
    )

    assert applied.state == "applied"
    assert replayed == applied
    assert calls == [chunk.digest]


def test_profile_owns_scope_catalog_and_retraction_authority() -> None:
    header = _header(requested_scope={"repo": "okg", "paths": ["docs"]})
    validator = FactBatchValidator(header, profile=_profile())
    validator.accept(_chunk())

    overbroad = _header(requested_scope={"repo": "other"})
    with pytest.raises(FactBatchAuthorityError, match="scope"):
        FactBatchValidator(overbroad, profile=_profile())

    wrong_catalog = _header(catalog_version_id=8)
    with pytest.raises(FactBatchAuthorityError, match="catalog"):
        FactBatchValidator(wrong_catalog, profile=_profile())

    retract = FactChunk.create(
        run_id=header.run_id,
        ordinal=0,
        facts=[_node(op="R")],
    )
    with pytest.raises(FactBatchAuthorityError, match="retraction"):
        FactBatchValidator(header, profile=_profile()).accept(retract)

    permitted = FactBatchValidator(
        header,
        profile=_profile(),
        retractions_allowed=True,
    )
    permitted.accept(retract)


def test_profile_fingerprint_is_server_resolved_not_batch_granted() -> None:
    header = _header(producer_profile_fingerprint="sha256:" + "f" * 64)
    with pytest.raises(FactBatchAuthorityError, match="fingerprint"):
        FactBatchValidator(header, profile=_profile())


def test_serialized_chunk_rejects_unknown_client_or_publication_fields() -> None:
    payload = _chunk().to_dict()
    payload["client"] = {"type": "provider"}
    with pytest.raises(FactBatchFieldError, match="unknown control fields"):
        FactChunk.from_dict(payload)

    payload = _chunk().to_dict()
    payload["publication_handle"] = "generation:1"
    with pytest.raises(FactBatchFieldError, match="unknown control fields"):
        FactChunk.from_dict(payload)


def test_chunk_rejects_secret_bearing_fact_payloads() -> None:
    secret_fact = NodeFact(
        node_id="document:secret",
        subtype="document",
        attrs={"access_token": "must-not-be-persisted"},
        source_record_id={"record_id": "secret"},
        source_revision={"revision": "1"},
    )

    with pytest.raises(FactBatchFieldError, match="forbidden"):
        FactChunk.create(run_id=_header().run_id, ordinal=0, facts=[secret_fact])

    # The scrub is recursive: a secret nested at any depth (and in
    # source_record_id / source_revision) is still rejected fail-closed.
    for attrs, record_id, revision in (
        ({"provider": {"credentials": {"password": "hunter2"}}}, {}, {}),
        ({}, {"auth": {"refresh_token": "leak"}}, {}),
        ({}, {}, {"env": {"api_key": "leak"}}),
    ):
        nested_secret = NodeFact(
            node_id="document:secret-nested",
            subtype="document",
            attrs=attrs,
            source_record_id={"record_id": "s", **record_id},
            source_revision={"revision": "1", **revision},
        )
        with pytest.raises(FactBatchFieldError, match="forbidden"):
            FactChunk.create(
                run_id=_header().run_id, ordinal=0, facts=[nested_secret]
            )


def test_chunk_accepts_control_plane_names_in_fact_data() -> None:
    # #822: `command`/`commands`/`client`/`clients` are legitimate graph DATA
    # (a node recording the CI/agent/verification command it observed, or an
    # API client name). Their self-granted-authority variants are rejected at
    # the ENVELOPE (see test_serialized_chunk_rejects_unknown_client_or_
    # publication_fields), never inside opaque fact data -- even nested, and
    # even in source_record_id / source_revision. Before the fix these hard-
    # failed admission and blocked every publish for the deployment.
    node_fact = NodeFact(
        node_id="note:analysis-1",
        subtype="note",
        attrs={
            "client": "acme",
            "commands": ["build", "test"],
            "metadata": {"command": "uv run pytest -q tests/foo.py"},
        },
        source_record_id={"record_id": "1", "clients": ["a", "b"]},
        source_revision={"revision": "1", "command": "git rev-parse HEAD"},
    )
    # Nodes and edges share the same fact-data scanner; the edge path must
    # admit the structural names too.
    edge_fact = EdgeFact(
        src="note:analysis-1",
        dst="note:analysis-2",
        edge_type="relates_to",
        attrs={"client": "acme", "metadata": {"command": "git status"}},
        source_record_id={"record_id": "1"},
        source_revision={"revision": "1"},
    )

    chunk = FactChunk.create(
        run_id=_header().run_id, ordinal=0, facts=[node_fact, edge_fact]
    )
    restored = FactChunk.from_dict(chunk.to_dict())

    node = restored.facts[0]
    assert node.attrs["client"] == "acme"
    assert node.attrs["metadata"]["command"] == "uv run pytest -q tests/foo.py"
    assert node.source_revision["command"] == "git rev-parse HEAD"
    edge = restored.facts[1]
    assert edge.attrs["metadata"]["command"] == "git status"


def test_control_surfaces_still_forbid_command_and_client() -> None:
    # The narrowing is scoped to persisted fact DATA. The header's
    # requested_scope and the summary's observed_scope / source_delta are
    # AUTHORITY surfaces -- "never self-granted batch fields" -- so a
    # control-plane name there is still rejected, alongside secrets.
    for scope in ({"command": "rm -rf"}, {"clients": ["evil"]}):
        payload = _header().to_dict()
        payload["requested_scope"] = scope
        with pytest.raises(FactBatchFieldError, match="forbidden"):
            FactBatchHeader.from_dict(payload)


def test_schema_defines_durable_receipts_and_recovery_index() -> None:
    schema = (ROOT / "src/okg/substrate/db/schema.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS okg.fact_batch_receipts" in schema
    assert "PRIMARY KEY (admission_id, run_id, chunk_ordinal)" in schema
    assert "canonical_digest" in schema
    assert "fact_batch_receipts_recovery_idx" in schema
    assert "okg.fact_batch_receipts," in schema


def _header(**overrides: object) -> FactBatchHeader:
    values: dict[str, object] = {
        "schema_version": 1,
        "run_id": uuid.UUID("10000000-0000-0000-0000-000000000001"),
        "admission_id": uuid.UUID("20000000-0000-0000-0000-000000000002"),
        "producer_profile_fingerprint": "sha256:" + "a" * 64,
        "deployment": "test",
        "target_branch": "default",
        "catalog_version_id": 7,
        "intent": "source_sync",
        "idempotency_key": "admission-replay-key",
        "requested_scope": {"repo": "okg"},
    }
    values.update(overrides)
    return FactBatchHeader(**values)


def _chunk(*, ordinal: int = 0, title: str = "Canonical") -> FactChunk:
    return FactChunk.create(
        run_id=_header().run_id,
        ordinal=ordinal,
        facts=[_node(title=title)],
        progress_markers=[ProgressMarker("record-1", "fingerprint-1")],
    )


def _node(*, title: str = "Canonical", op: str = "I") -> NodeFact:
    return NodeFact(
        node_id="document:1",
        subtype="document",
        attrs={"title": title},
        source_record_id={"record_id": "1"},
        source_revision={"revision": "1"},
        op=op,
    )


def _profile() -> ProducerProfile:
    return ProducerProfile(
        deployment_name="test",
        producer_id="source:test",
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={"repo": "okg", "paths": ["docs", "src"]},
        allowed_record_kinds=("node",),
        output_signature={"nodes": [{"subtype": "document"}], "edges": []},
        policy_clauses=(),
        output_scope_summary={},
        source_ref={"source": "test"},
        catalog_version_id=7,
        profile_fingerprint="sha256:" + "a" * 64,
        status="active",
    )


def _fact_range() -> FactRangeRef:
    return FactRangeRef(
        ref_kind="node_fact_range",
        ref_key="node:test:1:1",
        source="source:test",
        run_id=str(_header().run_id),
        fact_id_min=1,
        fact_id_max=1,
        record_count=1,
    )

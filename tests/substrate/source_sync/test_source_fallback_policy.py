from __future__ import annotations

import pytest

from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.source_sync_contract import (
    SourceDeltaBatch,
    SourceFallbackEvidence,
    SourceFallbackPolicy,
    SourceSyncContractError,
    scope_or_mode_widened,
    validate_source_delta_batch,
)
from okg.substrate.sources.registry import load_source_registry


def _node() -> NodeFact:
    return NodeFact(
        node_id="doc:a",
        subtype="document",
        attrs={"text": "A"},
        source_record_id={"path": "docs/a.md"},
        source_revision={"sha": "abc"},
    )


def test_registry_sync_fields_derive_fallback_policy(tmp_path) -> None:
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text(
        """
sources:
  doc-corpus:
    ownership_id: test.doc-corpus
    sync:
      triggers: [fs_event, manual, reconcile]
      default_event_mode: scope_incomplete
      reconcile_mode: scope_complete
      deletes_require_reconcile: true
      scope:
        accepts: [path]
        unsupported_fallback: scope_complete
""",
        encoding="utf-8",
    )

    registry = load_source_registry(registry_path)
    policy = SourceFallbackPolicy.from_sync(registry["doc-corpus"].sync)

    assert policy.supports_scoped_change is True
    assert policy.scope_kinds == ("path",)
    assert policy.default_event_mode == "scope_incomplete"
    assert policy.reconcile_mode == "scope_complete"
    assert policy.unsupported_scope_fallback == "scope_complete"
    assert policy.delete_fallback == "scope_complete"


def test_fallback_evidence_represents_requested_and_effective_scope() -> None:
    fallback = SourceFallbackEvidence(
        reason="adapter cannot prove scoped delete completeness",
        requested_mode="scope_incomplete",
        effective_mode="scope_complete",
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md", "docs/b.md"]},
        measured_input_size=2,
        affected_scopes=("docs/a.md", "docs/b.md"),
        readiness_impact="work widened to scope-complete scan",
        evidence_refs=("event:doc-corpus:1",),
    )
    run = SourceRun(
        facts=[_node()],
        next_cursor={"cursor": "4"},
        run_mode="scope_incomplete",
    )
    batch = SourceDeltaBatch.from_source_run(
        "doc-corpus",
        run,
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md", "docs/b.md"]},
        requested_mode="scope_incomplete",
        effective_mode="scope_complete",
        affected_scopes=("docs/a.md", "docs/b.md"),
        fallback=fallback,
        evidence_refs=("event:doc-corpus:1",),
    )

    assert scope_or_mode_widened(batch) is True
    validate_source_delta_batch(batch)
    assert batch.to_dict()["fallback"] == fallback.to_dict()


def test_validation_rejects_silent_widening() -> None:
    batch = SourceDeltaBatch(
        source_id="doc-corpus",
        next_frontier={"cursor": "4"},
        facts=(_node(),),
        changed_record_ids=('{"path":"docs/a.md"}',),
        affected_scopes=("docs/a.md", "docs/b.md"),
        requested_scope={"scope_keys": ["docs/a.md"]},
        effective_scope={"scope_keys": ["docs/a.md", "docs/b.md"]},
        requested_mode="scope_incomplete",
        effective_mode="scope_complete",
    )

    with pytest.raises(SourceSyncContractError, match="without fallback"):
        validate_source_delta_batch(batch)


def test_policy_requires_fallback_for_unsupported_scoped_request() -> None:
    batch = SourceDeltaBatch(
        source_id="compute-env-local",
        next_frontier={"host_fingerprint": "abc"},
        requested_scope={"scope_keys": ["host:localhost"]},
        effective_scope={"scope_keys": ["host:localhost"]},
        requested_mode="scope_incomplete",
        effective_mode="scope_incomplete",
    )

    with pytest.raises(SourceSyncContractError, match="does not support"):
        validate_source_delta_batch(
            batch,
            fallback_policy=SourceFallbackPolicy(),
        )

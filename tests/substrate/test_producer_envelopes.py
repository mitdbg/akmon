"""Producer write envelopes (PACT consistency-output-envelopes).

Named witness obligations from docs/develop/architecture/consistency.md §9.3
landing in this child's first wave:

* ``test_missing_declaration_fails_boot`` — a producer with no envelope
  declaration fails loudly at boot; observed history never substitutes.
* ``test_model_backed_producer_is_nondeterministic`` — a producer whose
  outputs depend on a model call carries the nondeterministic
  classification through producer authority; the unconditional
  derived-deterministic-trusted stamp is gone.

The remaining §9.3 witnesses (out-of-envelope poisons reuse and surfaces;
type-invalid output is an operator failure) land with
task.envelope.runtime-validator.
"""

from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.library.enrichers.extraction.claim_extractor import (
    ClaimExtractor,
)
from okg.substrate.producer_authority import (
    ENVELOPE_UNBOUNDED_DIMENSIONS,
    TRUST_LABEL_RANKS,
    WriteEnvelope,
    normalize_write_envelope,
)
from okg.substrate.publisher.enricher_loader import (
    EnricherEnvelopeMissing,
    derive_write_envelope,
    validate_enricher_envelopes,
)
from okg.substrate.publisher.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


class _FakeLLM:
    model = "fake-model-1"

    def complete(self, system: str, user: str, *, fragment_id: str = "",
                 caller: str = "", generation_id=None,
                 outcome_key_digest: str | None = None) -> str:
        return "[]"


class _UndeclaredEnricher:
    """No write_envelope, no emits_typed_subtype, no requires_narrowings."""

    name = "undeclared_enricher"

    def enrich(self, conn, *, generation_id, incremental=None):  # pragma: no cover
        raise NotImplementedError


class _DeterministicLinker:
    """A deterministic enricher shaped like the in-tree linkers."""

    name = "fixture_linker"
    requires_narrowings = (("git_commit", "references", "pact_change"),)

    def enrich(self, conn, *, generation_id, incremental=None):  # pragma: no cover
        raise NotImplementedError


class _UndeclaredNodeProvenanceEnricher:
    """Writes nodes but never says where they came from.

    PACT bind-derived-node-provenance. Its nodes would land with an empty
    lineage envelope: invisible to Nomos-enforced reads, quarantining for
    readiness, and rejected as an input by the trust check — all silent
    from the author's side.
    """

    name = "undeclared_node_provenance_enricher"
    emits_typed_subtype = "claim"
    requires_narrowings = (("paper", "states", "claim"),)

    def enrich(self, conn, *, generation_id, incremental=None):  # pragma: no cover
        raise NotImplementedError


class _DeclaredNodeProvenanceEnricher(_UndeclaredNodeProvenanceEnricher):
    """The same shape, with the provenance declaration."""

    name = "declared_node_provenance_enricher"
    declares_node_provenance = True


# --------------------------------------------------------------------------- #
# §9.3 named witness: missing declaration fails boot                           #
# --------------------------------------------------------------------------- #

def test_node_writer_without_provenance_declaration_fails_boot() -> None:
    """A node-emitting enricher must declare that it supplies provenance."""
    with pytest.raises(EnricherEnvelopeMissing) as excinfo:
        validate_enricher_envelopes([_UndeclaredNodeProvenanceEnricher()])
    message = str(excinfo.value)
    assert "undeclared_node_provenance_enricher" in message
    assert "node provenance" in message
    assert "derived_from" in (excinfo.value.fix_hint or "")


def test_node_writer_with_provenance_declaration_boots() -> None:
    envelopes = validate_enricher_envelopes([
        _DeclaredNodeProvenanceEnricher(),
    ])
    envelope = envelopes["enricher:declared_node_provenance_enricher"]
    assert [n["subtype"] for n in envelope.signature["nodes"]] == ["claim"]


def test_edge_only_enricher_needs_no_provenance_declaration() -> None:
    """The rule is scoped to node writers.

    An edge inherits provenance from its two structural endpoints, so an
    edge-only enricher has nothing to declare and must not be refused.
    """
    envelopes = validate_enricher_envelopes([_DeterministicLinker()])
    assert "enricher:fixture_linker" in envelopes


def test_extractor_envelope_covers_the_dropped_candidate_node() -> None:
    """Every LLM extractor writes `extraction_dropped`; it must be declared.

    `LLMExtractor._emit_dropped` writes one such node per below-floor
    candidate, for every subclass. It sat outside every extractor's declared
    write envelope — invisible while node emission was ungoverned, and a
    WHOLE-RUN rejection once admission checks node subtypes, because a fact
    range is grouped per (source, run_id) and one undeclared subtype fails
    the range, taking the typed nodes down with it.
    """
    from okg.substrate.library.enrichers.extraction.claim_extractor import (
        ClaimExtractor,
    )

    envelope = derive_write_envelope(
        ClaimExtractor, producer_id="enricher:claim_extractor",
    )
    subtypes = {node["subtype"] for node in envelope.signature["nodes"]}
    assert "claim" in subtypes
    assert "extraction_dropped" in subtypes, (
        "the dropped-candidate node is undeclared; a cycle that drops one "
        "candidate would have its entire node output rejected"
    )
    assert "node" in envelope.signature.get("allowed_record_kinds", ["node"])


def test_shipped_node_emitting_extractors_declare_provenance() -> None:
    """The migration actually happened for every shipped node emitter."""
    from okg.substrate.library.enrichers.extraction.claim_extractor import (
        ClaimExtractor,
    )
    from okg.substrate.library.enrichers.extraction.concept_extractor import (
        ConceptExtractor,
    )
    from okg.substrate.library.enrichers.extraction.evidence_extractor import (
        EvidenceExtractor,
    )
    from okg.substrate.library.enrichers.extraction.hypothesis_extractor import (
        HypothesisExtractor,
    )

    for cls in (
        ClaimExtractor, ConceptExtractor, EvidenceExtractor, HypothesisExtractor,
    ):
        assert getattr(cls, "declares_node_provenance", False) is True, (
            f"{cls.__name__} emits nodes but does not declare provenance"
        )


def test_missing_declaration_fails_boot() -> None:
    with pytest.raises(EnricherEnvelopeMissing) as excinfo:
        validate_enricher_envelopes([_UndeclaredEnricher()])
    message = str(excinfo.value)
    assert "undeclared_enricher" in message
    assert "write envelope" in message
    # The fix hint names the declaration surfaces, including the unbounded
    # marker for genuinely free-form operators — declaration, not inference.
    assert "unbounded" in (excinfo.value.fix_hint or "")


def test_derived_envelope_from_declarations() -> None:
    envelopes = validate_enricher_envelopes([_DeterministicLinker()])
    envelope = envelopes["enricher:fixture_linker"]
    assert isinstance(envelope, WriteEnvelope)
    assert envelope.nondeterministic is False
    assert envelope.unbounded == ()
    triples = [
        (e["src_subtype"], e["edge_type"], e["dst_subtype"])
        for e in envelope.signature["edges"]
    ]
    assert triples == [("git_commit", "references", "pact_change")]


def test_explicit_envelope_wins_over_derivation() -> None:
    class _Explicit(_DeterministicLinker):
        name = "explicit_linker"
        write_envelope = {
            "edges": [{
                "src_subtype": "a", "edge_type": "rel", "dst_subtype": "b",
            }],
            "nondeterministic": False,
        }

    envelope = derive_write_envelope(
        _Explicit(), producer_id="enricher:explicit_linker"
    )
    triples = [
        (e["src_subtype"], e["edge_type"], e["dst_subtype"])
        for e in envelope.signature["edges"]
    ]
    assert triples == [("a", "rel", "b")]


def test_unbounded_marker_round_trips_and_refuses_unknown_dimensions() -> None:
    envelope = normalize_write_envelope(
        {
            "edges": [{
                "src_subtype": "a", "edge_type": "rel", "dst_subtype": "b",
            }],
            "unbounded": ["nodes", "values"],
            "nondeterministic": True,
        },
        source_name="producer:fixture",
    )
    assert envelope.unbounded == ("nodes", "values")
    assert envelope.summary()["unbounded"] == ["nodes", "values"]
    assert set(envelope.summary()["unbounded"]) <= ENVELOPE_UNBOUNDED_DIMENSIONS

    with pytest.raises(ValueError) as excinfo:
        normalize_write_envelope(
            {"edges": [], "unbounded": ["everything"]},
            source_name="producer:fixture",
        )
    assert "unknown dimensions" in str(excinfo.value)

    with pytest.raises(ValueError):
        normalize_write_envelope(
            {"edges": [], "surprise_key": True}, source_name="producer:fixture"
        )


# --------------------------------------------------------------------------- #
# §9.3 named witness: honest nondeterministic classification                   #
# --------------------------------------------------------------------------- #

def _synthesized_policy(enricher: Any) -> dict:
    name = getattr(enricher, "name", type(enricher).__name__)
    # _enricher_policy reads only its arguments (no publisher state); invoke
    # it unbound so the witness needs no publisher fixture.
    return Publisher._enricher_policy(
        object(), enricher, producer_id=f"enricher:{name}"
    )


def test_unbounded_node_surface_permits_the_node_record_kind() -> None:
    """A structural enricher writes nodes it cannot enumerate.

    Its subtypes come from the parsed file, so the node dimension is
    declared UNBOUNDED and contributes no signature clauses. Record kinds
    must still include "node" — otherwise the producer is rejected as
    producer_record_kind_not_allowed against its OWN declared envelope, and
    every run's node output is discarded.
    """
    from okg.substrate.library.enrichers.scholarly.latex_structural import (
        LatexStructuralEnricher,
    )

    policy = _synthesized_policy(LatexStructuralEnricher())
    assert "node" in policy["allowed_record_kinds"]
    assert "edge" in policy["allowed_record_kinds"]


def test_model_backed_producer_is_nondeterministic() -> None:
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=_FakeLLM(), input_quarantine_max_attempts=3
    )
    policy = _synthesized_policy(extractor)
    assert policy["trust_label"] == "model-derived-trusted"
    assert "model-derived-trusted" in TRUST_LABEL_RANKS
    # The write surface is complete: the emitted typed node subtype rides
    # the signature next to the narrowing triples, and the closed claim-kind
    # vocabulary is declared, not parse-incidental.
    assert set(policy["allowed_record_kinds"]) == {"node", "edge"}
    node_subtypes = [n["subtype"] for n in policy["output_signature"]["nodes"]]
    # `extraction_dropped` joined the declared surface (PACT
    # admit-derived-node-emission): the base class writes it for every
    # below-floor candidate, so the previous single-subtype expectation was
    # pinning an INCOMPLETE envelope. That was invisible while node emission
    # was ungoverned; once admission checks node subtypes it would have
    # rejected the whole run.
    assert node_subtypes == ["claim", "extraction_dropped"]
    summary = policy["output_scope_summary"]["write_envelope"]
    assert summary["nondeterministic"] is True
    assert summary["value_vocabularies"]["kind"]
    # Signature clause ids and policy clauses stay consistent.
    clause_ids = {
        c["id"] for c in policy["policy_clauses"]
    }
    signature_clauses = {
        entry["clause_id"]
        for entry in (
            *policy["output_signature"]["nodes"],
            *policy["output_signature"]["edges"],
        )
    }
    assert signature_clauses == clause_ids


def test_deterministic_enricher_keeps_deterministic_label() -> None:
    policy = _synthesized_policy(_DeterministicLinker())
    assert policy["trust_label"] == "derived-deterministic-trusted"
    assert policy["output_scope_summary"]["write_envelope"][
        "nondeterministic"] is False


def test_deterministic_claim_by_model_calling_operator_is_lint_error() -> None:
    class _Dishonest(ClaimExtractor):
        name = "dishonest_extractor"
        nondeterministic = False

    with pytest.raises(EnricherEnvelopeMissing) as excinfo:
        derive_write_envelope(
            _Dishonest(
                confidence_floor=0.6, llm=_FakeLLM(), input_quarantine_max_attempts=3
            ),
            producer_id="enricher:dishonest_extractor",
        )
    assert "nondeterministic=False" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# Runtime validation (task.envelope.runtime-validator)                         #
# --------------------------------------------------------------------------- #

def _profile(*, signature: dict, scope_summary: dict,
             kinds: tuple[str, ...] = ("node", "edge")):
    from okg.substrate.producer_authority import ProducerProfile

    return ProducerProfile(
        deployment_name="default",
        producer_id="_enricher:fixture",
        producer_kind="enricher",
        trust_label="model-derived-trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=kinds,
        output_signature=signature,
        policy_clauses=(),
        output_scope_summary=scope_summary,
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="fixture-fingerprint",
        status="active",
    )


def test_evaluate_records_rejects_out_of_vocabulary_value() -> None:
    """A well-typed record whose declared-vocabulary field carries a value
    outside the closed list is an envelope violation — rejected with the
    envelope-class code, never widened into the vocabulary."""
    from okg.substrate.producer_authority import (
        ENVELOPE_VIOLATION_CODES,
        ProducerPolicyViolation,
        _evaluate_records,
    )

    profile = _profile(
        signature={"nodes": [{"subtype": "claim"}]},
        scope_summary={"write_envelope": {
            "nodes": [{"subtype": "claim"}],
            "value_vocabularies": {"kind": ["definitional", "empirical"]},
            "unbounded": [],
            "nondeterministic": True,
        }},
    )
    ok_record = {"fact_id": 1, "node_id": "claim:1", "subtype": "claim",
                 "attrs": {"kind": "empirical"}, "op": "I"}
    bad_record = {"fact_id": 2, "node_id": "claim:2", "subtype": "claim",
                  "attrs": {"kind": "vibes"}, "op": "I"}

    _evaluate_records(profile=profile, node_records=[ok_record],
                      edge_records=[], endpoint_scope="the records supplied to this evaluation")
    with pytest.raises(ProducerPolicyViolation) as excinfo:
        _evaluate_records(profile=profile, node_records=[bad_record],
                          edge_records=[], endpoint_scope="the records supplied to this evaluation")
    assert excinfo.value.code == "envelope_vocabulary_violation"
    assert excinfo.value.code in ENVELOPE_VIOLATION_CODES


def test_unbounded_dimension_skips_producer_signature_matching() -> None:
    """An unbounded dimension bounds the PRODUCER's declaration, not
    admission: records on it pass producer-signature matching (catalog law
    still applies downstream), while bounded dimensions keep rejecting."""
    from okg.substrate.producer_authority import (
        ProducerPolicyViolation,
        _evaluate_records,
    )

    profile = _profile(
        signature={"edges": [{"src_subtype": "a", "edge_type": "rel",
                              "dst_subtype": "b"}]},
        scope_summary={"write_envelope": {
            "edges": [{"src_subtype": "a", "edge_type": "rel",
                       "dst_subtype": "b"}],
            "unbounded": ["nodes"],
            "nondeterministic": True,
        }},
    )
    surprise_node = {"fact_id": 1, "node_id": "x:1", "subtype": "surprise",
                     "attrs": {}, "op": "I"}
    _evaluate_records(profile=profile, node_records=[surprise_node],
                      edge_records=[], endpoint_scope="the records supplied to this evaluation")

    rogue_edge = {"fact_id": 2, "src": "a:1", "src_subtype": "a",
                  "edge_type": "other", "dst": "b:1", "dst_subtype": "b",
                  "attrs": {}, "op": "I"}
    with pytest.raises(ProducerPolicyViolation):
        _evaluate_records(profile=profile, node_records=[],
                          edge_records=[rogue_edge],
                          endpoint_scope="the records supplied to this evaluation")


@pytest.mark.postgres
def test_out_of_envelope_output_poisons_reuse_and_surfaces(
    clean_runtime: str,
) -> None:
    """§9.3 named witness, end to end through a real publish cycle: an
    enricher emits a well-typed edge whose triple is catalog-legal but
    OUTSIDE its declared envelope. Admission rejects it with the
    envelope-class code, a poisoning violation row persists (queryable per
    deployment/generation, poisons_reuse TRUE — the substrate certificate
    validation must consult), the cycle outcome surfaces it, and the
    envelope is not widened (the profile still declares only the original
    triple)."""
    import datetime as _dt
    import uuid as _uuid

    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.producer_authority import (
        summarize_envelope_violations,
    )
    from tests.pytest.catalog_fixture_builders import seed_catalog

    dsn = clean_runtime

    class _RogueLinker:
        """Declares links_to; emits cites (catalog-legal, envelope-illegal)."""

        name = "rogue_linker"
        requires_narrowings = (("page", "links_to", "page"),)

        def enrich(self, conn, *, generation_id, incremental=None):
            from okg.substrate.enrichers.base import EnrichResult

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.edge_facts
                      (edge_id, src, dst, edge_type, attrs, provenance,
                       confidence, source, source_record_id, source_revision,
                       observed_at, dedupe_key, run_id, op)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s,
                            %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, 'I')
                    """,
                    (
                        _uuid.uuid4().int >> 80,
                        "page:a", "page:b", "cites", "{}",
                        "derived_deterministic", 1.0,
                        "_enricher:rogue_linker", "{}", "{}",
                        _dt.datetime.now(_dt.timezone.utc),
                        f"rogue-{_uuid.uuid4().hex}",
                        str(_uuid.uuid4()),
                    ),
                )
            return EnrichResult(enricher_name=self.name, n_edges_emitted=1)

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page"),
                        ("page", "cites", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("page:a", "page", {}, {"id": "page:a"}, {"v": 1}),
                NodeFact("page:b", "page", {}, {"id": "page:b"}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )

    from okg.substrate.publisher import Publisher

    out = Publisher(dsn, "default", enrichers=[_RogueLinker()], catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()

    surfaced = out.get("envelope_violations") or []
    assert surfaced, "cycle outcome must surface the envelope violation"
    entry = surfaced[0]
    assert entry["producer_id"] == "_enricher:rogue_linker"
    assert entry["code"] == "producer_edge_triple_not_allowed"
    assert entry["poisons_reuse"] is True

    with psycopg.connect(dsn) as conn:
        summary = summarize_envelope_violations(conn, deployment="default")
        assert summary["count"] >= 1
        assert summary["poisons_reuse"] is True
        producers = {row["producer_id"] for row in summary["by_producer"]}
        assert "_enricher:rogue_linker" in producers
        # Never widened: the persisted profile still declares only the
        # original triple.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT output_signature FROM okg.producer_profiles
                 WHERE deployment_name = %s AND producer_id = %s
                """,
                ("default", "_enricher:rogue_linker"),
            )
            row = cur.fetchone()
        assert row is not None
        declared_triples = {
            (e["src_subtype"], e["edge_type"], e["dst_subtype"])
            for e in (row[0].get("edges") or [])
        }
        assert declared_triples == {("page", "links_to", "page")}


def test_type_invalid_output_is_operator_failure() -> None:
    """§9.3 named witness: a response outside the declared response TYPE is
    the operator's failure — refused at the parse boundary with a loud
    warning and zero emissions. It is a DIFFERENT failure class from an
    envelope violation: nothing reaches admission, so nothing records a
    violation row and nothing can widen the envelope."""
    import logging

    from okg.substrate.library.enrichers.extraction.claim_extractor import (
        parse_llm_claims_response,
    )

    logger = logging.getLogger(
        "okg.substrate.library.enrichers.extraction.claim_extractor"
    )
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture(level=logging.WARNING)
    logger.addHandler(handler)
    try:
        assert parse_llm_claims_response("NOT JSON {{{") == []
        assert parse_llm_claims_response('{"an": "object"}') == []
    finally:
        logger.removeHandler(handler)
    assert any("did not parse as JSON" in r.getMessage() for r in records)
    assert any("not a JSON array" in r.getMessage() for r in records)


# --------------------------------------------------------------------------- #
# Source-adapter + agent envelope backfill (task.envelope.backfill)           #
# --------------------------------------------------------------------------- #

@pytest.mark.postgres
def test_source_write_envelope_round_trips_through_materialized_profile(
    clean_runtime: str,
) -> None:
    """A source admission_policy carrying an explicit write_envelope block
    normalizes through the real declaration surface, rides
    output_scope_summary onto the materialized producer profile, and is
    recovered by profile_write_envelope — ZERO evaluator changes."""
    import psycopg

    from okg.substrate.producer_authority import (
        load_profile_by_fingerprint,
        materialize_producer_profile,
        profile_write_envelope,
    )
    from okg.substrate.sources.registry import _admission_policy_from_dict

    policy = _admission_policy_from_dict(
        "security-analysis",
        {
            "trust_label": "external-source-trusted",
            "output_signature": {
                "nodes": [{"subtype": "finding",
                           "clause_id": "sa.node.finding"}],
            },
            "policy_clauses": [{
                "id": "sa.node.finding", "effect": "allow",
                "record_kind": "node", "subtype": "finding",
            }],
            "output_scope_summary": {"nodes": ["finding"]},
            "write_envelope": {
                "nodes": [{"subtype": "finding"}],
                "value_vocabularies": {"severity": ["high", "low"]},
                "claim_keys": ["cwe"],
                "nondeterministic": False,
            },
        },
        fallback_producer_id="security-analysis",
    )
    # The declared block normalizes onto the policy and rides its scope summary.
    assert policy.write_envelope is not None
    assert policy.output_scope_summary["write_envelope"] == policy.write_envelope

    dsn = clean_runtime
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_producer_profile(
            conn,
            deployment_name="default",
            producer_id=policy.producer_id,
            producer_kind=policy.producer_kind,
            trust_label=policy.trust_label,
            admission_mode=policy.admission_mode,
            authority_scope=policy.authority_scope,
            allowed_record_kinds=policy.allowed_record_kinds,
            output_signature=policy.output_signature,
            policy_clauses=policy.policy_clauses,
            output_scope_summary=policy.output_scope_summary,
            source_ref={"source_registry_name": "security-analysis"},
            catalog_version_id=None,
        )
        loaded = load_profile_by_fingerprint(
            conn,
            deployment_name="default",
            producer_id=policy.producer_id,
            profile_fingerprint=None,
        )
    recovered = profile_write_envelope(loaded)
    assert recovered is not None
    assert recovered["value_vocabularies"]["severity"] == ["high", "low"]
    assert recovered["claim_keys"] == ["cwe"]
    assert recovered["nondeterministic"] is False


def test_source_output_signature_only_derives_deterministic_envelope() -> None:
    """A source that declares only an output_signature (no explicit
    write_envelope) DERIVES its envelope from that declaration — the node
    subtypes and edge triples it already declares, deterministic (a source
    adapter is not a model call). Declaration-derived, never output-inferred."""
    from okg.substrate.sources.registry import _admission_policy_from_dict

    policy = _admission_policy_from_dict(
        "test-reports",
        {
            "trust_label": "external-source-trusted",
            "output_signature": {
                "nodes": [{"subtype": "test_case", "clause_id": "tr.n"}],
                "edges": [{
                    "src_subtype": "test_case", "edge_type": "in_suite",
                    "dst_subtype": "test_suite", "clause_id": "tr.e",
                }],
            },
            "policy_clauses": [
                {"id": "tr.n", "effect": "allow", "record_kind": "node",
                 "subtype": "test_case"},
                {"id": "tr.e", "effect": "allow", "record_kind": "edge",
                 "triple": ["test_case", "in_suite", "test_suite"]},
            ],
            "output_scope_summary": {
                "nodes": ["test_case"],
                "edges": ["test_case in_suite test_suite"],
            },
        },
        fallback_producer_id="test-reports",
    )
    env = policy.write_envelope
    assert env is not None
    assert env["nondeterministic"] is False
    assert env["unbounded"] == []
    assert [n["subtype"] for n in env["nodes"]] == ["test_case"]
    triples = [
        (e["src_subtype"], e["edge_type"], e["dst_subtype"])
        for e in env["edges"]
    ]
    assert triples == [("test_case", "in_suite", "test_suite")]
    # Rides the scope summary so the materialized profile carries it.
    assert policy.output_scope_summary["write_envelope"] == env


def test_agent_producer_is_declared_unbounded_and_admits_arbitrary_record(
) -> None:
    """The agent write-back is the one genuinely free-form producer.
    policy_from_authority stamps its envelope explicitly unbounded on every
    dimension and nondeterministic (the marker is USED, not dodged); the
    runtime evaluator then exempts the unbounded dimensions from
    producer-signature matching, so an arbitrary catalog-shaped record admits
    (catalog narrowing law still applies downstream)."""
    from okg.substrate.producer_authority import (
        ProducerProfile,
        _evaluate_records,
        policy_from_authority,
        profile_write_envelope,
    )

    payload = policy_from_authority(
        {"producer_kind": "agent"},
        producer_id="agent:mcp",
        producer_kind="agent",
    )
    env = payload["output_scope_summary"]["write_envelope"]
    assert set(env["unbounded"]) == set(ENVELOPE_UNBOUNDED_DIMENSIONS)
    assert env["nondeterministic"] is True

    # A profile carrying that envelope (with some active signature) admits a
    # record whose subtype/triple is NOT in the signature — nodes and edges
    # are unbounded.
    profile = ProducerProfile(
        deployment_name="default",
        producer_id="agent:mcp",
        producer_kind="agent",
        trust_label=payload["trust_label"],
        admission_mode="review",
        authority_scope={},
        allowed_record_kinds=("node", "edge"),
        output_signature={"nodes": [{"subtype": "claim"}]},
        policy_clauses=(),
        output_scope_summary=payload["output_scope_summary"],
        source_ref={},
        catalog_version_id=None,
        profile_fingerprint="agent-fixture",
        status="active",
    )
    recovered = profile_write_envelope(profile)
    assert recovered is not None
    assert set(recovered["unbounded"]) == set(ENVELOPE_UNBOUNDED_DIMENSIONS)

    arbitrary_node = {"fact_id": 1, "node_id": "anything:1",
                      "subtype": "totally_unrelated", "attrs": {}, "op": "I"}
    arbitrary_edge = {"fact_id": 2, "src": "a:1", "src_subtype": "a",
                      "edge_type": "whatever", "dst": "b:1", "dst_subtype": "b",
                      "attrs": {}, "op": "I"}
    result = _evaluate_records(
        profile=profile,
        node_records=[arbitrary_node],
        edge_records=[arbitrary_edge],
        endpoint_scope="the records supplied to this evaluation",
    )
    assert result["producer_policy"]["status"] == "matched"


def test_deployment_lint_reports_undeclared_source() -> None:
    """A source with no derivable write envelope is surfaced LOUDLY by the
    deployment lint as an advisory producer.envelope_missing finding — the gap
    is visible, never silently filled with an invented policy."""
    from pathlib import Path

    from okg.substrate.deployment_lint import lint_deployment

    repo_root = Path(__file__).resolve().parents[2]
    report = lint_deployment(
        repo_root / "deployments" / "synth-bench",
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg"},
    )
    envelope_missing = [
        f for f in report["findings"]
        if f["code"] == "producer.envelope_missing"
    ]
    assert envelope_missing, "an undeclared source must be surfaced"
    reasons = {f["details"]["reason"] for f in envelope_missing}
    assert "no_admission_policy" in reasons
    # Advisory tier: visible, but never blocks the deployment.
    assert all(f["severity"] == "info" for f in envelope_missing)
    assert report["ok"] is True

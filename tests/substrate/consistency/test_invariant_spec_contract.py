"""PACT ``concord-spec-views-contract``, req.svc.serialization — the spec
serialization contract witnesses.

Round-trip byte stability, fail-closed loading with named refusal codes,
every load-time check exhibited by a violating spec, the §10.5 defaults
materialization with explicit-0 clock sentinels, the certificate shape's
type-enforced join demotion, and invariant-digest invalidation keying.
"""
from __future__ import annotations

import copy

import pytest

from okg.substrate.consistency.invariant_spec import (
    CLASSIFICATION_INSTANCE,
    CLASSIFICATION_KIND_LEVEL,
    CLASSIFICATION_WHOLE_GRAPH,
    REFUSAL_BOUNDED_CLOCKLESS,
    REFUSAL_DANGLING_REFERENCE,
    REFUSAL_DERIVATION_CYCLE,
    REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY,
    REFUSAL_LAZY_INADMISSIBLE,
    REFUSAL_MISSING_FIELD,
    REFUSAL_QUERY_DIGEST_MISMATCH,
    REFUSAL_UNKNOWN_FIELD,
    SpecCertificate,
    SpecLoadRefused,
    WARNING_STRICT_OVER_ADVISORY,
    canonical_json,
    load_invariant_spec,
    spec_to_canonical_dict,
)
from okg.substrate.consistency.violation_identity import WitnessParserRegistry
from tests.substrate.consistency._invariant_spec_support import (
    annotation_reader_program,
    base_spec,
    build_registry,
    spec_with,
)


@pytest.fixture()
def registry_and_digests():
    return build_registry()


@pytest.fixture()
def parsers() -> WitnessParserRegistry:
    registry = WitnessParserRegistry()
    registry.register(
        "parser://section-header@v1", "regex",
        lambda text: tuple(
            line[len("SECTION: "):] for line in text.splitlines()
            if line.startswith("SECTION: ")
        ),
    )
    return registry


def _load(spec, registry, parsers):
    return load_invariant_spec(
        spec, program_resolver=registry.resolve, parser_registry=parsers,
    )


def _refusal_code(excinfo) -> str:
    return excinfo.value.data["code"]


# --------------------------------------------------------------------------- #
# Round trip.                                                                  #
# --------------------------------------------------------------------------- #
def test_spec_round_trips_byte_stably(registry_and_digests, parsers) -> None:
    registry, digests = registry_and_digests
    loaded = _load(base_spec(digests), registry, parsers)
    serialized = spec_to_canonical_dict(loaded)
    reloaded = _load(serialized, registry, parsers)
    assert canonical_json(spec_to_canonical_dict(reloaded)) == canonical_json(
        serialized
    )
    assert reloaded.spec_digest == loaded.spec_digest
    assert dict(reloaded.object_digests) == dict(loaded.object_digests)


def test_defaults_materialize_only_omitted_fields(
    registry_and_digests, parsers
) -> None:
    """The corpus with no authority gets the DEFAULT (advisory); bounded
    fields the author omitted come from defaults; an explicit value is
    never overwritten."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    # omit k: defaults fill 3; keep explicit time_bound_days 0 (author's 0).
    spec["invariants"][0]["policy"] = {"mode": "bounded",
                                      "time_bound_days": 0}
    loaded = _load(spec, registry, parsers)
    assert loaded.corpus("partner-wiki").authority == "advisory"
    policy = loaded.invariant("day-counts-match").policy
    assert policy.k == 3                 # filled from defaults
    assert policy.time_bound_days == 0   # the author's explicit 0 survives


def test_explicit_zero_clocks_are_values_and_both_zero_refuses(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    # passes-only bounded: k explicit, time bound explicitly 0.
    spec = base_spec(digests)
    spec["invariants"][0]["policy"] = {"mode": "bounded", "k": 2,
                                      "time_bound_days": 0}
    assert _load(spec, registry, parsers).invariant(
        "day-counts-match").policy.time_bound_days == 0
    # time-bound-only bounded.
    spec["invariants"][0]["policy"] = {"mode": "bounded", "k": 0,
                                      "time_bound_days": 14}
    assert _load(spec, registry, parsers).invariant(
        "day-counts-match").policy.k == 0
    # both zero: a bounded policy with no clock is unbounded — refused.
    spec["invariants"][0]["policy"] = {"mode": "bounded", "k": 0,
                                      "time_bound_days": 0}
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_BOUNDED_CLOCKLESS
    # the same rule guards the defaults block itself.
    spec = base_spec(digests)
    spec["defaults"]["bounded_k"] = 0
    spec["defaults"]["bounded_time_bound_days"] = 0
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_BOUNDED_CLOCKLESS


# --------------------------------------------------------------------------- #
# Fail-closed structure.                                                       #
# --------------------------------------------------------------------------- #
def test_unknown_field_refuses(registry_and_digests, parsers) -> None:
    registry, digests = registry_and_digests
    spec = spec_with(digests, surprise="value")
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_UNKNOWN_FIELD

    spec = base_spec(digests)
    spec["invariants"][0]["surprise"] = "value"
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_UNKNOWN_FIELD


def test_missing_field_and_null_and_float_refuse(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    del spec["defaults"]["repair_attempts_r"]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_MISSING_FIELD

    # the repairs block is required even when empty.
    spec = base_spec(digests)
    del spec["invariants"][0]["repairs"]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_MISSING_FIELD

    # floats never enter the canonical form (rates are basis points).
    spec = base_spec(digests)
    spec["defaults"]["eventual_slice_basis_points"] = 0.1
    with pytest.raises(SpecLoadRefused):
        _load(spec, registry, parsers)


def test_dangling_references_refuse(registry_and_digests, parsers) -> None:
    registry, digests = registry_and_digests
    cases = []
    spec = base_spec(digests)
    spec["invariants"][0]["scope"] = "corpus://nope"
    cases.append(spec)
    spec = base_spec(digests)
    spec["invariants"][1]["property"]["context_view"] = "nope"
    cases.append(spec)
    spec = base_spec(digests)
    spec["invariants"][1]["property"]["witness_parser"] = "parser://nope@v9"
    cases.append(spec)
    spec = base_spec(digests)
    spec["invariants"][0]["witness_key"] = ["not_a_column"]
    cases.append(spec)
    for spec in cases:
        with pytest.raises(SpecLoadRefused) as excinfo:
            _load(spec, registry, parsers)
        assert _refusal_code(excinfo) == REFUSAL_DANGLING_REFERENCE


def test_query_digest_mismatch_refuses(registry_and_digests, parsers) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    spec["invariants"][0]["property"]["query_digest"] = "sha256:0000"
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_QUERY_DIGEST_MISMATCH

    # omitting the authored digest is refused too — a digest recorded only
    # at load would let a program change invalidate nothing.
    spec = base_spec(digests)
    del spec["views"][0]["query_digest"]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_MISSING_FIELD


# --------------------------------------------------------------------------- #
# Load-time checks.                                                            #
# --------------------------------------------------------------------------- #
def test_attaches_edge_makes_a_view_cycle_visible(
    registry_and_digests, parsers
) -> None:
    """The reviewed edge: two generation views reading each other's ATTACHED
    annotation kinds form a true cycle only the view→attaches edge reveals."""
    registry, digests = registry_and_digests
    digest_a = registry.add(
        "ir://checks/view_a@v1",
        annotation_reader_program("spec.view_a.v1", "kind-b"))
    digest_b = registry.add(
        "ir://checks/view_b@v1",
        annotation_reader_program("spec.view_b.v1", "kind-a"))
    spec = base_spec(digests)
    spec["views"].extend([
        {"id": "view-a", "version": 1, "query": "ir://checks/view_a@v1",
         "query_digest": digest_a, "role": "generation",
         "renders": "doc://hr-wiki/a", "attaches": ["kind-a"]},
        {"id": "view-b", "version": 1, "query": "ir://checks/view_b@v1",
         "query_digest": digest_b, "role": "generation",
         "renders": "doc://hr-wiki/b", "attaches": ["kind-b"]},
    ])
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_DERIVATION_CYCLE
    # without the attaches declarations the same views load: the cycle is
    # carried by the reviewed edge, not by an accident of the base spec.
    spec["views"][-1]["attaches"] = []
    spec["views"][-2]["attaches"] = []
    _load(spec, registry, parsers)


def test_classification_is_recorded_per_object(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    digest_census = digests["ir://checks/corpus_census@v1"]
    spec = base_spec(digests)
    spec["invariants"].append({
        "id": "corpus-census", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "eventual"},
        "property": {"query": "ir://checks/corpus_census@v1",
                     "query_digest": digest_census},
        "witness_key": ["subtype"],
        "repairs": {"envelope": []},
    })
    loaded = _load(spec, registry, parsers)
    cls = loaded.report.classification
    # subject-local query: selections only -> instance certificates allowed.
    assert cls["invariant:day-counts-match"] == CLASSIFICATION_INSTANCE
    assert cls["view:handbook-vacation"] == CLASSIFICATION_INSTANCE
    # annotation-surface reader: kind-level only (reached means run).
    assert cls["view:ctx-sections"] == CLASSIFICATION_KIND_LEVEL
    # the judged invariant inherits its context view's classification.
    assert cls["invariant:no-contradiction"] == CLASSIFICATION_KIND_LEVEL
    # aggregation reads every row: whole graph, runs every pass.
    assert cls["invariant:corpus-census"] == CLASSIFICATION_WHOLE_GRAPH
    assert "invariant:corpus-census" in loaded.report.runs_every_pass


def test_lazy_inadmissible_refuses(registry_and_digests, parsers) -> None:
    registry, digests = registry_and_digests
    digest_reader = digests["ir://checks/day_count_reader@v1"]
    spec = base_spec(digests)
    # a STRICT invariant reading the day-count annotation kind...
    spec["invariants"].append({
        "id": "strict-day-count", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "strict"},
        "property": {"query": "ir://checks/day_count_reader@v1",
                     "query_digest": digest_reader},
        "witness_key": ["annotation_id"],
        "repairs": {"envelope": []},
    })
    # ...and a LAZY invariant whose repairs write that very kind.
    spec["invariants"].append({
        "id": "lazy-day-count", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "lazy"},
        "property": {"query": "ir://checks/day_count_match@v5",
                     "query_digest": digests["ir://checks/day_count_match@v5"]},
        "witness_key": ["node_id"],
        "repairs": {"envelope": ["annotation://hr-wiki/*/day-count"]},
    })
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_LAZY_INADMISSIBLE

    # an underivable strict read set (whole graph) contributes ⊤: every lazy
    # invariant with a NONEMPTY revelation bound is refused...
    spec = base_spec(digests)
    spec["invariants"].append({
        "id": "strict-census", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "strict"},
        "property": {"query": "ir://checks/corpus_census@v1",
                     "query_digest": digests["ir://checks/corpus_census@v1"]},
        "witness_key": ["subtype"],
        "repairs": {"envelope": []},
    })
    spec["invariants"].append({
        "id": "lazy-anything", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "lazy"},
        "property": {"query": "ir://checks/day_count_match@v5",
                     "query_digest": digests["ir://checks/day_count_match@v5"]},
        "witness_key": ["node_id"],
        "repairs": {"envelope": ["annotation://hr-wiki/*/day-count"]},
    })
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_LAZY_INADMISSIBLE
    # ...but a lazy invariant with an EMPTY envelope reveals nothing and
    # loads even under a ⊤ strict reader.
    spec["invariants"][-1]["repairs"] = {"envelope": []}
    _load(spec, registry, parsers)


def test_lazy_is_refused_when_a_free_form_view_exists(
    registry_and_digests, parsers
) -> None:
    """EWD-SVC-1: a free-form view's unenumerable reads consume ANY written
    kind, so the revelation bound is unknowable while strict invariants
    exist — the lazy declaration refuses instead of loading past it."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    spec["views"].append({
        "id": "render-notes", "version": 1,
        "query": "free-form://meeting-notes", "role": "generation",
        "renders": "doc://hr-wiki/notes", "attaches": ["day-count"],
    })
    spec["invariants"].append({
        "id": "strict-day-count", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "strict"},
        "property": {"query": "ir://checks/day_count_reader@v1",
                     "query_digest": digests["ir://checks/day_count_reader@v1"]},
        "witness_key": ["annotation_id"],
        "repairs": {"envelope": []},
    })
    spec["invariants"].append({
        "id": "lazy-graph-repair", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "lazy"},
        "property": {"query": "ir://checks/day_count_match@v5",
                     "query_digest": digests["ir://checks/day_count_match@v5"]},
        "witness_key": ["node_id"],
        "repairs": {"envelope": ["graph://fact/x"]},
    })
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_LAZY_INADMISSIBLE
    # the UNKNOWABLE-reads rule fired (its refusal names the view), not a
    # coincidental closure overlap.
    assert excinfo.value.data.get("unknown_reader_views") == ["render-notes"]
    # and with no strict invariant, deferral can hide nothing: the same
    # free-form view and lazy repair load.
    spec["invariants"] = [
        inv for inv in spec["invariants"] if inv["id"] != "strict-day-count"
    ]
    _load(spec, registry, parsers)


def test_lazy_is_refused_when_the_bound_reaches_an_unbounded_extractor(
    registry_and_digests, parsers
) -> None:
    """EWD-SVC-2: a corpus whose extractor declares NO output envelope can
    emit any kind from its documents; a lazy repair whose bound reaches
    those documents is unknowable and refuses."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    del spec["corpora"][0]["extractors"][0]["output_envelope"]
    spec["invariants"].append({
        "id": "strict-day-count", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "strict"},
        "property": {"query": "ir://checks/day_count_reader@v1",
                     "query_digest": digests["ir://checks/day_count_reader@v1"]},
        "witness_key": ["annotation_id"],
        "repairs": {"envelope": []},
    })
    spec["invariants"].append({
        "id": "lazy-doc-repair", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "lazy"},
        "property": {"query": "ir://checks/day_count_match@v5",
                     "query_digest": digests["ir://checks/day_count_match@v5"]},
        "witness_key": ["node_id"],
        "repairs": {"envelope": ["doc://hr-wiki/notes"]},
    })
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_LAZY_INADMISSIBLE


def test_lazy_is_refused_when_it_rewrites_a_strict_judges_subject_docs(
    registry_and_digests, parsers
) -> None:
    """EWD-SVC-3: a strict JUDGED invariant reads its subject documents
    (the prompt includes the scoped text), not only its context view; a
    lazy repair editing those documents refuses."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    spec["invariants"][1]["policy"] = {"mode": "strict"}
    spec["invariants"].append({
        "id": "lazy-doc-repair", "version": 1, "tier": "formal",
        "scope": "corpus://hr-wiki",
        "policy": {"mode": "lazy"},
        "property": {"query": "ir://checks/day_count_match@v5",
                     "query_digest": digests["ir://checks/day_count_match@v5"]},
        "witness_key": ["node_id"],
        "repairs": {"envelope": ["doc://hr-wiki/notes"]},
    })
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_LAZY_INADMISSIBLE


def test_incident_edges_programs_are_demoted_to_kind_level() -> None:
    """EWD-SVC-4: the frontier's node_with_incident_edges mode never
    verifies the join predicate is endpoint incidence, so classification
    demotes it — no instance certificate for a possibly join-defined
    enumeration."""
    from okg.substrate.consistency.invariant_spec import classify_program
    from tests.substrate.consistency._invariant_spec_support import (
        incident_edges_program,
    )

    assert classify_program(
        incident_edges_program("svc4.incident.v1")
    ) == CLASSIFICATION_KIND_LEVEL


def test_witness_key_refuses_on_an_underivable_projection(
    registry_and_digests, parsers
) -> None:
    """EWD-SVC-6: no derivable output columns means NO witness key can be
    validated — fail closed, never vacuously pass."""
    from tests.substrate.consistency._invariant_spec_support import (
        projectionless_program,
    )
    registry, digests = registry_and_digests
    digest = registry.add(
        "ir://checks/raw@v1", projectionless_program("svc6.raw.v1"))
    spec = base_spec(digests)
    spec["invariants"][0]["property"] = {
        "query": "ir://checks/raw@v1", "query_digest": digest,
    }
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_DANGLING_REFERENCE


def test_canonical_json_refuses_bools_floats_and_nulls() -> None:
    """EWD-SVC-7: the canonical closure is integers and strings — bool,
    float, and null all refuse at the canonical layer itself."""
    for bad in ({"x": True}, {"x": 0.1}, {"x": None}):
        with pytest.raises(SpecLoadRefused):
            canonical_json(bad)
    assert canonical_json({"x": 1, "y": "s"}) == '{"x":1,"y":"s"}'


def test_reserved_kind_names_and_context_attaches_refuse(
    registry_and_digests, parsers
) -> None:
    """EWD-SVC-9/-10: kind names colliding with the vocabulary's reserved
    names refuse, and a judged-context view may not declare attaches."""
    registry, digests = registry_and_digests
    for reserved in ("graph", "⊤", "doc:hr-wiki"):
        spec = base_spec(digests)
        spec["corpora"][0]["extractors"][0]["output_envelope"] = [reserved]
        with pytest.raises(SpecLoadRefused) as excinfo:
            _load(spec, registry, parsers)
        assert _refusal_code(excinfo) == REFUSAL_UNKNOWN_FIELD
    spec = base_spec(digests)
    spec["views"][1]["attaches"] = ["policy-citation"]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_UNKNOWN_FIELD


def test_strict_over_advisory_warns_but_loads(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    spec["invariants"][0]["scope"] = "corpus://partner-wiki"
    spec["invariants"][0]["policy"] = {"mode": "strict"}
    spec["invariants"][0]["repairs"] = {"envelope": []}
    loaded = _load(spec, registry, parsers)
    codes = [w["code"] for w in loaded.report.warnings]
    assert WARNING_STRICT_OVER_ADVISORY in codes


def test_envelope_exceeds_authority_refuses(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    # document edits over an ADVISORY corpus: propose only, never edit.
    spec = base_spec(digests)
    spec["invariants"][0]["repairs"]["envelope"] = [
        "annotation://partner-wiki/*/day-count"
    ]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY

    # AUTHORITATIVE documents are never repair targets...
    spec = base_spec(digests)
    spec["invariants"][0]["repairs"]["envelope"] = ["doc://reg-lib/reg-7"]
    with pytest.raises(SpecLoadRefused) as excinfo:
        _load(spec, registry, parsers)
    assert _refusal_code(excinfo) == REFUSAL_ENVELOPE_EXCEEDS_AUTHORITY

    # ...but the graph is: precedence-inverted repairs land on graph facts.
    spec = base_spec(digests)
    spec["invariants"][0]["scope"] = "corpus://reg-lib"
    spec["invariants"][0]["repairs"]["envelope"] = ["graph://fact/obligation"]
    _load(spec, registry, parsers)


def test_unbounded_extractor_is_recorded_not_refused(
    registry_and_digests, parsers
) -> None:
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    del spec["corpora"][0]["extractors"][0]["output_envelope"]
    loaded = _load(spec, registry, parsers)
    assert "hr-wiki/hr-annotations" in loaded.report.unbounded_extractors


# --------------------------------------------------------------------------- #
# Digests and invalidation keying.                                             #
# --------------------------------------------------------------------------- #
def test_invariant_digest_keys_invalidation_narrowly(
    registry_and_digests, parsers
) -> None:
    """Editing ONE invariant changes exactly that invariant's digest (and the
    spec digest); every other object digest is untouched — so certificate
    invalidation keyed on invariant_digest kills exactly that invariant's
    certificates, never the deployment's."""
    registry, digests = registry_and_digests
    before = _load(base_spec(digests), registry, parsers)
    spec = base_spec(digests)
    spec["invariants"][0]["version"] = 6
    after = _load(spec, registry, parsers)
    assert (
        after.invariant_digest("day-counts-match")
        != before.invariant_digest("day-counts-match")
    )
    assert after.spec_digest != before.spec_digest
    unchanged = [
        key for key in before.object_digests
        if key != "invariant:day-counts-match"
    ]
    for key in unchanged:
        assert after.object_digests[key] == before.object_digests[key], key


def test_judged_invariant_digest_covers_its_context_view(
    registry_and_digests, parsers
) -> None:
    """A context-view edit changes the judged invariant's digest transitively
    (slice doc §5): prompt/context changes invalidate exactly the invariants
    that read through the view."""
    registry, digests = registry_and_digests
    before = _load(base_spec(digests), registry, parsers)
    spec = base_spec(digests)
    spec["views"][1]["version"] = 2
    after = _load(spec, registry, parsers)
    assert (
        after.invariant_digest("no-contradiction")
        != before.invariant_digest("no-contradiction")
    )
    assert (
        after.invariant_digest("day-counts-match")
        == before.invariant_digest("day-counts-match")
    )


# --------------------------------------------------------------------------- #
# The certificate shape: the join demotion is enforced by type.                #
# --------------------------------------------------------------------------- #
def _certificate(**overrides):
    base = dict(
        invariant_id="day-counts-match", invariant_version=5,
        subject="doc://hr-wiki/handbook",
        classification=CLASSIFICATION_INSTANCE,
        read_facts=("fact:1", "fact:2"),
        read_collections=(("annotations:hr-wiki:day-count", "v3"),),
        result_digest="sha256:r", pass_stamp="pass:7",
        invariant_digest="sha256:i", spec_digest="sha256:s",
    )
    base.update(overrides)
    return SpecCertificate(**base)


def test_read_collections_are_type_limited_to_instance_certifiable() -> None:
    assert _certificate().read_collections  # instance_certifiable: legal
    for classification in (CLASSIFICATION_KIND_LEVEL,
                           CLASSIFICATION_WHOLE_GRAPH):
        with pytest.raises(SpecLoadRefused):
            _certificate(classification=classification)
        # without membership versions the demoted classes still certify
        # nothing-collection-shaped (the record itself is constructible).
        _certificate(classification=classification, read_collections=())


def test_certificate_refuses_unknown_classification_and_empty_keys() -> None:
    with pytest.raises(SpecLoadRefused):
        _certificate(classification="somewhat_certifiable")
    with pytest.raises(SpecLoadRefused):
        _certificate(invariant_digest="")


def test_spec_serialization_is_deterministic_across_processes_shape(
    registry_and_digests, parsers
) -> None:
    """Same input mapping, two independent loads: identical canonical JSON —
    no ordering, locale, or set-iteration dependence."""
    registry, digests = registry_and_digests
    spec = base_spec(digests)
    a = _load(copy.deepcopy(spec), registry, parsers)
    b = _load(copy.deepcopy(spec), registry, parsers)
    assert canonical_json(spec_to_canonical_dict(a)) == canonical_json(
        spec_to_canonical_dict(b)
    )
    assert a.spec_digest == b.spec_digest

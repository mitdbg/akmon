"""Reuse certificates — the persisted justification behind every cone skip.

PACT ``consistency-resolution-aware-reuse``, task.reuse.certificate-store
(req.reuse.certificates). Witnesses named VERBATIM by
docs/develop/architecture/consistency.md §9.5, mapped to the task verification
clause ("a skip writes a certificate whose recorded dependencies enumerate
every consumed versioned fact; mutating each dependency class in turn
invalidates it before the next decision; certificates are queryable per
generation and step"):

* ``test_every_cone_skip_has_certificate`` — the store API makes a
  certificate-less skip structurally impossible (``record_skip`` takes a
  :class:`ReuseCertificate`, not an optional), and the row it writes carries
  the whole certificate (digest + comparison + dependencies);
* ``test_each_dependency_class_invalidates`` — mutating EACH recorded
  dependency class in turn makes :func:`validate_certificate` report EXACTLY
  that class (validity is evaluated, never a stored flag);
* ``test_certificates_queryable_per_generation_and_step`` — against a real
  fixture DB, certificates are queryable per generation and per step, the
  supervisor-quotable rows print, and the read round-trips byte-stably.

Plus the task's extra obligations: certificate JSON round-trips byte-stable, a
tampered digest is refused, and a malformed comparison / incomplete
dependencies payload refuses loudly.
"""
from __future__ import annotations

import dataclasses
import inspect
import json
from typing import Any

import pytest

from okg.substrate.consistency.reuse_certificates import (
    COMPARISON_KIND_RESOLUTION,
    DEPENDENCY_CLASSES,
    HARMLESSNESS_FACTS,
    REQUIRED_COMPARISON_KEYS,
    RESOLUTION_V1_FACT_COVERAGE,
    CertificateDependencies,
    ReuseCertificate,
    ReuseCertificateStore,
    ValidationResult,
    validate_certificate,
)
from okg.substrate.errors import SubstrateError


# --------------------------------------------------------------------------- #
# Fixtures — a fully-populated certificate over every dependency class.        #
# --------------------------------------------------------------------------- #
def _comparison() -> dict[str, Any]:
    return {
        "kind": COMPARISON_KIND_RESOLUTION,
        "incumbent_outcome": "selected",
        "incumbent_basis": "source_authority",
        "effects_summary_digest": "effects:deadbeef",
        "competitor_bound": {
            "best_competitor_score": 3,
            "statement": "no permitted output beats the incumbent",
        },
        "tie_break": None,   # score decides here; key present, value null
        # Four-fact contract (§9.5, adjudication v2 F3) — the shared v1 map.
        "fact_coverage": dict(RESOLUTION_V1_FACT_COVERAGE),
    }


def _dependencies() -> CertificateDependencies:
    return CertificateDependencies(
        input_digests={"claim:a": "in:aaa", "claim:b": "in:bbb"},
        operator_versions={"enricher:claim_extractor": "v3", "projector": "v1"},
        model_prompt_keys={"enricher:claim_extractor": "omega:cafef00d"},
        envelope_fingerprint="envfp:base",
        constraint_set={"unique_preferred": "chk:1111"},
        generator_versions={"precedence-retraction": "v1"},
        policy_fingerprint="polfp:base",
        precedence_fingerprint="precfp:base",
        competitor_bounds={"per_output": {"vX": "bound-a"}},
        catalog_version_id="catver:7",
    )


def _certificate(
    *,
    deployment: str = "dep",
    generation_id: str = "7",
    step_id: str = "claim_projection",
    comparison: dict[str, Any] | None = None,
    dependencies: CertificateDependencies | None = None,
) -> ReuseCertificate:
    return ReuseCertificate(
        deployment=deployment,
        generation_id=generation_id,
        step_id=step_id,
        comparison=_comparison() if comparison is None else comparison,
        dependencies=_dependencies() if dependencies is None else dependencies,
    )


_SCALAR_CHANGE = {
    "envelope_fingerprint": "envfp:CHANGED",
    "policy_fingerprint": "polfp:CHANGED",
    "precedence_fingerprint": "precfp:CHANGED",
    "catalog_version_id": "catver:CHANGED",
}


def _mutate_dependency_class(
    deps: CertificateDependencies, cls: str
) -> CertificateDependencies:
    """Return a copy of ``deps`` with EXACTLY the class ``cls`` changed."""
    if cls in _SCALAR_CHANGE:
        return dataclasses.replace(deps, **{cls: _SCALAR_CHANGE[cls]})
    if cls == "competitor_bounds":
        return dataclasses.replace(
            deps, competitor_bounds={"per_output": {"vX": "bound-CHANGED"}}
        )
    current = dict(getattr(deps, cls))
    key = sorted(current)[0]
    current[key] = current[key] + ":CHANGED"
    return dataclasses.replace(deps, **{cls: current})


# --------------------------------------------------------------------------- #
# A fake connection: proves record_skip's write shape without Postgres.        #
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, sink: list[tuple[Any, ...]]) -> None:
        self._sink = sink

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self._sink.append((sql, params))


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.calls)


# --------------------------------------------------------------------------- #
# §9.5 witness 1 — every cone skip has a certificate (the load-bearing shape). #
# --------------------------------------------------------------------------- #
def test_every_cone_skip_has_certificate() -> None:
    store = ReuseCertificateStore(deployment="dep")

    # (1) the API is typed so a skip WITHOUT a certificate is impossible: the
    #     `certificate` parameter is required (no default) and typed
    #     ReuseCertificate — there is no keyless / certificate-less skip path.
    sig = inspect.signature(store.record_skip)
    cert_param = sig.parameters["certificate"]
    assert cert_param.default is inspect.Parameter.empty
    assert "ReuseCertificate" in str(cert_param.annotation)

    # (2) a certificate-less skip is refused BEFORE any write — nothing lands.
    conn = _FakeConn()
    for bad in (None, {"comparison": {}}, "not-a-cert"):
        with pytest.raises(SubstrateError, match="ReuseCertificate"):
            store.record_skip(conn, bad)  # type: ignore[arg-type]
    assert conn.calls == []

    # (3) a real skip writes exactly one row that CARRIES the whole certificate:
    #     digest + comparison + dependencies. There is no row without them.
    cert = _certificate()
    digest = store.record_skip(conn, cert)
    assert digest == cert.digest
    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert "INSERT INTO okg.reuse_certificates" in sql
    assert params[0] == cert.deployment
    assert params[1] == cert.generation_id
    assert params[2] == cert.step_id
    assert params[3] == cert.digest                                   # digest
    assert json.loads(params[4]) == dict(cert.comparison)            # comparison
    assert (                                                          # dependencies
        json.loads(params[5]) == cert.dependencies.to_json_dict()
    )
    # the recorded dependencies enumerate EVERY consumed versioned fact class.
    assert set(json.loads(params[5])) == set(DEPENDENCY_CLASSES)


def test_record_skip_rejects_foreign_deployment() -> None:
    store = ReuseCertificateStore(deployment="dep-a")
    cert = _certificate(deployment="dep-b")
    with pytest.raises(SubstrateError, match="deployment does not match"):
        store.record_skip(_FakeConn(), cert)


# --------------------------------------------------------------------------- #
# §9.5 witness 2 — each dependency class invalidates, exactly.                 #
# --------------------------------------------------------------------------- #
def test_each_dependency_class_invalidates() -> None:
    cert = _certificate()

    # Unchanged current facts -> valid, zero invalidated classes.
    result = validate_certificate(_dependencies(), cert)
    assert isinstance(result, ValidationResult)
    assert result.valid
    assert result.invalidated_classes == frozenset()

    # Every dependency class is covered (no class silently unvalidated).
    assert len(DEPENDENCY_CLASSES) == 10

    # Mutate EACH class in turn -> validate reports EXACTLY that class.
    for cls in DEPENDENCY_CLASSES:
        current = _mutate_dependency_class(_dependencies(), cls)
        result = validate_certificate(current, cert)
        assert result.invalidated_classes == frozenset({cls}), (
            cls, sorted(result.invalidated_classes)
        )
        assert not result.valid

    # Two classes at once -> both named (per-class, not first-wins).
    current = _mutate_dependency_class(
        _mutate_dependency_class(_dependencies(), "policy_fingerprint"),
        "catalog_version_id",
    )
    result = validate_certificate(current, cert)
    assert result.invalidated_classes == frozenset(
        {"policy_fingerprint", "catalog_version_id"}
    )


def test_validate_accepts_mapping_current_facts() -> None:
    # The current facts may arrive as the dependencies mapping form (what the
    # scheduler wave assembles); a matching snapshot validates clean.
    cert = _certificate()
    result = validate_certificate(_dependencies().to_json_dict(), cert)
    assert result.valid


def test_validate_refuses_a_bare_connection_loudly() -> None:
    # A live-connection-driven gather is the scheduler wave's job; passing
    # anything that is not the current-facts snapshot is a loud refusal, never a
    # silent "assume valid".
    cert = _certificate()

    class _Conn:
        def cursor(self) -> Any:  # pragma: no cover - shape only
            raise AssertionError("must not be queried")

    with pytest.raises(SubstrateError, match="current dependency facts"):
        validate_certificate(_Conn(), cert)


# --------------------------------------------------------------------------- #
# Round-trip byte-stability + tamper refusal + malformed-payload refusal.       #
# --------------------------------------------------------------------------- #
def test_certificate_json_round_trip_is_byte_stable() -> None:
    cert = _certificate()
    restored = ReuseCertificate.from_canonical_json(cert.canonical_json())
    assert restored.canonical_json() == cert.canonical_json()
    assert restored.digest == cert.digest
    # The dependencies sub-object round-trips byte-stable on its own too.
    deps = _dependencies()
    assert (
        CertificateDependencies.from_json_dict(deps.to_json_dict()).canonical_json()
        == deps.canonical_json()
    )


def test_tampered_digest_is_refused() -> None:
    cert = _certificate()
    # A recorded digest that disagrees with the content is refused.
    payload = cert.to_json_dict()
    payload["digest"] = "0" * 64
    with pytest.raises(SubstrateError, match="digest does not match"):
        ReuseCertificate.from_json_dict(payload)
    # Altering the content while keeping the old digest is likewise refused
    # (content-addressed: the certificate is not trusted once altered).
    payload2 = cert.to_json_dict()
    payload2["comparison"]["incumbent_basis"] = "TAMPERED"
    with pytest.raises(SubstrateError, match="digest does not match"):
        ReuseCertificate.from_json_dict(payload2)


def test_malformed_comparison_refuses_loudly() -> None:
    deps = _dependencies()
    # Missing a required comparison key.
    for missing in sorted(REQUIRED_COMPARISON_KEYS):
        bad = _comparison()
        del bad[missing]
        with pytest.raises(SubstrateError, match="missing required keys"):
            ReuseCertificate(
                deployment="d", generation_id="1", step_id="s",
                comparison=bad, dependencies=deps,
            )
    # A non-object comparison.
    with pytest.raises(SubstrateError, match="must be a JSON object"):
        ReuseCertificate(
            deployment="d", generation_id="1", step_id="s",
            comparison=["nope"], dependencies=deps,  # type: ignore[arg-type]
        )


def test_absent_or_unknown_comparison_kind_refuses_loudly() -> None:
    """The comparison ``kind`` is MANDATORY (PACT consistency-single-engine):
    now that every writer stamps a kind, an ABSENT kind is refused as loudly as
    an UNKNOWN one — never read as a default resolution shape (spec §9.5)."""
    deps = _dependencies()
    # An absent kind: the retired tolerance would have read this as resolution.
    kindless = _comparison()
    del kindless["kind"]
    with pytest.raises(SubstrateError, match="missing its `kind` discriminator"):
        ReuseCertificate(
            deployment="d", generation_id="1", step_id="s",
            comparison=kindless, dependencies=deps,
        )
    # An unknown kind is refused with the closed allow-list.
    unknown = {**_comparison(), "kind": "not_a_real_kind"}
    with pytest.raises(SubstrateError, match="unknown kind"):
        ReuseCertificate(
            deployment="d", generation_id="1", step_id="s",
            comparison=unknown, dependencies=deps,
        )


def test_fact_coverage_incomplete_refuses_loudly() -> None:
    """The four-fact contract (§9.5; adjudication v2 F3): a resolution
    comparison whose coverage omits a fact, supports one with an empty premise
    list, or cites a phantom premise is refused — a certificate that cannot
    say why the optimum set is preserved is not a certificate."""
    deps = _dependencies()
    # (a) A missing fact.
    for fact in sorted(HARMLESSNESS_FACTS):
        bad = _comparison()
        bad["fact_coverage"] = {
            k: v for k, v in RESOLUTION_V1_FACT_COVERAGE.items() if k != fact
        }
        with pytest.raises(SubstrateError, match="EXACTLY the\\s+four"):
            ReuseCertificate(
                deployment="d", generation_id="1", step_id="s",
                comparison=bad, dependencies=deps,
            )
    # (b) An unknown fifth fact.
    extra = _comparison()
    extra["fact_coverage"] = {
        **RESOLUTION_V1_FACT_COVERAGE, "vibes_preserved": ["tie_break"],
    }
    with pytest.raises(SubstrateError, match="EXACTLY the\\s+four"):
        ReuseCertificate(
            deployment="d", generation_id="1", step_id="s",
            comparison=extra, dependencies=deps,
        )
    # (c) An empty premise list: the fact is declared but unsupported.
    empty = _comparison()
    empty["fact_coverage"] = {
        **RESOLUTION_V1_FACT_COVERAGE, "optimum_set_unchanged": [],
    }
    with pytest.raises(SubstrateError, match="non-empty list of premise keys"):
        ReuseCertificate(
            deployment="d", generation_id="1", step_id="s",
            comparison=empty, dependencies=deps,
        )
    # (d) A phantom premise: coverage citing a key the comparison lacks.
    phantom = _comparison()
    phantom["fact_coverage"] = {
        **RESOLUTION_V1_FACT_COVERAGE,
        "optimum_set_unchanged": ["objective_bound_that_does_not_exist"],
    }
    with pytest.raises(SubstrateError, match="phantom"):
        ReuseCertificate(
            deployment="d", generation_id="1", step_id="s",
            comparison=phantom, dependencies=deps,
        )


def test_builders_stamp_complete_fact_coverage() -> None:
    """Both resolution-kind writers stamp the shared v1 coverage map, and the
    stamped comparison passes the strengthened validation end-to-end."""
    from types import SimpleNamespace

    from okg.substrate.consistency.reuse_scheduler import (
        build_dominance_comparison,
        build_skip_comparison,
    )

    effects = SimpleNamespace(digest=lambda: "effects:deadbeef")
    skip = build_skip_comparison(
        incumbent_outcome="selected",
        incumbent_basis="source_authority",
        effects=effects,
        competitor_bound={"statement": "bounded"},
    )
    assert skip["fact_coverage"] == RESOLUTION_V1_FACT_COVERAGE
    ReuseCertificate(   # validates on construction
        deployment="d", generation_id="1", step_id="s",
        comparison=skip, dependencies=_dependencies(),
    )

    incumbent = SimpleNamespace(
        outcome="selected",
        basis="declared_source_authority",
        to_canonical_dict=lambda: {"authority": "advisory"},
    )
    bound = SimpleNamespace(to_canonical_dict=lambda: {"best": "runbook"})
    judgment = SimpleNamespace(to_canonical_dict=lambda: {"dominates": True})
    dominance = build_dominance_comparison(
        incumbent=incumbent,
        effects=effects,
        competitor_bound=bound,
        per_bucket=[(("subject", "unit"), incumbent, judgment)],
    )
    assert dominance["fact_coverage"] == RESOLUTION_V1_FACT_COVERAGE
    ReuseCertificate(
        deployment="d", generation_id="1", step_id="s",
        comparison=dominance, dependencies=_dependencies(),
    )


def test_degenerate_comparison_is_exempt_from_fact_coverage() -> None:
    """The DEGENERATE kind carries no fact_coverage: byte-identical inputs
    trivially preserve all four facts; its unchanged_inputs statement IS the
    coverage (§9.5). It must keep validating without one."""
    from okg.substrate.consistency.reuse_scheduler import (
        build_degenerate_comparison,
    )

    degenerate = build_degenerate_comparison(
        incumbent_outcome="selected",
        incumbent_basis="source_authority",
    )
    assert "fact_coverage" not in degenerate
    ReuseCertificate(
        deployment="d", generation_id="1", step_id="s",
        comparison=degenerate, dependencies=_dependencies(),
    )


def test_incomplete_dependencies_refuse_loudly() -> None:
    incomplete = _dependencies().to_json_dict()
    del incomplete["policy_fingerprint"]
    with pytest.raises(SubstrateError, match="missing dependency"):
        CertificateDependencies.from_json_dict(incomplete)


def test_dependency_class_wrong_type_refuses_loudly() -> None:
    with pytest.raises(SubstrateError, match="must be a mapping"):
        dataclasses.replace(_dependencies(), operator_versions="not-a-map")  # type: ignore[arg-type]
    with pytest.raises(SubstrateError, match="must be a string"):
        dataclasses.replace(_dependencies(), envelope_fingerprint=123)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# §9.5 witness 3 — queryable per generation and per step (real fixture DB).    #
# --------------------------------------------------------------------------- #
psycopg = pytest.importorskip("psycopg")


@pytest.mark.postgres
def test_certificates_queryable_per_generation_and_step(
    clean_runtime: str,
) -> None:
    dep = "reuse-cert-fixture"
    store = ReuseCertificateStore(deployment=dep)

    recorded = {
        ("101", "stepA"): _certificate(
            deployment=dep, generation_id="101", step_id="stepA"
        ),
        ("101", "stepB"): _certificate(
            deployment=dep, generation_id="101", step_id="stepB"
        ),
        ("102", "stepA"): _certificate(
            deployment=dep, generation_id="102", step_id="stepA"
        ),
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for cert in recorded.values():
            store.record_skip(conn, cert)

        gen101 = store.for_generation(conn, "101")
        gen102 = store.for_generation(conn, "102")
        step_a = store.for_step(conn, "stepA")
        got = store.get(conn, generation_id="101", step_id="stepA")
        missing = store.get(conn, generation_id="999", step_id="stepA")

        rows_gen101 = store.quotable_rows(conn, generation_id="101")
        rows_step_a = store.quotable_rows(conn, step_id="stepA")

    # queryable per generation
    assert {c.step_id for c in gen101} == {"stepA", "stepB"}
    assert {c.generation_id for c in gen101} == {"101"}
    assert {c.step_id for c in gen102} == {"stepA"}
    # queryable per step (across generations)
    assert {c.generation_id for c in step_a} == {"101", "102"}
    assert {c.step_id for c in step_a} == {"stepA"}
    # exact get + honest miss (never a fuzzy match)
    assert got is not None
    assert missing is None

    # the DB read round-trips byte-stable against the in-memory certificate.
    assert got.canonical_json() == recorded[("101", "stepA")].canonical_json()
    round_tripped = ReuseCertificate.from_canonical_json(got.canonical_json())
    assert round_tripped.canonical_json() == got.canonical_json()
    # the recorded dependencies enumerate every consumed versioned fact.
    assert set(got.dependencies.to_json_dict()) == set(DEPENDENCY_CLASSES)

    # supervisor-quotable rows — printed so the operator can quote them.
    print("\n[reuse-cert] queryable per generation 101:")
    for r in rows_gen101:
        print("   ", r)
    print("[reuse-cert] queryable per step stepA (across generations):")
    for r in rows_step_a:
        print("   ", r)
    assert len(rows_gen101) == 2
    assert len(rows_step_a) == 2
    assert {r["step_id"] for r in rows_gen101} == {"stepA", "stepB"}
    assert {r["generation_id"] for r in rows_step_a} == {"101", "102"}
    for r in rows_gen101 + rows_step_a:
        assert r["digest"]                                   # non-empty digest
        assert r["dependency_classes"] == len(DEPENDENCY_CLASSES)
        assert r["incumbent_outcome"] == "selected"

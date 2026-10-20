"""Single authoritative consistency engine — the standing audit.

PACT ``consistency-single-engine``. Two falsifiable guards remain at program
close:

* **The one-skip audit** (req.single.one-skip-path, task.single.one-skip-audit).
  Over a REAL scheduler-governed publish with MIXED outcomes (a justified skip,
  a degenerate unchanged-inputs skip, and a run), every skip on the resolved
  state's dependency cone carries a persisted reuse certificate, and every
  persisted certificate answers a skip — a bijection read back from
  ``okg.reuse_certificates`` (not a code-reading claim). No manifest-hash /
  carry-forward-only skip path survives: the degenerate case rides the SAME
  certificate mechanism. Falsifiable: physically deleting a certificate row makes
  the audit report the now-uncertified skip. The artifacts OUTSIDE the cone are
  enumerated explicitly (``OUTSIDE_CONE``), never silently exempt.

* **The retirement-residue gate** (req.single.inventory-executed,
  task.single.inventory-lint). The symbols this program DELETED are absent from
  ``src/`` (blocker tier); the symbols still FROZEN behind the cross-repo
  cern-twiki gate are present and named — a loud, pending blockage rather than a
  quiet drop from the inventory.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Sequence

import pytest

from okg.substrate.consistency.possible_effects import ChangedStateTerms
from okg.substrate.consistency.reuse_certificates import (
    DEPENDENCY_CLASSES,
    CertificateDependencies,
    ReuseCertificate,
    ReuseCertificateStore,
)
from okg.substrate.consistency.reuse_scheduler import (
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_EFFECTS_OVERLAP,
    CAUSE_SKIP_JUSTIFIED,
    OUTSIDE_CONE,
    ReuseStep,
)
from okg.substrate.producer_authority import normalize_write_envelope

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCHEMA_SQL = _REPO_ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"
_CLI_MAIN = _REPO_ROOT / "src" / "okg" / "substrate" / "cli" / "main.py"
_SQL_GATE = _REPO_ROOT / "src" / "okg" / "substrate" / "consistency" / "sql_gate.py"
_EQUIVALENCE = (
    _REPO_ROOT / "src" / "okg" / "substrate" / "consistency" / "equivalence.py"
)
_CLAIM_MODEL = _REPO_ROOT / "src" / "okg" / "substrate" / "consistency" / "claim_model"
_SCOPE = _CLAIM_MODEL / "scope.py"
_APPLICABILITY = _CLAIM_MODEL / "applicability.py"
_CLAIM_MODEL_INIT = _CLAIM_MODEL / "__init__.py"
_CONFLICTS = _CLAIM_MODEL / "conflicts.py"
_PROJECTION = (
    _REPO_ROOT / "src" / "okg" / "substrate" / "derived_artifacts"
    / "claim_projection_derivation.py"
)

_PREDICATE = "preferred_target"


# --------------------------------------------------------------------------- #
# The audit's pure core — the skip↔certificate join, reused by the live run    #
# and by the falsifiability probe.                                             #
# --------------------------------------------------------------------------- #
def skips_without_certificates(
    skip_outcomes: Sequence[Any], persisted_certificates: Sequence[ReuseCertificate]
) -> list[str]:
    """Step ids that SKIPPED but carry no persisted certificate — the violation
    the single-skip-path invariant forbids. Empty means every cone skip is
    certificate-backed."""
    cert_steps = {c.step_id for c in persisted_certificates}
    return sorted(o.step_id for o in skip_outcomes if o.step_id not in cert_steps)


def certificates_without_skip(
    skip_outcomes: Sequence[Any], persisted_certificates: Sequence[ReuseCertificate]
) -> list[str]:
    """Persisted certificates with no answering skip — the other direction of the
    bijection (a stray certificate would mean a write outside the skip path)."""
    skip_steps = {o.step_id for o in skip_outcomes}
    return sorted(
        c.step_id for c in persisted_certificates if c.step_id not in skip_steps
    )


# =========================================================================== #
# Guard 2 (no DB): the retirement-residue gate.                                 #
# =========================================================================== #
def test_deleted_residue_is_absent_and_frozen_residue_is_named() -> None:
    """The single-engine deletions are grep-proven absent from ``src/`` (blocker
    tier); the items still frozen behind the cross-repo cern-twiki gate remain,
    named loudly so the blockage is visible rather than dropped."""
    # -- DELETED (blocker tier): the deprecated CLI alias residue is gone. The
    #    canonical verb `okg consistency-rerun` stays; only the nested
    #    `okg consistency rerun-post-publish` alias handling was removed.
    cli_src = _CLI_MAIN.read_text(encoding="utf-8")
    assert "rerun-post-publish" not in cli_src, (
        "the deprecated `okg consistency rerun-post-publish` alias must be gone "
        "from cli/main.py (consistency-single-engine inventory item #3)"
    )
    assert "consistency-rerun" in cli_src  # the canonical verb survives

    # -- DELETED (blocker tier): the M5 narrowing-trigger TODO skeleton comment.
    schema = _SCHEMA_SQL.read_text(encoding="utf-8")
    assert "skeleton only" not in schema, (
        "the M5 narrowing-trigger TODO skeleton comment must be gone from "
        "schema.sql (inventory item #6); enforcement is the publish gate + "
        "the application-path validator"
    )
    # Narrowing enforcement is REAL, not a stub — but it is no longer a DB
    # trigger. `edges_live_narrowing_check` was a CONSTRAINT TRIGGER on
    # okg.edges_live, and `schema-canonical-read-path` deleted that table with
    # the rest of the legacy live complex, so asserting the trigger's presence
    # would now pin a stub of the opposite kind: a name in a file that installs
    # nothing. The enforcement that exists is the publish gate's
    # `narrowing_violation` invariant.
    assert "edges_live_narrowing_check" not in schema, (
        "the legacy narrowing CONSTRAINT TRIGGER fired on an okg.edges_live "
        "INSERT; that table is deleted, so a reference here would install "
        "nothing"
    )
    error_codes = _REPO_ROOT / "docs" / "reference" / "error_codes.yaml"
    assert "narrowing_violation" in error_codes.read_text(encoding="utf-8"), (
        "narrowing enforcement must still be a NAMED, surfaced failure — if "
        "the code is gone too, the trigger's removal took the enforcement "
        "with it rather than relocating it"
    )

    # -- SUPERSEDED (ruling D6, issue #744): the retired standalone scope.py
    #    remains absent, while one typed applicability operator now has real
    #    library and maintained-projection consumers. Heading-only classification
    #    may not reappear beside it as a second engine.
    assert not _SCOPE.exists(), (
        "the retired standalone claim_model/scope.py must remain absent; typed "
        "applicability lives only in applicability.py"
    )
    applicability_src = _APPLICABILITY.read_text(encoding="utf-8")
    conflicts_src = _CONFLICTS.read_text(encoding="utf-8")
    projection_src = _PROJECTION.read_text(encoding="utf-8")
    assert applicability_src.count("def applicability_relation(") == 1
    assert "applicability_relation(" in conflicts_src
    assert "applicability_relation(" in projection_src
    assert "def _pair_outcome(" not in projection_src
    assert "def _not_disjoint(" not in projection_src
    claim_model_init = _CLAIM_MODEL_INIT.read_text(encoding="utf-8")
    assert "ScopeBounds" in claim_model_init
    assert "Applicability" in claim_model_init
    assert ".scope import" not in claim_model_init and "import scope\n" not in (
        claim_model_init
    ), "claim_model/__init__.py must not import the D6-retired scope module"

    # -- FROZEN (loud pending blockage): the legacy SQL gate path (item #1) and
    #    its equivalence harness (item #2) are deletable ONLY after the cross-repo
    #    cern-twiki IR re-authoring (consistency-checks-as-ir retire-sql-path /
    #    consistency-deployment-currency dep.twiki-ir-reauthor). Until then they
    #    stay, named here so the pending deletion is not silently dropped. WHEN
    #    that gate opens and these are deleted, flip these to absence assertions
    #    (blocker tier) and empty FROZEN_SQL_CHECK_ALLOWLIST.
    sql_gate = _SQL_GATE.read_text(encoding="utf-8")
    assert "FROZEN_SQL_CHECK_ALLOWLIST" in sql_gate and "candidate_sql" in sql_gate, (
        "sql_gate.py's frozen path is expected to remain until the cern-twiki "
        "gate opens; if you deleted it, promote this to an absence assertion and "
        "update the retirement inventory (roadmap #1/#2)"
    )
    assert _EQUIVALENCE.exists(), (
        "consistency/equivalence.py is frozen (imported by the cern-twiki parity "
        "witnesses) until retire-sql-path lands; if you deleted it, update the "
        "inventory and this gate"
    )
    print(
        "\n[single-engine/residue] deleted: rerun-post-publish alias, narrowing "
        "TODO skeleton, standalone scope.py; active: one typed applicability "
        "operator (#744); frozen (cern-twiki): sql_gate "
        "candidate_sql path, equivalence harness"
    )


# =========================================================================== #
# Guard 1 helpers — a bounded claim step, its facts, and an incumbent.          #
# =========================================================================== #
def _claim_envelope(keys: tuple[str, ...]) -> Any:
    return normalize_write_envelope(
        {
            "nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
            "claim_keys": list(keys),
        },
        source_name="producer",
    )


def _step(step_id: str, keys: tuple[str, ...]) -> ReuseStep:
    return ReuseStep(
        step_id=step_id,
        envelope=_claim_envelope(keys),
        producer_id=step_id,
        claim_predicates=(_PREDICATE,),
    )


def _facts(tag: str) -> CertificateDependencies:
    return CertificateDependencies(
        input_digests={"in": f"digest:{tag}"},
        operator_versions={"op": "v1"},
        model_prompt_keys={},
        envelope_fingerprint=f"env:{tag}",
        constraint_set={"chk": "cs:1"},
        generator_versions={"gen": "v1"},
        policy_fingerprint="pol:1",
        precedence_fingerprint="prec:1",
        competitor_bounds={"bound": tag},
        catalog_version_id="catver:1",
    )


def _incumbent() -> dict[str, Any]:
    return {"outcome": "selected", "basis": "source_authority"}


# =========================================================================== #
# Guard 1 (Postgres): the one-skip audit over a real scheduler-governed run.    #
# =========================================================================== #
psycopg = pytest.importorskip("psycopg")


@pytest.mark.postgres
def test_every_cone_skip_carries_a_certificate_over_a_real_run(
    clean_runtime: str,
) -> None:
    """A scheduler-governed publish with MIXED outcomes reaches a fixed point;
    read back from ``okg.reuse_certificates``, the skip step-set and the
    certificate step-set are a bijection (every skip certified, no stray
    certificate), and runs carry none. Falsifiable: deleting a certificate row
    makes the audit report the now-uncertified skip."""
    from okg.substrate.runtime.reuse_pass_workflow import run_reuse_pass_in_cycle

    dep = f"single-engine-audit-{uuid.uuid4().hex[:8]}"
    gen = "7"

    # Three reached steps, three outcomes on the resolved cone:
    #   carry     -> DEGENERATE skip (current facts byte-identical to prior)
    #   justified -> JUSTIFIED skip (the change is disjoint from its reach)
    #   recompute -> RUN (the change overlaps its reach)
    steps = {
        "carry": _step("carry", ("c1",)),
        "justified": _step("justified", ("j1",)),
        "recompute": _step("recompute", ("r1",)),
    }
    carry_facts = _facts("carry")
    current_facts = {
        "carry": carry_facts,
        "justified": _facts("justified"),
        "recompute": _facts("recompute"),
    }
    store = ReuseCertificateStore(deployment=dep)

    conn = psycopg.connect(clean_runtime)  # autocommit=False -> one transaction
    try:
        summary = run_reuse_pass_in_cycle(
            conn,
            deployment=dep,
            generation_id=gen,
            worklist_seed=["carry", "justified", "recompute"],
            steps=steps,
            checks=(),
            changed_terms={
                "justified": ChangedStateTerms(
                    buckets=frozenset({(_PREDICATE, "zzz")})
                ),
                "recompute": ChangedStateTerms(
                    buckets=frozenset({(_PREDICATE, "r1")})
                ),
            },
            current_facts=current_facts,
            prior_facts={"carry": carry_facts},
            prior_generation_id="prior",
            incumbent_selection={"carry": _incumbent(), "justified": _incumbent()},
            cert_store=store,
            checkpoint_writer=lambda _conn: gen,
        )
    finally:
        conn.close()

    result = summary["result"]
    outcomes = {o.step_id: o for o in result.outcomes}
    print(
        "\n[single-engine/one-skip] terminal=", result.terminal,
        "outcomes=", {s: (o.action, o.cause) for s, o in outcomes.items()},
    )
    assert result.reached_fixed_point, summary

    # the mixed shape actually materialized (two skip KINDS + a run).
    assert outcomes["carry"].cause == CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED
    assert outcomes["justified"].cause == CAUSE_SKIP_JUSTIFIED
    assert outcomes["recompute"].cause == CAUSE_EFFECTS_OVERLAP
    skips = list(result.skips)
    assert {o.step_id for o in skips} == {"carry", "justified"}

    # ---- THE AUDIT: read the persisted certificates and JOIN against the run's
    #      skip outcomes. Every skip on the cone is certificate-backed; no
    #      certificate is stray; no run carries one.
    with psycopg.connect(clean_runtime, autocommit=True) as read_conn:
        persisted = store.for_generation(read_conn, gen)

    missing = skips_without_certificates(skips, persisted)
    stray = certificates_without_skip(skips, persisted)
    assert missing == [], f"cone skips with NO certificate (single-skip-path violated): {missing}"
    assert stray == [], f"certificates with no answering skip: {stray}"
    assert {c.step_id for c in persisted} == {"carry", "justified"}
    # a run never persisted a certificate.
    assert "recompute" not in {c.step_id for c in persisted}
    # each skip outcome's recorded digest matches the persisted row.
    by_step = {c.step_id: c for c in persisted}
    for o in skips:
        assert o.certificate_digest == by_step[o.step_id].digest
        # every certificate enumerates the full closed dependency-class set.
        assert set(by_step[o.step_id].dependencies.to_json_dict()) == set(
            DEPENDENCY_CLASSES
        )

    # ---- FALSIFIABILITY: removing a certificate WRITE makes the audit fail.
    victim = sorted({o.step_id for o in skips})[0]
    with psycopg.connect(clean_runtime, autocommit=True) as del_conn, \
            del_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.reuse_certificates "
            "WHERE deployment_name = %s AND generation_id = %s AND step_id = %s",
            (dep, gen, victim),
        )
    with psycopg.connect(clean_runtime, autocommit=True) as read_conn:
        after = store.for_generation(read_conn, gen)
    assert skips_without_certificates(skips, after) == [victim], (
        "deleting a certificate row must surface the now-uncertified skip — the "
        "audit is not vacuous"
    )
    print(
        f"[single-engine/one-skip] audit clean; falsifiability: deleting "
        f"{victim!r}'s certificate surfaced it as uncertified"
    )


def test_out_of_cone_artifacts_are_enumerated_explicitly() -> None:
    """The single-mechanism claim scopes to the resolved state's dependency cone.
    The artifacts OUTSIDE it are ENUMERATED with reasons (never silently exempt),
    so an unlisted artifact cannot be quietly assumed out of scope."""
    assert set(OUTSIDE_CONE) == {
        "sources", "generic_enrichers", "native_views", "rollups", "embeddings",
    }, OUTSIDE_CONE
    for family, reason in OUTSIDE_CONE.items():
        assert isinstance(reason, str) and len(reason) > 10, family
    print("\n[single-engine/out-of-cone]", sorted(OUTSIDE_CONE))

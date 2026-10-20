"""IR-authored consistency checks — the successor to the SQL-blob contract.

PACT ``consistency-checks-as-ir``, task.ir.check-contract. A deployment
declares a check as a **transform-IR program** instead of raw
``candidate_sql``/``coverage_sql`` strings:

- **Read-only by construction.** The program's ``Effect`` and the IR
  validator's effect-typing rules reject write-shaped programs at load — no
  token scanning.
- **Structural identity.** Every hash in the check's
  :class:`ConsistencyCheckDefinition` derives from the program's
  content-addressed ``checksum()`` (canonical JSON of the AST) plus the
  declared surface bindings — never hand-pinned.
- **Deployment-registered surfaces.** A check composes with deployment
  relations (claim tables, view outputs) by declaring extra surfaces that
  register alongside the substrate's Chronos surfaces — the IR analogue of the
  SQL path's ``relation_inputs``.
- **Observation emission as program attrs.** The program projects offending
  rows; ``attrs["check"]`` declares how those rows become observations
  (predicate, participant roles, values/evidence columns) so the emission
  contract participates in the checksum.

deployment.yaml shape::

    ir_consistency_checks:
      - name: twiki_snapshot_hygiene_ir
        check_id: cern_twiki.snapshot_hygiene.ir.v1
        predicate_family: cern_twiki_snapshot_hygiene
        confidence_type: deterministic_boolean
        program_ref: cern_twiki.ir_checks.missing_title_program
        surfaces:                      # optional deployment relations
          claim_rows:
            binding: okg.derived_rel_claims
            kind: live

Scope of this module (task 1): the authoring contract, loader, validation,
identity derivation, and lower/execute helpers used by tests. Publisher wiring
(pinned Chronos session execution + observation-lifecycle emission) is
task.ir.session-executor; derived locality is task.ir.derived-locality.
"""
from __future__ import annotations

import importlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row

from okg.substrate import fastyaml
from okg.substrate.consistency.observations import (
    ConfidenceType,
    ConsistencyCheckDefinition,
)
from okg.substrate.derived_artifacts.relations import stable_hash
from okg.substrate.errors import SubstrateError
from okg.substrate.transform import (
    Effect,
    FunctionRegistry,
    OpKind,
    Registries,
    Surface,
    SurfaceKind,
    TransformProgram,
    chronos_surface_registry,
    lower_query,
)

IR_CHECK_EVALUATOR_REF = "okg.evaluator.ir.v1"
OFFENDING_OUTPUT = "offending"

# The claim-state kept tables a check may declare as a read surface: the
# post-merge effective claims (``okg.claim_effective``) and the
# canonical/contested/superseded projection (``okg.claim_projection``), produced
# by the claim-model derivations (PACT consistency-claim-graph-binding,
# req.binding.resolution-visible). These are Chronos-REGISTERED interval kept
# tables: they carry NO generation column, so their rows are generation-scoped
# ONLY by the pinned Chronos checkout's interval visibility (``live_lo <= bp <
# live_hi``), exactly as ``okg.graph_nodes`` is. A read that does NOT go through
# the run's pinned checkout session therefore sees the physical table's rows
# across ALL generations (or a stale un-versioned copy) — the silent
# generation-mixing this repo bans. Recognition is by the KEPT TABLE the surface
# binding reads, never the deployment-chosen surface name.
CLAIM_EFFECTIVE_BINDING = "okg.claim_effective"
CLAIM_PROJECTION_BINDING = "okg.claim_projection"
CLAIM_STATE_SURFACE_BINDINGS: frozenset[str] = frozenset(
    {CLAIM_EFFECTIVE_BINDING, CLAIM_PROJECTION_BINDING}
)
_CLAIM_STATE_TABLE_RE = re.compile(
    r"\b(okg\.claim_effective|okg\.claim_projection)\b"
)


def claim_state_tables_in_binding(binding: str) -> frozenset[str]:
    """The claim-state kept tables a surface ``binding`` reads (empty if none).

    Matches a bare-table binding (``okg.claim_projection``) or a column-renaming
    subquery binding that reads ``FROM okg.claim_projection`` — the same
    subquery-binding idiom the derivations use. Recognition is by the kept table
    the binding names, so a deployment may call the surface anything.
    """
    return frozenset(m.group(1) for m in _CLAIM_STATE_TABLE_RE.finditer(binding or ""))

_ALLOWED_ENTRY_KEYS = {
    "name",
    "type",
    "check_id",
    "predicate_family",
    "confidence_type",
    "program_ref",
    "surfaces",
    "severity_policy",
    "coverage_contract",
    "description",
}
_ALLOWED_SURFACE_KEYS = {"binding", "kind", "pinned"}
_SURFACE_KINDS = {
    "live": SurfaceKind.LIVE,
    "catalog_policy": SurfaceKind.CATALOG_POLICY,
    "index": SurfaceKind.INDEX,
    "fact_log": SurfaceKind.FACT_LOG,
}
_CONFIDENCE_TYPES = {
    "deterministic_boolean",
    "calibrated_probability",
    "thresholded_score",
    "ordinal_label",
}
_REQUIRED_CHECK_ATTRS = ("predicate_key", "normalized_predicate", "participant_roles")


class IrCheckLoadError(SubstrateError):
    code = "ir_consistency_check_load_error"


@dataclass(frozen=True)
class IrCheckSurface:
    """One deployment-declared surface the check's program may read."""

    name: str
    binding: str
    kind: SurfaceKind = SurfaceKind.LIVE
    pinned: bool = True

    def to_hash_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "binding": self.binding,
            "kind": self.kind.value,
            "pinned": self.pinned,
        }


@dataclass(frozen=True)
class IrConsistencyCheckSpec:
    """One IR-authored consistency check, loaded and validated."""

    name: str
    check_id: str
    predicate_family: str
    confidence_type: ConfidenceType
    program: TransformProgram
    program_ref: str
    surfaces: tuple[IrCheckSurface, ...] = ()
    severity_policy: Mapping[str, Any] = field(default_factory=dict)
    coverage_contract: Mapping[str, Any] = field(default_factory=dict)

    # ----- identity: every hash derives from the program structure -------- #

    def definition(self, deployment_name: str) -> ConsistencyCheckDefinition:
        program_checksum = self.program.checksum()
        surfaces_payload = [s.to_hash_payload() for s in self.surfaces]
        candidate_rule_hash = stable_hash({
            "kind": "ir_consistency_candidate_rule_v1",
            "check_id": self.check_id,
            "program_checksum": program_checksum,
            "surfaces": surfaces_payload,
        })
        evaluator_hash = stable_hash({
            "kind": "ir_consistency_evaluator_v1",
            "evaluator_ref": IR_CHECK_EVALUATOR_REF,
            "program_checksum": program_checksum,
        })
        compute_hash = stable_hash({
            "kind": "ir_consistency_compute_v1",
            "check_id": self.check_id,
            "program_checksum": program_checksum,
            "surfaces": surfaces_payload,
        })
        output_schema_hash = stable_hash({
            "kind": "ir_consistency_output_schema_v1",
            "outputs": [
                "consistency_candidates",
                "observation_events",
                "observation_current",
                "consistency_candidate_coverage",
            ],
            "confidence_type": self.confidence_type,
            "check_attrs": dict(self.program.attrs.get("check", {})),
        })
        renderer_hash = stable_hash({
            "kind": "ir_consistency_renderer_v1",
            "renderer": "okg.renderer.observation.v1",
        })
        dirty_rule_hash = stable_hash({
            "kind": "ir_consistency_dirty_rule_v1",
            "program_checksum": program_checksum,
            "coverage_contract": dict(self.coverage_contract),
        })
        return ConsistencyCheckDefinition(
            check_id=self.check_id,
            deployment_name=deployment_name,
            predicate_family=self.predicate_family,
            candidate_rule_hash=candidate_rule_hash,
            evaluator_ref=IR_CHECK_EVALUATOR_REF,
            evaluator_hash=evaluator_hash,
            confidence_type=self.confidence_type,
            compute_hash=compute_hash,
            output_schema_hash=output_schema_hash,
            renderer_hash=renderer_hash,
            dirty_rule_hash=dirty_rule_hash,
            coverage_contract=dict(self.coverage_contract),
            severity_policy=dict(self.severity_policy),
        )

    # ----- claim-state surface recognition -------------------------------- #

    def claim_state_binding_tables(self) -> frozenset[str]:
        """The claim-state kept tables this check's surfaces read (empty set if
        none). The pin's precondition: the executor must scope each of these to
        the run's pinned generation through the checkout session."""
        tables: set[str] = set()
        for surface in self.surfaces:
            tables |= claim_state_tables_in_binding(surface.binding)
        return frozenset(tables)

    def claim_state_surface_names(self) -> frozenset[str]:
        """The declared surface NAMES whose binding reads the claim-state kept
        tables — the input :func:`derive_frontier_plan` needs to record the
        claim-specific FULL reason (recognition is by binding, not name)."""
        return frozenset(
            surface.name
            for surface in self.surfaces
            if claim_state_tables_in_binding(surface.binding)
        )

    @property
    def reads_claim_state(self) -> bool:
        return bool(self.claim_state_binding_tables())

    # ----- execution helpers (tests + later executor) --------------------- #

    def registries(self) -> Registries:
        """Chronos surface registry + this check's deployment surfaces."""
        registry = chronos_surface_registry()
        for surface in self.surfaces:
            try:
                registry.register(Surface(
                    surface.name,
                    surface.kind,
                    surface.binding,
                    pinned=surface.pinned,
                ))
            except ValueError as exc:
                # e.g. shadowing a substrate surface name (nodes_live, ...)
                raise IrCheckLoadError(
                    f"check {self.check_id!r}: surface {surface.name!r} "
                    f"cannot register: {exc}",
                    data={"check_id": self.check_id, "surface": surface.name},
                    fix_hint="rename the deployment surface; substrate surface "
                    "names cannot be rebound",
                ) from exc
        return Registries(surfaces=registry, functions=FunctionRegistry())

    def lower(self) -> tuple[str, dict]:
        """Lower the program to (sql, params) over the registered surfaces."""
        return lower_query(self.program, self.registries())

    def run_rows(self, conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
        """Execute the lowered program read-only and return offending rows.

        Direct-connection execution for tests and ad-hoc runs; the publisher
        path (task.ir.session-executor) executes through the pinned Chronos
        checkout session instead.

        Refused for a check that reads the claim-state surface: a raw-connection
        read does NOT apply the checkout's interval visibility, so it would read
        the claim-state kept tables' rows across every generation (or a stale
        un-versioned copy) — the silent generation-mixing this repo bans. Such a
        check MUST run through the pinned executor
        (:class:`~okg.substrate.consistency.ir_executor.IrPostPublishCheck`).
        """
        if self.reads_claim_state:
            tables = sorted(self.claim_state_binding_tables())
            raise IrCheckLoadError(
                f"check {self.check_id!r} reads the claim-state surface(s) "
                f"{tables}; run_rows is a raw-connection (UNPINNED) read that "
                "would mix generations. Execute through the pinned Chronos "
                "checkout session (IrPostPublishCheck.evaluate), never run_rows.",
                data={"check_id": self.check_id, "claim_state_tables": tables},
                fix_hint="run this check via IrPostPublishCheck.evaluate",
            )
        sql, params = self.lower()
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]


def load_ir_consistency_checks(
    deployment_dir: Path | str,
) -> list[IrConsistencyCheckSpec]:
    """Load and validate ``deployment.yaml``'s ``ir_consistency_checks`` block."""
    deployment_dir = Path(deployment_dir)
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return []
    with manifest_path.open() as f:
        manifest = fastyaml.safe_load(f) or {}
    entries = manifest.get("ir_consistency_checks") or []
    if not entries:
        return []
    if not isinstance(entries, list):
        raise IrCheckLoadError(
            f"{manifest_path}: 'ir_consistency_checks' must be a list",
            data={"path": str(manifest_path)},
        )
    specs: list[IrConsistencyCheckSpec] = []
    seen_check_ids: dict[str, int] = {}
    for idx, raw in enumerate(entries):
        spec = _parse_entry(raw, where=f"{manifest_path}:ir_consistency_checks[{idx}]")
        if spec.check_id in seen_check_ids:
            raise IrCheckLoadError(
                f"{manifest_path}: duplicate check_id {spec.check_id!r} "
                f"(entries {seen_check_ids[spec.check_id]} and {idx}); the "
                "observation lifecycle is keyed by check_id",
                data={"path": str(manifest_path), "check_id": spec.check_id},
            )
        seen_check_ids[spec.check_id] = idx
        specs.append(spec)
    return specs


def _parse_entry(raw: Any, *, where: str) -> IrConsistencyCheckSpec:
    if not isinstance(raw, Mapping):
        raise IrCheckLoadError(
            f"{where}: check entry must be a mapping",
            data={"where": where},
        )
    extras = set(raw) - _ALLOWED_ENTRY_KEYS
    if extras:
        raise IrCheckLoadError(
            f"{where}: unknown keys {sorted(extras)}",
            data={"where": where, "extras": sorted(extras)},
        )
    check_type = raw.get("type", "transform_ir")
    if check_type != "transform_ir":
        raise IrCheckLoadError(
            f"{where}: unsupported check type {check_type!r}",
            data={"where": where, "type": check_type},
            fix_hint="use type: transform_ir",
        )
    confidence_type = _required_str(raw, "confidence_type", where=where)
    if confidence_type not in _CONFIDENCE_TYPES:
        raise IrCheckLoadError(
            f"{where}: unsupported confidence_type {confidence_type!r}",
            data={"where": where, "confidence_type": confidence_type},
        )
    surfaces = _parse_surfaces(raw.get("surfaces"), where=where)
    program_ref = _required_str(raw, "program_ref", where=where)
    program = _resolve_program(program_ref, where=where)
    spec = IrConsistencyCheckSpec(
        name=_required_str(raw, "name", where=where),
        check_id=_required_str(raw, "check_id", where=where),
        predicate_family=_required_str(raw, "predicate_family", where=where),
        confidence_type=confidence_type,  # type: ignore[arg-type]
        program=program,
        program_ref=program_ref,
        surfaces=surfaces,
        severity_policy=_mapping(raw, "severity_policy", where=where),
        coverage_contract=_mapping(raw, "coverage_contract", where=where),
    )
    _validate_program(spec, where=where)
    return spec


def _resolve_program(program_ref: str, *, where: str) -> TransformProgram:
    module_path, _, attr = program_ref.rpartition(".")
    if not module_path:
        raise IrCheckLoadError(
            f"{where}: program_ref must be a dotted module path, got {program_ref!r}",
            data={"where": where, "program_ref": program_ref},
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise IrCheckLoadError(
            f"{where}: could not import module {module_path!r}",
            data={"where": where, "program_ref": program_ref},
            fix_hint=f"check that {module_path} is importable from the deployment",
        ) from exc
    try:
        factory = getattr(module, attr)
    except AttributeError as exc:
        raise IrCheckLoadError(
            f"{where}: module {module_path!r} has no attribute {attr!r}",
            data={"where": where, "program_ref": program_ref},
        ) from exc
    program = factory() if callable(factory) else factory
    if not isinstance(program, TransformProgram):
        raise IrCheckLoadError(
            f"{where}: {program_ref!r} did not produce a TransformProgram "
            f"(got {type(program).__name__})",
            data={"where": where, "program_ref": program_ref},
        )
    # Snapshot via a serialization round-trip: TransformProgram is mutable and
    # a factory may hand out a shared module-level singleton — without the
    # copy, a post-load mutation would silently change definition() hashes on
    # a program that was validated in a different state.
    return TransformProgram.from_dict(program.to_dict())


def _validate_program(spec: IrConsistencyCheckSpec, *, where: str) -> None:
    program = spec.program
    # Effect typing: a consistency check is READ_ONLY by construction. The IR
    # validator enforces the effect/guard rules; a declared write effect or any
    # DELTA op is rejected here regardless of what the validator tolerates.
    if program.effect is not Effect.READ_ONLY:
        raise IrCheckLoadError(
            f"{where}: program {program.program_id!r} declares effect "
            f"{program.effect.value!r}; consistency checks must be READ_ONLY",
            data={"where": where, "program_id": program.program_id},
        )
    for _block, binding in program.iter_ops():
        if binding.op.kind is OpKind.DELTA:
            raise IrCheckLoadError(
                f"{where}: program {program.program_id!r} contains a DELTA op; "
                "consistency checks never mutate",
                data={"where": where, "program_id": program.program_id},
            )
    if OFFENDING_OUTPUT not in program.outputs:
        raise IrCheckLoadError(
            f"{where}: program {program.program_id!r} must declare the "
            f"{OFFENDING_OUTPUT!r} output",
            data={"where": where, "program_id": program.program_id},
        )
    check_attrs = program.attrs.get("check")
    if not isinstance(check_attrs, Mapping):
        raise IrCheckLoadError(
            f"{where}: program {program.program_id!r} must carry attrs['check'] "
            "(the observation-emission contract)",
            data={"where": where, "program_id": program.program_id},
        )
    missing = [k for k in _REQUIRED_CHECK_ATTRS if not check_attrs.get(k)]
    if missing:
        raise IrCheckLoadError(
            f"{where}: attrs['check'] is missing {missing}",
            data={"where": where, "missing": missing},
        )
    roles = check_attrs.get("participant_roles")
    if not isinstance(roles, Mapping) or not roles:
        raise IrCheckLoadError(
            f"{where}: attrs['check']['participant_roles'] must be a non-empty "
            "mapping of role -> projected column",
            data={"where": where},
        )
    # A finding's identity must be a pure function of (check, satisfying
    # assignment): the emission contract MUST name the projected columns that
    # constitute the assignment. There is no projection-derived default — the
    # executor's old alphabetical whole-row fallback was deleted (PACT
    # consistency-finding-identity, req.identity.declared-or-refused), so an
    # undeclared contract is an authoring error refused at load, per check.
    value_columns = check_attrs.get("normalized_value_columns")
    if (
        not isinstance(value_columns, (list, tuple))
        or not value_columns
        or not all(isinstance(c, str) and c for c in value_columns)
    ):
        raise IrCheckLoadError(
            f"{where}: check {spec.check_id!r} is missing "
            "attrs['check']['normalized_value_columns'] — a non-empty list of "
            "projected column names that constitute the finding's satisfying "
            "assignment; finding identity is never derived from the SELECT "
            "projection",
            data={
                "where": where,
                "check_id": spec.check_id,
                "missing_field": "normalized_value_columns",
            },
            fix_hint=(
                "declare normalized_value_columns (and finding_scope_columns "
                "when one subject can hold several findings) in the program's "
                "attrs['check']"
            ),
        )
    scope_columns = check_attrs.get("finding_scope_columns")
    if scope_columns is not None and (
        not isinstance(scope_columns, (list, tuple))
        or not scope_columns
        or not all(isinstance(c, str) and c for c in scope_columns)
    ):
        raise IrCheckLoadError(
            f"{where}: attrs['check']['finding_scope_columns'] must be a "
            "non-empty list of projected column names",
            data={"where": where},
        )
    for triage_attr in ("payload_column", "evidence_column"):
        value = check_attrs.get(triage_attr)
        if value is not None and (not isinstance(value, str) or not value):
            raise IrCheckLoadError(
                f"{where}: attrs['check'][{triage_attr!r}] must be a projected "
                "column name",
                data={"where": where},
            )
    # Full IR validation (structure, effect/guard typing, surface resolution)
    # against the check's actual registries.
    errors = program.validate(spec.registries())
    if errors:
        raise IrCheckLoadError(
            f"{where}: program {program.program_id!r} failed IR validation: "
            + "; ".join(str(e) for e in errors),
            data={
                "where": where,
                "program_id": program.program_id,
                "errors": [str(e) for e in errors],
            },
        )


def _parse_surfaces(raw: Any, *, where: str) -> tuple[IrCheckSurface, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, Mapping):
        raise IrCheckLoadError(
            f"{where}: 'surfaces' must be a mapping",
            data={"where": where},
        )
    out: list[IrCheckSurface] = []
    for name, spec in raw.items():
        if not isinstance(spec, Mapping):
            raise IrCheckLoadError(
                f"{where}: surface {name!r} must be a mapping",
                data={"where": where, "surface": str(name)},
            )
        extras = set(spec) - _ALLOWED_SURFACE_KEYS
        if extras:
            raise IrCheckLoadError(
                f"{where}: surface {name!r} has unknown keys {sorted(extras)}",
                data={"where": where, "surface": str(name), "extras": sorted(extras)},
            )
        kind_name = str(spec.get("kind", "live"))
        if kind_name not in _SURFACE_KINDS:
            raise IrCheckLoadError(
                f"{where}: surface {name!r} has unknown kind {kind_name!r}",
                data={"where": where, "surface": str(name), "kind": kind_name},
                fix_hint=f"use one of {sorted(_SURFACE_KINDS)}",
            )
        binding = spec.get("binding")
        if not isinstance(binding, str) or not binding.strip():
            raise IrCheckLoadError(
                f"{where}: surface {name!r} needs a non-empty 'binding'",
                data={"where": where, "surface": str(name)},
            )
        out.append(IrCheckSurface(
            name=str(name),
            binding=binding,
            kind=_SURFACE_KINDS[kind_name],
            pinned=bool(spec.get("pinned", True)),
        ))
    return tuple(sorted(out, key=lambda s: s.name))


def _required_str(raw: Mapping[str, Any], key: str, *, where: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise IrCheckLoadError(
            f"{where}: {key!r} must be a non-empty string",
            data={"where": where, "field": key},
        )
    return value


def _mapping(raw: Mapping[str, Any], key: str, *, where: str) -> dict[str, Any]:
    value = raw.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise IrCheckLoadError(
            f"{where}: {key!r} must be a mapping",
            data={"where": where, "field": key},
        )
    return dict(value)


__all__ = [
    "CLAIM_EFFECTIVE_BINDING",
    "CLAIM_PROJECTION_BINDING",
    "CLAIM_STATE_SURFACE_BINDINGS",
    "IR_CHECK_EVALUATOR_REF",
    "IrCheckLoadError",
    "IrCheckSurface",
    "IrConsistencyCheckSpec",
    "claim_state_tables_in_binding",
    "load_ir_consistency_checks",
]

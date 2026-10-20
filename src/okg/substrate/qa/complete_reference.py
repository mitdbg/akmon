"""Complete-execution reference and dependency-baseline runners.

PACT ``consistency-complete-equivalence`` (task.harness.complete-runner +
task.harness.dep-baseline); normative semantics in
docs/develop/architecture/consistency.md §9.7. This module builds the two
from-scratch execution modes the same-result contract measures against:

* :func:`run_complete_reference` — executes the ENTIRE declared pipeline for a
  deployment (catalog + facts + real publish cycle: derivations, checks,
  repair selection) into a FRESH scratch scaffold, under REPLAY of recorded
  model outcomes, and returns the reference resolved state for a pinned
  snapshot. It provably reads no prior maintenance state (two cold-start runs
  from the same pin are byte-equal). An operator failure aborts it loudly with
  no partial reference.
* :func:`run_dependency_baseline` — the same update with ALL reuse disabled
  (every maintained derivation forced full via ``decide_maintain_mode``'s
  ``force_full`` branch; checks run full-sweep). Its resolved state must equal
  the complete reference's; it is the paper's ``Dep_V``.

Design stance (all guards loud + typed, no silent fallback):

* The scratch DSN MUST name an ``okg_test``/``okg_test_*`` database AND hold no
  prior published generation. "Provably reads no prior maintenance state" is a
  refusal, not a promise: a dirty scratch DB is rejected before any work.
* An incomplete pin — no fact payload, or an unreadable outcome source —
  refuses to start.
* The run consults ONLY the pinned facts + the pinned recorded outcomes; it
  never reads the source deployment's derived/maintenance state.
* A replay miss is a recorded, NAMED :class:`ReplayExclusion` surfaced on the
  result — never a fresh model sample.

The canonical state dump (:func:`_dump_state`) is PRIVATE to this module. The
repo's single equivalence *differ* is the sibling module authored by
``task.harness.differ`` (``consistency/equivalence_differ.py``); the live-series
task consolidates. This module's dump exists only so the cold-start-determinism
and baseline-equals-reference witnesses can compare two of *its own* runs
byte-for-byte.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from okg.substrate.derived_artifacts.claim_effective_derivation import (
    ROW_KIND_EFFECTIVE,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    ROW_KIND_RESOLUTION,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    FULL_RECOMPUTE_CAUSES,
    ForceInputs,
)
from okg.substrate.llm.outcomes import (
    OutcomeKey,
    OutcomeRecorder,
    OutcomeStore,
    ReplayExclusion,
    ReplayMissError,
    canonical_input_hash,
    prompt_content_hash,
)

logger = logging.getLogger(__name__)

CLAIM_EFFECTIVE_TABLE = "okg.claim_effective"
CLAIM_PROJECTION_TABLE = "okg.claim_projection"

# State-dump column projections. Every column here is a deterministic function
# of the pinned inputs; run-varying columns (generation ids, ordinals, run/event
# UUIDs, timestamps, the sentinel cadence counter) are DELIBERATELY excluded so
# two cold-start runs of the SAME pin dump byte-identically.
_EFFECTIVE_DUMP_COLUMNS = (
    "row_key", "predicate", "normalized_key", "value", "scope", "scope_state",
    "claim_semantics", "source_id", "provenance", "applied_correction_id",
    "stale_correction_review", "reason",
)
_RESOLUTION_DUMP_COLUMNS = (
    "row_key", "predicate", "bucket_key", "constraint_id", "constraint_kind",
    "outcome", "locality", "member_claim_ids", "detail", "winner_claim_id",
    "basis", "ranked_claim_ids", "superseded_claim_ids",
)
_REPAIR_DUMP_COLUMNS = (
    "bucket_key", "outcome", "winner_claim_id", "generator_id",
    "generator_version", "basis", "distrusted_claim_ids", "asserted_claim_ids",
    "override_claim_ids", "unresolved_reason", "selection_digest",
)
# observation_current identity + classification only. The volatile
# latest_generation_id / latest_run_id / latest_event_id / updated_at columns
# are excluded; observation_id is the per-assignment finding identity
# (deterministic hash — no generation component; observations.observation_id_for).
_FINDING_DUMP_COLUMNS = (
    "observation_id", "check_id", "candidate_id", "finding_status",
    "evaluation_status", "input_scope", "predicate_key", "normalized_predicate",
    "normalized_values", "participant_roles", "participant_anchors", "severity",
    "confidence", "confidence_type",
)


# --------------------------------------------------------------------------- #
# Errors — every guard trip / abort is loud and typed.                        #
# --------------------------------------------------------------------------- #
class CompleteReferenceError(RuntimeError):
    """Base for complete-reference / dependency-baseline failures."""


class IncompleteSnapshotPin(CompleteReferenceError):
    """The snapshot pin is incomplete (no facts, or an unreadable outcome
    source). Refuses to start rather than execute against a partial pin."""


class ScratchGuardViolation(CompleteReferenceError):
    """The scratch DSN failed the okg_test_* naming rule or held a prior
    published generation. A reference execution proves it reads no prior
    maintenance state; a dirty scratch DB is a refusal, not a promise."""


class ReferenceExecutionAborted(CompleteReferenceError):
    """An operator failure aborted the execution; there is no partial
    reference. Carries the underlying cause when one exists."""


class HostHeadroomRefusal(CompleteReferenceError):
    """The host filesystem is below the deployment's declared blocker
    floor (PACT ``bound-host-storage-pressure``). Provisioning a scratch
    scaffold + running a full publish is a known big writer; starting it
    under blocker pressure is how the 2026-07-17 ENOSPC incidents wrote
    the disk to zero mid-operation. The message names the shortfall."""


# --------------------------------------------------------------------------- #
# Pinned inputs.                                                              #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OutcomeReplaySource:
    """The pinned recorded model outcomes a complete execution replays.

    ``rows`` are copied verbatim into the scratch ``okg.model_outcomes`` before
    the run, so replay consumes exactly what the caller pinned and nothing from
    any live deployment DB. Each row is a mapping over the ``okg.model_outcomes``
    columns (``key_digest``, ``step_id``, ``operator_version``, ``model_id``,
    ``prompt_hash``, ``input_hash``, ``content``; optional ``generation_id``).
    An empty ``rows`` is a valid, explicit pin ("this update has no model-backed
    step"); ``None`` as the whole source is an incomplete pin and refuses.
    """

    deployment: str
    rows: Sequence[Mapping[str, Any]] = ()

    @classmethod
    def from_dsn(
        cls, dsn: str, deployment: str,
    ) -> "OutcomeReplaySource":
        """Read a deployment's recorded outcomes from a DSN's
        ``okg.model_outcomes`` (the live-series path). Facts+outcomes in,
        nothing else: only the outcome bodies are read, never derived state."""
        from okg.substrate.db.connection import connect as db_connect

        try:
            with db_connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT key_digest, step_id, operator_version, model_id,
                           prompt_hash, input_hash, content, generation_id
                      FROM okg.model_outcomes
                     WHERE deployment_name = %s
                    """,
                    (deployment,),
                )
                cols = (
                    "key_digest", "step_id", "operator_version", "model_id",
                    "prompt_hash", "input_hash", "content", "generation_id",
                )
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001 - unreadable source => refuse
            from okg.substrate.db.dsn import redact, redact_text

            raise IncompleteSnapshotPin(
                f"outcome source unreadable at {redact(dsn)!r} for deployment "
                f"{deployment!r}: {redact_text(str(exc))}"
            ) from exc
        return cls(deployment=deployment, rows=rows)


@dataclass(frozen=True)
class OutcomeProbe:
    """A model-backed step exercised through the replay recorder.

    The fixture deployment has NO model-backed steps, so the publish cycle
    replays nothing; this probe is how the fixture witnesses the replay/exclusion
    contract end-to-end through the runner's result (spec §9.7 sanctions
    "exercising the OutcomeRecorder replay contract directly against the scratch
    store"). ``llm`` must be a client whose ``complete`` never has to run under
    replay — a client that RAISES on ``complete`` proves replay never samples
    fresh, on both a hit and a miss.
    """

    step_id: str
    operator_version: str
    model_id: str
    system_prompt: str
    user_prompt: str
    typed_input: Any
    llm: Any
    fragment_id: str = "probe"
    max_rows: int = 64

    def key(self) -> OutcomeKey:
        return OutcomeKey(
            step_id=self.step_id,
            operator_version=self.operator_version,
            model_id=self.model_id,
            prompt_hash=prompt_content_hash(self.system_prompt),
            input_hash=canonical_input_hash(self.typed_input),
        )


# --------------------------------------------------------------------------- #
# Structured result — canonical-JSON serializable for the live-series task.   #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class StepMode:
    """One maintained derivation's recorded mode + cause for the run."""

    derivation_id: str
    status: str
    mode: str | None
    full_reason: str | None
    dirty_bucket_count: int | None
    rows_written: int | None
    rows_deleted: int | None

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "derivation_id": self.derivation_id,
            "status": self.status,
            "mode": self.mode,
            "full_reason": self.full_reason,
            "dirty_bucket_count": self.dirty_bucket_count,
            "rows_written": self.rows_written,
            "rows_deleted": self.rows_deleted,
        }


@dataclass(frozen=True)
class CheckCoverage:
    """One post-publish check's coverage-row scope facts for the run."""

    check_id: str
    scoped: bool
    coverage_complete: bool
    scope_subject_count: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "check_id": self.check_id,
            "scoped": self.scoped,
            "coverage_complete": self.coverage_complete,
            "scope_subject_count": self.scope_subject_count,
        }


@dataclass(frozen=True)
class CompleteReferenceResult:
    """The reference (or baseline) resolved state + execution evidence.

    ``state_dump`` is the four-family canonical dump; ``state_dump_digest`` is
    its sha256 (two byte-equal dumps share a digest). ``exclusions`` names every
    replay miss; ``excluded`` is True iff any key could not replay (the update is
    then EXCLUDED from equality measurement, never counted as agreement).
    """

    mode: str  # "complete_reference" | "dependency_baseline"
    deployment: str
    generation_id: str
    compat_generation_id: int
    reuse_disabled: bool
    derivation_modes: dict[str, StepMode]
    check_coverage: list[CheckCoverage]
    exclusions: list[ReplayExclusion]
    probe_replayed_content: str | None
    state_dump: dict[str, Any]
    state_dump_digest: str

    @property
    def excluded(self) -> bool:
        return bool(self.exclusions)

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "deployment": self.deployment,
            "generation_id": str(self.generation_id),
            "compat_generation_id": int(self.compat_generation_id),
            "reuse_disabled": self.reuse_disabled,
            "excluded": self.excluded,
            "derivation_modes": {
                k: v.to_canonical_dict()
                for k, v in sorted(self.derivation_modes.items())
            },
            "check_coverage": [
                c.to_canonical_dict()
                for c in sorted(self.check_coverage, key=lambda c: c.check_id)
            ],
            "exclusions": [
                {
                    "step_id": e.step_id,
                    "key_digest": e.key_digest,
                    "fragment_id": e.fragment_id,
                }
                for e in sorted(
                    self.exclusions,
                    key=lambda e: (e.step_id, e.key_digest, e.fragment_id),
                )
            ],
            "state_dump": self.state_dump,
            "state_dump_digest": self.state_dump_digest,
        }

    def to_canonical_json(self) -> str:
        return _canonical_json(self.to_canonical_dict())


# --------------------------------------------------------------------------- #
# Canonicalization helpers.                                                   #
# --------------------------------------------------------------------------- #
def _canonical_json(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return str(value)


def _canon_cell(value: Any) -> Any:
    """Canonicalize a single DB cell for the dump.

    JSONB comes back already parsed (dict/list); leave it structural so the
    outer canonical-JSON sorts keys. NUMERIC comes back as ``Decimal`` — render
    as a string so the digest is driver-stable. Everything else passes through.
    """
    if isinstance(value, Decimal):
        return str(value)
    return value


# --------------------------------------------------------------------------- #
# Scratch-DB guards + provisioning.                                           #
# --------------------------------------------------------------------------- #
def _scratch_dbname(dsn: str) -> str:
    import psycopg

    return psycopg.conninfo.conninfo_to_dict(dsn).get("dbname") or ""


def _assert_scratch_dsn_named_safely(dsn: str) -> str:
    """The scratch DSN must name ``okg_test`` or ``okg_test_*`` (the conftest
    ephemeral-DB naming rule). Anything else is refused — a reference execution
    never provisions/mutates a DB that is not provably a throwaway."""
    dbname = _scratch_dbname(dsn)
    if dbname == "okg_test" or dbname.startswith("okg_test_"):
        return dbname
    raise ScratchGuardViolation(
        f"scratch DSN names database {dbname!r}; a complete-reference scratch "
        f"DB MUST be named 'okg_test' or 'okg_test_*' (the ephemeral-DB rule). "
        f"Refusing to provision/mutate a non-scratch database."
    )


def _preflight_scratch_host_headroom(deployment_dir: Path) -> None:
    """Refuse to provision the scratch scaffold under host blocker
    pressure (PACT ``bound-host-storage-pressure``).

    The scratch DB itself lives inside Postgres (often the Docker VM),
    which is backed by the host filesystem this preflight measures. The
    write size is not cheaply estimable up front, so the refusal keys on
    the blocker threshold alone (documented in the PACT task). Runs
    BEFORE any database connection.

    Enforcement requires the deployment dir to declare
    ``host_storage`` thresholds; an undeclared pair leaves enforcement
    explicitly un-armed with a loud log line (the standing degraded
    signal for undeclared thresholds lives in the scheduled doctor /
    dogfood status — never a silently-defaulted floor, P0).
    """
    import logging

    from okg.substrate.runtime import host_headroom

    verdict = host_headroom.big_writer_preflight(
        operation="complete-reference scratch provisioning",
        deployment=_deployment_name(Path(deployment_dir)),
        deployment_dir=Path(deployment_dir),
        target_path=Path(deployment_dir),
        needed_bytes=None,
    )
    if verdict.get("refuse"):
        raise HostHeadroomRefusal(str(verdict.get("message")))
    if verdict.get("status") == "misconfigured":
        logging.getLogger(__name__).warning(
            "complete-reference host-headroom enforcement is NOT armed: %s",
            verdict.get("message"),
        )


def _assert_zero_prior_generations(dsn: str, *, deployment: str) -> None:
    """Refuse a scratch DB that already holds a published generation.

    The chronos-native publisher records a published generation as a chronos
    *checkpoint* (queried by :meth:`ChronosGraphStore.list_generations`), NOT as
    an ``okg.graph_generations`` row — so this is the authoritative signal. Prior
    maintenance state would defeat the 'provably reads no prior state' contract;
    a non-empty scaffold is a refusal, not a promise. Both the target deployment
    and the ``default`` deployment are checked (a scratch DB should be pristine).
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        deployments = {deployment, "default"}
        for dep in deployments:
            published = store.list_generations(
                deployment_name=dep, status="published"
            )
            if published:
                raise ScratchGuardViolation(
                    f"scratch DB already holds {len(published)} published "
                    f"generation(s) for deployment {dep!r}; a complete reference "
                    f"requires an empty scaffold (it must provably read no prior "
                    f"maintenance state)."
                )


def _provision_scratch_schema(dsn: str) -> None:
    """Migrate the substrate schema into the scratch DB.

    A one-shot reference execution schedules NO cron-driven maintenance, so the
    ``pg_cron`` extension is dropped from the required set for the throwaway DB
    (mirroring the test harness's ephemeral-DB migrate) and the DBOS job runtime
    is removed. Bare ``migrate(dsn)`` runs ungated (no ``deployment_name``): the
    scratch DB matches no registered deployment, so the authority gate treats it
    as an ad-hoc clone.
    """
    from okg.substrate.db import migrate as _migrate_mod
    from okg.substrate.db.management import apply_remove_job_runtime_subsystem

    saved = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        _migrate_mod.migrate(dsn)
        apply_remove_job_runtime_subsystem(dsn, confirm=True)
    finally:
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(saved)
    # Register the scratch DB as ephemeral at creation: the janitor's drop
    # authority is this marker, never the name prefix alone.
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.db.management import stamp_ephemeral_database

    with db_connect(dsn, autocommit=True, connect_timeout=5) as conn:
        stamp_ephemeral_database(
            conn,
            registered_by="okg.substrate.qa.complete_reference",
            purpose="equivalence-harness scratch database",
        )


def _apply_composed_catalog(dsn: str, deployment_dir: Path) -> None:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        ensure_catalog_write_allowed,
    )

    composed = compose_catalog(deployment_dir)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=deployment_dir,
        command="qa complete reference catalog apply",
        writer_scope="deployment_root",
    )
    with db_connect(dsn, autocommit=True) as conn:
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(conn, composed, identity=identity)


def _apply_facts(dsn: str, facts: Sequence[Any], *, deployment: str) -> None:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(
            list(facts), source=deployment, strict_endpoints=False,
        )


def _copy_outcomes(dsn: str, source: OutcomeReplaySource) -> int:
    """Copy the pinned recorded outcomes into scratch ``okg.model_outcomes``.

    First-write-wins per key (``ON CONFLICT DO NOTHING``), matching
    :class:`OutcomeStore` semantics; a malformed row (missing a required
    column) refuses loudly rather than copying a partial replay store.
    """
    from okg.substrate.db.connection import connect as db_connect

    required = (
        "key_digest", "step_id", "operator_version", "model_id",
        "prompt_hash", "input_hash", "content",
    )
    copied = 0
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for index, row in enumerate(source.rows):
            missing = [c for c in required if row.get(c) in (None, "")]
            if missing:
                raise IncompleteSnapshotPin(
                    f"recorded outcome row {index} is missing required "
                    f"columns {missing}; the replay store pin is incomplete."
                )
            cur.execute(
                """
                INSERT INTO okg.model_outcomes
                  (deployment_name, key_digest, step_id, operator_version, model_id,
                   prompt_hash, input_hash, content, generation_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (deployment_name, key_digest) DO NOTHING
                """,
                (
                    source.deployment, row["key_digest"], row["step_id"],
                    row["operator_version"], row["model_id"], row["prompt_hash"],
                    row["input_hash"], row["content"], row.get("generation_id"),
                ),
            )
            copied += int(cur.rowcount or 0)
    return copied


# --------------------------------------------------------------------------- #
# Reuse-disabled registry (dependency baseline).                             #
# --------------------------------------------------------------------------- #
def _force_full_registry(registry: Any) -> Any:
    """Wrap every derivation row so it forces a FULL recompute.

    Overrides each row's ``ForceInputs`` to set ``force_full=True``, which
    ``decide_maintain_mode`` evaluates FIRST — the recorded cause is ``forced``.
    This is the "all reuse disabled" lever, expressed through the registry rows /
    ``decide_maintain_mode`` (no publisher change, no new ledger).
    """
    from okg.substrate.derived_artifacts.derivation_registry import (
        DerivationRegistry,
    )

    forced = DerivationRegistry()
    for row in registry.rows():
        orig_cycle = row.make_cycle_inputs
        orig_inputs = row.make_inputs

        def make_cycle(cycle: Any, _orig: Any = orig_cycle) -> ForceInputs:
            if _orig is not None:
                base = _orig(cycle)
            else:
                base = ForceInputs(
                    change_inputs=cycle,
                    force_full=True,
                    scope_available=cycle.dirty_scope is not None,
                )
            return base._replace(force_full=True)

        def make_inputs(
            dirty_scope: Any, force_full: bool, _orig: Any = orig_inputs,
        ) -> ForceInputs:
            return _orig(dirty_scope, True)._replace(force_full=True)

        forced.register(
            dataclasses.replace(
                row, make_cycle_inputs=make_cycle, make_inputs=make_inputs,
            )
        )
    return forced


# --------------------------------------------------------------------------- #
# State dump — PRIVATE to this module (the differ is task.harness.differ).    #
# --------------------------------------------------------------------------- #
def _dump_kept_table(
    checkout: Any, table: str, row_kind: str, columns: Sequence[str],
) -> list[dict[str, Any]]:
    cols = ", ".join(columns)
    rows = checkout.query(
        f"SELECT {cols} FROM {table} WHERE row_kind = :k", {"k": row_kind}
    )
    out = [
        {c: _canon_cell(row[c]) for c in columns} for row in rows
    ]
    out.sort(key=lambda r: _canonical_json(r))
    return out


def _dump_state(
    dsn: str,
    *,
    deployment: str,
    generation_ref: str,
    compat_generation_id: int,
) -> dict[str, Any]:
    """The four resolved-state families for a published generation.

    effective claims / resolution outcomes read the Chronos-registered kept
    tables through the generation checkout (so the pin is the checkout's
    interval visibility); repair selections + open findings read the relational
    tables keyed by the (compat) generation. All run-varying columns are
    excluded so two cold-start runs of the same pin dump byte-identically.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    dump: dict[str, Any] = {}
    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_ref)
        dump["effective_claims"] = _dump_kept_table(
            checkout, CLAIM_EFFECTIVE_TABLE, ROW_KIND_EFFECTIVE,
            _EFFECTIVE_DUMP_COLUMNS,
        )
        dump["resolution_outcomes"] = _dump_kept_table(
            checkout, CLAIM_PROJECTION_TABLE, ROW_KIND_RESOLUTION,
            _RESOLUTION_DUMP_COLUMNS,
        )

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {", ".join(_REPAIR_DUMP_COLUMNS)}
              FROM okg.repair_selections
             WHERE deployment_name = %s AND generation_id = %s
             ORDER BY bucket_key
            """,
            (deployment, str(compat_generation_id)),
        )
        dump["repair_selections"] = [
            {c: _canon_cell(v) for c, v in zip(_REPAIR_DUMP_COLUMNS, r)}
            for r in cur.fetchall()
        ]

        cur.execute(
            f"""
            SELECT {", ".join(_FINDING_DUMP_COLUMNS)}
              FROM okg.observation_current
             WHERE latest_generation_id = %s AND finding_status = 'open'
             ORDER BY observation_id
            """,
            (int(compat_generation_id),),
        )
        dump["open_findings"] = [
            {c: _canon_cell(v) for c, v in zip(_FINDING_DUMP_COLUMNS, r)}
            for r in cur.fetchall()
        ]

    return dump


def _read_check_coverage(
    dsn: str, *, compat_generation_id: int,
) -> list[CheckCoverage]:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT check_id, scoped, coverage_complete, scope_subject_count
              FROM okg.consistency_candidate_coverage
             WHERE target_generation_id = %s
             ORDER BY check_id, run_id
            """,
            (int(compat_generation_id),),
        )
        return [
            CheckCoverage(
                check_id=r[0],
                scoped=bool(r[1]),
                coverage_complete=bool(r[2]),
                scope_subject_count=int(r[3]),
            )
            for r in cur.fetchall()
        ]


def _state_dump_digest(dump: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(dump).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Runners.                                                                    #
# --------------------------------------------------------------------------- #
def _deployment_name(deployment_dir: Path) -> str:
    from okg.substrate import fastyaml

    manifest = deployment_dir / "deployment.yaml"
    if not manifest.is_file():
        raise IncompleteSnapshotPin(
            f"deployment.yaml not found under {deployment_dir!r}"
        )
    with manifest.open() as fh:
        doc = fastyaml.safe_load(fh) or {}
    name = doc.get("name") if isinstance(doc, dict) else None
    if not name:
        raise IncompleteSnapshotPin(
            f"deployment.yaml at {manifest!r} declares no name"
        )
    return str(name)


def _run_pipeline(
    source_snapshot: Any,
    *,
    deployment_dir: Path | str,
    scratch_dsn: str,
    outcome_store_source: OutcomeReplaySource | None,
    reuse_disabled: bool,
    outcome_probe: OutcomeProbe | None,
    derivation_registry_transform: Callable[[Any], Any] | None,
) -> CompleteReferenceResult:
    from okg.substrate.publisher.factory import build_deployment_publisher
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    # ---- pin validation (refuse an incomplete pin before any work) ----------
    if source_snapshot is None or isinstance(source_snapshot, str):
        raise IncompleteSnapshotPin(
            "source snapshot must be an explicit fact payload (a sequence of "
            "NodeFact); a DSN/implicit production read is not accepted — the "
            "caller pins exactly what complete execution sees."
        )
    facts = list(source_snapshot)
    if not facts:
        raise IncompleteSnapshotPin(
            "source snapshot has no facts; a complete reference cannot run "
            "against an empty pin."
        )
    if outcome_store_source is None:
        raise IncompleteSnapshotPin(
            "outcome_store_source is required (pass OutcomeReplaySource(rows=()) "
            "for a deployment with no model-backed step); a missing outcome "
            "source is an incomplete pin."
        )

    dep_dir = Path(deployment_dir)
    deployment = _deployment_name(dep_dir)

    # ---- scratch guards -----------------------------------------------------
    _assert_scratch_dsn_named_safely(scratch_dsn)
    # Host-headroom preflight BEFORE the first database touch: scratch
    # provisioning + a full publish is a known big writer, and starting it
    # under host blocker pressure is the 2026-07-17 ENOSPC shape.
    _preflight_scratch_host_headroom(dep_dir)
    _provision_scratch_schema(scratch_dsn)
    _assert_zero_prior_generations(scratch_dsn, deployment=deployment)

    # ---- pinned inputs into the fresh scaffold ------------------------------
    _apply_composed_catalog(scratch_dsn, dep_dir)
    _apply_facts(scratch_dsn, facts, deployment=deployment)
    _copy_outcomes(scratch_dsn, outcome_store_source)

    # ---- build the deployment stack; disable reuse if requested -------------
    stack = build_deployment_publisher_stack(
        dsn=scratch_dsn, deployment_name=deployment, deployment_dir=dep_dir,
    )
    kwargs = stack.publisher_kwargs()
    registry = kwargs.get("maintained_derivations")
    if derivation_registry_transform is not None and registry is not None:
        registry = derivation_registry_transform(registry)
    if reuse_disabled and registry is not None:
        registry = _force_full_registry(registry)
    kwargs["maintained_derivations"] = registry

    # ---- the real publish cycle (derivations, checks, selection) -----------
    #
    # For the fixture there are no model-backed steps, so the cycle replays
    # nothing; the replay recorder + copied outcomes still stand, and the probe
    # (below) exercises the replay/exclusion contract against the same scratch
    # store. There is no partial reference: any non-published outcome aborts.
    try:
        out = build_deployment_publisher(
            dsn=scratch_dsn,
            deployment_name=deployment,
            deployment_dir=dep_dir,
            command="qa complete reference",
            stack=stack,
            publisher_overrides={
                "maintained_derivations": registry,
            },
        ).cycle()
    except Exception as exc:  # noqa: BLE001 - operator failure => loud abort
        raise ReferenceExecutionAborted(
            f"operator failure during {'dependency baseline' if reuse_disabled else 'complete reference'} "
            f"for {deployment!r}; no partial reference emitted: {exc!r}"
        ) from exc
    status = out.get("status")
    if status != "published":
        raise ReferenceExecutionAborted(
            f"publish did not reach 'published' (status={status!r}, "
            f"blocked_reason={out.get('blocked_reason')!r}); there is no "
            f"partial reference."
        )

    generation_ref = str(out["generation_id"])
    compat_generation_id = int(out["compat_generation_id"])

    # ---- exclusion-path witness (replay recorder against the scratch store) -
    exclusions: list[ReplayExclusion] = []
    probe_content: str | None = None
    if outcome_probe is not None:
        probe_content, probe_exclusions = _run_outcome_probe(
            scratch_dsn,
            outcome_probe,
            deployment=outcome_store_source.deployment,
            generation_id=compat_generation_id,
        )
        exclusions.extend(probe_exclusions)

    # ---- assemble the structured result ------------------------------------
    derivation_modes = _read_derivation_modes(out)
    check_coverage = _read_check_coverage(
        scratch_dsn, compat_generation_id=compat_generation_id
    )
    state_dump = _dump_state(
        scratch_dsn,
        deployment=deployment,
        generation_ref=generation_ref,
        compat_generation_id=compat_generation_id,
    )
    return CompleteReferenceResult(
        mode="dependency_baseline" if reuse_disabled else "complete_reference",
        deployment=deployment,
        generation_id=generation_ref,
        compat_generation_id=compat_generation_id,
        reuse_disabled=reuse_disabled,
        derivation_modes=derivation_modes,
        check_coverage=check_coverage,
        exclusions=exclusions,
        probe_replayed_content=probe_content,
        state_dump=state_dump,
        state_dump_digest=_state_dump_digest(state_dump),
    )


def _read_derivation_modes(out: Mapping[str, Any]) -> dict[str, StepMode]:
    summaries = out.get("maintained_derivations") or {}
    modes: dict[str, StepMode] = {}
    for derivation_id, summary in summaries.items():
        summary = summary or {}
        modes[derivation_id] = StepMode(
            derivation_id=derivation_id,
            status=str(summary.get("status", "unknown")),
            mode=summary.get("mode"),
            full_reason=summary.get("full_reason"),
            dirty_bucket_count=summary.get("dirty_bucket_count"),
            rows_written=summary.get("rows_written"),
            rows_deleted=summary.get("rows_deleted"),
        )
    return modes


def _run_outcome_probe(
    dsn: str,
    probe: OutcomeProbe,
    *,
    deployment: str,
    generation_id: int,
) -> tuple[str | None, list[ReplayExclusion]]:
    """Exercise one model-backed step through a REPLAY recorder bound to the
    scratch store. A hit returns the recorded content (the model client is
    never called); a miss records a NAMED :class:`ReplayExclusion` and is
    surfaced — never a fresh sample."""
    from okg.substrate.db.connection import connect as db_connect

    store = OutcomeStore(deployment=deployment, max_rows=probe.max_rows)
    recorder = OutcomeRecorder(store=store, mode="replay")
    key = probe.key()
    with db_connect(dsn, autocommit=True) as conn:
        try:
            content = recorder.keyed_complete(
                conn, probe.llm, key, probe.system_prompt, probe.user_prompt,
                fragment_id=probe.fragment_id, caller=f"probe:{probe.step_id}",
                generation_id=generation_id,
            )
            return content, list(recorder.exclusions)
        except ReplayMissError:
            # The exclusion is recorded on the recorder BEFORE the raise; the
            # run is excluded for this step, never sampled fresh.
            return None, list(recorder.exclusions)


def run_complete_reference(
    source_snapshot: Any,
    *,
    deployment_dir: Path | str,
    scratch_dsn: str,
    outcome_store_source: OutcomeReplaySource | None,
    outcome_probe: OutcomeProbe | None = None,
    derivation_registry_transform: Callable[[Any], Any] | None = None,
) -> CompleteReferenceResult:
    """Run the complete-execution reference for a pinned snapshot.

    ``source_snapshot`` is the pinned fact payload (a sequence of ``NodeFact``);
    ``outcome_store_source`` pins the recorded model outcomes to replay (copied
    into the scratch store before the run). The entire declared pipeline runs
    into ``scratch_dsn`` (a fresh ``okg_test_*`` scaffold that must hold no prior
    generation), under replay of the pinned outcomes, and returns the reference
    resolved state. Guards are loud + typed; an operator failure aborts with no
    partial reference.

    ``derivation_registry_transform`` is an escape hatch used by
    :func:`run_dependency_baseline` (to force full) and by fault-injection
    witnesses (to make a derivation raise); production reference runs pass None.
    """
    return _run_pipeline(
        source_snapshot,
        deployment_dir=deployment_dir,
        scratch_dsn=scratch_dsn,
        outcome_store_source=outcome_store_source,
        reuse_disabled=False,
        outcome_probe=outcome_probe,
        derivation_registry_transform=derivation_registry_transform,
    )


def run_dependency_baseline(
    source_snapshot: Any,
    *,
    deployment_dir: Path | str,
    scratch_dsn: str,
    outcome_store_source: OutcomeReplaySource | None,
    outcome_probe: OutcomeProbe | None = None,
) -> CompleteReferenceResult:
    """Run the dependency baseline: the same update with ALL reuse disabled.

    Every maintained derivation is forced full (``decide_maintain_mode``'s
    ``force_full`` branch → recorded cause ``forced``) and the checks run
    full-sweep (no scoped runs). Its resolved state must equal the complete
    reference's (the paper's ``Dep_V``); the "every dependent reran" property
    rides the execution evidence already on the result — every derivation
    reports a full-recompute mode with a recorded cause, and every check
    coverage row reports ``scoped=false`` (assert on the result; no new ledger).
    """
    return _run_pipeline(
        source_snapshot,
        deployment_dir=deployment_dir,
        scratch_dsn=scratch_dsn,
        outcome_store_source=outcome_store_source,
        reuse_disabled=True,
        outcome_probe=outcome_probe,
        derivation_registry_transform=None,
    )


def assert_every_derivation_reran_full(
    result: CompleteReferenceResult,
) -> None:
    """Assert the dependency-baseline "every dependent reran" property from the
    result's execution evidence: each maintained derivation ran a full recompute
    with a recorded cause. Loud on violation (used by the baseline witness)."""
    if not result.derivation_modes:
        raise CompleteReferenceError(
            "dependency baseline recorded no maintained derivations; expected "
            "every dependent to have reran full."
        )
    for step in result.derivation_modes.values():
        if step.status != "maintained" or step.mode != "full_recount":
            raise CompleteReferenceError(
                f"derivation {step.derivation_id!r} did not run a full recompute "
                f"(status={step.status!r}, mode={step.mode!r}); reuse was not "
                f"disabled."
            )
        if step.full_reason not in FULL_RECOMPUTE_CAUSES:
            raise CompleteReferenceError(
                f"derivation {step.derivation_id!r} ran full without a recorded "
                f"cause (full_reason={step.full_reason!r})."
            )


def assert_every_check_full_sweep(result: CompleteReferenceResult) -> None:
    """Assert every post-publish check ran a full sweep (``scoped=false``)."""
    if not result.check_coverage:
        raise CompleteReferenceError(
            "no check coverage rows recorded; expected a full sweep per check."
        )
    for coverage in result.check_coverage:
        if coverage.scoped:
            raise CompleteReferenceError(
                f"check {coverage.check_id!r} ran scoped; the dependency "
                f"baseline requires a full sweep (scoped=false)."
            )


__all__ = [
    "CheckCoverage",
    "CompleteReferenceError",
    "CompleteReferenceResult",
    "HostHeadroomRefusal",
    "IncompleteSnapshotPin",
    "OutcomeProbe",
    "OutcomeReplaySource",
    "ReferenceExecutionAborted",
    "ScratchGuardViolation",
    "StepMode",
    "assert_every_check_full_sweep",
    "assert_every_derivation_reran_full",
    "run_complete_reference",
    "run_dependency_baseline",
]

"""Sound, branch-aware, backfill-safe generation ordinals.

ivm-effective-attribute-view / task.generation-ordinal-primitive.

The effective-attribute view's planned shared validity-interval rows
(``req.incremental-storage-representation``) need a ``from_gen <= G < to_gen``
predicate over generations. The only orderings the substrate exposes today are
wall-clock based -- the ``created_at`` checkpoint string and
``_chronos_compat_generation_id()`` (= ``int(time.time_ns() // 1000)``) -- and
BOTH are unsound as a *causal* order:

  * clock skew / NTP step / sub-microsecond collisions reorder
    near-simultaneous publishes, and
  * a backfill (``from_checkpoint=True``) re-materialises a PAST generation, but
    a wall-clock token would stamp it with ``now`` -- ordering a past generation
    AFTER newer ones.

Trusting either is a silent-stale / default-as-fallback P0. This module mints a
monotonic, branch-aware ordinal keyed to checkpoint *identity* (never wall
clock) and stores it in an OKG-owned side table (``okg.generation_ordinals``):
the Chronos checkpoint table (``_chronos_branch_interval_checkpoints``) is
library-managed and cannot be ``ALTER``ed by OKG.

Branch-awareness is the load-bearing subtlety. A single global counter would
interleave two branches' ordinals (two forks publishing concurrently would
trample each other's order). A fork inherits its parent branch's ordinals up to
the fork point, then diverges, so the ordinal is scoped per ``branch_id`` and a
fork's first generation continues from its ``base_generation_id``'s ordinal + 1.
Reads resolve a pinned generation G to its ``(branch_id, ordinal)`` and compare
WITHIN the branch lineage.

Density: ordinals are dense at mint time. Deleting a middle generation
(``delete_generation``) leaves a gap, which is harmless for an interval stab --
monotonicity is preserved and nothing is ever renumbered.

Stranded ordinals (okg#1587): before that fix, an interval-tracked publish that
failed after its ordinal committed -- in the retention prune, the integrity
preflight, or ``checkpoint_generation`` -- left an ordinal row naming a
checkpoint that was never created. The publisher now assigns the ordinal inside
the same transaction as the checkpoint, so new gaps of this kind cannot form.
Existing ones are TOLERATED, not repaired. Readers look an ordinal up by a real
checkpoint id or compare ordinals by order. Two read the position just below a
new ordinal: the interval sync reads the rows live there, and
a stab at a stranded position still returns the rows live at it; the
attribute-integrity receipt chain detects the gap and re-anchors once
(okg#1603, pinned by ``tests/substrate/test_attribute_receipt_retention.py``).
The visible effect is that the retention window and the disk report count the
stranded row as a generation. Renumbering to close a gap would be unsafe
(interval rows and receipts are keyed by ordinal), so there is no repair.

SCOPE: this module is the PREREQUISITE primitive only. It is intentionally NOT
yet wired into the live ``checkpoint_generation`` publish path, and is NOT yet
consumed by any read path. Minting on the live path is coupled with a
one-time production backfill of existing generations (a deployment with N
un-ordinaled generations must be seeded 0..N-1 BEFORE new publishes start
minting, or the new gens would mis-order against history). That rollout, plus
the representation change (PK rekey + interval rows + read rewrite on the ~23GB
table, ``task.interval-row-representation``), is deliberately deferred for
review.
"""
from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from okg.substrate.errors import SubstrateError

GENERATION_ORDINAL_TABLE = "okg.generation_ordinals"


def _as_checkpoint_id(generation_id: int | str) -> str:
    """Normalise a generation id / int token to its checkpoint-id form.

    Mirrors ``ChronosGenerationsMixin._generation_checkpoint_id`` so the ordinal
    registry keys on the same identity the checkpoint table uses.
    """
    text = str(generation_id)
    if text.startswith("okg:") or text.startswith("generation:"):
        return text
    return f"generation:{text}"


def ensure_generation_ordinal_schema(db: Any) -> None:
    """Idempotently create the ordinal registry (and its monotonicity index).

    Owned on the mint path (not only in ``migrate``) so it self-heals on the
    privileged publish path exactly like the okg#400 effective-attrs self-heal:
    any deployment that mints an ordinal gets the table without a migration step.
    ``CREATE ... IF NOT EXISTS`` is a cheap catalog check when the table already
    exists.
    """
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS okg.generation_ordinals (
          checkpoint_id          TEXT PRIMARY KEY,
          branch_id              TEXT   NOT NULL,
          ordinal                BIGINT NOT NULL,
          previous_checkpoint_id TEXT,
          deployment_name        TEXT,
          created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # Monotonicity guard: one ordinal per position per branch. A concurrent
    # mint that computed the same (branch_id, ordinal) trips this and fails
    # loud rather than silently double-occupying a slot.
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS generation_ordinals_branch_ordinal
          ON okg.generation_ordinals (branch_id, ordinal)
        """
    )
    # The TEXT-ref-keyed read surface the MCP sidecar uses instead of a direct
    # grant on this table (D2 amendment (a)). Created HERE, beside the table,
    # not only in views.sql: this table is runtime-ensured, so a deployment can
    # mint its first ordinal after views.sql last ran. The sidecar refuses
    # loudly when the view is missing, so leaving the view's existence to a
    # later re-migrate would turn a normal migrate -> publish -> read sequence
    # into an error. The grant is still views.sql's job; a runtime role may not
    # hold GRANT authority, and an ungranted view is inert rather than wrong.
    db.execute(
        """
        CREATE OR REPLACE VIEW okg.v_generation_interval_stab AS
        SELECT
          checkpoint_id          AS generation_ref,
          branch_id              AS chronos_branch_id,
          ordinal                AS stab_ordinal,
          previous_checkpoint_id AS previous_generation_ref,
          deployment_name
        FROM okg.generation_ordinals
        """
    )


def _ordinal_for_checkpoint(db: Any, checkpoint_id: str) -> int | None:
    row = db.execute(
        "SELECT ordinal FROM okg.generation_ordinals WHERE checkpoint_id = ?",
        (checkpoint_id,),
    ).fetchone()
    if row is None:
        return None
    return int(row["ordinal"])


def _branch_head(db: Any, branch: str) -> tuple[str | None, int | None]:
    """Return (checkpoint_id, ordinal) of the highest-ordinal row on ``branch``,
    or (None, None) if the branch has no ordinals yet."""
    row = db.execute(
        "SELECT checkpoint_id, ordinal FROM okg.generation_ordinals "
        "WHERE branch_id = ? ORDER BY ordinal DESC LIMIT 1",
        (branch,),
    ).fetchone()
    if row is None:
        return (None, None)
    return (row["checkpoint_id"], int(row["ordinal"]))


def _fork_base(context: Any, branch: str) -> str | None:
    """Return the parent fork-point generation id for ``branch`` if it is a
    fork, else None (a true root branch such as ``default``/``main``).

    Reads the branch's Chronos metadata, where the fork-creation paths record
    the base generation as ``base_generation_id`` (worktree forks; see
    ``read_branch.py``) or ``compacted_from_generation`` (compaction forks; see
    ``generations.py``).
    """
    try:
        info = context.get_branch(branch)
    except Exception:  # noqa: BLE001 -- a missing branch record == not a fork here
        return None
    md = (getattr(info, "metadata", None) or {}) if info is not None else {}
    base = md.get("base_generation_id") or md.get("compacted_from_generation")
    return str(base) if base else None


def _resolve_fork_base_ordinal(db: Any, context: Any, branch: str) -> int | None:
    """Ordinal of ``branch``'s fork point, or None if ``branch`` is a root.

    FAIL LOUD if the branch IS a fork but its base generation has no ordinal:
    that means the parent lineage was not backfilled, and inventing an ordinal
    would silently misplace the entire fork. Never a wall-clock fallback.
    """
    base = _fork_base(context, branch)
    if base is None:
        return None
    base_cp = _as_checkpoint_id(base)
    base_ord = _ordinal_for_checkpoint(db, base_cp)
    if base_ord is None:
        raise SubstrateError(
            "generation ordinal: fork base generation has no ordinal -- "
            "backfill the parent branch lineage before assigning fork ordinals",
            data={
                "branch": branch,
                "base_generation_id": base,
                "base_checkpoint_id": base_cp,
            },
        )
    return base_ord


# A branch whose Chronos parent is one of these is itself a ROOT working branch
# (its ordinal sequence legitimately starts at 0), not a fork. "main" is the
# Chronos root (parent NULL); OKG's working branch "default" forks from "main"
# and so is correctly classified a root WITHOUT listing it here. A fork's parent
# is the working branch it split from (e.g. "default"), so "default" must NOT be
# in this set or real forks-from-default would be misread as roots.
_ROOT_PARENTS = (None, "", "main")


def _structural_parent_branch(db: Any, branch: str) -> str | None:
    """Return the Chronos structural parent of ``branch`` (what it forked from),
    or None if ``branch`` is not a registered Chronos branch.

    Chronos records this in ``_chronos_branch_interval_branches.parent_branch_id``
    (and even infers it from segments when NULL). This is the STRUCTURAL fork
    signal, independent of whether the creating code stamped a
    ``base_generation_id`` in metadata -- several real fork paths (the ingest and
    merge_back quarantine branches) do NOT set one.
    """
    try:
        row = db.execute(
            "SELECT parent_branch_id FROM _chronos_branch_interval_branches "
            "WHERE branch_id = ?",
            (branch,),
        ).fetchone()
    except Exception:  # noqa: BLE001 -- Chronos branch table absent yet == no fork
        return None
    if row is None:
        return None
    return row["parent_branch_id"]


def _is_structural_fork(db: Any, branch: str) -> bool:
    """True if Chronos records ``branch`` as forked from a non-root parent."""
    parent = _structural_parent_branch(db, branch)
    return parent not in _ROOT_PARENTS and parent != branch


def is_interval_tracked_branch(db: Any, branch: str) -> bool:
    """Public: is ``branch`` eligible for the incremental interval effective-view?

    Only ROOT branches (the main publishing line) are interval-tracked: they mint a
    gap-free ordinal sequence, so the interval table + running counter are sound. A
    STRUCTURAL fork (forked from a non-root parent -- e.g. an ingest/merge_back
    quarantine branch) is NOT tracked: it has no recorded base ordinal, so the
    interval path is skipped and the fork falls back to the per-generation snapshot
    (a full re-materialize on that branch), which is correct but not incrementally
    accelerated (incremental fork-seeding deferred). Keeps fork publishes correct +
    fail-safe when OKG_IVM_INTERVAL_VIEW is on.
    """
    return not _is_structural_fork(db, branch)


def interval_representation_flag_enabled(
    env: Mapping[str, str] | None = None,
) -> bool:
    """Public: is the interval representation ENABLED by configuration?

    The branch-independent half of :func:`interval_representation_enabled`, for
    callers that hold an environment mapping but no database handle (deployment
    upgrade phases) or that want the cheap check before building one.

    Two conditions, both required:

    * ``OKG_IVM_INTERVAL_VIEW`` is on -- the operator-facing cutover flag.
    * ``OKG_IVM_GENERATION_ORDINAL`` is not ``0`` -- the ordinal kill switch
      (issue #1514). The interval sync and the
      attribute-integrity receipts are all KEYED by the ordinal, so "mint
      nothing but sync anyway" is not a coherent state: disabling the ordinal
      disables the representation that consumes it. ``0`` exactly, matching the
      switch's other half in
      ``ChronosGraphStore._maybe_mint_generation_ordinal``.
    """
    source: Mapping[str, str] = os.environ if env is None else env
    return (
        source.get("OKG_IVM_INTERVAL_VIEW", "1") == "1"
        and source.get("OKG_IVM_GENERATION_ORDINAL", "1") != "0"
    )


def interval_representation_enabled(db: Any, branch: str) -> bool:
    """Public: does the interval representation OWN this branch's publish?

    THE single predicate for that question. The publisher (which mints the
    ordinal and syncs the interval front) and the effective-attribute
    materializer (which DROPS the snapshot carry-forward on the strength of
    that sync) must answer it identically. When they disagreed, the
    materializer wrote a changed-only snapshot for a generation whose interval
    rows were never synced, and the unchanged majority existed in neither
    place.

    Configuration (see :func:`interval_representation_flag_enabled`) AND an
    interval-tracked branch (a ROOT branch, see
    :func:`is_interval_tracked_branch`). Either off means the correct,
    non-accelerated per-generation full snapshot arm.
    """
    return interval_representation_flag_enabled() and is_interval_tracked_branch(
        db, branch
    )


def _branch_start_ordinal(db: Any, context: Any, branch: str) -> int | None:
    """The ordinal a branch's FIRST generation continues from: a fork's base
    ordinal (so first = base + 1), or None for a root (first = 0).

    RAISES for a fork whose base cannot be resolved -- a structural fork (Chronos
    ``parent_branch_id``) with no recorded ``base_generation_id``, or a base that
    has not been backfilled. Never silently treats a fork as a root.
    """
    base_ord = _resolve_fork_base_ordinal(db, context, branch)
    if base_ord is not None:
        return base_ord
    if _is_structural_fork(db, branch):
        raise SubstrateError(
            "generation ordinal: branch is a structural fork (non-root parent) "
            "with no resolvable base generation -- record base_generation_id at "
            "fork creation, or backfill the fork's base ordinal before minting",
            data={
                "branch": branch,
                "parent_branch_id": _structural_parent_branch(db, branch),
            },
        )
    return None


def _insert_ordinal(
    db: Any,
    *,
    checkpoint_id: str,
    branch: str,
    ordinal: int,
    previous_checkpoint_id: str | None,
    deployment_name: str,
) -> None:
    db.execute(
        "INSERT INTO okg.generation_ordinals "
        "(checkpoint_id, branch_id, ordinal, previous_checkpoint_id, deployment_name) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (checkpoint_id) DO NOTHING",
        (checkpoint_id, branch, int(ordinal), previous_checkpoint_id, deployment_name),
    )


def assign_generation_ordinal(
    db: Any,
    context: Any,
    *,
    checkpoint_id: int | str,
    branch: str,
    deployment_name: str = "default",
) -> int:
    """Assign (or return the existing) ordinal for ``checkpoint_id``. O(1).

    ``ordinal = head(branch).ordinal + 1`` on a non-empty branch; for a fork's
    FIRST generation, ``base_generation_id.ordinal + 1``; ``0`` for a true root.
    Idempotent on ``checkpoint_id`` (re-checkpoint / re-materialise of a past
    generation returns the SAME ordinal -- the backfill-safety property).

    FAIL LOUD on a ``(branch_id, ordinal)`` collision or a fork with an
    un-backfilled base. Never derives order from wall clock.
    """
    cp = _as_checkpoint_id(checkpoint_id)
    ensure_generation_ordinal_schema(db)

    existing = _ordinal_for_checkpoint(db, cp)
    if existing is not None:
        return existing  # idempotent: a past generation keeps its ordinal

    head_cp, head_ord = _branch_head(db, branch)
    if head_ord is not None:
        ordinal = head_ord + 1
        previous_cp: str | None = head_cp
    else:
        # First generation on this branch: continue a fork from its base, start a
        # root at 0, or fail loud for an unresolvable fork (structural fork with
        # no recorded/backfilled base) -- never silently number a fork from 0.
        base_ord = _branch_start_ordinal(db, context, branch)
        if base_ord is not None:
            ordinal = base_ord + 1
            base = _fork_base(context, branch)
            previous_cp = _as_checkpoint_id(base) if base else None
        else:
            ordinal = 0
            previous_cp = None

    try:
        _insert_ordinal(
            db,
            checkpoint_id=cp,
            branch=branch,
            ordinal=ordinal,
            previous_checkpoint_id=previous_cp,
            deployment_name=deployment_name,
        )
    except Exception as exc:  # noqa: BLE001 -- surface a typed, loud failure
        raise SubstrateError(
            "generation ordinal: failed to assign a monotonic ordinal "
            "(likely a concurrent mint collided on (branch_id, ordinal))",
            data={
                "branch": branch,
                "checkpoint_id": cp,
                "attempted_ordinal": ordinal,
                "error": str(exc),
            },
        ) from exc

    final = _ordinal_for_checkpoint(db, cp)
    if final is None:
        raise SubstrateError(
            "generation ordinal: insert did not persist",
            data={"branch": branch, "checkpoint_id": cp, "attempted_ordinal": ordinal},
        )
    if final != ordinal:
        # A concurrent mint committed this checkpoint with a DIFFERENT ordinal
        # between our existence check and our insert (ON CONFLICT (checkpoint_id)
        # silently suppressed ours). Surface it loudly rather than returning an
        # ordinal we never computed against the head we read. (The per-branch
        # publisher lease makes this unreachable in normal operation; this is the
        # fail-loud backstop, never a silent accept.)
        raise SubstrateError(
            "generation ordinal: a concurrent mint assigned a divergent ordinal",
            data={
                "branch": branch,
                "checkpoint_id": cp,
                "computed_ordinal": ordinal,
                "stored_ordinal": final,
            },
        )
    return final


def mint_generation_ordinal(
    db: Any,
    context: Any,
    *,
    checkpoint_id: int | str,
    branch: str,
    deployment_name: str = "default",
    list_generations: Callable[[], list],
) -> int:
    """Live-publish-path entry: assign an ordinal to a freshly-created checkpoint.

    O(1) once the branch is seeded. On the FIRST mint for a branch that already
    has generations (an existing deployment adopting ordinals), one-time backfills
    the branch's prior generations in causal order BEFORE assigning, so a new mint
    can never start at 0 and mis-order against un-ordinaled history. This is what
    makes wiring the mint into the publish path safe without a manual migration.

    ``list_generations`` is a zero-arg callable returning the branch's checkpoints
    (ids, or objects with a ``checkpoint_id`` attribute) in causal (created_at)
    order -- the only causal proxy available for already-created generations (a
    documented one-time seed; ongoing order comes from the chain head, not the
    clock). Raises (like :func:`assign_generation_ordinal`) for an unresolvable
    fork; the publish-path caller decides fatal-vs-logged while the ordinal is not
    yet consumed by any reader.
    """
    ensure_generation_ordinal_schema(db)
    cp = _as_checkpoint_id(checkpoint_id)
    _, head_ord = _branch_head(db, branch)
    if head_ord is not None:
        # Branch already seeded -> O(1) assign from the head. (Common path.)
        return assign_generation_ordinal(
            db, context, checkpoint_id=cp, branch=branch, deployment_name=deployment_name
        )
    # First mint on this branch. Resolve the branch start FIRST -- it is cheap
    # (a branch-metadata + parent_branch_id lookup) and raises for an unresolvable
    # fork BEFORE the O(total) list_generations scan. So a structural fork with no
    # recorded base (e.g. an ingest/merge_back quarantine branch, which never gets
    # seeded) does NOT pay a full checkpoint scan + parse on every publish.
    base_ord = _branch_start_ordinal(db, context, branch)
    # One-time causal seed of all prior generations (and this one).
    ordered = [
        _as_checkpoint_id(getattr(g, "checkpoint_id", g)) for g in list_generations()
    ]
    if cp not in ordered:
        ordered.append(cp)
    backfill_generation_ordinals(
        db,
        branch=branch,
        ordered_checkpoint_ids=ordered,
        base_ordinal=base_ord,
        deployment_name=deployment_name,
    )
    return resolve_generation_ordinal(db, cp)


def resolve_generation_ordinal(db: Any, checkpoint_id: int | str) -> int:
    """Return the ordinal for a generation; FAIL LOUD if it has none.

    Never falls back to ``created_at`` / a wall-clock token. A missing ordinal
    means the generation predates the primitive and its deployment was not
    backfilled (run :func:`backfill_generation_ordinals`); returning a guessed
    order would be a silent-stale P0.
    """
    from okg.substrate.history_read_guard import raise_if_compacted

    cp = _as_checkpoint_id(checkpoint_id)
    raise_if_compacted(db, cp)
    reg = db.execute("SELECT to_regclass('okg.generation_ordinals') AS t").fetchone()
    if reg is None or reg["t"] is None:
        raise SubstrateError(
            "generation ordinal: registry table absent -- provision or backfill "
            "before resolving generation order",
            data={"checkpoint_id": cp},
        )
    ordinal = _ordinal_for_checkpoint(db, cp)
    if ordinal is None:
        raise_if_compacted(db, cp)
        raise SubstrateError(
            "generation ordinal: no ordinal for generation -- backfill the "
            "deployment's generation lineage (never trust wall-clock order)",
            data={"checkpoint_id": cp},
        )
    raise_if_compacted(db, cp)
    return ordinal


def resolve_generation_ordinal_row(
    db: Any, checkpoint_id: int | str
) -> tuple[str, int]:
    """Return ``(branch_id, ordinal)`` for a generation; FAIL LOUD if it has none.

    The interval read path needs BOTH: ordinals are branch-scoped, so a stab must
    pin the branch as well as the ordinal. Same fail-loud contract as
    :func:`resolve_generation_ordinal` -- never guesses an order or a branch.
    """
    from okg.substrate.history_read_guard import raise_if_compacted

    cp = _as_checkpoint_id(checkpoint_id)
    raise_if_compacted(db, cp)
    reg = db.execute("SELECT to_regclass('okg.generation_ordinals') AS t").fetchone()
    if reg is None or reg["t"] is None:
        raise SubstrateError(
            "generation ordinal: registry table absent -- provision or backfill "
            "before resolving generation order",
            data={"checkpoint_id": cp},
        )
    row = db.execute(
        "SELECT branch_id, ordinal FROM okg.generation_ordinals WHERE checkpoint_id = ?",
        (cp,),
    ).fetchone()
    if row is None:
        raise_if_compacted(db, cp)
        raise SubstrateError(
            "generation ordinal: no ordinal for generation -- backfill the "
            "deployment's generation lineage (never trust wall-clock order)",
            data={"checkpoint_id": cp},
        )
    raise_if_compacted(db, cp)
    return (str(row["branch_id"]), int(row["ordinal"]))


def backfill_generation_ordinals(
    db: Any,
    *,
    branch: str,
    ordered_checkpoint_ids: list[int | str],
    base_ordinal: int | None = None,
    deployment_name: str = "default",
) -> dict[str, Any]:
    """One-time seed: assign ordinals to a branch's EXISTING generations.

    ``ordered_checkpoint_ids`` MUST be the branch's generations in causal order.
    For already-created generations the only available proxy is the checkpoint
    ``created_at`` order (``list_generations``); the migration uses it ONCE here
    as a seed -- it is NOT an ongoing trust of wall clock (new generations get
    their order from the chain, not the clock). For a fork, pass
    ``base_ordinal`` = the parent fork-point ordinal so the seed continues from
    ``base_ordinal + 1`` (backfill parent branches first).

    Position is authoritative: ordinal(i) = start + i, where start is 0 for a
    root or ``base_ordinal + 1`` for a fork. Pass the FULL causal sequence from
    position 0 each time. Idempotent: an already-seeded checkpoint sitting at its
    expected position is skipped; one sitting at a DIFFERENT ordinal means the
    input order contradicts a prior seed and raises (never silently renumbers).
    """
    ensure_generation_ordinal_schema(db)
    start = (base_ordinal + 1) if base_ordinal is not None else 0
    cps = [_as_checkpoint_id(raw) for raw in ordered_checkpoint_ids]

    # Pass 1 -- VALIDATE before mutating: position is authoritative, so any
    # already-seeded checkpoint must already sit at its causal position. A
    # disagreement means this input order contradicts a prior seed; fail loud
    # rather than splice a new ordinal in and silently mis-number the tail.
    for i, cp in enumerate(cps):
        existing = _ordinal_for_checkpoint(db, cp)
        if existing is not None and existing != start + i:
            raise SubstrateError(
                "generation ordinal backfill: an existing ordinal disagrees with "
                "its causal position -- the input order is inconsistent with a "
                "prior seed (pass the FULL causal sequence from position 0)",
                data={
                    "branch": branch,
                    "checkpoint_id": cp,
                    "existing_ordinal": existing,
                    "expected_ordinal": start + i,
                },
            )

    # Pass 2 -- assign the gaps at their fixed positions.
    assigned = 0
    previous_cp: str | None = None
    for i, cp in enumerate(cps):
        if _ordinal_for_checkpoint(db, cp) is None:
            _insert_ordinal(
                db,
                checkpoint_id=cp,
                branch=branch,
                ordinal=start + i,
                previous_checkpoint_id=previous_cp,
                deployment_name=deployment_name,
            )
            assigned += 1
        previous_cp = cp

    return {
        "branch": branch,
        "assigned": assigned,
        "examined": len(cps),
        "max_ordinal": (start + len(cps) - 1) if cps else start - 1,
    }

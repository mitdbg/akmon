"""PACT v4 decision ledger.

Decisions are events in the deployment Postgres — never in pact.yaml.
Worktree-independent by construction; per-principal database roles make
authority structural (deferred until the first supervisor exists; the
single-operator deployment is the degenerate case).

Degraded-mode rule (design.md §6): with no reachable ledger you can author
and implement freely, but you cannot DECIDE or MERGE — both refuse loudly.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import envelope
from okg.substrate.pact.fsm import DecisionEvent

LEDGER_SCHEMA = "okg"
LEDGER_TABLE = "okg.pact_decisions"
INTENT_TABLE = "okg.pact_intents"
COORDINATION_EVENT_TABLE = "okg.pact_coordination_events"
#: Bare relation names, paired with their qualified form. The namespace
#: enforcement check (scripts/check_schema_namespace.py) and the adoption
#: refusal below both key on this pairing.
LEDGER_RELATIONS = (
    ("pact_decisions", LEDGER_TABLE),
    ("pact_intents", INTENT_TABLE),
    ("pact_coordination_events", COORDINATION_EVENT_TABLE),
)
DEFAULT_CLAIM_EXPIRY_SECONDS = 30 * 60
DEFAULT_COORDINATION_WRITER_ROLE = "okg_mcp"
TRANSCRIPT_WATERMARK_SOURCES = (
    "agent-sessions",
    "codex-sessions",
    "okg-workspace.agent-sessions",
    "okg-workspace.codex-sessions",
)

#: Grant reconciliation, split out of `_DDL` so it can run ALONE.
#: `okg pact` writes coordination events as `okg_mcp` (the append-only writer
#: role), and views.sql's `REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp`
#: strips that grant on every migrate now that the ledger lives in `okg`. The
#: probe below notices, and this block — pure GRANT, no ACCESS EXCLUSIVE — puts
#: it back without replaying the table DDL.
#:
#: OWNERSHIP MOVED. `schema-grant-coherence` landed the grant registry, and
#: `okg.grant_registry` now carries these three relations, so views.sql's own
#: REVOKE and re-GRANT are in the same file in that order and a migrate no
#: longer leaves the grant stripped. This block stays as the BOOTSTRAP path,
#: not as the repair path: a database that has never run `migrate()` has no
#: registry to apply, and `okg pact` still has to work there. It is also the
#: only route on a database that predates migration 0027. Keeping it costs one
#: idempotent GRANT; removing it would make the ledger's own ensure depend on a
#: migration having run first.
#:
#: The trap this used to be: the ensure path is PROBE-SKIPPED when the tables
#: already exist, so on an established deployment nothing here ran, and the
#: schema-wide REVOKE won by default.
_WRITER_GRANT_DDL = f"""
DO $pact_coordination_writer_grant$
DECLARE
    seq_name TEXT;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA {LEDGER_SCHEMA} TO okg_mcp';
        EXECUTE 'GRANT INSERT, SELECT ON {COORDINATION_EVENT_TABLE} TO okg_mcp';
        SELECT pg_get_serial_sequence('{COORDINATION_EVENT_TABLE}', 'id')
          INTO seq_name;
        IF seq_name IS NOT NULL THEN
            EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE %s TO okg_mcp', seq_name);
        END IF;
    END IF;
END
$pact_coordination_writer_grant$;
"""

#: Bootstrap/repair DDL. Demoted (schema-namespace-qualification): schema.sql
#: is now the authoring home for these three tables, so this runs only on a
#: database that never saw `migrate()` — a bare ledger DB, or one whose ledger
#: was dropped. It stays idempotent and schema-qualified; the leading
#: CREATE SCHEMA is what makes a bare ledger DB work at all.
_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {LEDGER_SCHEMA};
CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    change_id TEXT NOT NULL,
    verdict TEXT NOT NULL,
    note TEXT,
    target TEXT,
    principal TEXT NOT NULL,
    authority TEXT NOT NULL DEFAULT 'operator',
    intent_digest TEXT,
    from_state TEXT,
    to_state TEXT,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pact_decisions_change_idx
    ON {LEDGER_TABLE} (change_id, id);
CREATE TABLE IF NOT EXISTS {INTENT_TABLE} (
    change_id TEXT NOT NULL,
    worktree_suffix TEXT NOT NULL,
    worktree_path TEXT,
    branch TEXT,
    head_sha TEXT,
    session_id TEXT,
    intent_digest TEXT,
    derived_state TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_at TIMESTAMPTZ,
    PRIMARY KEY (change_id, worktree_suffix)
);
ALTER TABLE {INTENT_TABLE} ADD COLUMN IF NOT EXISTS head_sha TEXT;
ALTER TABLE {INTENT_TABLE} ADD COLUMN IF NOT EXISTS session_id TEXT;
ALTER TABLE {INTENT_TABLE} ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ;
CREATE TABLE IF NOT EXISTS {COORDINATION_EVENT_TABLE} (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type TEXT NOT NULL,
    change_id TEXT NOT NULL,
    task_id TEXT,
    worktree_suffix TEXT,
    worktree_path TEXT,
    branch TEXT,
    head_sha TEXT,
    session_id TEXT,
    principal TEXT,
    intent_digest TEXT,
    payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pact_coordination_events_change_idx
    ON {COORDINATION_EVENT_TABLE} (change_id, id);
CREATE INDEX IF NOT EXISTS pact_coordination_events_worktree_idx
    ON {COORDINATION_EVENT_TABLE} (worktree_suffix, occurred_at DESC);
{_WRITER_GRANT_DDL}
"""

# Steady-state guard for the DDL above. `ADD COLUMN IF NOT EXISTS` (and
# friends) take ACCESS EXCLUSIVE even when they change nothing, so running
# `_DDL` on every ledger touch briefly exclusive-locks `okg.pact_intents` and
# `okg.pact_coordination_events`. On 2026-07-22 one such touch, issued by a
# background agent while `okg deployment up` held dump locks, killed a
# 6-minute pg_dump twice.
#
# The probe returns THREE independent facts, not one verdict
# (schema-namespace-qualification): the objects being present, the okg_mcp
# writer grants being present, and a pre-qualification `public` ledger still
# waiting to be adopted. Collapsing them was affordable while the only repair
# was "run all of `_DDL`"; it is not now. Every migrate re-runs views.sql's
# `REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp`, so grants-missing is
# a routine state that must be repaired with a GRANT — not with table DDL that
# takes ACCESS EXCLUSIVE on a live ledger.
_LEDGER_STATE_PROBE = f"""
SELECT
    -- objects_present
    to_regclass('{LEDGER_TABLE}') IS NOT NULL
    AND to_regclass('{INTENT_TABLE}') IS NOT NULL
    AND to_regclass('{COORDINATION_EVENT_TABLE}') IS NOT NULL
    AND to_regclass('{LEDGER_SCHEMA}.pact_decisions_change_idx') IS NOT NULL
    AND to_regclass(
        '{LEDGER_SCHEMA}.pact_coordination_events_change_idx'
    ) IS NOT NULL
    AND to_regclass(
        '{LEDGER_SCHEMA}.pact_coordination_events_worktree_idx'
    ) IS NOT NULL
    AND (
        SELECT count(*) FROM pg_attribute
         WHERE attrelid = to_regclass('{INTENT_TABLE}')
           AND attname IN ('head_sha', 'session_id', 'last_event_at')
           AND NOT attisdropped
    ) = 3,
    -- writer_grants_present
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp')
            THEN TRUE
        WHEN to_regclass('{COORDINATION_EVENT_TABLE}') IS NULL
            THEN FALSE
        ELSE
            has_table_privilege(
                'okg_mcp', '{COORDINATION_EVENT_TABLE}', 'INSERT'
            )
            AND has_table_privilege(
                'okg_mcp', '{COORDINATION_EVENT_TABLE}', 'SELECT'
            )
            AND has_sequence_privilege(
                'okg_mcp',
                pg_get_serial_sequence('{COORDINATION_EVENT_TABLE}', 'id'),
                'USAGE'
            )
            AND has_sequence_privilege(
                'okg_mcp',
                pg_get_serial_sequence('{COORDINATION_EVENT_TABLE}', 'id'),
                'SELECT'
            )
    END,
    -- public_ledger_present: ANY ledger relation still sitting in `public`.
    -- Presence is the QUESTION, not the answer: `_public_shadow_verdict`
    -- reads the row counts on both sides before deciding, because the state
    -- this guard exists for is directional. See that function.
    ARRAY(
        SELECT relname FROM (VALUES
            ('pact_decisions'), ('pact_intents'), ('pact_coordination_events')
        ) AS candidates(relname)
        WHERE to_regclass('public.' || relname) IS NOT NULL
        ORDER BY relname
    )
"""


def _ledger_state(cur) -> tuple[bool, bool, list[str]]:
    """``(objects_present, writer_grants_present, public_ledger_relations)``."""
    cur.execute(_LEDGER_STATE_PROBE)
    row = cur.fetchone()
    if not row:
        return False, False, []
    return bool(row[0]), bool(row[1]), list(row[2] or [])


def _ledger_schema_current(cur) -> bool:
    """True when every object ``_DDL`` would create already exists AND the
    okg_mcp writer grants are in place."""
    objects_present, grants_present, _ = _ledger_state(cur)
    return objects_present and grants_present


#: Bare names only, DERIVED from the pairing above so the two cannot drift.
#: Rebinding `LEDGER_RELATIONS` itself broke every consumer that unpacks the
#: pairs - the namespace checks and the `clean_ledger` fixture among them.
_SHADOW_RELATION_NAMES = tuple(bare for bare, _qualified in LEDGER_RELATIONS)


# What a leftover `public` relation does to the caller.
SHADOW_REFUSE = "refuse"
SHADOW_WARN = "warn"

# Relations already warned about, keyed by NAME only. Keying on the row counts
# made the key change on every write, so the warning reprinted at each of the
# ~13 ledger touches in a single command.
_WARNED_SHADOWS: set[str] = set()


@dataclass(frozen=True)
class ShadowState:
    """What both copies of one ledger relation hold.

    `okg_rows` is -1 when the `okg` copy does not exist at all. `unreadable`
    is set when a copy could not be measured; it is never silently treated as
    empty, because "I could not look" and "there is nothing there" are the two
    answers this guard must never confuse.
    """

    public_rows: int = 0
    okg_rows: int = 0
    #: How many `public` rows have no counterpart in the `okg` copy. None
    #: means the two could not be compared at all, which is never treated as
    #: zero.
    orphan_rows: int | None = None
    unreadable: str | None = None


def _relation_is_table(cur, schema: str, relation: str) -> bool:
    """`to_regclass` matches ANY relkind - an index or sequence of the same
    name would otherwise be measured as if it were a ledger copy."""

    cur.execute(
        "SELECT c.relkind FROM pg_class c JOIN pg_namespace n "
        "ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s",
        (schema, relation),
    )
    row = cur.fetchone()
    return bool(row) and row[0] in ("r", "p")


def _row_count(cur, schema: str, relation: str) -> int:
    cur.execute(f'SELECT count(*) FROM "{schema}"."{relation}"')
    return int(cur.fetchone()[0])


def _shared_columns(cur, relation: str) -> list[str]:
    """Columns both copies have, which is what they can be compared on.

    A `public` copy predates whatever `ALTER TABLE ... ADD COLUMN` has since
    run against `okg`, so the shapes routinely differ. Comparing on the shared
    columns answers the question that matters - is this `public` row present
    in `okg` - without demanding identical shapes.
    """

    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        ("public", relation),
    )
    public_columns = {row[0] for row in cur.fetchall()}
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (LEDGER_SCHEMA, relation),
    )
    okg_columns = {row[0] for row in cur.fetchall()}
    return sorted(public_columns & okg_columns)


def _orphan_row_count(cur, relation: str) -> int | None:
    """How many `public` rows are absent from the ``okg`` copy.

    This is the containment question asked EXACTLY, with EXCEPT, rather than
    inferred from row counts. Counts cannot answer it: on the deployment that
    prompted this change the `okg` copy had MORE rows than `public` and a
    newer newest-row, and all 32 of `public`'s coordination events were still
    absent from it. The two copies had been written independently.

    Returns None when the comparison is impossible - no shared columns - which
    the caller must treat as "unknown", never as "contained".
    """

    columns = _shared_columns(cur, relation)
    if not columns:
        return None
    projection = ", ".join(f'"{column}"' for column in columns)
    cur.execute(
        f"SELECT count(*) FROM ("
        f'SELECT {projection} FROM public."{relation}" '
        f'EXCEPT SELECT {projection} FROM {LEDGER_SCHEMA}."{relation}"'
        f") orphans"
    )
    return int(cur.fetchone()[0])


def _public_shadow_counts(cur, relations) -> dict[str, ShadowState]:
    """Measure both copies of every relation left behind in ``public``.

    This is the first time this module has ever read the `public` schema, and
    it can fail in ways the ledger path has never had to survive: the role may
    hold no privilege there, or the name may belong to something that is not a
    table. Each measurement runs inside a SAVEPOINT, so a raised error cannot
    abort the surrounding transaction and take the DDL repairs below down with
    it.
    """

    import psycopg

    states: dict[str, ShadowState] = {}
    for relation in relations:
        if relation not in _SHADOW_RELATION_NAMES:
            # Belt and braces for the interpolated identifiers below. The
            # probe only ever names these three, and this check reads the
            # DERIVED tuple, so deleting either constant cannot leave it
            # passing vacuously.
            states[relation] = ShadowState(
                unreadable=f"{relation!r} is not a PACT ledger relation"
            )
            continue
        cur.execute("SAVEPOINT okg_pact_shadow_probe")
        try:
            if not _relation_is_table(cur, "public", relation):
                raise ValueError(f"public.{relation} exists but is not a table")
            public_rows = _row_count(cur, "public", relation)
            if not _relation_is_table(cur, LEDGER_SCHEMA, relation):
                states[relation] = ShadowState(
                    public_rows=public_rows,
                    okg_rows=-1,
                )
                cur.execute("RELEASE SAVEPOINT okg_pact_shadow_probe")
                continue
            okg_rows = _row_count(cur, LEDGER_SCHEMA, relation)
            orphan_rows = (
                0 if public_rows == 0 else _orphan_row_count(cur, relation)
            )
        except (psycopg.Error, ValueError) as exc:
            cur.execute("ROLLBACK TO SAVEPOINT okg_pact_shadow_probe")
            states[relation] = ShadowState(
                unreadable=f"{type(exc).__name__}: {exc}".strip()
            )
        else:
            states[relation] = ShadowState(
                public_rows=public_rows,
                okg_rows=okg_rows,
                orphan_rows=orphan_rows,
            )
        cur.execute("RELEASE SAVEPOINT okg_pact_shadow_probe")
    return states


def _public_shadow_verdict(
    states: dict[str, ShadowState],
) -> tuple[str, list[str]]:
    """Refuse unless every ``public`` row is already in the ``okg`` copy.

    Containment is asked exactly, with EXCEPT. Row counts were tried first and
    are not merely weaker - they are wrong. Measured on the deployment that
    prompted this change, after a few writes had landed: `okg` held 37
    coordination events to `public`'s 32, and a newer newest-row, and all 32
    of `public`'s rows were still absent from it. The two copies had been
    written independently, so "more rows and more recent" said contained while
    the truth was disjoint.

    A relation is passed over only when its `public` copy is empty, or when
    nothing in it is missing from `okg`. Anything else - orphan rows, a
    comparison that could not be made, a copy that could not be read, or no
    `okg` copy at all - refuses, because the alternative is letting two live
    ledgers drift further apart while advertising a repair that would destroy
    rows held nowhere else.
    """

    problems: list[str] = []
    leftovers: list[str] = []
    for relation, state in sorted(states.items()):
        qualified = f"{LEDGER_SCHEMA}.{relation}"
        if state.unreadable:
            problems.append(
                f"public.{relation} could not be measured ({state.unreadable})"
            )
            continue
        if state.okg_rows == -1:
            problems.append(
                f"public.{relation} has {state.public_rows} row(s) and there "
                f"is no {qualified} at all"
            )
            continue
        if state.public_rows == 0:
            leftovers.append(
                f"public.{relation} is an empty leftover table "
                f"({qualified} holds {state.okg_rows} row(s))"
            )
            continue
        if state.orphan_rows is None:
            problems.append(
                f"public.{relation} holds {state.public_rows} row(s) that "
                f"cannot be compared with {qualified} (no shared columns)"
            )
            continue
        if state.orphan_rows > 0:
            problems.append(
                f"public.{relation} holds {state.orphan_rows} row(s) that are "
                f"in no {qualified} row (public {state.public_rows}, "
                f"{LEDGER_SCHEMA} {state.okg_rows})"
            )
            continue
        leftovers.append(
            f"public.{relation} ({state.public_rows} row(s)) is fully "
            f"contained by {qualified} ({state.okg_rows} row(s))"
        )
    if problems:
        return SHADOW_REFUSE, problems
    return SHADOW_WARN, leftovers


_RECONCILE_HINT = (
    "reconcile by hand: export the public rows, compare them against the "
    f"{LEDGER_SCHEMA} copy, merge anything only public holds into "
    f"{LEDGER_SCHEMA}, and only then drop the public table. Do NOT drop it "
    "first - on a diverged shape it holds rows that exist nowhere else. `okg "
    "migrate` cannot be step one either: migration 0022 refuses with "
    "duplicate_table while both copies exist."
)


def _warn_public_shadow(relations: list[str], detail: list[str]) -> None:
    """Say, on stderr, that a leftover `public` copy is being passed over.

    Passing it over is not the same as it being gone. The warning is what
    stops "the write went through" from reading as "the migration finished".
    """

    key = "|".join(sorted(relations))
    if key in _WARNED_SHADOWS:
        return
    _WARNED_SHADOWS.add(key)
    print(
        "warning: PACT ledger relations remain in `public` from an incomplete "
        f"migration 0022; this CLI reads and writes the {LEDGER_SCHEMA} copy. "
        + "; ".join(detail)
        + ". Repair: "
        + _RECONCILE_HINT,
        file=sys.stderr,
    )


def ensure_ledger_schema(cur) -> str | None:
    """Probe-guard for the ledger relations. Returns the repair performed.

    schema.sql owns the shape of these three tables; this guard exists so a
    database that never ran ``migrate()`` still bootstraps, and so the okg_mcp
    writer grant is restored after every migrate strips it. It executes the
    CHEAPEST repair that closes the observed gap, and nothing when there is no
    gap.

    Refuses unless the ``okg`` copy demonstrably contains what a leftover
    ``public`` copy holds - see `_public_shadow_verdict`. A leftover copy that
    IS contained is passed over with a warning rather than a refusal, because
    every table constant in this module names the ``okg`` schema and refusing
    there recovers nothing while making the ledger unusable (issue #1476).
    """
    objects_present, grants_present, in_public = _ledger_state(cur)
    shadow_relations: list[str] = []
    shadow_warnings: list[str] = []
    if in_public:
        # Ask about the three tables DIRECTLY. `objects_present` also requires
        # indexes and columns, so it is False for a merely dropped index -
        # which would have sent a fully adopted ledger down the "not adopted"
        # path and told the operator to run a migration that has already run.
        adopted = [
            relation
            for relation in _SHADOW_RELATION_NAMES
            if _relation_is_table(cur, LEDGER_SCHEMA, relation)
        ]
        if not adopted:
            raise SubstrateError(
                "PACT ledger tables are still in the `public` schema and "
                "have not been adopted into `okg`: "
                + ", ".join(f"public.{name}" for name in in_public),
                data={
                    "public_ledger_relations": in_public,
                    "okg_ledger_present": False,
                    "cause": "ledger_not_adopted",
                },
                fix_hint=(
                    "run `okg migrate` (ordered migration "
                    "0022_pact_ledger_schema_qualification moves them with "
                    "ALTER TABLE ... SET SCHEMA, preserving every row) before "
                    "touching the ledger again"
                ),
            )
        states = _public_shadow_counts(cur, in_public)
        verdict, detail = _public_shadow_verdict(states)
        if verdict == SHADOW_REFUSE:
            raise SubstrateError(
                "PACT ledger relations exist in BOTH `public` and `okg` and "
                "the `okg` copy does not contain the `public` one, so which "
                "one holds the recorded decisions is undecidable: "
                + "; ".join(detail),
                data={
                    "public_ledger_relations": in_public,
                    "okg_ledger_present": True,
                    "stranded": detail,
                    "relation_counts": {
                        relation: {
                            "public_rows": state.public_rows,
                            "okg_rows": state.okg_rows,
                            "orphan_rows": state.orphan_rows,
                            "unreadable": state.unreadable,
                        }
                        for relation, state in sorted(states.items())
                    },
                    "cause": "ledger_shadowed_by_public",
                },
                fix_hint=_RECONCILE_HINT,
            )
        shadow_relations = list(in_public)
        shadow_warnings = detail
    if shadow_warnings:
        _warn_public_shadow(shadow_relations, shadow_warnings)
    if not objects_present:
        cur.execute(_DDL)
        return "bootstrap"
    if not grants_present:
        # Verify, do not assume. A GRANT issued by a role holding neither
        # ownership nor grant option raises no error: Postgres emits
        # `WARNING: no privileges were granted` and the statement SUCCEEDS.
        # Returning "writer_grants" on that would report a repair that did not
        # happen, and every later ledger touch would re-attempt it forever
        # while `okg pact` kept failing permission-denied somewhere else. Fail
        # at the repair site, naming the role that could not grant.
        cur.execute(_WRITER_GRANT_DDL)
        _, grants_now, _ = _ledger_state(cur)
        if not grants_now:
            cur.execute("SELECT current_user")
            actor = cur.fetchone()[0]
            raise SubstrateError(
                "PACT coordination writer grant could not be restored: "
                f"role {actor!r} issued the GRANT without error but okg_mcp "
                f"still lacks INSERT/SELECT on {COORDINATION_EVENT_TABLE}",
                data={
                    "granting_role": actor,
                    "table": COORDINATION_EVENT_TABLE,
                    "cause": "writer_grant_not_applied",
                },
                fix_hint=(
                    "connect as the owner of "
                    f"{COORDINATION_EVENT_TABLE} (or a role with GRANT "
                    "OPTION on it) and re-run, or re-run `okg migrate` as "
                    "the deployment owner"
                ),
            )
        return "writer_grants"
    return None


def worktree_local_env_dsn() -> str | None:
    """Resolve OKG_DSN from the MAIN checkout's ``.okg/local.env`` when
    running inside a linked git worktree.

    Finding (2) of `standardize-pact-review-interface`: agent worktrees
    never inherit the per-checkout ``.okg/local.env``, so the pact CLI
    silently lost ledger/graph context everywhere agents actually work.
    ``git rev-parse --git-common-dir`` names the main repo's ``.git`` even
    from a worktree; its parent holds the shared local env.
    """
    import subprocess

    try:
        common = subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
    except Exception:
        return None
    if not common:
        return None
    env_path = Path(common).resolve().parent / ".okg" / "local.env"
    if not env_path.is_file():
        return None
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("OKG_DSN="):
            value = line.split("=", 1)[1].strip().strip("'\"")
            return value or None
    return None


def _worktree_local_env_fallback_allowed() -> bool:
    return not (
        os.environ.get("PYTEST_CURRENT_TEST")
        and os.environ.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
    )


def resolve_ledger_dsn(
    *,
    explicit_dsn: str | None = None,
    deployment: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Resolve the decision-ledger DSN with the same authority chain the
    graph projection uses; the ledger lives in the deployment database."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    selected_source = "explicit"
    if deployment is None:
        deployment = os.environ.get("OKG_PACT_GRAPH_DEPLOYMENT")
        selected_source = "process_env"
    if deployment is None:
        selected_source = "pact_project"
        if repo_root is None:
            repo_root = _discover_pact_repo_root()
        if repo_root is not None:
            from okg.substrate.pact.project_settings import (
                graph_projection_deployment,
            )

            try:
                deployment = graph_projection_deployment(repo_root)
            except SubstrateError as exc:
                return {
                    "dsn": None,
                    "reason": str(exc),
                    "deployment": None,
                    "deployment_source": selected_source,
                    "next_action": "fix_pact_graph_projection_policy",
                    "no_dsn_configured": False,
                }
    if deployment is None:
        return {
            "dsn": None,
            "reason": (
                "decision ledger deployment is not configured; set "
                "OKG_PACT_GRAPH_DEPLOYMENT or "
                ".okg/coordination.yaml graph_projection.deployment"
            ),
            "deployment": None,
            "deployment_source": "unresolved",
            "next_action": "configure_pact_graph_deployment",
            "no_dsn_configured": False,
        }
    resolution = resolve_deployment_dsn_authority(
        deployment,
        explicit_dsn=explicit_dsn,
        local_profile_authoritative=deployment_self_hosted_authoritative(deployment),
    )
    if resolution.get("dsn"):
        return {
            **resolution,
            "deployment": deployment,
            "deployment_source": selected_source,
        }
    if explicit_dsn is not None:
        return {
            "dsn": None,
            "reason": resolution.get("reason") or (
                f"explicit decision-ledger DSN was refused for deployment "
                f"{deployment!r}"
            ),
            "deployment": deployment,
            "deployment_source": selected_source,
            "no_dsn_configured": False,
            "next_action": resolution.get("next_action") or "fix_explicit_dsn_identity",
            "authority": resolution.get("authority"),
            "target": resolution.get("target"),
        }
    # Classify WHY nothing resolved (issue #643, req.pactdx.ledger-hints):
    # "no DSN configured in this shell" (every source empty / unresolved)
    # is a different disease from a configured-but-refused DSN (identity
    # mismatch, unreadable manifest). Downstream blockers key on this so
    # the no-DSN case never masquerades as coordination disagreement.
    next_action = resolution.get("next_action")
    return {
        "dsn": None,
        "reason": resolution.get("reason") or (
            f"decision ledger deployment {deployment!r}: OKG_DSN is not set"
        ),
        "deployment": deployment,
        "deployment_source": selected_source,
        "no_dsn_configured": next_action in (None, "configure_okg_dsn"),
        **({"next_action": next_action} if next_action else {}),
    }


def _discover_pact_repo_root() -> Path | None:
    current = Path.cwd().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pact" / "project.yaml").is_file():
            return candidate
    return None


def require_ledger_connection(
    *,
    explicit_dsn: str | None = None,
    deployment: str | None = None,
    repo_root: Path | None = None,
):
    """Connect to the ledger or refuse loudly. Never degrades silently."""
    resolution = resolve_ledger_dsn(
        explicit_dsn=explicit_dsn,
        deployment=deployment,
        repo_root=repo_root,
    )
    dsn = resolution.get("dsn")
    if not dsn:
        if (
            resolution.get("deployment") is None
            and resolution.get("next_action") in {
                "configure_pact_graph_deployment",
                "fix_pact_graph_projection_policy",
            }
        ):
            raise SubstrateError(
                str(resolution.get("reason") or "decision ledger deployment is not configured"),
                data={
                    "reason": resolution.get("reason"),
                    "cause": "deployment_not_configured",
                    "deployment": None,
                },
                fix_hint=(
                    "set OKG_PACT_GRAPH_DEPLOYMENT=<deployment> or add "
                    "graph_projection.deployment to .okg/coordination.yaml"
                ),
            )
        if resolution.get("no_dsn_configured"):
            # Plain language + both fixes inline (issue #643,
            # req.pactdx.ledger-hints). The message must stay compact:
            # deferred-closure paths truncate it to 160 chars, and the
            # fix commands must survive the cut.
            deployment = str(resolution["deployment"])
            raise SubstrateError(
                "decision ledger: no database connection configured in "
                "this shell; fix: export OKG_DSN=... or `okg deployment "
                f"env {deployment} --write-local`",
                data={
                    "reason": resolution.get("reason"),
                    "cause": "no_dsn_configured",
                    "deployment": deployment,
                },
                fix_hint=(
                    "export OKG_DSN=<deployment dsn> for this shell, or run "
                    f"`okg deployment env {deployment} --write-local` to "
                    "persist it for this checkout"
                ),
            )
        raise SubstrateError(
            "decision ledger unreachable: cannot DECIDE in degraded mode "
            "(authoring and implementation remain available)",
            data={
                "reason": resolution.get("reason"),
                "deployment": resolution.get("deployment"),
            },
            fix_hint=(
                "export the deployment DSN or run `okg deployment env "
                f"{resolution.get('deployment')} --json` to resolve it"
            ),
        )
    from okg.substrate.db.connection import connect as db_connect

    try:
        conn = db_connect(dsn)
    except Exception as exc:  # psycopg.OperationalError and friends
        raise SubstrateError(
            "decision ledger unreachable: cannot DECIDE in degraded mode",
            data={"reason": str(exc)},
        ) from exc
    with conn.cursor() as cur:
        # Catalog-probe first: steady-state ledger access must take no DDL
        # locks (see _LEDGER_STATE_PROBE). Bootstrap runs the full idempotent
        # DDL; a stripped writer grant is repaired with a GRANT alone.
        ensure_ledger_schema(cur)
    conn.commit()
    return conn


def record_decision(
    conn,
    *,
    change_id: str,
    verdict: str,
    principal: str,
    authority: str = "operator",
    note: str | None = None,
    target: str | None = None,
    intent_digest: str | None = None,
    from_state: str | None = None,
    to_state: str | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {LEDGER_TABLE}
                (change_id, verdict, note, target, principal, authority,
                 intent_digest, from_state, to_state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, decided_at
            """,
            (
                change_id, verdict, note, target, principal, authority,
                intent_digest, from_state, to_state,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return {"decision_id": row[0], "decided_at": row[1].isoformat()}


def load_decisions(conn, change_id: str) -> list[DecisionEvent]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT change_id, verdict, principal, authority, note, target,
                   from_state, to_state, intent_digest, decided_at
            FROM {LEDGER_TABLE}
            WHERE change_id = %s
            ORDER BY id
            """,
            (change_id,),
        )
        rows = cur.fetchall()
    return [
        DecisionEvent(
            change_id=r[0], verdict=r[1], principal=r[2], authority=r[3],
            note=r[4], target=r[5], from_state=r[6], to_state=r[7],
            intent_digest=r[8],
            decided_at=r[9].isoformat() if r[9] else None,
        )
        for r in rows
    ]


def load_all_decisions(conn) -> dict[str, list[DecisionEvent]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT change_id, verdict, principal, authority, note, target,
                   from_state, to_state, intent_digest, decided_at
            FROM {LEDGER_TABLE}
            ORDER BY id
            """
        )
        rows = cur.fetchall()
    out: dict[str, list[DecisionEvent]] = {}
    for r in rows:
        out.setdefault(r[0], []).append(
            DecisionEvent(
                change_id=r[0], verdict=r[1], principal=r[2], authority=r[3],
                note=r[4], target=r[5], from_state=r[6], to_state=r[7],
                intent_digest=r[8],
                decided_at=r[9].isoformat() if r[9] else None,
            )
        )
    return out


def load_decision_watermark(conn) -> dict[str, int]:
    """Cheap decision-ledger watermark: ``(max id, row count)``.

    A new decision bumps ``max_id``; a removed decision drops ``count`` — so the
    pair detects both insert and delete without loading every row. Used by the
    incremental-source change-probe (issue #323) so pact's no-change sync can
    short-circuit while still busting on a new/removed decision.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(id), 0), COUNT(*) FROM {LEDGER_TABLE}")
        row = cur.fetchone()
    max_id = int(row[0]) if row and row[0] is not None else 0
    count = int(row[1]) if row and row[1] is not None else 0
    return {"max_id": max_id, "count": count}


def load_coordination_event_watermark(conn) -> dict[str, int]:
    """Cheap coordination-event diagnostic: ``(max id, row count)``.

    Coordination events and operator decisions are separate append-only
    ledgers. This pair is useful for status reporting, but is not a sound
    applied identity because an in-place update leaves it unchanged. Sources
    use :func:`load_coordination_event_snapshot` instead.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COALESCE(MAX(id), 0), COUNT(*) "
            f"FROM {COORDINATION_EVENT_TABLE}"
        )
        row = cur.fetchone()
    max_id = int(row[0]) if row and row[0] is not None else 0
    count = int(row[1]) if row and row[1] is not None else 0
    return {"max_id": max_id, "count": count}


def coordination_event_rows_identity(rows: list[dict[str, Any]]) -> str:
    """Complete identity for the exact coordination rows being projected.

    ``MAX(id):COUNT(*)`` is not a content identity: an in-place update leaves
    both values unchanged, and sequence allocation order is not commit order.
    Hash every projected column from the already-captured rows, retaining the
    visible max id and count as diagnostic sequence metadata.
    """
    payload = json.dumps(
        rows,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    max_id = max(
        (int(row.get("event_id") or 0) for row in rows),
        default=0,
    )
    return f"{max_id}:{len(rows)}:{hashlib.sha256(payload).hexdigest()}"


def load_coordination_event_snapshot(
    conn,
    change_id: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Capture coordination rows and their identity in one repeatable snapshot.

    The caller supplies a fresh, idle connection.  The digest is computed from
    the exact row objects returned for emission, so it cannot describe a
    different database view.  A transaction that allocated an identity value
    but has not committed is absent from both rows and digest; after it commits,
    the next snapshot necessarily has a different digest.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
        rows = load_coordination_events(conn, change_id=change_id)
        identity = coordination_event_rows_identity(rows)
        max_id = max(
            (int(row.get("event_id") or 0) for row in rows),
            default=0,
        )
        return rows, {
            "identity": identity,
            "max_id": max_id,
            "count": len(rows),
        }


def load_decision_rows(conn) -> list[dict[str, Any]]:
    """All ledger rows incl. row ids, for the graph projection (PACTSource)."""
    cols = [
        "decision_id", "change_id", "verdict", "principal", "authority",
        "note", "target", "from_state", "to_state", "intent_digest",
        "decided_at",
    ]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, change_id, verdict, principal, authority, note, target,
                   from_state, to_state, intent_digest, decided_at
            FROM {LEDGER_TABLE}
            ORDER BY id
            """
        )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        record = dict(zip(cols, r))
        if record["decided_at"] is not None:
            record["decided_at"] = record["decided_at"].isoformat()
        out.append(record)
    return out


def current_worktree_identity() -> dict[str, Any] | None:
    """Producing checkout provenance, or None when not in git."""
    import hashlib
    import subprocess

    try:
        path = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
        branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
        head_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001
        return None
    if not path:
        return None
    suffix = hashlib.sha256(
        str(Path(path).resolve()).encode("utf-8")
    ).hexdigest()[:10]
    return {
        "path": path,
        "suffix": suffix,
        "branch": branch,
        "head_sha": head_sha,
        "session_id": current_session_id(),
    }


def current_session_id() -> str | None:
    for name in (
        "OKG_AGENT_SESSION_ID",
        "CODEX_SESSION_ID",
        "CLAUDE_SESSION_ID",
        "AGENT_SESSION_ID",
    ):
        value = os.environ.get(name)
        if value:
            return value
    return None


def claim_expiry_seconds() -> int:
    raw = os.environ.get("OKG_PACT_CLAIM_EXPIRY_SECONDS")
    if not raw:
        return DEFAULT_CLAIM_EXPIRY_SECONDS
    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_CLAIM_EXPIRY_SECONDS
    return parsed if parsed > 0 else DEFAULT_CLAIM_EXPIRY_SECONDS


def claim_freshness(
    age_seconds: float | int | None,
    *,
    expiry_seconds: int | None = None,
) -> dict[str, Any]:
    threshold = expiry_seconds or claim_expiry_seconds()
    if age_seconds is None:
        return {
            "status": "unknown",
            "expired": True,
            "age_seconds": None,
            "expiry_seconds": threshold,
        }
    age = float(age_seconds)
    expired = age > threshold
    return {
        "status": "expired" if expired else "fresh",
        "expired": expired,
        "age_seconds": age,
        "expiry_seconds": threshold,
    }


def coordination_writer_role() -> str | None:
    raw = os.environ.get(
        "OKG_PACT_COORDINATION_WRITER_ROLE",
        DEFAULT_COORDINATION_WRITER_ROLE,
    )
    role = raw.strip() if raw else ""
    return role or None


def _set_coordination_writer_role(conn) -> str | None:
    role = coordination_writer_role()
    if role is None:
        return None
    import psycopg
    from psycopg import sql

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if cur.fetchone() is None:
            raise SubstrateError(
                "PACT coordination writer role is missing",
                data={"role": role},
                fix_hint=(
                    "provision the append-only ledger role or set "
                    "OKG_PACT_COORDINATION_WRITER_ROLE for this deployment"
                ),
            )
        try:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        except psycopg.Error as exc:
            raise SubstrateError(
                "PACT coordination event write requires append-only ledger role",
                data={"role": role, "reason": str(exc)},
            ) from exc
    return role


def record_coordination_event(
    conn,
    *,
    event_type: str,
    change_id: str,
    task_id: str | None = None,
    intent_digest: str | None = None,
    payload: dict[str, Any] | None = None,
    worktree: dict[str, Any] | None = None,
    principal: str | None = None,
) -> dict[str, Any]:
    worktree = worktree or current_worktree_identity() or {}
    payload = payload or {}
    principal = principal or acting_principal()
    writer_role: str | None = None
    with conn.cursor() as cur:
        writer_role = _set_coordination_writer_role(conn)
        cur.execute(
            f"""
            INSERT INTO {COORDINATION_EVENT_TABLE}
                (event_type, change_id, task_id, worktree_suffix,
                 worktree_path, branch, head_sha, session_id, principal,
                 intent_digest, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, occurred_at
            """,
            (
                event_type,
                change_id,
                task_id,
                worktree.get("suffix"),
                worktree.get("path"),
                worktree.get("branch"),
                worktree.get("head_sha"),
                worktree.get("session_id"),
                principal,
                intent_digest,
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = cur.fetchone()
        cur.execute("RESET ROLE")
        if worktree.get("suffix"):
            cur.execute(
                """
                INSERT INTO okg.pact_intents AS pact_intents
                    (change_id, worktree_suffix, worktree_path, branch,
                     head_sha, session_id, intent_digest, derived_state,
                     last_event_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (change_id, worktree_suffix) DO UPDATE
                  SET branch = EXCLUDED.branch,
                      worktree_path = EXCLUDED.worktree_path,
                      head_sha = EXCLUDED.head_sha,
                      session_id = EXCLUDED.session_id,
                      intent_digest = COALESCE(
                          EXCLUDED.intent_digest,
                          pact_intents.intent_digest
                      ),
                      derived_state = COALESCE(
                          EXCLUDED.derived_state,
                          pact_intents.derived_state
                      ),
                      last_event_at = EXCLUDED.last_event_at
                """,
                (
                    change_id,
                    worktree["suffix"],
                    worktree.get("path"),
                    worktree.get("branch"),
                    worktree.get("head_sha"),
                    worktree.get("session_id"),
                    intent_digest,
                    payload.get("derived_state"),
                    row[1],
                ),
            )
    conn.commit()
    return {
        "event_id": row[0],
        "occurred_at": row[1].isoformat(),
        "writer_role": writer_role,
    }


def register_intent(
    conn,
    *,
    change_id: str,
    intent_digest: str,
    derived_state: str,
    worktree: dict[str, Any],
) -> dict[str, Any]:
    """Tier E (worktree-access-tiers req.tiers.event-plane): in-flight
    intent is an EVENT on the shared plane - never a file projection."""
    return record_coordination_event(
        conn,
        event_type="intent",
        change_id=change_id,
        intent_digest=intent_digest,
        payload={"derived_state": derived_state},
        worktree=worktree,
    )


def load_intents(conn) -> list[dict[str, Any]]:
    cols = [
        "change_id", "worktree_suffix", "worktree_path", "branch",
        "head_sha", "session_id", "intent_digest", "derived_state",
        "registered_at", "last_event_at", "last_event_age_seconds",
    ]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT change_id, worktree_suffix, worktree_path, branch, "
            "head_sha, session_id, intent_digest, derived_state, "
            "registered_at, COALESCE(last_event_at, registered_at), "
            "EXTRACT(EPOCH FROM now() - COALESCE(last_event_at, registered_at)) "
            "FROM okg.pact_intents ORDER BY registered_at DESC"
        )
        rows = cur.fetchall()
    transcript_watermark = load_transcript_watermark_status(conn)
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        if d["registered_at"] is not None:
            d["registered_at"] = d["registered_at"].isoformat()
        if d["last_event_at"] is not None:
            d["last_event_at"] = d["last_event_at"].isoformat()
        if d["last_event_age_seconds"] is not None:
            d["last_event_age_seconds"] = float(d["last_event_age_seconds"])
        freshness = claim_freshness(d["last_event_age_seconds"])
        d["claim_freshness"] = freshness["status"]
        d["claim_expired"] = freshness["expired"]
        d["claim_expiry_seconds"] = freshness["expiry_seconds"]
        d.update(transcript_watermark)
        out.append(d)
    return out


def cleanup_stale_intents(
    conn,
    *,
    principal: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove expired intent claims from the active read model.

    The append-only coordination ledger keeps the provenance; ``okg.pact_intents`` is
    the current-claim read model that drives ``review-queue`` in-flight rows.
    """
    principal = principal or acting_principal()
    threshold = claim_expiry_seconds()
    cols = [
        "change_id", "worktree_suffix", "worktree_path", "branch",
        "head_sha", "session_id", "intent_digest", "derived_state",
        "registered_at", "last_event_at", "last_event_age_seconds",
    ]
    lock_clause = "" if dry_run else "FOR UPDATE"
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM okg.pact_intents")
        total = int(cur.fetchone()[0])
        cur.execute(
            f"""
            SELECT change_id, worktree_suffix, worktree_path, branch,
                   head_sha, session_id, intent_digest, derived_state,
                   registered_at, COALESCE(last_event_at, registered_at),
                   EXTRACT(EPOCH FROM now() - COALESCE(last_event_at, registered_at))
              FROM okg.pact_intents
             WHERE EXTRACT(EPOCH FROM now() - COALESCE(last_event_at, registered_at)) > %s
             ORDER BY registered_at, change_id, worktree_suffix
             {lock_clause}
            """,
            (threshold,),
        )
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    claims: list[dict[str, Any]] = []
    for row in rows:
        claim = {
            "change": row["change_id"],
            "worktree": row["worktree_suffix"],
            "branch": row["branch"],
            "head_sha": row["head_sha"],
            "session_id": row["session_id"],
            "derived_state": row["derived_state"],
            "last_event_age_seconds": (
                float(row["last_event_age_seconds"])
                if row["last_event_age_seconds"] is not None else None
            ),
        }
        claims.append(claim)

    claim_sample = claims[:20]
    omitted_claim_count = max(len(claims) - len(claim_sample), 0)

    if dry_run or not rows:
        return envelope.recorded(
            text=(
                f"stale claim cleanup dry-run: {len(rows)} eligible, "
                f"{max(total - len(rows), 0)} left alone"
                if dry_run else "stale claim cleanup: no expired claims found"
            ),
            status="dry_run" if dry_run else "clean",
            # `decision_verdict` is the `--verdict` the operator asked for
            # (approve/revise/defer/...). It is renamed off `verdict`, which
            # now names the envelope's recorded/deferred/refused outcome.
            decision_verdict="defer",
            eligible=len(rows),
            removed=0,
            skipped=max(total - len(rows), 0),
            events_recorded=0,
            claim_expiry_seconds=threshold,
            claims=claim_sample,
            omitted_claim_count=omitted_claim_count,
        )

    writer_role: str | None = None
    with conn.cursor() as cur:
        try:
            writer_role = _set_coordination_writer_role(conn)
            for row in rows:
                cur.execute(
                    f"""
                    INSERT INTO {COORDINATION_EVENT_TABLE}
                        (event_type, change_id, task_id, worktree_suffix,
                         worktree_path, branch, head_sha, session_id, principal,
                         intent_digest, payload)
                    VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        "stale_cleanup",
                        row["change_id"],
                        row["worktree_suffix"],
                        row["worktree_path"],
                        row["branch"],
                        row["head_sha"],
                        row["session_id"],
                        principal,
                        row["intent_digest"],
                        json.dumps(
                            {
                                "command": "decide --all-stale",
                                "verdict": "defer",
                                "claim_expiry_seconds": threshold,
                                "derived_state": row["derived_state"],
                                "last_event_age_seconds": (
                                    float(row["last_event_age_seconds"])
                                    if row["last_event_age_seconds"] is not None
                                    else None
                                ),
                            },
                            sort_keys=True,
                        ),
                    ),
                )
        finally:
            cur.execute("RESET ROLE")
        removed = 0
        for row in rows:
            cur.execute(
                """
                DELETE FROM okg.pact_intents
                 WHERE change_id = %s
                   AND worktree_suffix = %s
                   AND EXTRACT(EPOCH FROM now() - COALESCE(last_event_at, registered_at)) > %s
                """,
                (row["change_id"], row["worktree_suffix"], threshold),
            )
            removed += int(cur.rowcount)
    conn.commit()
    skipped = max(total - removed, 0)
    return envelope.recorded(
        text=(
            f"stale claim cleanup: removed {removed} expired "
            f"{'claim' if removed == 1 else 'claims'}; {skipped} left alone"
        ),
        status="cleaned",
        decision_verdict="defer",
        eligible=len(rows),
        removed=removed,
        skipped=skipped,
        events_recorded=len(rows),
        claim_expiry_seconds=threshold,
        writer_role=writer_role,
        claims=claim_sample,
        omitted_claim_count=omitted_claim_count,
    )


def load_transcript_watermark_status(conn) -> dict[str, Any]:
    """Best-effort freshness of the transcript ingestion plane.

    The coordination board is an intent/claim ledger, but operators also need
    to know whether the transcript plane that explains those claims is current.
    Keep this read optional so a bare decision-ledger test database still works.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.source_watermarks')")
            if cur.fetchone()[0] is None:
                return _empty_transcript_watermark("source_watermarks_missing")
            cur.execute(
                """
                SELECT source,
                       last_reconcile_at,
                       EXTRACT(EPOCH FROM now() - last_reconcile_at)
                  FROM okg.source_watermarks
                 WHERE source = ANY(%s)
                    OR source LIKE '%%.agent-sessions'
                    OR source LIKE '%%.codex-sessions'
                 ORDER BY last_reconcile_at DESC NULLS LAST, source
                """,
                (list(TRANSCRIPT_WATERMARK_SOURCES),),
            )
            rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        return _empty_transcript_watermark(
            "source_watermark_read_failed",
            error=str(exc),
        )

    watermarks: list[dict[str, Any]] = []
    ages: list[float] = []
    for source, reconciled_at, age in rows:
        age_seconds = float(age) if age is not None else None
        if age_seconds is not None:
            ages.append(age_seconds)
        watermarks.append({
            "source": source,
            "last_reconcile_at": (
                reconciled_at.isoformat() if reconciled_at is not None else None
            ),
            "age_seconds": age_seconds,
        })
    if not watermarks:
        return _empty_transcript_watermark("transcript_watermarks_missing")

    freshest = watermarks[0]
    return {
        "transcript_watermark_status": "available",
        "transcript_watermark_age_seconds": min(ages) if ages else None,
        "transcript_watermark_last_reconcile_at": freshest["last_reconcile_at"],
        "transcript_watermarks": watermarks,
    }


def _empty_transcript_watermark(
    status: str,
    *,
    error: str | None = None,
) -> dict[str, Any]:
    result = {
        "transcript_watermark_status": status,
        "transcript_watermark_age_seconds": None,
        "transcript_watermark_last_reconcile_at": None,
        "transcript_watermarks": [],
    }
    if error:
        result["transcript_watermark_error"] = error
    return result


def load_coordination_events(
    conn,
    change_id: str | None = None,
) -> list[dict[str, Any]]:
    cols = [
        "event_id", "event_type", "change_id", "task_id",
        "worktree_suffix", "worktree_path", "branch", "head_sha",
        "session_id", "principal", "intent_digest", "payload",
        "occurred_at",
    ]
    with conn.cursor() as cur:
        if change_id:
            cur.execute(
                f"""
                SELECT id, event_type, change_id, task_id, worktree_suffix,
                       worktree_path, branch, head_sha, session_id, principal,
                       intent_digest, payload, occurred_at
                FROM {COORDINATION_EVENT_TABLE}
                WHERE change_id = %s
                ORDER BY id
                """,
                (change_id,),
            )
        else:
            cur.execute(
                f"""
                SELECT id, event_type, change_id, task_id, worktree_suffix,
                       worktree_path, branch, head_sha, session_id, principal,
                       intent_digest, payload, occurred_at
                FROM {COORDINATION_EVENT_TABLE}
                ORDER BY id
                """
            )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        item = dict(zip(cols, r))
        if item["occurred_at"] is not None:
            item["occurred_at"] = item["occurred_at"].isoformat()
        out.append(item)
    return out


def acting_principal() -> str:
    principal = os.environ.get("OKG_PRINCIPAL")
    if principal:
        return principal
    import getpass

    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def is_operator(principal: str) -> bool:
    """Single-operator degenerate mode: every human principal is the
    operator until mandates exist. Agents set OKG_PRINCIPAL_KIND=agent."""
    return os.environ.get("OKG_PRINCIPAL_KIND", "operator") != "agent"

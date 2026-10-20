"""MCP session + generation pin primitives (M4.1, M4.5-corrected).

A Session carries a `deployment_name`, a frozen `generation_id`,
a pinned `catalog_version_id`, and a connection fingerprint
(see below). The pin is established once (at session open) from
the latest published generation for the deployment, or
explicitly requested by the caller. All subsequent tool calls
read the Chronos graph (okg.graph_nodes / okg.graph_edges) through
a checkout pinned to that generation, so concurrent publishes of
N+1 cannot perturb a session pinned at N.

Session also carries the pinned `catalog_version_id` (FK on
graph_generations) so catalog reads (subtypes, narrowings,
aliases) stay consistent with the fact view.

Connection fingerprint (M4.5 correctness fix, Dijkstra review
P0 #9):

  A Session is a value object — it doesn't hold a connection.
  The per-tool-call pattern is `tool(conn, session, ...)`. It
  is possible to open a Session against DSN-A and then pass a
  connection to DSN-B; the pin predicate still matches rows, but
  those rows belong to a different deployment's graph. Silent
  wrong answers.

  Fix: Session carries a `_db_fingerprint` — a short hash of
  (datname, cluster_system_identifier, okg.schema version) taken
  at open time. Every tool entry passes the connection through
  `assert_session_matches(conn, session)` which recomputes the
  fingerprint and raises on mismatch. The fingerprint is cheap
  (one SELECT) and caches per-connection via a private attribute
  on the psycopg.Connection.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Mapping, Optional

import psycopg

from okg.substrate.errors import SubstrateError


DEFAULT_DEPLOYMENT = "default"

# Private cache attribute we hang on psycopg.Connection so the
# fingerprint is computed at most once per connection lifetime.
_FP_ATTR = "_okg_substrate_db_fingerprint"
logger = logging.getLogger(__name__)


# Retired in the Chronos cutover: PIN_PREDICATE / pin_params /
# the legacy *_relation helpers returned the bitemporal pin predicate
# and legacy SQL relations for the MCP read path. All MCP/operator reads
# now go through a Chronos checkout (mcp/tools.py::_chronos_checkout over
# okg.graph_nodes/graph_edges), so these helpers have no callers and
# were removed.


@dataclass(frozen=True)
class Session:
    """Immutable session handle.

    All fields are pinned at `open_session()` time. A later
    publish of generation N+1 does NOT update any field — the
    point of the session is that they don't change.

    `db_fingerprint` ties the session to the database it was
    opened against. Passing this session to a tool with a
    connection to a different DB raises loudly.

    `branch_name` / `branch_id` (AKMON Phase A.3) identify the
    substrate branch the session reads from. Default is the
    deployment's `default` branch. Non-default branches must
    exist in `okg.branches` before the session opens.
    """

    deployment_name: str
    generation_id: str
    catalog_version_id: int
    db_fingerprint: str
    branch_name: str = "default"
    branch_id: str = "default"
    # The branch the PINNED GENERATION was actually published on (issue
    # #1437). Normally the same as `branch_name`; it differs when a fresh
    # fork with no published head of its own reads through to its
    # default-branch base. `None` means "not recorded" (a directly
    # constructed session), never "same as branch_name" -- an unrecorded
    # value must not read as agreement.
    generation_branch_name: str | None = None
    compat_generation_id: int | None = None
    nomos_subject: str | None = None
    nomos_identity_claim: Mapping[str, Any] | None = None
    nomos_access_context: Mapping[str, Any] | None = None
    # PACT mcp-principal-mapping: multi-user mode is an EXPLICIT server state
    # chosen at start, carried onto every session the server opens. It is a
    # session FACT, not an inference from whether a principal header happened
    # to arrive -- a client that simply stops sending the header must not
    # silently return the server to shared-principal behavior. The read
    # filter reads it to refuse instead of coalescing to the environment
    # subject.
    nomos_multi_user: bool = False
    # PACT operator-ui-search-view: the read surface this session's reads come
    # through. The Nomos row filter records its decisions under this action and
    # surface, so a read the operator UI makes is audited as the UI rather than
    # as MCP. `None` keeps the filter's historical MCP identity for every
    # session that does not name one.
    nomos_read_action: str | None = None
    nomos_read_surface: str | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "deployment_name": self.deployment_name,
            "generation_id": self.generation_id,
            "catalog_version_id": self.catalog_version_id,
            "branch_name": self.branch_name,
            "branch_id": self.branch_id,
        }
        # Name the branch the generation actually came from whenever it is
        # known. A fork session reading through to its default base said
        # only "branch_name: <fork>" before, which asserted a scope the
        # read did not have (issue #1437).
        if self.generation_branch_name is not None:
            payload["generation_branch_name"] = self.generation_branch_name
            payload["generation_branch_matches_session"] = (
                self.generation_branch_name == self.branch_name
            )
        if self.nomos_subject:
            payload["nomos_subject"] = self.nomos_subject
        if self.nomos_identity_claim:
            payload["nomos_identity_claim_present"] = True
        if self.nomos_multi_user:
            payload["nomos_multi_user"] = True
        if self.nomos_read_action:
            payload["nomos_read_action"] = self.nomos_read_action
        if self.nomos_access_context:
            payload["nomos_access_context_present"] = True
            context_id = self.nomos_access_context.get("context_id")
            if context_id is not None:
                payload["nomos_access_context_id"] = str(context_id)
        return payload


def apply_branch_pin(conn: psycopg.Connection, session: Session) -> None:
    """Set the `okg.pinned_branch_id` session GUC for the layered
    views so reads against `okg.v_branch_layered_nodes` /
    `okg.v_branch_layered_edges` return rows from the session's
    branch (with the default branch as fallback for missing
    overrides).

    Idempotent and cheap (one round-trip). Safe to call before
    every tool entry. The setting persists for the connection's
    session lifetime (set_config(..., false)) so successive tool
    calls on the same conn don't need to reset it.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('okg.pinned_branch_id', %s, false)",
            (str(session.branch_id),),
        )


def _db_fingerprint(conn: psycopg.Connection) -> str:
    """Short identity string for the physical DB the conn is
    bound to.

    Combines `current_database()` with the cluster's
    `system_identifier` (from pg_control_system). The cluster id
    is stable across restarts, unique per cluster, and cheap to
    query. A datname collision across two clusters is resolved
    by including the cluster id; a schema-rename within one
    cluster would keep the same fingerprint, which is the
    desired behavior (same substrate).

    Cached on the connection via a private attribute so
    repeated calls from the same conn pay one round-trip.
    """
    cached = getattr(conn, _FP_ATTR, None)
    if cached is not None:
        return cached
    with conn.cursor() as cur:
        cur.execute(
            "SELECT current_database(), system_identifier FROM pg_control_system()"
        )
        dbname, sysid = cur.fetchone()
    fp = f"{dbname}:{sysid}"
    try:
        setattr(conn, _FP_ATTR, fp)
    except Exception:  # noqa: BLE001 — connection wrapper may forbid setattr (e.g. __slots__); fp still returned, just lose the per-conn cache speedup.
        logger.debug(
            "connection wrapper disallowed session fingerprint cache", exc_info=True
        )
    return fp


def assert_session_matches(
    conn: psycopg.Connection,
    session: Session,
) -> None:
    """Raise if `conn` is bound to a different DB than the one
    the session was opened against.

    Every tool entry calls this before reading. Cheap (cached on
    the conn), and the cost of the silent-wrong-answer bug it
    prevents is unbounded.
    """
    fp = _db_fingerprint(conn)
    if fp != session.db_fingerprint:
        raise SubstrateError(
            f"session was opened against a different database "
            f"(session fp={session.db_fingerprint!r}, "
            f"conn fp={fp!r}); tools must receive a connection "
            f"to the same DB as `open_session`",
            data={
                "session_fingerprint": session.db_fingerprint,
                "conn_fingerprint": fp,
                "deployment_name": session.deployment_name,
                "generation_id": session.generation_id,
            },
            fix_hint="re-open the session against the same DSN "
            "you're passing to the tool",
        )


#: Words agents send as `generation_id` for "the newest one" (#3470). None
#: is a generation id; omitting the argument already means the latest.
_LATEST_WORDS = frozenset({"latest", "current", "newest", "head", "now"})

#: Checkpoint ids read for a cut-off id: the refusal path only. More than
#: this many ids sharing the prefix is reported as too many to name one.
_PREFIX_CANDIDATES = 50


def _ids_starting_with(
    conn: psycopg.Connection,
    stems: list[str],
    *,
    deployment_name: str,
    branch_name: str,
) -> tuple[list[str], str]:
    """The published generation ids on this deployment and branch that start
    with one of `stems`, and how the check went: "checked", "too_many" or
    "unavailable". One bounded read of checkpoint ids; each candidate is then
    resolved in the session's own scope, so an id from another deployment or
    branch is never named."""
    from okg.substrate.generation_resolver import resolve_okg_generation

    patterns = [
        stem.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        for stem in stems
    ]
    try:
        # A savepoint, so a failed read never aborts the caller's transaction.
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "SELECT checkpoint_id FROM public._chronos_branch_interval_checkpoints"
                " WHERE checkpoint_id LIKE ANY(%s) LIMIT %s",
                (patterns, _PREFIX_CANDIDATES + 1),
            )
            candidates = [str(row[0]) for row in cur.fetchall()]
        if len(candidates) > _PREFIX_CANDIDATES:
            return [], "too_many"
        found = []
        for candidate in candidates:
            if candidate in stems:
                continue
            if resolve_okg_generation(
                conn, deployment_name=deployment_name, requested=candidate,
                branch=branch_name, status="published",
            ) is not None:
                found.append(candidate)
    except Exception:  # noqa: BLE001 - diagnosis only, never the answer
        logger.debug("checking a cut-off generation id failed", exc_info=True)
        return [], "unavailable"
    return sorted(set(found)), "checked"


def _unpublished_generation(
    conn: psycopg.Connection,
    deployment_name: str,
    requested: int | str,
    branch_name: str,
) -> SubstrateError:
    """The refusal for a pinned generation that resolves to nothing, with
    the exact fix (#3470): a word such as "latest" is told to omit
    `generation_id`, and the start of exactly one published id on this
    branch is told that id. Neither is accepted in its place."""
    from okg.substrate.errors import GenerationNotFound

    data: dict[str, Any] = {
        "deployment_name": deployment_name,
        "requested_generation": requested,
        "session_branch": branch_name,
    }
    message = (
        f"requested generation {requested!r} is not a published "
        f"OKG generation for deployment {deployment_name!r} "
        f"on branch {branch_name!r}"
    )
    fix_hint = (
        "pin a generation published on this session's "
        "branch, or omit generation_id to use the latest"
    )
    text = str(requested).strip()
    if text.lower() in _LATEST_WORDS:
        message += f"; {text!r} is not a generation id"
        fix_hint = (
            "omit generation_id (or pass null) to read the latest published "
            "generation; the response header names the id it read"
        )
        return GenerationNotFound(message, data=data, fix_hint=fix_hint)
    stem = text.rstrip(".…").strip()
    if stem and isinstance(requested, str):
        # An agent that cut the id may also have dropped its `okg:` start.
        stems = [stem] if stem.startswith("okg:") else [stem, "okg:" + stem]
        matches, check = _ids_starting_with(
            conn, stems, deployment_name=deployment_name, branch_name=branch_name,
        )
        data["prefix_check"] = check
        if len(matches) == 1:
            data["matching_generation"] = matches[0]
            message += "; it is the start of one published id"
            fix_hint = f"pass the whole id: generation_id={matches[0]!r}"
        elif len(matches) > 1:
            data["matching_generations"] = matches[:5]
            message += f"; it is the start of {len(matches)} published ids"
    return GenerationNotFound(message, data=data, fix_hint=fix_hint)


def resolve_generation(
    conn: psycopg.Connection,
    deployment_name: str,
    *,
    requested: Optional[int | str] = None,
    branch_name: str = "default",
) -> tuple[str, int, int | None, str]:
    """Return (generation_id, catalog_version_id, compat_generation_id,
    generation_branch) for the requested pin, or the latest published
    generation if `requested is None`.

    `generation_branch` is the branch the resolved checkpoint was
    published on. It equals `branch_name` except on the documented
    read-through: a fresh fork with no published head of its own resolves
    to its default-branch base. Callers record it so a session can name
    the branch it actually read rather than the branch it asked for
    (issue #1437).

    Raises SubstrateError if no matching generation exists.
    """
    from okg.substrate.generation_resolver import resolve_okg_generation

    # Read-only: resolving the pinned generation must not DDL (PACT
    # least-privilege-mcp — the MCP read role lacks CREATE).
    resolved = resolve_okg_generation(
        conn,
        deployment_name=deployment_name,
        requested=requested,
        branch=branch_name,
        status="published",
        allow_default_branch_fallback=True,
    )
    if resolved is None:
        if requested is not None:
            raise _unpublished_generation(
                conn, deployment_name, requested, branch_name,
            )
        raise SubstrateError(
            f"no published generation for deployment "
            f"{deployment_name!r}; has a publish cycle completed?",
            data={"deployment_name": deployment_name},
            fix_hint=(
                "run `okg run --once --deployment <name> --apply` or start "
                "`okg runtime worker --deployment <name>` for DBOS schedules"
            ),
        )
    cv = resolved.catalog_version_id
    if cv is None:
        # No catalog version on the checkpoint metadata AND no applied
        # catalog row to fall back to — a default of 0 here would silently
        # pin reads to a nonexistent catalog. Fail loud instead.
        raise SubstrateError(
            f"resolved generation {resolved.generation_id!r} for deployment "
            f"{deployment_name!r} carries no catalog_version_id and no "
            "applied catalog version exists to fall back to",
            data={
                "deployment_name": deployment_name,
                "generation_id": resolved.generation_id,
            },
            fix_hint=(
                "run `okg catalog load --apply --deployment <name>` so an "
                "applied catalog version exists, then re-publish"
            ),
        )
    return (
        resolved.generation_id,
        int(cv),
        resolved.target_generation_id,
        str(resolved.branch or branch_name),
    )


def open_session(
    conn: psycopg.Connection,
    *,
    deployment_name: str = DEFAULT_DEPLOYMENT,
    requested_generation: Optional[int | str] = None,
    branch_name: str = "default",
    nomos_subject: str | None = None,
    nomos_identity_claim: Mapping[str, Any] | None = None,
    nomos_access_context: Mapping[str, Any] | None = None,
    nomos_multi_user: bool = False,
    nomos_read_action: str | None = None,
    nomos_read_surface: str | None = None,
) -> Session:
    """Open an MCP session against the given deployment.

    If `requested_generation` is None, pins to the latest
    published generation. Otherwise pins to the requested
    generation (must be status='published').

    `branch_name` selects the substrate branch the session reads
    from (default: 'default'). Non-default branches must exist in
    `okg.branches` for the deployment; an unknown branch raises a
    SubstrateError pointing the operator at `okg branch create`.

    Captures the DB fingerprint so tools later verify the caller
    isn't accidentally using a different DB's connection.
    """
    branch_id = _resolve_branch_id(conn, deployment_name, branch_name)
    gen_id, cv_id, compat_generation_id, generation_branch = resolve_generation(
        conn,
        deployment_name,
        requested=requested_generation,
        branch_name=branch_name,
    )
    return Session(
        deployment_name=deployment_name,
        generation_id=gen_id,
        catalog_version_id=cv_id,
        db_fingerprint=_db_fingerprint(conn),
        branch_name=branch_name,
        branch_id=branch_id,
        generation_branch_name=generation_branch,
        compat_generation_id=compat_generation_id,
        nomos_subject=nomos_subject,
        nomos_identity_claim=nomos_identity_claim,
        nomos_access_context=nomos_access_context,
        nomos_multi_user=nomos_multi_user,
        nomos_read_action=nomos_read_action,
        nomos_read_surface=nomos_read_surface,
    )


def _resolve_branch_id(
    conn: psycopg.Connection,
    deployment_name: str,
    branch_name: str,
) -> str:
    """Resolve an OKG branch through Chronos, read-only.

    The MCP read path runs under a least-privilege role (PACT
    ``least-privilege-mcp``) that lacks CREATE on the schema, so branch
    resolution must never DDL: it probes the already-provisioned branch
    rather than ensuring/creating it. Provisioning the default branch is a
    privileged step (``okg provision`` / publish), not a side effect of
    opening a read session. Using ``ensure_schema=False`` keeps the Chronos
    context from running ``ensure_metadata`` / ``ensure_branch`` DDL that the
    read role can't execute (the actual graph-read path in ``tools.py``
    already opens the store this way).
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    try:
        info = store.context.get_branch(branch_name)
    except Exception as exc:
        raise SubstrateError(
            f"branch {branch_name!r} not found for deployment {deployment_name!r}",
            fix_hint=(
                "provision the deployment (okg provision / publish) so the "
                "branch exists before opening a read session"
            ),
            data={
                "deployment_name": deployment_name,
                "branch_name": branch_name,
            },
        ) from exc
    # The default branch is unowned/shared; only enforce ownership for
    # explicitly-named branches.
    if branch_name != "default" and info.metadata.get("deployment_name") not in {
        None,
        deployment_name,
    }:
        raise SubstrateError(
            f"branch {branch_name!r} belongs to deployment "
            f"{info.metadata.get('deployment_name')!r}, not {deployment_name!r}",
            data={"branch_metadata": info.metadata},
        )
    return branch_name


def _latest_catalog_version_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        raise SubstrateError(
            "no applied catalog_version; okg migrate not run or catalog never loaded",
            fix_hint="run `okg migrate` then `okg catalog load`",
        )
    return int(row[0])


def advance_session(
    conn: psycopg.Connection,
    session: Session,
    *,
    to: int | str = "latest",
) -> Session:
    """Return a NEW Session pinned at `to`.

    `to` is either an int generation_id (must be published for
    the deployment) or the string "latest" (resolves to max
    published). The original `session` is unaffected — this is
    a pure function.

    Preserves:
      - deployment_name (advance is always within a deployment)
      - db_fingerprint (advance is always on the same conn/DB;
        an assert here catches accidental cross-DB advance)

    Refuses to return a pin older than the current session (the
    substrate's design point is *moving forward through the fact
    log*; rewinding is a separate operation, not named here).
    """
    assert_session_matches(conn, session)

    if to == "latest":
        gen_id, cv_id, compat_generation_id, generation_branch = resolve_generation(
            conn,
            session.deployment_name,
            requested=None,
            branch_name=session.branch_name,
        )
    elif isinstance(to, (int, str)):
        gen_id, cv_id, compat_generation_id, generation_branch = resolve_generation(
            conn,
            session.deployment_name,
            requested=to,
            branch_name=session.branch_name,
        )
    else:
        raise ValueError(
            f"advance_session: `to` must be an int generation_id "
            f"or the string 'latest'; got {to!r}"
        )

    order = _generation_order(
        conn,
        deployment_name=session.deployment_name,
        branch_name=session.branch_name,
    )
    if session.generation_id in order and gen_id in order:
        if order[gen_id] < order[session.generation_id]:
            raise SubstrateError(
                f"advance_session: target generation {gen_id} is "
                f"older than the current pin {session.generation_id}; "
                f"this operation only moves forward",
                data={
                    "from": session.generation_id,
                    "to": gen_id,
                    "deployment_name": session.deployment_name,
                },
                fix_hint="open a fresh session at the older generation "
                "via open_session(requested_generation=...)",
            )

    return Session(
        deployment_name=session.deployment_name,
        generation_id=gen_id,
        catalog_version_id=cv_id,
        db_fingerprint=session.db_fingerprint,
        branch_name=session.branch_name,
        branch_id=session.branch_id,
        generation_branch_name=generation_branch,
        compat_generation_id=compat_generation_id,
        nomos_subject=session.nomos_subject,
        nomos_identity_claim=session.nomos_identity_claim,
        nomos_access_context=session.nomos_access_context,
        nomos_multi_user=session.nomos_multi_user,
        nomos_read_action=session.nomos_read_action,
        nomos_read_surface=session.nomos_read_surface,
    )


def _generation_order(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    generations = store.list_generations(
        deployment_name=deployment_name,
        branch=branch_name,
        status="published",
    )
    return {generation.checkpoint_id: idx for idx, generation in enumerate(generations)}


# Delta-row budget above which preview_advance will not run the exact
# per-row Nomos filter under `enforce` mode (same rationale as the
# inspect(graph)/inspect(subtype)/aggregate(edge) budgets).
_PREVIEW_ADVANCE_EXACT_ROW_BUDGET = 5_000

# Content hash covering every field the Nomos row decision can depend on:
# attrs (classification + lineage attr), source, source_record_id,
# source_revision (as_of horizon facts). Rows whose hash matches on both
# sides receive identical decisions post-#635 (no session field that differs
# between the pins enters the decision), so they cancel out of the delta.
# jsonb_build_array gives an unambiguous field encoding — a delimiter join
# could let crafted values containing the delimiter collide two different
# contents and drop a changed row from the enforce delta.
_ROW_CONTENT_HASH_SQL = (
    "md5(jsonb_build_array(attrs, source, source_record_id, "
    "source_revision)::text)"
)


def preview_advance(
    conn: psycopg.Connection,
    session: Session,
    *,
    to: int | str = "latest",
) -> dict:
    """Delta summary of what ``advance_session(session, to=...)``
    WOULD produce, without actually producing it.

    Computes a bitemporal diff between ``session.generation_id``
    and the resolved target generation. Used for "ask before
    advance" UX: the agent sees how much changed and decides
    whether it's worth the cognitive reset.

    Bounds + visibility (issue #627): each side is scanned once through its
    own pinned checkout projecting only (id, content-hash) and the two maps
    are diffed in Python — neither generation is materialized. Under
    `off`/`audit` (no row can hide) the added/retracted counts are the
    id-set diff, exact at any size. Under `enforce` the per-row Nomos
    filter runs only over the delta rows (ids on exactly one side, plus ids
    whose content hash differs — rows identical on both sides receive
    identical decisions and cancel exactly); changed-hash ids are filtered
    as both content versions so a visibility flip counts as added or
    retracted. Above the delta budget the counts are omitted (null,
    ``truncated=True``, a ``note``) so hidden-source cardinality cannot
    leak. A probe failure raises — no fallback to full materialization.
    """
    from_gen = session.generation_id
    target = advance_session(conn, session, to=to)
    to_gen = target.generation_id

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    from_graph = store.checkout_generation(from_gen, branch=session.branch_name)
    to_graph = store.checkout_generation(to_gen, branch=session.branch_name)
    mode = deployment_nomos_enforcement_mode(session.deployment_name)

    def _id_hash_map(graph: Any, *, kind: str) -> dict[str, str]:
        id_col = "node_id" if kind == "node" else "edge_id"
        table = "okg.graph_nodes" if kind == "node" else "okg.graph_edges"
        return {
            str(row["row_id"]): str(row["content_hash"])
            for row in graph.query(
                f"SELECT {id_col} AS row_id, "
                f"{_ROW_CONTENT_HASH_SQL} AS content_hash FROM {table}",
            )
        }

    from_node_map = _id_hash_map(from_graph, kind="node")
    to_node_map = _id_hash_map(to_graph, kind="node")
    from_edge_map = _id_hash_map(from_graph, kind="edge")
    to_edge_map = _id_hash_map(to_graph, kind="edge")

    if mode != "enforce":
        n_added = len(to_node_map.keys() - from_node_map.keys())
        n_retracted = len(from_node_map.keys() - to_node_map.keys())
        e_added = len(to_edge_map.keys() - from_edge_map.keys())
        e_retracted = len(from_edge_map.keys() - to_edge_map.keys())
    else:
        node_delta = _delta_candidates(from_node_map, to_node_map)
        edge_delta = _delta_candidates(from_edge_map, to_edge_map)
        delta_size = sum(
            len(side)
            for delta in (node_delta, edge_delta)
            for side in delta
        )
        if delta_size > _PREVIEW_ADVANCE_EXACT_ROW_BUDGET:
            return {
                "from_generation_id": from_gen,
                "to_generation_id": to_gen,
                "would_advance": from_gen != to_gen,
                "added_node_count": None,
                "retracted_node_count": None,
                "added_edge_count": None,
                "retracted_edge_count": None,
                "truncated": True,
                "note": (
                    "generation delta exceeds the exact-count budget under "
                    "Nomos enforce mode; counts are omitted so "
                    "hidden-source cardinality cannot leak — advance and "
                    "use search/filter for visibility-accurate reads"
                ),
            }
        n_added, n_retracted = _visible_delta_counts(
            conn,
            from_session=session,
            to_session=target,
            from_graph=from_graph,
            to_graph=to_graph,
            delta=node_delta,
            kind="node",
        )
        e_added, e_retracted = _visible_delta_counts(
            conn,
            from_session=session,
            to_session=target,
            from_graph=from_graph,
            to_graph=to_graph,
            delta=edge_delta,
            kind="edge",
        )

    return {
        "from_generation_id": from_gen,
        "to_generation_id": to_gen,
        "would_advance": from_gen != to_gen,
        "added_node_count": int(n_added or 0),
        "retracted_node_count": int(n_retracted or 0),
        "added_edge_count": int(e_added or 0),
        "retracted_edge_count": int(e_retracted or 0),
        "truncated": False,
    }


def _delta_candidates(
    from_map: dict[str, str],
    to_map: dict[str, str],
) -> tuple[set[str], set[str], set[str]]:
    """Split two (id -> content-hash) maps into (from_only, to_only, changed)."""
    from_ids = from_map.keys()
    to_ids = to_map.keys()
    changed = {
        row_id
        for row_id in from_ids & to_ids
        if from_map[row_id] != to_map[row_id]
    }
    return (set(from_ids - to_ids), set(to_ids - from_ids), changed)


def _visible_delta_counts(
    conn: psycopg.Connection,
    *,
    from_session: Session,
    to_session: Session,
    from_graph: Any,
    to_graph: Any,
    delta: tuple[set[str], set[str], set[str]],
    kind: str,
) -> tuple[int, int]:
    """Exact enforce-mode (added, retracted) counts over the delta rows.

    One-sided ids are filtered as that side's content; changed ids are
    filtered as BOTH content versions (a hidden->visible flip is an add, a
    visible->hidden flip is a retraction). Rows outside the delta are
    identical on both sides and cancel, so this reproduces the legacy
    full-materialization semantics exactly.
    """
    from okg.substrate.nomos.read_filter import (
        filter_edge_rows_for_session,
        filter_node_rows_for_session,
    )

    from_only, to_only, changed = delta
    id_col = "node_id" if kind == "node" else "edge_id"
    table = "okg.graph_nodes" if kind == "node" else "okg.graph_edges"

    def _rows(graph: Any, ids: set[str]) -> list[dict]:
        if not ids:
            return []
        columns = f"{id_col}, attrs, source, source_record_id, source_revision"
        if kind == "node":
            columns = "subtype, " + columns
        # The id maps key by str() (edge_id is BIGINT, node_id TEXT); cast
        # the column so the text ids bind uniformly for both kinds.
        return [
            dict(row)
            for row in graph.query(
                f"SELECT {columns} FROM {table} "
                f"WHERE {id_col}::text = ANY(:ids)",
                {"ids": sorted(ids)},
            )
        ]

    def _visible_ids(session_obj: Session, rows: list[dict]) -> set[str]:
        if kind == "node":
            visible = filter_node_rows_for_session(conn, session_obj, rows)
        else:
            visible = filter_edge_rows_for_session(conn, session_obj, rows)
        return {str(row[id_col]) for row in visible}

    visible_from = _visible_ids(
        from_session, _rows(from_graph, from_only | changed),
    )
    visible_to = _visible_ids(
        to_session, _rows(to_graph, to_only | changed),
    )
    added = len({rid for rid in to_only if rid in visible_to}) + len(
        {rid for rid in changed if rid in visible_to and rid not in visible_from}
    )
    retracted = len({rid for rid in from_only if rid in visible_from}) + len(
        {rid for rid in changed if rid in visible_from and rid not in visible_to}
    )
    return added, retracted

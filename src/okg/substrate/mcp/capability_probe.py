"""What this MCP server can actually run, probed once at start (#3151 slice 1).

Before this module every deployment published the same `search` schema: all
five methods, a free `select` list, and every `rank_by` value. A server with
no embedder still advertised `semantic`, and a private deployment's rerun spent 57
calls learning that from refusals. The published surface now follows what
the server can run:

- `probe_capabilities` reads a few small facts at start: whether an embedder
  is configured (config), and, at the catalog version the server would pin,
  whether any alias is registered and which subtypes declare a passage
  rollup (two catalog reads). It never scans node data: the two full scans
  the design names (`semantic_readiness.probe_semantic_readiness` and the
  schema digest's attribute-key pass) are deliberately not used.
- `compile_tool_surface` narrows what `tools/list` SHOWS for `search`: the
  `method` enum, an items enum on `select`, and `rank_by`. The validator
  keeps the full models, so a client with a cached schema still reaches the
  named refusal instead of a schema error -- the same split
  `publish_narrowed_inspect_target` makes for `inspect` kinds.
- `assert_search_capability` is that refusal. It runs at the top of `search`,
  before any database work, and follows the probe: the surface is fixed for
  the life of the process (decision 1a), so a capability that appears after
  start stays refused until restart, and the served text always describes
  the behavior.
- `render_unavailable_lines` is the generated "not available here" block the
  server appends to its instructions. It states what the tools do not do on
  this server, never which method to use instead (the benchmark fairness
  rule).

Probe outcomes (decision 2): a read that ERRORS refuses start with
`agent_surface_probe_failed`, naming the read. A read that hits its statement
timeout publishes the capability as unknown, which compiles exactly like
unavailable plus a "not verified at start" reason. Of the capabilities this
version probes, none is published as available without a read or a config
fact behind it; `lexical`, `identifier_mentions`, `attrs` and `grep` are not
probed yet and are published as before (source `not_probed`).

Under `OKG_MCP_SURFACE_PROBE=1` there is no database. The capability record
then comes from a named fixture (`OKG_MCP_CAPABILITY_FIXTURE`), or, with no
fixture named, from config alone, with every database fact marked unknown
("not probed"). That is not a guess: unknown compiles as unavailable.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping, Optional, get_args

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import search_views
from okg.substrate.mcp.operator_models import SearchMethod

__all__ = [
    "AVAILABLE",
    "CAPABILITY_FIXTURE_ENV",
    "CAPABILITY_FIXTURES",
    "Capability",
    "CapabilityFixtureInvalid",
    "CapabilityProbeFailed",
    "GeneratedInstructionsOverBudget",
    "GENERATED_INSTRUCTIONS_BUDGET_BYTES",
    "NOT_AVAILABLE_HEADER",
    "PROBE_STATEMENT_TIMEOUT_MS",
    "SEARCH_METHODS",
    "SEARCH_RANK_BY_VALUES",
    "SearchMethodUnavailable",
    "SearchRankByUnavailable",
    "SearchSchemaUnreachable",
    "ServerCapabilities",
    "UNAVAILABLE",
    "UNKNOWN",
    "assert_search_capability",
    "capabilities_for_surface_probe",
    "compile_tool_surface",
    "compose_instructions",
    "probe_capabilities",
    "probe_capabilities_from_pool",
    "render_unavailable_lines",
    "split_generated_instructions",
]

AVAILABLE = "available"
UNAVAILABLE = "unavailable"
UNKNOWN = "unknown"

#: Every method the validator accepts, in the order the schema publishes them.
SEARCH_METHODS: tuple[str, ...] = tuple(get_args(SearchMethod))
#: Every `rank_by` value the validator accepts (`server.search`'s literal).
SEARCH_RANK_BY_VALUES: tuple[str, ...] = ("records", "self", "passages")

#: Each probe read runs under its own statement timeout. The reads are index
#: or catalog lookups that stop at the first row, so a timeout means a sick
#: database, and the capability then reads "unknown", never "available".
PROBE_STATEMENT_TIMEOUT_MS = 2_000

#: Names a capability fixture for a hermetic (`OKG_MCP_SURFACE_PROBE`) build.
CAPABILITY_FIXTURE_ENV = "OKG_MCP_CAPABILITY_FIXTURE"

#: Decision 12 (b): okg's fixed text keeps its own 4,000 B ceiling; the
#: generated part gets this separate budget.
GENERATED_INSTRUCTIONS_BUDGET_BYTES = 2_000

#: The first line of the generated block. Also how the authored part is told
#: apart from the generated part in served text (`split_generated_instructions`).
NOT_AVAILABLE_HEADER = (
    "Not available on this server (checked when it started; fixed until "
    "restart):"
)
_GENERATED_SEPARATOR = "\n\n"


class CapabilityProbeFailed(SubstrateError):
    """A start-time capability read failed, so the server refuses to start.

    Decision 2 (a): an ERROR names the read and stops the launch. Publishing
    a guess would serve a surface nobody verified; publishing "unavailable"
    would hide a broken database behind a smaller schema.
    """

    code = "agent_surface_probe_failed"


class CapabilityFixtureInvalid(SubstrateError):
    """`OKG_MCP_CAPABILITY_FIXTURE` names no fixture, or is set on a live server."""

    code = "agent_surface_capability_fixture_invalid"


class GeneratedInstructionsOverBudget(SubstrateError):
    """The generated capability lines exceed their own byte budget."""

    code = "agent_surface_generated_instructions_over_budget"


class SearchMethodUnavailable(SubstrateError):
    """A `search` call named a method this server's probe found unavailable."""

    code = "search_method_unavailable"


class SearchRankByUnavailable(SubstrateError):
    """A `search` call named a `rank_by` value this server cannot serve."""

    code = "search_rank_by_unavailable"


class SearchSchemaUnreachable(SubstrateError):
    """The registered `search` tool's published schema could not be reached."""

    code = "search_schema_unreachable"


#: A search method with at most this many selectable fields, and too few
#: shared with the first method to be given as a difference, is listed whole
#: in the `select` description (PACT select-fields-by-method).
_SELECT_LISTED_WHOLE_MAX = 6


@dataclass(frozen=True)
class Capability:
    """One fact about what the server can run, and where it came from."""

    status: str
    reason: str
    #: `config`, `catalog`, `fixture`, or `not_probed`.
    source: str

    @property
    def available(self) -> bool:
        return self.status == AVAILABLE

    def to_json(self) -> dict[str, str]:
        return {"status": self.status, "reason": self.reason, "source": self.source}


@dataclass(frozen=True)
class ServerCapabilities:
    """The probe record. Frozen: it is computed once per process."""

    methods: tuple[tuple[str, Capability], ...]
    rank_by: tuple[tuple[str, Capability], ...]
    #: `live`, `fixture:<name>`, or `surface_probe_unprobed`.
    mode: str
    rollup_subtypes: tuple[str, ...] = ()
    #: Selectable `search` fields per method, from the code constants.
    select_fields: tuple[tuple[str, tuple[str, ...]], ...] = ()
    #: What the probe read: generation and catalog version, or None.
    pinned: Optional[Mapping[str, Any]] = None
    #: Wall time of the probe's database reads, or None when none ran.
    probe_ms: Optional[float] = None
    reads: tuple[str, ...] = field(default=())

    def method(self, name: str) -> Capability:
        for key, cap in self.methods:
            if key == name:
                return cap
        return Capability(UNKNOWN, f"{name!r} is not a search method", "config")

    def rank_by_value(self, value: str) -> Capability:
        for key, cap in self.rank_by:
            if key == value:
                return cap
        return Capability(UNKNOWN, f"{value!r} is not a rank_by value", "config")

    def available_methods(self) -> tuple[str, ...]:
        return tuple(name for name, cap in self.methods if cap.available)

    def available_rank_by(self) -> tuple[str, ...]:
        return tuple(value for value, cap in self.rank_by if cap.available)

    def published_select_fields(self) -> tuple[str, ...]:
        """The union of the available methods' selectable fields, sorted."""
        available = set(self.available_methods())
        fields: set[str] = set()
        for method, names in self.select_fields:
            if method in available:
                fields.update(names)
        return tuple(sorted(fields))

    def select_fields_description(self) -> str:
        """Which `select` fields each available method returns (PACT
        select-field-instructions). The published items enum is the union
        over methods, so it alone names fields a method refuses (`handle`
        to grep, `id_type` to lexical). The first available method is
        listed whole, `node_id` first. A method that shares most of its
        fields with it is given as the difference, the fields it LACKS
        first (PACT select-fields-by-method: agents reused a lexical
        `select` on grep and were refused `handle`). A method with at most
        `_SELECT_LISTED_WHOLE_MAX` fields of its own is listed whole, so
        `identifier_mentions` says it has no `node_id`. The rest are named,
        and a refusal lists their fields (the read-surface token ceiling
        leaves no room to list them all)."""
        available = [
            (method, tuple(names)) for method, names in self.select_fields
            if method in set(self.available_methods()) and names
        ]
        if not available:
            return ""

        def ordered(names: Iterable[str]) -> str:
            return ", ".join(sorted(set(names), key=lambda n: (n != "node_id", n)))

        base_method, base = available[0]
        parts = [f"Fields differ by method. {base_method}: {ordered(base)}."]
        others: list[str] = []
        for method, names in available[1:]:
            missing = set(base) - set(names)
            extra = set(names) - set(base)
            if missing and len(missing) + len(extra) < len(set(names)) // 2:
                parts.append(
                    f"{method} has no {ordered(missing)}; otherwise as "
                    f"{base_method}"
                    + (f", plus {ordered(extra)}." if extra else ".")
                )
            elif len(set(names)) <= _SELECT_LISTED_WHOLE_MAX:
                parts.append(f"{method}: only {ordered(names)}.")
            else:
                others.append(method)
        if others:
            parts.append(
                f"{', '.join(others)}: a refusal lists their fields."
            )
        return " ".join(parts)

    def with_select_fields(
        self, select_fields: Mapping[str, Iterable[str]],
    ) -> "ServerCapabilities":
        return replace(
            self,
            select_fields=tuple(
                (method, tuple(select_fields.get(method, ())))
                for method in SEARCH_METHODS
            ),
        )

    def published_views(self) -> tuple[str, ...]:
        """The `view` values the published search schema lists."""
        matrix = search_views.view_matrix(
            list(self.available_methods()), list(self.available_rank_by()),
        )
        # The query-less lookup's list (issue #3666) adds no value to the
        # published schema: it is `list` only where the schema already
        # lists it, so a server without the records rank mode publishes
        # what it did. The validator accepts the value either way.
        return tuple(
            view for view in search_views.VIEWS
            if any(
                view in views
                for method, cells in matrix.items() for views in cells.values()
                if not (method == "attrs" and view == "list")
            )
        )

    def to_report(self) -> dict[str, Any]:
        """The `capabilities` block of `inspect(kind="surface")`."""
        return {
            "mode": self.mode,
            "fixed_for_process": True,
            "search": {
                "methods": {name: cap.to_json() for name, cap in self.methods},
                "published_methods": list(self.available_methods()),
                "rank_by": {value: cap.to_json() for value, cap in self.rank_by},
                "published_rank_by": list(self.available_rank_by()),
                "rollup_subtypes": list(self.rollup_subtypes),
                "select_fields": {
                    method: list(names) for method, names in self.select_fields
                },
                "published_select_fields": list(self.published_select_fields()),
                # PACT read-model-s4-list: method -> rank_by -> views, as
                # this server runs them, and each view's own row budget.
                "views": search_views.view_matrix(
                    list(self.available_methods()), list(self.available_rank_by()),
                ),
                "published_views": list(self.published_views()),
                "list": {
                    "method": "lexical", "rank_by": "records",
                    "default_limit": search_views.LIST_LIMIT,
                    "maximum_limit": search_views.LIST_MAX_LIMIT,
                    "select_fields": list(search_views.LIST_FIELDS),
                    # grep's list (#3659): fixed rows; a select keeps some.
                    "grep": {"row_fields": list(search_views.GREP_LIST_FIELDS)},
                },
                "cards": {"default_limit": search_views.CARDS_LIMIT},
            },
            "pinned": dict(self.pinned) if self.pinned is not None else None,
            "probe_ms": self.probe_ms,
            "reads": list(self.reads),
        }


# ── building a record ─────────────────────────────────────────────────


def _semantic_from_config(embedder_configured: bool) -> Capability:
    if embedder_configured:
        return Capability(AVAILABLE, "an embedder is configured", "config")
    return Capability(UNAVAILABLE, "no embedder is configured", "config")


def _record(
    *,
    semantic: Capability,
    alias: Capability,
    passages: Capability,
    mode: str,
    rollup_subtypes: tuple[str, ...] = (),
    pinned: Optional[Mapping[str, Any]] = None,
    probe_ms: Optional[float] = None,
    reads: tuple[str, ...] = (),
    static_source: str = "config",
) -> ServerCapabilities:
    """Assemble a record. Methods slice 1 does not probe are published as
    before and say so: `lexical`, `identifier_mentions`, `attrs` and `grep` keep their
    call-time refusals."""
    unprobed = Capability(
        AVAILABLE, "published as before; not probed in this version",
        "fixture" if static_source == "fixture" else "not_probed",
    )
    by_method = {
        "lexical": unprobed,
        "semantic": semantic,
        "alias": alias,
        "identifier_mentions": unprobed,
        "attrs": unprobed,
        "grep": unprobed,
    }
    by_rank = {
        "records": Capability(AVAILABLE, "lexical default", static_source),
        "self": Capability(AVAILABLE, "always available", static_source),
        "passages": passages,
    }
    return ServerCapabilities(
        methods=tuple((m, by_method[m]) for m in SEARCH_METHODS),
        rank_by=tuple((v, by_rank[v]) for v in SEARCH_RANK_BY_VALUES),
        mode=mode,
        rollup_subtypes=rollup_subtypes,
        pinned=pinned,
        probe_ms=probe_ms,
        reads=reads,
    )


def _fixture_none() -> ServerCapabilities:
    return _record(
        semantic=Capability(UNAVAILABLE, "no embedder is configured", "fixture"),
        alias=Capability(UNAVAILABLE, "no aliases are registered", "fixture"),
        passages=Capability(
            UNAVAILABLE, "no subtype declares a passage rollup", "fixture",
        ),
        mode="fixture:none",
        static_source="fixture",
    )


def _fixture_full() -> ServerCapabilities:
    return _record(
        semantic=Capability(AVAILABLE, "an embedder is configured", "fixture"),
        alias=Capability(AVAILABLE, "aliases are registered", "fixture"),
        passages=Capability(
            AVAILABLE, "a subtype declares a passage rollup", "fixture",
        ),
        mode="fixture:full",
        static_source="fixture",
    )


#: Named capability profiles for hermetic builds. `none` is the bare server
#: (no embedder, no aliases, no rollups); `full` serves everything.
CAPABILITY_FIXTURES = {
    "none": _fixture_none,
    "full": _fixture_full,
}


def capabilities_for_surface_probe(
    *,
    embedder_configured: bool,
    fixture: Optional[str],
) -> ServerCapabilities:
    """The record for a build with no database (`OKG_MCP_SURFACE_PROBE=1`).

    A named fixture wins. With none, config facts are used and every
    database fact is UNKNOWN, which publishes as unavailable. An unknown
    fixture name refuses rather than measuring some other profile.
    """
    if fixture:
        factory = CAPABILITY_FIXTURES.get(fixture)
        if factory is None:
            raise CapabilityFixtureInvalid(
                f"{CAPABILITY_FIXTURE_ENV}={fixture!r} names no capability "
                f"fixture; known fixtures: {sorted(CAPABILITY_FIXTURES)}",
                data={"fixture": fixture, "known": sorted(CAPABILITY_FIXTURES)},
                fix_hint=f"set {CAPABILITY_FIXTURE_ENV} to a known fixture",
            )
        return factory()
    not_probed = "not probed: OKG_MCP_SURFACE_PROBE builds the server without a database"
    return _record(
        semantic=_semantic_from_config(embedder_configured),
        alias=Capability(UNKNOWN, not_probed, "not_probed"),
        passages=Capability(UNKNOWN, not_probed, "not_probed"),
        mode="surface_probe_unprobed",
    )


class _ReadTimedOut(Exception):
    """A probe read hit its statement timeout."""


def _bounded_read(
    conn: psycopg.Connection,
    *,
    name: str,
    sql: str,
    params: Mapping[str, Any],
    timeout_ms: int,
) -> list[tuple[Any, ...]]:
    """Run one probe read under its own statement timeout.

    Each read gets its own transaction, so a timeout in one cannot poison the
    next. A timeout raises `_ReadTimedOut`; any other database error refuses
    start, naming the read.
    """
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('statement_timeout', %(ms)s, true)",
                    {"ms": str(int(timeout_ms))},
                )
                cur.execute(sql, dict(params))
                return list(cur.fetchall())
    except psycopg.errors.QueryCanceled as exc:
        raise _ReadTimedOut(name) from exc
    except psycopg.Error as exc:
        raise _probe_failed(name, exc) from exc


def _probe_failed(name: str, exc: BaseException) -> CapabilityProbeFailed:
    return CapabilityProbeFailed(
        f"the start-time capability read {name!r} failed "
        f"({type(exc).__name__}: {exc}); refusing to start rather than "
        "publish a search surface nobody verified",
        data={"read": name, "error_type": type(exc).__name__},
        fix_hint=(
            "fix the database error the read names (role privileges, a "
            "missing migration, the connection), then restart mcp-serve"
        ),
    )


_ALIASES_SQL = (
    "SELECT EXISTS (SELECT 1 FROM okg.aliases "
    "WHERE catalog_version_id = %(cv)s)"
)
_ROLLUPS_SQL = (
    "SELECT name FROM okg.subtypes "
    "WHERE catalog_version_id = %(cv)s "
    "AND metadata ? 'passage_rollup' "
    "AND coalesce(metadata -> 'passage_rollup' ->> 'edge_type', '') <> '' "
    "AND coalesce(metadata -> 'passage_rollup' ->> 'child_subtype', '') <> '' "
    "ORDER BY name"
)
def _resolve_pin(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    timeout_ms: int,
) -> Optional[dict[str, Any]]:
    """The generation and catalog version a new session would pin, or None.

    The same resolver, with the same arguments, that `open_session` uses
    (`resolve_okg_generation`, published, default-branch read-through).
    None means no published generation exists: a state, not an error, and
    the catalog facts then read unknown. The probe never substitutes some
    other catalog version for the one a session would pin.

    The statement timeout is set for the SESSION, not inside a transaction:
    the resolver's fast path swallows its own query errors and falls back
    to a second query, which inside one transaction would run in an aborted
    transaction and turn a timeout into an unrelated error. On the pool's
    autocommit connection each statement stands alone, so a timed-out fast
    path leaves the fallback runnable, and a fallback that also times out
    raises `QueryCanceled`, which is reported as a timeout.
    """
    from okg.substrate.generation_resolver import resolve_okg_generation

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %(ms)s, false)",
                {"ms": str(int(timeout_ms))},
            )
        try:
            resolved = resolve_okg_generation(
                conn,
                deployment_name=deployment_name,
                requested=None,
                branch=branch_name,
                status="published",
                allow_default_branch_fallback=True,
            )
        finally:
            with conn.cursor() as cur:
                cur.execute("RESET statement_timeout")
    except psycopg.errors.QueryCanceled as exc:
        raise _ReadTimedOut("pin") from exc
    except psycopg.Error as exc:
        raise _probe_failed("pin", exc) from exc
    except SubstrateError as exc:
        # An invalid or compacted checkpoint is a broken pin, not "no
        # generation yet": refuse, naming the read.
        raise _probe_failed("pin", exc) from exc
    if resolved is None:
        return None
    if resolved.catalog_version_id is None:
        raise _probe_failed(
            "pin",
            SubstrateError(
                f"generation {resolved.generation_id!r} carries no "
                "catalog_version_id"
            ),
        )
    return {
        "basis": "latest_published_generation",
        "generation_id": resolved.generation_id,
        "catalog_version_id": int(resolved.catalog_version_id),
    }


def probe_capabilities(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str = "default",
    embedder_configured: bool,
    timeout_ms: int = PROBE_STATEMENT_TIMEOUT_MS,
) -> ServerCapabilities:
    """Probe what this server can run. Called once, at start.

    At most three small reads: the pin (generation and catalog version), an
    `EXISTS` on `okg.aliases`, and the subtypes that declare a passage
    rollup. Errors refuse start (`CapabilityProbeFailed`); a timeout makes
    the capabilities that depend on that read unknown.
    """
    started = time.perf_counter()
    reads: list[str] = ["pin"]
    semantic = _semantic_from_config(embedder_configured)
    try:
        pinned: Optional[dict[str, Any]] = _resolve_pin(
            conn,
            deployment_name=deployment_name,
            branch_name=branch_name,
            timeout_ms=timeout_ms,
        )
    except _ReadTimedOut:
        pinned = None
        timed_out = (
            f"not verified at start: the pin read timed out after {timeout_ms} ms"
        )
        return _record(
            semantic=semantic,
            alias=Capability(UNKNOWN, timed_out, "catalog"),
            passages=Capability(UNKNOWN, timed_out, "catalog"),
            mode="live",
            pinned=None,
            probe_ms=_elapsed_ms(started),
            reads=tuple(reads),
        )
    if pinned is None:
        reason = "not verified at start: no published generation existed"
        return _record(
            semantic=semantic,
            alias=Capability(UNKNOWN, reason, "catalog"),
            passages=Capability(UNKNOWN, reason, "catalog"),
            mode="live",
            pinned=None,
            probe_ms=_elapsed_ms(started),
            reads=tuple(reads),
        )
    cv = pinned["catalog_version_id"]

    reads.append("aliases")
    try:
        rows = _bounded_read(
            conn, name="aliases", sql=_ALIASES_SQL, params={"cv": cv},
            timeout_ms=timeout_ms,
        )
        if rows and rows[0][0]:
            alias = Capability(
                AVAILABLE, f"aliases are registered at catalog version {cv}",
                "catalog",
            )
        else:
            alias = Capability(
                UNAVAILABLE,
                f"no aliases are registered at catalog version {cv}",
                "catalog",
            )
    except _ReadTimedOut:
        alias = Capability(
            UNKNOWN,
            f"not verified at start: the alias read timed out after {timeout_ms} ms",
            "catalog",
        )

    reads.append("passage_rollups")
    rollup_subtypes: tuple[str, ...] = ()
    try:
        rows = _bounded_read(
            conn, name="passage_rollups", sql=_ROLLUPS_SQL, params={"cv": cv},
            timeout_ms=timeout_ms,
        )
        rollup_subtypes = tuple(str(row[0]) for row in rows)
        if rollup_subtypes:
            passages = Capability(
                AVAILABLE,
                f"{len(rollup_subtypes)} subtype(s) declare a passage rollup "
                f"at catalog version {cv}",
                "catalog",
            )
        else:
            passages = Capability(
                UNAVAILABLE,
                f"no subtype declares a passage rollup at catalog version {cv}",
                "catalog",
            )
    except _ReadTimedOut:
        passages = Capability(
            UNKNOWN,
            "not verified at start: the passage-rollup read timed out after "
            f"{timeout_ms} ms",
            "catalog",
        )

    return _record(
        semantic=semantic,
        alias=alias,
        passages=passages,
        mode="live",
        rollup_subtypes=rollup_subtypes,
        pinned=pinned,
        probe_ms=_elapsed_ms(started),
        reads=tuple(reads),
    )


def probe_capabilities_from_pool(
    pool: Any,
    *,
    deployment_name: str,
    branch_name: str = "default",
    embedder_configured: bool,
    timeout_ms: int = PROBE_STATEMENT_TIMEOUT_MS,
) -> ServerCapabilities:
    """`probe_capabilities` on one connection borrowed from the server's pool.

    `probe_capabilities` turns every database error of its own reads into
    `CapabilityProbeFailed`, so a raw pool or psycopg error that reaches this
    function came from getting (or returning) the connection. That is named
    as the read `connection`, like the others, rather than escaping as a raw
    pool error from deep inside the server build.
    """
    from psycopg_pool import PoolTimeout

    try:
        with pool.connection() as conn:
            return probe_capabilities(
                conn,
                deployment_name=deployment_name,
                branch_name=branch_name,
                embedder_configured=embedder_configured,
                timeout_ms=timeout_ms,
            )
    except (PoolTimeout, psycopg.Error) as exc:
        raise _probe_failed("connection", exc) from exc


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)


# ── the guard ─────────────────────────────────────────────────────────


def assert_search_capability(
    caps: ServerCapabilities,
    *,
    method: str,
    rank_by: Optional[str],
) -> None:
    """Refuse an unavailable method or `rank_by` by name, before any query.

    `semantic` is NOT refused here: its refusal keeps its own code,
    `semantic_search_unavailable`, which callers already handle, and the
    server raises it from the same probe record. Every other method the
    probe found unavailable is refused with `search_method_unavailable`.
    """
    if method != "semantic" and method in SEARCH_METHODS:
        cap = caps.method(method)
        if not cap.available:
            raise SearchMethodUnavailable(
                f"search method={method!r} is not available on this server: "
                f"{cap.reason}",
                data={
                    "method": method,
                    "status": cap.status,
                    "reason": cap.reason,
                    "available_methods": list(caps.available_methods()),
                },
                fix_hint=(
                    "call one of available_methods; this server's surface is "
                    "fixed at start, so a capability added since then needs "
                    "a restart of mcp-serve"
                ),
            )
    if rank_by is not None and rank_by in SEARCH_RANK_BY_VALUES:
        cap = caps.rank_by_value(rank_by)
        if not cap.available:
            raise SearchRankByUnavailable(
                f"search rank_by={rank_by!r} is not available on this server: "
                f"{cap.reason}",
                data={
                    "rank_by": rank_by,
                    "status": cap.status,
                    "reason": cap.reason,
                    "available_rank_by": list(caps.available_rank_by()),
                },
                fix_hint=(
                    "omit rank_by or pass one of available_rank_by; a rollup "
                    "declared since start needs a restart of mcp-serve"
                ),
            )


# ── generated instructions ────────────────────────────────────────────


def _line_reason(cap: Capability) -> str:
    if cap.status == UNKNOWN and not cap.reason.startswith("not verified"):
        return f"not verified at start ({cap.reason})"
    return cap.reason


def render_unavailable_lines(
    caps: ServerCapabilities, *, served_tools: Iterable[str],
) -> str:
    """The generated "not available here" block, or "" when nothing is.

    Only for a surface that serves `search`. States what the tool does not do
    on this server; never which method to call instead.
    """
    if "search" not in set(served_tools):
        return ""
    # Prompt redesign (#3666, lead decision 2026-10-16): a method the
    # published schema hides (`lean_surface.SHOWN_SEARCH_METHODS`) is not
    # named here; naming it suggests a mode the agent was never shown. It is
    # still refused with its reason when called.
    from okg.substrate.mcp.lean_surface import SHOWN_SEARCH_METHODS

    rows: list[str] = []
    for name, cap in caps.methods:
        if not cap.available and name in SHOWN_SEARCH_METHODS:
            rows.append(f'- search method "{name}": {_line_reason(cap)}.')
    for value, cap in caps.rank_by:
        if not cap.available:
            rows.append(f'- search rank_by "{value}": {_line_reason(cap)}.')
    if not rows:
        return ""
    text = "\n".join([NOT_AVAILABLE_HEADER, *rows])
    size = len(text.encode("utf-8"))
    if size > GENERATED_INSTRUCTIONS_BUDGET_BYTES:
        raise GeneratedInstructionsOverBudget(
            f"the generated instructions are {size} B, past their "
            f"{GENERATED_INSTRUCTIONS_BUDGET_BYTES} B budget",
            data={"bytes": size, "budget": GENERATED_INSTRUCTIONS_BUDGET_BYTES},
            fix_hint="shorten the capability reasons in capability_probe.py",
        )
    return text


def compose_instructions(authored: str, generated: str) -> str:
    """Served text = the authored or rendered text, then the generated block."""
    if not generated:
        return authored
    return authored + _GENERATED_SEPARATOR + generated


def split_generated_instructions(served: str) -> tuple[str, str]:
    """Split served text back into (authored, generated).

    The fleet doctor compares the authored part to what the files say; the
    generated part depends on the database and cannot be recomputed there.
    """
    marker = _GENERATED_SEPARATOR + NOT_AVAILABLE_HEADER
    index = served.rfind(marker)
    if index < 0:
        return served, ""
    return served[:index], served[index + len(_GENERATED_SEPARATOR):]


# ── the published schema ──────────────────────────────────────────────


def _array_branch(node: Any) -> Optional[dict[str, Any]]:
    if not isinstance(node, dict):
        return None
    if node.get("type") == "array":
        return node
    for branch in node.get("anyOf") or ():
        if isinstance(branch, dict) and branch.get("type") == "array":
            return branch
    return None


def _enum_branch(node: Any) -> Optional[dict[str, Any]]:
    if not isinstance(node, dict):
        return None
    if isinstance(node.get("enum"), list):
        return node
    for branch in node.get("anyOf") or ():
        if isinstance(branch, dict) and isinstance(branch.get("enum"), list):
            return branch
    return None


def compile_tool_surface(mcp: Any, caps: ServerCapabilities) -> None:
    """Narrow the PUBLISHED `search` schema to what this server can run.

    Rewrites `tools/list` only; the validator keeps the full models, and
    `assert_search_capability` refuses what this removes, from the same
    record. Raises, never no-ops, when the SDK internals are not where this
    expects them: a silent no-op would publish methods the guard refuses.
    """
    manager = getattr(mcp, "_tool_manager", None)
    tools = getattr(manager, "_tools", None) if manager is not None else None
    if tools is None:
        raise SearchSchemaUnreachable(
            "could not reach the registered tools through the MCP SDK "
            "internals (_tool_manager._tools)",
            fix_hint="an MCP SDK upgrade likely renamed these internals",
        )
    tool = tools.get("search")
    if tool is None:
        return
    schema = getattr(tool, "parameters", None)
    props = schema.get("properties") if isinstance(schema, dict) else None
    method = props.get("method") if isinstance(props, dict) else None
    select = _array_branch(props.get("select")) if isinstance(props, dict) else None
    rank_by = _enum_branch(props.get("rank_by")) if isinstance(props, dict) else None
    view = _enum_branch(props.get("view")) if isinstance(props, dict) else None
    if not (
        isinstance(method, dict)
        and isinstance(method.get("enum"), list)
        and select is not None
        and rank_by is not None
        and view is not None
    ):
        raise SearchSchemaUnreachable(
            "could not reach the registered `search` tool's published "
            "method, select, rank_by and view schemas. Refusing rather than "
            "publishing methods this server refuses.",
            data={
                "has_parameters": isinstance(schema, dict),
                "has_method_enum": isinstance(method, dict)
                and isinstance(method.get("enum"), list),
                "has_select_array": select is not None,
                "has_rank_by_enum": rank_by is not None,
                "has_view_enum": view is not None,
            },
            fix_hint=(
                "an MCP SDK or pydantic upgrade likely changed the schema "
                "shape; update compile_tool_surface to match"
            ),
        )
    available = set(caps.available_methods())
    method["enum"] = [m for m in method["enum"] if m in available]
    if method.get("default") not in method["enum"]:
        raise SearchSchemaUnreachable(
            f"the published default search method {method.get('default')!r} "
            "is not available on this server",
            data={"available_methods": sorted(available)},
        )
    items = select.get("items")
    if not isinstance(items, dict):
        items = {"type": "string"}
        select["items"] = items
    items["enum"] = list(caps.published_select_fields())
    described = caps.select_fields_description()
    if described:
        select["description"] = described
    allowed_rank = set(caps.available_rank_by())
    rank_by["enum"] = [v for v in rank_by["enum"] if v in allowed_rank]
    view["enum"] = list(caps.published_views())

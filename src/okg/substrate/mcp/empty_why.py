"""Why a search or filter page is empty, and what the call finds loosened (lane ri F6).

A read that returns no row used to say only `0 rows`. Agents read that as
"the graph has nothing" and stopped: in the archi smoke test a model sent one
search with a record type the deployment does not have, got nothing, and
gave up; read-eval tries ended right after an empty page. So an empty FIRST
page of `search` (lexical, grep, or the query-less lookup) and of `filter`
(a node set) now says why, in one of these causes (`EMPTY_WHY_KEY`, rendered
by :func:`render`):

* `unknown_type`: a type condition of the call names only types the pinned
  catalog does not have. The catalog's types are listed, closest first. (An
  `in` that still names a real type is not the cause: the unknown names are
  said on a note line and the conditions are checked as usual.)
* `condition`: dropping that one condition brings rows back. For an
  attribute equality, other values seen on records holding the attribute are
  listed, only where the deployment runs no access rules.
* `conditions`: dropping any one condition still leaves 0 rows (and, for a
  text search, dropping all of them brings rows back).
* `terms`: a word of the query matches nothing on its own (2-4 plain words).
* `terms_together`: each word matches alone, no text holds them all.
* `query`: nothing matches the query itself (within the call's profile or
  source scope, which the reply names), with no condition to blame.
* `page_timed_out`: the read itself timed out; nothing is claimed.
* `not_found`: the probes that would tell did not finish; nothing is claimed.
* `not_checked`: a semantic search (a probe would embed and rank again).

Each cause comes from probes: the same call with one part dropped, run once
through the same read path (:class:`Prober`), on the read's own handle and
pin, as the same reader. The first probe that finds rows gives up to
`LOOSENED_ROWS` ids under `loosened`, with the part dropped and the call that
returns them. They are never page rows and never counted: the page stays the
answer. A probe that ran out of time, was refused, or was not run for want
of budget says so; no absence is claimed from it.

Access rules: a probe is a call the caller could send, run under the
caller's own session, so it sees only rows the caller can read and adds no
fact about a hidden row. The type list is catalog schema (no counts), as
`inspect(ontology)` serves it under every mode. Attribute values are
row-derived: read only when the deployment runs no access rules, as
`value_hint` and `exact_name_absent` are.
"""
from __future__ import annotations

import difflib
import json
import logging
import re
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping, Optional, Sequence

log = logging.getLogger(__name__)

__all__ = [
    "BUDGET_S",
    "CALL_LIMIT_S",
    "Call",
    "EMPTY_WHY_KEY",
    "LOOSENED_ROWS",
    "MAX_PROBES",
    "PROBE_S",
    "Prober",
    "catalog_types",
    "explain",
    "finish",
    "outcome",
    "render",
    "search_call_arguments",
    "short_where",
]

#: The payload key of an empty page's explanation.
EMPTY_WHY_KEY = "empty_why"
#: Reads (probes and the value read) one reply may run: the most, and the
#: fewer a grep, or a lookup or filter under access rules, may run. Each read
#: is the caller's own read path again (20-80 statements), so an empty page
#: costs at most about twice the call's own statements (`read_cap`).
MAX_PROBES = 3
NARROW_PROBES = 2
#: Seconds one probe may take (statement timeout and grep deadline).
PROBE_S = 1.0
#: Seconds all probes (and the value read) of one reply may take together.
BUDGET_S = 2.0
#: No probe starts once the call has run this long (from its first dispatch).
CALL_LIMIT_S = 5.0
#: Rows a loosened probe returns (its ids are shown).
LOOSENED_ROWS = 3
#: Record types listed for an unknown type.
TYPES_SHOWN = 12
#: Words of a query probed one at a time.
WORDS_PROBED = 4
#: Records an attribute's values are read from (a page limit no read tool
#: defaults to, so its page statement is told apart), and values shown.
VALUE_ROWS = 40
VALUES_SHOWN = 8
#: Longest value shown, in characters.
VALUE_CHARS = 60
#: Longest call printed on the loosened line.
CALL_CHARS = 240
#: Read methods without query text: a probe needs a condition left.
LOOKUPS = frozenset({"attrs", "filter"})
#: Predicate fields that name a record type.
TYPE_FIELDS = frozenset({"type", "subtype"})

#: Probe outcomes.
FOUND, NONE, NONE_UNPROVEN = "found", "none", "none_unproven"
TIMED_OUT, REFUSED, SKIPPED = "timed_out", "refused", "skipped"
#: A probe not run because the reply's reads were used up.
LIMITED = "probe_limit"


# ── the call and its parts ────────────────────────────────────────────────


@dataclass(frozen=True)
class Call:
    """The parts of a read a probe may drop. `where` is canonical (search's
    `normalize_where`, or filter's composed predicate); `filter` is search's
    `filter` argument (`subtype`, `attr_filters`, `source`). `scope` names
    what narrows the read and is never dropped: a profile, a source family,
    grep's `within` (e.g. `profile "docs"`)."""

    method: str
    query: Optional[str] = None
    where: Optional[dict[str, Any]] = None
    subtype_filter: Optional[str] = None
    attr_filters: Mapping[str, Any] = field(default_factory=dict)
    filter: Mapping[str, Any] = field(default_factory=dict)
    path_prefix: Optional[str] = None
    scope: Optional[str] = None
    #: grep's `within` ("record": each word anywhere in one record).
    within: Optional[str] = None

    def conditions(self) -> bool:
        return bool(
            self.where or self.subtype_filter or self.attr_filters
            or self.path_prefix or self.scope or any(
                self.filter.get(key) for key in ("subtype", "attr_filters", "source")
            )
        )

    def runnable(self) -> bool:
        """A lookup needs a condition; a text search needs its query."""
        if self.method in LOOKUPS:
            return bool(self.where or self.subtype_filter or self.attr_filters
                        or self.filter.get("subtype") or self.filter.get("attr_filters"))
        return bool(self.query)

    def bare(self) -> "Call":
        """The query alone, in the same scope."""
        return Call(method=self.method, query=self.query, scope=self.scope, within=self.within)


@dataclass(frozen=True)
class Part:
    """One droppable condition: its label and the call without it."""

    label: str
    without: Call
    #: `attrs.<key>` of an attribute equality (its values may be listed).
    attribute: Optional[str] = None
    #: The values the condition compared to (left out of a value list).
    sent: tuple[Any, ...] = ()


def _value_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _conjuncts(where: Optional[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not where:
        return []
    if where.get("op") == "and":
        out: list[dict[str, Any]] = []
        for clause in where["clauses"]:
            out.extend(_conjuncts(clause))
        return out
    return [dict(where)]


def _join(clauses: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not clauses:
        return None
    return dict(clauses[0]) if len(clauses) == 1 else {"op": "and", "clauses": list(clauses)}


def _clause_label(clause: Mapping[str, Any]) -> str:
    """`attrs.status = "open"`: an equality or `in` with its JSON operand,
    as the caller wrote it; any other clause as `where_applied` says it."""
    from okg.substrate.mcp.search_where import describe

    if clause.get("op") == "eq":
        return f"{clause.get('field')} = {_value_text(clause.get('value'))}"
    if clause.get("op") == "in":
        return f"{clause.get('field')} in {_value_text(clause.get('values'))}"
    try:
        return describe(clause) or "where"
    except (KeyError, TypeError):
        return _value_text(clause)


def _compared(clause: Mapping[str, Any]) -> tuple[Optional[str], tuple[Any, ...]]:
    """`(attrs.<key>, sent values)` of an attribute equality or `in`."""
    fld = clause.get("field")
    if not (isinstance(fld, str) and fld.startswith("attrs.")):
        return None, ()
    if clause.get("op") == "eq":
        return fld, (clause.get("value"),)
    if clause.get("op") == "in" and isinstance(clause.get("values"), list):
        return fld, tuple(clause["values"])
    return None, ()


def parts(call: Call) -> list[Part]:
    """The call's droppable conditions, in the order they were given."""
    out: list[Part] = []
    if call.subtype_filter:
        out.append(Part(f"type = {_value_text(call.subtype_filter)}",
                        replace(call, subtype_filter=None)))
    for key, value in call.attr_filters.items():
        rest = {k: v for k, v in call.attr_filters.items() if k != key}
        out.append(Part(f"attrs.{key} = {_value_text(value)}",
                        replace(call, attr_filters=rest), f"attrs.{key}", (value,)))
    for key in ("subtype", "source"):
        if call.filter.get(key):
            name = "type" if key == "subtype" else key
            rest = {k: v for k, v in call.filter.items() if k != key}
            out.append(Part(f"{name} = {_value_text(call.filter[key])}",
                            replace(call, filter=rest)))
    nested = call.filter.get("attr_filters") or {}
    for key, value in nested.items():
        rest_attrs = {k: v for k, v in nested.items() if k != key}
        rest = {**call.filter, "attr_filters": rest_attrs}
        if not rest_attrs:
            rest.pop("attr_filters")
        out.append(Part(f"attrs.{key} = {_value_text(value)}",
                        replace(call, filter=rest), f"attrs.{key}", (value,)))
    clauses = _conjuncts(call.where)
    for index, clause in enumerate(clauses):
        rest_clauses = clauses[:index] + clauses[index + 1:]
        attribute, sent = _compared(clause)
        out.append(Part(_clause_label(clause), replace(call, where=_join(rest_clauses)),
                        attribute, sent))
    if call.path_prefix:
        out.append(Part(f"path_prefix {_value_text(call.path_prefix)}",
                        replace(call, path_prefix=None)))
    return out


def type_spots(call: Call) -> list[list[str]]:
    """Each type condition of the call, as the type names it allows: the
    old arguments and each top-level type equality or `in` (a type under
    `or`/`not` names nothing)."""
    spots: list[list[str]] = []
    for value in (call.subtype_filter, call.filter.get("subtype")):
        if isinstance(value, str) and value:
            spots.append([value])
    for clause in _conjuncts(call.where):
        if clause.get("field") not in TYPE_FIELDS:
            continue
        if clause.get("op") == "eq" and isinstance(clause.get("value"), str):
            spots.append([clause["value"]])
        elif clause.get("op") == "in":
            values = [v for v in clause.get("values") or () if isinstance(v, str)]
            if values:
                spots.append(values)
    return spots


def named_types(call: Call) -> list[str]:
    """Every type name the call's type conditions allow, in order."""
    names: list[str] = []
    for spot in type_spots(call):
        names.extend(name for name in spot if name not in names)
    return names


def without_types(call: Call, unknown: set[str]) -> Call:
    """The call with every unknown type taken out (an `in` keeps the rest)."""
    subtype = call.subtype_filter if call.subtype_filter not in unknown else None
    filt = {k: v for k, v in call.filter.items()
            if not (k == "subtype" and v in unknown)}
    kept: list[dict[str, Any]] = []
    for clause in _conjuncts(call.where):
        if clause.get("field") in TYPE_FIELDS and clause.get("op") == "eq" and (
            clause.get("value") in unknown
        ):
            continue
        if clause.get("field") in TYPE_FIELDS and clause.get("op") == "in":
            values = [v for v in clause.get("values") or () if v not in unknown]
            if not values:
                continue
            clause = {**clause, "values": values}
        kept.append(clause)
    return replace(call, subtype_filter=subtype, filter=filt, where=_join(kept))


def type_label(names: Sequence[str]) -> str:
    return "type" + ("s " if len(names) > 1 else " ") + ", ".join(
        _value_text(name) for name in names
    )


# ── the catalog ───────────────────────────────────────────────────────────


def catalog_types(conn: Any, session: Any) -> list[str]:
    """Record type names of the pinned catalog (schema, never row counts)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM okg.subtypes WHERE catalog_version_id = %s ORDER BY name",
            (session.catalog_version_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def listed_types(unknown: Sequence[str], types: Sequence[str]) -> tuple[list[str], int]:
    """Up to `TYPES_SHOWN` types, those closest to an unknown one first,
    and how many more there are."""
    close: list[str] = []
    for name in unknown:
        for match in difflib.get_close_matches(name, types, n=3, cutoff=0.5):
            if match not in close:
                close.append(match)
    ordered = close + [name for name in types if name not in close]
    shown = ordered[:TYPES_SHOWN]
    return shown, len(ordered) - len(shown)


# ── probes ────────────────────────────────────────────────────────────────


@dataclass
class Probe:
    """One probe's outcome: its `status` and the ids it found."""

    status: str
    ids: list[str] = field(default_factory=list)


def outcome(page: Any, *, conditions: bool, every_row_readable: bool, method: str) -> Probe:
    """A page as an outcome. An empty page is `none` only when its count is
    an exact 0, or when a lexical read with no condition and no scope, on a
    deployment without access rules, found no candidate at all (nothing can
    have been cut away after ranking). A page whose read timed out is
    `timed_out`, whatever it holds."""
    if not isinstance(page, Mapping):
        return Probe(NONE_UNPROVEN)
    rows = page.get("results")
    if not isinstance(rows, list):
        rows = page.get("items") or []
    ids = [str(row["node_id"]) for row in rows
           if isinstance(row, Mapping) and isinstance(row.get("node_id"), str)]
    if ids:
        return Probe(FOUND, ids[:LOOSENED_ROWS])
    basis = page.get("total_count_basis")
    if basis == "timed_out" or page.get("page_basis") == "timed_out":
        return Probe(TIMED_OUT)
    if page.get("total_count") == 0 and basis == "exact":
        return Probe(NONE)
    if method == "lexical" and not conditions and every_row_readable:
        return Probe(NONE)
    return Probe(NONE_UNPROVEN)


def _arm_probe(cur: Any, milliseconds: int, *, local: bool) -> None:
    """Set the probe's statement timeout (a test seam: a test makes it
    raise to stand for a probe that ran out of time)."""
    cur.execute(
        "SELECT set_config('statement_timeout', %s, %s)", (f"{milliseconds}ms", local),
    )


def read_cap(method: str, every_row_readable: bool) -> int:
    """The reads one reply may run: `NARROW_PROBES` for grep (a probe costs
    35-80 statements) and for a lookup or filter under access rules (a
    reader view, a Nomos decision and a read lease each), else
    `MAX_PROBES`."""
    if method == "grep" or (method in LOOKUPS and not every_row_readable):
        return NARROW_PROBES
    return MAX_PROBES


class Prober:
    """Runs probes under the reply's read count and time budget: at most
    `cap` reads (`read_cap`; the value read takes one too), each at most
    `PROBE_S`, all within `BUDGET_S`, and none once the call has run
    `CALL_LIMIT_S` since `started_at` (its first dispatch) or past grep's
    deadline for the call. A probe past the count is `probe_limit`, one
    past the time is `skipped`. `allowed` False runs no probe at all."""

    def __init__(
        self,
        conn: Any,
        run: Callable[[Call, int], Any],
        *,
        every_row_readable: bool,
        started_at: Optional[float] = None,
        allowed: bool = True,
        cap: int = MAX_PROBES,
    ) -> None:
        self.conn = conn
        self.run = run
        self.every_row_readable = every_row_readable
        self.left = cap if allowed else 0
        self.allowed = allowed
        now = time.monotonic()
        self.until = now + BUDGET_S
        if started_at is not None:
            self.until = min(self.until, started_at + CALL_LIMIT_S)
        self.log: list[dict[str, Any]] = []
        #: The session statement timeout to put back (`close`).
        self.previous: Optional[str] = None

    def seconds_left(self, cap: float = PROBE_S) -> float:
        from okg.substrate.mcp.grep_search import _DEADLINE

        now = time.monotonic()
        seconds = min(cap, self.until - now)
        outer = _DEADLINE.get()
        if outer is not None:
            # Never past the deadline of the call the page belongs to.
            seconds = min(seconds, outer - now)
        return seconds

    def __call__(self, call: Call, *, limit: int, label: dict[str, Any]) -> Probe:
        found = self._probe(call, limit)
        self.log.append({**label, "status": found.status})
        return found

    def _probe(self, call: Call, limit: int) -> Probe:
        import psycopg

        from okg.substrate.errors import SubstrateError
        from okg.substrate.mcp.grep_search import GrepTimeout, deadline
        from okg.substrate.mcp.response_bounds import _CALL_REWRITE

        seconds = self.seconds_left()
        if self.left <= 0:
            return Probe(LIMITED if self.allowed else SKIPPED)
        if seconds <= 0.05:
            return Probe(SKIPPED)
        self.left -= 1
        state = _CALL_REWRITE.get()
        notes = list(state.body_notes) if state is not None else None
        try:
            page = self._timed(call, limit, seconds, deadline)
        except (psycopg.errors.QueryCanceled, GrepTimeout):
            return Probe(TIMED_OUT)
        except SubstrateError:
            return Probe(REFUSED)
        except (ValueError, psycopg.Error):
            # Not a refusal the read names: logged, and the reply says the
            # probe was not answered rather than claiming anything.
            log.warning("empty-page probe failed", exc_info=True)
            return Probe(REFUSED)
        finally:
            if state is not None and notes is not None:
                # A probe's rewrite notes describe a call nobody sent.
                state.body_notes[:] = notes
        return outcome(page, conditions=call.conditions(),
                       every_row_readable=self.every_row_readable, method=call.method)

    def _timed(self, call: Call, limit: int, seconds: float, deadline: Any) -> Any:
        """One probe under a statement timeout of `seconds` and grep's
        deadline. On an autocommit connection (the MCP read handle) the
        session's timeout is read once, set per probe, and put back once by
        `close` (some reads commit, a generation checkout, so a probe may
        not run inside a savepoint); inside a transaction it is a
        savepoint-local setting put back after each probe."""
        milliseconds = max(1, int(seconds * 1000))
        if getattr(self.conn, "autocommit", False):
            if self.previous is None:
                with self.conn.cursor() as cur:
                    cur.execute("SHOW statement_timeout")
                    (self.previous,) = cur.fetchone()
            with self.conn.cursor() as cur:
                _arm_probe(cur, milliseconds, local=False)
            with deadline(time.monotonic() + seconds):
                return self.run(call, limit)
        with self.conn.cursor() as cur:
            cur.execute("SHOW statement_timeout")
            (previous,) = cur.fetchone()
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                _arm_probe(cur, milliseconds, local=True)
            with deadline(time.monotonic() + seconds):
                page = self.run(call, limit)
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)", (previous,),
                )
        return page

    def take(self) -> bool:
        """Take one read for the value read; False when none is left."""
        if self.left <= 0:
            return False
        self.left -= 1
        return True

    def close(self) -> None:
        """Put the session's statement timeout back after the probes (the
        autocommit handle). Always called, also when a probe failed."""
        if self.previous is None:
            return
        previous, self.previous = self.previous, None
        with self.conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (previous,))


# ── the explanation ───────────────────────────────────────────────────────


_PLAIN_WORD = re.compile(r"^[^\W_][\w.\-]*$", re.UNICODE)
_OPERATOR_WORDS = frozenset({"OR", "AND", "NOT"})

#: values(attribute, call without the condition, seconds) -> values or None.
ValueReader = Callable[[str, Call, float], Optional[list[Any]]]


def plain_words(query: Optional[str]) -> list[str]:
    """The words of a query of 2 to `WORDS_PROBED` plain words (no quote,
    operator, regex, path or negation), else []."""
    if not query:
        return []
    words = query.split()
    if not 2 <= len(words) <= WORDS_PROBED:
        return []
    if any(not _PLAIN_WORD.match(word) or word.upper() in _OPERATOR_WORDS
           or word.upper().startswith("NEAR/") for word in words):
        return []
    return words


def _none(found: Probe) -> bool:
    return found.status in (NONE, NONE_UNPROVEN)


def explain(
    call: Call,
    *,
    types: Sequence[str],
    prober: Prober,
    values: ValueReader,
    page: Probe,
) -> dict[str, Any]:
    """The `empty_why` block of an empty first page. `page` is the empty
    page itself read as an outcome (`outcome`)."""
    out: dict[str, Any] = {}
    known = set(types)
    spots = type_spots(call)
    unknown = [name for name in named_types(call) if name not in known]
    # A type condition empties the page by itself only when every name it
    # allows is unknown; an `in` that keeps a real type does not.
    blocking = any(spot and all(name not in known for name in spot) for spot in spots)
    loosened: Optional[tuple[list[str], Call, Probe]] = None
    if unknown:
        shown, more = listed_types(unknown, types)
        out.update(unknown_types=unknown, types=shown)
        if more:
            out["types_more"] = more
    if blocking:
        out["cause"] = "unknown_type"
        without = without_types(call, set(unknown))
        dropped = [
            f"type = {_value_text(unknown[0])}" if len(unknown) == 1
            else f"type in {_value_text(unknown)}"
        ]
        if call.method == "semantic" or page.status == TIMED_OUT:
            # A semantic probe would embed and rank again; after a timed-out
            # read no probe runs.
            loosened = (dropped, without, Probe(SKIPPED))
        elif without.runnable():
            found = prober(without, limit=LOOSENED_ROWS, label={"dropped": dropped})
            loosened = (dropped, without, found)
    elif page.status == TIMED_OUT:
        # The read itself did not finish: neither its emptiness nor any
        # probe says what removed the rows.
        out["cause"] = "page_timed_out"
    elif call.method == "semantic":
        out["cause"] = "not_checked"
    else:
        if unknown:
            out["partial_types"] = True
        loosened = _explain_conditions(call, out, prober=prober, values=values, page=page)
    if loosened is not None:
        dropped, without, found = loosened
        block: dict[str, Any] = {"dropped": dropped, "status": found.status}
        if found.status == FOUND:
            block["ids"] = found.ids
            block["call"] = without
        out["loosened"] = block
    if out.get("cause") == "not_found" or out.get("each_alone") == "unchecked":
        out["unchecked"] = _unchecked_reason(prober.log)
    if call.scope and out.get("cause") in ("query", "terms", "terms_together"):
        out["scope"] = call.scope
    if prober.log:
        out["probes"] = prober.log
    if not prober.every_row_readable:
        out["access_rules"] = True
    return out


def _unchecked_reason(log: Sequence[Mapping[str, Any]]) -> str:
    """Why a check that would decide the cause did not answer."""
    statuses = {entry.get("status") for entry in log}
    for status in (LIMITED, TIMED_OUT, SKIPPED, REFUSED):
        if status in statuses:
            return status
    return SKIPPED


def _explain_conditions(
    call: Call,
    out: dict[str, Any],
    *,
    prober: Prober,
    values: ValueReader,
    page: Probe,
) -> Optional[tuple[list[str], Call, Probe]]:
    droppable = parts(call)
    readable = prober.every_row_readable
    lookup = call.method in LOOKUPS
    if lookup and len(droppable) == 1:
        # A lookup with one condition: that condition matches nothing, and
        # nothing is left to loosen to.
        out.update(cause="condition", removed_by=[droppable[0].label])
        if page.status == NONE_UNPROVEN:
            out["proven"] = False
        _attach_values(out, droppable[0], prober=prober, values=values, readable=readable)
        return None
    if droppable and not lookup and len(droppable) >= prober.left:
        # The drop-one probes would use every read: the query alone is
        # probed first, so the answer says whether the conditions matter.
        return _explain_bare_first(call, droppable, out, prober=prober, values=values)
    first: Optional[tuple[list[str], Call, Probe]] = None
    removed: list[Part] = []
    statuses: list[Probe] = []
    unsure = unproven = False
    for part in droppable:
        if not part.without.runnable():
            # Never probed: no claim may rest on it.
            unsure = True
            continue
        found = prober(part.without, limit=LOOSENED_ROWS, label={"dropped": [part.label]})
        statuses.append(found)
        if found.status == FOUND:
            removed.append(part)
            first = first or ([part.label], part.without, found)
        elif found.status == NONE_UNPROVEN:
            unproven = True
        elif found.status != NONE:
            unsure = True
    labels = [part.label for part in droppable]
    if removed:
        out.update(cause="condition", removed_by=[part.label for part in removed])
        if unsure:
            out["not_all_checked"] = True
        _attach_values(out, removed[0], prober=prober, values=values, readable=readable)
        return first
    if droppable and lookup:
        # Several conditions; dropping any one still leaves 0 rows.
        if unsure:
            out["cause"] = "not_found"
            return None
        out.update(cause="conditions", removed_by=labels, each_alone=NONE)
        if unproven:
            out["proven"] = False
        return None
    if droppable:
        bare = call.bare()
        if len(droppable) == 1:
            # Its one probe was the query alone.
            if unsure or not statuses:
                out["cause"] = "not_found"
                return None
            alone = statuses[0]
        else:
            alone = prober(bare, limit=LOOSENED_ROWS, label={"dropped": labels})
            if alone.status == FOUND:
                out.update(
                    cause="conditions", removed_by=labels, all_dropped=FOUND,
                    each_alone="unchecked" if unsure else NONE,
                )
                if unproven:
                    out["proven"] = False
                return labels, bare, alone
            if not _none(alone):
                out["cause"] = "not_found"
                return None
        if alone.status == NONE_UNPROVEN:
            out["proven"] = False
        return _explain_words(call, bare, out, prober=prober)
    if page.status == NONE_UNPROVEN:
        out["proven"] = False
    return _explain_words(call, call, out, prober=prober)


def _explain_bare_first(
    call: Call,
    droppable: Sequence[Part],
    out: dict[str, Any],
    *,
    prober: Prober,
    values: ValueReader,
) -> Optional[tuple[list[str], Call, Probe]]:
    """A text search with as many conditions as reads: the query alone
    first. Rows back: the conditions removed every match, and the reads
    left try dropping one condition at a time. None: the query itself is
    the cause, checked word by word with the reads left."""
    labels = [part.label for part in droppable]
    bare = call.bare()
    alone = prober(bare, limit=LOOSENED_ROWS, label={"dropped": labels})
    if alone.status == FOUND:
        removed: list[Part] = []
        first: Optional[tuple[list[str], Call, Probe]] = None
        checked = 0
        for part in droppable:
            # Past the read count a part is logged `probe_limit`, unread.
            if not part.without.runnable():
                continue
            found = prober(part.without, limit=LOOSENED_ROWS, label={"dropped": [part.label]})
            if found.status in (FOUND, NONE, NONE_UNPROVEN):
                checked += 1
            if found.status == FOUND:
                removed.append(part)
                first = first or ([part.label], part.without, found)
        if removed:
            out.update(cause="condition", removed_by=[part.label for part in removed],
                       not_all_checked=True)
            _attach_values(out, removed[0], prober=prober, values=values,
                           readable=prober.every_row_readable)
            return first
        out.update(cause="conditions", removed_by=labels, all_dropped=FOUND,
                   each_alone=NONE if checked == len(droppable) else "unchecked")
        return labels, bare, alone
    if not _none(alone):
        out["cause"] = "not_found"
        return None
    if alone.status == NONE_UNPROVEN:
        out["proven"] = False
    return _explain_words(call, bare, out, prober=prober)


def _explain_words(
    call: Call, words_call: Call, out: dict[str, Any], *, prober: Prober,
) -> Optional[tuple[list[str], Call, Probe]]:
    without_conditions = words_call is not call
    words = plain_words(call.query)
    if not words:
        out.update(cause="query", without_conditions=without_conditions)
        return None
    alone = {
        word: prober(replace(words_call, query=word), limit=LOOSENED_ROWS, label={"query": word})
        for word in words
    }
    absent = [word for word, found in alone.items() if _none(found)]
    present = [word for word, found in alone.items() if found.status == FOUND]
    if any(alone[word].status == NONE_UNPROVEN for word in absent):
        out["proven"] = False
    if len(absent) == len(words):
        out.update(cause="query", without_conditions=without_conditions)
        return None
    if absent:
        out.update(cause="terms", absent_terms=absent)
        unchecked = [w for w in words if w not in absent and w not in present]
        if unchecked:
            out["terms_unchecked"] = unchecked
        rest = " ".join(word for word in words if word not in absent)
        loosened = replace(words_call, query=rest)
        if rest in present:
            # One word is left: its own probe already read the loosened rows.
            found = alone[rest]
        else:
            found = prober(loosened, limit=LOOSENED_ROWS, label={"query": rest})
        return [f"word {_value_text(word)}" for word in absent], loosened, found
    if len(present) == len(words):
        out["cause"] = "terms_together"
        if call.within == "record":
            out["within_record"] = True
        out.pop("proven", None)
        return None
    out["cause"] = "not_found"
    return None


def _attach_values(
    out: dict[str, Any], part: Part, *, prober: Prober, values: ValueReader, readable: bool,
) -> None:
    if part.attribute is None:
        return
    if not readable:
        out["values_listed"] = False
        return
    seconds = prober.seconds_left(cap=0.5)
    if seconds <= 0.05 or not prober.take():
        # No time or no read left: no values, and none claimed.
        return
    # Said in the block: the read is database work the reply costs.
    out["values_read"] = True
    seen = values(part.attribute, part.without, seconds)
    # The value the call sent is no typo hint: left out.
    seen = [value for value in seen or () if value not in part.sent]
    if seen:
        out["values_seen"] = {"field": part.attribute, "values": seen}


def values_of(rows: Sequence[Mapping[str, Any]], key: str) -> list[Any]:
    """Distinct short scalar values of `key` on `rows`, at most `VALUES_SHOWN`."""
    found: list[Any] = []
    for row in rows:
        value = (row.get("attrs") or {}).get(key)
        candidates = value if isinstance(value, list) else [value]
        for item in candidates:
            if isinstance(item, bool) or not isinstance(item, (str, int, float)):
                continue
            if (isinstance(item, str) and not item) or len(_value_text(item)) > VALUE_CHARS:
                # Long text or a long number: not a hint worth a line.
                continue
            if item not in found:
                found.append(item)
    return sorted(found, key=lambda v: (not isinstance(v, str), str(v)))[:VALUES_SHOWN]


# ── the call a reader sends for the loosened rows ─────────────────────────


def short_where(where: Optional[Mapping[str, Any]]) -> Any:
    """A canonical `where` in the short form agents write
    (`{"type": "ticket", "attrs.path": {"op": "prefix", "value": "a/"}}`),
    or as it is when it cannot be written so (`or`, `not`, a field twice)."""
    if not where:
        return None
    clauses = _conjuncts(dict(where))
    fields = [clause.get("field") for clause in clauses]
    if any(not isinstance(f, str) for f in fields) or len(set(fields)) != len(fields):
        return dict(where)
    out: dict[str, Any] = {}
    for clause in clauses:
        op = clause["op"]
        if op == "eq" and not isinstance(clause.get("value"), (dict, list)):
            out[clause["field"]] = clause.get("value")
        else:
            out[clause["field"]] = {k: v for k, v in clause.items() if k != "field"}
    return out


#: Search arguments the loosened call sets itself or leaves out (paging).
_SET_OR_DROPPED = frozenset({
    "query", "method", "where", "subtype_filter", "attr_filters", "filter", "path_prefix",
    "offset", "limit", "continuation_token", "profile", "source_family",
})


def search_call_arguments(
    sent: Optional[Mapping[str, Any]], without: Call,
    served: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """The search call that returns the loosened rows: the caller's own
    arguments (its `generation_id` and `authority` too) with the dropped part
    taken out and paging left out. `served` holds `profile` and
    `source_family` as the call was served: a rewrite may have read a sent
    `profile` as a type (issue #3666 step 2b), so the sent ones are not
    repeated."""
    arguments: dict[str, Any] = {}
    if without.query:
        arguments["query"] = without.query
    if without.method not in ("attrs", "lexical"):
        arguments["method"] = without.method
    where = short_where(without.where)
    if where:
        arguments["where"] = where
    if without.subtype_filter:
        arguments["subtype_filter"] = without.subtype_filter
    if without.attr_filters:
        arguments["attr_filters"] = dict(without.attr_filters)
    filt = {k: v for k, v in without.filter.items() if v}
    if filt:
        arguments["filter"] = filt
    if without.path_prefix:
        arguments["path_prefix"] = without.path_prefix
    arguments.update({k: v for k, v in (served or {}).items() if v is not None})
    arguments.update({
        key: value for key, value in (sent or {}).items()
        if key not in _SET_OR_DROPPED and value is not None
    })
    return arguments


def finish(block: dict[str, Any], build: Callable[[Call], dict[str, Any]]) -> dict[str, Any]:
    """`block` as served: the loosened call as `{tool, arguments}`."""
    loosened = block.get("loosened")
    if isinstance(loosened, dict) and isinstance(loosened.get("call"), Call):
        loosened["call"] = build(loosened["call"])
    return block


# ── the text ──────────────────────────────────────────────────────────────


def _strings(value: Any) -> list[str]:
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    return []


def _id_text(node_id: str) -> str:
    from okg.substrate.mcp.result_text import SEP, safe_json

    plain = node_id.isprintable() and SEP.strip() not in node_id and "," not in node_id
    return node_id if plain else safe_json(node_id)


def _types_tail(value: Mapping[str, Any]) -> str:
    types = _strings(value.get("types"))
    if not types:
        return ""
    tail = "; types: " + ", ".join(types)
    more = value.get("types_more")
    if isinstance(more, int) and not isinstance(more, bool) and more > 0:
        tail += f" and {more} more"
    return tail


#: Why a cause was not found, as the line says it.
_UNCHECKED_TEXT = {
    LIMITED: "probe limit reached",
    TIMED_OUT: "a check timed out",
    SKIPPED: "a check was not run: time budget spent",
    REFUSED: "a check was refused",
}


def _cause_line(value: Mapping[str, Any], who: str) -> Optional[str]:
    from okg.substrate.mcp.result_text import safe_json

    cause = value.get("cause")
    removed = _strings(value.get("removed_by"))
    scope = value.get("scope")
    within = f" within {scope}" if isinstance(scope, str) and scope else ""
    if cause == "unknown_type":
        names = _strings(value.get("unknown_types"))
        if not names:
            return None
        are = "is not a record type" if len(names) == 1 else "are not record types"
        return f"! 0 rows because {type_label(names)} {are} here" + _types_tail(value)
    if cause == "condition" and removed:
        line = "! 0 rows because of " + (
            removed[0] if len(removed) == 1
            else "each of: " + "; ".join(removed) + " (dropping any one brings rows back)"
        )
        if value.get("not_all_checked"):
            line += "; other conditions not all checked"
        seen = value.get("values_seen")
        if isinstance(seen, Mapping) and isinstance(seen.get("values"), list):
            line += (
                f"; other values of {seen.get('field')} seen: "
                + ", ".join(safe_json(v) for v in seen["values"])
            )
        elif value.get("values_listed") is False:
            line += "; values not listed (access rules)"
        return line
    if cause == "conditions" and removed:
        line = "! 0 rows because of the conditions (" + "; ".join(removed) + "): "
        line += (
            "not each checked alone ("
            + _UNCHECKED_TEXT.get(value.get("unchecked"), "a check was not run") + ")"
            if value.get("each_alone") == "unchecked"
            else "dropping any one still leaves 0 rows"
        )
        if value.get("all_dropped") == FOUND:
            line += "; dropping all brings rows back"
        return line
    if cause == "terms":
        absent = _strings(value.get("absent_terms"))
        if not absent:
            return None
        line = (
            "! 0 rows: " + ("word " if len(absent) == 1 else "words ")
            + ", ".join(safe_json(w) for w in absent)
            + (" alone matches " if len(absent) == 1 else " alone match ") + who + within
        )
        unchecked = _strings(value.get("terms_unchecked"))
        if unchecked:
            line += "; not checked: " + ", ".join(safe_json(w) for w in unchecked)
        return line
    if cause == "terms_together":
        holder = "record" if value.get("within_record") else "text"
        return f"! 0 rows: each word matches alone{within}; no single {holder} holds them all"
    if cause == "query":
        tail = (
            "even without the conditions" if value.get("without_conditions")
            else "and the call has no condition to loosen"
        )
        return f"! 0 rows: the query itself matches {who}{within}, {tail}"
    if cause == "page_timed_out":
        return "! 0 rows: the read timed out before it finished; the cause is unknown, nothing claimed"
    if cause == "not_found":
        why = _UNCHECKED_TEXT.get(value.get("unchecked"), "a check was not run")
        return f"! 0 rows: cause not found ({why})"
    if cause == "not_checked":
        return "! 0 rows: cause not checked (semantic search)"
    return None


def _partial_types_line(value: Mapping[str, Any]) -> Optional[str]:
    names = _strings(value.get("unknown_types"))
    if not value.get("partial_types") or not names:
        return None
    are = "is not a record type" if len(names) == 1 else "are not record types"
    return (
        f"! {type_label(names)} {are} here; the other types named still applied"
        + _types_tail(value)
    )


def _loosened_line(loosened: Any, who: str) -> Optional[str]:
    from okg.substrate.mcp.result_text import _retry_call_text

    if not isinstance(loosened, Mapping):
        return None
    dropped = "; ".join(_strings(loosened.get("dropped")))
    status = loosened.get("status")
    if status == FOUND:
        ids = _strings(loosened.get("ids"))
        if not ids:
            return None
        call = loosened.get("call")
        text = (
            _retry_call_text(dict(call))
            if isinstance(call, Mapping) and isinstance(call.get("tool"), str)
            and isinstance(call.get("arguments"), Mapping) else None
        )
        line = (
            f"! loosened, not results (not counted): without {dropped}: "
            + ", ".join(_id_text(node_id) for node_id in ids)
        )
        if text is not None and len(text) <= CALL_CHARS:
            line += f"; {text}"
        return line
    if status in (TIMED_OUT, SKIPPED, REFUSED, LIMITED):
        why = {
            TIMED_OUT: "timed out", SKIPPED: "not run",
            REFUSED: "that call is refused", LIMITED: "probe limit reached",
        }[status]
        return f"! loosened: without {dropped} not checked ({why})"
    if status == NONE_UNPROVEN:
        return f"! loosened: without {dropped}, no row on its first page (not a total)"
    if status == NONE:
        return f"! loosened: without {dropped}, still {who}"
    return None


def render(value: Any) -> list[str]:
    """The notice lines an `empty_why` block prints (none when malformed)."""
    if not isinstance(value, Mapping):
        return []
    who = "no record you can read" if value.get("access_rules") else "no record"
    line = _cause_line(value, who)
    if line is None:
        return []
    if value.get("proven") is False:
        line += " (a bounded read: not proven complete)"
    lines = [line]
    partial = _partial_types_line(value)
    if partial is not None:
        lines.append(partial)
    loosened = _loosened_line(value.get("loosened"), who)
    if loosened is not None:
        lines.append(loosened)
    return lines

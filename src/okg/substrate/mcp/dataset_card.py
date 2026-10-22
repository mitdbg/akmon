"""The dataset card: what this graph holds, compiled at server start.

PACT dataset-card. Before it, an agent connected to okg learned almost
nothing about the data behind the server: okg's own instructions described
okg internals, and a deployment's hand-written file replaced them and went
stale ("plain text search does NOT match file paths" was false). The card is
compiled, never authored: every sentence comes from the catalog, the pinned
published generation, the deployment manifest, or a check run at start.

Two steps, so the database part stays small and testable:

- `gather_dataset_facts` reads the facts once, at start, at the generation
  the capability probe pinned. Every statement is capped at the time left of
  its read (`CARD_STATEMENT_TIMEOUT_MS`) and of one deadline shared by all
  reads (`GATHER_DEADLINE_MS`), and a statement with no time left is not
  sent, so the gather ends within the deadline plus client-side work. Any failure of a part (a timeout, a database error, a bug) is
  logged, the part is recorded as missing with the reason, and the card says
  that part is missing instead of guessing. A card failure never stops the
  server from starting.
- `render_dataset_card` turns the facts into text of at most
  `CARD_BUDGET_BYTES`, sections ordered by value. When the text is too long
  the lowest sections lose lines first, and each cut says how many lines it
  dropped; nothing is dropped silently.

Two rules keep row content out of the server instructions:

- Visibility. The card is one text for every session, with no reader
  identity. Whenever Nomos is not `off` (`audit` filters rows per identity
  too, and a multi-user server sets identities per request), nothing
  row-derived is read: no counts, source names, ids, dates or path check.
  Catalog facts (nesting, live relation names) and the publish date stay.
- Prose. Ids are stated as patterns only, never as raw examples. A segment
  is kept literally only when every sampled id shares it, it is a short
  identifier (`_LITERAL`), it is not a run of three or more `_`-joined words,
  and every sampled id of that type comes from a trusted producer
  (`provenance.classify_sources`); anything else becomes a placeholder.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, TypeVar

import psycopg

__all__ = [
    "CARD_BUDGET_BYTES",
    "CARD_HEADER",
    "CARD_STATEMENT_TIMEOUT_MS",
    "DEPLOYMENT_GUIDANCE_HEADING",
    "DatasetFacts",
    "GATHER_DEADLINE_MS",
    "ID_SAMPLES_PER_SUBTYPE",
    "PathProbe",
    "SourceCount",
    "card_inputs",
    "card_unavailable",
    "facts_without_database",
    "gather_dataset_facts",
    "id_pattern",
    "render_dataset_card",
    "run_example_steps",
    "split_dataset_card",
]

log = logging.getLogger(__name__)

#: The card's hard size limit, in UTF-8 bytes.
CARD_BUDGET_BYTES = 2_048
#: Each start-up read of the card runs under at most this statement timeout.
CARD_STATEMENT_TIMEOUT_MS = 3_000
#: All start-up reads together. A part whose turn comes later is not read.
GATHER_DEADLINE_MS = 5_000
#: How many ids per type the id-pattern read samples (both ends of the id order plus a scan-order sample).
ID_SAMPLES_PER_SUBTYPE = 50
#: Where the counts came from, labelled as `inspect(kind="graph")` labels them.
COUNTS_SOURCE_ROLLUP = "console_count_rollup"
COUNTS_SOURCE_SCAN = "graph_scan"
#: The label of nodes no producer is recorded for.
NO_SOURCE_LABEL = "no source"
#: The card's first words.
CARD_HEADER = "DATASET CARD"
#: The fixed heading a deployment's own instruction file is served under.
DEPLOYMENT_GUIDANCE_HEADING = "## Deployment guidance"

#: The parts of the card a start-up read can leave missing.
PART_PUBLISHED = "published_date"
PART_CATALOG = "catalog"
PART_COUNTS = "counts"
PART_TRUST = "trust"
PART_IDS = "ids"
PART_DATES = "dates"
PART_PATH_CHECK = "path_check"
PART_RUN_EXAMPLE = "run_example"
PART_ATTRS = "attrs"
#: The `query` description's fields line (issue #3666): how many of the
#: largest types, and how many rows of each, the start-up sample reads.
ATTR_SAMPLE_TYPES = 5
ATTR_SAMPLE_ROWS = 50
#: The `run` example's key holds at least this many distinct non-null
#: values in the attribute sample, so its groups are more than one.
RUN_EXAMPLE_MIN_VALUES = 2
#: The `run` example prefers a type with at most this many records, so
#: both of its steps finish well inside a step's time budget.
RUN_EXAMPLE_MAX_ROWS = 10_000
#: How many of the largest types under that bound the start-up sample also
#: reads for the example, beside the largest types.
RUN_EXAMPLE_SAMPLE_TYPES = 5
#: Groups the `run` example prints: the most common values of its key.
RUN_EXAMPLE_GROUPS = 10

_MAX_SUBTYPES_PER_SOURCE = 3
#: A segment of an id, or a source name, that the card may state literally.
_LITERAL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
#: The word-like parts of a value: split at `_`, `-`, `.` and CamelCase
#: boundaries, keeping parts that contain a letter. More than
#: `_MAX_LITERAL_WORDS` of them reads as a phrase, and is never stated.
_WORD_PARTS = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
_MAX_LITERAL_WORDS = 2
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_HEX = re.compile(r"^[0-9a-f]{7,}$")
_WORDS = re.compile(r"[A-Za-z]{4,}")

_T = TypeVar("_T")


@dataclass(frozen=True)
class SourceCount:
    """Rows of one source at the pin, by subtype (largest first)."""

    source: str
    total: int
    subtypes: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class PathProbe:
    """The start-up check that a word inside a path matches lexical search."""

    subtype: str
    node_id: str
    field: str
    word: str
    hit: bool


@dataclass(frozen=True)
class DatasetFacts:
    """Everything the card says, and where each part came from."""

    deployment: str
    #: `live`, `no_database`, `no_generation`, or `unavailable`.
    mode: str
    generation_id: Optional[str] = None
    catalog_version_id: Optional[int] = None
    #: The day the pinned generation was published (#3666: it was called
    #: `snapshot_date`, which reads as the day the source data was taken).
    published_date: Optional[str] = None
    #: None when the counts were not read; see `missing`.
    source_counts: Optional[tuple[SourceCount, ...]] = None
    #: `COUNTS_SOURCE_ROLLUP` or `COUNTS_SOURCE_SCAN`, and why a scan.
    counts_source: Optional[str] = None
    counts_source_reason: Optional[str] = None
    #: (subtype, pattern), largest subtype first, or None when not read.
    id_patterns: Optional[tuple[tuple[str, str], ...]] = None
    #: (subtype, meaning, oldest, newest), or None when the read failed.
    date_ranges: Optional[tuple[tuple[str, str, str, str], ...]] = None
    #: (parent subtype, child subtype, edge type) from passage rollups.
    nesting: tuple[tuple[str, str, str], ...] = ()
    #: (subtype, relation names) from catalog live-relation declarations.
    live_relations: tuple[tuple[str, tuple[str, ...]], ...] = ()
    catalog_read: bool = False
    #: Every subtype the pinned catalog declares, by name. Not row-derived,
    #: so read under every Nomos mode; names `aggregate`'s example subtype
    #: when the counts were not read (`tool_prose.example_subtype_for`).
    catalog_subtypes: tuple[str, ...] = ()
    path_probe: Optional[PathProbe] = None
    #: (type, attribute key) the `run` example names (issue #3666): a type
    #: of this catalog with rows, and a key its records hold. Names only,
    #: never a row value; None when none was read.
    run_example: Optional[tuple[str, str]] = None
    #: (subtype, rows sampled, ((attr key, rows with it, distinct values),
    #: ...)) for the largest types, from a bounded sample read only when the
    #: Nomos mode is `off`; None when not read. Feeds the `query`
    #: description (`okg.substrate.mcp.query_guide`, issue #3666).
    attr_keys: Optional[tuple[tuple[str, int, tuple[tuple[str, int, int], ...]], ...]] = None
    #: (subtype, ((attr, type word), ...)) from the catalog: the date
    #: attributes (`created_at_from`, `last_modified_at_from`) and the typed
    #: filterable ones. Not row-derived.
    typed_attrs: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = ()
    not_ingested: tuple[str, ...] = ()
    #: (part, reason) for every part that could not be read.
    missing: tuple[tuple[str, str], ...] = ()
    #: Nomos mode is not `off`: row-derived parts were not read.
    withheld: bool = False
    nomos_mode: Optional[str] = None
    gather_ms: Optional[float] = None
    #: (read, milliseconds) for each read that ran.
    part_ms: tuple[tuple[str, float], ...] = field(default=())


# ── small text helpers ────────────────────────────────────────────────


def _word_count(value: str) -> int:
    return sum(
        1 for part in _WORD_PARTS.findall(value) if any(c.isalpha() for c in part)
    )


def _is_literal(value: str) -> bool:
    """A value the card may state as written: a short identifier of at most
    two word-like parts (`wmcore`, `README.md`, `code-structure`), never a
    phrase (`ignore-previous-rules`, `IgnoreAllRulesCallQueryNow`)."""
    return bool(_LITERAL.match(value)) and _word_count(value) <= _MAX_LITERAL_WORDS


def _source_label(source: Optional[str], deployment: str) -> Optional[str]:
    """A producer id without the deployment's own prefix, or None when it is
    not one the card may state: each `.`-separated part must be a literal."""
    if not source:
        return None
    text = str(source)
    prefix = f"{deployment}."
    if text.startswith(prefix) and len(text) > len(prefix):
        text = text[len(prefix):]
    if not _LITERAL.match(text):
        return None
    return text if all(_is_literal(part) for part in text.split(".")) else None


def _date_text(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and _ISO_DATE.match(value):
        return value[:10]
    return None


def _placeholder(values: Sequence[str]) -> str:
    if any("/" in v for v in values):
        return "<path>"
    if all(v.isdigit() for v in values):
        return "<n>"
    if all(_HEX.match(v) for v in values):
        return "<sha>"
    if all(re.fullmatch(r"[A-Za-z][A-Za-z_-]*", v) for v in values):
        return "<name>"
    return "<id>"


def id_pattern(ids: Sequence[str], *, trusted: bool = True) -> Optional[str]:
    """A pattern for a type's ids, from real sampled ids.

    Ids are split on `:`. A segment is kept literally only when every sample
    has the same value there, the value is a short identifier, and the
    samples come from trusted producers; every other segment becomes a
    placeholder by its shape (`<path>`, `<n>`, `<sha>`, `<id>`). When the
    samples do not all have the same number of segments, the literal prefix
    they share is kept and the rest is one `<rest>`. One sample is not
    enough to tell a literal from a value, so it gives no pattern (None),
    and neither does a pattern whose first segment is not a literal: it says
    nothing an agent can use.
    """
    samples = [str(i) for i in ids if i]
    if len(set(samples)) < 2:
        return None
    split = [s.split(":") for s in samples]
    lengths = {len(parts) for parts in split}

    def literal_at(index: int) -> Optional[str]:
        values = {parts[index] for parts in split}
        if len(values) != 1 or not trusted:
            return None
        value = next(iter(values))
        return value if _is_literal(value) else None

    if literal_at(0) is None:
        return None
    if len(lengths) == 1:
        # Every sample has this many segments: each keeps its own shape.
        out = []
        for index in range(lengths.pop()):
            literal = literal_at(index)
            out.append(
                literal if literal is not None
                else _placeholder([parts[index] for parts in split])
            )
        return ":".join(out)
    prefix: list[str] = []
    for index in range(min(lengths)):
        literal = literal_at(index)
        if literal is None:
            break
        prefix.append(literal)
    return ":".join([*prefix, "<rest>"])


def _path_word(path: str, other_text: str) -> Optional[str]:
    """A word from a path that the rest of the row's text lacks.

    The longest word of 4+ letters, file name first. A word that also appears
    in the row's other searchable text would let the check pass without the
    path, so it is skipped.
    """
    lowered = other_text.lower()
    components = [c for c in re.split(r"[/\\]+", path) if c]
    for component in reversed(components):
        words = sorted(set(_WORDS.findall(component)), key=lambda w: (-len(w), w))
        for word in words:
            if word.lower() not in lowered:
                return word
    return None


def _string_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(v.strip() for v in value.split(",") if v.strip())
    if isinstance(value, (list, tuple)):
        return tuple(str(v) for v in value if str(v))
    return ()


def _date_meaning(meta: Mapping[str, Any]) -> Optional[tuple[str, str]]:
    """The attribute a subtype's record date comes from, read as
    `handle_fields` reads it: `created_at_from`, else `last_modified_at_from`."""
    for meaning, key in (
        ("created", "created_at_from"),
        ("modified", "last_modified_at_from"),
    ):
        attr = meta.get(key)
        if isinstance(attr, str) and attr:
            return meaning, attr
    return None


# ── gathering (one connection, at start) ──────────────────────────────


class _PartFailed(Exception):
    """One part of the card could not be read; `reason` says why."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _DeadlineSpent(Exception):
    """A statement was not sent: no time was left for it."""


class _Gather:
    """The reads of one gather: a shared deadline and per-read timings."""

    def __init__(self, conn: psycopg.Connection, *, timeout_ms: int, deadline_ms: int) -> None:
        self.conn = conn
        self.timeout_ms = timeout_ms
        self.deadline_ms = deadline_ms
        self.started = time.perf_counter()
        self.part_ms: list[tuple[str, float]] = []
        self.broken = False

    def remaining_ms(self) -> int:
        return int(self.deadline_ms - (time.perf_counter() - self.started) * 1000.0)

    def read(self, name: str, fn: Callable[[], _T]) -> _T:
        """Run `fn()` in its own rolled-back transaction.

        Every statement `fn` runs on this connection, however many, first
        checks the time left: its `statement_timeout` is the smaller of what
        is left of this read's `timeout_ms` and of the shared deadline, and a
        statement with no time left is not sent. So the reads together end
        within `deadline_ms` plus the client-side work between statements.

        Every failure (a timeout, a database error, anything else) becomes
        `_PartFailed` naming the read, so one part never takes the rest of
        the card, or the server start, with it.
        """
        if self.broken:
            raise _PartFailed(f"the {name} read was skipped: the connection failed earlier")
        remaining = self.remaining_ms()
        if remaining < 50:
            raise _PartFailed(
                f"the {name} read was skipped: the {self.deadline_ms} ms start-up "
                "budget was spent"
            )
        conn = self.conn
        previous = conn.autocommit
        previous_factory = conn.cursor_factory
        began = time.perf_counter()
        gather = self

        def left_ms() -> int:
            spent = (time.perf_counter() - began) * 1000.0
            return int(min(gather.timeout_ms - spent, gather.remaining_ms()))

        class _BoundedCursor(previous_factory):  # type: ignore[misc,valid-type]
            """Caps each statement at the time left (see `read`)."""

            def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
                left = left_ms()
                if left < 20:
                    raise _DeadlineSpent()
                super().execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{left}ms",),
                )
                return super().execute(query, params, **kwargs)

        try:
            conn.autocommit = False
            conn.cursor_factory = _BoundedCursor
            return fn()
        except (psycopg.errors.QueryCanceled, _DeadlineSpent) as exc:
            raise _PartFailed(
                f"the {name} read ran out of time (at most "
                f"{min(self.timeout_ms, remaining)} ms)"
            ) from exc
        except Exception as exc:  # noqa: BLE001 - a card part never stops the server
            log.warning("dataset card: the %s read failed", name, exc_info=True)
            raise _PartFailed(f"the {name} read failed ({type(exc).__name__})") from exc
        finally:
            self.part_ms.append((name, round((time.perf_counter() - began) * 1000.0, 3)))
            conn.cursor_factory = previous_factory
            try:
                conn.rollback()
                conn.autocommit = previous
            except Exception:  # noqa: BLE001 - see `broken`
                log.warning("dataset card: the connection failed after the %s read", name)
                self.broken = True


def _generation_reader(
    conn: psycopg.Connection, generation_id: str,
) -> Callable[..., list[dict[str, Any]]]:
    """One statement at a time against the pinned generation's checkpoint
    (`query_generation_once`, the read path the schema digest uses)."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(
        conn, ensure_schema=False, autocommit=False,
    )

    def read(sql: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        return store.query_generation_once(generation_id, sql, params or {})

    return read


_CATALOG_SQL = (
    "SELECT name, metadata FROM okg.subtypes "
    "WHERE catalog_version_id = %(cv)s ORDER BY name"
)
#: Fallback when the maintained count rollup cannot serve the pin.
_COUNTS_SCAN_SQL = (
    "SELECT source, subtype, count(*) AS n FROM okg.graph_nodes "
    "GROUP BY source, subtype"
)
#: The first and last id of every subtype in id order: one grouped pass.
#: Measured on a 633k-node graph: about 200 ms. (Reading 25 ids from each
#: end per subtype with ORDER BY took 19 s there: one sort per subtype.)
_ID_ENDS_SQL = (
    "SELECT subtype, min(node_id) AS first_id, max(node_id) AS last_id "
    "FROM okg.graph_nodes WHERE subtype = ANY(%(subtypes)s) GROUP BY subtype"
)
#: An attribute key the example may print inside its JSON.
_SAFE_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$")
#: A type name the example may print inside its JSON.
_SAFE_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,63}$")
#: Up to `ID_SAMPLES_PER_SUBTYPE - 2` more ids per subtype, in scan order: a
#: scan that stops at the limit (9 ms on that graph). With the two ends above,
#: a segment kept literally is shared by both ends of the id order too.
_ID_SAMPLE_SQL = (
    "SELECT s.subtype, x.node_id FROM unnest(%(subtypes)s::text[]) "
    "AS s(subtype) CROSS JOIN LATERAL ("
    "SELECT node_id FROM okg.graph_nodes n WHERE n.subtype = s.subtype "
    "LIMIT %(limit)s) x"
)
#: Up to `ATTR_SAMPLE_ROWS` rows of each of the largest types, in scan order:
#: per attribute key, how many sampled rows carry it and how many distinct
#: values (the first 200 characters) they hold, plus the rows sampled. The
#: `query` description names the frequent keys and counts by a few-valued
#: one (issue #3666). Row-derived: read only when the Nomos mode is `off`.
_ATTR_SAMPLE_SQL = (
    "SELECT x.subtype, k.key, count(*) AS rows_with, "
    "count(DISTINCT left(x.attrs ->> k.key, 200)) AS distinct_values, "
    "max(x.sampled) AS sampled FROM ("
    "SELECT s.subtype, y.attrs, count(*) OVER (PARTITION BY s.subtype) AS sampled "
    "FROM unnest(%(subtypes)s::text[]) AS s(subtype) CROSS JOIN LATERAL ("
    "SELECT attrs FROM okg.graph_nodes n WHERE n.subtype = s.subtype "
    "LIMIT %(limit)s) y WHERE jsonb_typeof(y.attrs) = 'object') x "
    "CROSS JOIN LATERAL jsonb_object_keys(x.attrs) AS k(key) "
    "GROUP BY x.subtype, k.key"
)


def _published_date(
    conn: psycopg.Connection, deployment_name: str, generation_id: str,
) -> Optional[str]:
    from okg.substrate.generation_resolver import resolve_okg_generation

    resolved = resolve_okg_generation(
        conn, deployment_name=deployment_name, requested=generation_id,
        branch=None, status="published",
    )
    if resolved is None:
        return None
    published = (resolved.metadata or {}).get("published_at")
    return _date_text(published) or _date_text(resolved.created_at)


def _rollup_counts(
    conn: psycopg.Connection, generation_id: str, branch_name: str,
) -> tuple[Optional[list[tuple[Optional[str], str, int]]], Optional[str]]:
    """Per-(producer, subtype) counts from the maintained count rollup, the
    source `inspect(kind="graph")` uses, or (None, why it cannot serve).

    The rollup's per-producer rows count only nodes that HAVE a source; its
    per-subtype rows, which `inspect(kind="graph")` reports, count every
    node. Both are read in one statement from the same snapshot, and each
    subtype's difference is returned as a `None` (no source) row, so the
    card's numbers add up to `inspect(kind="graph")`'s exactly.
    """
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupUnavailable,
        read_console_count_rollup,
        require_console_count_rollup_enabled,
    )
    from okg.substrate.derived_artifacts.maintained_derivation_constants import (
        COUNT_KIND_SOURCE,
        COUNT_KIND_SOURCE_SUBTYPE,
        COUNT_KIND_SUBTYPE,
        parse_source_subtype,
    )
    from okg.substrate.graph_store import DEFAULT_BRANCH

    if branch_name != DEFAULT_BRANCH:
        return None, "non_default_branch"
    try:
        require_console_count_rollup_enabled(generation_id=generation_id)
        counts = read_console_count_rollup(
            conn, generation_id=generation_id,
            kinds={COUNT_KIND_SOURCE_SUBTYPE, COUNT_KIND_SOURCE, COUNT_KIND_SUBTYPE},
        )
    except ConsoleCountRollupUnavailable as exc:
        return None, str(getattr(exc, "reason", None) or "unavailable")
    rows: list[tuple[Optional[str], str, int]] = []
    by_subtype: dict[str, int] = {}
    attributed: dict[str, int] = {}
    sources_present = False
    for (kind, value), n in counts.items():
        if kind == COUNT_KIND_SUBTYPE:
            by_subtype[value] = int(n)
        elif kind == COUNT_KIND_SOURCE:
            sources_present = True
        else:
            producer, subtype = parse_source_subtype(value)
            rows.append((producer, subtype, int(n)))
            attributed[subtype] = attributed.get(subtype, 0) + int(n)
    if sources_present and not rows:
        return None, "source_subtype_dimension_absent"
    for subtype, total in by_subtype.items():
        unattributed = total - attributed.get(subtype, 0)
        if unattributed < 0:
            return None, "rollup_dimensions_disagree"
        if unattributed:
            rows.append((None, subtype, unattributed))
    return rows, None


def _run_example(
    totals: Mapping[str, int],
    nesting: Sequence[tuple[str, str, str]],
    metadata: Mapping[str, Mapping[str, Any]],
    attr_keys: Optional[tuple[tuple[str, int, tuple[tuple[str, int, int], ...]], ...]],
    missing: list[tuple[str, str]],
) -> Optional[tuple[str, str]]:
    """(type, key) for the `run` example (issue #3666), from what the card
    already read. No read of its own.

    A key qualifies only when the attribute sample saw at least
    `RUN_EXAMPLE_MIN_VALUES` distinct non-null values of it: grouping by an
    all-null key prints one group and teaches nothing. Of a type's
    qualifying keys, a declared filterable one comes first, then the one
    most sampled rows hold, then the fewest distinct values, then the name.
    Of the sampled types (not passage children) with such a key, the
    largest with at most `RUN_EXAMPLE_MAX_ROWS` records is chosen: the
    example must finish well inside a step's budget. With no such type
    there is no example, and the card records why under `missing`."""
    children = {child for _parent, child, _edge in nesting}
    if attr_keys is None:
        missing.append((PART_RUN_EXAMPLE, "the attribute sample was not read"))
        return None
    candidates: list[tuple[str, str]] = []
    for subtype, _rows, keys in attr_keys:
        if subtype in children or not _SAFE_TYPE.match(subtype) or totals.get(subtype, 0) <= 0:
            continue
        filterable = metadata.get(subtype, {}).get("filterable_attrs")
        declared = {str(k) for k in filterable} if isinstance(filterable, dict) else set()
        ranked = sorted(
            (
                key for key in keys
                if _SAFE_KEY.match(key[0]) and key[2] >= RUN_EXAMPLE_MIN_VALUES
            ),
            key=lambda key: (key[0] not in declared, -key[1], key[2], key[0]),
        )
        if ranked:
            candidates.append((subtype, ranked[0][0]))
    if not candidates:
        missing.append((
            PART_RUN_EXAMPLE,
            f"no sampled type holds a key with {RUN_EXAMPLE_MIN_VALUES} or more values",
        ))
        return None
    bounded = [c for c in candidates if totals[c[0]] <= RUN_EXAMPLE_MAX_ROWS]
    if not bounded:
        missing.append((
            PART_RUN_EXAMPLE,
            f"every sampled type with such a key holds more than "
            f"{RUN_EXAMPLE_MAX_ROWS:,} records",
        ))
        return None
    return min(bounded, key=lambda c: (-totals[c[0]], c[0]))


def gather_dataset_facts(
    conn: Optional[psycopg.Connection],
    *,
    deployment_name: str,
    branch_name: str,
    pinned: Optional[Mapping[str, Any]],
    not_ingested: Sequence[str] = (),
    nomos_mode: str = "off",
    no_pin_reason: str = "no published generation existed at server start",
    timeout_ms: Optional[int] = None,
    deadline_ms: Optional[int] = None,
) -> DatasetFacts:
    """Read the card's facts at the generation the capability probe pinned.

    `pinned` is the capability probe's pin (`generation_id`,
    `catalog_version_id`), so the card describes the generation a new
    session reads. None means the probe pinned nothing (`no_pin_reason`
    says why); `conn` is then not used and may be None. Unless `nomos_mode`
    is `off`, no row-derived read runs. Never raises for a part that fails.
    """
    started = time.perf_counter()
    declared = tuple(str(item) for item in not_ingested)
    if pinned is None:
        return _finish(
            DatasetFacts(
                deployment=deployment_name, mode="no_generation",
                not_ingested=declared, nomos_mode=nomos_mode,
            ),
            [(PART_PUBLISHED, no_pin_reason)], (), started, withheld=False,
        )
    if conn is None:
        raise ValueError("gather_dataset_facts needs a connection when pinned")
    g = _Gather(
        conn,
        timeout_ms=CARD_STATEMENT_TIMEOUT_MS if timeout_ms is None else timeout_ms,
        deadline_ms=GATHER_DEADLINE_MS if deadline_ms is None else deadline_ms,
    )
    generation_id = str(pinned["generation_id"])
    cv = int(pinned["catalog_version_id"])
    missing: list[tuple[str, str]] = []

    # ── publish date and catalog: not row-derived ──
    published: Optional[str] = None
    try:
        published = g.read(
            PART_PUBLISHED, lambda: _published_date(conn, deployment_name, generation_id),
        )
        if published is None:
            missing.append((PART_PUBLISHED, "the generation carries no publish date"))
    except _PartFailed as exc:
        missing.append((PART_PUBLISHED, exc.reason))

    metadata: dict[str, dict[str, Any]] = {}
    catalog_read = False

    def _catalog() -> list[tuple[Any, ...]]:
        with conn.cursor() as cur:
            cur.execute(_CATALOG_SQL, {"cv": cv})
            return list(cur.fetchall())

    try:
        for name, meta in g.read("catalog", _catalog):
            metadata[str(name)] = dict(meta) if isinstance(meta, dict) else {}
        catalog_read = True
    except _PartFailed as exc:
        missing.append((PART_CATALOG, exc.reason))

    nesting: list[tuple[str, str, str]] = []
    live: list[tuple[str, tuple[str, ...]]] = []
    for name, meta in sorted(metadata.items()):
        rollup = meta.get("passage_rollup")
        if isinstance(rollup, dict):
            child, edge = rollup.get("child_subtype"), rollup.get("edge_type")
            if isinstance(child, str) and child and isinstance(edge, str) and edge:
                nesting.append((name, child, edge))
        relations = meta.get("live_relations")
        if isinstance(relations, dict) and relations:
            live.append((name, tuple(sorted(str(k) for k in relations))))

    typed: list[tuple[str, tuple[tuple[str, str], ...]]] = []
    for name, meta in sorted(metadata.items()):
        typed_attrs: dict[str, str] = {}
        filterable = meta.get("filterable_attrs")
        if isinstance(filterable, dict):
            for attr, kind in filterable.items():
                word = {"num": "number", "ts": "date", "bool": "bool"}.get(str(kind))
                if word:
                    typed_attrs[str(attr)] = word
        for key in ("created_at_from", "last_modified_at_from"):
            attr = meta.get(key)
            if isinstance(attr, str) and attr:
                typed_attrs[attr] = "date"
        if typed_attrs:
            typed.append((name, tuple(sorted(typed_attrs.items()))))

    base = DatasetFacts(
        deployment=deployment_name, mode="live",
        generation_id=generation_id, catalog_version_id=cv,
        published_date=published, nesting=tuple(nesting),
        live_relations=tuple(live), catalog_read=catalog_read,
        catalog_subtypes=tuple(sorted(metadata)),
        typed_attrs=tuple(typed),
        not_ingested=declared, nomos_mode=nomos_mode,
    )
    # One subject-less text for every session. `audit` filters rows per
    # identity too, and a multi-user server sets identities per request, so
    # under any mode but `off` nothing row-derived is read.
    if nomos_mode != "off":
        return _finish(base, missing, g.part_ms, started, withheld=True)

    read = _generation_reader(conn, generation_id)

    # ── counts: the maintained rollup, else one bounded grouped scan ──
    counts: Optional[tuple[SourceCount, ...]] = None
    counts_source: Optional[str] = None
    counts_reason: Optional[str] = None
    raw_sources: dict[str, str] = {}
    producers: dict[str, set[str]] = {}
    totals: dict[str, int] = {}
    try:
        rows, counts_reason = g.read(
            "counts_rollup", lambda: _rollup_counts(conn, generation_id, branch_name),
        )
        counts_source = COUNTS_SOURCE_ROLLUP
        if rows is None:
            log.warning(
                "dataset card: the count rollup cannot serve generation %s (%s); "
                "counting with a bounded scan",
                generation_id, counts_reason,
            )
            counts_source = COUNTS_SOURCE_SCAN
            rows = [
                (r.get("source"), str(r["subtype"]), int(r["n"]))
                for r in g.read("counts_scan", lambda: read(_COUNTS_SCAN_SQL))
            ]
        by_source: dict[str, dict[str, int]] = {}
        for source, subtype, n in sorted(rows, key=lambda r: (str(r[0] or ""), r[1])):
            key = str(source or "")
            label = (
                _source_label(source, deployment_name) if source else NO_SOURCE_LABEL
            )
            raw_sources.setdefault(key, label or f"source #{len(raw_sources) + 1}")
            producers.setdefault(key, set()).add(subtype)
            per = by_source.setdefault(raw_sources[key], {})
            per[subtype] = per.get(subtype, 0) + n
            totals[subtype] = totals.get(subtype, 0) + n
        counts = tuple(sorted(
            (
                SourceCount(
                    source=source,
                    total=sum(per.values()),
                    subtypes=tuple(sorted(per.items(), key=lambda kv: (-kv[1], kv[0]))),
                )
                for source, per in by_source.items()
            ),
            key=lambda sc: (-sc.total, sc.source),
        ))
    except _PartFailed as exc:
        missing.append((PART_COUNTS, exc.reason))

    # ── id patterns: sampled ids, literals only from trusted producers ──
    patterns: Optional[tuple[tuple[str, str], ...]] = None
    if counts is not None and totals:
        try:
            trust = g.read(
                "trust", lambda: _classify(conn, deployment_name, list(raw_sources)),
            )
            def _sample() -> list[dict[str, Any]]:
                subtypes = sorted(totals)
                return [
                    *read(_ID_ENDS_SQL, {"subtypes": subtypes}),
                    *read(_ID_SAMPLE_SQL, {
                        "subtypes": subtypes, "limit": ID_SAMPLES_PER_SUBTYPE - 2,
                    }),
                ]

            by_type: dict[str, list[str]] = {}
            for row in g.read("ids", _sample):
                ids = by_type.setdefault(str(row["subtype"]), [])
                for key in ("first_id", "last_id", "node_id"):
                    if row.get(key) is not None:
                        ids.append(str(row[key]))
            found = []
            for subtype in sorted(by_type, key=lambda s: (-totals.get(s, 0), s)):
                # Trusted only when every producer of this type is trusted.
                trusted = all(
                    trust.get(key) == "trusted"
                    for key, subtypes in producers.items() if subtype in subtypes
                )
                pattern = id_pattern(by_type[subtype], trusted=trusted)
                if pattern is not None:
                    found.append((subtype, pattern))
            patterns = tuple(found)
        except _PartFailed as exc:
            missing.append((PART_IDS, exc.reason))
    elif counts is None:
        missing.append((PART_IDS, "skipped: the row counts were not read"))

    # ── dates: one grouped scan over the subtypes that declare a date ──
    dated: dict[str, tuple[str, str]] = {}
    for name, meta in metadata.items():
        meaning = _date_meaning(meta)
        if meaning is not None and (counts is None or name in totals):
            dated[name] = meaning
    date_ranges: Optional[tuple[tuple[str, str, str, str], ...]] = ()
    if dated:
        cases: list[str] = []
        params: dict[str, Any] = {"subtypes": sorted(dated)}
        for index, name in enumerate(sorted(dated)):
            cases.append(f"WHEN %(s{index})s THEN attrs ->> %(a{index})s")
            params[f"s{index}"] = name
            params[f"a{index}"] = dated[name][1]
        sql = (
            "SELECT subtype, min(d) AS oldest, max(d) AS newest FROM ("
            "SELECT subtype, CASE subtype " + " ".join(cases) + " END AS d "
            "FROM okg.graph_nodes WHERE subtype = ANY(%(subtypes)s)) dated "
            "WHERE d ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' GROUP BY subtype"
        )
        try:
            rows = g.read("dates", lambda: read(sql, params))
            found_dates = []
            for row in rows:
                subtype = str(row["subtype"])
                oldest, newest = _date_text(row["oldest"]), _date_text(row["newest"])
                if oldest and newest and subtype in dated:
                    found_dates.append((subtype, dated[subtype][0], oldest, newest))
            date_ranges = tuple(sorted(
                found_dates, key=lambda r: (-totals.get(r[0], 0), r[0]),
            ))
        except _PartFailed as exc:
            date_ranges = None
            missing.append((PART_DATES, exc.reason))

    # ── the path-word check: the lexical search agents call, unfiltered ──
    probe: Optional[PathProbe] = None
    path_subtypes = sorted(
        (
            name for name, meta in metadata.items()
            if _string_list(meta.get("searchable_text_path_fields"))
            and totals.get(name, 0) > 0
        ),
        key=lambda s: (-totals.get(s, 0), s),
    )
    if path_subtypes:
        try:
            probe = _probe_path_words(
                g, read,
                deployment_name=deployment_name, branch_name=branch_name,
                generation_id=generation_id, subtype=path_subtypes[0],
                meta=metadata[path_subtypes[0]],
            )
            if probe is None:
                missing.append((
                    PART_PATH_CHECK,
                    "no sampled row had a path word the rest of its text lacks",
                ))
        except _PartFailed as exc:
            missing.append((PART_PATH_CHECK, exc.reason))

    # ── attribute keys of the largest types: the `query` description ──
    # Read last, so the card's own parts keep the shared deadline first.
    attr_keys: Optional[tuple[tuple[str, int, tuple[tuple[str, int, int], ...]], ...]] = None
    example_keys: Optional[tuple[tuple[str, int, tuple[tuple[str, int, int], ...]], ...]] = None
    if counts is not None and totals:
        ranked = sorted(totals, key=lambda s: (-totals[s], s))
        largest = ranked[:ATTR_SAMPLE_TYPES]
        # The `run` example's candidates are also read, in the same
        # statement: the largest types (not passage children) of at most
        # RUN_EXAMPLE_MAX_ROWS records, which the largest types may not hold.
        children = {child for _parent, child, _edge in nesting}
        bounded = [
            s for s in ranked
            if s not in largest and s not in children and 0 < totals[s] <= RUN_EXAMPLE_MAX_ROWS
        ][:RUN_EXAMPLE_SAMPLE_TYPES]
        sample = largest + bounded
        try:
            sampled: dict[str, tuple[int, list[tuple[str, int, int]]]] = {}
            for row in g.read("attrs", lambda: read(
                _ATTR_SAMPLE_SQL, {"subtypes": sample, "limit": ATTR_SAMPLE_ROWS},
            )):
                subtype = str(row["subtype"])
                _rows, keys = sampled.setdefault(subtype, (int(row["sampled"]), []))
                keys.append((
                    str(row["key"]), int(row["rows_with"]), int(row["distinct_values"]),
                ))
            example_keys = tuple(
                (subtype, sampled[subtype][0], tuple(sorted(sampled[subtype][1])))
                for subtype in sample if subtype in sampled
            )
            attr_keys = tuple(item for item in example_keys if item[0] in largest)
        except _PartFailed as exc:
            missing.append((PART_ATTRS, exc.reason))

    run_example = _run_example(totals, nesting, metadata, example_keys, missing)

    return _finish(
        replace(
            base,
            source_counts=counts,
            counts_source=counts_source,
            counts_source_reason=counts_reason,
            id_patterns=patterns,
            date_ranges=date_ranges,
            path_probe=probe,
            run_example=run_example,
            attr_keys=attr_keys,
        ),
        missing, g.part_ms, started, withheld=False,
    )


def _classify(
    conn: psycopg.Connection, deployment_name: str, sources: list[str],
) -> dict[str, str]:
    """`trusted` only for a producer whose own profile says so.

    Stricter than the read path's `classify_sources` on purpose: nodes with
    no source, and system producers (`_extractor:`, `_enricher:`, `_lens:`)
    with no profile, are trusted there but not here. A literal in the
    server's instructions needs a positive statement of trust.
    """
    from okg.substrate.mcp.provenance import classify_source_evidence

    evidence = classify_source_evidence(
        conn, deployment_name=deployment_name,
        sources=[s for s in sources if s],
    )
    out = {s: "untrusted" for s in sources}
    for source, item in evidence.items():
        if (
            source
            and item.get("classification_basis") == "profile"
            and item.get("effective_classification") == "trusted"
        ):
            out[str(source)] = "trusted"
    return out


#: How many results the path check reads back from its unfiltered search.
_PATH_CHECK_LIMIT = 50


def _probe_path_words(
    g: _Gather,
    read: Callable[..., list[dict[str, Any]]],
    *,
    deployment_name: str,
    branch_name: str,
    generation_id: str,
    subtype: str,
    meta: Mapping[str, Any],
) -> Optional[PathProbe]:
    """Search one real row's path word with the lexical search agents call.

    The search is the same call an agent makes with a bare query: no
    subtype, attribute or path filter, so it runs the unfiltered lexical
    plan. It is a hit when the sampled row is among the first
    `_PATH_CHECK_LIMIT` results. The word is the longest one of the path
    that the row's other searchable text lacks, so a hit is the path's.
    A miss is inconclusive (the row may rank lower), and the card then
    states nothing about paths.
    """
    from okg.substrate.mcp.search import search as lexical_search
    from okg.substrate.mcp.session import open_session

    conn = g.conn
    path_fields = _string_list(meta.get("searchable_text_path_fields"))
    path_field = path_fields[0]
    other_fields = [
        f for f in _string_list(meta.get("searchable_text_fields"))
        if f not in path_fields
    ]
    rows = g.read(
        "path_sample",
        lambda: read(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE subtype = %(subtype)s "
            "AND jsonb_typeof(attrs -> %(field)s) = 'string' LIMIT 20",
            {"subtype": subtype, "field": path_field},
        ),
    )
    for row in rows:
        attrs = row.get("attrs") or {}
        path = attrs.get(path_field)
        if not isinstance(path, str):
            continue
        word = _path_word(
            path, " ".join(str(attrs.get(f) or "") for f in other_fields),
        )
        if word is None:
            continue
        node_id = str(row["node_id"])

        def _search(word: str = word) -> dict[str, Any]:
            session = open_session(
                conn, deployment_name=deployment_name,
                requested_generation=generation_id, branch_name=branch_name,
            )
            return lexical_search(
                conn, session, word, method="lexical",
                limit=_PATH_CHECK_LIMIT, select=["node_id"],
            )

        result = g.read("path_check", _search)
        hit = any(
            str(r.get("node_id")) == node_id for r in result.get("results") or ()
        )
        return PathProbe(
            subtype=subtype, node_id=node_id, field=path_field, word=word, hit=hit,
        )
    return None


def _finish(
    facts: DatasetFacts,
    missing: list[tuple[str, str]],
    part_ms: Iterable[tuple[str, float]],
    started: float,
    *,
    withheld: bool,
) -> DatasetFacts:
    for part, reason in missing:
        log.warning("dataset card: %s unavailable at start: %s", part, reason)
    return replace(
        facts,
        missing=tuple(missing),
        part_ms=tuple(part_ms),
        withheld=withheld,
        gather_ms=round((time.perf_counter() - started) * 1000.0, 3),
    )


def facts_without_database(
    *, deployment_name: str, not_ingested: Sequence[str] = (), reason: str,
) -> DatasetFacts:
    """The facts of a server built with no database (probe or test mode)."""
    return DatasetFacts(
        deployment=deployment_name, mode="no_database",
        not_ingested=tuple(str(item) for item in not_ingested),
        missing=((PART_PUBLISHED, reason),),
    )


def card_unavailable(
    *, deployment_name: str, not_ingested: Sequence[str] = (), error: BaseException,
) -> DatasetFacts:
    """The facts when gathering itself failed (never a server-start failure)."""
    return DatasetFacts(
        deployment=deployment_name, mode="unavailable",
        not_ingested=tuple(str(item) for item in not_ingested),
        missing=((PART_PUBLISHED, f"the card could not be compiled ({type(error).__name__})"),),
    )


# ── rendering ─────────────────────────────────────────────────────────


#: Section byte budgets (lane I lead, tuned on the comp-ops copy, whose
#: 75 producers otherwise filled the card): the source list stops near
#: `_SOURCES_MAX_BYTES`, and while anything else can be cut, the id
#: patterns, nesting and dates keep at least their floor.
_SOURCES_MAX_BYTES = 700
_IDS_MIN_BYTES = 600
_NESTING_MIN_BYTES = 200
_DATES_MIN_BYTES = 200


@dataclass
class _Section:
    """A heading line and item lines. The renderer drops items from the end
    when the card is too long, and the section then says how many, and where
    to read them when there is such a place. `records` (sources only) holds
    each item's row count, so the cut line can say how many rows it hides."""

    heading: Optional[str]
    items: list[str]
    more_hint: Optional[str]
    dropped: int = 0
    max_bytes: Optional[int] = None
    min_bytes: int = 0
    records: Optional[list[int]] = None
    dropped_records: int = 0

    def cut_one(self) -> None:
        self.items.pop()
        self.dropped += 1
        if self.records:
            self.dropped_records += self.records.pop()

    def lines(self) -> list[str]:
        out = [self.heading] if self.heading else []
        out.extend(self.items)
        if self.dropped:
            where = f": {self.more_hint}" if self.more_hint else ""
            if self.records is not None:
                out.append(
                    f"- +{self.dropped} more sources "
                    f"({self.dropped_records:,} records){where}"
                )
            else:
                out.append(f"- ... ({self.dropped} more{where})")
        return out

    def size(self) -> int:
        return len("\n".join(self.lines()).encode("utf-8"))


def _grouped_sources(
    counts: Sequence[SourceCount],
) -> list[tuple[str, int, tuple[tuple[str, int], ...]]]:
    """(label, total, subtypes) per line, largest first.

    Producers named `<name>.<kind>` that share a kind with another producer
    become one line: `<kind> (<n> sources: <names>, +k)`, with summed counts.
    The names are source labels, which already passed the literal rule.
    """
    by_kind: dict[str, list[SourceCount]] = {}
    for sc in counts:
        name, dot, kind = sc.source.partition(".")
        if dot and name and kind:
            by_kind.setdefault(kind, []).append(sc)
    lines: list[tuple[str, int, tuple[tuple[str, int], ...]]] = []
    grouped: set[str] = set()
    for kind, members in by_kind.items():
        if len(members) < 2:
            continue
        members = sorted(members, key=lambda sc: (-sc.total, sc.source))
        names = [sc.source.partition(".")[0] for sc in members]
        shown = ", ".join(names[:3]) + (f", +{len(names) - 3}" if len(names) > 3 else "")
        per: dict[str, int] = {}
        for sc in members:
            grouped.add(sc.source)
            for subtype, n in sc.subtypes:
                per[subtype] = per.get(subtype, 0) + n
        lines.append((
            f"{kind} ({len(members)} sources: {shown})",
            sum(sc.total for sc in members),
            tuple(sorted(per.items(), key=lambda kv: (-kv[1], kv[0]))),
        ))
    for sc in counts:
        if sc.source not in grouped:
            lines.append((sc.source, sc.total, sc.subtypes))
    return sorted(lines, key=lambda line: (-line[1], line[0]))


def run_example_steps(facts: DatasetFacts) -> Optional[list[dict[str, Any]]]:
    """The steps of the `run` example, or None when the card has no type
    and key for it (no live catalog, rows withheld under access rules,
    under which `run` is refused, or no key read)."""
    if facts.mode != "live" or facts.withheld or facts.run_example is None:
        return None
    subtype, key = facts.run_example
    field = f"attrs.{key}"
    # The limit bounds the groups printed: the sample cannot tell how many
    # values a key holds over the whole type, and a printed step past 200
    # groups is refused (on one graph the chosen key held 3,486).
    return [
        {"filter": {"where": {"type": subtype, field: {"op": "exists"}}}, "show": False},
        {"aggregate": {
            "where": "$1", "by": [field], "metrics": ["count"],
            "limit": RUN_EXAMPLE_GROUPS,
        }},
    ]


def render_dataset_card(
    facts: DatasetFacts,
    *,
    served_tools: Iterable[str],
    text_kind_served: bool = False,
    run_live: bool = True,
    budget: int = CARD_BUDGET_BYTES,
) -> str:
    """The card text, at most `budget` bytes, with no blank line in it.

    Names a tool only when this surface serves it: the card is part of the
    served instructions, which must not tell a model to call a pruned tool.

    Which `select` fields each search method accepts is NOT repeated here:
    the served `search` schema's `select` description already states it,
    generated from the same capability record
    (`ServerCapabilities.select_fields_description`), and a second copy costs
    about 1,000 B on every session.
    """
    served = set(served_tools)
    inspect_ok = "inspect" in served
    graph_hint = 'inspect(target={"kind":"graph"})' if inspect_ok else None
    missing = dict(facts.missing)

    if facts.mode == "live":
        # Generation text is off by default (read-passage-rows-3666 D8, Jason
        # 2026-10-15: "optional and off by default"): the card names neither
        # the generation id nor session pinning; inspect of the graph reports
        # both. The publish date is not when the sources were read, so the
        # header points at the per-type newest dates (prompt redesign: a
        # harness "snapshot date" was wrong for one source).
        when = (
            f"graph published {facts.published_date}" if facts.published_date
            else f"publish date unavailable: {missing.get(PART_PUBLISHED, 'not read')}"
        )
        # "record" is a tool name to the served-tools word scan: not used here.
        dated = "; each type's newest date is under Dates" if facts.date_ranges else ""
        head = [f"{CARD_HEADER} ({when}{dated}):"]
    else:
        head = [f"{CARD_HEADER}:"]
    if facts.mode == "no_database":
        head.append(
            "- Dataset facts unavailable (sources, row counts, ids, dates, "
            f"nesting, search checks): {missing.get(PART_PUBLISHED, 'no database')}."
        )
    elif facts.mode in {"no_generation", "unavailable"}:
        head.append(
            "- Dataset facts unavailable: "
            f"{missing.get(PART_PUBLISHED, 'no generation was pinned at server start')}."
        )
    elif facts.withheld:
        head.append(
            "- Sources, row counts, ids, dates and the path check are withheld: "
            "Nomos filters rows per reader, which one start-up summary cannot."
        )
    if facts.mode == "live" and not facts.catalog_read:
        head.append(
            "- Catalog facts (nesting, live relations) unavailable at server "
            f"start: {missing.get(PART_CATALOG, 'not read')}."
        )

    sections: list[_Section] = []

    # 1. sources and counts
    if facts.source_counts is not None:
        items = []
        records = []
        for label, total, subtypes in _grouped_sources(facts.source_counts):
            shown = subtypes[:_MAX_SUBTYPES_PER_SOURCE]
            by_type = ", ".join(f"{s} {n:,}" for s, n in shown)
            if len(subtypes) > len(shown):
                by_type += f", +{len(subtypes) - len(shown)} types"
            items.append(f"- {label}: {total:,} ({by_type})")
            records.append(total)
        sections.append(_Section(
            "Sources (rows by type):", items, graph_hint,
            max_bytes=_SOURCES_MAX_BYTES, records=records,
        ))
    elif PART_COUNTS in missing:
        sections.append(_Section(
            None,
            [f"- Row counts unavailable at server start: {missing[PART_COUNTS]}."],
            graph_hint,
        ))

    # 1b. the `run` example, built from this catalog (issue #3666): only
    # where run and both tools its steps call are served.
    example = run_example_steps(facts) if {"run", "filter", "aggregate"} <= served else None
    if example is not None:
        subtype, key = facts.run_example  # type: ignore[misc]
        sections.append(_Section(
            "Run example (types and fields of this graph):",
            [
                f"- {subtype} records per attrs.{key}: run(steps="
                + json.dumps(example, separators=(",", ":"))
                + '); "$1" is step 1\'s records, and only the last step prints rows.'
            ],
            None,
        ))

    # 2. declared absences
    if facts.not_ingested:
        sections.append(_Section(
            "Not ingested (declared by the deployment; the graph has none of it):",
            [f"- {item}" for item in facts.not_ingested],
            None,
        ))

    # 2b. live relations: short, and above the sections cut first for size
    live = _live_section(
        facts, inspect_ok, graph_hint, run_ok={"run", "expand"} <= served,
    )
    if live is not None:
        sections.append(live)

    # 3. id patterns
    if facts.id_patterns:
        sections.append(_Section(
            # A repo or path part is known; a sha or numeric id part is not
            # (the server text says file, commit and doc ids are built from
            # them, so "never a guessed one" contradicted it).
            "Id patterns (from sampled ids; take a <sha> or <id> part from a "
            "result, never guess it):",
            [f"- {subtype}: `{pattern}`" for subtype, pattern in facts.id_patterns],
            graph_hint, min_bytes=_IDS_MIN_BYTES,
        ))
    elif PART_IDS in missing and PART_COUNTS not in missing:
        sections.append(_Section(
            None,
            [f"- Id patterns unavailable at server start: {missing[PART_IDS]}."],
            graph_hint,
        ))

    # 4. nesting
    present = (
        {s for sc in facts.source_counts for s, _n in sc.subtypes}
        if facts.source_counts is not None else None
    )
    nest_items = []
    for parent, child, edge in facts.nesting:
        if present is not None and parent not in present and child not in present:
            continue
        line = f"- a {child} is part of a {parent} (edge `{edge}`)"
        if inspect_ok and text_kind_served:
            line += (
                f"; read the whole {parent} with "
                'inspect(target={"kind":"text","node_id":<its id>})'
            )
        nest_items.append(line + ".")
    if nest_items:
        sections.append(_Section(
            "Nesting:", nest_items, graph_hint, min_bytes=_NESTING_MIN_BYTES,
        ))

    # 5. dates
    if facts.date_ranges:
        sections.append(_Section(
            "Dates per type (oldest to newest):",
            [
                f"- {subtype} ({meaning}): {oldest} to {newest}"
                for subtype, meaning, oldest, newest in facts.date_ranges
            ],
            graph_hint, min_bytes=_DATES_MIN_BYTES,
        ))
    elif facts.date_ranges is None and PART_DATES in missing:
        sections.append(_Section(
            None,
            [f"- Dates unavailable at server start: {missing[PART_DATES]}."],
            graph_hint,
        ))

    # 6. search behavior: only what a start-up check saw
    probe = facts.path_probe
    if "search" in served and probe is not None and probe.hit:
        sections.append(_Section(
            "Search behavior (checked on this server):",
            [
                f"- lexical search matches words inside {probe.subtype} paths "
                "(checked at start with a word from one row's path)."
            ],
            None,
        ))

    card = _fit(head, sections, budget, graph_hint)
    if not run_live:
        # The operator turned run's live steps off: the card is the one the
        # switch-on server serves, minus the live-step sentence. Cutting for
        # size with the sentence in place keeps every other line where it
        # was (leaving it out first lets lower lines in, a second difference).
        card = "\n".join(
            line for line in card.split("\n") if line != RUN_LIVE_SENTENCE
        )
    return card


#: Relation names listed per type before the line says how many more there
#: are; the full list (with arguments) is one inspect of a record of the type.
_LIVE_NAMES_SHOWN = 8


def _live_section(
    facts: "DatasetFacts", inspect_ok: bool, graph_hint: Optional[str], *,
    run_ok: bool = False,
) -> Optional[_Section]:
    """The live relations, one short line per type.

    Placed above the sections cut first: an agent that is never told the
    relations exist must find them by inspecting the right record (#3666,
    live arm: a 65-name list was the lowest section and was cut for size)."""
    if facts.mode != "live" or not facts.catalog_read:
        return None
    if not facts.live_relations:
        return _Section(
            "Live relations:",
            ["- none declared; every row is as published."],
            None,
        )
    items = []
    for subtype, names in facts.live_relations:
        shown = list(names[:_LIVE_NAMES_SHOWN])
        line = f"- {subtype}: {len(names)}: " + ", ".join(shown)
        if len(names) > len(shown):
            line += f", +{len(names) - len(shown)} more"
        if inspect_ok:
            line += (
                '; inspect(target={"kind":"node","node_id":<a ' + subtype
                + ' id>}) lists all with their arguments'
            )
        items.append(line + ".")
    if run_ok:
        # #3666, lane ri: agents asked one relation once per value, call
        # after call; one run step makes those calls.
        items.append(RUN_LIVE_SENTENCE)
    return _Section(
        "Live relations (they read the source now; every other row is as "
        "published):",
        items,
        graph_hint,
    )


#: The card's one sentence on `run` live steps, served with `run` and
#: `expand` where the catalog declares live relations (#3666, lane ri).
RUN_LIVE_SENTENCE = (
    '- In run, an expand live step with "each": {"<arg>": "$1.<column>"} '
    "calls the relation once per value of step 1's column."
)


def _fit(
    head: list[str], sections: list[_Section], budget: int, graph_hint: Optional[str],
) -> str:
    """Join the card, cutting from the lowest section until it fits."""

    def text_of(cut_sections: int) -> str:
        lines = list(head)
        for section in sections:
            lines.extend(section.lines())
        if cut_sections:
            where = f": {graph_hint}" if graph_hint else ""
            lines.append(f"- ... ({cut_sections} lower section(s) cut for size{where})")
        return "\n".join(line for line in lines if line)

    # A section with its own ceiling stops there first (keeping one line).
    for section in sections:
        if section.max_bytes is not None:
            while len(section.items) > 1 and section.size() > section.max_bytes:
                section.cut_one()

    cut = 0
    text = text_of(cut)
    while len(text.encode("utf-8")) > budget:
        trimmable = [s for s in sections if len(s.items) > 1]
        # Cut from the lowest section above its floor; floors give way only
        # when nothing else is left to cut.
        above_floor = [s for s in trimmable if s.size() > s.min_bytes]
        if above_floor or trimmable:
            # Keep one line of every section while any section has more.
            (above_floor or trimmable)[-1].cut_one()
        elif sections:
            sections.pop()
            cut += 1
        else:
            # Only the head is left and it is still too long: cut it at a
            # line, never leaving a blank line, so the card stays one block.
            kept: list[str] = []
            for line in text.split("\n"):
                candidate = "\n".join([*kept, line, "- ..."])
                if len(candidate.encode("utf-8")) > budget:
                    break
                kept.append(line)
            return "\n".join([*kept, "- ..."]) if kept else CARD_HEADER + ":"
        text = text_of(cut)
    return text


def split_dataset_card(served: str) -> tuple[str, str]:
    """Split served instructions into (card, the rest).

    The card is the first block: it starts with `CARD_HEADER` and holds no
    blank line, and the server separates it from okg's text with one. Text
    that does not start with the card is returned whole as the rest.
    """
    if not served.startswith(CARD_HEADER):
        return "", served
    card, sep, rest = served.partition("\n\n")
    return card, rest if sep else ""


def _sha256(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def card_inputs(facts: DatasetFacts, *, card: str) -> dict[str, Any]:
    """The `dataset_card.inputs` block of `inspect(kind="session_start")`."""
    probe = facts.path_probe
    by_subtype: Optional[dict[str, int]] = None
    if facts.source_counts is not None:
        by_subtype = {}
        for sc in facts.source_counts:
            for subtype, n in sc.subtypes:
                by_subtype[subtype] = by_subtype.get(subtype, 0) + n
    return {
        "mode": facts.mode,
        "generation_id": facts.generation_id,
        "catalog_version_id": facts.catalog_version_id,
        "published_date": facts.published_date,
        "nomos_mode": facts.nomos_mode,
        "withheld_rows": facts.withheld,
        "counts_source": facts.counts_source,
        "counts_source_reason": facts.counts_source_reason,
        "sources": (
            len(facts.source_counts) if facts.source_counts is not None else None
        ),
        # The numbers the card's source lines add up to; equal to
        # inspect(kind="graph")'s `subtype_counts` at this generation.
        "subtype_counts": by_subtype,
        "total_rows": sum(by_subtype.values()) if by_subtype is not None else None,
        "id_patterns": (
            len(facts.id_patterns) if facts.id_patterns is not None else None
        ),
        "dated_subtypes": (
            len(facts.date_ranges) if facts.date_ranges is not None else None
        ),
        "nesting_declared": len(facts.nesting),
        "live_relation_subtypes": len(facts.live_relations),
        "not_ingested": list(facts.not_ingested),
        "path_check": (
            {
                "subtype": probe.subtype,
                "node_id": probe.node_id,
                "field": probe.field,
                "word": probe.word,
                "hit": probe.hit,
            }
            if probe is not None else None
        ),
        "missing": [{"part": p, "reason": r} for p, r in facts.missing],
        "part_ms": {name: ms for name, ms in facts.part_ms},
        "gather_ms": facts.gather_ms,
        "deadline_ms": GATHER_DEADLINE_MS,
        # The real bound: every statement is capped at the time left, so the
        # reads end within deadline_ms plus client-side work between them.
        "deadline_rule": (
            "each statement's statement_timeout is the time left of its read's "
            f"{CARD_STATEMENT_TIMEOUT_MS} ms and of the shared deadline_ms; a "
            "statement with no time left is not sent"
        ),
        "bytes": len(card.encode("utf-8")),
        "budget_bytes": CARD_BUDGET_BYTES,
        "sha256": _sha256(card),
    }

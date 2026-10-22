"""Generation-pinned MCP tool implementations (M4.1).

Every tool takes a `Session` + a connection (or opens one) and
reads nodes_live / edges_live / catalog rows at the session's
pinned generation. The bitemporal predicate

    WHERE valid_from_gen <= :pin
      AND (valid_to_gen IS NULL OR valid_to_gen > :pin)

is applied uniformly — captured once in `_pin_predicate()` so
that a schema change to the valid_from/valid_to columns only
needs touching in one place.

Tools shipped at M4.1:

  - graph_stats         counts of nodes / edges at the pin
  - get_ontology        subtypes + narrowings + aliases at the pin
  - get_node            single-node attrs + text_hash
  - list_neighbors      outbound edges from a node
  - aggregate_by_type   count of nodes by subtype
  - aggregate_neighbors count of neighbors by (edge_type, dst_subtype)
  - traverse_path       typed edge-path walk, max 3 hops

The M4.2 search consolidation (`search(method=...)`) lives in
`okg.substrate.mcp.search`.
"""
from __future__ import annotations

import json
import logging
import re
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Callable, Iterator, Mapping, Optional
from urllib.parse import unquote

import psycopg

from okg.substrate.attributes import (
    apply_effective_attrs_to_rows,
    attribute_history_for_target,
    effective_attrs_for_target,
)
from okg.substrate.errors import NodeNotFound, SubstrateError
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.envelope import (
    project_row,
    validate_select,
)
from okg.substrate.mcp.continuation import (
    consume_continuation,
    continuation_binding,
    issue_continuation,
)
from okg.substrate.mcp.path_diagnostics import (
    PATH_WORK_CAP,
    InvalidPathDirection,
    InvalidPathGrammar,
    empty_path_outcome,
    parse_path_hops as _parse_actionable_path_hops,
    scan_visible_path,
    start_node_visibility,
    validate_path_edge_types,
    validate_path_limit,
)
from okg.substrate.mcp.session import Session, assert_session_matches
from okg.substrate.mcp.subtype_summary import (
    DEFAULT_SECTION_PAGE_SIZE,
    InvalidSubtypeInspection,
    catalog_subtype_shape,
    compact_subtype_summary,
    merge_declared_and_observed_attributes,
)
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
    read_attr_classifier_for_row,
)
from okg.substrate.nomos.read_masking import (
    READ_DROP_CLASSES,
    REDACTION_MARKER,
    ReadRedactedAttrs,
    mask_attrs,
)
from okg.substrate.nomos.runtime_enforcement import (
    deployment_nomos_enforcement_mode,
    deployment_requires_nomos_runtime,
)
from okg.substrate.transform.operators import (
    execute_list_neighbors_chronos,
    lower_program_over_chronos,
    node_by_id_program,
    subtype_edges_in_program,
    subtype_edges_out_program,
)

logger = logging.getLogger(__name__)


__all__ = [
    "INSPECT_GRAPH_BLOCKS",
    "NodeRow",
    "aggregate_edge",
    "aggregate_node",
    "expand_neighbors",
    "expand_path",
    "get_ontology",
    "inspect_graph",
    "inspect_attr_provenance",
    "inspect_node",
    "inspect_subtype",
    "parse_path_hops",
]


MAX_TRAVERSE_HOPS = 3


@dataclass(frozen=True)
class NodeRow:
    node_id: str
    subtype: str
    attrs: dict[str, Any]
    text_hash: Optional[str]


def _chronos_checkout(conn: psycopg.Connection, session: Session):
    assert_session_matches(conn, session)
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.checkout_generation(
        session.generation_id,
        branch=session.branch_name,
    )


#: The fix hint every "node not visible" refusal carries when no close id
#: is named: `inspect(node)`, `inspect(attr)` and the text reads share it.
NODE_NOT_VISIBLE_FIX_HINT = (
    "check the node_id or session pin; to resolve "
    "a node by attrs instead of guessing ids, use "
    'search(method="attrs", where={"op":"eq",'
    '"field":"attrs.path","value":...})'
)

#: Close ids named in a not-found refusal, and the rows read to find them.
CLOSE_IDS_SHOWN = 5
_CLOSE_IDS_READ = 25
#: The close-id read's own statement timeout: the refusal path only.
_CLOSE_IDS_TIMEOUT_MS = 1_000
#: Most matching ids one round-two candidate read collects, unordered,
#: before the ordered bounded read takes its first `_CLOSE_IDS_READ + 1`.
_CLOSE_IDS_PROBE = 256


def _clean_guessed_id(node_id: str) -> str:
    """A hand-built id as a writer most likely meant it (#3666): `%XX`
    escapes decoded (`Read%20me.md`), no space around a `:`
    (`kind: name`), no surrounding space. Only a lookup key: the cleaned
    id is never served in place of the one asked for."""
    cleaned = unquote(node_id) if "%" in node_id else node_id
    return re.sub(r"\s*:\s*", ":", cleaned.strip())


def guessed_id_forms(node_id: str) -> list[str]:
    """Other spellings of a hand-built id that may name a stored record,
    in the order tried, never `node_id` itself: the cleaned id
    (`_clean_guessed_id`), and, for an id with one `:` whose rest holds a
    `/`, that id with the first `/` read as the `:` after the source
    (`doc:<source>/<path>` -> `doc:<source>:<path>`)."""
    cleaned = _clean_guessed_id(node_id)
    forms = [cleaned]
    prefix, sep, rest = cleaned.partition(":")
    if sep and prefix and ":" not in rest and "/" in rest.strip("/"):
        source, _, path = rest.strip("/").partition("/")
        forms.append(f"{prefix}:{source}:{path}")
    seen = {node_id}
    out = []
    for form in forms:
        if form and form not in seen:
            seen.add(form)
            out.append(form)
    return out


def _id_path(node_id: str) -> Optional[str]:
    """The file path inside a hand-built id such as `doc:<repo>:<path>`,
    `symbol:<repo>:<path>:<qname>:<line>` or `doc:<repo>/<path>`: the
    first part after the prefix that holds a "/", else one that holds a
    "." (a top-level file), read from the cleaned id (`%XX` decoded, no
    space around `:`). None when the id names no path. A source with a "."
    (`doc:<site>.<kind>:...`) is never read as a file when parts follow it
    (#3666 round 2: `doc:<site>.<kind>:` named its source as the path)."""
    parts = _clean_guessed_id(node_id).split(":")[1:]
    for part in parts:
        if "/" in part:
            return part.strip().strip("/") or None
    for part in parts[1:] if len(parts) > 1 else parts:
        if "." in part:
            return part.strip().strip("/") or None
    return None


def _ends_with_path(stored: str, tail: str) -> bool:
    """`stored` is `tail` or ends in `/<tail>`, ignoring case."""
    stored = stored.strip().strip("/").casefold()
    tail = tail.strip().strip("/").casefold()
    return bool(tail) and (stored == tail or stored.endswith("/" + tail))


def _path_ends_with(row: Mapping[str, Any], tail: str) -> bool:
    attrs = row.get("attrs")
    stored = str(attrs.get("path") or "") if isinstance(attrs, Mapping) else ""
    return _ends_with_path(stored, tail)


def path_closeness(guessed: str, stored: str) -> tuple[int, int, float]:
    """How close a stored path is to a guessed one, larger first: the
    trailing path parts they share (ignoring case), whether those parts
    match in case too, then how alike the whole paths read."""
    want = [part for part in guessed.strip("/").split("/") if part]
    have = [part for part in stored.strip("/").split("/") if part]
    shared = 0
    for mine, theirs in zip(reversed(want), reversed(have)):
        if mine.casefold() != theirs.casefold():
            break
        shared += 1
    exact_case = int(shared > 0 and want[-shared:] == have[-shared:])
    alike = SequenceMatcher(None, guessed.casefold(), stored.casefold()).ratio()
    return shared, exact_case, round(alike, 6)


def rank_near_paths(
    guessed: str, found: Mapping[str, str],
) -> list[str]:
    """The ids in `found` (id -> stored path), closest path first; ties go
    by id so the order is stable."""
    return sorted(
        found,
        key=lambda node_id: (
            tuple(-value for value in path_closeness(guessed, found[node_id])),
            node_id,
        ),
    )


def _tail_and_name(path: str) -> tuple[str, str]:
    """A path's last two parts and its last part (the file name)."""
    parts = [part for part in path.strip("/").split("/") if part]
    return "/".join(parts[-2:]), (parts[-1] if parts else "")


@contextmanager
def _read_budget(
    conn: psycopg.Connection, session: Session, budget_ms: int,
) -> Iterator[Callable[..., list[dict[str, Any]]]]:
    """A `_visible_node_rows` reader whose reads share ONE statement-timeout
    budget: before each read the timeout is set to what is left of
    `budget_ms`, and a read with nothing left raises `TimeoutError`. The
    caller's own timeout is put back afterwards.

    On an autocommit connection each checkout query commits itself, so no
    transaction block wraps it; inside a caller's transaction a savepoint
    keeps a failed read from aborting it (a rolled-back savepoint reverts
    the timeout as well)."""
    from psycopg.rows import tuple_row

    block = nullcontext() if conn.autocommit else conn.transaction()
    with block:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute("SELECT current_setting('statement_timeout')")
            before = cur.fetchone()[0]
        deadline = time.monotonic() + budget_ms / 1000

        def read(**kwargs: Any) -> list[dict[str, Any]]:
            left = int((deadline - time.monotonic()) * 1000)
            if left < 1:
                raise TimeoutError("the close-id read budget is spent")
            conn.execute(
                "SELECT set_config('statement_timeout', %s, false)", (str(left),),
            )
            return _visible_node_rows(conn, session, **kwargs)

        try:
            yield read
        finally:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.INERROR:
                conn.execute(
                    "SELECT set_config('statement_timeout', %s, false)", (before,),
                )


#: A URL's scheme inside a hand-built id (`doc:<site>:https://host/page.md`).
_URL_SCHEME = re.compile(r"[a-z][a-z0-9+.-]*://", re.IGNORECASE)
#: Page suffixes a URL is compared without.
_PAGE_SUFFIXES = (".md", ".html", ".htm")
#: The attributes that hold a record's page URL: `url` (a crawled site
#: page), `citation_url` (a source-supplied citation link).
_URL_ATTRS = ("url", "citation_url")
#: A last id part that only stands in for one: punctuation (`?`, `...`)
#: or a `<placeholder>`.
_PLACEHOLDER_PART = re.compile(r"[\W_]*|<[^<>]*>")
#: A hash start shorter than this is not looked up (it would match too many).
_HASH_PREFIX_MIN = 6
#: Prefixes whose documented id pattern is `<prefix>:<source>:<rest>`
#: (the server instructions' deterministic ids), so `doc:<source>` alone
#: lacks its last part.
_SOURCE_SCOPED_PREFIXES = frozenset({"doc", "file", "commit", "symbol"})
#: Each source-scoped prefix's documented id pattern (server instructions;
#: `_code_graph_common.symbol_id` for symbols), as a fix hint names it.
_SOURCE_SCOPED_PATTERNS = {
    "doc": "doc:<source>:<path>",
    "file": "file:<source>:<path>",
    "commit": "commit:<repo>:<sha>",
    "symbol": "symbol:<repo>:<path>:<qualified name>",
}


def _commit_sha_without_repo(parts: list[str]) -> bool:
    """`commit:<sha>`: a commit id with its repo left out. The sha is the
    LAST part of `commit:<repo>:<sha>`, so the missing part is the repo
    before it, and a sha start long enough is still looked up under any
    repo (`close_node_ids`)."""
    return len(parts) == 2 and parts[0] == "commit" and _is_hash_like(parts[1])


def _is_hash_like(part: str) -> bool:
    """`part` reads as a hex hash or its start: hex only, with a digit and
    a letter, so a plain number or word is not taken for one."""
    lowered = part.lower()
    return (
        len(lowered) >= 4
        and all(char in "0123456789abcdef" for char in lowered)
        and any(char.isdigit() for char in lowered)
        and any(char.isalpha() for char in lowered)
    )


def id_part_missing_hint(node_id: str) -> Optional[str]:
    """The fix for an id that lacks the part a lookup needs (#3666 round 2):
    an empty or placeholder last part (`doc:<source>:`, `doc:<source>:?`,
    also when the source holds a "." as in `doc:<site>.<kind>:`), a
    source-scoped prefix with one part after it (`doc:<source>`), or a hash
    too short to look up (`chunk:10fe`). None for any other id, including
    one with a path or a URL, which the near-id reads handle."""
    cleaned = _clean_guessed_id(node_id)
    parts = cleaned.split(":")
    if len(parts) < 2 or not parts[0] or _URL_SCHEME.search(cleaned):
        return None
    take = "; take ids from a search result, where an id is exact"
    if _commit_sha_without_repo(parts):
        return (
            f"the id needs the repo before the sha ({parts[0]}: ids read "
            f"'{_SOURCE_SCOPED_PATTERNS['commit']}')" + take
        )
    if (
        len(parts) == 2 and parts[0] in _SOURCE_SCOPED_PREFIXES
        and parts[1] and "/" not in parts[1]
    ):
        return (
            f"the id needs the part after {cleaned + ':'!r} ({parts[0]}: ids "
            f"read '{_SOURCE_SCOPED_PATTERNS[parts[0]]}')" + take
        )
    if _id_path(node_id) is not None:
        return None
    head, last = ":".join(parts[:-1]) + ":", parts[-1]
    if not last or _PLACEHOLDER_PART.fullmatch(last):
        return f"the id needs the part after {head!r}" + take
    if _is_hash_like(last) and len(last) < _HASH_PREFIX_MIN:
        return (
            f"{last!r} is too short a hash to look up (a start of "
            f"{_HASH_PREFIX_MIN} or more hex characters is looked up)" + take
        )
    return None


def _url_stem(text: str) -> str:
    """A URL as compared: no scheme, lower case, no trailing `/`, and no
    page suffix (`.md`, `.html`, `.htm`)."""
    stem = text.strip()
    found = _URL_SCHEME.match(stem)
    if found:
        stem = stem[found.end():]
    stem = stem.split("#", 1)[0].split("?", 1)[0].lower().rstrip("/")
    for suffix in _PAGE_SUFFIXES:
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return stem.rstrip("/")


def _id_url(node_id: str) -> Optional[str]:
    """The URL inside a URL-shaped id (`<prefix>:<site>:https://<host>/<page>`),
    from its scheme on; None when the id holds no `scheme://`."""
    cleaned = _clean_guessed_id(node_id)
    found = _URL_SCHEME.search(cleaned)
    if not found or not cleaned[found.end():].strip("/"):
        return None
    return cleaned[found.start():]


def _url_spellings(url: str) -> list[str]:
    """The stored values a page URL may have, for an exact (indexed) match:
    https or http, with no suffix, a trailing `/`, `.md` or `.html`, in the
    case given and in lower case. At most 16."""
    address = url[_URL_SCHEME.match(url).end():].split("#", 1)[0].split("?", 1)[0]
    base = address.rstrip("/")
    for suffix in _PAGE_SUFFIXES:
        if base.lower().endswith(suffix):
            base = base[: -len(suffix)]
            break
    spellings: list[str] = []
    for page in dict.fromkeys([base, base.lower()]):
        for scheme in ("https://", "http://"):
            for suffix in ("", "/", ".md", ".html"):
                spellings.append(scheme + page + suffix)
    return spellings


def _url_containment_docs(url: str) -> list[str]:
    """`attrs @> <doc>` documents for each URL attribute and spelling: each
    is answered by the node attrs GIN index (`jsonb_path_ops`)."""
    return [
        json.dumps({attr: spelling}, separators=(",", ":"))
        for spelling in _url_spellings(url) for attr in _URL_ATTRS
    ]


def _url_read(url: str) -> tuple[str, dict[str, Any]]:
    """The round-two URL read: its `candidates` condition (over the row
    alias `hit`, for `_visible_node_rows`) and its parameters.

    One `attrs @> <doc>` per spelling, OR-ed, in an unordered subquery: each
    term is a GIN index condition, so the matches come from a bitmap of the
    index (`@> ANY(<array>)` is no GIN index condition, and under the outer
    `ORDER BY node_id LIMIT` the planner walked the id index instead; review
    P2-4). The outer read orders the few matching ids and keeps its bound;
    the reader's filter runs on its rows before any URL is compared."""
    docs = _url_containment_docs(url)
    candidates = " OR ".join(
        f"hit.attrs @> CAST(:url_doc_{index} AS JSONB)" for index in range(len(docs))
    )
    return candidates, {f"url_doc_{index}": doc for index, doc in enumerate(docs)}


def _name_read(name: str) -> tuple[str, dict[str, Any]]:
    """The round-two name read, as `_url_read` gives its own: ids of any
    prefix whose last `:` part is `name`, ignoring case. A scan of the ids
    (no index serves a suffix),
    over the id and its last characters only, in an unordered subquery that
    stops at `_CLOSE_IDS_PROBE` matches. It runs in what is left of the
    shared budget; cut there, the check says "incomplete"."""
    tail = ":" + name
    candidates = "lower(right(hit.node_id, :name_len)) = lower(:name_tail)"
    return candidates, {"name_len": len(tail), "name_tail": tail}


def _stored_url_matching(row: Mapping[str, Any], url: str) -> Optional[str]:
    """The visible row's page URL (`_URL_ATTRS`, in order) that names the
    same page as `url` (`_url_stem` equal), as a stem; None when none does.
    Read after the reader's filter, so a masked URL never matches."""
    attrs = row.get("attrs")
    if not isinstance(attrs, Mapping):
        return None
    wanted = _url_stem(url)
    for attr in _URL_ATTRS:
        value = attrs.get(attr)
        if isinstance(value, str) and _url_stem(value) == wanted:
            return wanted
    return None


def _hex_upper_bound(start: str) -> Optional[str]:
    """The least hex string past every string that starts with `start`
    (lower-case hex), for an index range; None when there is none."""
    digits = "0123456789abcdef"
    chars = list(start)
    while chars:
        position = digits.find(chars[-1])
        if 0 <= position < len(digits) - 1:
            chars[-1] = digits[position + 1]
            return "".join(chars)
        chars.pop()
    return None


def close_node_ids(
    conn: psycopg.Connection, session: Session, node_id: str,
) -> tuple[list[str], str]:
    """Visible ids that may be the record a missing `node_id` meant
    (#3605: 197 of 225 lane X not-found ids were built by hand from the id
    patterns, with a wrong repo or path; #3666: case, `%20`, a `/` for the
    `:` after the source, a space after the `:`, another directory, a page
    URL, a name under another prefix, a short hash).

    First reads, under ONE statement-timeout budget
    (`_CLOSE_IDS_TIMEOUT_MS`), each bounded to `_CLOSE_IDS_READ` rows: the
    other spellings of the id (`guessed_id_forms`), looked up by id; the
    same id prefix with a stored `attrs.path` ending in the guessed path's
    last two parts, ignoring case; and, only when that finds none, the same
    prefix with the same file name.

    Only when those name nothing, round two, in what is left of the same
    budget: for an id holding a URL, records of any prefix whose page URL
    (`_URL_ATTRS`) is that URL in one of its spellings (`_url_spellings`;
    exact, so the attrs GIN index answers it); then, for an id whose last
    part is its path or that has no path, ids under the same head that
    start with a hash-like last part (an id range), or ids of any prefix
    whose last `:` part is that part ignoring case (a scan of the ids). A
    round-two read that fails or runs out of time never undoes the first
    reads: the check says "incomplete", also when the id needed no first
    read (a name only): the check began and was cut, so near ids were not
    all checked.

    The reader's own filter runs BEFORE any path or URL is compared, so
    neither a hidden node nor a masked value is ever confirmed. Nothing is
    served in the id's place. Returns the ids (at most `CLOSE_IDS_SHOWN`)
    and how the check went: "checked"; "incomplete" when stored rows
    (hidden ones included) lay past a read's bound, or a round-two read did
    not finish, so the ids are a lower bound and none is not "none";
    "id_incomplete" when the id lacks the part a lookup needs
    (`id_part_missing_hint`) and no read named a record; "no_path"; or
    "unavailable" when the first reads did not finish."""
    missing = id_part_missing_hint(node_id)
    forms = guessed_id_forms(node_id)
    path = _id_path(node_id)
    cleaned = _clean_guessed_id(node_id)
    parts = cleaned.split(":")
    # `commit:<sha>` lacks its repo, but its sha (a start of at least
    # `_HASH_PREFIX_MIN`) is still looked up under every repo
    # (`commit:<repo>:<sha>`); with no id found it says id_incomplete.
    commit_sha = (
        _commit_sha_without_repo(parts) and len(parts[1]) >= _HASH_PREFIX_MIN
    )
    if missing is not None and path is None and not commit_sha:
        return [], "id_incomplete"
    prefix = parts[0]
    last = parts[-1].strip()
    url = _id_url(node_id)
    # A last part to look up: no `/`, and the id's path itself when it has
    # one (never a symbol's line number). A hash start is looked up under
    # its own head (`chunk:<hash>`); a name under any prefix needs three or
    # more parts, so a `<kind>:<key>` id keeps its own meaning.
    by_last = (
        last if (missing is None or commit_sha) and last and "/" not in last
        and (path is None or path == last)
        and (len(parts) >= 3 or (len(parts) == 2 and _is_hash_like(last)))
        else None
    )
    if path is None and not forms and by_last is None and url is None:
        return [], "no_path"
    spelled: list[str] = []
    found: dict[str, str] = {}
    by_url: dict[str, str] = {}
    by_name: list[str] = []
    cut = False
    deadline = time.monotonic() + _CLOSE_IDS_TIMEOUT_MS / 1000

    def bounded(read: Callable[..., list[dict[str, Any]]], where: str,
                params: dict[str, Any], *,
                candidates: Optional[str] = None) -> list[dict[str, Any]]:
        # Stored rows (hidden ones too) past the read: what it names is a
        # lower bound, and none is not "none". The row past the bound only
        # proves there is more; it is never named.
        nonlocal cut
        stored: list[str] = []
        extra_args = {} if candidates is None else {"candidates": candidates}
        rows = read(where=where, params=params,
                    limit=_CLOSE_IDS_READ + 1, stored_ids=stored, **extra_args)
        if len(stored) > _CLOSE_IDS_READ:
            cut = True
            extra = stored[_CLOSE_IDS_READ]
            rows = [row for row in rows if str(row.get("node_id")) != extra]
        return rows

    if forms or path is not None:
        try:
            with _read_budget(conn, session, _CLOSE_IDS_TIMEOUT_MS) as read:
                if forms:
                    rows = read(
                        where="WHERE node_id = ANY(:ids)",
                        params={"ids": forms}, limit=_CLOSE_IDS_READ,
                    )
                    spelled = sorted(
                        {str(row["node_id"]) for row in rows}, key=forms.index,
                    )
                wanted_paths = (
                    dict.fromkeys(_tail_and_name(path)) if path is not None else {}
                )
                for wanted in wanted_paths:
                    if found or not wanted:
                        continue
                    # No `?` operator here: the checkout's query layer reads
                    # `?` as a placeholder. The budget and the id prefix bound
                    # it. The path is the wanted one or ends in `/<wanted>` in
                    # SQL, so `xname.py` never takes the read's room from
                    # `name.py`.
                    rows = bounded(
                        read,
                        "WHERE " + _PATH_IS_OR_ENDS_IN + " AND node_id LIKE :id_prefix",
                        {"id_prefix": _like_escape(prefix) + ":%",
                         **_path_is_or_ends_in_params(wanted)},
                    )
                    found = {
                        str(row["node_id"]): _stored_path(row) for row in rows
                        if str(row.get("node_id", "")).startswith(prefix + ":")
                        and _path_ends_with(row, wanted)
                    }
        except Exception:  # noqa: BLE001 - diagnosis only, never the answer
            logger.debug("close-id read for %r failed", node_id, exc_info=True)
            return [], "unavailable"
    round_two = not (spelled or found) and (url is not None or by_last is not None)
    round_two_done = True
    if round_two:
        left = int((deadline - time.monotonic()) * 1000)
        try:
            if left < 1:
                raise TimeoutError("the close-id read budget is spent")
            with _read_budget(conn, session, left) as read:
                if url is not None:
                    # A page named by its URL: exact spellings, so the node
                    # attrs GIN index answers it; no scan (`_url_read`).
                    candidates, url_params = _url_read(url)
                    for row in bounded(read, "", url_params, candidates=candidates):
                        stored_url = _stored_url_matching(row, url)
                        if stored_url is not None:
                            by_url[str(row["node_id"])] = stored_url
                if not by_url and by_last is not None:
                    if commit_sha:
                        # `commit:<sha>`: the sha is the last part of
                        # `commit:<repo>:<sha>`, so ids under any repo whose
                        # last part starts with it. The `commit:` head
                        # bounds the id range; the rest is checked here.
                        sha = by_last.lower()
                        by_name = sorted(
                            str(row["node_id"]) for row in bounded(
                                read,
                                "WHERE node_id >= :commit_low AND node_id < :commit_high "
                                "AND lower(node_id) LIKE :commit_sha",
                                {
                                    "commit_low": "commit:",
                                    "commit_high": "commit;",
                                    "commit_sha": "commit:%:" + _like_escape(sha) + "%",
                                },
                            )
                            if str(row["node_id"]).count(":") == 2
                            and str(row["node_id"]).rsplit(":", 1)[-1].lower().startswith(sha)
                        )
                    elif _is_hash_like(by_last):
                        # A hash or its start: ids under the same head that
                        # start with it, read as an id range (indexed) and
                        # checked as a prefix. A start too short to look up
                        # never reaches here (`id_part_missing_hint`).
                        head = ":".join(parts[:-1]) + ":" + by_last.lower()
                        upper = _hex_upper_bound(by_last.lower())
                        where = "WHERE node_id >= :hash_low AND node_id LIKE :hash_head"
                        params: dict[str, Any] = {
                            "hash_low": head,
                            "hash_head": _like_escape(head) + "%",
                        }
                        if upper is not None:
                            where += " AND node_id < :hash_high"
                            params["hash_high"] = ":".join(parts[:-1]) + ":" + upper
                        by_name = sorted(
                            str(row["node_id"]) for row in bounded(read, where, params)
                            if str(row["node_id"]).lower().startswith(head)
                        )
                    else:
                        # A name under another prefix or source (a wiki page
                        # under `doc:`): the same last part, ignoring case;
                        # the same prefix first, then the same case. A scan
                        # of the ids, bounded by the rows and by what is
                        # left of the shared budget: cut there, the check
                        # says "incomplete".
                        candidates, name_params = _name_read(by_last)
                        by_name = sorted(
                            (
                                str(row["node_id"]) for row in bounded(
                                    read, "", name_params, candidates=candidates,
                                )
                                if str(row["node_id"]).rsplit(":", 1)[-1].casefold()
                                == by_last.casefold()
                            ),
                            key=lambda found_id: (
                                not found_id.startswith(prefix + ":"),
                                found_id.rsplit(":", 1)[-1] != by_last,
                                found_id,
                            ),
                        )
        except Exception:  # noqa: BLE001 - said as "incomplete", never as none
            # A cut second-stage read says "incomplete" (near-ids-2), with or
            # without first reads before it: "unavailable" is only for first
            # reads that did not finish.
            logger.debug("close-id round two for %r failed", node_id, exc_info=True)
            round_two_done = False
    ranked = rank_near_paths(path, found) if path is not None else []
    if prefix == "symbol" and path is not None:
        rest = cleaned.split(path, 1)[1] if path in cleaned else ""
        name = rest.strip(":").split(":")[0]
        if name:
            ranked.sort(key=lambda found_id: f":{name}" not in found_id)
    ids = [
        found_id
        for found_id in dict.fromkeys([*spelled, *ranked, *sorted(by_url), *by_name])
        if found_id != node_id
    ]
    if cut or not round_two_done:
        return ids[:CLOSE_IDS_SHOWN], "incomplete"
    if missing is not None and not ids:
        return [], "id_incomplete"
    return ids[:CLOSE_IDS_SHOWN], "checked"


def _like_escape(text: str) -> str:
    """`text` as a literal inside a LIKE pattern."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


#: A stored path that IS the wanted one or ends in `/<wanted>`, ignoring
#: case: a part boundary in SQL, so `xname.py` never fills a bounded read
#: meant for `name.py` (#3666 fix E review).
_PATH_IS_OR_ENDS_IN = (
    "(lower(attrs->>'path') = :path_is OR lower(attrs->>'path') LIKE :path_ends_in)"
)


def _path_is_or_ends_in_params(wanted: str) -> dict[str, str]:
    lowered = wanted.strip().strip("/").lower()
    return {"path_is": lowered, "path_ends_in": "%/" + _like_escape(lowered)}


def _visible_rows_within(
    conn: psycopg.Connection,
    session: Session,
    *,
    where: str,
    params: dict[str, Any],
    limit: int,
    timeout_ms: int,
    stored_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """`_visible_node_rows` under its own statement timeout, set and put
    back as `close_node_ids` does. A timeout raises. `stored_ids` receives
    the ids of the stored rows read, before the reader's filter."""
    from psycopg.rows import tuple_row

    block = nullcontext() if conn.autocommit else conn.transaction()
    with block:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT current_setting('statement_timeout'), "
                "set_config('statement_timeout', %s, false)",
                (str(int(timeout_ms)),),
            )
            before = cur.fetchone()[0]
        try:
            return _visible_node_rows(
                conn, session, where=where, params=params, limit=limit,
                stored_ids=stored_ids,
            )
        finally:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.INERROR:
                conn.execute(
                    "SELECT set_config('statement_timeout', %s, false)", (before,),
                )


def _visible_rows_counted(
    conn: psycopg.Connection,
    session: Session,
    *,
    where: str,
    params: dict[str, Any],
    limit: int,
    timeout_ms: int,
) -> tuple[list[dict[str, Any]], bool]:
    """The first `limit` stored rows (id order) under a statement timeout,
    filtered for this reader, and whether stored rows lay past them. The
    flag counts STORED rows, hidden ones included, so a caller never reads
    a cut-off read as the whole answer (#3666 step 3b review)."""
    stored: list[str] = []
    rows = _visible_rows_within(
        conn, session, where=where, params=params, limit=int(limit) + 1,
        timeout_ms=timeout_ms, stored_ids=stored,
    )
    if len(stored) <= int(limit):
        return rows, False
    # The stored row past the limit only proves there is more; never served.
    extra = stored[int(limit)]
    return [row for row in rows if str(row.get("node_id")) != extra], True


#: Candidate ids a path lookup names, and the rows it reads to find them.
PATH_CANDIDATES_SHOWN = 5
_PATH_LOOKUP_READ = 50
#: The path lookup's own statement timeout, per read.
_PATH_LOOKUP_TIMEOUT_MS = 2_000


def _stored_path(row: Mapping[str, Any]) -> str:
    attrs = row.get("attrs")
    stored = attrs.get("path") if isinstance(attrs, Mapping) else None
    return stored.strip().strip("/") if isinstance(stored, str) else ""


def _names_file(row: Mapping[str, Any], path: str) -> bool:
    """The record of the file itself: its `attrs.path` is `path` and its id
    ends with `:<path>` (`doc:<repo>:<path>`, `file:<repo>:<path>`). A
    symbol of the file carries the path too, but its id goes on past it."""
    return bool(path) and _stored_path(row) == path and str(
        row.get("node_id", "")
    ).endswith(":" + path)


def _from_source(row: Mapping[str, Any], path: str, source: str) -> bool:
    """`source` is the record's source, its repository (`attrs.repo`), or
    the middle of its id (`<kind>:<source>:<path>`)."""
    attrs = row.get("attrs") if isinstance(row.get("attrs"), Mapping) else {}
    return (
        row.get("source") == source
        or attrs.get("repo") == source
        or str(row.get("node_id", "")).split(":", 1)[-1] == f"{source}:{path}"
    )


@dataclass(frozen=True)
class PathLookup:
    """What `inspect`'s `path` names (#3666 step 3b). `matches`: the records
    of that file (from `source` when one was given). `near`, only when
    nothing matched: the same file in another source, or the same last two
    path parts (ignoring case) under another directory, else the same file
    name; closest path first. `status`: "checked";
    "incomplete" when more stored records have the path than one read
    covers (hidden ones included), so `matches` is a lower bound and an
    empty one is not "none"; or "unavailable" when the exact read did not
    finish. `near_checked`: False when the near read did not finish or
    was cut, so an empty `near` is not "nothing near"."""

    matches: list[str]
    near: list[str]
    status: str
    near_checked: bool = True


def normalize_target_path(path: str) -> str:
    """A path as stored: no surrounding space, no leading `./` or `/`."""
    path = path.strip()
    while path.startswith("./"):
        path = path[2:]
    return path.strip("/")


def lookup_path(
    conn: psycopg.Connection, session: Session, path: str, source: Optional[str],
) -> PathLookup:
    """The records `path` (and `source`) name, for `inspect` (Jason,
    2026-10-12). Bounded reads, each under its own timeout, then the
    reader's own filter BEFORE the path is compared, so neither a hidden
    record nor a masked path is ever confirmed. Nothing is guessed: the
    caller serves a single match only.

    `source` is tested in SQL, so records of other sources never take the
    read's room; a read that stops at its bound (stored rows, hidden ones
    included) says "incomplete", never "none" or an exact count."""
    exact = "WHERE node_id LIKE :id_tail AND attrs->>'path' = :path"
    params: dict[str, Any] = {"path": path, "id_tail": "%:" + _like_escape(path)}
    source_sql = (
        " AND (source = :source_a OR attrs->>'repo' = :source_b"
        " OR node_id LIKE :source_tail)"
    )
    try:
        if source is not None:
            rows, more = _visible_rows_counted(
                conn, session, where=exact + source_sql,
                params={**params, "source_a": source, "source_b": source,
                        "source_tail": "%:" + _like_escape(f"{source}:{path}")},
                limit=_PATH_LOOKUP_READ, timeout_ms=_PATH_LOOKUP_TIMEOUT_MS,
            )
            matches = sorted({
                str(row["node_id"]) for row in rows
                if _names_file(row, path) and _from_source(row, path, source)
            })
            if matches or more:
                return PathLookup(matches, [], "incomplete" if more else "checked")
        # No source, or none from it: every source's records of the path.
        rows, more = _visible_rows_counted(
            conn, session, where=exact, params=params,
            limit=_PATH_LOOKUP_READ, timeout_ms=_PATH_LOOKUP_TIMEOUT_MS,
        )
    except Exception:  # noqa: BLE001 - said as "unavailable", never as no match
        logger.debug("path lookup for %r failed", path, exc_info=True)
        return PathLookup([], [], "unavailable")
    files = sorted({str(row["node_id"]) for row in rows if _names_file(row, path)})
    if source is not None:
        # None from `source`: the same file elsewhere is the near one.
        if files or more:
            return PathLookup([], files, "checked", near_checked=not more)
    elif files or more:
        return PathLookup(files, [], "incomplete" if more else "checked")
    # A moved file, or a guessed spelling (#3666): the same last two parts
    # under another directory or in another case, `%XX` decoded; when that
    # finds none, the same file name. One budget covers both reads.
    guessed = unquote(path) if "%" in path else path
    deadline = time.monotonic() + _PATH_LOOKUP_TIMEOUT_MS / 1000
    near_found: dict[str, str] = {}
    cut = False
    for wanted in dict.fromkeys(_tail_and_name(guessed)):
        if near_found or not wanted:
            continue
        near_where = "WHERE lower(node_id) LIKE :id_tail AND " + _PATH_IS_OR_ENDS_IN
        near_params: dict[str, Any] = {
            "id_tail": "%" + _like_escape(wanted.lower()),
            **_path_is_or_ends_in_params(wanted),
        }
        if source is not None:
            near_where += (
                " AND (source = :source_a OR attrs->>'repo' = :source_b"
                " OR node_id LIKE :source_like)"
            )
            near_params.update({
                "source_a": source, "source_b": source,
                "source_like": "%:" + _like_escape(source) + ":%",
            })
        left = int((deadline - time.monotonic()) * 1000)
        try:
            if left < 1:
                raise TimeoutError("the near-path read budget is spent")
            rows, more = _visible_rows_counted(
                conn, session, where=near_where, params=near_params,
                limit=_PATH_LOOKUP_READ, timeout_ms=left,
            )
        except Exception:  # noqa: BLE001 - the exact read finished; the reply says the hints are missing
            logger.debug("near-path lookup for %r failed", path, exc_info=True)
            return PathLookup([], rank_near_paths(guessed, near_found), "checked",
                              near_checked=False)
        cut = cut or more
        near_found = {
            str(row["node_id"]): _stored_path(row) for row in rows
            if _ends_with_path(_stored_path(row), wanted)
            and _stored_path(row) != path
            and _names_file(row, _stored_path(row))
            and (source is None or _from_source(row, _stored_path(row), source))
        }
    return PathLookup([], rank_near_paths(guessed, near_found), "checked",
                      near_checked=not cut)


def resolve_target_path(
    conn: psycopg.Connection, session: Session, target: Mapping[str, Any],
) -> Optional[str]:
    """The node id an `inspect` `node` or `text` target names by `path`
    (and optional `source`), or None when it names none by path.

    Exactly one record: its id. Otherwise refused: no match is
    `node_not_found`, several are `invalid_operator_arguments`; each lists
    at most `PATH_CANDIDATES_SHOWN` candidate ids, and exactly one near
    candidate comes as a call to retry. A target with both `node_id` and
    `path` is refused: the two could disagree."""
    path = target.get("path")
    if path is None:
        return None
    kind = target.get("kind")
    if target.get("node_id") is not None:
        raise InvalidOperatorArguments(
            f"inspect {kind}: pass node_id or path, not both",
            data={"tool": "inspect", "invalid_fields": ["target.path"],
                  "validation_kinds": ["argument_check"]},
            fix_hint="drop path, or drop node_id",
        )
    source = target.get("source")
    if not isinstance(path, str) or not normalize_target_path(path) or (
        source is not None and (not isinstance(source, str) or not source.strip())
    ):
        raise InvalidOperatorArguments(
            f"inspect {kind}: path must be a file path such as lib/x.py, and "
            "source, when given, a source or repository name",
            data={"tool": "inspect", "invalid_fields": ["target.path"],
                  "validation_kinds": ["argument_check"]},
            fix_hint='pass target={"kind":"text","path":"lib/x.py"}',
        )
    path = normalize_target_path(path)
    source = source.strip() if isinstance(source, str) else None
    found = lookup_path(conn, session, path, source)
    named = f"path {path!r}" + (f" in source {source!r}" if source else "")
    data: dict[str, Any] = {
        "path": path, "source": source,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "path_check": found.status,
    }
    lookup_hint = (
        'find the record with search(method="attrs", where={"op":"eq",'
        f'"field":"attrs.path","value":{json.dumps(path)}}}) and pass its node_id'
    )
    if len(found.matches) == 1 and found.status == "checked":
        return found.matches[0]
    if found.matches:
        # Several, or one when more records with the path were not read:
        # never served, since another could be the one meant.
        shown = found.matches[:PATH_CANDIDATES_SHOWN]
        count = (
            f"at least {len(found.matches)} records (the lookup read the first "
            f"{_PATH_LOOKUP_READ} with that path)"
            if found.status == "incomplete" else f"{len(found.matches)} records"
        )
        error = InvalidOperatorArguments(
            f"inspect {kind}: {named} names {count}; one of them: {shown}",
            data={"tool": "inspect", "invalid_fields": ["target.path"],
                  "validation_kinds": ["argument_check"],
                  **data, "candidate_ids": shown},
            fix_hint=(
                "pass the node_id of the one you mean"
                + ("" if source else ", or add source")
            ),
        )
        raise error
    if found.status == "incomplete":
        raise NodeNotFound(
            f"inspect {kind}: the lookup of {named} read the first "
            f"{_PATH_LOOKUP_READ} stored records with that path and found none "
            "you can read; more were not read, so this is not a finding that "
            "none exists",
            data={**data, "exists": None},
            fix_hint=(
                lookup_hint if source else "add source to narrow it, or " + lookup_hint
            ),
        )
    if found.status != "checked":
        raise NodeNotFound(
            f"inspect {kind}: the lookup of {named} did not finish in "
            f"{_PATH_LOOKUP_TIMEOUT_MS // 1000} s",
            data={**data, "exists": None},
            fix_hint=lookup_hint,
        )
    shown = found.near[:PATH_CANDIDATES_SHOWN]
    message = f"inspect {kind}: no visible record has {named}"
    if shown:
        message += f"; records with a near path: {shown}"
    if not found.near_checked:
        message += (
            "; near ids not all checked: the lookup did not finish in "
            f"{_PATH_LOOKUP_TIMEOUT_MS // 1000} s or read only its first "
            f"{_PATH_LOOKUP_READ} records"
        )
        data["near_check"] = "unavailable"
    error = NodeNotFound(
        message,
        data={**data, "exists": False, **({"close_ids": shown} if shown else {})},
        fix_hint=(
            f"retry with one of {shown}; an id read from a result is exact"
            if shown else
            "check the path; a path is the file's path inside its repository, "
            "as a search result's attrs.path shows it"
        ),
    )
    if len(found.near) == 1:
        retry_target = {
            key: value for key, value in target.items()
            if key not in {"path", "source"} and value is not None
        }
        retry_target["node_id"] = found.near[0]
        error.retry_call = {
            "tool": "inspect", "mode": "guess",
            "arguments": {"target": retry_target},
        }
    raise error


def node_not_visible(
    node_id: str,
    generation_id: Any,
    *,
    close: Optional[tuple[list[str], str]] = None,
) -> NodeNotFound:
    """The refusal for a node that is absent OR hidden from this reader.

    expose-mcp-node-lookup req.lookup.inspect-not-found: absence is an
    explicit signal (exists: false), never a payload echoing the requested
    id with null attrs. Under Nomos enforcement a denied node takes this same
    path by design: absence and denial must be indistinguishable to a
    least-privilege reader. `close` (from `close_node_ids`) names visible
    ids the caller may have meant: the exact fix for a hand-built id.
    """
    data: dict[str, Any] = {
        "node_id": node_id,
        "generation_id": generation_id,
        "pinned_generation_id": generation_id,
        "exists": False,
    }
    message = (
        f"node {node_id!r} is not visible at "
        f"generation {generation_id} "
        "(exists: false at this pin)"
    )
    fix_hint = NODE_NOT_VISIBLE_FIX_HINT
    if close is not None:
        ids, check = close
        data["close_id_check"] = check
        if ids:
            data["close_ids"] = ids
            message += f"; visible ids with a near id or path: {ids}"
            fix_hint = (
                f"retry with one of {ids}; an id read from a result is exact"
            )
        if check == "incomplete":
            message += (
                f"; near ids not all checked: a lookup read only the first "
                f"{_CLOSE_IDS_READ} records with that path, file name, URL, "
                f"name or hash, or did not finish in "
                f"{_CLOSE_IDS_TIMEOUT_MS / 1000:g} s"
            )
        elif check == "id_incomplete":
            missing = id_part_missing_hint(node_id)
            if missing is not None:
                message += "; the id lacks the part a lookup needs"
                fix_hint = missing
        elif not ids and check == "checked":
            fix_hint = (
                f"no visible {node_id.split(':', 1)[0]}: id has that path; "
                if _id_path(node_id) is not None else
                "no visible id has that last part under any prefix; "
            ) + NODE_NOT_VISIBLE_FIX_HINT
    return NodeNotFound(message, data=data, fix_hint=fix_hint)


def _edge_not_visible(edge_id: str, generation_id: Any) -> NodeNotFound:
    """`node_not_visible` for an edge target: the same code and shape."""
    return NodeNotFound(
        f"edge {edge_id!r} is not visible at "
        f"generation {generation_id} "
        "(exists: false at this pin)",
        data={
            "edge_id": edge_id,
            "target_kind": "edge",
            "generation_id": generation_id,
            "pinned_generation_id": generation_id,
            "exists": False,
        },
        fix_hint=(
            "check the edge_id or session pin; expand(...) from a visible "
            "node lists the edges this reader can see"
        ),
    )


def _visible_attr_target(
    conn: psycopg.Connection,
    session: Session,
    *,
    target_kind: str,
    target_id: str,
) -> dict[str, Any]:
    """The target's row after the session's read filter, or the refusal.

    A node is decided exactly as `inspect_node` decides it: the same
    `node_by_id_program` lookup, then `filter_node_rows_for_session`. An edge
    is visible only when the edge row AND both endpoint nodes pass the
    filter, the rule `expand` applies before it shows an edge. Absent and
    hidden targets raise the same `NodeNotFound`.
    """
    if target_kind == "node":
        rows = lower_program_over_chronos(
            node_by_id_program(),
            conn,
            generation_id=session.generation_id,
            branch=session.branch_name,
            extra_params={"focal": target_id},
        )
        rows = filter_node_rows_for_session(conn, session, rows) if rows else []
        if not rows:
            raise node_not_visible(
                target_id, session.generation_id,
                close=close_node_ids(conn, session, target_id),
            )
        return rows[0]
    # `okg.graph_edges.edge_id` is BIGINT: a non-numeric id names no edge.
    if not target_id.isdigit():
        raise _edge_not_visible(target_id, session.generation_id)
    edges = _visible_edge_rows(
        conn,
        session,
        where="WHERE e.edge_id = :edge_id",
        params={"edge_id": int(target_id)},
    )
    if not edges:
        raise _edge_not_visible(target_id, session.generation_id)
    edge = edges[0]
    endpoints = {str(edge["src"]), str(edge["dst"])}
    visible = _visible_node_rows(
        conn,
        session,
        where="WHERE node_id = ANY(:ids)",
        params={"ids": sorted(endpoints)},
    )
    if {str(row["node_id"]) for row in visible} != endpoints:
        raise _edge_not_visible(target_id, session.generation_id)
    return edge


#: Sentinel: `mask_attrs` removed the key (a drop class such as free text).
_DROPPED = object()

#: What survives of an observation's free-form metadata when the reader's
#: read of the record is redacted. It applies to EVERY observation row of
#: such a record, not only to rows whose own value is withheld: a row for a
#: visible key (say `classification`) can quote a withheld value in its
#: metadata just as easily. An ALLOWLIST, not a denylist: these mappings are
#: written by callers and by the store, and any of them can carry a value --
#: `record_attribute_observation` writes `decision.diff` with the raw old and
#: new values and both hashes, and a producer may quote a value in
#: `provenance` or `dependencies`. Only keys listed here, and only when their
#: value is a string, are kept; every other key, at any depth, is dropped.
#:
#: * `decision.superseded_by`: the id of the observation that replaced this
#:   one (an `attr:<hex>` id the store mints), already public as the row's
#:   own `superseded_by` field.
#: * `provenance`, `dependencies`: nothing. They are entirely caller-defined,
#:   so no key is known to be safe.
#:
#: `dependency_fingerprint` is caller-supplied and may be derived from a
#: value, so every row of a redacted record returns it as None.
_REDACTED_RECORD_METADATA_ALLOWLIST: dict[str, frozenset[str]] = {
    "decision": frozenset({"superseded_by"}),
    "provenance": frozenset(),
    "dependencies": frozenset(),
}


def _allowlisted_metadata(value: Any, allowed: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {
        key: item
        for key, item in value.items()
        if key in allowed and isinstance(item, str)
    }


def _mask_observation(
    observation: dict[str, Any],
    *,
    classifier: Any,
) -> dict[str, Any]:
    """One observation of a redacted record, masked as the record's attrs are.

    Every row keeps only `_REDACTED_RECORD_METADATA_ALLOWLIST` of its
    `decision`, `provenance` and `dependencies`, and a null
    `dependency_fingerprint`. Then `mask_attrs` decides the value per key, so
    masking `{attr_key: value}` gives the value the redacted attrs carry for
    that key: revealed, `MASK_TOKEN`, or dropped. A withheld value also loses
    its `value_hash` (the hash of a short identifier is reversible by
    guessing), is masked the same way in `diff`, and carries the redaction
    marker.
    """
    key = str(observation.get("attr_key"))
    out = dict(observation)
    for field, allowed in _REDACTED_RECORD_METADATA_ALLOWLIST.items():
        if field in out:
            out[field] = _allowlisted_metadata(out[field], allowed)
    if "dependency_fingerprint" in out:
        out["dependency_fingerprint"] = None

    def masked(value: Any) -> tuple[bool, Any]:
        result = mask_attrs(
            {key: value}, classifier=classifier, drop_classes=READ_DROP_CLASSES
        )
        if result.get(REDACTION_MARKER) is not True:
            return False, value
        return True, result.get(key, _DROPPED)

    withheld, value = masked(observation.get("value"))
    if not withheld:
        return out
    out[REDACTION_MARKER] = True
    out.pop("value_hash", None)
    if value is _DROPPED:
        out.pop("value", None)
    else:
        out["value"] = value
    diff = out.get("diff")
    if isinstance(diff, Mapping):
        new_diff: dict[str, Any] = {}
        for side in ("old", "new"):
            if side in diff:
                _, side_value = masked(diff[side])
                if side_value is not _DROPPED:
                    new_diff[side] = side_value
        out["diff"] = new_diff
    return out


def inspect_attr_provenance(
    conn: psycopg.Connection,
    session: Session,
    *,
    target_kind: str,
    target_id: int | str,
    attr_key: str | None = None,
    namespace: str = "default",
) -> dict[str, Any]:
    """Opt-in attribute observation history for one node or edge target.

    Under the Nomos runtime (audit or enforce) the target is decided first,
    with the reader's session, by the rule the node and edge reads use
    (issue #3189): a hidden or absent target raises the same `NodeNotFound`
    as `inspect(node)` before any history is read, and a target whose read
    was redacted returns its observation values masked exactly as its attrs
    are. With the runtime off nothing is decided or masked, as before.
    """
    assert_session_matches(conn, session)
    redact_with = None
    if (
        target_kind in ("node", "edge")
        and deployment_requires_nomos_runtime(session.deployment_name)
    ):
        row = _visible_attr_target(
            conn, session, target_kind=target_kind, target_id=str(target_id),
        )
        if isinstance(row.get("attrs"), ReadRedactedAttrs):
            redact_with = read_attr_classifier_for_row(conn, session, row)
    observations = attribute_history_for_target(
        conn,
        target_kind=target_kind,
        target_id=target_id,
        generation_id=session.generation_id,
        attr_key=attr_key,
        namespace=namespace,
    )
    if redact_with is not None:
        observations = [
            _mask_observation(row, classifier=redact_with) for row in observations
        ]
    payload = {
        "target_kind": target_kind,
        "target_id": str(target_id),
        "attr_key": attr_key,
        "namespace": namespace,
        "observations": observations,
        "total_count": len(observations),
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
    }
    if redact_with is not None:
        payload[REDACTION_MARKER] = True
    return payload


#: The selectable blocks on the graph target, and the `summary` key each one
#: produces. `select` names these; anything else is refused by `validate_select`.
_BLOCK_SUBTYPES = "subtypes"
_BLOCK_EDGE_TYPES = "edge_types"
_BLOCK_EMBED_LAG = "embed_lag"
_BLOCK_UNRESOLVED_MENTIONS = "unresolved_mentions"
_BLOCK_KEYS: dict[str, str] = {
    _BLOCK_SUBTYPES: "subtype_counts",
    _BLOCK_EDGE_TYPES: "edge_type_counts",
    _BLOCK_EMBED_LAG: "embed_lag",
    _BLOCK_UNRESOLVED_MENTIONS: "unresolved_mentions",
}
INSPECT_GRAPH_BLOCKS = frozenset(_BLOCK_KEYS)
#: The schema digest needs BOTH count maps; built from one it reports the other
#: as zero (see the note at the assembly site).
_SCHEMA_BLOCKS = frozenset({_BLOCK_SUBTYPES, _BLOCK_EDGE_TYPES})
#: Blocks the maintained count rollup can serve, and its dimension names.
#: IMPORTED, not re-declared: the constants module exists so a read's per-kind
#: filter matches the write's stamping by construction. A local literal that
#: drifted from the write side would return {} for that kind and report it as
#: authoritative -- a silent zero.
from okg.substrate.derived_artifacts.maintained_derivation_constants import (  # noqa: E402
    COUNT_KIND_EDGE_TYPE as _ROLLUP_KIND_EDGE_TYPE,
    COUNT_KIND_SUBTYPE as _ROLLUP_KIND_SUBTYPE,
)

_ROLLUP_KINDS_FOR_BLOCKS = (_ROLLUP_KIND_SUBTYPE, _ROLLUP_KIND_EDGE_TYPE)
_ROLLUP_SERVED_BLOCKS = _SCHEMA_BLOCKS
#: Where the subtype/edge-type counts came from. A caller that requires O(1)
#: counts can detect the degradation instead of inferring it from latency.
_COUNTS_SOURCE_ROLLUP = "console_count_rollup"
_COUNTS_SOURCE_SCAN = "graph_scan"


def inspect_graph(
    conn: psycopg.Connection,
    session: Session,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """The canonical "what's in this graph?" verb.

    Returns counts (nodes, edges, distinct subtypes / edge types
    at the pin), operational observability (embed_lag,
    entity-mention resolution rate by entity_type), AND the full
    schema digest (per-subtype attr keys + edge routes + score
    functions) — all in one call.

    Per `retire-redundant-mcp-tools` (2026-04-27): replaces the
    prior `graph_stats` tool; subsumes the schema-digest payload
    so agents have one introspection verb. The
    `okg://schema-digest/<deployment>` MCP resource still exists
    for clients that auto-load resources at session open; this
    tool is the callable path for everyone else.

    Field semantics (carried forward from `graph_stats`):

    Field semantics (documented 2026-04-25 from
    `debug-substrate-okg-self` Phase 10):

    - `subtype_count`: the count of DISTINCT subtypes that
      currently have at least one live node. NOT the count
      of registered subtypes. Use `get_ontology(...)` to see
      every registered subtype, including ones that the
      catalog declares but no source has emitted (registered-
      not-emitted). The two values can legitimately differ —
      a deployment may load modules that declare subtypes for
      cross-module bridges without itself emitting them.
    - `edge_type_count`: same semantic — DISTINCT edge_types
      with ≥1 live edge.

    Visibility: counts use the same identity/lineage filter as graph reads
    whenever enforcement or an identity claim can hide rows. Affordable graphs
    have exact global visible counts; larger filtered graphs withhold counts.
    Off mode and audit sessions without an identity claim retain whole-graph
    aggregates. `visibility` and `truncated` disclose which basis was used.
    """
    assert_session_matches(conn, session)
    # `select` narrows the WORK, not just the payload: an unselected block does
    # not run its query. Refused through the shared `validate_select` so an
    # unknown name reports the same way it does on every other tool surface --
    # a misspelled block must not silently widen back to the full (slow) answer.
    validate_select(select, INSPECT_GRAPH_BLOCKS, tool_name="inspect(graph)")
    blocks = (
        INSPECT_GRAPH_BLOCKS if not select else frozenset(select)
    )
    graph = _chronos_checkout(conn, session)
    deployment = session.deployment_name
    mode = deployment_nomos_enforcement_mode(deployment)

    # The identity loop hard-denies rows in audit too; only the later policy
    # loop downgrades audit decisions. Match read_filter's None check (even an
    # empty claim is evaluated) and its off-mode early return. This applies to
    # single-user sessions carrying a claim as well as mapped multi-user ones.
    identity_filter = mode != "off" and session.nomos_identity_claim is not None
    filter_reason = "nomos_identity_filter" if identity_filter else "nomos_enforce"
    if mode != "enforce" and not identity_filter:
        # The two count blocks come from the maintained rollup when it can serve
        # this pin; anything it cannot serve falls back to the aggregate AND says
        # so via counts_source/counts_source_reason.
        counts_source = _COUNTS_SOURCE_ROLLUP
        counts_source_reason = None
        summary = {}
        wanted_counts = blocks & _ROLLUP_SERVED_BLOCKS
        if wanted_counts:
            summary, counts_source_reason = _graph_counts_from_rollup(
                conn, session, wanted_counts
            )
            if counts_source_reason is not None:
                counts_source = _COUNTS_SOURCE_SCAN
                logger.warning(
                    "inspect(graph) counts fell back to a whole-graph scan "
                    "(deployment=%s generation=%s reason=%s); the maintained "
                    "rollup could not serve this pin",
                    deployment,
                    session.generation_id,
                    counts_source_reason,
                )
        else:
            counts_source = _COUNTS_SOURCE_SCAN
            counts_source_reason = "counts_not_selected"
        # Blocks the rollup does not cover, plus the count blocks when it could
        # not serve them, still run as aggregates.
        remaining = blocks - {
            b for b in _ROLLUP_SERVED_BLOCKS if _BLOCK_KEYS[b] in summary
        }
        if remaining:
            summary.update(_graph_summary_sql(graph, remaining))
        visibility = "unfiltered"
        truncated = False
    elif _graph_total_rows(graph) <= _INSPECT_GRAPH_EXACT_ROW_BUDGET:
        # Filtered counts: hiding is real and per-row. Small graphs can afford the
        # exact per-row lineage filter (the original implementation). The rollup
        # carries NO source lineage, so it cannot answer a visibility-filtered
        # question and is not consulted here.
        summary = _graph_summary_per_row(conn, session)
        summary = {_BLOCK_KEYS[b]: summary[_BLOCK_KEYS[b]] for b in blocks}
        visibility = "enforced"
        truncated = False
        counts_source = _COUNTS_SOURCE_SCAN
        counts_source_reason = filter_reason
    else:
        # On a large filtered graph the exact per-row filter is the
        # minutes/GBs blowup this change fixes, and returning whole-graph SQL
        # counts would disclose the cardinality of sources the policy may hide
        # (aggregate counts are still information). So return generation
        # metadata with counts omitted and tell the caller to narrow the
        # target; the per-row read surfaces (search/filter/aggregate) stay
        # exact at any size.
        return _inspect_graph_counts_unavailable(
            session,
            reason=(
                "graph exceeds the exact-count budget under Nomos read "
                "filtering; use search/filter/aggregate for visibility-accurate "
                "results, or narrow the target"
            ),
            filter_reason=filter_reason,
        )

    # Identity, pin and counts provenance are ALWAYS present; everything else is
    # gated on its block. A key that is neither always-present nor gated is how a
    # zero gets emitted for a block that did not run.
    payload: dict[str, Any] = {
        "deployment_name": deployment,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "counts_source": counts_source,
        "visibility": visibility,
        "truncated": truncated,
    }
    if counts_source_reason is not None:
        payload["counts_source_reason"] = counts_source_reason

    if _BLOCK_SUBTYPES in blocks:
        subtype_counts = summary["subtype_counts"]
        payload["subtype_counts"] = subtype_counts
        payload["subtype_count"] = len(subtype_counts)
        payload["node_count"] = sum(subtype_counts.values())
    if _BLOCK_EDGE_TYPES in blocks:
        edge_type_counts = summary["edge_type_counts"]
        payload["edge_type_counts"] = edge_type_counts
        payload["edge_type_count"] = len(edge_type_counts)
        payload["edge_count"] = sum(edge_type_counts.values())
    if _BLOCK_EMBED_LAG in blocks:
        payload["embed_lag"] = summary["embed_lag"]
    if _BLOCK_UNRESOLVED_MENTIONS in blocks:
        payload["unresolved_mentions"] = summary["unresolved_mentions"]

    # The digest is built from BOTH count maps, and `minimal_digest` coerces a
    # missing one to empty -- which would publish `edge_type_count: 0` and
    # `edge_types: []` for a caller who selected only `subtypes`, telling it the
    # graph has no edge types at all. Omit the digest rather than build it from
    # partial inputs.
    if _SCHEMA_BLOCKS <= blocks:
        from okg.substrate.mcp.schema_digest import minimal_digest
        payload["schema"] = minimal_digest(
            deployment_name=deployment,
            pinned_generation_id=session.generation_id,
            subtype_counts=summary["subtype_counts"],
            edge_type_counts=summary["edge_type_counts"],
            minimal_reason="Chronos-backed graph read path",
        )
    return payload


def _inspect_graph_counts_unavailable(
    session: Session, *, reason: str, filter_reason: str = "nomos_enforce"
) -> dict[str, Any]:
    """Bounded inspect(graph) payload with counts omitted.

    Returned under identity/enforcement filtering above the exact-count budget:
    the exact per-row filter is too expensive and whole-graph counts would
    disclose hidden-source cardinality, so the count fields are null and the
    caller is pointed at the per-row read surfaces. Keeps the payload keys
    stable (counts are None, `truncated` True) so consumers don't KeyError.
    """
    return {
        "deployment_name": session.deployment_name,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "node_count": None,
        "edge_count": None,
        "subtype_count": None,
        "edge_type_count": None,
        "embed_lag": None,
        "unresolved_mentions": None,
        "schema": None,
        # This payload's key set is hand-written, so it would otherwise be the
        # one response that omits the counts provenance keys.
        "counts_source": _COUNTS_SOURCE_SCAN,
        "counts_source_reason": f"{filter_reason}_over_budget",
        "visibility": (
            "identity-filtered; exact counts unavailable for a graph this size"
            if filter_reason == "nomos_identity_filter"
            else "enforce; exact counts unavailable for a graph this size"
        ),
        "truncated": True,
        "note": reason,
    }


# Node+edge ceiling for exact per-row identity/enforcement filtering. Larger
# graphs withhold totals rather than disclose hidden-source cardinality.
_INSPECT_GRAPH_EXACT_ROW_BUDGET = 5_000


def _graph_summary_per_row(
    conn: psycopg.Connection, session: Session,
) -> dict[str, Any]:
    """Exact whole-graph summary via the per-row Nomos filter.

    This is the original inspect(graph) implementation: materialize the
    visible node/edge rows (each already lineage-filtered by
    `_visible_node_rows`/`_visible_edge_rows`) and count them in Python. Used
    for affordable graphs whenever identity or enforcement can hide rows.
    Larger filtered graphs withhold counts; unfiltered graphs aggregate in SQL.
    """
    nodes = _visible_node_rows(conn, session)
    edges = _visible_edge_rows(conn, session)
    subtype_counts: dict[str, int] = {}
    for row in nodes:
        subtype = str(row["subtype"])
        subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1
    edge_type_counts: dict[str, int] = {}
    for row in edges:
        edge_type = str(row["edge_type"])
        edge_type_counts[edge_type] = edge_type_counts.get(edge_type, 0) + 1
    return {
        "subtype_counts": subtype_counts,
        "edge_type_counts": edge_type_counts,
        "embed_lag": sum(1 for row in nodes if row.get("embedding") is None),
        "unresolved_mentions": _unresolved_mentions_block(nodes, edges),
    }


def _graph_summary_sql(graph: Any, blocks: frozenset[str]) -> dict[str, Any]:
    """Compute the requested inspect(graph) counts as whole-graph SQL aggregates.

    Used in off mode or audit sessions without an identity claim. Counts are
    pushed into SQL so the graph is never materialized in Python.

    `blocks` is the set of blocks the caller selected. A block that was NOT
    selected does not run its query at all -- that is the point
    (bound-inspect-graph-select). Measured on live okg-workspace, the four
    blocks cost 1992 / 2413 / 2435 / 104 ms, so a caller wanting only the two
    count blocks was paying ~2.4s for an embedding-lag scan it never asked for.
    Absent keys mean "not computed"; callers must not read a missing key as 0.
    """
    out: dict[str, Any] = {}
    if _BLOCK_SUBTYPES in blocks:
        out["subtype_counts"] = {
            str(r["subtype"]): int(r["c"])
            for r in graph.query(
                "SELECT subtype, count(*) AS c FROM okg.graph_nodes "
                "GROUP BY subtype",
                {},
            )
        }
    if _BLOCK_EDGE_TYPES in blocks:
        out["edge_type_counts"] = {
            str(r["edge_type"]): int(r["c"])
            for r in graph.query(
                "SELECT edge_type, count(*) AS c FROM okg.graph_edges "
                "GROUP BY edge_type",
                {},
            )
        }
    if _BLOCK_EMBED_LAG in blocks:
        out["embed_lag"] = int(
            graph.query(
                "SELECT count(*) AS c FROM okg.graph_nodes "
                "WHERE embedding IS NULL",
                {},
            )[0]["c"]
        )
    if _BLOCK_UNRESOLVED_MENTIONS in blocks:
        out["unresolved_mentions"] = _unresolved_mentions_sql(graph)
    return out


def _graph_counts_from_rollup(
    conn: psycopg.Connection, session: Session, blocks: frozenset[str]
) -> tuple[dict[str, Any], str | None]:
    """Read the subtype / edge-type counts from `okg.console_count_rollup`.

    Returns ``(counts, None)`` on success, or ``({}, reason)`` when the rollup
    cannot serve this pin -- the caller then falls back to the whole-graph
    aggregate and REPORTS the reason. The fallback is a declared degradation:
    silently taking the slow path is how a fail-loud read becomes fail-open.

    Three preconditions, any one of which selects the scan:

    * the flag. ``read_console_count_rollup`` does NOT check it itself, so
      without this call a deployment that set ``OKG_IVM_CONSOLE_COUNT_ROLLUP=0``
      as its documented emergency rollback would keep being served rows left
      over from earlier generations. One flag governs maintain AND read.
    * the branch. The rollup read checks out with NO branch (i.e. the default
      branch) while the scan it replaces is branch-correct via
      ``_chronos_checkout``. Off the default branch the rollup would report
      ANOTHER branch's numbers under an authoritative label -- wrong, not stale.
    * availability, signalled by ``ConsoleCountRollupUnavailable``.

    Only that one exception selects the scan. Anything else propagates:
    catching broadly would turn a real database fault into a silent multi-second
    scan.
    """
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupUnavailable,
        read_console_count_rollup,
        require_console_count_rollup_enabled,
    )
    from okg.substrate.graph_store import DEFAULT_BRANCH

    # Read the attribute directly. `getattr(..., None) or DEFAULT_BRANCH` would
    # treat a MISSING branch as "default branch" -- absence read as satisfied,
    # which is the exact failure this guard exists to prevent. `_chronos_checkout`
    # accesses `session.branch_name` unguarded, so it is not optional.
    if session.branch_name != DEFAULT_BRANCH:
        return {}, "non_default_branch"
    try:
        require_console_count_rollup_enabled(
            generation_id=session.generation_id
        )
        counts = read_console_count_rollup(
            conn,
            generation_id=session.generation_id,
            kinds=_ROLLUP_KINDS_FOR_BLOCKS,
        )
    except ConsoleCountRollupUnavailable as exc:
        return {}, str(getattr(exc, "reason", None) or "unavailable")
    out: dict[str, Any] = {}
    if _BLOCK_SUBTYPES in blocks:
        out["subtype_counts"] = {
            value: int(n)
            for (kind, value), n in counts.items()
            if kind == _ROLLUP_KIND_SUBTYPE
        }
    if _BLOCK_EDGE_TYPES in blocks:
        out["edge_type_counts"] = {
            value: int(n)
            for (kind, value), n in counts.items()
            if kind == _ROLLUP_KIND_EDGE_TYPE
        }
    return out, None


def _unresolved_mentions_sql(graph: Any) -> dict[str, Any]:
    """SQL form of `_unresolved_mentions_block` over the whole graph.

    A mention is resolved iff it is the src of at least one mentions/references
    edge. Groups on entity_type; the NULL-entity_type group is counted in the
    totals but omitted from `by_entity_type`, matching the Python block.
    """
    rows = graph.query(
        "SELECT n.attrs->>'entity_type' AS entity_type, count(*) AS total, "
        "count(*) FILTER (WHERE EXISTS ("
        "  SELECT 1 FROM okg.graph_edges e "
        "   WHERE e.src = n.node_id "
        "     AND e.edge_type IN ('mentions','references'))) AS resolved "
        "FROM okg.graph_nodes n "
        "WHERE n.subtype = 'entity_mention' "
        "GROUP BY n.attrs->>'entity_type'",
        {},
    )
    total = sum(int(r["total"]) for r in rows)
    resolved = sum(int(r["resolved"]) for r in rows)
    by_entity_type: dict[str, dict[str, int]] = {}
    for r in rows:
        et = r["entity_type"]
        if et is None:
            continue
        by_entity_type[str(et)] = {
            "total": int(r["total"]),
            "resolved": int(r["resolved"]),
        }
    return {
        "total": total,
        "resolved": resolved,
        "rate_resolved": (resolved / total if total else 0.0),
        "by_entity_type": by_entity_type,
    }


def _graph_total_rows(graph: Any) -> int:
    """Node + edge count at the pin (the enforce-mode work-ceiling probe)."""
    n = int(graph.query("SELECT count(*) AS c FROM okg.graph_nodes", {})[0]["c"])
    e = int(graph.query("SELECT count(*) AS c FROM okg.graph_edges", {})[0]["c"])
    return n + e


def _visible_node_rows(
    conn: psycopg.Connection,
    session: Session,
    *,
    where: str = "",
    params: dict[str, Any] | None = None,
    limit: Optional[int] = None,
    stored_ids: list[str] | None = None,
    candidates: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Node rows at the pin, filtered for this reader. `limit` bounds the
    read (the first rows in node_id order) before the filter runs.
    `stored_ids`, when given, receives the ids of every stored row read,
    hidden ones included (only their ids: what lies past a bound).

    `candidates`, in place of `where`, is a condition over the row alias
    `hit` that picks the rows in an unordered subquery over the same
    pinned relation, cut at `_CLOSE_IDS_PROBE` ids; the read then orders
    those ids and applies `limit` (review P2-4). With no `ORDER BY` inside,
    an index answers the condition (an attrs GIN bitmap) instead of a walk
    of the id index under the outer order. The rows still pass the
    reader's filter below."""
    if candidates is not None:
        if where:
            raise ValueError("_visible_node_rows: pass `where` or `candidates`, not both")
        where = (
            "WHERE node_id = ANY(ARRAY(SELECT hit.node_id FROM okg.graph_nodes hit "
            f"WHERE {candidates} LIMIT {_CLOSE_IDS_PROBE}))"
        )
    graph = _chronos_checkout(conn, session)
    bound = "" if limit is None else f"LIMIT {int(limit)}"
    rows = graph.query(
        f"""
        SELECT node_id, subtype, attrs, text_hash, embedding,
               source, source_record_id, source_revision
          FROM okg.graph_nodes
          {where}
         ORDER BY node_id
         {bound}
        """,
        params or {},
    )
    if stored_ids is not None:
        stored_ids.extend(str(row["node_id"]) for row in rows)
    return filter_node_rows_for_session(conn, session, rows)


def _visible_edge_rows(
    conn: psycopg.Connection,
    session: Session,
    *,
    where: str = "",
    params: dict[str, Any] | None = None,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Edge rows at the pin, filtered for this reader. `limit` bounds the
    scan (unordered: a bounded count needs no order, and ordering a hub's
    edges would sort them all first)."""
    graph = _chronos_checkout(conn, session)
    order = "ORDER BY e.edge_id" if limit is None else f"LIMIT {int(limit)}"
    rows = graph.query(
        f"""
        SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs,
               e.source, e.source_record_id, e.source_revision,
               src.subtype AS src_subtype,
               dst.subtype AS dst_subtype
          FROM okg.graph_edges e
          LEFT JOIN okg.graph_nodes src ON src.node_id = e.src
          LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst
          {where}
         {order}
        """,
        params or {},
    )
    return filter_edge_rows_for_session(conn, session, rows)


def _unresolved_mentions_block(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> dict[str, Any]:
    """W4: report mention-resolution rate at the pin.

    Resolved = mention has at least one outgoing canonicalization
    edge in the checked-out graph. `mentions` is the canonical edge type;
    `references` is counted during the transition because the
    declarative alias-table linker emitted it before the self-play
    evaluation change migrated the linker to `mentions`. Per-entity-type
    breakdown helps operators decide which alias dictionaries to seed.

    Counts are derived from rows already filtered by Nomos source lineage, so
    mention totals cannot leak hidden sources.
    """
    mentions = [row for row in nodes if row.get("subtype") == "entity_mention"]
    resolved_ids = {
        row["src"]
        for row in edges
        if row.get("edge_type") in {"mentions", "references"}
    }
    total = len(mentions)
    resolved = sum(
        1 for row in mentions if row["node_id"] in resolved_ids
    )
    by_entity_type: dict[str, dict[str, int]] = {}
    for row in mentions:
        attrs = row.get("attrs") or {}
        entity_type = attrs.get("entity_type")
        if entity_type is None:
            continue
        slot = by_entity_type.setdefault(
            str(entity_type),
            {"total": 0, "resolved": 0},
        )
        slot["total"] += 1
        if row["node_id"] in resolved_ids:
            slot["resolved"] += 1
    return {
        "total": total,
        "resolved": resolved,
        "rate_resolved": (
            resolved / total if total else 0.0
        ),
        "by_entity_type": by_entity_type,
    }


def get_ontology(
    conn: psycopg.Connection, session: Session,
) -> dict[str, Any]:
    """Catalog view at the session's pinned catalog_version_id.

    Returns subtypes (name + description if present), narrowings
    (parent/edge/child triples), and alias counts. Ontology lookup
    is decoupled from nodes_live state — it's a pure function of
    `catalog_version_id`.
    """
    assert_session_matches(conn, session)
    cv = session.catalog_version_id
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, metadata FROM okg.subtypes "
            "WHERE catalog_version_id = %s ORDER BY name",
            (cv,),
        )
        subtypes = [
            {"name": r[0], "metadata": r[1]} for r in cur.fetchall()
        ]

        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype "
            "FROM okg.narrowings "
            "WHERE catalog_version_id = %s "
            "ORDER BY parent_subtype, edge_type, child_subtype",
            (cv,),
        )
        narrowings = [
            {"parent": r[0], "edge": r[1], "child": r[2]}
            for r in cur.fetchall()
        ]

        cur.execute(
            "SELECT count(*) FROM okg.aliases "
            "WHERE catalog_version_id = %s",
            (cv,),
        )
        alias_count = cur.fetchone()[0]

    return {
        "catalog_version_id": cv,
        "subtypes": subtypes,
        "narrowings": narrowings,
        "alias_count": alias_count,
    }


# Selectable fields for `get_node` — agents can project to a
# subset to keep returns small. `node_id` and `subtype` are
# always-included per the envelope contract.
_GET_NODE_FIELDS: tuple[str, ...] = (
    "node_id", "subtype", "attrs", "text_hash", "parent", "position",
)
_GET_NODE_DEFAULT: tuple[str, ...] = _GET_NODE_FIELDS  # current default = all
#: Fields computed after projection (issue #2675), never read off the row.
_GET_NODE_PASSAGE_FIELDS: frozenset[str] = frozenset({"parent", "position"})
#: Blocks of the node page a `select` may name (issue #3666, lane RD): each
#: is served exactly as the page with no `select` serves it. Agents selected
#: `live_relations` to get the listing without the attributes, and
#: `provenance`, both printed as blocks of the page.
NODE_PAGE_BLOCKS: tuple[str, ...] = ("live_relations", "provenance")
#: What `select` names on inspect(node) and inspect(nodes), besides
#: `attrs.<key>` (one stored attribute, as search takes it).
INSPECT_NODE_SELECT: tuple[str, ...] = (*_GET_NODE_FIELDS, *NODE_PAGE_BLOCKS)
#: An `attrs.<key>` an inspect(node) select does not narrow to: the record's
#: text is read with inspect(kind=text), which the refusal names.
_NODE_TEXT_NAMES = frozenset({"attrs.text"})


def node_select_parts(
    select: Optional[list[str]],
) -> tuple[Optional[list[str]], frozenset[str], list[str]]:
    """`(row fields, page blocks, attribute keys)` of an inspect(node) select.

    Pure. The row fields are what the row is projected to: `attrs` joins
    them when an `attrs.<key>` is named, and the attributes are then
    narrowed to the named keys (unless `attrs` itself was named). None
    stays None: the whole page."""
    if select is None:
        return None, frozenset(), []
    from okg.substrate.mcp.select_vocabulary import attr_path_key

    fields: list[str] = []
    blocks: set[str] = set()
    keys: list[str] = []
    for name in select:
        key = attr_path_key(name) if name not in _NODE_TEXT_NAMES else None
        if key is not None:
            if key not in keys:
                keys.append(key)
        elif name in NODE_PAGE_BLOCKS:
            blocks.add(name)
        elif name not in fields:
            fields.append(name)
    if keys and "attrs" not in fields:
        fields.append("attrs")
    return fields, frozenset(blocks), keys


def validate_node_select(
    select: Optional[list[str]], *, tool_name: str, text_node_id: Optional[str] = None,
) -> None:
    """Refuse an inspect(node) or inspect(nodes) `select` name that is no
    field, page block or `attrs.<key>` of the record read."""
    if select is None:
        return
    _, _, keys = node_select_parts(select)
    validate_select(
        select, (*INSPECT_NODE_SELECT, *(f"attrs.{key}" for key in keys)),
        tool_name=tool_name, text_node_id=text_node_id,
    )


def _narrow_node_attrs(result: dict[str, Any], keys: list[str], whole: bool) -> None:
    """The served record's `attrs` cut to the selected keys, in place.

    A key the record does not store is named in `attrs_not_stored`, beside
    the keys it has, so an empty `attrs` never reads as an answer. A masked
    or withheld map (not a mapping) is left as it is, and the redaction
    marker always travels."""
    attrs = result.get("attrs")
    if not keys or not isinstance(attrs, Mapping):
        return
    if not whole:
        kept = [*keys, REDACTION_MARKER]
        result["attrs"] = {key: attrs[key] for key in kept if key in attrs}
    missing = [key for key in keys if key not in attrs]
    if missing:
        result["attrs_not_stored"] = [f"attrs.{key}" for key in missing]
        result["attr_keys"] = sorted(
            str(key) for key in attrs if key != REDACTION_MARKER
        )[:24]


def _passage_context(
    conn: psycopg.Connection,
    session: Session,
    *,
    node_id: str,
    subtype: str,
    attrs: Any,
    provenance: Any,
    select: Optional[list[str]],
) -> dict[str, Any]:
    """`parent` and `position` for inspect(node) of a passage child, else `{}`.

    Only a subtype the pinned catalog declares as a passage child gets either
    key, and only that case pays for the node segment and branch point: the
    declaration is read first, so inspecting any other node costs one catalog
    lookup and can never fail on a segment-registry problem it does not use.
    The segment is the SEMANTIC resolver's, never the BM25 index's, so a
    missing lexical index cannot refuse an inspect. `parent` is the exact block
    search emits (pinned, Nomos-filtered, fenced by the parent's own trust).
    """
    wanted = (
        set(_GET_NODE_PASSAGE_FIELDS)
        if select is None
        else _GET_NODE_PASSAGE_FIELDS & set(select)
    )
    if not wanted or not subtype:
        return {}
    from okg.substrate.mcp.passage_rollup import (
        fence_position_heading,
        passage_child_declarations,
        passage_position,
        resolve_passage_parents,
    )

    declarations = passage_child_declarations(
        conn,
        catalog_version_id=session.catalog_version_id,
        child_subtypes=[subtype],
    )
    if subtype not in declarations:
        return {}
    out: dict[str, Any] = {}
    if "parent" in wanted:
        from okg.substrate.bm25_node_search import branch_point
        from okg.substrate.mcp.search import (
            _citation_from_attrs,
            passage_counts_withheld,
        )
        from okg.substrate.mcp.semantic_readiness import resolve_node_segment

        withheld = passage_counts_withheld(session)
        blocks, envelope = resolve_passage_parents(
            conn,
            session=session,
            page=[{"node_id": node_id, "subtype": subtype}],
            node_segment=resolve_node_segment(conn),
            branch_point_value=branch_point(conn, session),
            citation_from_attrs=_citation_from_attrs,
            withhold_passage_counts=withheld,
        )
        out["parent"] = blocks.get(node_id)
        if withheld:
            # Only when it changes the reading: a null count is then
            # "withheld", never "over the cap". Search states this once per
            # page; inspect has no page, so it rides on the result.
            out["parent_relation"] = {
                "passage_count": envelope["passage_count"],
            }
    if "position" in wanted:
        out["position"] = passage_position(attrs)
        out["provenance"] = provenance
        fence_position_heading(out, attrs=attrs)
        del out["provenance"]
    return out


def inspect_node(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    select: Optional[list[str]] = None,
    hydrate: Any = None,
    principal: Optional[str] = None,
    probe_gate: Any = None,
    probe_runners: Any = None,
    include_documents: bool = True,
    record_text_bytes: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """Fetch one node at the pin. Returns None if not visible at
    this generation (never projected, or already retired).

    `select` (optional) lists the fields to return per the row.
    `node_id` and `subtype` are always included. Default returns
    all fields. The response also carries `generation_id` and
    `pinned_generation_id` (the latter is the canonical envelope
    name; the former is preserved for backward compat).

    `hydrate` (external-backed nodes, issue #83) is OPT-IN and
    backward-compatible: None/False returns only the pinned view (no
    fetch). When set (True for all backed fields, or a list of field
    names), the resolver hydrates `current` for those fields and
    attaches an `external_live` overlay labeled OUTSIDE the pin. A
    `probe` field passes a deny-by-default gate first. Hydration writes
    only the non-Chronos cache; the pinned graph is never mutated by the
    read. `search`/`expand` deliberately do not hydrate.
    """
    # `select` may also name a block of the page (`live_relations`,
    # `provenance`) and `attrs.<key>` (issue #3666, lane RD): the row is
    # projected to its fields, a named block is served as the whole page
    # serves it, and `attrs` is narrowed to the named keys.
    validate_node_select(select, tool_name="inspect(node)", text_node_id=node_id)
    caller_select = select
    select, page_blocks, attr_keys = node_select_parts(select)
    if record_text_bytes is None:
        record_text_bytes = RECORD_THREAD_NODE_BYTES
    assert_session_matches(conn, session)
    # inspect(node) executes by lowering a TransformProgram over the nodes_live
    # registered surface (akmon.inspect.get_node) rather than hand-built SQL.
    rows = lower_program_over_chronos(
        node_by_id_program(),
        conn,
        generation_id=session.generation_id,
        branch=session.branch_name,
        extra_params={"focal": node_id},
    )
    if not rows:
        return None
    rows = filter_node_rows_for_session(conn, session, rows)
    if not rows:
        return None
    row = rows[0]
    raw_attrs = row["attrs"] or {}
    effective_attrs = effective_attrs_for_target(
        conn,
        target_kind="node",
        target_id=node_id,
        generation_id=session.generation_id,
        raw_attrs=raw_attrs,
    )
    full_row = {
        "node_id": row["node_id"],
        "subtype": row["subtype"],
        "attrs": effective_attrs,
        **({"text_hash": row["text_hash"]} if "text_hash" in row else {}),
    }
    projected = project_row(
        full_row, select, default=_GET_NODE_DEFAULT,
    )
    _narrow_node_attrs(
        projected, attr_keys, whole=caller_select is not None and "attrs" in caller_select,
    )
    # Read-side trust boundary (PACT fence-untrusted-node-text):
    # classify the node's origin and, for untrusted sources, fence its
    # string attrs + attach a provenance block before it reaches the agent.
    from okg.substrate.mcp.provenance import serialize_node_read_rows

    fenced = serialize_node_read_rows(
        conn,
        [{**projected, "source": row.get("source")}],
        deployment_name=session.deployment_name,
        # Trust reads the full attrs even when `select` drops them (#2689).
        marker_attrs=[effective_attrs],
    )[0]
    result = {
        **fenced,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
    }
    # PACT read-names-list-3666: the id a reader cites, whatever `select`
    # names, read from the attrs the reader may see and fenced by trust.
    from okg.substrate.mcp.citation_id import (
        CITATION_KEY,
        citation_field,
        fenced_citation,
    )

    citation = fenced_citation(citation_field(effective_attrs), fenced.get("provenance"))
    if citation:
        result[CITATION_KEY] = citation
    from okg.substrate.catalog.linker_config import (
        LOAD_STATE_ATTR,
        NOT_LOADED,
        is_link_endpoint_row,
    )

    if is_link_endpoint_row({"source": row.get("source"), "attrs": raw_attrs}):
        # Issue #3666: a record cited by key that the graph does not hold.
        # Only readable with no access rules (the read filter hides it).
        from okg.substrate.mcp.link_targets import (
            LOAD_NOTE_KEY,
            endpoint_load_note,
        )

        result[LOAD_STATE_ATTR] = NOT_LOADED
        result[LOAD_NOTE_KEY] = endpoint_load_note(raw_attrs)
    # External-backed hydration (opt-in). The overlay is "as of now", attached
    # outside the pin; the resolver writes only the non-Chronos cache.
    if hydrate:
        from okg.substrate.external_backed.resolver import hydrate_node

        overlay = hydrate_node(
            conn,
            deployment_name=session.deployment_name,
            node_id=node_id,
            attrs=raw_attrs,
            live_fields=hydrate,
            principal=principal,
            gate=probe_gate,
            probe_runners=probe_runners,
        )
        if overlay is not None:
            result["external_live"] = overlay
    # Issue #2675: a passage child carries the same compact parent summary and
    # position a search row does, so inspecting a chunk is not followed by a
    # second call to learn what record it belongs to.
    result.update(
        _passage_context(
            conn,
            session,
            node_id=str(row["node_id"]),
            subtype=str(row.get("subtype") or ""),
            attrs=effective_attrs,
            provenance=result.get("provenance"),
            select=select,
        )
    )
    # Issue #2013: the live relations this NODE's subtype declares, so an
    # agent that inspected a node can write `expand(live=...)` without a
    # second round trip to `inspect(kind=subtype)`.
    node_subtype = row.get("subtype")
    if node_subtype:
        from okg.substrate.mcp.subtype_summary import catalog_subtype_shape

        shape = catalog_subtype_shape(conn, session, str(node_subtype))
        declared = shape.get("live_relations")
        if declared and (select is None or "live_relations" in page_blocks):
            # A `select` names the fields it wants: a selected read leaves the
            # listing out unless it names `live_relations`, and then serves
            # the very block the page with no select serves (#3666 lane RD).
            # inspect(kind=subtype) lists the relations with their arguments.
            result["live_relations"] = declared
        elif "live_relations" in page_blocks:
            from okg.substrate.mcp.response_bounds import note_adjusted

            note_adjusted([{
                "rule": "select_page_block",
                "sent": "select `live_relations`",
                "served": "no listing: this record's type declares no live relations",
            }])
        # Issue #2669: the documents derived from this node, so an agent
        # that found a `file:` node (whose own text is only its path) learns
        # that a `doc:` node with readable chunks exists. The `derived_from`
        # edge points INTO this node, so a default outgoing `expand` never
        # shows it. The lookup runs only when the catalog declares
        # `document derived_from <this subtype>`: other subtypes pay nothing,
        # and a hub node's incoming edges are never scanned for it.
        if include_documents and _declares_derived_documents(shape):
            documents, truncated = _derived_documents(conn, session, node_id)
            if documents:
                result["documents"] = documents
                if truncated:
                    result["documents_truncated"] = True
        # Issue #3467: a record whose subtype declares passages (a ticket's
        # comments, a document's chunks) keeps them out of this read. Say how
        # many there are and which read returns them, so "complete" is not
        # taken to mean the comments do not exist. Counted by the same
        # visibility rule as expand's total_count.
        passages = (
            _declared_passages(conn, session, node_id, str(node_subtype))
            if include_documents else None
        )
        if passages is not None:
            result[PASSAGES_NOT_SHOWN_KEY] = passages
            if select is None:
                # PACT read-passage-rows-3666 (R6): a ticket-shaped record
                # also carries its thread: fields, the description once, then
                # the dated comments, the newest kept when it is cut.
                from okg.substrate.mcp.record_text import (
                    RECORD_TEXT_KEY,
                    _json_bytes,
                    fit_record_text,
                    record_thread,
                )

                thread = record_thread(conn, session, node_id)
                if thread is not None:
                    fitted = fit_record_text(
                        thread,
                        lambda candidate: _json_bytes(candidate) <= record_text_bytes,
                    )
                    if fitted is not None:
                        result[RECORD_TEXT_KEY] = fitted
    return result


#: Bytes of JSON a node read's ticket thread may take (`record_text`).
RECORD_THREAD_NODE_BYTES = 24_000


#: The node-read key for declared passages the read did not include.
PASSAGES_NOT_SHOWN_KEY = "passages_not_shown"


#: Counts in the #3467 notices stop here and say `lower_bound` past it.
_NOTICE_COUNT_CAP = 1_000


def _counts_disclose_nothing(session: Session) -> bool:
    """Whether an omission count may be read: only with no Nomos runtime.
    Under audit the authorized read records a decision per counted edge the
    reader never receives; under enforce a count would also reflect rows the
    reader may not see. Either way no count is read: the notice says only
    what the catalog or the call shape implies (count null, `not_counted`)."""
    return not deployment_requires_nomos_runtime(session.deployment_name)


def _capped_edge_count(
    conn: psycopg.Connection,
    session: Session,
    *,
    where: str,
    params: dict[str, Any],
) -> int:
    """Edges matching `where` (aliases `e`, `src`, `dst`) at the pin, read
    through the authorized edge read up to `_NOTICE_COUNT_CAP` + 1 rows (one
    bounded scan, never a full count). Callers check
    `_counts_disclose_nothing` first."""
    return len(_visible_edge_rows(
        conn, session, where=f"WHERE {where}", params=params,
        limit=_NOTICE_COUNT_CAP + 1,
    ))


def _passage_declaration(
    conn: psycopg.Connection, session: Session, subtype: str,
) -> Optional[tuple[str, str]]:
    """`(edge_type, child_subtype)` the subtype declares, or None (one read)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT metadata -> 'passage_rollup' FROM okg.subtypes "
            "WHERE catalog_version_id = %s AND name = %s",
            (session.catalog_version_id, subtype),
        )
        row = cur.fetchone()
    rollup = row[0] if row else None
    if not isinstance(rollup, dict):
        return None
    edge_type, child = rollup.get("edge_type"), rollup.get("child_subtype")
    if not edge_type or not child:
        return None
    return str(edge_type), str(child)


def _declared_passages(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    subtype: str,
) -> Optional[dict[str, Any]]:
    """`{count, count_basis, edge_type, child_subtype, read}` when `subtype`
    declares a passage relation and this node has passages, else None.

    Counts children of the declared child subtype only (a document also
    contains figures and symbols). The count is read only with Nomos off,
    for the reason the incoming count is (`_counts_disclose_nothing`); with
    a runtime the notice comes from the declaration alone, `count` null and
    basis `not_counted`, on every node of the subtype. `read` is the call
    that returns them: the text read for document chunks, which it gathers;
    an outgoing expand on the declared edge for any other child (a commit's
    file diffs), which the text read does not return."""
    declared = _passage_declaration(conn, session, subtype)
    if declared is None:
        return None
    edge_type, child = declared
    counted = _counts_disclose_nothing(session)
    count = _capped_edge_count(
        conn, session,
        where="e.src = :focal AND e.edge_type = :etype AND dst.subtype = :child",
        params={"focal": node_id, "etype": edge_type, "child": child},
    ) if counted else None
    if counted and not count:
        return None
    read = (
        f'inspect(target={{"kind":"text","node_id":{json.dumps(node_id)}}})'
        if child == "document_chunk"
        else (
            f'expand(input={{"node_id":{json.dumps(node_id)}}}, '
            f'edge_type="{edge_type}", direction="out")'
        )
    )
    if count is None:
        # A Nomos runtime: no edge is read for the notice, so it depends on
        # the catalog alone and says the same for a record with no passages,
        # hidden ones, or readable ones (issue #3467).
        basis: dict[str, Any] = {"count": None, "count_basis": "not_counted"}
    else:
        basis = {
            "count": min(count, _NOTICE_COUNT_CAP),
            "count_basis": "lower_bound" if count > _NOTICE_COUNT_CAP else "exact",
        }
    return {
        **basis,
        "edge_type": edge_type,
        "child_subtype": child,
        "read": read,
    }


def text_twin_document(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
) -> Optional[str]:
    """The readable `document` that holds `node_id`'s text, or None (issue
    #3467): a document that directly contains the node, else the document
    derived from the node, else the one derived from the `source_file` that
    contains it. Only a document with readable chunks is named; every edge
    and node passes the session's read filter first, so a hidden document is
    never named."""
    containers = _visible_edge_rows(
        conn, session,
        where="WHERE e.dst = :focal AND e.edge_type = 'contains' "
              "AND src.subtype IN ('document', 'source_file')",
        params={"focal": node_id},
    )
    direct_docs = sorted(
        str(edge["src"]) for edge in containers if edge.get("src_subtype") == "document"
    )
    files = sorted(
        str(edge["src"]) for edge in containers if edge.get("src_subtype") == "source_file"
    )
    enforcing = deployment_nomos_enforcement_mode(session.deployment_name) == "enforce"
    visible_direct = {
        str(row["node_id"]) for row in _visible_node_rows(
            conn, session, where="WHERE node_id = ANY(:ids)", params={"ids": direct_docs},
        )
    } if direct_docs else set()
    # The first document with chunks, in this order; later ones are not read.
    for doc_id in direct_docs:
        if doc_id in visible_direct:
            chunks, _spent = _document_chunk_count(
                conn, session, doc_id, enforcing=enforcing,
                budget=_NEIGHBORS_EXACT_COUNT_BUDGET,
            )
            if chunks:
                return doc_id
    for file_id in [node_id, *files]:
        documents, _ = _derived_documents(conn, session, file_id)
        for document in documents:
            if document.get("chunk_count"):
                return str(document["node_id"])
    return None


_DERIVED_DOCUMENTS_LIMIT = 10


def _declares_derived_documents(shape: Mapping[str, Any]) -> bool:
    return any(
        route.get("edge_type") == "derived_from"
        and route.get("src_subtype") == "document"
        for route in shape.get("declared_edges_in") or ()
    )


def _derived_documents(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
) -> tuple[list[dict[str, Any]], bool]:
    """The `document` nodes with a `derived_from` edge into `node_id`.

    Returns `(rows, truncated)`: at most `_DERIVED_DOCUMENTS_LIMIT` rows of
    `{node_id, chunk_count}` ordered by node_id, plus `chunk_count_basis` on
    a row whose count is withheld. Both reads go through the filtered row
    helpers, so a document is listed only when its edge and its node pass the
    session's read filter, and the list is cut to size only AFTER the filter:
    hidden rows can neither crowd out visible ones nor set `truncated` on
    their own. The candidate set is this node's incoming `derived_from`
    edges from documents, read by the indexed `dst` lookup; the caller runs
    this only for subtypes the catalog declares as such a target. Chunk
    counts follow `_neighbors_match_count`'s rule, with one exact-count
    budget shared by the whole call.
    """
    edges = _visible_edge_rows(
        conn,
        session,
        where=(
            "WHERE e.dst = :focal AND e.edge_type = 'derived_from' "
            "AND src.subtype = 'document'"
        ),
        params={"focal": node_id},
    )
    doc_ids = sorted({str(edge["src"]) for edge in edges})
    if not doc_ids:
        return [], False
    visible_docs = [
        str(doc["node_id"])
        for doc in _visible_node_rows(
            conn,
            session,
            where="WHERE node_id = ANY(:ids)",
            params={"ids": doc_ids},
        )
    ]
    truncated = len(visible_docs) > _DERIVED_DOCUMENTS_LIMIT
    enforcing = (
        deployment_nomos_enforcement_mode(session.deployment_name) == "enforce"
    )
    budget = _NEIGHBORS_EXACT_COUNT_BUDGET
    documents: list[dict[str, Any]] = []
    for doc_id in visible_docs[:_DERIVED_DOCUMENTS_LIMIT]:
        count, spent = _document_chunk_count(
            conn, session, doc_id, enforcing=enforcing, budget=budget,
        )
        budget -= spent
        row: dict[str, Any] = {"node_id": doc_id, "chunk_count": count}
        if count is None:
            row["chunk_count_basis"] = "omitted_over_budget_under_enforce"
        documents.append(row)
    return documents, truncated


def _document_chunk_count(
    conn: psycopg.Connection,
    session: Session,
    doc_id: str,
    *,
    enforcing: bool,
    budget: int,
) -> tuple[Optional[int], int]:
    """Count one document's chunks by `_neighbors_match_count`'s rule.

    A chunk is `document --contains--> document_chunk`, the edge the read
    route follows (`expand(doc, path=["contains:out"],
    target_subtype="document_chunk")`), so the count equals the rows that
    read returns. Code symbols and visual artifacts are also contained in a
    document; the subtype condition keeps them out. `(src, edge_type)` is
    indexed.

    Returns `(count, rows_spent)`. Under off/audit nothing can hide, so the
    SQL count is exact and spends nothing. Under enforce that raw count only
    sizes the budget: the reported count comes from rows that passed the
    filtered row helpers, counting a chunk only when both its `contains` edge
    and the chunk node are visible (the two checks the `contains:out` read
    applies to each path). When this document's edges exceed what is left of
    the shared budget, the count is withheld (None) rather than guessed.
    """
    graph = _chronos_checkout(conn, session)
    raw_total = int(
        graph.query(
            "SELECT count(*) AS c FROM okg.graph_edges e "
            "JOIN okg.graph_nodes n ON n.node_id = e.dst "
            "WHERE e.src = :doc AND e.edge_type = 'contains' "
            "AND n.subtype = 'document_chunk'",
            {"doc": doc_id},
        )[0]["c"]
    )
    if not enforcing:
        return raw_total, 0
    if raw_total > budget:
        return None, 0
    edges = _visible_edge_rows(
        conn,
        session,
        where=(
            "WHERE e.src = :doc AND e.edge_type = 'contains' "
            "AND dst.subtype = 'document_chunk'"
        ),
        params={"doc": doc_id},
    )
    chunk_ids = sorted({str(edge["dst"]) for edge in edges})
    if not chunk_ids:
        return 0, raw_total
    visible_chunks = _visible_node_rows(
        conn,
        session,
        where="WHERE node_id = ANY(:ids)",
        params={"ids": chunk_ids},
    )
    return len(visible_chunks), raw_total


# `list_neighbors` returns one row per edge. The row carries
# both `src` and `dst` plus a per-row `direction` ("out" |
# "in") so the contract is uniform across direction modes:
# the agent derives the neighbor id from src/dst + direction.
# Per the MCP design doc's external feedback (2026-04-27): the
# prior outbound-only shape mis-rendered "what references this
# node" queries — `list_neighbors(persona)` returned [] because
# personas have no outbound edges, when the agent meant "what
# uses this persona". Adding `direction='in' | 'both'` closes
# the gap without breaking outbound callers (default stays
# `"out"`; legacy `dst` still surfaces the neighbor for that
# default).
_LIST_NEIGHBORS_FIELDS: tuple[str, ...] = (
    "edge_id", "src", "dst", "edge_type", "attrs", "read_provenance",
    "direction",
)
_LIST_NEIGHBORS_DEFAULT: tuple[str, ...] = _LIST_NEIGHBORS_FIELDS
#: Where the node fields agents ask a 1-hop `expand` for live (PACT
#: select-field-instructions): its rows are edges, so a refusal names the
#: edge field, the 1-hop filter, or the path read that has them. Nothing is
#: accepted in their place.
_EXPAND_NODE_FIELD_HINT = (
    "1-hop expand rows are edges: the neighbour's id is dst (src for "
    "direction='in'), and each row's neighbor gives its subtype and title; "
    "path and depth>1 rows carry node_id, subtype, attrs, "
    "provenance; read other node fields with inspect or search"
)
EXPAND_SELECT_ELSEWHERE: dict[str, str] = {
    "node_id": _EXPAND_NODE_FIELD_HINT,
    "neighbor": (
        "neighbor (the far end's subtype and title) comes on every 1-hop row "
        "whatever select names; it is not a select field"
    ),
    "id": _EXPAND_NODE_FIELD_HINT,
    "subtype": (
        "1-hop expand rows are edges: each row's neighbor.subtype names the "
        "neighbour's type; keep neighbours of one type with "
        'filter={"op":"eq","field":"subtype","value":"<type>"}; path and '
        "depth>1 rows carry subtype"
    ),
    "handle": _EXPAND_NODE_FIELD_HINT,
    "excerpt": _EXPAND_NODE_FIELD_HINT,
    "citation": _EXPAND_NODE_FIELD_HINT,
}
_LIST_NEIGHBORS_ALWAYS: tuple[str, ...] = (
    "edge_id", "src", "dst", "edge_type", "direction",
)
_LIST_NEIGHBORS_ALLOWED_DIRECTIONS: frozenset[str] = frozenset({
    "out", "in", "both",
})

# Budget for the exact neighbor count under Nomos `enforce`, mirroring
# aggregate_edge_routes' `_AGGREGATE_EDGE_EXACT_ROW_BUDGET`: above it the
# count is omitted rather than leaked or computed at blowup cost.
_NEIGHBORS_EXACT_COUNT_BUDGET = 5_000


def _neighbors_where(direction: str) -> str:
    """The `expand` anchor predicate, in the shape `neighbors_program` lowers
    (see transform/operators.py). The count probe below and the page query
    must range over the same edges or `total_count` would disagree with the
    rows it counts."""
    if direction == "out":
        return "e.src = :focal"
    if direction == "in":
        return "e.dst = :focal"
    return "(e.src = :focal OR e.dst = :focal)"


def _neighbors_match_count(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str],
    direction: str,
) -> tuple[Optional[int], str]:
    """The true number of edges `expand` would return for this anchor.

    Returns `(count, basis)`. Visibility follows the issue-#627 rule already
    used by `aggregate_edge_routes`: under `off`/`audit` no row can hide, so a
    SQL COUNT is exact and cheap. Under `enforce` an exact SQL count would
    disclose the cardinality of rows the reader may not see, so the matching
    edges are materialized (only within the budget) and run through the same
    per-row lineage filter; above the budget the count is omitted with a stated
    reason rather than guessed at.
    """
    params: dict[str, Any] = {"focal": node_id}
    where = _neighbors_where(direction)
    if edge_type is not None:
        where += " AND e.edge_type = :etype"
        params["etype"] = edge_type
    graph = _chronos_checkout(conn, session)
    raw_total = int(
        graph.query(
            f"SELECT count(*) AS c FROM okg.graph_edges e WHERE {where}",
            params,
        )[0]["c"]
    )
    if deployment_nomos_enforcement_mode(session.deployment_name) != "enforce":
        return raw_total, "exact"
    if raw_total > _NEIGHBORS_EXACT_COUNT_BUDGET:
        return None, "omitted_over_budget_under_enforce"
    rows = graph.query(
        "SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source, "
        "e.source_record_id, e.source_revision "
        f"FROM okg.graph_edges e WHERE {where} ORDER BY e.edge_id",
        params,
    )
    return len(filter_edge_rows_for_session(conn, session, rows)), "exact_visible"


def expand_neighbors(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str] = None,
    direction: str = "out",
    limit: int = 100,
    offset: int = 0,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """List edges incident on `node_id` at the pin.

    Decision-shaped envelope:
      - `neighbors`: list of edge rows
      - `returned_count`: rows in this page
      - `total_count`: the true number of matching edges at the pin —
        not the page size. `None` only when Nomos `enforce` puts an
        exact count out of budget, in which case `total_count_basis`
        says so (PACT fix-expand-pagination-contract, issue #904)
      - `total_count_basis`: how `total_count` was established
        (`exact`, `exact_visible`, or `omitted_over_budget_under_enforce`)
      - `offset` / `limit` / `next_offset`: the cursor. A truncated page
        always names the call that continues it; `next_offset` is `None`
        on the last page
      - `truncated`: true iff more raw rows exist beyond this page
        (pre-Nomos-filter, so a hidden row cannot mislabel the page
        as complete; under enforce a page may under-fill — fewer
        than `limit` rows with truncated=True)
      - `pinned_generation_id`: the generation actually queried

    `offset` pages this same result set. It is a paging parameter only:
    every page of one neighborhood is served by this function, so no page
    can differ in shape from the one before it (issue #903).

    `direction`:
      - `"out"` (default): edges whose `src = node_id`. The
        legacy shape; `dst` is the neighbor.
      - `"in"`: edges whose `dst = node_id`. The neighbor is
        `src`. Use this for "what references this node?".
      - `"both"`: union of the two. Each row carries
        `direction="out"` or `direction="in"` so the agent can
        tell which side the edge points; for self-loops
        (src == dst == node_id) `direction="out"` is reported
        arbitrarily.

    `select` (optional) projects each edge row to a subset of
    `edge_id, src, dst, edge_type, attrs, read_provenance, direction`.
    `edge_id`, `src`, `dst`, `edge_type`, and `direction` are always
    included. `attrs` is the large column callers can omit to save tokens;
    when `attrs` is returned, `read_provenance` travels with it.
    """
    if direction not in _LIST_NEIGHBORS_ALLOWED_DIRECTIONS:
        raise SubstrateError(
            f"expand: direction must be one of "
            f"{sorted(_LIST_NEIGHBORS_ALLOWED_DIRECTIONS)}; "
            f"got {direction!r}",
            data={
                "direction": direction,
                "allowed": sorted(_LIST_NEIGHBORS_ALLOWED_DIRECTIONS),
            },
        )
    validate_select(
        select, _LIST_NEIGHBORS_FIELDS, tool_name="expand",
        elsewhere=EXPAND_SELECT_ELSEWHERE,
    )
    pin = session.generation_id
    # Routed read (PACT chronos-native-graph-cutover, IR-as-seam): the expand
    # operator executes by lowering a TransformProgram over the registered
    # edges_live surface (bound to okg.graph_edges) and running it through the
    # pinned Chronos checkout session. The lowered SELECT projects edge columns;
    # the 'out'/'in' direction label is computed caller-side (not a stored column).
    # Validate, never clamp (the tool boundary checks too, but this is a
    # public library entry point). `limit < 1` is the dangerous one: it makes
    # `truncated` true with a `consumed` of 0, so `next_offset == offset` and
    # the documented follow-the-cursor loop never terminates.
    offset, limit = int(offset), int(limit)
    if offset < 0:
        raise SubstrateError(
            "expand: offset must be >= 0",
            data={"offset": offset},
        )
    if limit < 1:
        raise SubstrateError(
            "expand: limit must be >= 1 (a non-positive page size yields a "
            "cursor that cannot advance)",
            data={"limit": limit},
        )
    graph = _chronos_checkout(conn, session)
    raw_rows = execute_list_neighbors_chronos(
        graph,
        node_id=node_id,
        edge_type=edge_type,
        direction=direction,
        limit=limit + 1,
        offset=offset,
    )
    # `truncated` is computed from the PRE-filter fetch (issue #627): a row
    # hidden by Nomos inside the window must not mislabel the page as
    # complete. Under enforce a page may therefore under-fill (fewer than
    # `limit` rows with truncated=True) — re-call with a higher limit.
    truncated = len(raw_rows) > limit
    rows = filter_edge_rows_for_session(conn, session, raw_rows)[:limit]
    # A page keeps its fill under enforce: a row hidden inside the window lets
    # the next raw row take its place. That means the page can consume MORE
    # raw positions than it returns rows, so the cursor must advance by raw
    # positions consumed — `offset + limit` would re-serve the row that was
    # pulled forward. `consumed` is the raw index just past the last row this
    # page emitted; with nothing hidden it is exactly `limit`.
    if rows:
        last_edge_id = rows[-1]["edge_id"]
        consumed = 1 + next(
            i for i, r in enumerate(raw_rows)
            if r["edge_id"] == last_edge_id
        )
    else:
        consumed = min(len(raw_rows), limit)
    full_rows = [
        {
            "edge_id": r["edge_id"],
            "src": r["src"], "dst": r["dst"],
            "edge_type": r["edge_type"], "attrs": r["attrs"],
            "source": r.get("source"),
            "direction": ("out" if r["src"] == node_id else "in"),
        }
        for r in rows
    ]
    full_rows = apply_effective_attrs_to_rows(
        conn,
        full_rows,
        target_kind="edge",
        target_id_field="edge_id",
        generation_id=pin,
    )
    from okg.substrate.mcp.provenance import serialize_edge_read_rows

    full_rows = serialize_edge_read_rows(
        conn,
        full_rows,
        deployment_name=session.deployment_name,
    )
    neighbors = []
    for row in full_rows:
        projected = project_row(
            row, select,
            default=_LIST_NEIGHBORS_DEFAULT,
            always=_LIST_NEIGHBORS_ALWAYS,
        )
        if "attrs" in projected and "read_provenance" not in projected:
            projected["read_provenance"] = row.get("read_provenance")
        neighbors.append(projected)
    attach_neighbor_summaries(conn, session, full_rows, neighbors)
    # `total_count` is the true match count, not the page size (issue #904).
    # Under off/audit a complete page needs no second query — nothing can
    # hide, so the matches are exactly what was fetched, offset included.
    # Under enforce that shortcut is wrong twice over: `offset` counts RAW
    # positions while the page counts VISIBLE rows, so adding them reports the
    # pre-filter cardinality — the very number the enforce branch exists to
    # withhold — and it would disagree with the count every earlier page of
    # the same walk reported. So under enforce the count always comes from the
    # filtered probe, whether or not this page truncated.
    enforcing = (
        deployment_nomos_enforcement_mode(session.deployment_name) == "enforce"
    )
    if truncated or enforcing:
        total_count, total_basis = _neighbors_match_count(
            conn, session, node_id,
            edge_type=edge_type, direction=direction,
        )
    else:
        total_count, total_basis = offset + len(raw_rows), "exact"
    payload = {
        "node_id": node_id,
        "direction": direction,
        "neighbors": neighbors,
        "returned_count": len(neighbors),
        "total_count": total_count,
        "total_count_basis": total_basis,
        "offset": offset,
        "limit": limit,
        # Explicit, and `None` on the last page — the shape `filter`, `map`,
        # and `search` already use. add_pagination_hint then adds the
        # `pagination` block and the prose hint on top of it.
        "next_offset": offset + consumed if truncated else None,
        "truncated": truncated,
        "generation_id": pin,
        "pinned_generation_id": pin,
    }
    if total_count is None:
        payload["note"] = (
            "matching edges exceed the exact-count budget under Nomos "
            "enforce mode; total_count is omitted so hidden-source "
            "cardinality cannot leak — narrow edge_type or page through "
            "the result set"
        )
    # Issue #3666: how many cited records are only known by key.
    payload.update(expand_link_rows_link_targets(
        session, neighbors, complete=offset == 0 and not truncated,
    ))
    if direction == "out" and offset == 0 and not _counts_disclose_nothing(session):
        # Issue #3467: with a Nomos runtime no edge is read for the notice
        # (`_counts_disclose_nothing`), so every outgoing first page says the
        # same thing whether or not incoming links exist, readable or not.
        payload[INCOMING_NOT_SHOWN_KEY] = {
            "count": None, "count_basis": "not_counted", "direction": "in",
        }
    elif direction == "out" and offset == 0:
        # Issue #3467: the default outgoing read says how many incoming links
        # it left out, so an answer that sits on a backlink (a ticket that
        # references this one) is not mistaken for absent. Capped, so a hub's
        # in-degree costs at most one bounded scan.
        incoming = _capped_edge_count(
            conn, session,
            where="e.dst = :focal AND e.src <> :focal"
            + (" AND e.edge_type = :etype" if edge_type is not None else ""),
            params={"focal": node_id, **({"etype": edge_type} if edge_type else {})},
        )
        if incoming:
            payload[INCOMING_NOT_SHOWN_KEY] = {
                "count": min(incoming, _NOTICE_COUNT_CAP),
                "count_basis": (
                    "lower_bound" if incoming > _NOTICE_COUNT_CAP else "exact"
                ),
                "direction": "in",
            }
    return payload


#: The page key for incoming links an outgoing `expand` did not read.
INCOMING_NOT_SHOWN_KEY = "incoming_not_shown"


# --------------------------------------------------------------------------- #
# 1-hop rows as the linked records (issue #3666)                               #
# --------------------------------------------------------------------------- #

#: What a `select` may name on a 1-hop row that is a linked record: the
#: record's own fields, the link that reached it, and the link's old field
#: names (`edge_id`, `edge_type` and `direction` are always in `link`; `src`
#: and `dst` join them when named).
EXPAND_RECORD_FIELDS: tuple[str, ...] = (
    "node_id", "subtype", "title", "attrs", "provenance",
    "link", "link.attrs", "link_count",
    "edge_id", "src", "dst", "edge_type", "direction",
)
#: The keys every record row has, whatever `select` names.
EXPAND_RECORD_ALWAYS: tuple[str, ...] = (
    "node_id", "subtype", "title", "link", "link_count",
)
#: The names that ask for the link view: a `select` made of these alone, with
#: at least one that is neither `attrs` nor `link.attrs`, returns one row per
#: link exactly as before records became the default. Beside a link field
#: `attrs` keeps its old meaning, the link's own, and `link.attrs` is the
#: same field by its new name. `attrs` with no link field beside it is the
#: linked record's.
EXPAND_LINK_SELECT: tuple[str, ...] = (*_LIST_NEIGHBORS_FIELDS, "link.attrs")
#: Most links one record read keeps while access rules are evaluated (audit
#: and enforce), where each link and each far record is decided by the
#: per-row read check and cannot be grouped in SQL. Counted on the links
#: the reader may see that reach a record the reader may see. With access
#: rules off there is no budget: the far
#: records, their link counts and the total are computed in SQL
#: (`expand_far_page`, `expand_page_links`).
EXPAND_LINK_BUDGET = _NEIGHBORS_EXACT_COUNT_BUDGET
#: Stored links read per batch while finding the visible ones.
_EXPAND_LINK_BATCH = 2_000
#: Most STORED links one record read examines under access rules, hidden
#: ones included (the budget above counts visible ones only, so without
#: this a record with many hidden links would have every one read). Ten
#: times the budget. Module level, read at call time, so a test can lower
#: it. Reaching it refuses with `EXPAND_LINK_SCAN_BOUND_MESSAGE`.
EXPAND_LINK_SCAN_CAP = 10 * EXPAND_LINK_BUDGET
#: The one fixed refusal at that cap. It says nothing about how many links
#: are stored or hidden.
EXPAND_LINK_SCAN_BOUND_MESSAGE = (
    "expand: this record has more links than one read checks under access "
    "rules; name an edge_type or a single direction to read fewer"
)


class ExpandLinkScanBound(SubstrateError):
    """A one-hop record read under access rules reached
    `EXPAND_LINK_SCAN_CAP` stored links before it knew its far records."""

    code = "expand_link_scan_bound"
#: Why a record read gives no total: it stopped at `EXPAND_LINK_BUDGET` links.
OVER_LINK_BUDGET = "omitted_over_link_budget"


def expand_select_is_links(select: Any) -> bool:
    """Whether a 1-hop `select` asks for link rows (see `EXPAND_LINK_SELECT`)."""
    if not isinstance(select, (list, tuple)) or not select:
        return False
    if not all(isinstance(name, str) and name in EXPAND_LINK_SELECT for name in select):
        return False
    return any(name not in ("attrs", "link.attrs") for name in select)


#: A link's far end and its direction as seen from `:focal`, in SQL (a
#: self-loop is "out" and its far end is the record itself).
_FAR_END_SQL = "CASE WHEN e.src = :focal THEN e.dst ELSE e.src END"
_LINK_DIRECTION_SQL = "CASE WHEN e.src = :focal THEN 'out' ELSE 'in' END"
_LINK_COLUMNS = (
    "e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source, "
    "e.source_record_id, e.source_revision"
)
_RECORD_COLUMNS = (
    "n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
    "n.source_record_id, n.source_revision"
)


def expand_anchor_sql(
    node_id: str, edge_type: Optional[str], direction: str,
) -> tuple[str, dict[str, Any]]:
    """The WHERE over `okg.graph_edges e` for one anchor, and its parameters.

    Refuses a direction that is not in, out or both.
    """
    if direction not in _LIST_NEIGHBORS_ALLOWED_DIRECTIONS:
        raise SubstrateError(
            "expand: direction must be one of "
            f"{sorted(_LIST_NEIGHBORS_ALLOWED_DIRECTIONS)}; got {direction!r}",
            data={
                "direction": direction,
                "allowed": sorted(_LIST_NEIGHBORS_ALLOWED_DIRECTIONS),
            },
        )
    params: dict[str, Any] = {"focal": node_id}
    where = _neighbors_where(direction)
    if edge_type is not None:
        where += " AND e.edge_type = :etype"
        params["etype"] = edge_type
    return where, params


def _expand_visible_records(
    conn: psycopg.Connection,
    session: Session,
    ids: list[str],
    *,
    count: bool,
) -> list[dict[str, Any]]:
    """The records with these ids at the pin, in record order, that pass
    this reader's record read check. The only place an expand record read
    under access rules touches the stored records. Every row it returns has
    passed `filter_node_rows_for_session` with the runtime check always on
    (`runtime_required=True`): there is no path that returns a stored row
    unchecked. `count=True` records the read with the counting purpose
    (records examined, not returned)."""
    if not ids:
        return []
    graph = _chronos_checkout(conn, session)
    stored = graph.query(
        f"SELECT {_RECORD_COLUMNS} FROM okg.graph_nodes n "
        "WHERE n.node_id = ANY(:ids) ORDER BY n.node_id",
        {"ids": list(ids)},
    )
    return filter_node_rows_for_session(
        conn, session, stored, runtime_required=True, count=count,
    )


def _expand_visible_links(
    conn: psycopg.Connection,
    session: Session,
    *,
    where: str,
    params: dict[str, Any],
    after: Any,
    limit: int,
) -> tuple[int, Any, list[dict[str, Any]]]:
    """One batch of an anchor's stored links after link id `after`, in link
    id order, as `(links read, last link id read, the ones that pass this
    reader's link read check)`. The read is recorded with the counting
    purpose: these links are examined, not returned. Only the count and the
    last id of the stored batch leave here unchecked; every link row
    returned has passed `filter_edge_rows_for_session` with the runtime
    check always on (`runtime_required=True`)."""
    graph = _chronos_checkout(conn, session)
    batch = list(graph.query(
        f"SELECT {_LINK_COLUMNS} FROM okg.graph_edges e WHERE {where}"
        + (" AND e.edge_id > :after" if after is not None else "")
        + " ORDER BY e.edge_id LIMIT :raw_limit",
        {**params, "raw_limit": int(limit),
         **({"after": after} if after is not None else {})},
    ))
    if not batch:
        return 0, after, []
    readable = filter_edge_rows_for_session(
        conn, session, batch, runtime_required=True, count=True,
    )
    return len(batch), batch[-1]["edge_id"], readable


def _record_sql_under_access_rules(session: Session, what: str) -> SubstrateError:
    """The refusal of an SQL record read asked for while access rules are
    evaluated. Such a read groups and counts links and records with no
    per-row check, so each one checks the mode itself, first, and raises
    this under audit or enforce rather than read."""
    return SubstrateError(
        f"expand: {what} reads without the per-row access check and is "
        "not used while access rules are evaluated",
        data={"deployment": session.deployment_name},
    )


def expand_far_page(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str],
    direction: str,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    """One page of the distinct far records of an anchor, and how many there
    are, computed in SQL. For a read with access rules off only: nothing
    here is checked per row, so it has no link budget and the page of a hub
    is as exact as the page of any record. Records are in id order; a far
    end with no record at the pin has no row and is not counted. Under
    audit or enforce it refuses before it reads.
    """
    if deployment_requires_nomos_runtime(session.deployment_name):
        raise _record_sql_under_access_rules(session, "the SQL page of linked records")
    where, params = expand_anchor_sql(node_id, edge_type, direction)
    graph = _chronos_checkout(conn, session)
    reached = (
        f"WITH reached AS (SELECT DISTINCT {_FAR_END_SQL} AS far "
        f"FROM okg.graph_edges e WHERE {where}) "
    )
    records = graph.query(
        reached + f"SELECT {_RECORD_COLUMNS} FROM reached r "
        "JOIN okg.graph_nodes n ON n.node_id = r.far "
        "ORDER BY n.node_id LIMIT :raw_limit OFFSET :raw_offset",
        {**params, "raw_limit": limit, "raw_offset": offset},
    )
    total = graph.query(
        reached + "SELECT count(*) AS c FROM reached r "
        "JOIN okg.graph_nodes n ON n.node_id = r.far",
        params,
    )
    return [dict(row) for row in records], int(total[0]["c"])


def expand_page_links(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    far_ids: list[str],
    *,
    edge_type: Optional[str],
    direction: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """For one page of far records, the link each row shows and how many
    links reach it, grouped in SQL. For a read with access rules off only.

    Returns `(links, link_counts)`: `links[far]` holds the one shown link,
    the first by link type, then direction, then link id (the order
    `expand_far_links` sorts in; `COLLATE "C"` is Python's string order),
    and `link_counts[far]` every link from the anchor to that record. A far
    record with no link here is refused, never served without one. Under
    audit or enforce it refuses before it reads.
    """
    if deployment_requires_nomos_runtime(session.deployment_name):
        raise _record_sql_under_access_rules(session, "the SQL link count")
    if not far_ids:
        return {}, {}
    where, params = expand_anchor_sql(node_id, edge_type, direction)
    graph = _chronos_checkout(conn, session)
    picked = graph.query(
        f"WITH links AS (SELECT e.edge_id, e.edge_type, {_FAR_END_SQL} AS far, "
        f"{_LINK_DIRECTION_SQL} AS direction FROM okg.graph_edges e WHERE {where}) "
        "SELECT l.far, count(*) AS link_count, "
        '(array_agg(l.edge_id ORDER BY l.edge_type COLLATE "C", l.direction, '
        "l.edge_id))[1] AS first_edge "
        "FROM links l WHERE l.far = ANY(:far_ids) GROUP BY l.far",
        {**params, "far_ids": list(far_ids)},
    )
    first = {row["first_edge"]: str(row["far"]) for row in picked}
    link_counts = {str(row["far"]): int(row["link_count"]) for row in picked}
    missing = sorted(set(far_ids) - set(link_counts))
    if missing:
        raise SubstrateError(
            "expand: a linked record has no link from this record in the "
            "graph at this pin",
            data={"node_id": node_id, "linked_records": missing[:5]},
            fix_hint="retry; if it persists, the attribute view and the graph disagree",
        )
    links: dict[str, list[dict[str, Any]]] = {}
    for row in graph.query(
        f"SELECT {_LINK_COLUMNS} FROM okg.graph_edges e "
        "WHERE e.edge_id = ANY(:edge_ids)",
        {"edge_ids": list(first)},
    ):
        links[first[row["edge_id"]]] = [{
            **row, "direction": "out" if row["src"] == node_id else "in",
        }]
    return links, link_counts


def expand_far_links(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str] = None,
    direction: str = "out",
    runtime_required: bool | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], bool]:
    """The links at `node_id` this reader may see that reach a record it
    may see, grouped by far-end id.

    Returns `(links, over_budget)`. Each list is ordered by link type, then
    direction, then link id, and each
    link carries its own `direction` (a self-loop is "out", as on a link
    row). At most `EXPAND_LINK_BUDGET` such links are kept, lowest id first;
    `over_budget` says the anchor has more, so the far ends are not all here.
    The links pass the same read check a link row passes, and the far
    records the record read check, both as counts (nothing is returned
    here). A hidden link, and a link to a hidden or absent record, takes no
    budget slot, so neither can change what a reply above the budget says.
    The stored links read, hidden ones included, are capped at
    `EXPAND_LINK_SCAN_CAP`: a read that reaches it with more stored links
    left raises `ExpandLinkScanBound`, whose message is fixed.

    The read for audit and enforce, where a link's visibility is decided
    per row. With access rules off `expand_far_page` and
    `expand_page_links` answer in SQL with no budget. Its reads always run
    the per-row check, so `runtime_required=False` (a caller that read the
    access rules as off) is refused rather than served unchecked rows.
    """
    if runtime_required is False:
        raise ValueError(
            "expand_far_links serves audit and enforce only; with access rules "
            "off the far records are read by expand_far_page"
        )
    where, params = expand_anchor_sql(node_id, edge_type, direction)
    # The budget, the cut and the links considered are decided on the links
    # this reader may see, never on the stored ones: links are read in id
    # order in batches and checked as COUNTS (they are examined, not
    # returned) until one more visible link than the budget is known or the
    # stored links end. A hidden link takes no budget slot.
    visible: list[dict[str, Any]] = []
    # Whether each far end is a record this reader may see. A visible link
    # to a record that is hidden, or absent at the pin, is not kept and takes
    # no budget slot either: otherwise a hidden record could decide which
    # records are served, or whether the read is over the budget.
    reachable: dict[str, bool] = {}

    def far_of(row: Mapping[str, Any]) -> str:
        return str(row["dst"] if row["src"] == node_id else row["src"])

    after: Any = None
    scanned = 0
    scan_cap = EXPAND_LINK_SCAN_CAP
    while len(visible) <= EXPAND_LINK_BUDGET:
        if scanned >= scan_cap:
            more, _last, _readable = _expand_visible_links(
                conn, session, where=where, params=params, after=after, limit=1,
            )
            if not more:
                break
            # More stored links remain and the visible ones do not yet say
            # where the far records end: refuse, never serve a cut set as
            # if it were whole. The message is fixed and names no count.
            # That a refusal happens at all does say the record has more
            # stored links than the cap, hidden ones included (one bit,
            # disclosed in the PACT).
            raise ExpandLinkScanBound(
                EXPAND_LINK_SCAN_BOUND_MESSAGE,
                data={"node_id": node_id, "edge_type": edge_type, "direction": direction},
                fix_hint="name an edge_type or a single direction",
            )
        requested = min(_EXPAND_LINK_BATCH, scan_cap - scanned)
        read, after, readable = _expand_visible_links(
            conn, session, where=where, params=params, after=after, limit=requested,
        )
        if not read:
            break
        scanned += read
        undecided = sorted({far_of(row) for row in readable} - set(reachable))
        if undecided:
            served = {
                str(row["node_id"]) for row in _expand_visible_records(
                    conn, session, undecided, count=True,
                )
            }
            reachable.update({far: far in served for far in undecided})
        visible += [row for row in readable if reachable[far_of(row)]]
        if read < requested:
            break
    over_budget = len(visible) > EXPAND_LINK_BUDGET
    visible = visible[:EXPAND_LINK_BUDGET]
    links: dict[str, list[dict[str, Any]]] = {}
    for row in visible:
        links.setdefault(far_of(row), []).append(
            {**row, "direction": "out" if row["src"] == node_id else "in"}
        )
    # Link ids are not assigned in any order a reader can use, so the link a
    # row shows is chosen by what it is: link type, then direction, then id.
    for reached in links.values():
        reached.sort(
            key=lambda link: (str(link["edge_type"]), link["direction"], link["edge_id"])
        )
    return links, over_budget


def expand_read_links(
    conn: psycopg.Connection,
    session: Session,
    records: list[dict[str, Any]],
    links: dict[str, list[dict[str, Any]]],
    *,
    runtime_required: bool | None = None,
) -> None:
    """Record as READ the one link each page row returns (its first).

    `expand_far_links` examined every link as a count. Only the links a
    page returns are logged as reads, here, once per page. In place.
    """
    shown = [links[str(record["node_id"])][0] for record in records]
    if not shown:
        return
    read = {
        row["edge_id"]: row for row in filter_edge_rows_for_session(
            conn, session, [dict(link) for link in shown],
            runtime_required=runtime_required,
        )
    }
    for link in shown:
        if link["edge_id"] in read:
            link.update({**read[link["edge_id"]], "direction": link["direction"]})


def validate_expand_record_select(select: Optional[list[str]]) -> None:
    """Refuse a 1-hop record-row `select` name that is no field of the row.

    `attrs.<key>` names one attribute of the linked record (issue #3666, lane
    RD), as on a path row: the row's `attrs` then holds only the named keys,
    unless `attrs` is named too."""
    from okg.substrate.mcp.select_vocabulary import attr_path_key

    keys = [key for key in (attr_path_key(name) for name in select or ()) if key is not None]
    validate_select(
        select, (*EXPAND_RECORD_FIELDS, *(f"attrs.{key}" for key in keys)),
        tool_name="expand",
    )


def expand_record_wants_attrs(select: Optional[list[str]]) -> bool:
    """Whether a 1-hop record-row `select` reads the linked records' attrs."""
    from okg.substrate.mcp.select_vocabulary import attr_path_key

    return any(
        name in ("attrs", "provenance") or attr_path_key(name) is not None
        for name in select or ()
    )


def expand_record_rows(
    records: list[dict[str, Any]],
    links: Mapping[str, list[dict[str, Any]]],
    select: Optional[list[str]],
    *,
    titles: Mapping[str, Any],
    link_attrs: Mapping[Any, dict[str, Any]],
    link_counts: Optional[Mapping[str, int]] = None,
) -> list[dict[str, Any]]:
    """Shape served far records as 1-hop rows. Pure.

    `records` are the page's records in page order, already read as this
    reader is served them (with `provenance` when `attrs` is wanted).
    `links[node_id]` are the visible links that reach each, in `expand_far_links` order:
    the first is the row's `link`, and `link_count` says how many there are
    (`link_counts[node_id]` when the caller counted them in SQL and `links`
    holds only the shown one).
    `titles` is `resolve_neighbor_summaries`' answer; `link_attrs` maps a
    link id to its served `{attrs, read_provenance}` when `link.attrs` is
    selected. Every row has the same keys, except that a "known by key, not
    loaded" endpoint's row adds `load_state` (issue #3666).
    """
    from okg.substrate.mcp.select_vocabulary import attr_path_key

    wanted = set(select or ())
    attr_keys = list(dict.fromkeys(
        key for key in (attr_path_key(name) for name in select or ()) if key is not None
    ))
    rows: list[dict[str, Any]] = []
    for record in records:
        node_id = str(record["node_id"])
        reached = links[node_id]
        first = reached[0]
        link: dict[str, Any] = {
            "edge_type": first["edge_type"],
            "direction": first["direction"],
            "edge_id": first["edge_id"],
        }
        for end in ("src", "dst"):
            if end in wanted:
                link[end] = first[end]
        if "link.attrs" in wanted:
            served = link_attrs.get(first["edge_id"]) or {}
            link["attrs"] = served.get("attrs")
            link["read_provenance"] = served.get("read_provenance")
        summary = titles.get(node_id)
        row: dict[str, Any] = {
            "node_id": record["node_id"],
            "subtype": record.get("subtype"),
            "title": summary.get("title") if isinstance(summary, Mapping) else None,
        }
        if isinstance(summary, Mapping) and summary.get("load_state") == "not_loaded":
            # Issue #3666: a record known only by key. Only such a row has
            # the key, so every loaded record's row keeps its shape.
            row["load_state"] = "not_loaded"
        if "attrs" in wanted:
            row["attrs"] = record.get("attrs")
        elif attr_keys:
            stored = record.get("attrs")
            # A masked or withheld map (not a mapping) is served as it is;
            # the redaction marker always travels, as on a path row.
            row["attrs"] = (
                {key: stored[key] for key in (*attr_keys, REDACTION_MARKER) if key in stored}
                if isinstance(stored, Mapping) else stored
            )
        if "attrs" in wanted or "provenance" in wanted or attr_keys:
            # The trust label travels with the attributes, as on a path row.
            row["provenance"] = record.get("provenance")
        row["link"] = link
        row["link_count"] = (
            len(reached) if link_counts is None else int(link_counts[node_id])
        )
        rows.append(row)
    return rows


def expand_link_attrs(
    conn: psycopg.Connection,
    session: Session,
    records: list[dict[str, Any]],
    links: Mapping[str, list[dict[str, Any]]],
) -> dict[Any, dict[str, Any]]:
    """Each page row's first link as a link row serves it: effective
    attributes, then the edge read serializer, keyed by link id."""
    first = [dict(links[str(record["node_id"])][0]) for record in records]
    if not first:
        return {}
    first = apply_effective_attrs_to_rows(
        conn, first, target_kind="edge", target_id_field="edge_id",
        generation_id=session.generation_id,
    )
    from okg.substrate.mcp.provenance import serialize_edge_read_rows

    first = serialize_edge_read_rows(
        conn, first, deployment_name=session.deployment_name,
    )
    return {
        row["edge_id"]: {
            "attrs": row.get("attrs"), "read_provenance": row.get("read_provenance"),
        }
        for row in first
    }


def expand_records_page(
    session: Session,
    node_id: str,
    *,
    direction: str,
    rows: list[dict[str, Any]],
    limit: int,
    offset: int,
    has_more: bool,
    over_budget: bool,
    count_fields: Mapping[str, Any],
) -> dict[str, Any]:
    """The reply of a record read: the keys a 1-hop page always had.

    Offsets count served records, so `next_offset` is `offset + limit`.
    `over_budget` is only ever true while access rules are evaluated (see
    `EXPAND_LINK_BUDGET`). Above the link budget the far records are not all known: no total is
    given, the page is never called complete, and when the known records
    run out the page says paging stops and why.
    """
    pin = session.generation_id
    payload: dict[str, Any] = {
        "node_id": node_id,
        "direction": direction,
        "neighbors": rows,
        "returned_count": len(rows),
        **count_fields,
        "offset": offset,
        "limit": limit,
        "next_offset": offset + limit if has_more else None,
        "truncated": has_more,
        "generation_id": pin,
        "pinned_generation_id": pin,
    }
    if over_budget:
        payload.pop("retrieval_exhaustive", None)
        payload.pop("delivery_last_page", None)
        payload["total_count"] = None
        payload["total_count_basis"] = OVER_LINK_BUDGET
        payload["truncated"] = True
        notice = (
            f"this record has more than {EXPAND_LINK_BUDGET:,} links you may read here; the "
            f"rows are the records its first {EXPAND_LINK_BUDGET:,} links reach "
            "and no total is given. where, limit and offset work within "
            "those records. To reach the rest, narrow edge_type or "
            "direction, or select only link fields (edge_id, src, dst, "
            "edge_type, direction), which pages every link."
        )
        if has_more:
            payload["note"] = notice
        else:
            payload["pagination_capped"] = True
            payload["pagination_notice"] = notice
    return payload


def expand_records(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str] = None,
    direction: str = "out",
    limit: int = 100,
    offset: int = 0,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """The records linked to `node_id` at the pin, one row per record.

    The read with no condition on the far record and no order (the server
    serves those through the effective-attribute page). Rows are in record
    id order. A far end that is hidden from this reader, or has no record at
    the pin, has no row and is not counted: the two answer alike.
    `total_count` is the number of far records this reader may see.

    With access rules off the page, the total, each row's `link_count` and
    its shown link are computed in SQL, with no link budget. Under audit and
    enforce the far records are found within `EXPAND_LINK_BUDGET` visible
    links to visible records, decided as counts; then the page's own
    records are read, once each.
    """
    validate_expand_record_select(select)
    offset, limit = int(offset), int(limit)
    if offset < 0:
        raise SubstrateError("expand: offset must be >= 0", data={"offset": offset})
    if limit < 1:
        raise SubstrateError(
            "expand: limit must be >= 1 (a non-positive page size yields a "
            "cursor that cannot advance)",
            data={"limit": limit},
        )
    runtime = deployment_requires_nomos_runtime(session.deployment_name)
    link_counts: Optional[dict[str, int]] = None
    if runtime:
        # Audit and enforce: every link and every far record is decided by
        # the per-row read check, so the far records are found in Python
        # within the link budget.
        links, over_budget = expand_far_links(
            conn, session, node_id, edge_type=edge_type, direction=direction,
            runtime_required=runtime,
        )
        # `links` holds only far records this reader may see (decided as
        # counts in `expand_far_links`); this puts them in record order,
        # through the same record read check, as counts. Only the page's own
        # records are then read, once.
        seen = [
            str(row["node_id"]) for row in _expand_visible_records(
                conn, session, sorted(links), count=True,
            )
        ]
        total = len(seen)
        records = _expand_visible_records(
            conn, session, seen[offset: offset + limit], count=False,
        )
    else:
        # Access rules off: distinct far records, the page, the total, each
        # row's link count and its shown link all come from SQL. No budget.
        over_budget = False
        stored, total = expand_far_page(
            conn, session, node_id, edge_type=edge_type, direction=direction,
            limit=limit, offset=offset,
        )
        records = filter_node_rows_for_session(
            conn, session, stored, runtime_required=runtime,
        )
        links, link_counts = expand_page_links(
            conn, session, node_id,
            [str(record["node_id"]) for record in records],
            edge_type=edge_type, direction=direction,
        )
    if expand_record_wants_attrs(select):
        records = apply_effective_attrs_to_rows(
            conn, records, target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
        from okg.substrate.mcp.provenance import serialize_node_read_rows

        records = serialize_node_read_rows(
            conn, records, deployment_name=session.deployment_name,
        )
    from okg.substrate.mcp.passage_rollup import resolve_neighbor_summaries

    titles = resolve_neighbor_summaries(
        conn, session=session,
        node_ids=[str(record["node_id"]) for record in records],
        runtime_required=runtime,
        # The page's records were read just above; their titles come from
        # the same records, so this pass records no second read.
        count=True,
    )
    expand_read_links(conn, session, records, links, runtime_required=runtime)
    rows = expand_record_rows(
        records, links, select, titles=titles,
        link_attrs=(
            expand_link_attrs(conn, session, records, links)
            if "link.attrs" in set(select or ()) else {}
        ),
        link_counts=link_counts,
    )
    enforcing = deployment_nomos_enforcement_mode(session.deployment_name) == "enforce"
    payload = expand_records_page(
        session, node_id, direction=direction, rows=rows, limit=limit, offset=offset,
        has_more=offset + limit < total, over_budget=over_budget,
        count_fields={
            "total_count": total,
            "total_count_basis": "exact_visible" if enforcing else "exact",
            "retrieval_exhaustive": True,
        },
    )
    payload.update(expand_records_link_targets(
        session, rows, titles=titles, runtime=runtime,
        complete=offset == 0 and offset + limit >= total and not over_budget,
    ))
    if direction == "out" and offset == 0:
        payload.update(incoming_not_shown(conn, session, node_id, edge_type=edge_type))
    return payload


def expand_records_link_targets(
    session: Session,
    rows: list[dict[str, Any]],
    *,
    titles: Mapping[str, Any],
    runtime: bool,
    complete: bool,
) -> dict[str, Any]:
    """Issue #3666: `{"link_targets": ...}` for a record page with an
    outgoing link of a type an `on_miss` route produces, else `{}` (a
    deployment with no such route gets nothing). Under audit and enforce
    nothing is counted and no key is named (an endpoint is never served
    there)."""
    from okg.substrate.mcp.link_targets import (
        LINK_TARGETS_KEY,
        endpoint_edge_types,
        record_link_target_counts,
    )

    edge_types = endpoint_edge_types(session.deployment_name)
    if not any(
        isinstance(row.get("link"), Mapping)
        and row["link"].get("direction") == "out"
        and row["link"].get("edge_type") in edge_types
        for row in rows
    ):
        return {}
    return {LINK_TARGETS_KEY: record_link_target_counts(
        rows, titles=titles, counted=not runtime, complete=complete,
        edge_types=edge_types,
    )}


def expand_link_rows_link_targets(
    session: Session,
    neighbors: list[dict[str, Any]],
    *,
    complete: bool,
) -> dict[str, Any]:
    """Issue #3666: the same field for a page of link rows."""
    from okg.substrate.mcp.link_targets import (
        LINK_TARGETS_KEY,
        endpoint_edge_types,
        link_target_counts,
    )

    edge_types = endpoint_edge_types(session.deployment_name)
    if not any(
        row.get("direction") == "out" and row.get("edge_type") in edge_types
        for row in neighbors
    ):
        return {}
    return {LINK_TARGETS_KEY: link_target_counts(
        neighbors,
        counted=_counts_disclose_nothing(session),
        complete=complete,
        edge_types=edge_types,
    )}


def incoming_not_shown(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str],
) -> dict[str, Any]:
    """What a first outgoing page says about the incoming links it did not
    read (issue #3467), as `expand_neighbors` says it: `{}` when there are
    none to say."""
    if not _counts_disclose_nothing(session):
        return {INCOMING_NOT_SHOWN_KEY: {
            "count": None, "count_basis": "not_counted", "direction": "in",
        }}
    incoming = _capped_edge_count(
        conn, session,
        where="e.dst = :focal AND e.src <> :focal"
        + (" AND e.edge_type = :etype" if edge_type is not None else ""),
        params={"focal": node_id, **({"etype": edge_type} if edge_type else {})},
    )
    if not incoming:
        return {}
    return {INCOMING_NOT_SHOWN_KEY: {
        "count": min(incoming, _NOTICE_COUNT_CAP),
        "count_basis": "lower_bound" if incoming > _NOTICE_COUNT_CAP else "exact",
        "direction": "in",
    }}


def inspect_view(
    conn: psycopg.Connection,
    session: Session,
    name: str,
) -> dict[str, Any]:
    """Column-level introspection for one okg.* view.

    operator-surface-list-work req.listwork.introspection: agents were
    reverse-engineering v_nodes/v_edges columns by burning SELECT *
    probe calls (run_sql refuses information_schema). This is the named
    allowed path: columns with data types, served from the server's own
    connection, plus the reminder that v_* views read through the
    session's Chronos checkout.
    """
    clean = str(name or "").strip().lower()
    if clean.startswith("okg."):
        clean = clean[4:]
    if not clean or not clean.replace("_", "").isalnum():
        raise InvalidOperatorArguments(
            "inspect view: name must be a bare okg-schema view name, "
            "e.g. 'v_nodes' or 'okg.v_edges'"
        )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'okg' AND table_name = %s "
            "ORDER BY ordinal_position",
            (clean,),
        )
        columns = [
            {"name": row[0], "type": row[1]} for row in cur.fetchall()
        ]
    if not columns:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'okg' AND table_name LIKE 'v\\_%' "
                "ORDER BY table_name",
            )
            available = [row[0] for row in cur.fetchall()]
        raise InvalidOperatorArguments(
            _unknown_view_message(clean, available),
            data={
                "invalid_fields": ["target.name"],
                "validation_kinds": ["argument_check"],
            },
        )
    return {
        "view": f"okg.{clean}",
        "columns": columns,
        "note": (
            "v_* views read through the session's pinned Chronos "
            "checkout; raw psql outside a session sees them empty - "
            "query via the MCP query tool. For per-subtype attr keys "
            "use inspect(target={'kind':'subtype','subtype':<name>})."
        ),
        "pinned_generation_id": session.generation_id,
    }


def inspect_generation(
    conn: psycopg.Connection,
    session: Session,
    requested: int | str | None = None,
) -> dict[str, Any]:
    """Metadata for one OKG generation, resolved Chronos-natively.

    Issue #1436: this used to SELECT from `okg.graph_generations` with the
    public generation id bound to its BIGSERIAL `generation_id` column. The
    public id is a Chronos checkpoint string (`okg:<deployment>:branch:...`)
    and the chronos-native publisher no longer writes those legacy rows, so
    both an explicit valid checkpoint and the session's own implicit pin
    raised a bigint-conversion or not-found error. Resolution now goes
    through the same `resolve_okg_generation` authority the CLI
    `okg generation describe` uses, scoped to the session's branch.

    `status=None` so a BLOCKED checkpoint is inspectable too — a blocked
    generation is exactly the one an operator needs to look at, and it is
    reported by its `status`/`blocked_reason`, not by an error.
    """
    from okg.substrate.generation_resolver import resolve_okg_generation

    target = requested if requested is not None else session.generation_id
    resolved = resolve_okg_generation(
        conn,
        deployment_name=session.deployment_name,
        requested=target,
        branch=session.branch_name,
        status=None,
    )
    if resolved is None:
        raise _generation_scope_error(
            conn,
            deployment_name=session.deployment_name,
            branch_name=session.branch_name,
            requested=target,
        )
    metadata = resolved.metadata
    # The publisher stamps blocked_reason={} on a generation that published;
    # report that as null rather than as an empty-but-present reason.
    blocked_reason = metadata.get("blocked_reason") or None
    published = resolved.status == "published"
    return {
        "generation_id": resolved.generation_id,
        "deployment_name": resolved.deployment_name,
        # The branch the checkpoint was published on. A fork-scoped session
        # and the default branch can hold different heads; naming the branch
        # keeps a copied id traceable to the scope that owns it.
        "branch": resolved.branch,
        "status": resolved.status,
        "blocked_reason": blocked_reason,
        "catalog_version_id": resolved.catalog_version_id,
        # A Chronos checkpoint has no separate published_at column; its
        # created_at IS the generation boundary. Report it as published_at
        # only when the checkpoint actually published.
        "published_at": resolved.created_at if published else None,
        "created_at": resolved.created_at,
        "target_generation_id": resolved.target_generation_id,
        "node_count": metadata.get("node_count"),
        "edge_count": metadata.get("edge_count"),
        "predecessor_generation_id": metadata.get("predecessor_generation_id"),
        "pinned_generation_id": session.generation_id,
    }


def _generation_scope_error(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    requested: int | str,
) -> SubstrateError:
    """Tell "this id does not exist" apart from "you asked from the wrong
    scope" (issue #1436 acceptance).

    The probe is unscoped on purpose: it answers whether the checkpoint
    exists at all, and if it does, which deployment/branch owns it. It is
    diagnosis only — it never becomes the inspected payload.
    """
    from okg.substrate.errors import GenerationNotFound, GenerationOutOfScope
    from okg.substrate.graph_store import ChronosGraphStore

    data: dict[str, Any] = {
        "requested_generation": requested,
        "deployment_name": deployment_name,
        "branch_name": branch_name,
    }
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    try:
        checkpoint = store.resolve_generation(
            requested, deployment_name=None, branch=None, status=None,
        )
    except SubstrateError:
        # A compaction tombstone is its own answer; let it propagate.
        raise
    except Exception:  # noqa: BLE001 - absence probe, never the answer
        checkpoint = None
    metadata = dict(getattr(checkpoint, "metadata", None) or {})
    if metadata.get("kind") == "okg_generation":
        owner = metadata.get("deployment_name")
        owner_branch = metadata.get("branch")
        data["generation_deployment_name"] = owner
        data["generation_branch"] = owner_branch
        return GenerationOutOfScope(
            f"generation {requested!r} belongs to deployment {owner!r} on "
            f"branch {owner_branch!r}; this session reads "
            f"{deployment_name!r} on branch {branch_name!r}",
            data=data,
            fix_hint=(
                "read this generation from the deployment and branch that "
                "own it, or pass an id published in this session's scope"
            ),
        )
    return GenerationNotFound(
        f"generation {requested!r} is not an OKG generation checkpoint",
        data=data,
        fix_hint=(
            f"list generations for deployment {deployment_name!r} on branch "
            f"{branch_name!r} (`okg generation list --deployment "
            f"{deployment_name} --branch {branch_name}`) and inspect one of "
            "those ids"
        ),
    )


def inspect_subtype(
    conn: psycopg.Connection,
    session: Session,
    subtype: Any,
    *,
    continuation_token: Any = None,
    detail: Any = "summary",
    page_size: Any = DEFAULT_SECTION_PAGE_SIZE,
    max_bytes: int = 64 * 1024,
) -> dict[str, Any]:
    """Per-subtype metadata for agent schema discovery.

    Returns a compact, independently paged schema summary. Full node samples
    are available only through ``detail='sample_nodes'``.

    Per `improve-mcp-tool-surface` (2026-04-27, task 2.4):
    complements the session-open schema digest. The digest gives
    every subtype's high-level shape; `describe_subtype` lets the
    agent drill into one subtype without grep'ing LinkML.

    The response is NOT a list-returning shape — it describes a
    single subtype — so it carries `pinned_generation_id` (per
    the envelope's pin contract) but not `total_count` / `select`.

    Under ``off``/``audit`` (where no row can hide), counts, attr-key
    frequencies, and routes use pinned SQL aggregates and only bounded value
    previews are loaded. Under ``enforce``, the default never probes raw row
    counts or observed structure: it returns only the declared catalog schema,
    declared routes, recommended filters, and a typed visibility explanation.
    That unconditional branch prevents hidden rows from influencing counts,
    truncation, cursors, routes, or samples.
    """
    if not isinstance(subtype, str) or not subtype:
        raise InvalidSubtypeInspection(
            "inspect subtype: subtype must be a non-empty string",
            data={"field": "subtype"},
            fix_hint="pass target.subtype as a non-empty catalog subtype name",
        )
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
        raise InvalidSubtypeInspection(
            "inspect subtype: page_size must be a positive integer",
            data={"field": "page_size", "requested_page_size": page_size},
            fix_hint="pass target.page_size as a positive integer",
        )
    assert_session_matches(conn, session)
    if detail not in {"summary", "sample_nodes"}:
        raise InvalidSubtypeInspection(
            "inspect subtype: detail must be 'summary' or 'sample_nodes'",
            data={"field": "detail", "requested_detail": detail},
            fix_hint="use target.detail='summary' or target.detail='sample_nodes'",
        )
    graph = _chronos_checkout(conn, session)
    catalog = catalog_subtype_shape(conn, session, subtype)
    mode = deployment_nomos_enforcement_mode(session.deployment_name)
    if mode == "enforce":
        attributes = catalog["declared_attributes"]
        node_count = None
        edges_out = catalog["declared_edges_out"]
        edges_in = catalog["declared_edges_in"]
        visibility = {
            "status": "observed_statistics_withheld",
            "reason": "nomos_enforcement_requires_non_disclosing_declared_schema",
            "next_move": (
                "use the declared filters/routes, or retry under an authority "
                "allowed to disclose observed subtype statistics"
            ),
        }
        sample_nodes = None
        sample_value_graph = None
    else:
        summary = _subtype_summary_sql(conn, session, graph, subtype)
        attributes = merge_declared_and_observed_attributes(
            catalog["declared_attributes"],
            summary["attr_keys"],
        )
        node_count = int(summary["node_count"])
        declared_out = {
            (item["edge_type"], item["dst_subtype"]): item
            for item in catalog["declared_edges_out"]
        }
        observed_out = {
            (item["edge_type"], item["dst_subtype"]): item
            for item in summary["edges_out"]
        }
        edges_out = [
            {
                **(observed_out.get(key) or declared_out[key]),
                "source": (
                    "declared_and_observed"
                    if key in declared_out and key in observed_out
                    else "observed"
                    if key in observed_out
                    else "declared"
                ),
            }
            for key in sorted(set(declared_out) | set(observed_out))
        ]
        declared_in = {
            (item["edge_type"], item["src_subtype"]): item
            for item in catalog["declared_edges_in"]
        }
        observed_in = {
            (item["edge_type"], item["src_subtype"]): item
            for item in summary["edges_in"]
        }
        edges_in = [
            {
                **(observed_in.get(key) or declared_in[key]),
                "source": (
                    "declared_and_observed"
                    if key in declared_in and key in observed_in
                    else "observed"
                    if key in observed_in
                    else "declared"
                ),
            }
            for key in sorted(set(declared_in) | set(observed_in))
        ]
        visibility = {
            "status": "observed_statistics_available",
            "frequency_denominator": "visible_node_count",
        }
        sample_nodes = (
            _subtype_sample_nodes(conn, session, subtype)
            if detail == "sample_nodes"
            else None
        )
        sample_value_graph = graph
    payload = compact_subtype_summary(
        conn,
        session,
        subtype=subtype,
        node_count=node_count,
        attributes=attributes,
        edges_out=edges_out,
        edges_in=edges_in,
        recommended_filters=catalog["recommended_filters"],
        visibility=visibility,
        max_bytes=max_bytes,
        requested_page_size=page_size,
        continuation_token=continuation_token,
        detail=detail,
        sample_nodes=sample_nodes,
        sample_value_graph=sample_value_graph,
    )
    # Issue #2013. Attached AFTER the section pager, deliberately: the live
    # relation index is bounded by the declaration (a handful of entries with
    # a handful of arguments each), it is the thing an agent needs in order to
    # write its NEXT call, and paging it would mean an agent could be told a
    # relation exists on page 2 of a response it never fetched.
    live_relations = catalog.get("live_relations")
    if live_relations:
        payload["live_relations"] = live_relations
    return payload


def _subtype_total_rows(graph: Any, subtype: str) -> int:
    """Legacy subtype row probe retained for bounded-read regression coverage."""
    nodes = int(
        graph.query(
            "SELECT count(*) AS c FROM okg.graph_nodes "
            "WHERE subtype = :subtype",
            {"subtype": subtype},
        )[0]["c"]
    )
    edges = int(
        graph.query(
            "SELECT count(*) AS c FROM okg.graph_edges e "
            "WHERE EXISTS (SELECT 1 FROM okg.graph_nodes a "
            "  WHERE a.node_id = e.src AND a.subtype = :subtype) "
            "   OR EXISTS (SELECT 1 FROM okg.graph_nodes a "
            "  WHERE a.node_id = e.dst AND a.subtype = :subtype)",
            {"subtype": subtype},
        )[0]["c"]
    )
    return nodes + edges


def _attr_keys_from_type_counts(
    attr_observed: Mapping[str, Mapping[str, int]],
    node_count: int,
) -> list[dict[str, Any]]:
    """Fold per-key JSON-type counts into the attr_keys payload shape."""
    attr_keys = []
    for key, types in attr_observed.items():
        occurrences = sum(types.values())
        if occurrences <= 0:
            continue
        sorted_types = sorted(
            types.items(),
            key=lambda item: (-item[1], item[0]),
        )
        attr_type = sorted_types[0][0]
        null_count = int(types.get("null", 0))
        attr_keys.append(
            {
                "name": key,
                "freq": float(occurrences) / node_count,
                "type": attr_type,
                "value_types": [
                    name for name, count in sorted(types.items()) if count > 0
                ],
                "presence_frequency": float(occurrences) / node_count,
                "null_frequency": float(null_count) / node_count,
                "frequency_denominator": node_count,
            }
        )
    attr_keys.sort(key=lambda item: item["name"])
    return attr_keys


def _subtype_sample_nodes(
    conn: psycopg.Connection,
    session: Session,
    subtype: str,
) -> list[dict[str, Any]]:
    """First 3 subtype nodes, ALWAYS served through the per-row Nomos filter.

    Bounded (LIMIT is a small constant before filtering, so a hidden row can
    only under-fill the sample, never blow up the read), and safe in every
    mode: `off`/`audit` pass rows through, `enforce` drops hidden lineage.
    """
    rows = _visible_node_rows(
        conn,
        session,
        where="WHERE subtype = :subtype AND node_id IN ("
        "SELECT node_id FROM okg.graph_nodes WHERE subtype = :subtype "
        "ORDER BY node_id LIMIT 3)",
        params={"subtype": subtype},
    )
    rows = apply_effective_attrs_to_rows(
        conn,
        rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )
    from okg.substrate.mcp.provenance import serialize_node_read_rows

    fenced = serialize_node_read_rows(
        conn,
        rows,
        deployment_name=session.deployment_name,
    )
    return [
        {
            "node_id": row["node_id"],
            "attrs": row["attrs"],
            "provenance": row["provenance"],
        }
        for row in fenced[:3]
    ]


def _subtype_summary_sql(
    conn: psycopg.Connection,
    session: Session,
    graph: Any,
    subtype: str,
) -> dict[str, Any]:
    """SQL-aggregate inspect(subtype) summary (off/audit: no row can hide).

    Counts and attr-key frequencies are computed in SQL over the raw attrs,
    then corrected for effective-attr overlays by re-running the canonical
    overlay projection (apply_effective_attrs_to_rows) over ONLY the
    overlaid nodes — bounded by overlay count, and semantics-identical to
    the per-row path by construction.
    """
    pin = session.generation_id
    node_count = int(
        graph.query(
            "SELECT count(*) AS c FROM okg.graph_nodes WHERE subtype = :subtype",
            {"subtype": subtype},
        )[0]["c"]
    )
    if node_count == 0:
        return {
            "node_count": 0,
            "attr_keys": [],
            "edges_out": [],
            "edges_in": [],
            "sample_nodes": [],
        }
    attr_observed: dict[str, dict[str, int]] = {}
    for row in graph.query(
        "SELECT a.key AS key, jsonb_typeof(a.value) AS value_type, "
        "count(*) AS c "
        "FROM okg.graph_nodes n, jsonb_each(n.attrs) AS a "
        "WHERE n.subtype = :subtype AND jsonb_typeof(n.attrs) = 'object' "
        "GROUP BY a.key, jsonb_typeof(a.value)",
        {"subtype": subtype},
    ):
        types = attr_observed.setdefault(str(row["key"]), {})
        types[str(row["value_type"])] = int(row["c"])
    _fold_overlay_corrections(
        conn,
        graph,
        subtype=subtype,
        generation_id=pin,
        attr_observed=attr_observed,
    )

    def _routes(program):
        return lower_program_over_chronos(
            program, conn,
            generation_id=session.generation_id,
            branch=session.branch_name,
            extra_params={"subtype": subtype},
        )

    return {
        "node_count": node_count,
        "attr_keys": _attr_keys_from_type_counts(attr_observed, node_count),
        "edges_out": _routes(subtype_edges_out_program()),
        "edges_in": _routes(subtype_edges_in_program()),
        "sample_nodes": [],
    }


def _fold_overlay_corrections(
    conn: psycopg.Connection,
    graph: Any,
    *,
    subtype: str,
    generation_id: Any,
    attr_observed: dict[str, dict[str, int]],
) -> None:
    """Correct raw SQL attr-type counts for effective-attr overlays.

    Only nodes that actually carry an overlay at this generation are
    re-projected (through the same apply_effective_attrs_to_rows the per-row
    path uses); for each such node the raw attrs' type contributions are
    subtracted and the effective attrs' contributions added. Exact because
    un-overlaid nodes have raw == effective.
    """
    from okg.substrate.attributes import (
        ATTRIBUTE_EFFECTIVE_TABLE,
        _table_exists,
    )

    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_TABLE):
        return
    overlaid = graph.query(
        "SELECT DISTINCT o.target_id AS node_id "
        f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} o "
        "JOIN okg.graph_nodes n ON n.node_id = o.target_id "
        "WHERE n.subtype = :subtype AND o.target_kind = 'node' "
        "AND o.generation_id = :generation_id AND o.namespace = 'default'",
        {"subtype": subtype, "generation_id": str(generation_id)},
    )
    ids = sorted({str(r["node_id"]) for r in overlaid})
    if not ids:
        return
    raw_rows = graph.query(
        "SELECT node_id, attrs FROM okg.graph_nodes "
        "WHERE node_id = ANY(:ids)",
        {"ids": ids},
    )
    effective_rows = apply_effective_attrs_to_rows(
        conn,
        [dict(row) for row in raw_rows],
        target_kind="node",
        target_id_field="node_id",
        generation_id=generation_id,
    )
    for raw, effective in zip(raw_rows, effective_rows):
        for key, value in (raw.get("attrs") or {}).items():
            types = attr_observed.setdefault(str(key), {})
            value_type = _json_value_type(value)
            types[value_type] = types.get(value_type, 0) - 1
        for key, value in (effective.get("attrs") or {}).items():
            types = attr_observed.setdefault(str(key), {})
            value_type = _json_value_type(value)
            types[value_type] = types.get(value_type, 0) + 1


def _subtype_summary_per_row(
    conn: psycopg.Connection,
    session: Session,
    subtype: str,
) -> dict[str, Any]:
    """Exact enforce-mode inspect(subtype) summary via the per-row filter.

    Every input — nodes AND the incident edge rows the route summaries are
    recomputed from — flows through the Nomos lineage filter, so a hidden
    source cannot influence node_count, attr-key frequencies, routes, or
    samples. Only runs for subtypes within the exact row budget.
    """
    pin = session.generation_id
    nodes = _visible_node_rows(
        conn,
        session,
        where="WHERE subtype = :subtype",
        params={"subtype": subtype},
    )
    nodes = apply_effective_attrs_to_rows(
        conn,
        nodes,
        target_kind="node",
        target_id_field="node_id",
        generation_id=pin,
    )
    node_count = len(nodes)
    if node_count == 0:
        return {
            "node_count": 0,
            "attr_keys": [],
            "edges_out": [],
            "edges_in": [],
            "sample_nodes": [],
        }
    attr_observed: dict[str, dict[str, int]] = {}
    for node in nodes:
        for key, value in (node.get("attrs") or {}).items():
            types = attr_observed.setdefault(str(key), {})
            value_type = _json_value_type(value)
            types[value_type] = types.get(value_type, 0) + 1

    # The lowered route programs return DISTINCT aggregates, which cannot be
    # lineage-filtered per row; recompute routes from raw incident edge rows
    # served through the filtered edge read path.
    edges_out_rows = _visible_edge_rows(
        conn,
        session,
        where="WHERE src.subtype = :subtype",
        params={"subtype": subtype},
    )
    edges_in_rows = _visible_edge_rows(
        conn,
        session,
        where="WHERE dst.subtype = :subtype",
        params={"subtype": subtype},
    )
    edges_out = sorted(
        {
            (
                str(r.get("edge_type")),
                str(r.get("dst_subtype") or "<unresolved>"),
            )
            for r in edges_out_rows
        }
    )
    edges_in = sorted(
        {
            (
                str(r.get("edge_type")),
                str(r.get("src_subtype") or "<unresolved>"),
            )
            for r in edges_in_rows
        }
    )
    return {
        "node_count": node_count,
        "attr_keys": _attr_keys_from_type_counts(attr_observed, node_count),
        "edges_out": [
            {"edge_type": etype, "dst_subtype": other} for etype, other in edges_out
        ],
        "edges_in": [
            {"edge_type": etype, "src_subtype": other} for etype, other in edges_in
        ],
        "sample_nodes": [],
    }


# `aggregate_by_type` was retired by `retire-redundant-mcp-tools`
# (2026-04-27). The replacement is the documented `mcp-views.md`
# recipe: `SELECT subtype, count(*) FROM okg.v_nodes GROUP BY
# subtype ORDER BY count(*) DESC`. The same data is also returned
# by `describe_graph()`'s `schema.subtypes` field, so prefer that
# verb when the question is "what's in this graph?".


# `aggregate_neighbors` returns one row per (edge_type, dst_subtype).
# Bounded by edge-type × subtype cardinality, which is small.
_AGG_NEIGHBORS_FIELDS: tuple[str, ...] = (
    "edge_type", "dst_subtype", "count",
)
_AGG_NEIGHBORS_DEFAULT: tuple[str, ...] = _AGG_NEIGHBORS_FIELDS
_AGG_NEIGHBORS_ALWAYS: tuple[str, ...] = ("edge_type", "dst_subtype")


_AGG_EDGE_ROUTES_FIELDS: tuple[str, ...] = (
    "src_subtype", "edge_type", "dst_subtype", "count",
)
_AGG_EDGE_ROUTES_DEFAULT: tuple[str, ...] = _AGG_EDGE_ROUTES_FIELDS
_AGG_EDGE_ROUTES_ALWAYS: tuple[str, ...] = (
    "src_subtype", "edge_type", "dst_subtype",
)


def attach_neighbor_summaries(
    conn: Any,
    session: Session,
    rows: list[dict[str, Any]],
    served: list[dict[str, Any]],
    *,
    runtime_required: bool | None = None,
) -> None:
    """Give each served 1-hop row its far end's `neighbor` summary
    (`{subtype, title}`; the row's `dst` or `src` is its id).

    Issues #2675 and #3072: a row named only the neighbour's id, so agents
    did not follow links. `rows[i]` is the full edge row behind `served[i]`;
    the far end is `dst` going out and `src` coming in. On every row, with or
    without `select`, like the edge's own identity fields
    (`_LIST_NEIGHBORS_ALWAYS`): agents narrowed `select` on 25 of 44 one-hop
    calls in lane Q's confirm run, and those 68 rows got no title. A neighbour
    the caller may not read, or that
    is absent at the pin, gets `neighbor: null`, so every row of a page has
    the same keys (#903) and the two cases answer alike
    (`resolve_neighbor_summaries`).
    """
    if not served:
        return
    from okg.substrate.mcp.passage_rollup import resolve_neighbor_summaries

    far = [
        str(row["src"] if row.get("direction") == "in" else row["dst"])
        for row in rows
    ]
    summaries = resolve_neighbor_summaries(
        conn, session=session, node_ids=far, runtime_required=runtime_required,
    )
    for out, far_id in zip(served, far, strict=True):
        out["neighbor"] = summaries.get(far_id)


#: Room for an unknown view's message under the refusal's 1,024-character
#: message cap (`response_bounds._expected_error_refusal`).
_UNKNOWN_VIEW_MESSAGE_CHARS = 1_000


def _unknown_view_message(name: str, available: list[str]) -> str:
    """Name the closest views first, then as many others as fit the message
    cap, and say how many did not fit (#2809): about 45 views overflow the
    cap, and a list cut silently reads as views that do not exist."""
    import difflib

    closest = difflib.get_close_matches(name, available, n=5, cutoff=0.6)
    head = f"inspect view: okg.{name} not found; {len(available)} views"
    if closest:
        head += f"; closest: {', '.join(closest)}"
    shown: list[str] = []
    rest = [view for view in available if view not in closest]
    for view in rest:
        tail = f" (+{len(rest) - len(shown) - 1} more not shown)"
        line = f"{head}; others: {', '.join([*shown, view])}{tail}"
        if len(line) > _UNKNOWN_VIEW_MESSAGE_CHARS:
            break
        shown.append(view)
    message = head
    if shown:
        message += f"; others: {', '.join(shown)}"
    if len(shown) < len(rest):
        message += f" (+{len(rest) - len(shown)} more not shown)"
    return message


def _validate_optional_text_filter(
    value: Optional[str],
    *,
    field: str,
) -> None:
    if value is not None and (not isinstance(value, str) or not value):
        raise InvalidOperatorArguments(f"{field} must be a non-empty string or null")


def _json_value_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def aggregate_edge(
    conn: psycopg.Connection,
    session: Session,
    *,
    src_subtype: Optional[str] = None,
    edge_type: Optional[str] = None,
    dst_subtype: Optional[str] = None,
    limit: int = 100,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Count live edges grouped by (src subtype, edge type, dst subtype).

    This is the evidence-promoted version of the self-play
    `direct_edge_path_count` recipe. Use it when an agent needs to know
    whether a typed one-hop route exists globally, before drilling into
    concrete nodes with `search`, `list_neighbors`, or `traverse_path`.

    Visibility + bounds (issue #627): under `off`/`audit` the route counts
    are a SQL GROUP BY (exact — no row can hide — and never materializes the
    matching edges in Python; per-row audit decisions are not written for
    the aggregate, the accepted inspect(graph) trade). Under `enforce`
    within the row budget, every matching edge passes the per-row lineage
    filter so hidden sources contribute to no count; above the budget the
    counts are omitted (`routes=None`, `truncated=True`, a `note`) instead
    of leaked or computed at blowup cost.
    """
    validate_select(
        select, _AGG_EDGE_ROUTES_FIELDS,
        tool_name="aggregate",
    )
    _validate_optional_text_filter(src_subtype, field="src_subtype")
    _validate_optional_text_filter(edge_type, field="edge_type")
    _validate_optional_text_filter(dst_subtype, field="dst_subtype")
    assert_session_matches(conn, session)
    pin = session.generation_id
    page_size = max(1, min(int(limit), 1000))
    extra: dict[str, Any] = {}
    if src_subtype is not None:
        extra["src_subtype"] = src_subtype
    if edge_type is not None:
        extra["edge_type"] = edge_type
    if dst_subtype is not None:
        extra["dst_subtype"] = dst_subtype
    where_parts = []
    if src_subtype is not None:
        where_parts.append("src.subtype = :src_subtype")
    if edge_type is not None:
        where_parts.append("e.edge_type = :edge_type")
    if dst_subtype is not None:
        where_parts.append("dst.subtype = :dst_subtype")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # Mode-aware route counting (issue #627): under off/audit no row can
    # hide, so the counts are a SQL GROUP BY and the matching edges are
    # never materialized in Python. Under enforce the exact per-row lineage
    # filter runs only when the matching edge count (a cheap SQL probe over
    # the same WHERE) is within the budget; above it the counts are omitted
    # (routes=None, truncated=True, a note) so hidden-source cardinality
    # cannot leak. A probe or SQL failure raises — no fallback to unbounded
    # materialization.
    graph = _chronos_checkout(conn, session)
    mode = deployment_nomos_enforcement_mode(session.deployment_name)
    if mode != "enforce":
        grouped_rows = _aggregate_edge_routes_sql(graph, where, extra)
    else:
        matching = int(
            graph.query(
                "SELECT count(*) AS c FROM okg.graph_edges e "
                "LEFT JOIN okg.graph_nodes src ON src.node_id = e.src "
                "LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst "
                f"{where}",
                extra,
            )[0]["c"]
        )
        if matching > _AGGREGATE_EDGE_EXACT_ROW_BUDGET:
            return {
                "generation_id": pin,
                "pinned_generation_id": pin,
                "filters": {
                    "src_subtype": src_subtype,
                    "edge_type": edge_type,
                    "dst_subtype": dst_subtype,
                },
                "routes": None,
                "total_count": None,
                "truncated": True,
                "note": (
                    "matching edges exceed the exact-count budget under "
                    "Nomos enforce mode; counts are omitted so "
                    "hidden-source cardinality cannot leak — narrow the "
                    "filters or use anchored reads"
                ),
            }
        rows = _visible_edge_rows(
            conn,
            session,
            where=where,
            params=extra or None,
        )
        grouped: dict[tuple[str, str, str], int] = {}
        for row in rows:
            key = (
                str(row.get("src_subtype")),
                str(row.get("edge_type")),
                str(row.get("dst_subtype")),
            )
            grouped[key] = grouped.get(key, 0) + 1
        grouped_rows = [
            {
                "src_subtype": key[0],
                "edge_type": key[1],
                "dst_subtype": key[2],
                "count": count,
            }
            for key, count in sorted(
                grouped.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
    total_count = len(grouped_rows)
    full_rows = [
        {
            "src_subtype": r["src_subtype"],
            "edge_type": r["edge_type"],
            "dst_subtype": r["dst_subtype"],
            "count": int(r["count"]),
        }
        for r in grouped_rows[:page_size]
    ]
    routes = [
        project_row(
            r, select,
            default=_AGG_EDGE_ROUTES_DEFAULT,
            always=_AGG_EDGE_ROUTES_ALWAYS,
        )
        for r in full_rows
    ]
    return {
        "generation_id": pin,
        "pinned_generation_id": pin,
        "filters": {
            "src_subtype": src_subtype,
            "edge_type": edge_type,
            "dst_subtype": dst_subtype,
        },
        "routes": routes,
        "total_count": total_count,
        "truncated": total_count > len(routes),
    }


# Matching-edge count above which aggregate(edge) will not run the exact
# per-row Nomos filter under `enforce` mode (same rationale as the
# inspect(graph)/inspect(subtype) budgets).
_AGGREGATE_EDGE_EXACT_ROW_BUDGET = 5_000


def _aggregate_edge_routes_sql(
    graph: Any,
    where: str,
    params: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Route counts as a SQL GROUP BY (off/audit: no row can hide).

    Reproduces the per-row grouping exactly, including its str() rendering
    of an unresolved endpoint subtype as 'None' and its (count desc, key
    asc) ordering.
    """
    rows = graph.query(
        "SELECT src.subtype AS src_subtype, e.edge_type AS edge_type, "
        "dst.subtype AS dst_subtype, count(*) AS c "
        "FROM okg.graph_edges e "
        "LEFT JOIN okg.graph_nodes src ON src.node_id = e.src "
        "LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst "
        f"{where} "
        "GROUP BY src.subtype, e.edge_type, dst.subtype",
        dict(params),
    )
    grouped = {
        (
            str(r["src_subtype"]),
            str(r["edge_type"]),
            str(r["dst_subtype"]),
        ): int(r["c"])
        for r in rows
    }
    return [
        {
            "src_subtype": key[0],
            "edge_type": key[1],
            "dst_subtype": key[2],
            "count": count,
        }
        for key, count in sorted(
            grouped.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]


def aggregate_node(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Count of outbound edges from `node_id` grouped by
    (edge_type, dst subtype) at the pin. Lets an agent decide
    whether to drill into a large neighbor set via list_neighbors
    or filter by edge_type first.

    Decision envelope: `total_count` is the exact group count;
    `truncated` is always false (no limit applied).
    """
    validate_select(
        select, _AGG_NEIGHBORS_FIELDS, tool_name="aggregate",
    )
    assert_session_matches(conn, session)
    pin = session.generation_id
    # aggregate(node) executes by lowering a TransformProgram (akmon.aggregate.neighbors)
    # over the edges_live/nodes_live registered surfaces -- GROUP + LEFT JOIN + coalesce
    # + multi-key ORDER -- rather than hand-built SQL against the base graph tables.
    rows = _visible_edge_rows(
        conn,
        session,
        where="WHERE e.src = :focal",
        params={"focal": node_id},
    )
    grouped: dict[tuple[str, str], int] = {}
    for row in rows:
        key = (str(row.get("edge_type")), str(row.get("dst_subtype")))
        grouped[key] = grouped.get(key, 0) + 1
    grouped_rows = [
        {"edge_type": key[0], "dst_subtype": key[1], "cnt": count}
        for key, count in sorted(
            grouped.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]
    full_rows = [
        {
            "edge_type": r["edge_type"],
            "dst_subtype": r["dst_subtype"],
            "count": r["cnt"],
        }
        for r in grouped_rows
    ]
    counts = [
        project_row(
            r, select,
            default=_AGG_NEIGHBORS_DEFAULT,
            always=_AGG_NEIGHBORS_ALWAYS,
        )
        for r in full_rows
    ]
    return {
        "node_id": node_id,
        "generation_id": pin,
        "pinned_generation_id": pin,
        "counts": counts,
        "total_count": len(counts),
        "truncated": False,
    }


# `traverse_path` returns one row per terminal node. Per-row
# fields: node_id, subtype, attrs, provenance.
_TRAVERSE_FIELDS: tuple[str, ...] = (
    "node_id", "subtype", "attrs", "provenance",
)
_TRAVERSE_DEFAULT: tuple[str, ...] = _TRAVERSE_FIELDS
_TRAVERSE_ALWAYS: tuple[str, ...] = ("node_id", "subtype")


def parse_path_hops(
    path: list[str], *, default_direction: Optional[str] = None,
) -> list[tuple[Optional[str], str]]:
    """Parse a typed-path spec (``["edge_type:out", ...]``) into ``(edge_type,
    direction)`` hops (``'*:out'`` gives edge_type None, any link), validating
    grammar + depth. A hop without a direction takes `default_direction`
    when one is given (the expand call's own `direction`), noted as
    adjusted. Raises ValueError on a malformed
    hop or a path exceeding MAX_TRAVERSE_HOPS (no DB access -- arg validation only)."""
    return _parse_actionable_path_hops(
        path, max_hops=MAX_TRAVERSE_HOPS, default_direction=default_direction,
    )


#: `expand_path(caller_select=...)` default: the binding records `select`.
_SELECT_AS_GIVEN: Any = object()


def _signed_projection(token: Any) -> Optional[dict[str, Any]]:
    """The projection a path token signed, or None when it does not verify.

    A token that fails here is refused by name by ``consume_continuation``
    right after; this only reads the signed budget.
    """
    from okg.substrate.mcp.continuation import (
        ContinuationTokenInvalid,
        open_continuation,
    )

    try:
        projection = open_continuation(token)["binding"].get("projection")
    except ContinuationTokenInvalid:
        return None
    return projection if isinstance(projection, dict) else None


def _hops_to_path(hops) -> list[str]:
    """Echo hops as path strings for the response envelope; a None edge_type
    (uniform any-edge traversal) renders as '*'."""
    return [
        f"{etype if etype is not None else '*'}:{direction}"
        for etype, direction in hops
    ]


def expand_path(
    conn: psycopg.Connection,
    session: Session,
    start_node: str,
    *,
    hops,
    target_subtype: Optional[str] = None,
    limit: int = 100,
    select: Optional[list[str]] = None,
    continuation_token: Optional[str] = None,
    caller_select: Any = _SELECT_AS_GIVEN,
    max_bytes: Optional[int] = None,
) -> dict[str, Any]:
    """Follow a typed path / uniform-depth traversal from `start_node` at the pin.

    `hops` is a list of (edge_type, direction) (direction in {"out","in"};
    edge_type=None follows any edge for that hop); at most MAX_TRAVERSE_HOPS hops.
    `target_subtype` filters the terminal nodes only.

    Executes a bounded, generation-pinned join chain over the registered Chronos
    graph surfaces, streaming at most ``PATH_WORK_CAP`` raw terminals through the
    visibility boundary. The bounded depth is unrolled and terminal node ids are
    de-duplicated in stable order. A degenerate length-1 path remains distinct from
    the established offset-paginated one-hop neighbor surface.

    Semantics: reads are pinned through the Chronos checkout; an "out" hop matches
    `src = frontier`, an "in" hop matches `dst = frontier`. If ``start_node`` is
    not visible at the pin, the result uses the same typed outcome for an absent
    or hidden start. Cycles are allowed and terminal node ids are de-duplicated.

    Decision envelope: `total_count = limit` if truncated, else `len(results)`;
    `select` projects each result row to a subset of node_id, subtype, attrs.

    The continuation binding records the call a `next` token rebuilds (PACT
    read-model-s6-page-next): the hops as ``[edge_type, direction]`` pairs
    with ``None`` kept for an any-edge hop (the ``path`` echo spells it
    ``'*'``, which the path grammar reads as any link), the
    caller's own ``select`` (``caller_select``, when the server rewrote it
    before this read, as for ``source_lineage``), and the response budget
    ``max_bytes`` the page was served under.
    """
    if caller_select is _SELECT_AS_GIVEN:
        caller_select = select
    from okg.substrate.mcp.select_vocabulary import attr_path_key

    # `attrs.<key>` names one attribute of the far record, as in filter and
    # map: the row's `attrs` holds only the keys named (unless the select
    # also names `attrs`, the whole map).
    attr_keys = [
        key for key in (attr_path_key(name) for name in select or ()) if key is not None
    ]
    validate_select(
        select,
        (*_TRAVERSE_FIELDS, *(f"attrs.{key}" for key in attr_keys)),
        tool_name="expand(path)",
        # PACT select-field-instructions: an edge field asked of a path read.
        elsewhere={
            name: "path and depth>1 rows are records; a 1-hop expand has "
            "this field"
            for name in _LIST_NEIGHBORS_FIELDS if name not in _TRAVERSE_FIELDS
        },
    )
    validate_path_limit(limit)
    if not hops:
        raise InvalidPathGrammar(
            "expand: path must have at least one hop",
            data={"hop_index": 0},
            fix_hint="pass a non-empty path such as [('contains', 'out')]",
        )
    if len(hops) > MAX_TRAVERSE_HOPS:
        raise InvalidPathGrammar(
            f"expand: path exceeds the maximum of {MAX_TRAVERSE_HOPS} hops",
            data={
                "hop_index": MAX_TRAVERSE_HOPS,
                "max_hops": MAX_TRAVERSE_HOPS,
                "pinned_generation_id": session.generation_id,
                "catalog_version_id": session.catalog_version_id,
            },
            fix_hint=f"shorten the path to at most {MAX_TRAVERSE_HOPS} hops",
        )
    for _etype, direction in hops:
        if direction not in ("out", "in"):
            hop_index = next(
                index for index, item in enumerate(hops) if item[1] == direction
            )
            raise InvalidPathDirection(
                f"expand: hop {hop_index} direction must be 'out' or 'in'",
                data={
                    "hop_index": hop_index,
                    "edge_type": _etype,
                    "direction": direction,
                },
                fix_hint="replace the direction with 'out' or 'in'",
            )
    # Fingerprint check happens after arg validation so arg-validation callers can
    # pass conn=None without a DB round-trip.
    assert_session_matches(conn, session)
    pin = session.generation_id
    path_echo = _hops_to_path(hops)
    authority = validate_path_edge_types(conn, session, hops)

    if not start_node_visibility(conn, session, start_node=start_node):
        return empty_path_outcome(
            session=session,
            authority=authority,
            start_node=start_node,
            hops=hops,
            outcome="start_not_visible",
            hop_index=0,
            target_subtype=target_subtype,
        )

    enforcement_mode = deployment_nomos_enforcement_mode(session.deployment_name)
    if enforcement_mode == "enforce":
        # The current Nomos evaluator is a per-row Python boundary, not a SQL
        # visibility predicate. Raw bounded scans would therefore let hidden
        # rows move the work-cap/truncation boundary. Refuse that side channel
        # with one typed outcome for every enforce-mode path; one-hop neighbor
        # expansion remains available through its established filtered surface.
        return empty_path_outcome(
            session=session,
            authority=authority,
            start_node=start_node,
            hops=hops,
            outcome="visibility_limited",
            hop_index=0,
            target_subtype=target_subtype,
            visibility_limited=True,
        )
    for hop_index in range(len(hops)):
        visible, exhausted, cap_reached = scan_visible_path(
            conn,
            session,
            start_node=start_node,
            hops=hops[: hop_index + 1],
            target_subtype=None,
            after_terminal=None,
            visible_limit=1,
        )
        if visible:
            continue
        visibility_limited = enforcement_mode == "enforce" or cap_reached
        return empty_path_outcome(
            session=session,
            authority=authority,
            start_node=start_node,
            hops=hops,
            outcome=(
                "visibility_limited"
                if visibility_limited
                else (
                    "no_matching_first_hop"
                    if hop_index == 0
                    else "empty_intermediate_hop"
                )
            ),
            hop_index=hop_index,
            target_subtype=target_subtype,
            visibility_limited=visibility_limited,
        )

    binding = continuation_binding(
        conn,
        session,
        kind="expand_path",
        path=path_echo,
        sort="node_id:asc",
        projection={
            "start_node": start_node,
            "select": (
                sorted(str(item) for item in caller_select)
                if caller_select is not None
                else None
            ),
            "target_subtype": target_subtype,
            "limit": limit,
            "hops": [[etype, direction] for etype, direction in hops],
            "max_bytes": max_bytes,
        },
    )
    after_terminal: str | None = None
    if continuation_token is not None:
        # The budget a page was served under is signed so `next` can restore
        # it, but it does not decide WHICH rows a path page holds: path pages
        # are never byte-cut, the budget only decides serve or refuse. So the
        # comparison takes the token's own signed budget, and the caller's
        # `max_bytes` -- the restored one, or a new one passed with the
        # legacy `continuation_token` spelling -- is signed into the next
        # token, as text continuations do (docs/reference/mcp-results.md).
        expected = binding
        signed = _signed_projection(continuation_token)
        if signed is not None and "max_bytes" in signed:
            expected = {
                **binding,
                "projection": {
                    **binding["projection"],
                    "max_bytes": signed["max_bytes"],
                },
            }
        cursor = consume_continuation(
            continuation_token,
            expected_binding=expected,
        )
        value = cursor.get("after_node_id")
        if not isinstance(value, str) or not value:
            from okg.substrate.mcp.continuation import ContinuationTokenInvalid

            raise ContinuationTokenInvalid(
                "the path continuation has no valid node position",
                data={"reason": "cursor_invalid", "pinned_generation_id": pin},
                fix_hint="restart the path expansion without a continuation token",
            )
        after_terminal = value

    rows, exhausted, cap_reached = scan_visible_path(
        conn,
        session,
        start_node=start_node,
        hops=hops,
        target_subtype=target_subtype,
        after_terminal=after_terminal,
        visible_limit=limit + 1,
    )
    if not rows and target_subtype is not None and continuation_token is None:
        return empty_path_outcome(
            session=session,
            authority=authority,
            start_node=start_node,
            hops=hops,
            outcome=(
                "visibility_limited"
                if enforcement_mode == "enforce" or cap_reached
                else "empty_terminal_result"
            ),
            hop_index=len(hops) - 1,
            target_subtype=target_subtype,
            visibility_limited=enforcement_mode == "enforce" or cap_reached,
        )
    truncated = len(rows) > limit or cap_reached
    rows = rows[:limit]
    full_rows = [
        {
            "node_id": r["node_id"],
            "subtype": r["subtype"],
            "attrs": r["attrs"],
            "source": r.get("source"),
        }
        for r in rows[:limit]
    ]
    full_rows = apply_effective_attrs_to_rows(
        conn,
        full_rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=pin,
    )
    from okg.substrate.mcp.provenance import serialize_node_read_rows

    full_rows = serialize_node_read_rows(
        conn,
        full_rows,
        deployment_name=session.deployment_name,
    )
    results = []
    whole_attrs = select is None or "attrs" in select
    row_select = select if not attr_keys else [
        name for name in select if attr_path_key(name) is None
    ] + ["attrs"]
    for row in full_rows:
        projected = project_row(
            row,
            row_select,
            default=_TRAVERSE_DEFAULT,
            always=_TRAVERSE_ALWAYS,
        )
        if attr_keys and not whole_attrs:
            stored = projected.get("attrs")
            if isinstance(stored, Mapping):
                # A masked or withheld map (not a mapping) is served as it is.
                # The redaction marker always travels: without it a key the
                # access rules dropped reads as a key the record lacks.
                kept = [*attr_keys, REDACTION_MARKER]
                projected["attrs"] = {key: stored[key] for key in kept if key in stored}
        if "attrs" in projected and "provenance" not in projected:
            projected["provenance"] = row["provenance"]
        results.append(projected)
    return {
        "status": "ok",
        "start_node": start_node,
        "path": path_echo,
        "generation_id": pin,
        "pinned_generation_id": pin,
        "catalog_version_id": session.catalog_version_id,
        "authority": authority,
        "target_subtype": target_subtype,
        "results": results,
        "total_count": (limit if truncated else len(results)),
        "truncated": truncated,
        "outcome": {
            "code": "nonempty_truncated" if truncated else "nonempty_complete",
            "hop_index": len(hops) - 1,
            "edge_type": hops[-1][0],
            "direction": hops[-1][1],
            "next_move": (
                "resume with continuation_token"
                if truncated
                else "consume the complete result"
            ),
        },
        "diagnostic_work_cap": PATH_WORK_CAP,
        **(
            {
                "continuation_token": issue_continuation(
                    binding,
                    cursor={"after_node_id": str(results[-1]["node_id"])},
                )
            }
            if truncated and results
            else {}
        ),
    }


# search_basic(): deleted at M4.5 per the M4.5 design review —
# it was a LIKE-based substring matcher that duplicated what
# pg_textsearch BM25 does better. Callers wanting the old shape
# should use `search(method='lexical', ...)` from search.py.


# `find_identifier_mentions` returns one row per match. The
# result set is bounded by `len(text)`-ish; no limit applied.
_FIND_MENTIONS_FIELDS: tuple[str, ...] = (
    "id_type", "value", "span_start", "span_end",
    "evidence_kind", "evidence_kind_provenance",
)
_FIND_MENTIONS_DEFAULT: tuple[str, ...] = _FIND_MENTIONS_FIELDS
_FIND_MENTIONS_ALWAYS: tuple[str, ...] = (
    "id_type", "value", "evidence_kind", "evidence_kind_provenance",
)


def find_identifier_mentions(
    conn: psycopg.Connection,
    session: Session,
    text: str,
    *,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Find substrings of `text` that match any identifier pattern
    registered in the active catalog (e.g. an OpenSpec change-id
    slug, a JIRA ticket, an arxiv id). Useful for linking free
    text to canonical graph nodes.

    What gets matched: every pattern in `okg.identifier_patterns`
    at the session's pinned `catalog_version_id`. Patterns are
    deployment-defined; inspect them with
    `run_sql("SELECT id_type, regex FROM okg.identifier_patterns")`.
    Common id_types include `change_id`, `ticket`, `arxiv_id`,
    `commit_sha` — varies per deployment.

    Returns one entry per match with `id_type`, `value`,
    `span_start`, `span_end` (0-indexed, end-exclusive — Python
    slice semantics), and the stable evidence classification fields
    `evidence_kind` and `evidence_kind_provenance`. `total_count` is
    exact (no limit applied); `truncated` is always false. `select`
    projects each mention to a subset of these six fields.

    Example:
      text = "see add-substrate-attr-filters and JIRA-1234"
      mentions = [
        {id_type: "change_id", value: "add-substrate-attr-filters",
         span_start: 4, span_end: 30},
        {id_type: "ticket", value: "JIRA-1234",
         span_start: 35, span_end: 44},
      ]
    """
    from okg.substrate.mcp.search import search_select_elsewhere

    validate_select(
        select, _FIND_MENTIONS_FIELDS,
        tool_name="search(method=identifier_mentions)",
        elsewhere=search_select_elsewhere("identifier_mentions", query=text),
    )
    assert_session_matches(conn, session)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id_type, value, span_start, span_end "
            "FROM okg.find_identifier_mentions(%s, %s)",
            (text, session.catalog_version_id),
        )
        rows = cur.fetchall()
    full_rows = [
        {
            "id_type": r[0],
            "value": r[1],
            "span_start": r[2],
            "span_end": r[3],
            "evidence_kind": "extracted_entity",
            "evidence_kind_provenance": {
                "basis": "identifier_pattern",
                "id_type": str(r[0])[:64],
            },
        }
        for r in rows
    ]
    mentions = [
        project_row(
            r, select,
            default=_FIND_MENTIONS_DEFAULT,
            always=_FIND_MENTIONS_ALWAYS,
        )
        for r in full_rows
    ]
    payload = {
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "mentions": mentions,
        "total_count": len(mentions),
        "truncated": False,
    }

    # 2026-04-25 fix (debug-substrate-okg-self Phase 5):
    # if there are no mentions AND the catalog has zero
    # identifier patterns at the pinned cv, surface a
    # structured `config` block so the caller can tell "no
    # match" from "no patterns registered". Empty results
    # from a populated catalog do NOT carry the hint.
    if not payload["mentions"]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.identifier_patterns "
                " WHERE catalog_version_id = %s",
                (session.catalog_version_id,),
            )
            n_patterns = cur.fetchone()[0]
        if n_patterns == 0:
            payload["config"] = {
                "patterns_registered": 0,
                "fix_hint": (
                    f"no identifier_patterns registered at "
                    f"catalog_version "
                    f"{session.catalog_version_id}; install "
                    f"via `okg catalog load --apply` after "
                    f"defining `identifier_patterns.yaml` in "
                    f"the deployment"
                ),
            }

    return payload


# `active_work` was retired by `retire-redundant-mcp-tools`
# (2026-04-27). Replacement: deployment-specific recipe in
# `docs/reference/mcp.md` (the SQL template parameterizes
# on the deployment's slug regex / file-path predicate).

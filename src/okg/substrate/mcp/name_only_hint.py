"""The file-text search a read of file NAMES names (CompOps q020, lane ri).

A git file is two records here. Its `source_file` record (`file:<repo>:<path>`)
holds the path, split into words, as its text; its contents are the `document`
record (`doc:<repo>:<path>`) and that document's passages. So a read that picks
files by name never sees what is in them:

* a `filter` (or a `search` with no query) whose condition is a text
  `contains` on `path` / `attrs.path`;
* a `search` with a query whose record types are `source_file` only;
* a `search` with a query AND one path test over `document` /
  `document_chunk` (q020, r36d): the text search ran, but only over files
  whose names match, so the named call is the query without the path test.

In CompOps q020 the reaper settings that mattered sat in
`apps/production/prod-rucio-daemons.yaml`, whose name says nothing of reapers.
Five tries listed the nine files named `*reaper*`, read "complete" or "9
records, all shown", and answered with a third of the pods. The shell found
the file with `rg -il reaper <repo>`.

Such a reply gets one line naming the grep over the file TEXTS, with how many
there are (`plan` -> the server runs the named call once, limit 1, under
:data:`COUNT_BUDGET_S`, and keeps its count -> `hint_block` -> `render`):

    ! matched file names only (the path test reads names, not file text);
      18 file texts in rucioflux contain "reaper":
      search(query="reaper", method="grep",
             where={"type":"document","id":{"op":"prefix","value":"doc:rucioflux:"}})

What carries into the named call, and what stops the line:

* the term: the path test's text, or the query (as a grep query);
* the repository: `attrs.repo = R`, an id prefix `file:R:` / `doc:R:`, or a
  `source` id `R.<family>` (the part before the first `.`, as `source`
  conditions read a repository name), written as the id prefix `doc:R:`;
  node ids are `doc:<repo>:<path>` (server instructions);
* `path_prefix`, as given;
* a `type` of `source_file` / `document` is replaced by `document`.

Any other condition (another attribute, a date, an `or`/`not`, two repos, a
second path test) cannot be carried as written, so no line is offered: a
count over a wider set would answer a different question. The count is the
named call's own `total_count`; when it is a lower bound the line says "at
least"; past the budget the line names the call and says it was not counted;
a call the server would refuse is never named. A count of 0 names no call.
"""
from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

__all__ = [
    "CONTENT_SUBTYPE",
    "COUNT_BUDGET_S",
    "FILE_SUBTYPES",
    "NAMES_ONLY_KEY",
    "NAME_ONLY_SUBTYPES",
    "Plan",
    "hint_block",
    "plan",
    "render",
]

#: Record types whose text is a file's path words, not its contents.
NAME_ONLY_SUBTYPES = frozenset({"source_file"})
#: The record type holding a file's contents (`doc:<repo>:<path>`).
CONTENT_SUBTYPE = "document"
#: The record types a path test reads file names on.
FILE_SUBTYPES = frozenset({"source_file", "document", "document_chunk"})
#: The record types that hold file text (a document and its passages).
CONTENT_SUBTYPES = frozenset({"document", "document_chunk"})
#: The payload key the line is built from.
NAMES_ONLY_KEY = "names_only"
#: Seconds the server spends counting the named call before it names the
#: call uncounted.
COUNT_BUDGET_S = 2.0

_TYPE_FIELDS = frozenset({"type", "subtype"})
_ID_FIELDS = frozenset({"id", "node_id"})
_PATH_FIELDS = frozenset({"path", "attrs.path"})
_REPO = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}")
_ID_REPO = re.compile(r"(?:file|doc):([A-Za-z0-9][A-Za-z0-9_.\-]{0,127}):")
_BARE_TERM = re.compile(r"[A-Za-z0-9_.]+")
_MAX_TERM_CHARS = 200

_WHY = {
    "path_test": "the path test reads names, not file text",
    "file_records": "a source_file holds a file's path, not its text",
    "path_filter": "the path test kept only files with matching names",
}


@dataclass(frozen=True)
class Plan:
    """The named call: a grep for `query` over `document` records."""

    reason: str  # "path_test" | "file_records"
    query: str
    repo: Optional[str]
    path_prefix: Optional[str]
    #: Positive grep terms in `query`: two or more must all be in one text.
    terms: int

    def arguments(self) -> dict[str, Any]:
        """The call as the reply prints it (the short `where` form)."""
        where: dict[str, Any] = {"type": CONTENT_SUBTYPE}
        if self.repo is not None:
            where["id"] = {"op": "prefix", "value": f"doc:{self.repo}:"}
        arguments: dict[str, Any] = {"query": self.query, "method": "grep", "where": where}
        if self.path_prefix is not None:
            arguments["path_prefix"] = self.path_prefix
        return arguments

    def canonical_where(self) -> dict[str, Any]:
        """The same `where` as the predicate tree the search reads."""
        clauses: list[dict[str, Any]] = [
            {"op": "eq", "field": "type", "value": CONTENT_SUBTYPE},
        ]
        if self.repo is not None:
            clauses.append({"op": "prefix", "field": "id", "value": f"doc:{self.repo}:"})
        return clauses[0] if len(clauses) == 1 else {"op": "and", "clauses": clauses}


def _conjuncts(predicate: Any) -> Optional[list[dict[str, Any]]]:
    """Top-level conjuncts of a predicate tree; None under `or`/`not` or a
    shape this module does not read."""
    if predicate is None or predicate is True or predicate == {}:
        return []
    if hasattr(predicate, "model_dump"):
        predicate = predicate.model_dump(exclude_none=True)
    if not isinstance(predicate, Mapping):
        return None
    op = predicate.get("op")
    if op == "and":
        out: list[dict[str, Any]] = []
        clauses = predicate.get("clauses")
        if not isinstance(clauses, list):
            return None
        for clause in clauses:
            part = _conjuncts(clause)
            if part is None:
                return None
            out.extend(part)
        return out
    if not isinstance(op, str) or op in ("or", "not"):
        return None
    return [dict(predicate)]


def _grep_term(text: str) -> Optional[str]:
    """A path test's text as one grep term: bare when it is one word, else
    a quoted phrase; None when grep cannot take it as written."""
    from okg.substrate.mcp.grep_query import GrepQueryInvalid, parse_grep_query

    text = text.strip()
    if not text or len(text) > _MAX_TERM_CHARS or '"' in text:
        return None
    query = text if _BARE_TERM.fullmatch(text) else f'"{text}"'
    try:
        parsed = parse_grep_query(query)
    except GrepQueryInvalid:
        return None
    if len(parsed.positive) != 1 or parsed.negative or parsed.path_terms:
        return None
    return query


def _query_terms(query: str) -> Optional[int]:
    """Positive text terms of a grep query; None when grep refuses it or it
    holds no text term."""
    from okg.substrate.mcp.grep_query import GrepQueryInvalid, parse_grep_query

    try:
        parsed = parse_grep_query(query)
    except GrepQueryInvalid:
        return None
    return len(parsed.positive) or None


def plan(
    predicate: Any,
    *,
    query: Any = None,
    method: Optional[str] = None,
    subtype_filter: Any = None,
    path_prefix: Any = None,
) -> Optional[Plan]:
    """The grep over file texts a read of file names names, or None.

    `predicate` is the call's condition as a predicate tree (search's
    `where`, filter's composed `predicate`); `query` the search text, None on
    `filter`; `subtype_filter` a type the call named outside its condition.
    """
    conjuncts = _conjuncts(predicate)
    if conjuncts is None:
        return None
    types: Optional[frozenset[str]] = None
    if isinstance(subtype_filter, str) and subtype_filter:
        types = frozenset({subtype_filter})
    repos: set[str] = set()
    path_tests: list[str] = []
    for clause in conjuncts:
        field, op, value = clause.get("field"), clause.get("op"), clause.get("value")
        if field in _TYPE_FIELDS:
            if op == "eq" and isinstance(value, str):
                named = frozenset({value})
            elif op == "in" and isinstance(clause.get("values"), list) and all(
                isinstance(v, str) for v in clause["values"]
            ):
                named = frozenset(clause["values"])
            else:
                return None
            types = named if types is None else types & named
        elif field in _PATH_FIELDS and op == "contains" and isinstance(value, str):
            path_tests.append(value)
        elif field == "attrs.repo" and op == "eq" and isinstance(value, str) and _REPO.fullmatch(value):
            repos.add(value)
        elif field in _ID_FIELDS and op == "prefix" and isinstance(value, str):
            match = _ID_REPO.match(value)
            if match is None or match.end() != len(value):
                return None
            repos.add(match.group(1))
        elif field == "source" and op == "eq" and isinstance(value, str):
            repo = value.split(".", 1)[0]
            if not _REPO.fullmatch(repo):
                return None
            repos.add(repo)
        else:
            # A condition the file-text search cannot carry as written.
            return None
    if len(repos) > 1:
        return None
    repo = next(iter(repos)) if repos else None
    prefix = path_prefix if isinstance(path_prefix, str) and path_prefix else None
    text = query.strip() if isinstance(query, str) else ""
    if not text:
        if len(path_tests) != 1:
            return None
        if types is not None and not (types and types <= FILE_SUBTYPES):
            return None
        term = _grep_term(path_tests[0])
        if term is None:
            return None
        return Plan("path_test", term, repo, prefix, 1)
    if method not in ("grep", "lexical"):
        return None
    if path_tests:
        # A query over file TEXTS with a path test (CompOps q020, new t1
        # call 8: `query="reaper"`, `type = document`, `attrs.path contains
        # reaper`). The text search ran, but only over the files whose names
        # match, so `prod-rucio-daemons.yaml` never came up. The named call is
        # the same query without the path test.
        if len(path_tests) != 1 or not types or not types <= CONTENT_SUBTYPES:
            return None
        terms = _query_terms(text)
        if terms is None:
            return None
        return Plan("path_filter", text, repo, prefix, terms)
    if types is None or not types or not types <= NAME_ONLY_SUBTYPES:
        return None
    terms = _query_terms(text)
    if terms is None:
        return None
    return Plan("file_records", text, repo, prefix, terms)


def hint_block(found: Plan, *, total: Optional[int], basis: Optional[str]) -> dict[str, Any]:
    """The payload block: the plan, the named call and its count.

    `total`/`basis` are the named call's own `total_count` and
    `total_count_basis`; `total=None` means it was not counted in time."""
    counted = total if isinstance(total, int) and basis in ("exact", "lower_bound") else None
    block: dict[str, Any] = {
        "reason": found.reason,
        "query": found.query,
        "terms": found.terms,
        "count": counted,
        "count_basis": basis if counted is not None else "not_counted",
    }
    if found.repo is not None:
        block["repo"] = found.repo
    if counted is None or counted > 0:
        block["call"] = {"tool": "search", "arguments": found.arguments()}
    return block


def _call_text(call: Mapping[str, Any]) -> Optional[str]:
    arguments = call.get("arguments")
    if call.get("tool") != "search" or not isinstance(arguments, Mapping):
        return None
    return "search(" + ", ".join(
        f"{key}={json.dumps(value, separators=(',', ':'), ensure_ascii=False)}"
        for key, value in arguments.items()
    ) + ")"


def render(value: Any) -> Optional[str]:
    """The one line a reply prints for :data:`NAMES_ONLY_KEY`, or None."""
    if not isinstance(value, Mapping):
        return None
    why = _WHY.get(str(value.get("reason")))
    query = value.get("query")
    if why is None or not isinstance(query, str):
        return None
    term = query if query.startswith('"') else json.dumps(query, ensure_ascii=False)
    several = isinstance(value.get("terms"), int) and value["terms"] > 1
    repo = value.get("repo")
    scope = f" in {repo}" if isinstance(repo, str) else ""
    head = f"! matched file names only ({why}); "
    count = value.get("count")
    call = value.get("call")
    call_text = _call_text(call) if isinstance(call, Mapping) else None
    if count == 0:
        holds = "holds every term of" if several else "contains"
        return head + f"no file text{scope} {holds} {term}"
    if call_text is None:
        return None
    if not isinstance(count, int):
        return head + f"file texts{scope} holding {term} were not counted in time: {call_text}"
    number = f"at least {count:,}" if value.get("count_basis") == "lower_bound" else f"{count:,}"
    noun = "file text" if count == 1 and value.get("count_basis") != "lower_bound" else "file texts"
    if several:
        verb = "holds every term of" if noun == "file text" else "hold every term of"
    else:
        verb = "contains" if noun == "file text" else "contain"
    return head + f"{number} {noun}{scope} {verb} {term}: {call_text}"

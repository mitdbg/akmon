"""Search arguments that can mean only one valid call (issue #3666, step 2b).

Agents pass a source name as `source_family` and a record type as `profile`.
Neither names what the argument takes, and both have one reading that can
only narrow the search:

* `source_family=<S>`, where S is no declared family, is a condition on the
  source: `where` source = S, AND-ed with the caller's `where`;
* `profile=<T>`, where T is no declared profile, is a condition on the type:
  `where` type = T.

:func:`read_scope_arguments` makes that rewrite from the arguments and the
deployment's declared names alone. The search tool calls it first, so the
call that runs is the corrected call from its first line, and the reply to
it is that call's reply plus an `adjusted` note. Whether S is a source and T
a record type is checked once the session is open (:func:`source_known`,
:func:`is_record_type`), and that check can only refuse.

Sources are read without confirming one a reader may not see. With access
rules off the maintained count rollup lists every source at the pin. Under
access rules only the deployment's producer registry is read, which is
policy state and not derived from rows; a name it does not hold is neither
confirmed nor denied, and the call is refused the same way whether or not
the graph has such a source.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

__all__ = [
    "RULE_CONTINUATION_TOKEN_METHOD",
    "RULE_PROFILE_AS_TYPE",
    "RULE_SOURCE_FAMILY_AS_SOURCE",
    "ScopeReading",
    "SourceNames",
    "close_names",
    "continuation_kind",
    "is_record_type",
    "load_source_names",
    "read_continuation_method",
    "read_scope_arguments",
    "source_condition",
    "type_condition",
]

RULE_SOURCE_FAMILY_AS_SOURCE = "source_family_as_source"
RULE_PROFILE_AS_TYPE = "profile_as_type"
RULE_CONTINUATION_TOKEN_METHOD = "continuation_token_method"

#: The reserved profile that means unscoped (`search_profiles.RESERVED_ALL`).
_RESERVED_ALL = "all"
#: Methods whose ranked or scanned read serves a `where` on the source.
_SOURCE_METHODS = ("lexical", "grep")
#: Methods that serve a `where` on the type.
_TYPE_METHODS = ("lexical", "grep", "attrs")
#: A name a note may echo: printed on one line of the text reply.
_PLAIN_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:/\-]{0,95}\Z")
#: Most close source names a refusal lists.
MAX_CLOSE_NAMES = 5


def source_condition(name: str) -> dict[str, Any]:
    return {"op": "eq", "field": "source", "value": name}


def type_condition(name: str) -> dict[str, Any]:
    return {"op": "eq", "field": "type", "value": name}


def _and(where: Optional[Mapping[str, Any]], clause: dict[str, Any]) -> dict[str, Any]:
    """`where` AND `clause`, flattened one level; nothing of `where` is lost."""
    if not where:
        return clause
    if where.get("op") == "and" and isinstance(where.get("clauses"), list):
        return {"op": "and", "clauses": [*where["clauses"], clause]}
    return {"op": "and", "clauses": [dict(where), clause]}


def _one_name(value: Any) -> Optional[str]:
    """The single plain name an argument holds, or None."""
    if isinstance(value, (list, tuple)) and len(value) == 1:
        value = value[0]
    if not isinstance(value, str):
        return None
    name = value.strip()
    return name if _PLAIN_NAME.match(name) else None


@dataclass(frozen=True)
class ScopeReading:
    """One search call's `where`, `source_family` and `profile` as they run.

    `source` and `record_type` are the names the rewrite read as a source
    and as a record type: each must be checked once the session is open.
    `notes` are the `adjusted` records the reply carries.
    """

    where: Optional[dict[str, Any]]
    source_family: Any
    profile: Optional[str]
    source: Optional[str] = None
    record_type: Optional[str] = None
    notes: tuple[dict[str, str], ...] = ()


def read_scope_arguments(
    *,
    source_family: Any,
    profile: Any,
    where: Optional[Mapping[str, Any]],
    method: Any,
    query: Any,
    view: Any,
    declared_families: Optional[Sequence[str]],
    declared_profiles: Optional[Sequence[str]],
) -> ScopeReading:
    """Read a source name sent as `source_family` and a record type sent as
    `profile` as the `where` conditions they mean. Pure.

    `declared_families` and `declared_profiles` are the deployment's own
    names, or None when its manifest could not be read: then nothing is
    rewritten and the call meets the checks it always met. A declared name,
    several names, a blank or unprintable name, a windows read and a method
    that serves no such condition are all left exactly as sent.

    A source is read only beside query text on lexical or grep: the
    query-less lookup pages before it knows a row's source, so a source
    condition alone would page through every record.
    """
    out_where = dict(where) if where else None
    notes: list[dict[str, str]] = []
    source: Optional[str] = None
    record_type: Optional[str] = None
    if view == "windows":
        return ScopeReading(out_where, source_family, profile)
    name = _one_name(source_family)
    if (
        name is not None
        and declared_families is not None and name not in declared_families
        and method in _SOURCE_METHODS
        and isinstance(query, str) and query.strip()
    ):
        source = name
        out_where = _and(out_where, source_condition(name))
        source_family = None
        notes.append({
            "rule": RULE_SOURCE_FAMILY_AS_SOURCE,
            "sent": f"source_family `{name}`",
            "served": f"where source = `{name}`",
        })
    name = _one_name(profile)
    if (
        name is not None and name != _RESERVED_ALL
        and declared_profiles is not None and name not in declared_profiles
        and method in _TYPE_METHODS
    ):
        record_type = name
        out_where = _and(out_where, type_condition(name))
        profile = None
        notes.append({
            "rule": RULE_PROFILE_AS_TYPE,
            "sent": f"profile `{name}`",
            "served": f"where type = `{name}`",
        })
    return ScopeReading(
        out_where, source_family, profile,
        source=source, record_type=record_type, notes=tuple(notes),
    )


# ---------- a continuation token's method ----------


def continuation_kind(token: Any) -> Optional[str]:
    """The kind a continuation token's verified binding names, or None when
    the token does not verify (its refusal comes from the read it reaches)."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.continuation import open_continuation

    try:
        binding = open_continuation(token)["binding"]
    except (SubstrateError, ValueError, TypeError, KeyError):
        return None
    kind = binding.get("kind") if isinstance(binding, Mapping) else None
    return kind if isinstance(kind, str) else None


def read_continuation_method(
    method: str, continuation_token: Any, *, method_sent: Optional[bool],
) -> tuple[str, tuple[dict[str, str], ...]]:
    """The method a call with a grep page's token means (Format v10 item G2).

    A grep page names its token as `continuation_token=<token>`; agents sent
    it back without `method`, and search, defaulting to lexical, refused the
    token it had just printed. A verified `grep_page` token on a call that
    did not send `method` can only mean grep, so it is read as grep with an
    `adjusted` note. An explicit other method is left as sent (and refused
    by name), as is a token that does not verify. `method_sent` is None when
    the sent arguments are unknown (an in-process call): read as not sent.
    """
    if continuation_token is None or method == "grep" or method_sent:
        return method, ()
    if continuation_kind(continuation_token) != "grep_page":
        return method, ()
    return "grep", ({
        "rule": RULE_CONTINUATION_TOKEN_METHOD,
        "sent": "continuation_token of a grep page, no method",
        "served": 'method="grep"',
    },)


# ---------- what the deployment has ----------


@dataclass(frozen=True)
class SourceNames:
    """The source names the read path may state, and whether they are all.

    `names` come from the count rollup (every source at the pin, read only
    with access rules off) and the producer registry. `complete` is true
    only when the rollup answered: then a name outside `names` is not a
    source. Otherwise such a name is unknown, neither confirmed nor denied.
    """

    names: frozenset[str] = frozenset()
    complete: bool = False

    def known(self, name: str) -> Optional[bool]:
        """True for a source, False for a name that is none, None when the
        list cannot say."""
        if name in self.names:
            return True
        return False if self.complete else None


def close_names(name: str, names: frozenset[str]) -> list[str]:
    """A handful of real names close to `name`, nearest first."""
    pool = sorted(names)
    found = difflib.get_close_matches(name, pool, n=MAX_CLOSE_NAMES, cutoff=0.6)
    tail = name.rsplit(".", 1)[-1]
    for candidate in pool:
        if len(found) >= MAX_CLOSE_NAMES:
            break
        if candidate not in found and tail and candidate.rsplit(".", 1)[-1] == tail:
            found.append(candidate)
    return [candidate for candidate in found if _PLAIN_NAME.match(candidate)]


def _registry_names(conn: Any, deployment: str) -> frozenset[str]:
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT producer_id FROM okg.producer_profiles "
                    "WHERE deployment_name = %(deployment)s",
                    {"deployment": deployment},
                )
                return frozenset(str(row[0]) for row in cur.fetchall() if row[0])
    except Exception:  # noqa: BLE001 - an unreadable registry names nothing.
        return frozenset()


def _rollup_names(conn: Any, session: Any) -> Optional[frozenset[str]]:
    from okg.substrate.mcp.dataset_card import _rollup_counts

    try:
        with conn.transaction():
            rows, _reason = _rollup_counts(conn, session.generation_id, session.branch_name)
    except Exception:  # noqa: BLE001 - no rollup: the list is not known to be whole.
        return None
    if rows is None:
        return None
    return frozenset(str(source) for source, _subtype, _n in rows if source)


def load_source_names(conn: Any, session: Any, *, access_rules: bool) -> SourceNames:
    """The sources this deployment has, as far as they may be stated.

    `access_rules` is whether a policy can hide rows from a reader (any mode
    but off). Then nothing derived from rows is read.
    """
    registry = _registry_names(conn, session.deployment_name)
    if access_rules:
        return SourceNames(names=registry, complete=False)
    rollup = _rollup_names(conn, session)
    if rollup is None:
        return SourceNames(names=registry, complete=False)
    return SourceNames(names=registry | rollup, complete=True)


def is_record_type(conn: Any, session: Any, name: str) -> bool:
    """Whether the pinned catalog has a record type called `name`."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM okg.subtypes "
            "WHERE catalog_version_id = %(cv)s AND name = %(name)s LIMIT 1",
            {"cv": session.catalog_version_id, "name": name},
        )
        return cur.fetchone() is not None

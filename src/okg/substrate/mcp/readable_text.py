"""Admit a matched text unit only when the reader reads its text as stored.

Lexical search ranks, and grep matches, the STORED text of a row. Under a
Nomos policy that can redact, the reader may be served that text masked
(`sensitive_attribute`, `[nomos:masked]`) or not at all (`free_text`). A row
admitted on a term only its hidden text holds would answer a query that an
absent term leaves empty: the difference confirms what the hidden text says
(issues #3377 and #3384).

So every site that admits a matched unit keeps it only when the attributes
that decided the match -- its `text`, plus whatever else the caller names --
read exactly as stored, after the read filter's masking and the effective-
attribute overlay. The hit is dropped, never re-matched against the masked
text: a pattern that matches the mask would be its own oracle.

The check runs only when the active policy can redact (`redaction_possible`,
decided at the policy level, never per row). It fails closed: a row the read
filter does not return, or returns with the deciding attribute changed,
missing or masked, is dropped whatever its stored read class says.
"""
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Sequence, Union

import psycopg

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.nomos.read_filter import filter_rows_by_read_class

#: The attributes that decide whether a lexical or grep unit matched.
TEXT_DECISIVE = frozenset({"text"})

Decisive = Union[Iterable[str], Callable[[Mapping[str, Any]], Iterable[str]]]

_ABSENT = object()


def redaction_possible(conn: psycopg.Connection, session: Any) -> bool:
    """Whether the active policy can redact a row for any reader (policy level,
    never per row, so the answer reveals nothing about the match set)."""
    from okg.substrate.nomos.runtime_enforcement import deployment_requires_nomos_runtime
    from okg.substrate.nomos.store import load_active_policy_bundle

    if not deployment_requires_nomos_runtime(session.deployment_name):
        return False
    bundle = load_active_policy_bundle(
        conn,
        deployment_name=session.deployment_name,
        scope=f"deployment:{session.deployment_name}",
    )
    if bundle is None:
        return False
    return bundle.default_effect == "redact" or any(
        rule.effect == "redact" for rule in bundle.rules
    )


def _keys(decisive: Decisive, row: Mapping[str, Any]) -> Iterable[str]:
    return decisive(row) if callable(decisive) else decisive


def unchanged_by_read(
    stored: Sequence[Mapping[str, Any]],
    read: Sequence[dict[str, Any]],
    *,
    decisive: Decisive = TEXT_DECISIVE,
) -> list[dict[str, Any]]:
    """The rows of `read` whose deciding attributes equal those of the same
    row (by `node_id`) in `stored`, in `read`'s order.

    `read` is what a read filter returned for `stored`; a row it dropped, or
    whose deciding attribute it changed, removed or masked, is left out. An
    attribute absent on both sides is unchanged."""
    before_by_id = {str(row["node_id"]): row for row in stored}
    out = []
    for row in read:
        before = before_by_id.get(str(row["node_id"]))
        if before is None:
            continue
        attrs_before = before.get("attrs") or {}
        attrs_after = row.get("attrs") or {}
        if all(
            attrs_before.get(key, _ABSENT) == attrs_after.get(key, _ABSENT)
            for key in _keys(decisive, row)
        ):
            out.append(row)
    return out


def read_unchanged(
    conn: psycopg.Connection,
    session: Any,
    rows: list[dict[str, Any]],
    *,
    decisive: Decisive = TEXT_DECISIVE,
) -> list[dict[str, Any]]:
    """The rows as the session reads them (masked, overlaid), keeping only
    those whose deciding attributes the read leaves unchanged.

    `filter_rows_by_read_class` records `read` decisions for these rows: under
    a redacting policy the decision needs the masked view (PACT mcp-grep-3067).
    """
    raw = apply_effective_attrs_to_rows(
        conn, [dict(r) for r in rows], target_kind="node", target_id_field="node_id",
        generation_id=session.generation_id,
    )
    read = filter_rows_by_read_class(conn, session, rows, row_kind="node")
    read = apply_effective_attrs_to_rows(
        conn, read, target_kind="node", target_id_field="node_id",
        generation_id=session.generation_id,
    )
    return unchanged_by_read(raw, read, decisive=decisive)


def keep_readable_text(
    stored: Sequence[Mapping[str, Any]],
    read: Sequence[dict[str, Any]],
    *,
    redaction: bool,
) -> list[dict[str, Any]]:
    """`read` (the read filter's rows for the matched units `stored`), keeping
    only the units whose text the read left as stored when the policy can
    redact; `read` unchanged otherwise (Nomos off, or no redacting rule)."""
    if not redaction:
        return list(read)
    return unchanged_by_read(stored, read, decisive=TEXT_DECISIVE)

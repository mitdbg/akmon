"""The database reads lane ri F6 (PACT read-empty-why-f6) adds to a filter call.

F6 runs only on an empty first page (offset 0, no keyset continuation). On
filter each of its reads is one effective-view page statement: a probe (the
condition with one part dropped) reads `LOOSENED_ROWS` rows, the value read
`VALUE_ROWS` rows. They run after the call's own page reads. These helpers
assert their number exactly (each also carries `page_limit` 4 or 41, a
sanity check only: a call's own read with limit 3 or 40 has the same): the number the condition's
shape gives (one probe per droppable part, at most the read cap (`empty_why.read_cap`); none for a
one-condition read; one value read when the block says it ran), which must
also be the number the reply's `empty_why` block reports. Every statement
is counted: the call's own page reads are the rest, and
test_empty_why_db.py proves a page with rows makes no F6 read at all.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from okg.substrate.mcp import empty_why

#: `page_limit` of an F6 page statement on filter: a probe, the value read.
F6_PAGE_LIMITS = frozenset({empty_why.LOOSENED_ROWS + 1, empty_why.VALUE_ROWS + 1})


def f6_page_reads(page: Any) -> int:
    """The page reads the F6 block of `page` reports: one per probe that ran
    (not `skipped`) and one for the value read."""
    why = page.get("empty_why") if isinstance(page, Mapping) else None
    if not isinstance(why, Mapping):
        return 0
    ran = sum(
        1 for probe in why.get("probes") or ()
        if probe.get("status") not in (empty_why.SKIPPED, empty_why.LIMITED)
    )
    return ran + (1 if why.get("values_read") else 0)


def expected_f6_reads(scope: Mapping[str, Any], predicate: Any, page: Any) -> int:
    """F6's page reads on a filter call over `scope` with `predicate`, from
    the condition's shape: none on a page with rows; one probe per part
    (the scope's type, each top-level clause), at most the reply's read cap
    (`empty_why.read_cap`), or none
    when there is only one part; plus the value read when the block ran it."""
    why = page.get("empty_why") if isinstance(page, Mapping) else None
    if not isinstance(why, Mapping):
        return 0
    clauses = predicate.get("clauses") if isinstance(predicate, Mapping) and (
        predicate.get("op") == "and") else ([predicate] if predicate else [])
    parts = (1 if scope.get("subtype") else 0) + len(clauses)
    cap = empty_why.read_cap("filter", not why.get("access_rules"))
    probes = 0 if parts <= 1 else min(parts, cap)
    return probes + (1 if why.get("values_read") else 0)


def own_reads(
    statements: Sequence[tuple[str, Mapping[str, Any]]], page: Any,
    *, scope: Mapping[str, Any], predicate: Any,
) -> list[tuple[str, Mapping[str, Any]]]:
    """The call's own page statements, after asserting the F6 ones. F6 reads
    run after the call's own, so they are the last `expected_f6_reads`
    statements: each must carry an F6 `page_limit`, and their number must be
    what the block reports. A page without a block has no F6 statement."""
    expected = expected_f6_reads(scope, predicate, page)
    assert expected == f6_page_reads(page), (expected, f6_page_reads(page), page.get("empty_why"))
    if not expected:
        return list(statements)
    assert len(statements) >= expected, (len(statements), expected)
    f6 = statements[len(statements) - expected:]
    assert all((s[1] or {}).get("page_limit") in F6_PAGE_LIMITS for s in f6), [
        (s[1] or {}).get("page_limit") for s in statements]
    return list(statements[: len(statements) - expected])

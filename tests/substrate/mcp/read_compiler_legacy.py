"""The pre-change invariant SQL helpers, frozen for old-versus-new comparison.

Copied verbatim (minus the refusal branch) from ``search_where`` at
835121b56f3a494edc413e0d5dcdbf82e7dfd102, the base PACT
shared-read-scanners-3199 stacks on. Tests compare the shared compiler with
this copy; nothing in ``src/`` imports it.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Mapping, Optional, Sequence


def legacy_bind(params: dict[str, Any], prefix: str, value: Any) -> str:
    index = len(params)
    name = f"{prefix}{index}"
    while name in params:
        index += 1
        name = f"{prefix}{index}"
    params[name] = value
    return f"%({name})s"


def legacy_sql(
    predicate: Mapping[str, Any],
    params: dict[str, Any],
    *,
    columns: Mapping[str, str],
    prefix: str,
) -> str:
    op = predicate["op"]
    if op in ("and", "or"):
        joiner = " AND " if op == "and" else " OR "
        return "(" + joiner.join(
            legacy_sql(c, params, columns=columns, prefix=prefix)
            for c in predicate["clauses"]
        ) + ")"
    if op == "not":
        return f"(NOT {legacy_sql(predicate['clause'], params, columns=columns, prefix=prefix)})"
    column = columns[predicate["field"]]
    if op == "exists":
        return f"({column} IS NOT NULL)"
    if op == "eq":
        value = predicate.get("value")
        if value is None:
            return f"({column} IS NULL)"
        if not isinstance(value, str):
            return "FALSE"
        return f"COALESCE({column} = {legacy_bind(params, prefix, value)}, FALSE)"
    if op == "in":
        texts = [v for v in predicate["values"] if isinstance(v, str)]
        parts = []
        if texts:
            parts.append(f"COALESCE({column} = ANY({legacy_bind(params, prefix, texts)}), FALSE)")
        if any(v is None for v in predicate["values"]):
            parts.append(f"({column} IS NULL)")
        return "(" + " OR ".join(parts) + ")" if parts else "FALSE"
    if op in ("prefix", "suffix"):
        bound = legacy_bind(params, prefix, predicate["value"])
        side = "left" if op == "prefix" else "right"
        return f"COALESCE({side}({column}, char_length({bound})) = {bound}, FALSE)"
    if op in ("lt", "gt", "between"):
        bounds = predicate["values"] if op == "between" else [predicate.get("value")]
        if not all(isinstance(b, str) for b in bounds):
            return "FALSE"
        if op == "between":
            low, high = (legacy_bind(params, prefix, b) for b in bounds)
            return f"COALESCE({column} BETWEEN {low} AND {high}, FALSE)"
        sign = "<" if op == "lt" else ">"
        return f"COALESCE({column} {sign} {legacy_bind(params, prefix, bounds[0])}, FALSE)"
    raise AssertionError(f"legacy helper never lowered {op!r}")


def legacy_invariant_sql(
    conjuncts: Sequence[Mapping[str, Any]],
    params: dict[str, Any],
    *,
    columns: Mapping[str, str],
    prefix: str = "w",
) -> list[str]:
    return [legacy_sql(c, params, columns=columns, prefix=prefix) for c in conjuncts]


def legacy_compile_where(
    predicate: Optional[Mapping[str, Any]], *, columns: Mapping[str, str],
    prefix: str = "w", params: Optional[Mapping[str, Any]] = None,
) -> SimpleNamespace:
    """What base lexical search put in its ranked SQL, in the new call shape.

    Base lowered only top-level conjuncts that read id/type/source alone.
    """
    from okg.substrate.mcp import search_where

    seed = dict(params or {})
    bound = dict(seed)
    parts = legacy_invariant_sql(
        search_where.invariant_conjuncts(predicate), bound, columns=columns, prefix=prefix,
    )
    sql = " AND ".join(parts) if parts else "TRUE"
    own = {k: v for k, v in bound.items() if k not in seed}

    def bind_into(target: dict[str, Any]) -> str:
        # Same merge contract as CompiledPredicate.bind_into.
        assert not set(own) & set(target), sorted(set(own) & set(target))
        target.update(own)
        return sql

    # `exact` when the lowered conjuncts are the whole predicate, as the
    # shared compiler says it (issue #3666 reads it).
    whole = len(search_where.invariant_conjuncts(predicate)) == len(
        search_where.top_conjuncts(dict(predicate) if predicate is not None else None)
    )
    return SimpleNamespace(sql=sql, params=dict(own), bind_into=bind_into,
                           qualification="exact" if whole else "superset")

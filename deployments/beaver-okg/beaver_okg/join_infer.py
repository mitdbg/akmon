"""Instance-overlap join-key inference over a relational database (reusable).

Finds likely join keys that FK declarations and name-matching MISS: pairs of
columns in different tables whose VALUE SETS overlap. Names are not used, so it
catches joins like `COURSE...MEET_PLACE -> FCLT_ROOMS.FCLT_ROOM_KEY` (different
names, same values) and avoids same-name-different-meaning false positives.

Method (bounded, value-driven):
  1. candidate columns = joinable type, per database (skip text/date/blob/bool).
  2. sample up to `row_sample` rows per column -> set of distinct string values
     (bounded scan; no full DISTINCT over 9M-row tables).
  3. inverted index value -> columns; candidate pairs share >= `min_shared`
     sampled values (cheap blocking, avoids O(n^2)).
  4. containment(c1,c2) = |shared| / min(|vals(c1)|, |vals(c2)|); emit a join
     if containment >= `min_containment`.

Returns directed (fk_col, pk_col, score) where pk is the higher-cardinality
(more-contained-into) side. Used by the join enricher (emits `references`
edges) and by eval_joins (F1 vs name-match against the gold annotations).
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

# columns whose VALUES can be join keys (ids/codes/strings/numbers), not free text
JOINABLE_TYPES = {
    "int", "integer", "bigint", "smallint", "mediumint", "tinyint",
    "decimal", "numeric", "char", "varchar",
}
SKIP_TYPES = {"text", "longtext", "mediumtext", "tinytext", "blob", "json",
              "datetime", "timestamp", "date", "time", "year", "float", "double"}


@dataclass(frozen=True)
class JoinCandidate:
    a: str  # "db.table.column" (fk side)
    b: str  # "db.table.column" (pk side)
    containment: float
    shared: int


def _qi(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def _candidate_columns(cur, db: str) -> list[tuple[str, str]]:
    cur.execute(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s",
        (db,),
    )
    out = []
    for t, c, dt in cur.fetchall():
        if (dt or "").lower() in SKIP_TYPES:
            continue
        if (dt or "").lower() in JOINABLE_TYPES:
            out.append((t, c))
    return out


def _sample_values(cur, db: str, table: str, col: str, row_sample: int) -> set[str]:
    cur.execute(
        f"SELECT {_qi(col)} FROM {_qi(db)}.{_qi(table)} "
        f"WHERE {_qi(col)} IS NOT NULL LIMIT {int(row_sample)}"
    )
    return {str(r[0]).strip() for r in cur.fetchall() if r[0] is not None and str(r[0]).strip()}


def infer_joins_for_db(
    connect: Callable[[], object],
    db: str,
    *,
    row_sample: int = 5000,
    min_distinct: int = 16,
    min_shared: int = 5,
    min_containment: float = 0.85,
) -> list[JoinCandidate]:
    conn = connect()
    try:
        cur = conn.cursor()
        cols = _candidate_columns(cur, db)
        vals: dict[tuple[str, str], set[str]] = {}
        for t, c in cols:
            try:
                s = _sample_values(cur, db, t, c, row_sample)
            except Exception:
                continue
            if len(s) >= min_distinct:
                vals[(t, c)] = s
        cur.close()
    finally:
        conn.close()

    # inverted index value -> columns (blocking)
    index: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for key, s in vals.items():
        for v in s:
            index[v].append(key)

    # count shared sampled values per candidate pair (different tables)
    shared: dict[tuple, int] = defaultdict(int)
    for cols_with_v in index.values():
        if len(cols_with_v) < 2:
            continue
        for i in range(len(cols_with_v)):
            for j in range(i + 1, len(cols_with_v)):
                a, b = cols_with_v[i], cols_with_v[j]
                if a[0] == b[0]:  # same table — not a join
                    continue
                shared[tuple(sorted((a, b)))] += 1

    out: list[JoinCandidate] = []
    for (k1, k2), n in shared.items():
        if n < min_shared:
            continue
        s1, s2 = vals[k1], vals[k2]
        inter = len(s1 & s2)
        if inter < min_shared:
            continue
        c1 = inter / len(s1)  # containment of k1 into k2
        c2 = inter / len(s2)
        best = max(c1, c2)
        if best < min_containment:
            continue
        # fk side = the more-contained one (its values are a subset of the other)
        if c1 >= c2:
            fk, pk, cont = k1, k2, c1
        else:
            fk, pk, cont = k2, k1, c2
        out.append(JoinCandidate(
            a=f"{db}.{fk[0]}.{fk[1]}", b=f"{db}.{pk[0]}.{pk[1]}",
            containment=round(cont, 4), shared=inter,
        ))
    return out


def infer_joins(connect: Callable[[], object], databases: list[str], **kw) -> list[JoinCandidate]:
    out: list[JoinCandidate] = []
    for db in databases:
        out.extend(infer_joins_for_db(connect, db, **kw))
    return out

"""Reference interpreter for the transform IR -- the executable denotation.

This module gives each operator a concrete meaning ``[[op]]`` over the bag
carrier (a relation is a ``list[dict]`` multiset). It is the line between an
algebra and a vocabulary list (design 01, req.gtir.language), and it is the
equivalence oracle for the SQL lowering (task.gtir.normalize-and-lower).

The non-relational pieces (predicates, projections, join conditions, group
aggregates, search scorers, diff keys, CALL bodies) are supplied as named
``kernels`` -- the same discipline ``CALL`` uses for opaque computation. So
``[[WHERE p]](bag) = [r for r in bag if kernels[p](r)]``.

Order discipline (design 01) is realized here: SEARCH/ORDER return ordered
relations; WHERE/SELECT/LIMIT preserve order; JOIN/GROUP/UNION are bag operations.
Guard-dominance (req.gtir.guarded-mutation) is realized at runtime: a DELTA whose
transitively-consumed AssertionResult failed yields the empty DeltaSet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from .expr import _jsonb_element_text, eval_expr
from .ir import (
    _RANK_PEER_INTERNAL,
    AssertionResult,
    Block,
    DeltaRow,
    DeltaSet,
    TransformProgram,
)
from .registry import DeltaOp, OpKind, TerminatorKind

Row = dict[str, Any]
Relation = list[Row]
Kernels = Mapping[str, Callable[..., Any]]

CHANGE_OP = "_change_op"  # field DIFF stamps on each change row


class InterpreterError(Exception):
    pass


@dataclass
class EvalResult:
    outputs: dict[str, Any] = field(default_factory=dict)  # EMIT'd, by target name
    values: dict[str, Any] = field(default_factory=dict)  # every bound symbol


def _backward_closure(program: TransformProgram, start: set[str]) -> set[str]:
    """All symbols that transitively feed ``start`` along SSA def-use edges."""
    out = {b.output: b for _blk, b in program.iter_ops() if b.output is not None}
    frontier = set(start)
    reached: set[str] = set()
    while frontier:
        sym = frontier.pop()
        if sym in reached:
            continue
        reached.add(sym)
        binding = out.get(sym)
        if binding is not None:
            frontier.update(binding.inputs)
    return reached


def evaluate(
    program: TransformProgram,
    surfaces: Mapping[str, Relation],
    kernels: Kernels,
    params: Mapping[str, Any] | None = None,
) -> EvalResult:
    """Run a transform program. Pure: no durable writes, only values + outputs."""
    params = dict(params or {})
    res = EvalResult(values=dict(params))
    checksum = program.checksum()
    blocks = {b.block_id: b for b in program.blocks}
    _run_block(program, blocks, program.entry_block, res, surfaces, kernels, checksum, depth=0)
    return res


def _run_block(
    program: TransformProgram,
    blocks: Mapping[str, Block],
    block_id: str,
    res: EvalResult,
    surfaces: Mapping[str, Relation],
    kernels: Kernels,
    checksum: str,
    depth: int,
) -> None:
    block = blocks[block_id]
    for binding in block.bindings:
        value = _eval_op(program, binding, res, surfaces, kernels, checksum)
        if binding.output is not None:
            res.values[binding.output] = value
    _run_terminator(program, blocks, block, res, surfaces, kernels, checksum, depth)


def _run_terminator(program, blocks, block, res, surfaces, kernels, checksum, depth) -> None:
    term = block.terminator
    if term.kind is TerminatorKind.RETURN:
        return
    if term.kind is TerminatorKind.IF:
        cond = bool(res.values.get(term.args.get("condition")))
        target = term.args.get("then_block") if cond else term.args.get("else_block")
        if target in blocks:
            _run_block(program, blocks, target, res, surfaces, kernels, checksum, depth)
        return
    if term.kind is TerminatorKind.ITERATE:
        max_steps = int(term.args["max_steps"])
        state_sym = term.args.get("state_symbol", "state")
        step_block = term.args.get("step_block")
        stop = term.args.get("stop_predicate")
        state = res.values.get(term.args.get("state0"))
        for _ in range(max_steps):
            if stop is not None and kernels[stop](state):
                break
            if step_block not in blocks:
                break
            res.values[state_sym] = state
            _run_block(program, blocks, step_block, res, surfaces, kernels, checksum, depth + 1)
            state = res.values.get(
                blocks[step_block].terminator.args.get("values", [state_sym])[0], state
            )
        result_sym = term.args.get("result", state_sym)
        res.values[result_sym] = state
        res.outputs[result_sym] = state


def _eval_op(program, binding, res, surfaces, kernels, checksum) -> Any:
    op = binding.op
    a = op.args
    ins = [res.values.get(s) for s in binding.inputs]
    k = op.kind

    pv = res.values  # param values for expr evaluation

    if k is OpKind.FROM:
        return [dict(r) for r in surfaces.get(a["surface"], [])]
    if k is OpKind.WHERE:
        if "expr" in a:
            return [r for r in ins[0] if eval_expr(a["expr"], r, pv)]
        pred = kernels[a["predicate"]]
        return [r for r in ins[0] if pred(r)]            # preserves order
    if k is OpKind.SELECT:
        if "project" in a:
            out = [{n: eval_expr(e, r, pv) for n, e in a["project"].items()} for r in ins[0]]
            return _dedup(out) if a.get("distinct") else out
        proj = kernels[a["projection"]]
        return [proj(r) for r in ins[0]]                 # preserves order
    if k is OpKind.JOIN:
        if "on" in a:
            # Python ``==`` here (expr.py header): NULL = NULL keeps the pair.
            hashed = _hash_join(ins[0], ins[1], a.get("how", "inner"), a["on"], null_matches=True)
            if hashed is not None:
                return hashed
            cond = lambda lr, rr: eval_expr(a["on"], {**rr, **lr}, pv)  # noqa: E731
        else:
            cond = kernels[a["condition"]]
        return _join(ins[0], ins[1], a.get("how", "inner"), cond)
    if k is OpKind.GROUP:
        if isinstance(a["key"], str):                     # kernel-name form
            return _group(ins[0], kernels[a["key"]], kernels[a["agg"]])
        return _group_expr(ins[0], a["key"], a["agg"], pv)  # SQL-lowerable expr form
    if k is OpKind.UNION:
        out: Relation = []
        for rel in ins:
            out.extend(rel or [])
        return out
    if k is OpKind.ORDER:
        if "keys" in a:                                   # multi-key expr sort
            rows = list(ins[0])
            for kspec in reversed(a["keys"]):             # stable: primary applied last
                rows = sorted(
                    rows,
                    key=lambda r, e=kspec["expr"]: _sort_key(eval_expr(e, r, pv)),
                    reverse=bool(kspec.get("desc", False)),
                )
            return rows
        if "expr" in a:
            key = lambda r: eval_expr(a["expr"], r, pv)  # noqa: E731
        else:
            key = kernels[a["key"]]
        return sorted(ins[0], key=key, reverse=bool(a.get("desc", False)))  # introduces order
    if k is OpKind.LIMIT:                                  # preserves order
        rows = list(ins[0])
        off = int(a.get("offset") or 0)
        return rows[off: off + int(a["n"])]
    if k is OpKind.RANK:
        return _rank(ins[0], a, pv)
    if k is OpKind.UNNEST:
        exploded: Relation = []
        field = a["field"]
        for r in ins[0]:
            arr = eval_expr(a["expr"], r, pv)
            if arr is None:
                continue  # strict SRF: NULL array yields no rows
            if not isinstance(arr, (list, tuple)):
                raise InterpreterError(
                    f"UNNEST input is not a JSON array (got {type(arr).__name__})"
                )
            exploded.extend({**r, field: _jsonb_element_text(el)} for el in arr)
        return exploded
    if k is OpKind.SEARCH:
        scorer = kernels[a["scorer"]]
        query = res.values.get(a["query"], a["query"])
        rows = surfaces.get(a["surface"], [])
        scored = sorted(
            ({**r, "score": scorer(query, r)} for r in rows),
            key=lambda r: r["score"],
            reverse=True,
        )
        for rank, r in enumerate(scored):
            r["rank"], r["method"] = rank, a.get("method")
        return scored
    if k is OpKind.DIFF:
        return _diff(ins[0], ins[1], kernels[a["key"]])
    if k is OpKind.CALL:
        return kernels[a["function_id"]](*ins, **a.get("args", {}))
    if k is OpKind.ASSERT:
        pred = kernels[a["predicate"]]
        bag = ins[0] if ins else []
        passed = bool(pred(bag))
        offending = tuple() if passed else tuple(bag)
        return AssertionResult(contract_id=a["contract"], passed=passed, offending_rows=offending)
    if k is OpKind.DELTA:
        return _delta(program, binding, res, kernels, checksum)
    if k is OpKind.EMIT:
        value = ins[0] if ins else None
        res.outputs[a["target"]] = value
        return value
    raise InterpreterError(f"no denotation for op kind {k!r}")


_NO_MATCH = object()  # a key that equals nothing (NaN; NULL when NULL never matches)
_HASH_KEY_TYPES = frozenset({str, int, float, bool, type(None)})  # == agrees with hash()


def _hash_join(
    left: Relation, right: Relation, how: str, on: Any, *, null_matches: bool
) -> Relation | None:
    """``_join`` for an ``on`` made only of column equalities, in O(|left| + |right|).

    ``on`` must be ``eq(col, col)`` or a non-empty ``and`` of them, each pairing a
    column every left row carries with a column no left row carries (read from
    the right row, as the merged ``{**rr, **lr}`` lookup does), and every key
    value must be a ``str``/``int``/``float``/``bool``/``None``. Then the result
    is exactly the nested loop's: the same rows in the same order (left order,
    and right order within one left row), the same merged rows, the same
    unmatched rows for ``left``/``full``. ``null_matches`` says whether a NULL
    key equals a NULL key (Python ``==``) or nothing (SQL); NaN never matches.
    Returns None when ``on`` or the rows do not qualify; the caller then runs
    the nested loop. With an empty side the nested loop is already linear.
    """
    pairs = _column_equalities(on)
    if pairs is None or not left or not right:
        return None
    left_readers: list[Callable[[Row], Any]] = []
    right_readers: list[Callable[[Row], Any]] = []
    for x, y in pairs:
        sx, rx = _column_side(x, left)
        sy, ry = _column_side(y, left)
        if (sx, sy) == ("left", "right"):
            left_readers.append(rx)
            right_readers.append(ry)
        elif (sx, sy) == ("right", "left"):
            left_readers.append(ry)
            right_readers.append(rx)
        else:
            return None
    left_keys = _join_keys(left, left_readers, null_matches)
    right_keys = _join_keys(right, right_readers, null_matches)
    if left_keys is None or right_keys is None:
        return None
    index: dict[tuple, list[int]] = {}
    for j, key in enumerate(right_keys):
        if key is not _NO_MATCH:
            index.setdefault(key, []).append(j)
    out: Relation = []
    matched_right: set[int] = set()
    for lr, key in zip(left, left_keys):
        hits = index.get(key, ()) if key is not _NO_MATCH else ()
        for j in hits:
            out.append({**right[j], **lr})
            matched_right.add(j)
        if not hits and how in ("left", "full"):
            out.append(dict(lr))
    if how == "full":
        for j, rr in enumerate(right):
            if j not in matched_right:
                out.append(dict(rr))
    return out


def _column_equalities(on: Any) -> list[tuple[str, str]] | None:
    """The (column, column) pairs of ``eq(col, col)`` or a non-empty ``and`` of them."""
    if not isinstance(on, dict) or len(on) != 1:
        return None
    if "and" in on:
        terms = on["and"]
        if not isinstance(terms, list) or not terms:
            return None
    else:
        terms = [on]
    pairs: list[tuple[str, str]] = []
    for term in terms:
        if not isinstance(term, dict) or list(term) != ["eq"]:
            return None
        operands = term["eq"]
        if not isinstance(operands, (list, tuple)) or len(operands) != 2:
            return None
        names = []
        for operand in operands:
            if not isinstance(operand, dict) or list(operand) != ["col"]:
                return None
            if not isinstance(operand["col"], str):
                return None
            names.append(operand["col"])
        pairs.append((names[0], names[1]))
    return pairs


def _column_side(name: str, left: Relation) -> tuple[str, Callable[[Row], Any] | None]:
    """Which input a ``col`` resolves from in the merged row ``{**rr, **lr}``.

    ``eval_expr`` reads ``row[name]`` when present, else the bare name after the
    last dot. So the value comes from the left row when every left row has
    ``name``, and from the right row when no left row has ``name`` or its bare
    name. Anything else depends on the pair and is ``"mixed"``.
    """
    bare = name.split(".")[-1]
    if all(name in lr for lr in left):
        return "left", lambda r: r[name]
    if not any(name in lr or bare in lr for lr in left):
        return "right", lambda r: r[name] if name in r else r.get(bare)
    return "mixed", None


def _join_keys(
    rows: Relation, readers: list[Callable[[Row], Any]], null_matches: bool
) -> list[Any] | None:
    """One hash key per row (a tuple, or ``_NO_MATCH``); None if a value is not hashable-safe."""
    keys: list[Any] = []
    for r in rows:
        key = tuple(read(r) for read in readers)
        for v in key:
            if type(v) not in _HASH_KEY_TYPES:
                return None
        if any(v != v or (v is None and not null_matches) for v in key):
            keys.append(_NO_MATCH)  # NaN != NaN, so the nested loop never matches it
        else:
            keys.append(key)
    return keys


def _join(left: Relation, right: Relation, how: str, cond: Callable) -> Relation:
    """The nested loop: ``cond`` for every (left, right) pair, |left| x |right| calls."""
    out: Relation = []
    matched_right: set[int] = set()
    for lr in left:
        hit = False
        for j, rr in enumerate(right):
            if cond(lr, rr):
                out.append({**rr, **lr})
                matched_right.add(j)
                hit = True
        if not hit and how in ("left", "full"):
            out.append(dict(lr))            # right cols absent -> null on access
    if how == "full":
        for j, rr in enumerate(right):
            if j not in matched_right:
                out.append(dict(rr))
    return out


def _group(rel: Relation, key: Callable, agg: Callable) -> Relation:
    buckets: dict[Any, Relation] = {}
    for r in rel:
        buckets.setdefault(key(r), []).append(r)
    return [agg(rows) for rows in buckets.values()]


def _group_expr(rel: Relation, keys: Mapping, aggs: Mapping, pv: Mapping) -> Relation:
    """GROUP in the SQL-lowerable expr form: ``keys`` is {alias: expr}, ``aggs``
    is {alias: {fn, expr?}}. Mirrors lower.py's GROUP so SQL == interpreter."""
    buckets: dict[tuple, Relation] = {}
    order: list[tuple] = []
    aliases = list(keys.keys())
    for r in rel:
        kt = tuple(eval_expr(e, r, pv) for e in keys.values())
        if kt not in buckets:
            buckets[kt] = []
            order.append(kt)
        buckets[kt].append(r)
    out: Relation = []
    for kt in order:
        row = {aliases[i]: kt[i] for i in range(len(aliases))}
        for alias, spec in aggs.items():
            row[alias] = _agg_value(spec, buckets[kt], pv)
        out.append(row)
    return out


def _agg_value(spec: Mapping, rows: Relation, pv: Mapping) -> Any:
    fn = str(spec.get("fn", "count")).lower()
    vals = (
        [eval_expr(spec["expr"], r, pv) for r in rows] if "expr" in spec else None
    )
    if vals is not None:
        vals = [x for x in vals if x is not None]
    if fn == "count":
        return len(rows) if vals is None else len(vals)
    if vals is None:
        raise InterpreterError(f"agg {fn!r} requires an expr")
    if fn == "count_distinct":
        # Lowers to COUNT(DISTINCT expr). Exists so callers never reach for
        # a distinct SELECT ahead of a GROUP, which mis-compiles silently
        # (see the guard in transform/lower.py).
        return len(set(vals))
    if fn == "sum":
        return sum(vals)
    if fn == "min":
        return min(vals) if vals else None
    if fn == "max":
        return max(vals) if vals else None
    if fn == "avg":
        return sum(vals) / len(vals) if vals else None
    raise InterpreterError(f"unknown agg fn {fn!r}")


def _rank(rel: Relation, a: Mapping, pv: Mapping) -> Relation:
    """[[RANK]]: per-partition ranking under an ordered component list.

    Adds ``rank_as`` (1-based, deterministic via components then the optional
    tiebreak) to every row, and — when ``basis_as`` is set — the comparison
    provenance on each partition's LEADER row: the name of the first component
    separating it from the runner-up, ``"sole"`` for a one-row partition, or
    ``"tie"`` when every component agrees (the tiebreak deliberately does NOT
    participate in the basis). NULL ordering is pinned NULLS LAST in both
    directions (the SQL lowering renders it explicitly — PG's bare DESC would
    default NULLS FIRST and diverge).

    Determinism holds only when (components + tiebreak) form a KEY: on
    residual ties this stable sort ranks by input order while ROW_NUMBER's
    tie order is unspecified — the SQL backend is authoritative (expr.py
    header posture). The claim-model consumer always passes a unique
    claim_id tiebreak.
    """
    partition = a.get("partition", [])
    comps = a["components"]
    tiebreak = a.get("tiebreak")
    rank_as = str(a.get("rank_as", "rnk"))
    basis_as = a.get("basis_as")

    def _null_last_key(value: Any, desc: bool) -> tuple:
        if value is None:
            return (1, 0)  # NULLS LAST regardless of direction
        return (0, _Reversed(value) if desc else value)

    def _sort_tuple(r: Row) -> tuple:
        parts = [
            _null_last_key(eval_expr(c["expr"], r, pv), bool(c.get("desc")))
            for c in comps
        ]
        if tiebreak is not None:
            parts.append(
                _null_last_key(
                    eval_expr(tiebreak["expr"], r, pv), bool(tiebreak.get("desc"))
                )
            )
        return tuple(parts)

    buckets: dict[tuple, Relation] = {}
    order: list[tuple] = []
    for r in rel:
        kt = tuple(
            _freeze(eval_expr(e, r, pv)) for e in partition
        )
        if kt not in buckets:
            buckets[kt] = []
            order.append(kt)
        buckets[kt].append(r)

    peer = a.get("peer")

    out: Relation = []
    for kt in order:
        rows = sorted(buckets[kt], key=_sort_tuple)
        basis: Any = None
        if basis_as is not None:
            if len(rows) == 1:
                basis = "sole"
            else:
                top = rows[0]
                if peer is None:
                    runner: Row | None = rows[1]
                else:
                    # The runner-up is the best row in a DIFFERENT peer group,
                    # not merely the next row. Rows sharing the leader's peer
                    # do not compete with it — they restate it — so comparing
                    # against one reports a tie that says nothing about which
                    # peer group wins.
                    top_peer = eval_expr(peer, top, pv)
                    runner = next(
                        (r for r in rows[1:]
                         if eval_expr(peer, r, pv) != top_peer),
                        None,
                    )
                if runner is None:
                    # Every row restates one peer group: nothing to separate.
                    basis = "sole"
                else:
                    basis = "tie"
                    for c in comps:
                        tv = eval_expr(c["expr"], top, pv)
                        rv = eval_expr(c["expr"], runner, pv)
                        if tv != rv:  # None == None -> same (IS DISTINCT FROM)
                            basis = str(c["name"])
                            break
        for i, r in enumerate(rows):
            new = dict(r)
            new[rank_as] = i + 1
            if basis_as is not None:
                new[basis_as] = basis if i == 0 else None
            if peer is not None:
                # Part of the peer-RANK output contract. The SQL lowering must
                # materialize the leader's peer in an inner level (a window
                # function cannot be nested in another window's ORDER BY) and
                # postgres has no `SELECT * EXCEPT`, so it cannot project the
                # column away. Emitting it here too keeps the two readers'
                # schemas identical rather than merely their values.
                new[_RANK_PEER_INTERNAL] = eval_expr(peer, rows[0], pv)
            out.append(new)
    return out


class _Reversed:
    """Inverts comparison for a single sort component (descending order)."""

    __slots__ = ("v",)

    def __init__(self, v: Any) -> None:
        self.v = v

    def __lt__(self, other: "_Reversed") -> bool:
        return other.v < self.v

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Reversed) and other.v == self.v


def _freeze(value: Any) -> Any:
    """A hashable partition-key component (lists/dicts via their JSON text)."""
    if isinstance(value, (list, dict)):
        import json as _json

        return _json.dumps(value, sort_keys=True, default=str)
    return value


def _dedup(rows: Relation) -> Relation:
    import json as _json

    seen: set[str] = set()
    out: Relation = []
    for r in rows:
        kk = _json.dumps(r, sort_keys=True, default=str)
        if kk not in seen:
            seen.add(kk)
            out.append(r)
    return out


def _sort_key(value: Any) -> tuple:
    """Total order tolerant of None (NULLS LAST for ASC, matching Postgres
    default) and homogeneous per ORDER key."""
    return (value is None, value if value is not None else 0)


def _diff(before: Relation, after: Relation, key: Callable) -> Relation:
    """DIFF : R x R -> R_changes. Classifies upsert/replace/retract by value."""
    before_by = {key(r): r for r in before}
    after_by = {key(r): r for r in after}
    changes: Relation = []
    for kk, row in after_by.items():
        if kk not in before_by:
            changes.append({CHANGE_OP: DeltaOp.UPSERT.value, "key": kk, "after": row})
        elif before_by[kk] != row:
            changes.append(
                {CHANGE_OP: DeltaOp.REPLACE.value, "key": kk, "before": before_by[kk], "after": row}
            )
    for kk, row in before_by.items():
        if kk not in after_by:
            changes.append({CHANGE_OP: DeltaOp.RETRACT.value, "key": kk, "before": row})
    return changes


def _delta(program, binding, res, kernels, checksum) -> DeltaSet:
    a = binding.op.args
    target = a["target_surface"]
    # Runtime guard-dominance: if any transitively-consumed AssertionResult
    # failed, the DELTA yields the empty DeltaSet (req.gtir.guarded-mutation).
    for sym in _backward_closure(program, set(binding.inputs)):
        val = res.values.get(sym)
        if isinstance(val, AssertionResult) and not val.passed:
            return DeltaSet(target_surface=target, rows=())
    change_rel = res.values.get(binding.inputs[0]) if binding.inputs else []
    rows: list[DeltaRow] = []
    for r in change_rel:
        op = DeltaOp(r.get(CHANGE_OP, a.get("op", "upsert")))
        rows.append(
            DeltaRow(
                target_surface=target,
                op=op,
                key=r.get("key"),
                after=r.get("after"),
                before=r.get("before"),
                source_program_checksum=checksum,
            )
        )
    return DeltaSet(target_surface=target, rows=tuple(rows))


__all__ = ["evaluate", "EvalResult", "InterpreterError", "CHANGE_OP"]

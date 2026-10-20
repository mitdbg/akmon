"""The delta-centric graph transform IR — typed nouns, validation, checksum.

A ``TransformProgram`` is a pure, pinned function from input surfaces to typed
``DeltaSet``s, ``AssertionResult``s, and ``Evidence``. It never commits; the
substrate commit boundary is the only writer (see design record
``02-mutation-as-data``). This module defines the program AST (``Op`` /
``Binding`` / ``Block`` / ``Terminator`` / ``TransformProgram``), the runtime
value nouns (``DeltaRow`` / ``DeltaSet`` / ``AssertionResult`` / ``Evidence``),
``validate()`` (which enforces the effect-typing and registry rules), and a
content-addressed ``checksum()`` over a canonical JSON encoding.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .registry import (
    MAX_ITERATE_NESTING,
    DeltaOp,
    Determinism,
    Effect,
    JoinHow,
    OpKind,
    Registries,
    TerminatorKind,
)

# --------------------------------------------------------------------------- #
# Validation errors
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class IRError:
    """A structured validation failure: a stable ``code``, a message, a path."""

    code: str
    message: str
    path: str = ""

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        loc = f" at {self.path}" if self.path else ""
        return f"[{self.code}] {self.message}{loc}"


class IRValidationError(Exception):
    """Raised by ``validate_or_raise`` when a program fails validation."""

    def __init__(self, errors: Sequence[IRError]) -> None:
        self.errors = list(errors)
        super().__init__("; ".join(str(e) for e in self.errors))


# --------------------------------------------------------------------------- #
# Program AST
# --------------------------------------------------------------------------- #


@dataclass
class Op:
    """One dataflow operator applied to its named inputs.

    ``args`` is a JSON-ready mapping whose well-known keys depend on ``kind``:

    - ``FROM``      ``{surface, alias?, pin?}``
    - ``SEARCH``    ``{surface, method, query, fields?, scorer?, params?}``
    - ``WHERE``     ``{predicate}``
    - ``SELECT``    ``{projection}``
    - ``JOIN``      ``{condition}``                       (two relation inputs)
    - ``GROUP``     ``{keys, aggregates}``
    - ``UNION``     ``{}``                                (N relation inputs)
    - ``ORDER``     ``{keys}``
    - ``LIMIT``     ``{n}``
    - ``DIFF``      ``{key}``                             (before, after inputs)
    - ``CALL``      ``{function_id, args?}``
    - ``ASSERT``    ``{predicate, contract}``             -> binds AssertionResult
    - ``DELTA``     ``{op, target_surface, key?, mapping?}``
    - ``EMIT``      ``{target}``                          (names a program output)
    """

    kind: OpKind
    args: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"kind": self.kind.value, "args": _canonical(self.args)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Op":
        return cls(kind=OpKind(data["kind"]), args=dict(data.get("args", {})))


@dataclass
class Binding:
    """SSA binding: ``output = op(inputs...)``.

    ``output`` is the symbol name this op defines (``None`` for ops that only
    name a program output, e.g. ``EMIT``). ``inputs`` are the symbol names this
    op reads — earlier bindings' outputs, program params, or ``FROM``/``SEARCH``
    surface reads (which take no symbol inputs).
    """

    op: Op
    output: str | None = None
    inputs: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "output": self.output,
            "op": self.op.to_dict(),
            "inputs": list(self.inputs),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Binding":
        return cls(
            op=Op.from_dict(data["op"]),
            output=data.get("output"),
            inputs=tuple(data.get("inputs", ())),
        )


@dataclass
class Terminator:
    """Block terminator: ``RETURN`` / ``IF`` / bounded ``ITERATE``.

    Well-known ``args`` per kind:

    - ``RETURN``   ``{values: [symbol, ...]}``
    - ``IF``       ``{condition: symbol, then_block, else_block?}``
    - ``ITERATE``  ``{state0: symbol, step_block, max_steps: int, stop_predicate?}``

    ``ITERATE`` REQUIRES a positive integer ``max_steps``; there is no unbounded
    loop construct in the language.
    """

    kind: TerminatorKind
    args: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"kind": self.kind.value, "args": _canonical(self.args)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Terminator":
        return cls(kind=TerminatorKind(data["kind"]), args=dict(data.get("args", {})))


@dataclass
class Block:
    block_id: str
    bindings: tuple[Binding, ...]
    terminator: Terminator

    def to_dict(self) -> dict:
        return {
            "block_id": self.block_id,
            "bindings": [b.to_dict() for b in self.bindings],
            "terminator": self.terminator.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Block":
        return cls(
            block_id=data["block_id"],
            bindings=tuple(Binding.from_dict(b) for b in data.get("bindings", ())),
            terminator=Terminator.from_dict(data["terminator"]),
        )


@dataclass
class TransformProgram:
    """A pinned, content-addressed graph-transform program."""

    program_id: str
    blocks: tuple[Block, ...]
    entry_block: str
    effect: Effect = Effect.READ_ONLY
    determinism: Determinism = Determinism.DETERMINISTIC
    params: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    attrs: Mapping[str, Any] = field(default_factory=dict)

    # ----- serialization ------------------------------------------------- #

    def to_dict(self) -> dict:
        return {
            "program_id": self.program_id,
            "effect": self.effect.value,
            "determinism": self.determinism.value,
            "params": list(self.params),
            "inputs": list(self.inputs),
            "outputs": list(self.outputs),
            "entry_block": self.entry_block,
            "blocks": [b.to_dict() for b in self.blocks],
            "attrs": _canonical(self.attrs),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TransformProgram":
        return cls(
            program_id=data["program_id"],
            blocks=tuple(Block.from_dict(b) for b in data.get("blocks", ())),
            entry_block=data["entry_block"],
            effect=Effect(data.get("effect", Effect.READ_ONLY.value)),
            determinism=Determinism(
                data.get("determinism", Determinism.DETERMINISTIC.value)
            ),
            params=tuple(data.get("params", ())),
            inputs=tuple(data.get("inputs", ())),
            outputs=tuple(data.get("outputs", ())),
            attrs=dict(data.get("attrs", {})),
        )

    def canonical_json(self) -> str:
        """Stable JSON encoding used for content addressing."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def checksum(self) -> str:
        """Content address: ``sha256:`` over the canonical JSON encoding."""
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    # ----- helpers -------------------------------------------------------- #

    def iter_ops(self):
        for block in self.blocks:
            for binding in block.bindings:
                yield block, binding

    # ----- validation ----------------------------------------------------- #

    def validate(self, registries: Registries | None = None) -> list[IRError]:
        """Return a (possibly empty) list of validation errors."""
        registries = registries or Registries()
        errors: list[IRError] = []
        _validate_structure(self, errors)
        _validate_ops(self, registries, errors)
        _validate_iterate_nesting(self, errors)
        _validate_taint_and_guard(self, registries, errors)
        return errors

    def validate_or_raise(self, registries: Registries | None = None) -> None:
        errors = self.validate(registries)
        if errors:
            raise IRValidationError(errors)


# --------------------------------------------------------------------------- #
# Runtime value nouns (produced by executing programs; pure data in wave 1)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DeltaRow:
    """The atomic, bitemporal unit of mutation. See design 02-mutation-as-data."""

    target_surface: str
    op: DeltaOp
    key: Any
    after: Any = None
    before: Any = None
    valid_from: str | None = None
    valid_to: str | None = None
    provenance: Mapping[str, Any] = field(default_factory=dict)
    source_program_checksum: str = ""
    evidence_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "target_surface": self.target_surface,
            "op": self.op.value,
            "key": _canonical(self.key),
            "after": _canonical(self.after),
            "before": _canonical(self.before),
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "provenance": _canonical(self.provenance),
            "source_program_checksum": self.source_program_checksum,
            "evidence_refs": list(self.evidence_refs),
        }


@dataclass(frozen=True)
class DeltaSet:
    """A collection of ``DeltaRow``s for ONE target surface.

    This single noun (indexed by ``target_surface``) replaces the prior sketch's
    eighteen named output surfaces. It is the only thing the substrate commits.
    """

    target_surface: str
    rows: tuple[DeltaRow, ...] = ()

    def __post_init__(self) -> None:
        for row in self.rows:
            if row.target_surface != self.target_surface:
                raise ValueError(
                    "DeltaSet target mismatch: "
                    f"{row.target_surface!r} != {self.target_surface!r}"
                )

    def coherence_errors(self) -> list[IRError]:
        """Well-formedness check run at the commit boundary.

        Commit-witness proves *provenance*, not *coherence*: a fully witnessed
        DeltaSet can still be self-contradictory. This checks the op-machine
        invariants (design 02) so the boundary can reject an incoherent set.
        """
        errors: list[IRError] = []
        seen_keys: dict[str, DeltaRow] = {}
        for i, row in enumerate(self.rows):
            path = f"{self.target_surface}.rows[{i}]"
            if row.op is DeltaOp.UPSERT and row.after is None:
                errors.append(IRError("incoherent_delta", "upsert requires an after", path))
            if row.op is DeltaOp.PATCH and row.after is None:
                errors.append(IRError("incoherent_delta", "patch requires an after", path))
            if row.op is DeltaOp.RETRACT and row.after is not None:
                errors.append(IRError("incoherent_delta", "retract must not carry an after", path))
            if row.op is DeltaOp.REPLACE and (row.before is None or row.after is None):
                errors.append(
                    IRError("incoherent_delta", "replace requires before and after", path)
                )
            key = json.dumps(_canonical(row.key), sort_keys=True)
            if key in seen_keys:
                errors.append(
                    IRError(
                        "conflicting_delta_key",
                        f"two DeltaRows target key {row.key!r} in one DeltaSet",
                        path,
                    )
                )
            seen_keys[key] = row
        return errors

    def to_dict(self) -> dict:
        return {
            "target_surface": self.target_surface,
            "rows": [r.to_dict() for r in self.rows],
        }


@dataclass(frozen=True)
class AssertionResult:
    """Outcome of an ``ASSERT`` against a contract. The consistency output."""

    contract_id: str
    passed: bool
    offending_rows: tuple[Any, ...] = ()

    def to_dict(self) -> dict:
        return {
            "contract_id": self.contract_id,
            "passed": self.passed,
            "offending_rows": [_canonical(r) for r in self.offending_rows],
        }


@dataclass(frozen=True)
class Evidence:
    """Append-only provenance a program emits (traces, external-compute output)."""

    evidence_id: str
    kind: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "evidence_id": self.evidence_id,
            "kind": self.kind,
            "payload": _canonical(self.payload),
        }


# --------------------------------------------------------------------------- #
# Validation internals
# --------------------------------------------------------------------------- #

_SURFACE_OPS = {OpKind.FROM, OpKind.SEARCH}
# Ops with a kernel-name form (interpreter only) OR an expr form (SQL-lowerable).
_DUAL_FORM = {
    OpKind.WHERE: ("predicate", "expr"),
    OpKind.SELECT: ("projection", "project"),
    OpKind.JOIN: ("condition", "on"),
    OpKind.ORDER: ("key", "expr"),
}
_REQUIRED_ARGS: dict[OpKind, tuple[str, ...]] = {
    OpKind.FROM: ("surface",),
    OpKind.SEARCH: ("surface", "method", "query", "scorer"),
    OpKind.WHERE: (),
    OpKind.SELECT: (),
    OpKind.JOIN: (),
    OpKind.GROUP: ("key", "agg"),
    OpKind.ORDER: (),
    OpKind.LIMIT: ("n",),
    OpKind.UNNEST: ("expr", "field"),
    OpKind.RANK: ("components",),
    OpKind.DIFF: ("key",),
    OpKind.CALL: ("function_id",),
    OpKind.ASSERT: ("predicate", "contract"),
    OpKind.DELTA: ("op", "target_surface"),
    OpKind.EMIT: ("target",),
    OpKind.UNION: (),
}


def _validate_structure(program: TransformProgram, errors: list[IRError]) -> None:
    block_ids = {b.block_id for b in program.blocks}
    if len(block_ids) != len(program.blocks):
        errors.append(IRError("duplicate_block_id", "block ids must be unique"))
    if program.entry_block not in block_ids:
        errors.append(
            IRError(
                "unknown_entry_block",
                f"entry_block {program.entry_block!r} is not a defined block",
                path="entry_block",
            )
        )

    # A block that is the step_block of an ITERATE has that loop's state symbol
    # injected into its scope each iteration.
    injected: dict[str, set[str]] = {}
    for b in program.blocks:
        t = b.terminator
        if t.kind is TerminatorKind.ITERATE:
            step = t.args.get("step_block")
            if step is not None:
                injected.setdefault(step, set()).add(t.args.get("state_symbol", "state"))

    # SSA scope: a symbol is defined by params, program inputs, an injected loop
    # state symbol, or an earlier binding output within the same block.
    for block in program.blocks:
        defined: set[str] = (
            set(program.params) | set(program.inputs) | injected.get(block.block_id, set())
        )
        for idx, binding in enumerate(block.bindings):
            path = f"{block.block_id}.bindings[{idx}]"
            for sym in binding.inputs:
                if sym not in defined:
                    errors.append(
                        IRError(
                            "undefined_symbol",
                            f"input symbol {sym!r} used before definition",
                            path=path,
                        )
                    )
            if binding.output is not None:
                if binding.output in defined:
                    errors.append(
                        IRError(
                            "duplicate_output_symbol",
                            f"symbol {binding.output!r} bound more than once",
                            path=path,
                        )
                    )
                defined.add(binding.output)
        _validate_terminator(program, block, block_ids, defined, errors)


def _validate_terminator(
    program: TransformProgram,
    block: Block,
    block_ids: set[str],
    defined: set[str],
    errors: list[IRError],
) -> None:
    term = block.terminator
    path = f"{block.block_id}.terminator"
    if term.kind is TerminatorKind.ITERATE:
        max_steps = term.args.get("max_steps")
        if not isinstance(max_steps, int) or max_steps <= 0:
            errors.append(
                IRError(
                    "unbounded_iteration",
                    "ITERATE requires a positive integer max_steps",
                    path=path,
                )
            )
        step_block = term.args.get("step_block")
        if step_block is not None and step_block not in block_ids:
            errors.append(
                IRError("unknown_block", f"step_block {step_block!r} not defined", path=path)
            )
    elif term.kind is TerminatorKind.IF:
        for key in ("then_block", "else_block"):
            ref = term.args.get(key)
            if ref is not None and ref not in block_ids:
                errors.append(
                    IRError("unknown_block", f"{key} {ref!r} not defined", path=path)
                )


def _validate_ops(
    program: TransformProgram, registries: Registries, errors: list[IRError]
) -> None:
    for block, binding in program.iter_ops():
        op = binding.op
        path = f"{block.block_id}:{op.kind.value}"
        for required in _REQUIRED_ARGS.get(op.kind, ()):  # missing required arg
            if required not in op.args:
                errors.append(
                    IRError(
                        "missing_op_arg",
                        f"{op.kind.value} requires arg {required!r}",
                        path=path,
                    )
                )
        if op.kind in _DUAL_FORM:  # one of kernel-name OR expr form must be present
            kernel_key, expr_key = _DUAL_FORM[op.kind]
            has_form = kernel_key in op.args or expr_key in op.args
            # ORDER also accepts the multi-key expr form `keys` (a list of
            # {expr, desc?}); lower_query / the interpreter sort by it in order.
            if op.kind is OpKind.ORDER and "keys" in op.args:
                has_form = True
            if not has_form:
                errors.append(
                    IRError(
                        "missing_op_arg",
                        f"{op.kind.value} requires {kernel_key!r} (kernel) or {expr_key!r} (expr)",
                        path=path,
                    )
                )
        if op.kind in _SURFACE_OPS:
            _check_surface(op.args.get("surface"), registries, errors, path)
        if op.kind is OpKind.DELTA:
            _check_surface(op.args.get("target_surface"), registries, errors, path)
            raw_op = op.args.get("op")
            if raw_op is not None and raw_op not in {o.value for o in DeltaOp}:
                errors.append(
                    IRError("invalid_delta_op", f"unknown delta op {raw_op!r}", path=path)
                )
        if op.kind is OpKind.CALL:
            fid = op.args.get("function_id")
            if fid is not None and registries.functions.resolve(fid) is None:
                errors.append(
                    IRError(
                        "unregistered_call_function",
                        f"CALL function {fid!r} is not registered",
                        path=path,
                    )
                )
        if op.kind is OpKind.LIMIT:
            n = op.args.get("n")
            if n is not None and (not isinstance(n, int) or n < 0):
                errors.append(
                    IRError("invalid_limit", "LIMIT n must be a non-negative int", path=path)
                )
        if op.kind is OpKind.JOIN:
            how = op.args.get("how", JoinHow.INNER.value)
            if how not in {h.value for h in JoinHow}:
                errors.append(
                    IRError("invalid_join_how", f"unknown JOIN variant {how!r}", path=path)
                )
            if len(binding.inputs) != 2:
                errors.append(
                    IRError("join_arity", "JOIN takes exactly two relation inputs", path=path)
                )
        if op.kind is OpKind.UNNEST and len(binding.inputs) != 1:
            errors.append(
                IRError(
                    "unnest_arity",
                    "UNNEST takes exactly one relation input",
                    path=path,
                )
            )
        if op.kind is OpKind.RANK:
            _validate_rank(binding, errors, path)
        if op.kind is OpKind.DIFF and len(binding.inputs) != 2:
            errors.append(
                IRError(
                    "diff_arity",
                    "DIFF takes exactly two relation inputs (before, after)",
                    path=path,
                )
            )


# The column the peer lowering materializes for the leader's peer value. Kept
# here so validation and lowering cannot drift apart on the name.
_RANK_PEER_INTERNAL = "__okg_leader_peer"


def _validate_rank(binding, errors: list[IRError], path: str) -> None:
    """RANK arg discipline: per-group ranking under an ordered component list.

    ``components`` is a non-empty list of ``{name, expr, desc?}`` with unique
    names (the basis vocabulary); ``partition`` is an optional list of exprs;
    ``tiebreak`` an optional ``{expr, desc?}``; ``rank_as`` / ``basis_as`` are
    output column names, must differ from each other, and must not collide
    with component names. FRESH-NAME CONTRACT: rank_as/basis_as must also not
    name an existing input column — that collision is not statically checkable
    here (input schemas are dynamic); the interpreter would silently
    overwrite while the SQL raises AmbiguousColumn or returns both columns.
    """
    a = binding.op.args
    if len(binding.inputs) != 1:
        errors.append(
            IRError("rank_arity", "RANK takes exactly one relation input", path=path)
        )
    comps = a.get("components")
    if not isinstance(comps, list) or not comps:
        errors.append(
            IRError(
                "invalid_rank_components",
                "RANK components must be a non-empty list of {name, expr, desc?}",
                path=path,
            )
        )
        return
    names: list[str] = []
    for c in comps:
        if not isinstance(c, dict) or "name" not in c or "expr" not in c:
            errors.append(
                IRError(
                    "invalid_rank_components",
                    "each RANK component requires `name` and `expr`",
                    path=path,
                )
            )
            return
        names.append(str(c["name"]))
    if len(set(names)) != len(names):
        errors.append(
            IRError(
                "invalid_rank_components",
                "RANK component names must be unique (they are the basis vocabulary)",
                path=path,
            )
        )
    partition = a.get("partition", [])
    if not isinstance(partition, list):
        errors.append(
            IRError(
                "invalid_rank_partition",
                "RANK partition must be a list of exprs",
                path=path,
            )
        )
    tiebreak = a.get("tiebreak")
    if tiebreak is not None and (
        not isinstance(tiebreak, dict) or "expr" not in tiebreak
    ):
        errors.append(
            IRError(
                "invalid_rank_tiebreak",
                "RANK tiebreak must be a {expr, desc?} dict",
                path=path,
            )
        )
    # `peer` groups rows that RESTATE one another rather than competing. It
    # only changes which row the basis compares against, so it is meaningless
    # without basis_as — and silently ignoring it there would hide a wiring
    # mistake in the one place the basis vocabulary is decided.
    if a.get("peer") is not None and a.get("basis_as") is None:
        errors.append(
            IRError(
                "invalid_rank_peer",
                "RANK peer requires basis_as (peer only affects the basis)",
                path=path,
            )
        )
    rank_as = str(a.get("rank_as", "rnk"))
    reserved = {rank_as}
    basis_as = a.get("basis_as")
    if basis_as is not None:
        if str(basis_as) == rank_as:
            errors.append(
                IRError(
                    "invalid_rank_components",
                    "RANK rank_as and basis_as must be distinct output columns",
                    path=path,
                )
            )
        reserved.add(str(basis_as))
    collide = reserved & set(names)
    if collide:
        errors.append(
            IRError(
                "invalid_rank_components",
                f"RANK output columns collide with component names: {sorted(collide)}",
                path=path,
            )
        )
    if a.get("peer") is not None and _RANK_PEER_INTERNAL in reserved | set(names):
        errors.append(
            IRError(
                "invalid_rank_peer",
                f"{_RANK_PEER_INTERNAL!r} is reserved: the peer lowering "
                "materializes the leader's peer under that name, so reusing it "
                "yields a duplicate column and an ambiguous reference",
                path=path,
            )
        )


def _check_surface(
    name: object, registries: Registries, errors: list[IRError], path: str
) -> None:
    if name is None:
        return  # missing-arg already reported
    if registries.surfaces.resolve(str(name)) is None:
        errors.append(
            IRError(
                "unregistered_surface",
                f"surface {name!r} is not registered",
                path=path,
            )
        )


def _propagate_symbols(program: TransformProgram, seeds: set[str]) -> set[str]:
    """Forward dataflow closure: every symbol reachable from ``seeds`` along
    SSA def-use edges (a binding whose input is in the set taints its output).

    Iterates to a fixpoint so order of bindings/blocks does not matter.
    """
    reached = set(seeds)
    changed = True
    while changed:
        changed = False
        for _block, binding in program.iter_ops():
            if binding.output is None or binding.output in reached:
                continue
            if any(sym in reached for sym in binding.inputs):
                reached.add(binding.output)
                changed = True
    return reached


def _validate_taint_and_guard(
    program: TransformProgram, registries: Registries, errors: list[IRError]
) -> None:
    """Effect confinement as dataflow taint (P1-c) + guard-dominance (P0-b).

    - Taint: a symbol produced by an external/random CALL is tainted; taint
      propagates along SSA edges; no DELTA input may be tainted (CALL ~> DELTA).
    - Guard: every DELTA must transitively consume an AssertionResult produced
      by an ASSERT (guard-dominance). An ASSERT bound to the ``always`` contract
      is the explicit escape for a genuinely unconditional mutation.
    """
    taint_seeds: set[str] = set()
    assert_outputs: set[str] = set()
    has_delta = False
    for _block, binding in program.iter_ops():
        op = binding.op
        if op.kind is OpKind.DELTA:
            has_delta = True
        if op.kind is OpKind.ASSERT and binding.output is not None:
            assert_outputs.add(binding.output)
        if op.kind is OpKind.CALL:
            spec = registries.functions.resolve(op.args.get("function_id"))
            if (
                spec is not None
                and program.effect is Effect.READ_ONLY
                and spec.effect is not Effect.READ_ONLY
            ):
                # A read_only program may only CALL read_only kernels — a
                # delta-effect kernel is neither external/random taint nor a
                # DELTA op, so without this rule it slips both checks below.
                errors.append(
                    IRError(
                        "read_only_program_calls_effectful_kernel",
                        "a read_only program may not CALL kernel "
                        f"{op.args.get('function_id')!r} with effect "
                        f"{spec.effect.value!r}",
                    )
                )
            if spec is not None and binding.output is not None and (
                spec.effect in (Effect.EXTERNAL, Effect.RANDOM)
            ):
                taint_seeds.add(binding.output)

    tainted = _propagate_symbols(program, taint_seeds)
    guarded = _propagate_symbols(program, assert_outputs)

    # read_only programs are pure: no DELTA, no external/random taint at all.
    if program.effect is Effect.READ_ONLY and has_delta:
        errors.append(
            IRError("read_only_program_has_delta", "a read_only program may not contain a DELTA")
        )
    if program.effect is Effect.READ_ONLY and taint_seeds:
        errors.append(
            IRError(
                "read_only_program_has_external_call",
                "a read_only program may not CALL an external/random kernel",
            )
        )
    if has_delta and program.effect is not Effect.DELTA:
        errors.append(
            IRError(
                "effect_mismatch_expected_delta",
                f"program emits a DELTA but declares effect={program.effect.value}",
            )
        )

    for block, binding in program.iter_ops():
        if binding.op.kind is not OpKind.DELTA:
            continue
        path = f"{block.block_id}:delta"
        if any(sym in tainted for sym in binding.inputs):
            errors.append(
                IRError(
                    "tainted_delta_input",
                    "a DELTA input is data-dependent on an external/random CALL; "
                    "split off a pure-compute program and link via evidence_refs "
                    "persisted under an observed_replay contract",
                    path=path,
                )
            )
        if not any(sym in guarded for sym in binding.inputs):
            errors.append(
                IRError(
                    "unguarded_delta",
                    "a DELTA must be guard-dominated by an ASSERT it transitively "
                    "consumes (wire an `always`-contract ASSERT for an "
                    "unconditional mutation)",
                    path=path,
                )
            )


def _validate_iterate_nesting(program: TransformProgram, errors: list[IRError]) -> None:
    """Bound ITERATE nesting depth so the step counter stays a real measure."""
    blocks = {b.block_id: b for b in program.blocks}

    def depth(block_id: str, seen: frozenset[str]) -> int:
        block = blocks.get(block_id)
        if block is None or block_id in seen:
            return 0
        term = block.terminator
        if term.kind is TerminatorKind.ITERATE:
            step = term.args.get("step_block")
            return 1 + depth(step, seen | {block_id}) if step in blocks else 1
        if term.kind is TerminatorKind.IF:
            branches = [term.args.get("then_block"), term.args.get("else_block")]
            return max(
                (depth(b, seen | {block_id}) for b in branches if b in blocks),
                default=0,
            )
        return 0

    d = depth(program.entry_block, frozenset())
    if d > MAX_ITERATE_NESTING:
        errors.append(
            IRError(
                "iterate_nesting_exceeded",
                f"ITERATE nesting depth {d} exceeds the bound {MAX_ITERATE_NESTING}",
            )
        )


# --------------------------------------------------------------------------- #
# Canonicalization
# --------------------------------------------------------------------------- #


def _canonical(value: Any) -> Any:
    """Recursively coerce a value into JSON-stable primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    return str(value)

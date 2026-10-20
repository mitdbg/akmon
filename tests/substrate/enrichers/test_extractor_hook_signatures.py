"""Every `emit_typed_node` override can accept the call the base class makes.

`LLMExtractor.enrich` invokes the hook as::

    self.emit_typed_node(conn, candidate, generation_id=..., run_id=..., observed_at=...)

An override that takes `generation_id` positionally and omits `run_id` /
`observed_at` cannot be called at all. The `TypeError` never reaches an operator:
`enrich` catches every per-candidate failure, counts an error, logs a warning,
and holds the watermark. The symptom is an enricher emitting ZERO typed nodes
every cycle and retrying forever — the "enricher emitted zero derived edges"
silent failure, which is materially harder to notice than a crash.

Five test doubles drifted into exactly that shape: the keyword-only parameters
were added to the base class in May, the module docstring kept advertising the
old three-argument form, and doubles written against that docstring in July
inherited a contract they could no longer satisfy. Nothing caught it, because a
double whose only job is to let an abstract class be instantiated is never
called — it passes by never being exercised.

Static, not reflective. `LLMExtractor.__subclasses__()` only sees a class once
its defining scope has run, and two of the drifted doubles are declared inside
test functions; a reflective guard would have reported them clean until the
tests defining them happened to run first. Scanning the source text is
decidable and order-independent.

The predicate is `inspect.Signature.bind` against the real call, not a
hand-written parameter comparison. Binding is the actual rule Python applies, so
it gets `**kwargs`, defaulted extras, and renamed positional parameters right by
construction rather than by enumerating cases.
"""
from __future__ import annotations

import ast
import inspect
import pathlib

import pytest
from tests.pytest.tracked_files import tracked_files

REPO = pathlib.Path(__file__).resolve().parents[3]
ROOTS = (REPO / "src", REPO / "tests")

HOOK = "emit_typed_node"

#: Each root must contribute at least this many definitions. Per-root, not a
#: single total: `src/` alone holds exactly 5, so one shared floor of 5 would
#: still pass if `tests/` silently stopped being scanned — and `tests/` is where
#: every drifted double lived.
MIN_DEFINITIONS_PER_ROOT = {"src": 5, "tests": 10}

#: The call `LLMExtractor.enrich` makes, minus `self`.
CALL_ARGS = ("<conn>", "<candidate>")
CALL_KWARGS = {
    "generation_id": 1,
    "run_id": "<run>",
    "observed_at": "<ts>",
}


def _hook_definitions():
    """Every `def emit_typed_node` under src/ and tests/, at any nesting depth.

    Files that do not mention the hook are never parsed: it keeps the scan cheap
    and means an unrelated file that fails to parse under some interpreter
    cannot red this guard.
    """
    for root in ROOTS:
        for path in tracked_files(root, "*.py"):
            rel = path.relative_to(REPO).as_posix()
            text = path.read_text()
            if HOOK not in text:
                continue
            try:
                tree = ast.parse(text, filename=rel)
            except SyntaxError as exc:
                raise AssertionError(
                    f"{rel} mentions {HOOK} but does not parse ({exc}); a file "
                    f"that fails to parse is silently unguarded here."
                ) from exc
            for node in ast.walk(tree):
                if (
                    isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name == HOOK
                ):
                    yield root.name, rel, node


def _signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> inspect.Signature:
    """The definition's parameters as an `inspect.Signature`, minus `self`.

    Defaults are recorded only as "has a default", since binding cares whether a
    parameter may be omitted, never what it would be.
    """
    args = node.args
    empty = inspect.Parameter.empty
    params: list[inspect.Parameter] = []

    positional = args.posonlyargs + args.args
    n_defaults = len(args.defaults)
    first_defaulted = len(positional) - n_defaults
    for index, arg in enumerate(positional):
        if index == 0 and arg.arg == "self":
            continue
        kind = (
            inspect.Parameter.POSITIONAL_ONLY
            if index < len(args.posonlyargs)
            else inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        default = None if index >= first_defaulted else empty
        params.append(inspect.Parameter(arg.arg, kind, default=default))

    if args.vararg is not None:
        params.append(
            inspect.Parameter(args.vararg.arg, inspect.Parameter.VAR_POSITIONAL)
        )
    for arg, node_default in zip(args.kwonlyargs, args.kw_defaults):
        params.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.KEYWORD_ONLY,
                default=empty if node_default is None else None,
            )
        )
    if args.kwarg is not None:
        params.append(inspect.Parameter(args.kwarg.arg, inspect.Parameter.VAR_KEYWORD))
    return inspect.Signature(params)


def _mismatch(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """Why this definition cannot serve the base class's call, or None."""
    if isinstance(node, ast.AsyncFunctionDef):
        # `enrich` does `n_edges += self.emit_typed_node(...)` synchronously; a
        # coroutine would fail on the addition, not on the call.
        return "is `async def`, but the base class calls the hook synchronously"
    try:
        _signature(node).bind(*CALL_ARGS, **CALL_KWARGS)
    except TypeError as exc:
        return f"cannot bind the call: {exc}"
    return None


@pytest.mark.parametrize("root_name,minimum", sorted(MIN_DEFINITIONS_PER_ROOT.items()))
def test_each_root_contributes_definitions(root_name, minimum) -> None:
    """Guard against a vacuous pass: a scan that finds nothing proves nothing,
    and a scan that silently loses one root proves only half of what it says."""
    found = [rel for name, rel, _ in _hook_definitions() if name == root_name]
    assert len(found) >= minimum, (
        f"expected at least {minimum} `{HOOK}` definitions under {root_name}/, "
        f"found {len(found)} — the scan roots are wrong"
    )


def test_every_override_can_take_the_call_the_base_class_makes() -> None:
    offenders = [
        f"{rel}:{node.lineno} {reason}"
        for _, rel, node in _hook_definitions()
        if (reason := _mismatch(node)) is not None
    ]
    assert not offenders, (
        "these `emit_typed_node` definitions cannot accept "
        "`(conn, candidate, generation_id=..., run_id=..., observed_at=...)`. "
        "enrich() swallows the resulting TypeError, so the symptom is an "
        "enricher emitting zero typed nodes every cycle while holding its "
        "watermark, not a crash:\n  " + "\n  ".join(offenders)
    )


#: The parameter list the base class's call actually needs.
GOOD_PARAMS = "self, conn, candidate, *, generation_id, run_id, observed_at"


def _definition(params: str, *, is_async: bool = False):
    """Parse a synthetic `emit_typed_node` with this parameter list.

    The cases below name only the parameters, never a whole `def` line: a case
    long enough to wrap would otherwise become two adjacent string literals in a
    list, which is indistinguishable from a missing comma.
    """
    prefix = "async def" if is_async else "def"
    return ast.parse(f"{prefix} {HOOK}({params}): ...").body[0]


@pytest.mark.parametrize(
    "params",
    [
        # The shape the five doubles actually drifted into.
        "self, conn, candidate, generation_id",
        # Near-misses either side of it.
        "self, conn, candidate, *, generation_id",
        "self, conn, candidate, *, generation_id, run_id, observed_at, extra",
        "self, conn",
    ],
)
def test_the_guard_rejects_shapes_that_cannot_bind(params) -> None:
    """A guard that only ever returns None is indistinguishable from no guard."""
    reason = _mismatch(_definition(params))
    assert reason is not None and "cannot bind" in reason


def test_the_guard_rejects_a_coroutine_with_the_right_parameters() -> None:
    """Right parameters, wrong kind of callable. `enrich` does
    `n_edges += self.emit_typed_node(...)`, so a coroutine fails at the
    addition rather than at the call — which binding alone would not catch."""
    reason = _mismatch(_definition(GOOD_PARAMS, is_async=True))
    assert reason is not None and "async def" in reason


@pytest.mark.parametrize(
    "params",
    [
        # The real contract.
        GOOD_PARAMS,
        # Positional parameters are passed positionally, so their names are not
        # part of the contract.
        "self, c, cand, *, generation_id, run_id, observed_at",
        # An extra keyword-only parameter the caller never passes is fine when
        # it has a default.
        "self, conn, candidate, *, generation_id, run_id, observed_at, x=False",
        # As is an extra positional with a default.
        "self, conn, candidate, extra=None, *, generation_id, run_id, observed_at",
        # A fully permissive forwarder binds anything.
        "self, *args, **kwargs",
        # **kwargs absorbing the keyword-only trio.
        "self, conn, candidate, **kwargs",
    ],
)
def test_the_guard_accepts_every_shape_that_really_binds(params) -> None:
    """False positives cost as much as false negatives: a guard that rejects a
    legitimate override teaches people to delete it."""
    assert _mismatch(_definition(params)) is None

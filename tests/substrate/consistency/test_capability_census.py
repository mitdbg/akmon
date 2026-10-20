"""PACT ``concord-system-stress`` — req.stress.census, task.stress.census.

Which layer is each capability on?

Every number this program reports has to say where it came from, because the
system is three layers with real seams between them: a pipeline that runs, a
planner that is built and unconnected, and a specification that is validated
and executed by nothing. A stress result pooled across those describes no
system that exists.

The layer is DERIVED from the source tree — the import graph from a publish,
plus symbol references — never authored, so it cannot drift from the code.
Each capability's expected layer is pinned below; a capability that changes
layer FAILS here and must move its pin in the same reviewable diff.

The three layers
----------------
``WIRED``                the defining module is import-reachable from a
                         publish, and the capability's symbols are referenced
                         inside that closure. It runs; it can be measured end
                         to end.
``BUILT_NOT_CONNECTED``  the defining module is NOT reachable from a publish,
                         but other non-test source modules reference it. It has
                         an implementation and consumers; no production path
                         reaches it. Measurable only against its own checker.
``DECLARED_ONLY``        not reachable, and no source module outside its own
                         defining module references it. Expressible; no
                         runtime behaviour.

Import reachability is a NECESSARY condition, not a sufficient one: a module
in the closure may still hold a symbol nothing calls. That is why the symbol
reference set is required as well, and why the report names the consuming
modules rather than just the verdict.
"""
from __future__ import annotations

import ast
import pathlib
from collections import defaultdict

import pytest

SRC = pathlib.Path(__file__).resolve().parents[3] / "src"

#: A publish is what the pipeline DOES. Reachability is measured from it.
PRODUCTION_ENTRY = "okg.substrate.publisher.publisher"

LAYER_WIRED = "WIRED"
LAYER_BUILT = "BUILT_NOT_CONNECTED"
LAYER_DECLARED = "DECLARED_ONLY"

#: (capability, defining module, symbols that identify it, PINNED layer)
CAPABILITIES: tuple[tuple[str, str, tuple[str, ...], str], ...] = (
    (
        "incremental_reuse_engine",
        "okg.substrate.consistency.reuse_scheduler",
        ("run_reuse_pass", "ReuseRungConfig"),
        LAYER_WIRED,
    ),
    (
        "checker",
        "okg.substrate.consistency.ir_check",
        ("IrConsistencyCheckSpec", "load_ir_consistency_checks"),
        LAYER_WIRED,
    ),
    (
        "repair_precedence",
        "okg.substrate.consistency.claim_model.precedence",
        ("SIGNAL_SOURCE_AUTHORITY", "SIGNAL_INDEPENDENT_SUPPORT",
         "OUTCOME_CANONICAL", "OUTCOME_CONTESTED"),
        LAYER_WIRED,
    ),
    (
        "batch_planner",
        "okg.substrate.consistency.batch_schedule",
        ("BatchInstance",),
        LAYER_BUILT,
    ),
    (
        "consistency_policy_modes",
        "okg.substrate.consistency.invariant_spec",
        ("MODE_STRICT", "MODE_BOUNDED", "MODE_EVENTUAL", "MODE_LAZY"),
        LAYER_DECLARED,
    ),
    (
        "generation_role_views",
        "okg.substrate.consistency.invariant_spec",
        ("ROLE_GENERATION",),
        LAYER_DECLARED,
    ),
    (
        "graph_region_corpus",
        "okg.substrate.consistency.invariant_spec",
        ("graph-region",),
        LAYER_DECLARED,
    ),
)


def _defining_source(module: str) -> str:
    path = SRC / (module.replace(".", "/") + ".py")
    assert path.exists(), f"capability names a module that does not exist: {module}"
    return path.read_text(encoding="utf-8")


def test_every_pinned_symbol_exists_in_its_defining_module() -> None:
    """A symbol that does not exist has no consumers, and so reads EXACTLY like
    a capability with no runtime behaviour. Without this guard a typo in the
    pin table below would be reported as a finding about the system. Two of the
    pins were wrong on first authoring and this is what caught them."""
    missing = []
    for name, defining, symbols, _pinned in CAPABILITIES:
        source = _defining_source(defining)
        for symbol in symbols:
            if symbol not in source:
                missing.append(f"{name}: {symbol!r} not found in {defining}")
    assert not missing, (
        "pinned symbols do not exist — these would be silently misread as "
        "DECLARED_ONLY:\n  " + "\n  ".join(missing)
    )


def _module_name(path: pathlib.Path) -> str:
    return str(path.relative_to(SRC).with_suffix("")).replace("/", ".")


def _scan() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """(module -> okg imports, symbol -> modules that reference it)."""
    imports: dict[str, set[str]] = {}
    refs: dict[str, set[str]] = defaultdict(set)
    for path in SRC.rglob("*.py"):
        module = _module_name(path)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - a broken tree is not our subject
            continue
        found: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found |= {a.name for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module:
                found.add(node.module)
                # `from X import Y` references Y in this module too
                for alias in node.names:
                    refs[alias.name].add(module)
            elif isinstance(node, ast.Name):
                refs[node.id].add(module)
            elif isinstance(node, ast.Attribute):
                refs[node.attr].add(module)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                refs[node.value].add(module)
        imports[module] = {i for i in found if i.startswith("okg")}
    return imports, refs


def _closure(imports: dict[str, set[str]], entry: str) -> set[str]:
    seen: set[str] = set()
    stack = [entry]
    while stack:
        module = stack.pop()
        if module in seen:
            continue
        seen.add(module)
        stack.extend(imports.get(module, ()))
    return seen


@pytest.fixture(scope="module")
def census() -> dict[str, dict]:
    imports, refs = _scan()
    production = _closure(imports, PRODUCTION_ENTRY)
    assert PRODUCTION_ENTRY in imports, "the production entry point moved"
    assert len(production) > 50, f"closure implausibly small: {len(production)}"

    out: dict[str, dict] = {}
    for name, defining, symbols, _pinned in CAPABILITIES:
        consumers: set[str] = set()
        for symbol in symbols:
            consumers |= refs.get(symbol, set())
        external = {m for m in consumers if m != defining}
        in_closure = consumers & production
        if defining in production and in_closure:
            layer = LAYER_WIRED
        elif external:
            layer = LAYER_BUILT
        else:
            layer = LAYER_DECLARED
        out[name] = {
            "defining_module": defining,
            "symbols": list(symbols),
            "layer": layer,
            "defining_module_reachable_from_publish": defining in production,
            "consuming_modules_outside_definition": sorted(external),
            "consuming_modules_inside_production_closure": sorted(in_closure),
        }
    return out


def test_every_capability_is_on_its_pinned_layer(census) -> None:
    """The census is DERIVED. A capability that changes layer fails here and
    must move its pin in the same diff — which is the point: a capability
    quietly becoming (or ceasing to be) production behaviour is exactly the
    drift that makes an old measurement mean something new."""
    moved = []
    for name, _defining, _symbols, pinned in CAPABILITIES:
        actual = census[name]["layer"]
        if actual != pinned:
            moved.append(
                f"{name}: pinned {pinned}, derived {actual} "
                f"(reachable={census[name]['defining_module_reachable_from_publish']}, "
                f"consumers={census[name]['consuming_modules_outside_definition']})"
            )
    assert not moved, "capabilities changed layer:\n  " + "\n  ".join(moved)


def test_the_running_pipeline_is_actually_reachable(census) -> None:
    """Non-vacuity: if nothing were reachable the census would call everything
    DECLARED_ONLY and read as a dramatic finding rather than a broken probe."""
    wired = [n for n, c in census.items() if c["layer"] == LAYER_WIRED]
    assert "incremental_reuse_engine" in wired
    assert "checker" in wired
    for name in wired:
        assert census[name]["consuming_modules_inside_production_closure"], name


def test_consistency_semantics_reach_no_runtime(census) -> None:
    """The finding this census exists to pin, stated as an assertion so that a
    future wiring of policy modes BREAKS it and forces the claim to be revised.

    All four policy modes are validated by the specification loader and
    executed by nothing: no module outside the loader references them, so no
    runtime can behave differently for a lazy invariant than a strict one."""
    cell = census["consistency_policy_modes"]
    assert cell["layer"] == LAYER_DECLARED
    assert cell["consuming_modules_outside_definition"] == [], (
        "a policy mode gained a consumer — consistency semantics may now have "
        f"runtime behaviour: {cell['consuming_modules_outside_definition']}"
    )


def test_the_planner_is_built_but_no_publish_reaches_it(census) -> None:
    """Scheduling has two mechanisms on two layers; pooling them would report a
    number about no system that exists."""
    planner = census["batch_planner"]
    assert planner["layer"] == LAYER_BUILT
    assert not planner["defining_module_reachable_from_publish"]
    assert planner["consuming_modules_outside_definition"], (
        "the planner has no consumers at all — it would be DECLARED_ONLY, not built"
    )


def test_census_renders_for_the_report(census) -> None:
    """The census is a deliverable, not only a gate: print it so the run log
    carries the table the report cites."""
    print("\n\nCAPABILITY CENSUS (derived from the source tree)")
    print(f"  production entry: {PRODUCTION_ENTRY}\n")
    for name, cell in sorted(census.items(), key=lambda kv: (kv[1]["layer"], kv[0])):
        print(f"  {name:<28}{cell['layer']:<22}"
              f"reachable={str(cell['defining_module_reachable_from_publish']):<6}"
              f"consumers={len(cell['consuming_modules_outside_definition'])}")
    assert census

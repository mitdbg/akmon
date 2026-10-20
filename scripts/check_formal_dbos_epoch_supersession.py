#!/usr/bin/env python3
"""Check DBOS application/epoch supersession and emit strict PACT evidence."""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Mapping

import yaml

from okg.substrate.formal_tlc import TLCProperty, TLCSetupError, run_tlc_formal_report


ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "formal" / "dbos_epoch_supersession"
DEFAULT_OUT = ROOT / ".scratch" / "formal" / "dbos-epoch-supersession"
BOUNDARY_REGISTRY_PATH = MODEL_DIR / "boundary-registry.yaml"
BOUNDARY_REGISTRY_DOCUMENT = yaml.safe_load(
    BOUNDARY_REGISTRY_PATH.read_text(encoding="utf-8")
)
BOUNDARY_REGISTRY = tuple(row["id"] for row in BOUNDARY_REGISTRY_DOCUMENT["boundaries"])
REQUIRED_BINDING_FIELDS = tuple(BOUNDARY_REGISTRY_DOCUMENT["binding_profiles"]["full"])
AUTHORITY_BINDING_FIELDS = (
    "lifecycle_activation_epoch",
    "activation_receipt_digest",
    "database_writer_epoch",
    "dbos_application_version",
    "worktree_write_capability_epoch",
    "binding_epoch",
)
DIRECT_PRODUCER_REGISTRY = {
    row["path"]: row["owner"] for row in BOUNDARY_REGISTRY_DOCUMENT["direct_producers"]
}
INVENTORY_CATEGORY_BOUNDARIES = {
    "dbos_direct_producer": ("enqueue",),
    "dbos_decorated_entry": ("body_entry",),
    "sql_mutation": ("mutation",),
    "chronos_graph_write": ("mutation",),
    "nomos_apply": ("mutation",),
    "progress_state": ("mutation",),
    "authority_transition": ("lease", "receipt_transition", "publish"),
}
SQL_MUTATION_RE = re.compile(
    r"\b(?:INSERT\s+INTO\s+[A-Za-z_][A-Za-z0-9_.]*|"
    r"UPDATE\s+[A-Za-z_][A-Za-z0-9_.]*|"
    r"DELETE\s+FROM\s+[A-Za-z_][A-Za-z0-9_.]*|"
    r"CREATE\s+(?:TABLE|INDEX|SCHEMA)|ALTER\s+(?:TABLE|ROLE)|"
    r"DROP\s+(?:TABLE|INDEX|SCHEMA)|TRUNCATE)\b",
    re.IGNORECASE,
)
# A literal counts as an executed statement only when the keyword OPENS it and
# the statement has a body. Single-word verbs such as SET, COMMIT and RELEASE
# are omitted: as bare literals they are ordinary status values, not SQL, and
# admitting them would classify from a bare word again.
SQL_STATEMENT_RE = re.compile(
    r"^\s*(?:WITH\s+|\(\s*)?(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|"
    r"TRUNCATE|LOCK|GRANT|REVOKE|REFRESH|COMMENT\s+ON)\b",
    re.IGNORECASE,
)
SQL_STATEMENT_MIN_TOKENS = 3
INVARIANTS = (
    "TypeInvariant",
    "EpochBindingComplete",
    "SupersededBodyCannotMutate",
    "StaleBindingNeverMutates",
    "SingleCurrentRecovery",
    "LeaseFencedOnSupersession",
    "SupersededProducerCannotRecreate",
    "OutcomeOwnedByCurrentEpoch",
    "NoStaleTransactionCommitAcrossEpochAdvance",
    "SharedAndExclusiveFencesNeverOverlap",
    "MutationBoundaryCarriesCompleteIdentity",
    "RecoveryIsUniquePerEquivalenceClass",
)
REFINEMENT_TESTS = (
    "tests/substrate/test_local_fleet_workflow_supersession.py::test_epoch_binding_registry_is_complete",
    "tests/substrate/test_local_fleet_workflow_supersession.py::test_superseded_body_cannot_mutate",
    "tests/substrate/test_local_fleet_workflow_supersession.py::test_single_current_recovery_per_equivalence_class",
    "tests/substrate/test_local_fleet_workflow_supersession.py::test_postgres_transaction_cannot_commit_across_epoch_advance",
    "tests/substrate/test_local_fleet_workflow_supersession.py::test_direct_producer_registry_is_exhaustive",
    "tests/substrate/test_local_fleet_workflow_supersession.py::test_shared_and_exclusive_epoch_fence_is_real",
)
PROPERTY = TLCProperty(
    "DbosEpochSupersessionAggregate",
    (
        "TLC verified application/epoch binding, stale-body and lease fencing, "
        "single recovery per equivalence class, real shared/exclusive fencing, "
        "complete mutation identity, and current-epoch outcome ownership."
    ),
    refinement_tests=REFINEMENT_TESTS,
    expected_invariants=INVARIANTS,
)


@dataclass(frozen=True)
class EpochFence:
    binding: tuple[tuple[str, object], ...]
    shared_holders: frozenset[str] = frozenset()
    exclusive_owner: str | None = None


def validate_boundary_registry(root: Path = ROOT) -> None:
    registry = BOUNDARY_REGISTRY_DOCUMENT
    if registry.get("schema") != "okg.dbos_boundary_registry/v1":
        raise ValueError("unsupported DBOS boundary registry schema")
    if len(set(BOUNDARY_REGISTRY)) != len(BOUNDARY_REGISTRY):
        raise ValueError("duplicate boundary id")
    if set(BOUNDARY_REGISTRY) != {
        "enqueue",
        "dequeue",
        "body_entry",
        "schedule",
        "poller",
        "compactor",
        "lease",
        "mutation",
        "receipt_transition",
        "publish",
    }:
        raise ValueError("boundary registry is incomplete")
    equivalence = registry["equivalence"]
    if equivalence["recovery_cardinality"] != 1:
        raise ValueError("recovery must be unique per equivalence class")
    fence = registry["fence"]
    if (
        fence["shared"]["implementation"] != "pg_advisory_xact_lock_shared"
        or fence["exclusive"]["implementation"] != "pg_advisory_lock"
        or fence["exclusive"]["precondition"] != "shared_holders_drained"
    ):
        raise ValueError("registry does not declare a real shared/exclusive fence")
    required = set(REQUIRED_BINDING_FIELDS)
    tla_mapping = registry["tla_mapping"]
    if set(tla_mapping) != required:
        raise ValueError("TLA mapping must cover the exact wire-field profile")
    model_text = (MODEL_DIR / "DbosEpochSupersession.tla").read_text(encoding="utf-8")
    for wire_field, tla_identifier in tla_mapping.items():
        if not isinstance(tla_identifier, str) or tla_identifier not in model_text:
            raise ValueError(
                f"wire field {wire_field} lacks an explicit TLA identifier mapping"
            )
    for boundary in registry["boundaries"]:
        if set(registry["binding_profiles"][boundary["binding_profile"]]) != required:
            raise ValueError(f"incomplete binding profile at {boundary['id']}")
        if boundary["equivalence_fields"] != equivalence["key_fields"]:
            raise ValueError(f"equivalence fields drift at {boundary['id']}")
        if boundary["expected_parent_field"] != registry["expected_parent"]["field"]:
            raise ValueError(f"expected-parent field drift at {boundary['id']}")
        if boundary["fence_mode"] not in {"shared", "exclusive"}:
            raise ValueError(f"invalid fence mode at {boundary['id']}")
        if not boundary["sources"]:
            raise ValueError(f"boundary {boundary['id']} has no sources")
        for source in boundary["sources"]:
            path = root / source["path"]
            if not path.is_file():
                raise ValueError(f"boundary source is missing: {source['path']}")
            if source["entrypoint"] not in path.read_text(encoding="utf-8"):
                raise ValueError(
                    f"boundary entrypoint is missing: {source['path']}::{source['entrypoint']}"
                )
    inventory_path = BOUNDARY_REGISTRY_PATH.parent / registry["inventory_file"]
    inventory = yaml.safe_load(inventory_path.read_text(encoding="utf-8"))
    if inventory.get("schema") != "okg.dbos_boundary_inventory/v1":
        raise ValueError("unsupported DBOS boundary inventory schema")
    validate_discovered_inventory(
        discover_mutation_inventory(root), inventory.get("entries", [])
    )
    discovered_direct_paths = {
        entry["path"]
        for entry in inventory["entries"]
        if "dbos_direct_producer" in entry["categories"]
    }
    if discovered_direct_paths != set(DIRECT_PRODUCER_REGISTRY):
        raise ValueError("direct producer registry does not cover discovered paths")


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return ""


def _prose_constant_ids(subtrees: list[ast.AST]) -> set[int]:
    """Identify every string constant that is a bare statement, i.e. prose.

    A docstring is a bare string statement, and so is any free-floating string
    used as commentary. Both are excluded wherever they appear, including
    f-string forms, which parse as ``JoinedStr`` rather than ``Constant``.
    """
    prose: set[int] = set()
    for subtree in subtrees:
        for child in ast.walk(subtree):
            if not isinstance(child, ast.Expr):
                continue
            value = child.value
            if isinstance(value, ast.Constant) and isinstance(
                value.value, (str, bytes)
            ):
                prose.add(id(value))
            elif isinstance(value, ast.JoinedStr):
                for inner in ast.walk(value):
                    if isinstance(inner, ast.Constant):
                        prose.add(id(inner))
    return prose


def _literal_text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    return None


def _code_signal(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[str, str]:
    """Split a function into the text that is code and the text that is SQL.

    Comments never reach the syntax tree, and bare string statements are
    dropped as prose. A remaining string literal counts as code only when it
    reads as an executed SQL statement, meaning a SQL keyword opens it. A bare
    word and an English sentence are both prose and carry no signal.
    """
    subtrees: list[ast.AST] = [node.args, *node.body]
    if node.returns is not None:
        subtrees.append(node.returns)
    prose = _prose_constant_ids(subtrees)
    identifiers: list[str] = [node.name]
    sql_literals: list[str] = []
    for subtree in subtrees:
        for child in ast.walk(subtree):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                identifiers.append(child.name)
            elif isinstance(child, ast.Name):
                identifiers.append(child.id)
            elif isinstance(child, ast.Attribute):
                identifiers.append(child.attr)
            elif isinstance(child, ast.arg):
                identifiers.append(child.arg)
            elif isinstance(child, ast.keyword) and child.arg:
                identifiers.append(child.arg)
            elif isinstance(child, ast.alias):
                identifiers.append(child.name)
                if child.asname:
                    identifiers.append(child.asname)
            elif isinstance(child, ast.ImportFrom) and child.module:
                identifiers.append(child.module)
            elif isinstance(child, ast.ExceptHandler) and child.name:
                identifiers.append(child.name)
            elif isinstance(child, (ast.Global, ast.Nonlocal)):
                identifiers.extend(child.names)
            elif isinstance(child, ast.MatchAs) and child.name:
                identifiers.append(child.name)
            elif isinstance(child, ast.MatchStar) and child.name:
                identifiers.append(child.name)
            elif isinstance(child, ast.MatchMapping) and child.rest:
                identifiers.append(child.rest)
            elif isinstance(child, ast.MatchClass) and child.kwd_attrs:
                identifiers.extend(child.kwd_attrs)
            elif isinstance(child, ast.Constant) and id(child) not in prose:
                text = _literal_text(child.value)
                if text is None:
                    continue
                if SQL_MUTATION_RE.search(text):
                    # A DML keyword with a table after it is specific enough to
                    # stand alone, and f-string SQL arrives in fragments that
                    # do not start with the keyword.
                    sql_literals.append(text)
                    identifiers.append(text)
                elif (
                    SQL_STATEMENT_RE.match(text)
                    and len(text.split()) >= SQL_STATEMENT_MIN_TOKENS
                ):
                    identifiers.append(text)
    return "\n".join(identifiers), "\n".join(sql_literals)


# Python's own containers. Building one, or calling a method on a local name
# that every binding in the function sets to one, changes no database or
# workflow state.
BUILTIN_CONTAINER_CONSTRUCTORS = frozenset({"dict", "set", "frozenset", "list"})
BUILTIN_CONTAINER_DISPLAYS = (
    ast.Dict,
    ast.DictComp,
    ast.Set,
    ast.SetComp,
    ast.List,
    ast.ListComp,
)
_NESTED_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)


def _non_name_bindings(child: ast.AST) -> set[str]:
    """Names one node binds other than through a ``Name`` target or a def."""
    if isinstance(child, ast.alias):
        return {child.asname or child.name.split(".")[0]}
    if isinstance(child, ast.arg):
        return {child.arg}
    if isinstance(child, ast.ExceptHandler) and child.name:
        return {child.name}
    if isinstance(child, (ast.Global, ast.Nonlocal)):
        return set(child.names)
    if isinstance(child, (ast.MatchAs, ast.MatchStar)) and child.name:
        return {child.name}
    if isinstance(child, ast.MatchMapping) and child.rest:
        return {child.rest}
    return set()


def _bound_names(tree: ast.AST) -> set[str]:
    """Every name any node anywhere in ``tree`` binds or declares."""
    bound: set[str] = set()
    for child in ast.walk(tree):
        if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
            bound.add(child.id)
        elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(child.name)
        else:
            bound.update(_non_name_bindings(child))
    return bound


def module_shadowed_builtins(tree: ast.AST) -> frozenset[str]:
    """Container constructor names a module rebinds, e.g. ``import x as set``."""
    return frozenset(_bound_names(tree) & BUILTIN_CONTAINER_CONSTRUCTORS)


def _own_scope_nodes(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.AST]:
    """Every node of the function body; nested scopes are listed, not entered.

    The parameter list is left out: the caller handles parameters itself so
    that ``**kwargs`` can count as a dict.
    """
    found: list[ast.AST] = []
    stack: list[ast.AST] = list(node.body)
    while stack:
        child = stack.pop()
        found.append(child)
        if not isinstance(child, _NESTED_SCOPES):
            stack.extend(ast.iter_child_nodes(child))
    return found


def _builtin_container_names(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    shadowed_builtins: frozenset[str] = frozenset(),
) -> tuple[frozenset[str], frozenset[str]]:
    """Return ``(containers, bound)`` for one function.

    ``containers`` are local names that every binding in the function sets to
    a builtin container, by a plain ``name = <container>``, ``name: T =
    <container>`` or ``(name := <container>)``; ``**kwargs`` counts as a dict.
    Any other binding rebinds the name, so its method calls still count as
    possible state writes: a parameter, tuple unpacking, a loop, ``with`` or
    ``except`` target, an import, a match capture, ``global``/``nonlocal``, or
    any binding at all inside a nested function, lambda or class. An attribute
    or subscript target (``store.cache = {}``, ``registry["k"] = []``) binds no
    local name and makes nothing a container.

    ``bound`` is every name the function binds anywhere plus
    ``shadowed_builtins``: a constructor call is a builtin container only when
    its name is not in it.
    """
    bound = frozenset(_bound_names(node) | shadowed_builtins)
    kwarg = node.args.kwarg.arg if node.args.kwarg is not None else None
    other: set[str] = {
        name for arg in ast.walk(node.args) for name in _non_name_bindings(arg)
    } - {kwarg}
    scope_nodes = _own_scope_nodes(node)
    container_targets: set[int] = set()
    for child in scope_nodes:
        if isinstance(child, _NESTED_SCOPES):
            other.update(_bound_names(child))
            if not isinstance(child, ast.Lambda):
                other.add(child.name)
            continue
        other.update(_non_name_bindings(child))
        if isinstance(child, ast.Assign):
            targets, value = child.targets, child.value
        elif isinstance(child, (ast.AnnAssign, ast.NamedExpr)):
            targets, value = [child.target], child.value
        else:
            continue
        if _is_container_value(value, bound):
            container_targets.update(
                id(target) for target in targets if isinstance(target, ast.Name)
            )
    container: set[str] = {kwarg} if kwarg else set()
    for child in scope_nodes:
        if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
            (container if id(child) in container_targets else other).add(child.id)
    return frozenset(container - other), bound


def _is_container_value(value: ast.AST | None, bound: frozenset[str]) -> bool:
    if value is None:
        return False
    if isinstance(value, BUILTIN_CONTAINER_DISPLAYS):
        return True
    return (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id in BUILTIN_CONTAINER_CONSTRUCTORS
        and value.func.id not in bound
    )


def _is_builtin_container_call(
    call: ast.Call, containers: frozenset[str], bound: frozenset[str]
) -> bool:
    """A builtin container constructor, or a method call on a local container."""
    if isinstance(call.func, ast.Name):
        return (
            call.func.id in BUILTIN_CONTAINER_CONSTRUCTORS and call.func.id not in bound
        )
    return (
        isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id in containers
    )


def _function_categories(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    shadowed_builtins: frozenset[str] = frozenset(),
) -> tuple[str, ...]:
    categories: set[str] = set()
    if any(
        isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and child.func.attr in {"enqueue", "start_workflow"}
        for child in ast.walk(node)
    ):
        categories.add("dbos_direct_producer")
    if any(
        isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr in {"workflow", "scheduled", "step", "workflow_step"}
        for decorator in node.decorator_list
    ):
        categories.add("dbos_decorated_entry")
    identifier_text, sql_text = _code_signal(node)
    has_sql_mutation = bool(sql_text)
    if has_sql_mutation:
        categories.add("sql_mutation")
    lowered = f"{identifier_text}\n{sql_text}".lower()
    containers, bound = _builtin_container_names(
        node, shadowed_builtins=shadowed_builtins
    )
    mutation_calls = {
        _call_name(child).lower()
        for child in ast.walk(node)
        if isinstance(child, ast.Call)
        and not _is_builtin_container_call(child, containers, bound)
    }
    state_mutation_calls = {
        name
        for name in mutation_calls
        if re.search(
            r"(?:^|_)(?:apply|publish|write|put|insert|update|delete|persist|save|"
            r"set|advance|commit|acquire|release|transition|mark|record)(?:_|$)",
            name,
        )
    }
    if "chronos" in lowered and state_mutation_calls:
        categories.add("chronos_graph_write")
    if "nomos" in lowered and state_mutation_calls:
        categories.add("nomos_apply")
    if re.search(r"\b(?:cursor|watermark|checkpoint)\w*", lowered) and (
        has_sql_mutation or state_mutation_calls
    ):
        categories.add("progress_state")
    if re.search(r"\b(?:session|lease|lock|receipt|publish)\w*", lowered) and (
        has_sql_mutation or state_mutation_calls
    ):
        categories.add("authority_transition")
    return tuple(sorted(categories))


def discover_file_inventory(*, path: str, source: str) -> list[dict[str, object]]:
    tree = ast.parse(source)
    shadowed = module_shadowed_builtins(tree)
    entries: list[dict[str, object]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        categories = _function_categories(node, shadowed_builtins=shadowed)
        if not categories:
            continue
        boundaries = sorted(
            {
                boundary
                for category in categories
                for boundary in INVENTORY_CATEGORY_BOUNDARIES[category]
            }
        )
        diagnostic = path.endswith(
            (
                "/runtime/recovery_test.py",
                "/runtime/smoke.py",
            )
        )
        entries.append(
            {
                "path": path,
                "entrypoint": node.name,
                "categories": list(categories),
                "boundaries": boundaries,
                "disposition": (
                    "nonmutating/diagnostic"
                    if diagnostic
                    else "implementation_required"
                ),
                "owner_change": "fence-local-okg-workflow-versions",
                "reason": (
                    "diagnostic-only workflow producer; production admission is "
                    "forbidden and the diagnostic boundary remains inventoried"
                    if diagnostic
                    else "must validate the full epoch envelope and hold the declared "
                    "shared or exclusive fence before this mutation class can ship"
                ),
            }
        )
    return entries


def discover_mutation_inventory(root: Path = ROOT) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for relative in BOUNDARY_REGISTRY_DOCUMENT["inventory_roots"]:
        target = root / relative
        paths = sorted(target.rglob("*.py")) if target.is_dir() else [target]
        for path in paths:
            if not path.is_file():
                raise ValueError(f"inventory root is missing: {relative}")
            entries.extend(
                discover_file_inventory(
                    path=str(path.relative_to(root)),
                    source=path.read_text(encoding="utf-8"),
                )
            )
    return sorted(
        entries,
        key=lambda entry: (
            str(entry["path"]),
            str(entry["entrypoint"]),
            tuple(entry["categories"]),
        ),
    )


def function_is_defined(path: Path, entrypoint: str) -> bool:
    """Report whether ``entrypoint`` is a function defined in ``path``."""
    if not path.is_file():
        return False
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == entrypoint
        for node in ast.walk(tree)
    )


def validate_explicit_entry(entry: Mapping[str, object], *, root: Path) -> None:
    """Check one asserted boundary the code-only scan cannot discover.

    An explicit entry is a deliberate claim rather than a measurement, so it
    has to name a function that exists and say why the scan cannot see it.
    """
    path = str(entry.get("path", ""))
    entrypoint = str(entry.get("entrypoint", ""))
    label = f"{path}::{entrypoint}"
    if not str(entry.get("reason", "")).strip():
        raise ValueError(f"explicit boundary entry lacks a reason: {label}")
    categories = list(entry.get("categories", []))
    if not categories:
        raise ValueError(f"explicit boundary entry declares no categories: {label}")
    unknown = sorted(set(categories) - set(INVENTORY_CATEGORY_BOUNDARIES))
    if unknown:
        raise ValueError(f"explicit boundary entry has unknown categories: {label}")
    expected = sorted(
        {
            boundary
            for category in categories
            for boundary in INVENTORY_CATEGORY_BOUNDARIES[category]
        }
    )
    if list(entry.get("boundaries", [])) != expected:
        raise ValueError(
            f"explicit boundary entry boundaries do not match categories: {label}"
        )
    if not function_is_defined(root / path, entrypoint):
        raise ValueError(f"explicit boundary entry names no such function: {label}")


def validate_discovered_inventory(
    discovered: Iterable[Mapping[str, object]],
    registered: Iterable[Mapping[str, object]],
    *,
    root: Path = ROOT,
) -> None:
    def identity(entry: Mapping[str, object]) -> tuple[object, ...]:
        return (
            entry.get("path"),
            entry.get("entrypoint"),
            tuple(entry.get("categories", [])),
            tuple(entry.get("boundaries", [])),
        )

    registered = list(registered)
    explicit = [entry for entry in registered if entry.get("explicit")]
    for entry in explicit:
        validate_explicit_entry(entry, root=root)
    # An explicit entry asserts a boundary the scan cannot see, so it stands in
    # place of discovery for that function rather than alongside it.
    asserted = {(entry.get("path"), entry.get("entrypoint")) for entry in explicit}
    discovered_by_id = {
        identity(entry): entry
        for entry in discovered
        if (entry.get("path"), entry.get("entrypoint")) not in asserted
    }
    registered_by_id = {
        identity(entry): entry for entry in registered if not entry.get("explicit")
    }
    if discovered_by_id != registered_by_id:
        missing = sorted(set(discovered_by_id) - set(registered_by_id))
        extra = sorted(set(registered_by_id) - set(discovered_by_id))
        raise ValueError(
            f"boundary inventory drift: unregistered={missing[:5]}, stale={extra[:5]}"
        )
    for entry in [*registered_by_id.values(), *explicit]:
        if (
            entry.get("disposition")
            not in {"covered", "implementation_required", "nonmutating/diagnostic"}
            or not entry.get("owner_change")
            or not entry.get("reason")
        ):
            raise ValueError(
                f"inventory entry lacks disposition/owner/reason: {identity(entry)}"
            )


def acquire_shared_fence(
    fence: EpochFence,
    *,
    holder: str,
    binding: Mapping[str, object],
) -> EpochFence | None:
    exact = tuple(sorted(binding.items()))
    if fence.exclusive_owner is not None or fence.binding != exact:
        return None
    return replace(fence, shared_holders=fence.shared_holders | {holder})


def acquire_exclusive_fence(
    fence: EpochFence,
    *,
    owner: str,
    binding: Mapping[str, object],
) -> EpochFence | None:
    exact = tuple(sorted(binding.items()))
    if (
        fence.shared_holders
        or fence.exclusive_owner is not None
        or fence.binding != exact
    ):
        return None
    return replace(fence, exclusive_owner=owner)


def mutation_admitted(
    binding: Mapping[str, object], current: Mapping[str, object]
) -> bool:
    """Executable refinement of the body-entry/mutation epoch guard."""
    return (
        all(field in binding for field in REQUIRED_BINDING_FIELDS)
        and all(
            binding.get(key) == current.get(key) for key in AUTHORITY_BINDING_FIELDS
        )
        and all(
            bool(binding.get(key))
            for key in (
                "operation_id",
                "operation_digest",
                "equivalence_class",
                "capsule_digest",
            )
        )
        and binding.get("expected_parent") == current.get("published_generation")
        and bool(binding.get("body_running"))
        and not bool(binding.get("superseded"))
    )


def postgres_transaction_commit_admitted(
    transaction: Mapping[str, object], current: Mapping[str, object]
) -> bool:
    """A transaction opened under another activation cannot commit."""
    return transaction.get("state") == "open" and all(
        transaction.get(key) == current.get(key) for key in AUTHORITY_BINDING_FIELDS
    )


def discover_direct_producer_paths(root: Path = ROOT) -> set[str]:
    """Mechanically inventory direct DBOS enqueue/start calls."""
    discovered: set[str] = set()
    substrate = root / "src" / "okg" / "substrate"
    for path in substrate.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"enqueue", "start_workflow"}
            for node in ast.walk(tree)
        ):
            discovered.add(str(path.relative_to(root)))
    return discovered


def select_current_recoveries(
    rows: Iterable[Mapping[str, object]], current: Mapping[str, object]
) -> dict[str, Mapping[str, object]]:
    """Choose at most one exact-current row per operation equivalence class."""
    selected: dict[str, Mapping[str, object]] = {}
    ordered = sorted(rows, key=lambda row: str(row.get("operation_id", "")))
    for row in ordered:
        if not mutation_admitted({**row, "body_running": True}, current):
            continue
        equivalence = str(row["equivalence_class"])
        selected.setdefault(equivalence, row)
    return selected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all", action="store_true", help="run the complete bounded model"
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    validate_boundary_registry()
    command = (
        "uv run python scripts/check_formal_dbos_epoch_supersession.py --all "
        f"--out {args.out}"
    )
    try:
        result = run_tlc_formal_report(
            repo_root=ROOT,
            model_dir=MODEL_DIR,
            model=MODEL_DIR / "DbosEpochSupersession.tla",
            config=MODEL_DIR / "DbosEpochSupersession.cfg",
            out_dir=args.out,
            properties=(PROPERTY,),
            expected_properties=(PROPERTY,),
            checker_command=command,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=12,
        )
    except TLCSetupError as exc:
        print(f"formal-dbos-epoch-supersession: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            f"formal-dbos-epoch-supersession: failed; see {result.stdout_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1
    print(f"formal-dbos-epoch-supersession: passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""One check of a source's registry ``params`` against its reader's constructor.

Issue #2571 item 2 (#2484). The comparison used to live only inside strict
ingest (``source_adapter_init_params``), so a misspelled or missing key passed
``okg deployment lint`` and ``okg doctor --check sources`` and failed only when
the source ran. Lint, doctor and ingest now call :func:`check_source_params`
and print the same message.

The constructor side is a :class:`ConstructorParameters` value. Ingest and
doctor build it from the loaded class (:meth:`ConstructorParameters.from_class`).
Lint must not import deployment code, so it builds it from the class's source
text (:func:`static_constructor_parameters`), which answers ``None`` -- no
finding -- for every shape it does not state exactly (listed on that
function). Rebinding done at run time (``globals()``, ``global`` inside a
called function, patching through an alias) is not detected.
A wrong answer there would be a false blocker; no answer leaves the source to
doctor and ingest.
"""
from __future__ import annotations

import ast
import difflib
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from okg.substrate.ingest.source_construction import (
    _annotation_name,
    _field_init_flag,
    _is_field_without_default,
    _is_unreadable_field_call,
)


INJECTED_ADAPTER_PARAM_NAMES = frozenset({
    "dsn",
    "deployment",
    "source_name",
    "graph_branch",
})
PROTOCOL_AUTHORITY_PARAM_NAMES = frozenset({
    "name",
    "profile",
    "change_probe_kind",
    "run_id",
    "mode",
    "sync_scope",
})
RESERVED_ADAPTER_PARAM_NAMES = (
    INJECTED_ADAPTER_PARAM_NAMES | PROTOCOL_AUTHORITY_PARAM_NAMES
)

SOURCE_PARAM_RESERVED = "source_param_reserved"
SOURCE_PARAM_UNKNOWN = "source_param_unconsumed"
SOURCE_PARAM_MISSING = "source_param_missing"
SOURCE_PARAM_KEY_INVALID = "source_param_key_invalid"

_KEYWORD_KINDS = frozenset({
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    inspect.Parameter.KEYWORD_ONLY,
})


@dataclass(frozen=True)
class ConstructorParameters:
    """What a reader's constructor accepts by keyword."""

    target: str
    named: frozenset[str]
    required: frozenset[str]
    accepts_var_keyword: bool

    @classmethod
    def from_signature(
        cls, signature: inspect.Signature, *, target: str,
    ) -> ConstructorParameters:
        named = frozenset(
            name for name, parameter in signature.parameters.items()
            if parameter.kind in _KEYWORD_KINDS
        )
        required = frozenset(
            name for name in named
            if signature.parameters[name].default is inspect.Parameter.empty
        )
        return cls(
            target=target,
            named=named,
            required=required,
            accepts_var_keyword=any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            ),
        )

    @classmethod
    def from_class(cls, adapter_class: Any) -> ConstructorParameters:
        """Read the loaded class; raises ``TypeError``/``ValueError`` when
        its signature is not inspectable."""
        return cls.from_signature(
            inspect.signature(adapter_class),
            target=class_target(adapter_class),
        )

    @property
    def accepted(self) -> tuple[str, ...]:
        """Keys a registry may author: named, minus substrate-owned ones."""
        return tuple(sorted(self.named - RESERVED_ADAPTER_PARAM_NAMES))


@dataclass(frozen=True)
class SourceParamProblem:
    code: str
    key: str
    message: str
    #: Whether a legacy-contract registry fails on this too. Strict refuses
    #: every problem; legacy passes a reserved key through and lets a
    #: ``**kwargs`` constructor absorb an unknown one.
    refused_by_legacy: bool

    def render(self, source: str) -> str:
        """The one-line form every caller prints."""
        return (
            f"{self.code}: source {source!r} at "
            f"/sources/{source}/params/{self.key}: {self.message}"
        )


def class_target(adapter_class: Any) -> str:
    module = getattr(adapter_class, "__module__", None)
    name = getattr(adapter_class, "__qualname__", None) or getattr(
        adapter_class, "__name__", repr(adapter_class),
    )
    return f"{module}.{name}" if module else str(name)


def check_source_params(
    params: Mapping[Any, Any],
    parameters: ConstructorParameters | None,
) -> tuple[SourceParamProblem, ...]:
    """Every non-text, reserved, unknown and missing key, in that order.

    ``params`` is the registry entry's authored ``params`` mapping (not the
    kwargs after injection). A key is unknown when it is not a named keyword
    parameter: ``**kwargs`` is not proof the reader consumes it. A key is
    missing when the constructor requires it and neither the registry nor the
    substrate supplies it; required substrate-owned names are left to the
    final bind check, because the author can neither add nor delete them.
    With ``parameters=None`` (the constructor could not be read) only
    non-text and reserved keys are reported.
    """
    problems: list[SourceParamProblem] = [
        SourceParamProblem(
            code=SOURCE_PARAM_KEY_INVALID,
            key=str(key),
            message=(
                f"params key {key!r} is a {type(key).__name__}, not a "
                "constructor parameter name."
            ),
            refused_by_legacy=True,
        )
        for key in params if not isinstance(key, str)
    ]
    authored = {key for key in params if isinstance(key, str)}
    accepted_sentence = reserved_sentence = ""
    if parameters is not None:
        accepted_text = ", ".join(parameters.accepted) or "(none)"
        accepted_sentence = f" Accepted keys: {accepted_text}."
        reserved_sentence = (
            f" Accepted keys for {parameters.target}: {accepted_text}."
        )
    for key in sorted(authored & RESERVED_ADAPTER_PARAM_NAMES):
        problems.append(SourceParamProblem(
            code=SOURCE_PARAM_RESERVED,
            key=key,
            message=(
                f"params authors substrate-owned key {key!r}; the substrate "
                "injects it from deployment authority, so delete it."
                + reserved_sentence
            ),
            refused_by_legacy=False,
        ))
    if parameters is None:
        return tuple(problems)
    for key in sorted(authored - parameters.named - RESERVED_ADAPTER_PARAM_NAMES):
        hint = difflib.get_close_matches(key, parameters.accepted, n=1)
        problems.append(SourceParamProblem(
            code=SOURCE_PARAM_UNKNOWN,
            key=key,
            message=(
                f"params key {key!r} is not a parameter of "
                f"{parameters.target}"
                + (
                    " (its **kwargs is not proof the key is used)"
                    if parameters.accepts_var_keyword else ""
                )
                + "."
                + (f" Did you mean {hint[0]!r}?" if hint else "")
                + accepted_sentence
            ),
            refused_by_legacy=not parameters.accepts_var_keyword,
        ))
    for key in sorted(
        parameters.required - authored - RESERVED_ADAPTER_PARAM_NAMES,
    ):
        problems.append(SourceParamProblem(
            code=SOURCE_PARAM_MISSING,
            key=key,
            message=(
                f"params is missing {key!r}, which {parameters.target} "
                "requires." + accepted_sentence
            ),
            refused_by_legacy=True,
        ))
    return tuple(problems)


# ---------------------------------------------------------------- static read

_DATACLASS_SAFE_OPTIONS = frozenset({
    "repr", "eq", "order", "unsafe_hash", "frozen", "match_args", "slots",
    "weakref_slot",
})
_CLASS_BODY_BLOCKERS = frozenset({"__new__", "__signature__", "__init__"})


def static_constructor_parameters(
    tree: ast.Module,
    class_name: str,
    *,
    target: str,
    base_is_safe: Callable[[ast.expr], bool] | None = None,
) -> ConstructorParameters | None:
    """Constructor parameters of module-level ``class_name`` in ``tree``.

    Answers only the two shapes a file states directly: one undecorated
    ``def __init__`` in the class body, or a plain ``@dataclass`` with no base
    class and no ``__init__``. Returns ``None`` for: a name bound more than once at module level, a
    metaclass, ``__new__``, ``__signature__``, an assigned or decorated or
    repeated ``__init__``, an inherited constructor, a first parameter that is
    not ``self``-like, a required positional-only parameter, a dataclass with
    options that change ``__init__``, fields under a conditional, or aliased
    ``ClassVar``/``InitVar``/``KW_ONLY`` imports, a module-level
    ``setattr``/attribute assignment on the class, and an explicit
    ``__init__`` under a base class that ``base_is_safe`` does not clear
    (a base's metaclass ``__call__``, ``__signature__`` or
    ``__init_subclass__`` would change what the runtime signature is).
    """
    bindings = [
        node for node in tree.body if class_name in bound_names(node)
    ]
    if len(bindings) != 1 or not isinstance(bindings[0], ast.ClassDef):
        return None
    node = bindings[0]
    if mutates_class_at_module_level(tree, class_name):
        return None
    if any(keyword.arg == "metaclass" for keyword in node.keywords):
        return None
    inits: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if item.name == "__init__":
                inits.append(item)
            elif item.name in _CLASS_BODY_BLOCKERS:
                return None
        elif bound_names(item) & _CLASS_BODY_BLOCKERS:
            return None
    if len(inits) > 1:
        return None
    if inits:
        init = inits[0]
        if init.decorator_list or node.decorator_list:
            return None
        if node.bases and (
            base_is_safe is None
            or not all(base_is_safe(base) for base in node.bases)
        ):
            return None
        return _parameters_from_init(init.args, target=target)
    if node.bases or not is_plain_dataclass(node, tree):
        return None
    if _imports_alias(tree, {"ClassVar", "InitVar", "KW_ONLY"}):
        return None
    return _parameters_from_plain_dataclass(node, target=target)


_COMPOUND_STATEMENTS = tuple(
    kind for kind in (
        ast.If, ast.Try, getattr(ast, "TryStar", None), ast.With,
        ast.AsyncWith, ast.For, ast.AsyncFor, ast.While,
        getattr(ast, "Match", None),
    )
    if kind is not None
)


def bound_names(node: ast.stmt) -> set[str]:
    """Names one module-level statement binds (or deletes).

    A compound statement counts every binding inside it: a conditional,
    looped or matched binding may or may not be the runtime one.
    """
    names: set[str] = set()
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        # Walrus targets anywhere in the statement, outside nested scopes.
        stack: list[ast.AST] = [node]
        while stack:
            child = stack.pop()
            if isinstance(child, ast.NamedExpr) and isinstance(
                child.target, ast.Name,
            ):
                names.add(child.target.id)
            if not isinstance(child, (
                ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda,
            )) or child is node:
                stack.extend(ast.iter_child_nodes(child))
    if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return {node.name}
    targets: list[ast.expr] = []
    if isinstance(node, ast.Assign):
        targets = list(node.targets)
    elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        targets = [node.target]
    elif isinstance(node, ast.Delete):
        targets = list(node.targets)
    elif type(node).__name__ == "TypeAlias":
        targets = [node.name]  # type: ignore[attr-defined]
    elif isinstance(node, (ast.Import, ast.ImportFrom)):
        return names | {
            (alias.asname or alias.name).split(".")[0] for alias in node.names
        }
    elif isinstance(node, _COMPOUND_STATEMENTS):
        for child in ast.walk(node):
            if isinstance(child, ast.stmt) and child is not node:
                names |= bound_names(child)
            elif isinstance(child, ast.ExceptHandler) and child.name:
                names.add(child.name)
            elif type(child).__name__ in {"MatchAs", "MatchStar"} and getattr(
                child, "name", None,
            ):
                names.add(child.name)  # type: ignore[attr-defined]
            elif type(child).__name__ == "MatchMapping" and getattr(
                child, "rest", None,
            ):
                names.add(child.rest)  # type: ignore[attr-defined]
        if isinstance(node, (ast.For, ast.AsyncFor)):
            targets = [node.target]
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            targets = [
                item.optional_vars for item in node.items
                if item.optional_vars is not None
            ]
    for target in targets:
        for child in ast.walk(target):
            if isinstance(child, ast.Name):
                names.add(child.id)
    return names


def mutates_class_at_module_level(tree: ast.Module, class_name: str) -> bool:
    """``Reader.__init__ = f``, ``setattr(Reader, ...)`` and the like."""
    for statement in tree.body:
        if isinstance(statement, (
            ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef,
        )):
            continue
        for child in ast.walk(statement):
            if (
                isinstance(child, ast.Attribute)
                and isinstance(child.ctx, (ast.Store, ast.Del))
                and isinstance(child.value, ast.Name)
                and child.value.id == class_name
            ):
                return True
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id in {"setattr", "delattr"}
                and child.args
                and isinstance(child.args[0], ast.Name)
                and child.args[0].id == class_name
            ):
                return True
    return False


def _parameters_from_init(
    args: ast.arguments, *, target: str,
) -> ConstructorParameters | None:
    positional = [*args.posonlyargs, *args.args]
    if not positional:
        return None  # no receiver: not a plain instance method
    # The receiver is the first positional parameter whatever its name.
    receiver_is_posonly = bool(args.posonlyargs)
    posonly = args.posonlyargs[1:] if receiver_is_posonly else []
    keyword_positional = args.args if receiver_is_posonly else args.args[1:]
    positional_defaults = len(args.defaults)
    rest = [*posonly, *keyword_positional]
    first_defaulted = len(rest) - positional_defaults
    if any(index < first_defaulted for index in range(len(posonly))):
        return None  # a required positional-only parameter: never bindable
    required = {
        arg.arg for index, arg in enumerate(rest)
        if index >= len(posonly) and index < first_defaulted
    }
    required |= {
        arg.arg for arg, default in zip(args.kwonlyargs, args.kw_defaults)
        if default is None
    }
    named = {arg.arg for arg in (*keyword_positional, *args.kwonlyargs)}
    return ConstructorParameters(
        target=target,
        named=frozenset(named),
        required=frozenset(required),
        accepts_var_keyword=args.kwarg is not None,
    )


def _dataclass_import_names(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Names bound to ``dataclasses.dataclass`` and to the module itself."""
    direct: set[str] = set()
    modules: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "dataclasses":
            for alias in node.names:
                if alias.name == "dataclass":
                    direct.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "dataclasses":
                    modules.add(alias.asname or alias.name)
    return direct, modules


def is_plain_dataclass(node: ast.ClassDef, tree: ast.Module) -> bool:
    if len(node.decorator_list) != 1:
        return False
    decorator = node.decorator_list[0]
    call = decorator if isinstance(decorator, ast.Call) else None
    target = call.func if call is not None else decorator
    direct, modules = _dataclass_import_names(tree)
    # A later `def dataclass(...)` shadows the import.
    direct = {
        name for name in direct
        if sum(name in bound_names(node) for node in tree.body) == 1
    }
    modules = {
        name for name in modules
        if sum(name in bound_names(node) for node in tree.body) == 1
    }
    if isinstance(target, ast.Name):
        if target.id not in direct:
            return False
    elif isinstance(target, ast.Attribute):
        if not (
            target.attr == "dataclass"
            and isinstance(target.value, ast.Name)
            and target.value.id in modules
        ):
            return False
    else:
        return False
    if call is None:
        return True
    if call.args:
        return False
    # `init=False` removes the generated __init__; `kw_only` changes nothing
    # this answers (names and defaults), but any other or unreadable option
    # is refused.
    return all(
        keyword.arg in _DATACLASS_SAFE_OPTIONS | {"kw_only"}
        and isinstance(keyword.value, ast.Constant)
        for keyword in call.keywords
    )


def _imports_alias(tree: ast.Module, names: set[str]) -> bool:
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in names and alias.asname not in (None, alias.name):
                    return True
    return False


def _parameters_from_plain_dataclass(
    node: ast.ClassDef, *, target: str,
) -> ConstructorParameters | None:
    counts: dict[str, int] = {}
    for item in node.body:
        for name in bound_names(item):
            counts[name] = counts.get(name, 0) + 1
    if any(count > 1 for count in counts.values()):
        # `root: str` then `def root(self)`: the later binding becomes the
        # default, so the field is not required at runtime.
        return None
    named: set[str] = set()
    required: set[str] = set()
    for item in node.body:
        if isinstance(item, ast.AnnAssign):
            if not isinstance(item.target, ast.Name):
                return None
            kind = _annotation_name(item.annotation)
            if kind in {"ClassVar", "KW_ONLY"}:
                continue
            init_flag = _field_init_flag(item.value)
            if init_flag is False:
                continue
            if init_flag is None and _is_unreadable_field_call(item.value):
                return None
            named.add(item.target.id)
            if item.value is None or _is_field_without_default(item.value):
                required.add(item.target.id)
        elif any(
            isinstance(child, ast.AnnAssign) for child in ast.walk(item)
            if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        ):
            return None  # a field declared under a conditional
    return ConstructorParameters(
        target=target,
        named=frozenset(named),
        required=frozenset(required),
        accepts_var_keyword=False,
    )


def explain_legacy_construct_failure(
    diagnosis: dict[str, Any],
    *,
    source: str,
    params: Any,
    adapter_class: Any,
) -> None:
    """Lead a legacy constructor failure's message with the shared check.

    Legacy ingest constructs first, so the #945 recovery can still re-import
    a class whose loaded copy predates its file. Once that diagnosis says the
    loaded class is current (or cannot tell), the registry disagrees with the
    reader, and the message names the key the same way lint and doctor do.
    """
    if not isinstance(params, Mapping):
        return
    try:
        parameters = ConstructorParameters.from_class(adapter_class)
    except (TypeError, ValueError):
        return
    problems = [
        problem for problem in check_source_params(params, parameters)
        if problem.refused_by_legacy
    ]
    if not problems:
        return
    diagnosis["source_param_problems"] = [
        {"code": problem.code, "key": problem.key, "message": problem.message}
        for problem in problems
    ]
    diagnosis["message"] = (
        "; ".join(problem.render(source) for problem in problems)
        + " | "
        + str(diagnosis.get("message") or "")
    )

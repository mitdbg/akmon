"""Diagnose — and recover from — a source adapter that fails to construct.

Issue #945. When a source adapter raises ``TypeError`` because it does
not accept an argument the registry (or the substrate's own injection)
passed it, the failure looks exactly like any other run-time error: an
opaque ``TypeError`` recorded against the source. On ``cooper`` that
shape repeated 177 times over ~36 hours with a premise that was false
the whole time — the class *on disk* did accept ``dsn``. The running
worker had imported the class 84 minutes before the parameter existed
and, being a long-lived process, kept it.

Two things happen here.

**Proof, not a heuristic.** The cause is established by reading the
class definition on disk with :mod:`ast` — no import, no side effects —
and comparing its ``__init__`` parameters to the loaded class's
signature. If the file accepts the rejected argument and the loaded
object does not, staleness is *proven*: the two definitions differ, and
the loaded one is not the file. If the file rejects it too, the registry
and the adapter genuinely disagree. No clock is involved, so this does
not invert in the ordinary edit loop (edit, then first import) and does
not lie for a deploy that preserves mtimes. File mtime versus the
module's recorded import time is kept only as a labelled fallback for
when the file cannot be parsed.

**Recovery, not just a better message.** When staleness is proven, the
adapter's module is evicted and re-imported and construction is retried
once. That is what turns 177 consecutive failures into zero without an
operator noticing anything. Eviction is scoped to the failing module's
own name subtree rather than going through
``evict_deployment_modules``, whose ``_DEPLOYMENT_LOCAL_MODULE_PREFIXES``
list does not cover package-style deployment sources
(``<deployment>.sources.x``) and which would evict far more live code
than the failure justifies. If the stale definition lives in a sibling
helper the retry fails, and the failure is reported rather than a
recovery being claimed.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import os
import re
import sys
import threading
from pathlib import Path
from typing import Any, Mapping

__all__ = [
    "SOURCE_ADAPTER_SIGNATURE_MISMATCH",
    "classify_source_construction_failure",
    "evict_module_tree",
    "on_disk_init_parameters",
    "prove_stale_definition",
    "retry_construction_after_eviction",
]

SOURCE_ADAPTER_SIGNATURE_MISMATCH = "source_adapter_signature_mismatch"

#: Serialises evict + re-import against OTHER RECOVERIES. Sources run
#: concurrently and may share an adapter module; two threads mutating
#: `sys.modules` for the same name is how you get a half-imported module
#: or two class objects for one name.
#:
#: What this does NOT serialise: a recovery against the ordinary
#: construction path. A thread that resolved its adapter class before
#: another thread evicted the module still holds — and constructs — the
#: old class object. That is inherent to replacing a module in a live
#: process and is not claimed to be fixed here; the eviction is scoped
#: to a proven-stale failure precisely to keep the window rare.
#:
#: Reentrant because an adapter module's import can run arbitrary code,
#: including code that constructs another source: a non-reentrant lock
#: would deadlock that thread against itself with no timeout.
_RECOVERY_LOCK = threading.RLock()

_UNEXPECTED_KWARG = re.compile(
    r"unexpected keyword argument '(?P<argument>[^']+)'"
)
_MISSING_ARGUMENT = re.compile(
    r"missing \d+ required (?:positional|keyword-only) arguments?: "
    r"(?P<arguments>.+)"
)


def classify_source_construction_failure(
    *,
    exc: BaseException,
    stage: str,
    module_name: str | None,
    class_name: str | None,
    params: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Classify ``exc`` if it is an adapter signature mismatch.

    Returns ``None`` for anything else, so this never masks or
    reinterprets an unrelated failure.
    """
    parsed = _parse_signature_type_error(exc, stage=stage)
    if parsed is None:
        return None
    arguments, direction = parsed

    proof = prove_stale_definition(
        module_name=module_name,
        class_name=class_name,
        arguments=arguments,
        direction=direction,
    )
    cause = {
        "stale": "stale_loaded_code",
        "current": "registry_adapter_mismatch",
    }.get(str(proof.get("status")), "undetermined")

    target = ".".join(
        part for part in (module_name, class_name) if part
    ) or "the source adapter"
    argument_text = ", ".join(repr(name) for name in arguments) or "an argument"
    if direction == "adapter_rejected_argument":
        headline = (
            f"{target} does not accept {argument_text}, "
            "which the source registry passed"
        )
    else:
        headline = (
            f"{target} requires {argument_text}, "
            "which the source registry did not pass"
        )

    if cause == "stale_loaded_code":
        explanation = (
            f"the definition of {class_name} loaded in this process "
            f"differs from the one in {proof.get('path')} "
            f"({proof.get('evidence')}), so this process is running code "
            "that no longer exists on disk — restart the runtime worker"
        )
    elif cause == "registry_adapter_mismatch":
        explanation = (
            f"the definition of {class_name} loaded in this process "
            f"matches the one on disk ({proof.get('evidence')}), so this "
            "is a real disagreement between the source registry and the "
            "adapter's signature — change one of them"
        )
    else:
        explanation = (
            f"this process could not compare the loaded definition of "
            f"{class_name} with the one on disk "
            f"({proof.get('reason')}), so the cause is undetermined: "
            "either this process is serving stale code (restart it and "
            "re-check) or the registry and the adapter genuinely disagree"
        )

    return {
        "error_kind": SOURCE_ADAPTER_SIGNATURE_MISMATCH,
        "cause": cause,
        "direction": direction,
        "arguments": arguments,
        "module": module_name,
        "class": class_name,
        "declared_params": sorted(params or {}),
        "staleness_proof": proof,
        "message": (
            f"{SOURCE_ADAPTER_SIGNATURE_MISMATCH}: {headline}; "
            f"{explanation} [{type(exc).__name__}: {exc}]"
        ),
    }


def retry_construction_after_eviction(
    *,
    module_name: str,
    class_name: str,
    params: Mapping[str, Any],
    prepare_params: Any = None,
) -> dict[str, Any]:
    """Re-import ``module_name`` from disk and retry construction once.

    ``prepare_params(cls, params) -> params`` is re-applied against the
    FRESH class before construction. This matters: substrate-supplied
    arguments are injected based on the target's own signature, and the
    whole premise here is that the fresh definition declares parameters
    the stale one did not.

    Returns ``{"recovered": True, "adapter": ...}`` when the fresh
    definition constructs, and ``{"recovered": False, "reason": ...}``
    otherwise. Never raises: a failed recovery leaves the caller's
    original, classified failure standing.
    """
    # Sources run concurrently (`_run_source` is driven from a thread
    # pool), and two of them can name the same adapter module. Evicting
    # a `sys.modules` entry while another thread is mid-import of it
    # yields a half-executed module or two distinct class objects for
    # one name. Serialise the evict/re-import so at most one recovery is
    # in flight; a second thread arriving after the first has already
    # healed the module finds a fresh definition and constructs without
    # evicting anything.
    with _RECOVERY_LOCK:
        try:
            module = sys.modules.get(module_name)
            if module is not None and _already_fresh(module, class_name,
                                                     params):
                evicted: list[str] = []
            else:
                evicted = evict_module_tree(module_name)
                # Bytecode caches key on mtime+size; a rewrite landing in
                # the same second at the same size can otherwise be
                # served from a stale .pyc, silently re-importing the
                # code we just threw away.
                importlib.invalidate_caches()
            from okg.substrate.distributions.packaged_release import revalidate_active_release
            revalidate_active_release()
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            retry_params = dict(params)
            if callable(prepare_params):
                retry_params = dict(prepare_params(cls, retry_params))
            adapter = cls(**retry_params)
        except Exception as exc:  # noqa: BLE001 - recovery is best-effort.
            return {
                "recovered": False,
                "reason": f"{type(exc).__name__}: {exc}",
            }
    return {
        "recovered": True,
        "adapter": adapter,
        "evicted_modules": evicted,
        "retry_params": sorted(retry_params),
    }


def evict_module_tree(module_name: str) -> list[str]:
    """Drop ``module_name`` and its submodules from ``sys.modules``.

    Returns the names evicted. Scoped by module NAME to the failing
    module's own subtree — deliberately narrow. A stale definition that
    lives in a sibling module is NOT recovered here, and the retry then
    fails rather than silently reporting a recovery it did not achieve.

    Also clears the evicted names' recorded import times, so the
    re-import is stamped afresh; otherwise the healed module keeps the
    stamp of the copy that was discarded and is reported
    loaded-then-modified for the rest of the process's life.
    """
    evicted = [
        name for name in list(sys.modules)
        if name == module_name or name.startswith(f"{module_name}.")
    ]
    for name in evicted:
        module = sys.modules.pop(name, None)
        _discard_bytecode_cache(module)
    try:
        from okg.substrate.runtime.code_identity import (
            forget_module_import_times,
        )

        forget_module_import_times(evicted)
    except Exception:  # noqa: BLE001 - eviction must not depend on this.
        pass
    return evicted


def _discard_bytecode_cache(module: Any) -> None:
    """Prevent a proven-stale source module from reloading its old `.pyc`.

    Python validates timestamp-based bytecode with the source mtime at
    second precision plus its size. A deployment rewrite can preserve both,
    so `importlib.invalidate_caches()` alone would re-import the bytecode we
    deliberately evicted. This is deliberately limited to the module tree
    being recovered; normal imports retain their bytecode cache.
    """
    source = getattr(module, "__file__", None)
    if not source or not str(source).endswith(".py"):
        return
    # Preserve the import-visible filename. `resolve()` would dereference a
    # symlinked source even though Python keys its `.pyc` by the symlink name.
    source_path = Path(os.path.abspath(os.fspath(source)))
    cache_tag = sys.implementation.cache_tag
    if cache_tag is None:
        raise ValueError("Python bytecode cache tag is unavailable")
    cache_path = Path(
        importlib.util.cache_from_source(str(source_path)),
    )
    expected_cache = source_path.parent / "__pycache__" / (
        f"{source_path.stem}.{cache_tag}.pyc"
    )
    if cache_path != expected_cache:
        raise ValueError(
            f"unexpected bytecode cache path for {source_path}: {cache_path}",
        )
    # A missing cache is harmless. Any other failure must reach the recovery
    # envelope: re-importing after a failed purge could falsely claim success
    # while executing the definition we just proved stale.
    cache_path.unlink(missing_ok=True)


def _already_fresh(
    module: Any,
    class_name: str,
    params: Mapping[str, Any],
) -> bool:
    """Has a concurrent recovery already replaced this definition?

    True only when the loaded class's parameters match the file's on
    BOTH axes this module reasons about — which names are accepted and
    which are required. Comparing names alone would return True for a
    definition that differs only in requiredness, which is exactly the
    axis the ``adapter_requires_argument`` direction is proven on, and
    would then skip an eviction the proof had just established was
    needed.

    Unknown answers are False, so the worst case is a redundant (and
    correct) eviction rather than a skipped one.
    """
    path = _definition_path(module, class_name) or _module_path(module)
    if path is None:
        return False
    disk = on_disk_init_parameters(path, class_name)
    loaded = _loaded_init_parameters(module, class_name)
    if disk is None or loaded is None:
        return False
    if disk["all"] != loaded["all"] or disk["required"] != loaded["required"]:
        return False
    return _accepts_all(loaded, list(params))


def prove_stale_definition(
    *,
    module_name: str | None,
    class_name: str | None,
    arguments: list[str],
    direction: str,
) -> dict[str, Any]:
    """Is the loaded class definition the one that is on disk?

    ``status`` is one of:

    ``stale``
        The on-disk definition and the loaded definition disagree about
        the arguments in question, in the direction that explains the
        failure. Proven: the loaded object is not the file.
    ``current``
        Both definitions agree, so the failure is a genuine
        registry/adapter disagreement.
    ``unknown``
        The comparison could not be made. This is a real answer and must
        not be read as either of the other two.
    """
    if not module_name or not class_name:
        return {"status": "unknown", "reason": "module_or_class_unavailable"}
    module = sys.modules.get(module_name)
    if module is None:
        return {"status": "unknown", "reason": "module_not_loaded"}
    # Read the file the class was DEFINED in, which is not always the
    # module that exports it (re-exports, subclass shims). Falling back
    # to the module's own file keeps the common case working.
    path = _definition_path(module, class_name) or _module_path(module)
    if path is None:
        return {"status": "unknown", "reason": "module_has_no_source_file"}

    disk_params = on_disk_init_parameters(path, class_name)
    loaded_params = _loaded_init_parameters(module, class_name)
    if disk_params is None or loaded_params is None:
        return _mtime_fallback(
            module_name=module_name,
            path=path,
            reason=(
                "on_disk_definition_unparseable"
                if disk_params is None
                else "loaded_definition_uninspectable"
            ),
        )

    if direction == "adapter_rejected_argument":
        disk_accepts = _accepts_all(disk_params, arguments)
        loaded_accepts = _accepts_all(loaded_params, arguments)
        differs = disk_accepts and not loaded_accepts
    else:
        disk_requires = _requires_any(disk_params, arguments)
        loaded_requires = _requires_any(loaded_params, arguments)
        differs = loaded_requires and not disk_requires

    return {
        "status": "stale" if differs else "current",
        "path": str(path),
        "evidence": (
            f"on-disk parameters {sorted(disk_params['all'])} vs loaded "
            f"parameters {sorted(loaded_params['all'])}"
        ),
        "on_disk_parameters": sorted(disk_params["all"]),
        "loaded_parameters": sorted(loaded_params["all"]),
        "basis": "definition_comparison",
    }


def on_disk_init_parameters(
    path: Path,
    class_name: str,
) -> dict[str, Any] | None:
    """Parameters of ``class_name.__init__`` as written in ``path``.

    Parses the file — it never imports it, so a module whose import has
    side effects is not re-executed just to answer this question.
    Returns ``None`` when the file cannot be read or the class or its
    ``__init__`` is not found by static inspection (a class built by a
    factory or a decorator, for instance).
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, ValueError, UnicodeDecodeError):
        return None
    # Module-level definitions only. A class produced by a factory or a
    # decorator inside a function body may not be the object the module
    # exports under this name, and answering from it would be a guess —
    # this returns None instead, which the caller renders as
    # "undetermined".
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for item in node.body:
            if (
                isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name == "__init__"
            ):
                return _parameters_from_ast(item.args)
        # No explicit __init__. A dataclass still has a statically
        # readable one — its annotated class-level fields — and
        # dataclass source adapters are a plausible enough shape that
        # falling through would leave the #945 remedy unable to fire for
        # them.
        if _is_dataclass_decorated(node):
            return _parameters_from_dataclass(node)
        # Otherwise the signature comes from a base class this is not
        # statically resolving.
        return None
    return None


# ----------------------------------------------------------------- helpers


def _parse_signature_type_error(
    exc: BaseException,
    *,
    stage: str,
) -> tuple[list[str], str] | None:
    if stage != "construct" or not isinstance(exc, TypeError):
        return None
    text = str(exc)
    unexpected = _UNEXPECTED_KWARG.search(text)
    if unexpected is not None:
        return [unexpected.group("argument")], "adapter_rejected_argument"
    missing = _MISSING_ARGUMENT.search(text)
    if missing is None:
        return None
    arguments = re.findall(r"'([^']+)'", missing.group("arguments"))
    if not arguments:
        return None
    return arguments, "adapter_requires_argument"


def _is_dataclass_decorated(node: ast.ClassDef) -> bool:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(
            decorator, ast.Call,
        ) else decorator
        name = (
            target.attr if isinstance(target, ast.Attribute)
            else getattr(target, "id", None)
        )
        if name == "dataclass":
            return True
    return False


def _parameters_from_dataclass(node: ast.ClassDef) -> dict[str, Any] | None:
    """Init parameters of a ``@dataclass`` as written in the file.

    Returns ``None`` — unknown, which the caller renders as
    "undetermined" — whenever this cannot be answered exactly. A
    partial answer would be worse than none: reporting the wrong
    parameter set makes the caller assert `registry_adapter_mismatch`
    ("change the registry or the adapter") for a process that is in fact
    serving stale code, which is the confidently-wrong answer #945 cost
    36 hours to.

    Refuses on:

    * any base class other than ``object`` — inherited fields are real
      init parameters and are not statically resolved here;
    * ``field(init=False)`` — declared but NOT an init parameter;
    * the ``KW_ONLY`` sentinel — a marker, not a parameter.

    ``ClassVar`` annotations are skipped: they are not fields at all.
    """
    if [base for base in node.bases if getattr(base, "id", None) != "object"]:
        return None

    named: list[str] = []
    defaulted: set[str] = set()
    for item in node.body:
        if not isinstance(item, ast.AnnAssign):
            continue
        target = item.target
        if not isinstance(target, ast.Name):
            continue
        if _annotation_name(item.annotation) == "ClassVar":
            continue
        if _annotation_name(item.annotation) == "KW_ONLY":
            continue
        init_flag = _field_init_flag(item.value)
        if init_flag is False:
            continue
        if init_flag is None and _is_unreadable_field_call(item.value):
            # A `field(...)` this cannot read: it may or may not be an
            # init parameter, so refuse the whole class.
            return None
        named.append(target.id)
        if item.value is not None and not _is_field_without_default(
            item.value,
        ):
            defaulted.add(target.id)
    return {
        "all": set(named),
        "required": set(named) - defaulted,
        "accepts_var_keyword": False,
    }


def _field_init_flag(value: ast.expr | None) -> bool | None:
    """``init=`` of a literal ``field(...)`` call, if readable."""
    if not _is_field_call(value):
        return None
    for keyword in value.keywords:  # type: ignore[union-attr]
        if keyword.arg != "init":
            continue
        if isinstance(keyword.value, ast.Constant):
            return bool(keyword.value.value)
        return None
    return True


def _is_field_call(value: ast.expr | None) -> bool:
    if not isinstance(value, ast.Call):
        return False
    target = value.func
    name = (
        target.attr if isinstance(target, ast.Attribute)
        else getattr(target, "id", None)
    )
    return name == "field"


def _is_unreadable_field_call(value: ast.expr | None) -> bool:
    """A ``field(...)`` whose ``init=`` is a non-literal expression."""
    if not _is_field_call(value):
        return False
    for keyword in value.keywords:  # type: ignore[union-attr]
        if keyword.arg == "init" and not isinstance(
            keyword.value, ast.Constant,
        ):
            return True
    return False


def _is_field_without_default(value: ast.expr | None) -> bool:
    """``field()`` with neither ``default`` nor ``default_factory``.

    Such a field is still REQUIRED despite having an assigned value.
    """
    if not _is_field_call(value):
        return False
    return not any(
        keyword.arg in {"default", "default_factory"}
        for keyword in value.keywords  # type: ignore[union-attr]
    )


def _annotation_name(annotation: ast.expr) -> str | None:
    node = annotation.value if isinstance(
        annotation, ast.Subscript,
    ) else annotation
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        # A stringised annotation, e.g. `x: "ClassVar[int]"`.
        return node.value.split("[")[0].split(".")[-1].strip()
    return (
        node.attr if isinstance(node, ast.Attribute)
        else getattr(node, "id", None)
    )


def _parameters_from_ast(args: ast.arguments) -> dict[str, Any]:
    named = [
        arg.arg
        for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)
        if arg.arg != "self"
    ]
    required = set(named)
    defaulted = {
        arg.arg
        for arg, default in zip(
            reversed([*args.posonlyargs, *args.args]),
            reversed(args.defaults),
        )
    }
    defaulted |= {
        arg.arg
        for arg, default in zip(args.kwonlyargs, args.kw_defaults)
        if default is not None
    }
    return {
        "all": set(named),
        "required": required - defaulted,
        "accepts_var_keyword": args.kwarg is not None,
    }


def _loaded_init_parameters(
    module: Any,
    class_name: str,
) -> dict[str, Any] | None:
    cls = getattr(module, class_name, None)
    if cls is None:
        return None
    try:
        signature = inspect.signature(cls)
    except (TypeError, ValueError):
        return None
    named: set[str] = set()
    required: set[str] = set()
    accepts_var_keyword = False
    for name, parameter in signature.parameters.items():
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            accepts_var_keyword = True
            continue
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            continue
        named.add(name)
        if parameter.default is inspect.Parameter.empty:
            required.add(name)
    return {
        "all": named,
        "required": required,
        "accepts_var_keyword": accepts_var_keyword,
    }


def _accepts_all(params: Mapping[str, Any], arguments: list[str]) -> bool:
    if params.get("accepts_var_keyword"):
        return True
    return all(name in params["all"] for name in arguments)


def _requires_any(params: Mapping[str, Any], arguments: list[str]) -> bool:
    return any(name in params["required"] for name in arguments)


def _mtime_fallback(
    *,
    module_name: str,
    path: Path,
    reason: str,
) -> dict[str, Any]:
    """Weaker evidence: was the file modified after this process loaded it?

    Only reached when the definitions cannot be compared. "Modified
    since import" proves the loaded copy is not the current file; the
    converse does NOT prove freshness (a deploy can preserve mtimes), so
    a negative answer here reports ``unknown``, not ``current``.
    """
    try:
        from okg.substrate.runtime.code_identity import module_is_stale

        observed = module_is_stale(module_name)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "unknown",
            "reason": f"{reason}; staleness_probe_failed: {exc}",
            "path": str(path),
        }
    if str(observed.get("status")) == "stale":
        return {
            "status": "stale",
            "path": str(path),
            "basis": "module_file_modified_since_import",
            "evidence": (
                f"loaded {observed.get('loaded_at')}, file modified "
                f"{observed.get('modified_at')}"
            ),
            "reason": reason,
        }
    return {
        "status": "unknown",
        "reason": f"{reason}; {observed.get('reason') or observed.get('status')}",
        "path": str(path),
        "basis": "module_file_modified_since_import",
    }


def _definition_path(module: Any, class_name: str) -> Path | None:
    cls = getattr(module, class_name, None)
    if cls is None:
        return None
    try:
        raw = inspect.getfile(cls)
    except (TypeError, OSError):
        return None
    if not raw.endswith(".py"):
        return None
    try:
        return Path(raw).resolve()
    except OSError:
        return None


def _module_path(module: Any) -> Path | None:
    raw = getattr(module, "__file__", None)
    if not raw or not str(raw).endswith(".py"):
        return None
    try:
        return Path(raw).resolve()
    except OSError:
        return None

"""Callsite-level inventory for graph mutation and publication authority."""

from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_MUTATION_RE = re.compile(
    r"\b(INSERT\s+INTO|COPY|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?|"
    r"MERGE\s+INTO)\s+(?:ONLY\s+)?(?:okg\.)?([a-zA-Z_][a-zA-Z0-9_]*|"
    r"\{dynamic\})",
    re.IGNORECASE,
)
_RUNTIME_TARGETS = frozenset(
    {
        "node_facts",
        "edge_facts",
        "graph_nodes",
        "graph_edges",
        "nodes_live",
        "edges_live",
        "graph_generations",
        "graph_generation_checkpoints",
        "chronos_graph_nodes_current",
        "chronos_graph_edges_current",
    }
)
_SQL_CALLS = frozenset({"execute", "executemany", "copy"})
_FORMAT_FIELD_RE = re.compile(r"\{[^{}]*\}")
_PERCENT_FIELD_RE = re.compile(r"%(?:\([^)]+\))?[#0 +\-]*\d*(?:\.\d+)?[a-zA-Z]")


class WriteInventoryParseError(RuntimeError):
    """Raised when a runtime Python file cannot be inventoried safely."""


@dataclass(frozen=True, order=True)
class WriteCallsite:
    fingerprint: str
    file: str
    symbol: str
    operation: str
    target: str
    line: int

    def to_dict(self) -> dict[str, object]:
        return {
            "fingerprint": self.fingerprint,
            "file": self.file,
            "symbol": self.symbol,
            "operation": self.operation,
            "target": self.target,
            "line": self.line,
        }


def discover_write_callsites(
    root: Path,
    *,
    source_roots: Iterable[Path] | None = None,
) -> tuple[WriteCallsite, ...]:
    """Parse Python runtime callsites; regex is used only inside SQL text."""
    roots = tuple(source_roots or (root / "src" / "okg",))
    discovered: list[WriteCallsite] = []
    for source_root in roots:
        for path in sorted(source_root.rglob("*.py")):
            if any(part in {"tests", "test", "__pycache__"} for part in path.parts):
                continue
            discovered.extend(_discover_file(root, path))
    return tuple(sorted(discovered))


def _discover_file(root: Path, path: Path) -> list[WriteCallsite]:
    text = path.read_text(encoding="utf-8")
    relative = path.relative_to(root).as_posix()
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        raise WriteInventoryParseError(
            f"cannot inventory syntactically invalid runtime file {relative}"
        ) from exc
    assignments = _assignment_index(tree)
    parents = _parent_index(tree)
    found: list[WriteCallsite] = []
    occurrence: dict[tuple[str, str, str], int] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        call_name = _call_name(node.func)
        records: list[tuple[str, str]] = []
        if call_name == "apply_facts":
            records.append(("APPLY_FACTS", "chronos_graph"))
        elif call_name == "checkpoint_generation":
            records.append(("CHECKPOINT", "graph_generations"))
        elif call_name in _SQL_CALLS and node.args:
            sql = _string_value(node.args[0], assignments)
            if sql is not None:
                for match in _MUTATION_RE.finditer(sql):
                    operation = " ".join(match.group(1).upper().split())
                    target = match.group(2).lower()
                    if target in _RUNTIME_TARGETS or target == "{dynamic}":
                        records.append((operation, target))
                records.extend(_additional_truncate_targets(sql, records))
        for operation, target in records:
            symbol = _enclosing_symbol(node, parents)
            key = (symbol, operation, target)
            index = occurrence.get(key, 0)
            occurrence[key] = index + 1
            canonical = "|".join((relative, symbol, operation, target, str(index)))
            fingerprint = (
                "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            )
            found.append(
                WriteCallsite(
                    fingerprint=fingerprint,
                    file=relative,
                    symbol=symbol,
                    operation=operation,
                    target=target,
                    line=node.lineno,
                )
            )
    return found


def _assignment_index(tree: ast.AST) -> dict[str, ast.AST]:
    result: dict[str, ast.AST] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    result[target.id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.value is not None:
                result[node.target.id] = node.value
    return result


def _parent_index(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    result: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            result[child] = parent
    return result


def _enclosing_symbol(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str:
    names: list[str] = []
    current = parents.get(node)
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(current.name)
        current = parents.get(current)
    return ".".join(reversed(names)) or "<module>"


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _string_value(
    node: ast.AST,
    assignments: dict[str, ast.AST],
    seen: frozenset[str] = frozenset(),
) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            else:
                parts.append("{dynamic}")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _string_value(node.left, assignments, seen)
        right = _string_value(node.right, assignments, seen)
        if left is not None and right is not None:
            return left + right
        return None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        template = _string_value(node.left, assignments, seen)
        if template is not None:
            return _PERCENT_FIELD_RE.sub("{dynamic}", template)
        return None
    if isinstance(node, ast.Call):
        if _call_name(node.func) == "SQL" and node.args:
            return _string_value(node.args[0], assignments, seen)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
            template = _string_value(node.func.value, assignments, seen)
            if template is not None:
                return _FORMAT_FIELD_RE.sub("{dynamic}", template)
        return None
    if isinstance(node, ast.Name) and node.id not in seen:
        assigned = assignments.get(node.id)
        if assigned is not None:
            return _string_value(assigned, assignments, seen | {node.id})
    return None


def _additional_truncate_targets(
    sql: str,
    existing: Iterable[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Return protected targets after the first target in a TRUNCATE list."""
    already = set(existing)
    if not any(operation == "TRUNCATE" for operation, _target in already):
        return []
    found: list[tuple[str, str]] = []
    for target in sorted(_RUNTIME_TARGETS):
        if not re.search(
            rf"(?<![a-zA-Z0-9_])(?:okg\.)?{re.escape(target)}(?![a-zA-Z0-9_])",
            sql,
            re.IGNORECASE,
        ):
            continue
        record = ("TRUNCATE", target)
        if record not in already:
            found.append(record)
    return found


__all__ = [
    "WriteCallsite",
    "WriteInventoryParseError",
    "discover_write_callsites",
]

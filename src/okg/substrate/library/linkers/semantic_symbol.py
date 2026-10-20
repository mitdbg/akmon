"""Shared semantic symbol matching helpers for deterministic linkers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


__all__ = [
    "SemanticSymbolIndex",
    "SemanticSymbolMatch",
    "has_semantic_symbol_hint",
    "resolution_method_attrs",
]


SEMANTIC_KEY_HINT_KEYS = (
    "target_semantic_key",
    "semantic_target_key",
    "symbol_semantic_key",
    "code_symbol_semantic_key",
    "handler_semantic_key",
    "function_semantic_key",
    "source_semantic_key",
    "semantic_key",
)

CANONICAL_NAME_HINT_KEYS = (
    "target_canonical_name",
    "symbol_canonical_name",
    "code_symbol_canonical_name",
    "handler_canonical_name",
    "function_canonical_name",
    "source_canonical_name",
    "canonical_name",
)


@dataclass(frozen=True)
class SemanticSymbolMatch:
    node_id: str
    resolution: str
    match_key: str
    match_value: str

    @property
    def attrs(self) -> dict[str, str]:
        return {
            "resolution_method": "semantic_identity",
            "semantic_match_key": self.match_key,
            "semantic_match_value": self.match_value,
        }


class SemanticSymbolIndex:
    """Unique-match lookup for line-independent code_symbol identity attrs."""

    def __init__(self, symbols: Iterable[Any]) -> None:
        by_semantic_key: dict[str, list[str]] = {}
        by_canonical_name: dict[str, list[str]] = {}
        for symbol in symbols:
            node_id = str(getattr(symbol, "node_id", "") or "")
            attrs = getattr(symbol, "attrs", None)
            if not node_id or not isinstance(attrs, Mapping):
                continue
            semantic_key = _clean(attrs.get("semantic_key"))
            if semantic_key:
                by_semantic_key.setdefault(semantic_key, []).append(node_id)
            canonical_name = _clean(attrs.get("canonical_name"))
            if canonical_name:
                by_canonical_name.setdefault(canonical_name, []).append(node_id)
        self._by_semantic_key = _unique_values(by_semantic_key)
        self._by_canonical_name = _unique_values(by_canonical_name)

    def match_attrs(self, attrs: Mapping[str, Any]) -> SemanticSymbolMatch | None:
        for attr_name in SEMANTIC_KEY_HINT_KEYS:
            value = _clean(attrs.get(attr_name))
            if not value:
                continue
            node_id = self._by_semantic_key.get(value)
            if node_id:
                return SemanticSymbolMatch(
                    node_id=node_id,
                    resolution=f"{attr_name}_semantic_key",
                    match_key=attr_name,
                    match_value=value,
                )
        for attr_name in CANONICAL_NAME_HINT_KEYS:
            value = _clean(attrs.get(attr_name))
            if not value:
                continue
            node_id = self._by_canonical_name.get(value)
            if node_id:
                return SemanticSymbolMatch(
                    node_id=node_id,
                    resolution=f"{attr_name}_canonical_name",
                    match_key=attr_name,
                    match_value=value,
                )
        return None

    def by_semantic_key(self, semantic_key: Any) -> SemanticSymbolMatch | None:
        value = _clean(semantic_key)
        if not value:
            return None
        node_id = self._by_semantic_key.get(value)
        if not node_id:
            return None
        return SemanticSymbolMatch(
            node_id=node_id,
            resolution="semantic_key",
            match_key="semantic_key",
            match_value=value,
        )


def has_semantic_symbol_hint(attrs: Mapping[str, Any]) -> bool:
    return any(
        _clean(attrs.get(key))
        for key in (*SEMANTIC_KEY_HINT_KEYS, *CANONICAL_NAME_HINT_KEYS)
    )


def resolution_method_attrs(resolution: str) -> dict[str, str]:
    if "semantic_key" in resolution or "canonical_name" in resolution:
        return {"resolution_method": "semantic_identity"}
    if resolution.endswith("_id") or resolution in {
        "handler_symbol_id",
        "source_symbol_id",
    }:
        return {"resolution_method": "node_id"}
    if "line" in resolution or "span" in resolution:
        return {"resolution_method": "line_span"}
    if "name" in resolution or "handler" in resolution or "symbol_hint" in resolution:
        return {"resolution_method": "name_hint"}
    return {"resolution_method": "heuristic"}


def _unique_values(values: dict[str, list[str]]) -> dict[str, str]:
    return {
        key: node_ids[0]
        for key, node_ids in values.items()
        if len(set(node_ids)) == 1
    }


def _clean(value: Any) -> str:
    return str(value or "").strip()

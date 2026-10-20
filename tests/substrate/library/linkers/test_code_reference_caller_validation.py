"""Stale-caller validation in the code_reference linker
(harden-producer-emission-coherence
req.coherence.linker-caller-validated).

Symbol ids bake line numbers in, so a reference node's
enclosing_symbol_id goes stale on any edit until the owning file
re-syncs. A calls edge from a symbol nobody emits fails
producer-signature admission and zeroes the whole enricher run (the
live 00:13Z 2026-06-11 rejection). The caller must be validated against
the same live symbol set used for dst resolution.
"""

from __future__ import annotations

from typing import Any

import pytest

import okg.substrate.library.linkers.code_reference as code_reference

CodeReferenceResolver = code_reference.CodeReferenceResolver
_Node = code_reference._Node


def _patch_nodes(monkeypatch: pytest.MonkeyPatch, *, symbols, references):
    nodes: dict[str, list[_Node]] = {
        "code_import": [],
        "code_reference": references,
        "code_symbol": symbols,
        "dependency_package": [],
    }

    def fake_fetch(cur: Any, subtype: str) -> list[_Node]:
        return nodes[subtype]

    monkeypatch.setattr(code_reference, "_fetch_nodes", fake_fetch)


_CALLEE = _Node("symbol:okg:src/okg/a.py:callee:5", {
    "repo": "okg",
    "path": "src/okg/a.py",
    "name": "callee",
    "qualified_name": "callee",
})


def _reference(enclosing: str) -> _Node:
    return _Node("reference:okg:src/okg/b.py:callee:12", {
        "repo": "okg",
        "path": "src/okg/a.py",
        "name": "callee",
        "qualified_name": "callee",
        "enclosing_symbol_id": enclosing,
        "start_line": 12,
    })


def test_stale_caller_emits_no_calls_edge_and_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_caller = "symbol:okg:src/okg/b.py:caller:3668"  # not in symbols
    _patch_nodes(
        monkeypatch,
        symbols=[_CALLEE],
        references=[_reference(stale_caller)],
    )
    resolver = CodeReferenceResolver()
    edges, unmatched = resolver._resolve_edges(None, skip_existing=False)

    edge_types = {edge_type for _, edge_type, _, _ in edges}
    assert "calls" not in edge_types
    # The references edge (reference node -> resolved symbol) still emits.
    assert ("reference:okg:src/okg/b.py:callee:12", "references",
            _CALLEE.node_id) in {(s, e, d) for s, e, d, _ in edges}
    assert unmatched["callers"] == [{
        "node_id": "reference:okg:src/okg/b.py:callee:12",
        "caller": stale_caller,
        "callee": _CALLEE.node_id,
        "reason": "enclosing_symbol_id_not_in_live_symbols",
    }]


def test_live_caller_still_emits_calls_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caller = _Node("symbol:okg:src/okg/b.py:caller:3", {
        "repo": "okg",
        "path": "src/okg/b.py",
        "name": "caller",
        "qualified_name": "caller",
    })
    _patch_nodes(
        monkeypatch,
        symbols=[_CALLEE, caller],
        references=[_reference(caller.node_id)],
    )
    resolver = CodeReferenceResolver()
    edges, unmatched = resolver._resolve_edges(None, skip_existing=False)

    assert (caller.node_id, "calls", _CALLEE.node_id) in {
        (s, e, d) for s, e, d, _ in edges
    }
    assert unmatched["callers"] == []

from __future__ import annotations

from unittest.mock import patch

from okg.substrate.library.linkers.coverage_symbol import (
    _CodeSymbol,
    _Node,
    _filter_new_edges,
    _resolve_coverage_symbol_edges,
)


def test_coverage_symbol_edges_link_executed_lines_to_enclosing_symbols() -> None:
    report = _Node(
        node_id="coverage-report:repo:1",
        attrs={
            "covered_lines": {"src/demo/api.py": [5, 6, 21]},
            "missing_lines": {"src/demo/api.py": [7, 8]},
        },
    )
    symbols = [
        _CodeSymbol(
            node_id="symbol:repo:api:covered",
            path="src/demo/api.py",
            start_line=4,
            end_line=8,
            kind="function",
        ),
        _CodeSymbol(
            node_id="symbol:repo:api:missed",
            path="src/demo/api.py",
            start_line=12,
            end_line=18,
            kind="function",
        ),
    ]

    edges = _resolve_coverage_symbol_edges([report], symbols)

    assert len(edges) == 1
    assert edges[0][0:3] == (
        "coverage-report:repo:1",
        "references",
        "symbol:repo:api:covered",
    )
    assert edges[0][3]["covered_line_count"] == 2
    assert edges[0][3]["missing_line_count"] == 2


def test_coverage_symbol_edges_skip_existing_links() -> None:
    report = _Node(
        node_id="coverage-report:repo:1",
        attrs={"covered_lines": {"src/demo/api.py": [5]}},
    )
    symbol = _CodeSymbol(
        node_id="symbol:repo:api:covered",
        path="src/demo/api.py",
        start_line=4,
        end_line=8,
        kind="function",
    )

    edges = _resolve_coverage_symbol_edges([report], [symbol])

    with patch(
        "okg.substrate.library.linkers.coverage_symbol._chronos.query",
        return_value=[{
            "src": "coverage-report:repo:1",
            "edge_type": "references",
            "dst": symbol.node_id,
        }],
    ) as query:
        filtered = _filter_new_edges(None, edges)

    assert filtered == []
    assert "edge_id = ANY(:edge_ids)" in query.call_args.args[1]

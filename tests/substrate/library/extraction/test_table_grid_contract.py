from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from decimal import Decimal

import pytest

from okg.substrate.library.extraction.table_grid import (
    TABLE_GRID_STATUSES,
    TableExtractionResult,
    TableGridError,
    TableGridV1,
    canonical_pdf_bbox,
    latex_table_locator,
    pdf_table_locator,
    table_occurrence_id,
    validate_table_grid,
)


def _grid_mapping() -> dict[str, object]:
    return {
        "schema_version": 1,
        "column_count": 3,
        "col_headers": [" Method ", "Cafe\u0301 metric", " Notes\r\ncolumn "],
        "row_label_column_index": 0,
        "rows": [
            {
                "row_index": 0,
                "row_label": " Alpha ",
                "cells": [
                    {
                        "column_index": 0,
                        "column_header": "Method",
                        "text": "Alpha",
                        "row_span": 1,
                        "column_span": 1,
                    },
                    {
                        "column_index": 1,
                        "column_header": "Café metric / Notes column",
                        "text": " 10\t±\n2 ",
                        "row_span": 1,
                        "column_span": 2,
                    },
                ],
            },
            {
                "row_index": 1,
                "row_label": "Beta",
                "cells": [
                    {
                        "column_index": 0,
                        "column_header": "Method",
                        "text": "Beta",
                        "row_span": 1,
                        "column_span": 1,
                    },
                    {
                        "column_index": 1,
                        "column_header": "Café metric",
                        "text": "11",
                        "row_span": 1,
                        "column_span": 1,
                    },
                    {
                        "column_index": 2,
                        "column_header": "Notes column",
                        "text": "✓",
                        "row_span": 1,
                        "column_span": 1,
                    },
                ],
            },
        ],
    }


def _plain_grid_mapping() -> dict[str, object]:
    value = _grid_mapping()
    value["col_headers"] = ["Method", "Café metric", "Notes column"]
    rows = value["rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    first["row_label"] = "Alpha"
    cells = first["cells"]
    assert isinstance(cells, list)
    second_cell = cells[1]
    assert isinstance(second_cell, dict)
    second_cell["text"] = "10 ± 2"
    return value


def test_both_lanes_share_byte_identical_grid_and_search_text() -> None:
    latex_grid = TableGridV1.from_mapping(_grid_mapping())
    provider_grid = TableGridV1.from_json(
        json.dumps(_plain_grid_mapping(), ensure_ascii=False)
    )

    assert latex_grid == provider_grid
    assert latex_grid.canonical_bytes() == provider_grid.canonical_bytes()
    assert latex_grid.searchable_text() == provider_grid.searchable_text()
    assert latex_grid.searchable_text() == (
        "Method\tCafé metric\tNotes column\nAlpha\t10 ± 2\t\nBeta\t11\t✓"
    )


def test_canonical_json_is_sorted_compact_utf8_and_round_trips() -> None:
    grid = validate_table_grid(_grid_mapping())
    encoded = grid.canonical_bytes()

    assert b'": ' not in encoded
    assert b', "' not in encoded
    assert "✓".encode() in encoded
    assert b"\\u2713" not in encoded
    assert TableGridV1.from_json(encoded) == grid
    assert json.loads(encoded) == grid.to_mapping()


def test_grid_schema_is_exact_and_normalizes_every_string() -> None:
    grid = TableGridV1.from_mapping(_grid_mapping())

    assert grid.schema_version == 1
    assert grid.col_headers == ("Method", "Café metric", "Notes column")
    assert grid.rows[0].row_label == "Alpha"
    assert grid.rows[0].cells[1].text == "10 ± 2"
    assert set(grid.to_mapping()) == {
        "schema_version",
        "column_count",
        "col_headers",
        "row_label_column_index",
        "rows",
    }

    extra = _grid_mapping()
    extra["producer"] = "latex"
    with pytest.raises(TableGridError, match="extra keys") as exc_info:
        TableGridV1.from_mapping(extra)
    assert exc_info.value.code == "invalid_object_shape"


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda grid: grid.update(schema_version=2), "unsupported_schema_version"),
        (lambda grid: grid.update(column_count=0), "invalid_integer"),
        (lambda grid: grid.update(column_count=True), "invalid_integer"),
        (
            lambda grid: grid.update(col_headers=["only one"]),
            "column_header_count_mismatch",
        ),
        (
            lambda grid: grid.update(row_label_column_index=1),
            "unsupported_row_label_column",
        ),
        (lambda grid: grid.update(rows=[]), "empty_grid"),
        (lambda grid: grid["rows"][0].update(row_index=1), "noncontiguous_row_index"),
        (lambda grid: grid["rows"][0].update(row_label=None), "row_label_mismatch"),
        (lambda grid: grid["rows"][0].update(cells=[]), "empty_row"),
        (
            lambda grid: grid["rows"][0]["cells"][0].update(row_span=2),
            "unsupported_row_span",
        ),
        (
            lambda grid: grid["rows"][0]["cells"][0].update(column_span=0),
            "invalid_integer",
        ),
        (
            lambda grid: grid["rows"][0]["cells"][1].update(column_index=2),
            "invalid_column_coverage",
        ),
        (
            lambda grid: grid["rows"][0]["cells"][1].update(column_span=3),
            "invalid_column_coverage",
        ),
        (
            lambda grid: grid["rows"][0]["cells"][1].update(column_header="wrong"),
            "column_header_mismatch",
        ),
        (
            lambda grid: grid["rows"][0]["cells"][0].update(text=None),
            "invalid_string",
        ),
    ],
)
def test_grid_rejects_every_type_and_coverage_violation(mutate, code: str) -> None:
    value = deepcopy(_grid_mapping())
    mutate(value)
    with pytest.raises(TableGridError) as exc_info:
        TableGridV1.from_mapping(value)
    assert exc_info.value.code == code


def test_row_label_must_be_null_without_label_column() -> None:
    value = _plain_grid_mapping()
    value["row_label_column_index"] = None
    rows = value["rows"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        row["row_label"] = None
    assert TableGridV1.from_mapping(value).rows[0].row_label is None

    rows[0]["row_label"] = "Alpha"
    with pytest.raises(TableGridError) as exc_info:
        TableGridV1.from_mapping(value)
    assert exc_info.value.code == "row_label_mismatch"


def test_occurrence_identity_preserves_duplicate_tables_and_non_ascii_doc_ids() -> None:
    first = table_occurrence_id("doc:résumé", latex_table_locator(12, 40))
    second = table_occurrence_id("doc:résumé", latex_table_locator(80, 108))

    assert first.startswith("chunk:table:")
    assert len(first.removeprefix("chunk:table:")) == 16
    assert first != second
    assert first == table_occurrence_id("doc:résumé", "tex:12:40")
    expected = hashlib.sha256("doc:résumé\0tex:12:40".encode()).hexdigest()[:16]
    assert first == f"chunk:table:{expected}"


def test_source_locators_are_canonical_and_pdf_rounding_is_half_even() -> None:
    assert latex_table_locator(0, 25) == "tex:0:25"
    assert pdf_table_locator(3, [0, "1.2345", Decimal("2.3455"), 10]) == (
        "pdf:3:0.000,1.234,2.346,10.000"
    )
    assert canonical_pdf_bbox(["-0.0000", "0.0004", "0.0006", "1.0"]) == (
        Decimal("0"),
        Decimal("0"),
        Decimal("0.001"),
        Decimal("1.000"),
    )

    with pytest.raises(TableGridError) as exc_info:
        canonical_pdf_bbox([0, 0, "0.0004", 1])
    assert exc_info.value.code == "invalid_pdf_bbox"
    with pytest.raises(TableGridError):
        latex_table_locator(4, 4)
    with pytest.raises(TableGridError):
        table_occurrence_id("doc:1", "tex:04:10")


def test_parsed_and_failed_occurrence_results_are_closed_and_explicit() -> None:
    grid = TableGridV1.from_mapping(_grid_mapping())
    digest = hashlib.sha256(b"raw table").hexdigest()
    parsed = TableExtractionResult(
        lane="latex",
        status="parsed",
        source_locator="tex:12:40",
        source_content_sha256=digest,
        grid=grid,
    )
    parsed_attrs = parsed.to_chunk_extras()
    assert parsed_attrs["table_grid"] == grid.to_mapping()
    assert parsed_attrs["table_grid_error_code"] is None

    for status in TABLE_GRID_STATUSES - {"parsed"}:
        failed = TableExtractionResult(
            lane="pdf_vlm",
            status=status,
            source_locator="pdf:0:0.000,0.000,10.000,20.000",
            source_content_sha256=digest,
            error_code=f"fixture_{status}",
            cache_signature="cache:v1",
        )
        attrs = failed.to_chunk_extras()
        assert "table_grid" not in attrs
        assert attrs["table_grid_error_code"] == f"fixture_{status}"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": "parsed", "grid": None, "error_code": None},
        {"status": "parsed", "grid": _grid_mapping(), "error_code": None},
        {
            "status": "parsed",
            "grid": TableGridV1.from_mapping(_grid_mapping()),
            "error_code": "bad",
        },
        {
            "status": "parse_failed",
            "grid": TableGridV1.from_mapping(_grid_mapping()),
            "error_code": "bad",
        },
        {"status": "parse_failed", "grid": None, "error_code": None},
        {"status": "unknown", "grid": None, "error_code": "bad"},
    ],
)
def test_failure_contract_never_synthesizes_or_reuses_a_grid(kwargs) -> None:
    with pytest.raises(TableGridError):
        TableExtractionResult(
            lane="latex",
            source_locator="tex:0:10",
            source_content_sha256=hashlib.sha256(b"table").hexdigest(),
            **kwargs,
        )

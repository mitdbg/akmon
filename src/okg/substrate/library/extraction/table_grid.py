"""Canonical structured-table value, identity, and failure contract.

Both deterministic and model-backed table extractors lower into the types in
this module.  Keeping validation and rendering here prevents either lane from
silently inventing a slightly different grid representation.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from typing import Any, Final, Mapping, Sequence


TABLE_GRID_SCHEMA_VERSION: Final = 1
TABLE_GRID_STATUSES: Final = frozenset(
    {
        "parsed",
        "parse_failed",
        "provider_failed",
        "budget_exhausted",
    }
)

_GRID_KEYS = frozenset(
    {
        "schema_version",
        "column_count",
        "col_headers",
        "row_label_column_index",
        "rows",
    }
)
_ROW_KEYS = frozenset({"row_index", "row_label", "cells"})
_CELL_KEYS = frozenset(
    {
        "column_index",
        "column_header",
        "text",
        "row_span",
        "column_span",
    }
)
_LOWER_SNAKE_RE = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_TEX_LOCATOR_RE = re.compile(r"tex:(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)\Z")
_PDF_LOCATOR_RE = re.compile(
    r"pdf:(?:0|[1-9][0-9]*):"
    r"(?:0|[1-9][0-9]*)\.[0-9]{3},"
    r"(?:0|[1-9][0-9]*)\.[0-9]{3},"
    r"(?:0|[1-9][0-9]*)\.[0-9]{3},"
    r"(?:0|[1-9][0-9]*)\.[0-9]{3}\Z"
)
_PDF_QUANTUM = Decimal("0.001")


class TableGridError(ValueError):
    """A stable, typed rejection of a malformed table-grid value."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> None:
    raise TableGridError(code, message)


def _exact_int(value: Any, *, field: str, minimum: int | None = None) -> int:
    if type(value) is not int:
        _fail("invalid_integer", f"{field} must be an integer")
    if minimum is not None and value < minimum:
        _fail("invalid_integer", f"{field} must be >= {minimum}")
    return value


def _sequence(value: Any, *, field: str) -> Sequence[Any]:
    if not isinstance(value, (list, tuple)):
        _fail("invalid_sequence", f"{field} must be a list")
    return value


def _mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail("invalid_object", f"{field} must be an object")
    if not all(isinstance(key, str) for key in value):
        _fail("invalid_object_key", f"{field} keys must be strings")
    return value


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    field: str,
) -> None:
    actual = frozenset(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        _fail(
            "invalid_object_shape",
            f"{field} has missing keys {missing!r} and extra keys {extra!r}",
        )


def normalize_table_text(value: Any, *, field: str = "value") -> str:
    """Apply TableGridV1 Unicode and whitespace normalization."""

    if not isinstance(value, str):
        _fail("invalid_string", f"{field} must be a string")
    normalized = unicodedata.normalize(
        "NFC", value.replace("\r\n", "\n").replace("\r", "\n")
    )
    return " ".join(normalized.split())


@dataclass(frozen=True, slots=True)
class TableCellV1:
    column_index: int
    column_header: str
    text: str
    row_span: int = 1
    column_span: int = 1

    def __post_init__(self) -> None:
        _exact_int(self.column_index, field="column_index", minimum=0)
        _exact_int(self.row_span, field="row_span")
        _exact_int(self.column_span, field="column_span", minimum=1)
        if self.row_span != 1:
            _fail("unsupported_row_span", "TableGridV1 only supports row_span=1")
        object.__setattr__(
            self,
            "column_header",
            normalize_table_text(self.column_header, field="column_header"),
        )
        object.__setattr__(self, "text", normalize_table_text(self.text, field="text"))

    @classmethod
    def from_mapping(cls, value: Any, *, row_index: int) -> TableCellV1:
        item = _mapping(value, field=f"rows[{row_index}].cells[]")
        _require_exact_keys(item, _CELL_KEYS, field=f"rows[{row_index}].cells[]")
        return cls(
            column_index=_exact_int(
                item["column_index"],
                field=f"rows[{row_index}].cells[].column_index",
                minimum=0,
            ),
            column_header=normalize_table_text(
                item["column_header"],
                field=f"rows[{row_index}].cells[].column_header",
            ),
            text=normalize_table_text(
                item["text"], field=f"rows[{row_index}].cells[].text"
            ),
            row_span=_exact_int(
                item["row_span"], field=f"rows[{row_index}].cells[].row_span"
            ),
            column_span=_exact_int(
                item["column_span"],
                field=f"rows[{row_index}].cells[].column_span",
                minimum=1,
            ),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "column_index": self.column_index,
            "column_header": self.column_header,
            "text": self.text,
            "row_span": self.row_span,
            "column_span": self.column_span,
        }


@dataclass(frozen=True, slots=True)
class TableRowV1:
    row_index: int
    row_label: str | None
    cells: tuple[TableCellV1, ...]

    def __post_init__(self) -> None:
        _exact_int(self.row_index, field="row_index", minimum=0)
        if self.row_label is not None:
            object.__setattr__(
                self,
                "row_label",
                normalize_table_text(self.row_label, field="row_label"),
            )
        if not isinstance(self.cells, tuple) or not all(
            isinstance(cell, TableCellV1) for cell in self.cells
        ):
            _fail("invalid_cells", "cells must be a tuple of TableCellV1 values")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "row_index": self.row_index,
            "row_label": self.row_label,
            "cells": [cell.to_mapping() for cell in self.cells],
        }


@dataclass(frozen=True, slots=True)
class TableGridV1:
    """Validated, immutable semantic table grid."""

    column_count: int
    col_headers: tuple[str, ...]
    row_label_column_index: int | None
    rows: tuple[TableRowV1, ...]
    schema_version: int = TABLE_GRID_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _exact_int(self.schema_version, field="schema_version")
        if self.schema_version != TABLE_GRID_SCHEMA_VERSION:
            _fail(
                "unsupported_schema_version",
                f"schema_version must be {TABLE_GRID_SCHEMA_VERSION}",
            )
        _exact_int(self.column_count, field="column_count", minimum=1)
        if not isinstance(self.col_headers, tuple):
            _fail("invalid_sequence", "col_headers must be a tuple")
        normalized_headers = tuple(
            normalize_table_text(header, field=f"col_headers[{index}]")
            for index, header in enumerate(self.col_headers)
        )
        object.__setattr__(self, "col_headers", normalized_headers)
        if len(normalized_headers) != self.column_count:
            _fail(
                "column_header_count_mismatch",
                "col_headers must contain exactly column_count strings",
            )
        if self.row_label_column_index not in (None, 0):
            _fail(
                "unsupported_row_label_column",
                "row_label_column_index must be 0 or null",
            )
        if not isinstance(self.rows, tuple) or not self.rows:
            _fail("empty_grid", "rows must be a non-empty tuple")
        for expected_row_index, row in enumerate(self.rows):
            if not isinstance(row, TableRowV1):
                _fail("invalid_row", "rows must contain TableRowV1 values")
            if row.row_index != expected_row_index:
                _fail(
                    "noncontiguous_row_index",
                    "row_index values must be contiguous, ordered, and zero-based",
                )
            _validate_row(
                row_index=row.row_index,
                row_label=row.row_label,
                cells=row.cells,
                headers=normalized_headers,
                column_count=self.column_count,
                row_label_column_index=self.row_label_column_index,
            )

    @classmethod
    def from_mapping(cls, value: Any) -> TableGridV1:
        item = _mapping(value, field="table_grid")
        _require_exact_keys(item, _GRID_KEYS, field="table_grid")
        version = _exact_int(item["schema_version"], field="schema_version")
        if version != TABLE_GRID_SCHEMA_VERSION:
            _fail(
                "unsupported_schema_version",
                f"schema_version must be {TABLE_GRID_SCHEMA_VERSION}",
            )
        column_count = _exact_int(item["column_count"], field="column_count", minimum=1)
        headers = tuple(
            normalize_table_text(header, field=f"col_headers[{index}]")
            for index, header in enumerate(
                _sequence(item["col_headers"], field="col_headers")
            )
        )
        if len(headers) != column_count:
            _fail(
                "column_header_count_mismatch",
                "col_headers must contain exactly column_count strings",
            )

        row_label_column_index = item["row_label_column_index"]
        if row_label_column_index is not None:
            row_label_column_index = _exact_int(
                row_label_column_index, field="row_label_column_index"
            )
            if row_label_column_index != 0:
                _fail(
                    "unsupported_row_label_column",
                    "row_label_column_index must be 0 or null",
                )

        raw_rows = _sequence(item["rows"], field="rows")
        if not raw_rows:
            _fail("empty_grid", "rows must be non-empty")
        rows: list[TableRowV1] = []
        for expected_row_index, raw_row in enumerate(raw_rows):
            row = _mapping(raw_row, field=f"rows[{expected_row_index}]")
            _require_exact_keys(row, _ROW_KEYS, field=f"rows[{expected_row_index}]")
            row_index = _exact_int(
                row["row_index"], field=f"rows[{expected_row_index}].row_index"
            )
            if row_index != expected_row_index:
                _fail(
                    "noncontiguous_row_index",
                    "row_index values must be contiguous, ordered, and zero-based",
                )
            row_label_value = row["row_label"]
            row_label = (
                None
                if row_label_value is None
                else normalize_table_text(
                    row_label_value, field=f"rows[{row_index}].row_label"
                )
            )
            cells = tuple(
                TableCellV1.from_mapping(raw_cell, row_index=row_index)
                for raw_cell in _sequence(
                    row["cells"], field=f"rows[{row_index}].cells"
                )
            )
            _validate_row(
                row_index=row_index,
                row_label=row_label,
                cells=cells,
                headers=headers,
                column_count=column_count,
                row_label_column_index=row_label_column_index,
            )
            rows.append(TableRowV1(row_index, row_label, cells))

        return cls(
            schema_version=version,
            column_count=column_count,
            col_headers=headers,
            row_label_column_index=row_label_column_index,
            rows=tuple(rows),
        )

    @classmethod
    def from_json(cls, value: bytes | str) -> TableGridV1:
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise TableGridError(
                    "invalid_json", "table grid JSON must be UTF-8"
                ) from exc
        if not isinstance(value, str):
            _fail("invalid_json", "table grid JSON must be bytes or a string")
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise TableGridError("invalid_json", "table grid JSON is invalid") from exc
        return cls.from_mapping(parsed)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "column_count": self.column_count,
            "col_headers": list(self.col_headers),
            "row_label_column_index": self.row_label_column_index,
            "rows": [row.to_mapping() for row in self.rows],
        }

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.to_mapping(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def searchable_text(self) -> str:
        lines = ["\t".join(self.col_headers)]
        for row in self.rows:
            physical = [""] * self.column_count
            for cell in row.cells:
                physical[cell.column_index] = cell.text
            lines.append("\t".join(physical))
        return "\n".join(lines)


def _validate_row(
    *,
    row_index: int,
    row_label: str | None,
    cells: tuple[TableCellV1, ...],
    headers: tuple[str, ...],
    column_count: int,
    row_label_column_index: int | None,
) -> None:
    if not cells:
        _fail("empty_row", f"rows[{row_index}].cells must be non-empty")
    cursor = 0
    for cell in cells:
        if cell.row_span != 1:
            _fail("unsupported_row_span", "TableGridV1 only supports row_span=1")
        if cell.column_index != cursor:
            _fail(
                "invalid_column_coverage",
                f"rows[{row_index}] cells must be ordered and cover every column",
            )
        end = cell.column_index + cell.column_span
        if end > column_count:
            _fail(
                "invalid_column_coverage",
                f"rows[{row_index}] cell extends beyond column_count",
            )
        expected_header = normalize_table_text(
            " / ".join(headers[cell.column_index : end]), field="expected_column_header"
        )
        if cell.column_header != expected_header:
            _fail(
                "column_header_mismatch",
                f"rows[{row_index}] cell header does not match covered columns",
            )
        cursor = end
    if cursor != column_count:
        _fail(
            "invalid_column_coverage",
            f"rows[{row_index}] cells do not cover every physical column",
        )

    if row_label_column_index is None:
        if row_label is not None:
            _fail(
                "row_label_mismatch",
                f"rows[{row_index}].row_label must be null without a label column",
            )
    elif row_label != cells[0].text:
        _fail(
            "row_label_mismatch",
            f"rows[{row_index}].row_label must equal the cell covering column zero",
        )


def validate_table_grid(value: Any) -> TableGridV1:
    """Validate a mapping or return an already-validated grid unchanged."""

    if isinstance(value, TableGridV1):
        return value
    return TableGridV1.from_mapping(value)


def latex_table_locator(start: int, end: int) -> str:
    start = _exact_int(start, field="start", minimum=0)
    end = _exact_int(end, field="end", minimum=0)
    if end <= start:
        _fail("invalid_source_span", "LaTeX table end must be greater than start")
    return f"tex:{start}:{end}"


def _decimal_coordinate(value: Any, *, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float, str)):
        _fail("invalid_pdf_coordinate", f"{field} must be a finite number")
    if isinstance(value, float) and not math.isfinite(value):
        _fail("invalid_pdf_coordinate", f"{field} must be a finite number")
    try:
        parsed = Decimal(str(value))
        if not parsed.is_finite() or parsed < 0:
            _fail("invalid_pdf_coordinate", f"{field} must be finite and nonnegative")
        quantized = parsed.quantize(_PDF_QUANTUM, rounding=ROUND_HALF_EVEN)
    except (InvalidOperation, ValueError) as exc:
        raise TableGridError(
            "invalid_pdf_coordinate", f"{field} must be a finite number"
        ) from exc
    return Decimal(0) if quantized == 0 else quantized


def canonical_pdf_bbox(
    bbox: Sequence[Any],
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    values = _sequence(bbox, field="bbox")
    if len(values) != 4:
        _fail("invalid_pdf_bbox", "bbox must contain exactly four coordinates")
    x0, y0, x1, y1 = (
        _decimal_coordinate(value, field=f"bbox[{index}]")
        for index, value in enumerate(values)
    )
    if x1 <= x0 or y1 <= y0:
        _fail(
            "invalid_pdf_bbox",
            "bbox must retain positive width and height after quantization",
        )
    return x0, y0, x1, y1


def pdf_table_locator(page_index: int, bbox: Sequence[Any]) -> str:
    page_index = _exact_int(page_index, field="page_index", minimum=0)
    canonical = canonical_pdf_bbox(bbox)
    coordinates = ",".join(format(value, ".3f") for value in canonical)
    return f"pdf:{page_index}:{coordinates}"


def _validate_source_locator(source_locator: str) -> None:
    if not isinstance(source_locator, str) or not source_locator:
        _fail("invalid_source_locator", "source_locator must be a non-empty string")
    if _TEX_LOCATOR_RE.fullmatch(source_locator):
        _, start, end = source_locator.split(":")
        if int(end) <= int(start):
            _fail("invalid_source_locator", "LaTeX source locator span is empty")
        return
    if _PDF_LOCATOR_RE.fullmatch(source_locator):
        _, _, coordinates = source_locator.split(":")
        canonical_pdf_bbox(coordinates.split(","))
        return
    _fail("invalid_source_locator", "source_locator is not canonical")


def table_occurrence_id(doc_id: str, source_locator: str) -> str:
    if not isinstance(doc_id, str) or not doc_id:
        _fail("invalid_doc_id", "doc_id must be a non-empty string")
    _validate_source_locator(source_locator)
    digest = hashlib.sha256(f"{doc_id}\0{source_locator}".encode("utf-8")).hexdigest()[
        :16
    ]
    return f"chunk:table:{digest}"


@dataclass(frozen=True, slots=True)
class TableExtractionResult:
    """Occurrence-scoped table extraction outcome with no silent fallback."""

    lane: str
    status: str
    source_locator: str
    source_content_sha256: str
    grid: TableGridV1 | None = None
    error_code: str | None = None
    cache_signature: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.lane, str)
            or _LOWER_SNAKE_RE.fullmatch(self.lane) is None
        ):
            _fail("invalid_extraction_lane", "lane must be lower snake_case")
        if self.status not in TABLE_GRID_STATUSES:
            _fail("invalid_table_grid_status", "status is not a TableGridV1 status")
        _validate_source_locator(self.source_locator)
        if (
            not isinstance(self.source_content_sha256, str)
            or _SHA256_RE.fullmatch(self.source_content_sha256) is None
        ):
            _fail(
                "invalid_source_content_hash",
                "source_content_sha256 must be a lowercase SHA-256 hex digest",
            )
        if self.cache_signature is not None and (
            not isinstance(self.cache_signature, str) or not self.cache_signature
        ):
            _fail("invalid_cache_signature", "cache_signature must be non-empty")
        if self.status == "parsed":
            if not isinstance(self.grid, TableGridV1) or self.error_code is not None:
                _fail(
                    "invalid_parsed_result",
                    "parsed results require a grid and must not carry an error code",
                )
        elif (
            self.grid is not None
            or not isinstance(self.error_code, str)
            or not self.error_code
        ):
            _fail(
                "invalid_failed_result",
                "failed results require an error code and must not carry a grid",
            )

    def to_chunk_extras(self) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            "table_extraction_lane": self.lane,
            "table_grid_status": self.status,
            "table_grid_error_code": self.error_code,
            "source_locator": self.source_locator,
            "source_content_sha256": self.source_content_sha256,
        }
        if self.cache_signature is not None:
            attrs["cache_signature"] = self.cache_signature
        if self.grid is not None:
            attrs["table_grid"] = self.grid.to_mapping()
        return attrs


__all__ = [
    "TABLE_GRID_SCHEMA_VERSION",
    "TABLE_GRID_STATUSES",
    "TableCellV1",
    "TableExtractionResult",
    "TableGridError",
    "TableGridV1",
    "TableRowV1",
    "canonical_pdf_bbox",
    "latex_table_locator",
    "normalize_table_text",
    "pdf_table_locator",
    "table_occurrence_id",
    "validate_table_grid",
]

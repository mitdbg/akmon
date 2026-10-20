"""CSV / TSV parser. Wraps stdlib `csv` only — no third-party.

XLSX lives in the companion module `parse/xlsx.py` (openpyxl).

Format keys: `"csv"`, `"tsv"`.
"""

from __future__ import annotations

import csv as _csv
from io import StringIO
from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class CsvParser:
    """CSV/TSV -> ParsedDocument via stdlib `csv`."""

    def __init__(self, delimiter: str = ",") -> None:
        self.delimiter = delimiter

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")

        reader = _csv.reader(StringIO(content), delimiter=self.delimiter)
        rows = list(reader)
        if not rows:
            return ParsedDocument(
                text="", metadata={"row_count": 0, "col_count": 0}
            )

        col_count = max(len(r) for r in rows)
        text = "\n".join("\t".join(r) for r in rows)
        return ParsedDocument(
            text=text,
            metadata={
                "row_count": len(rows),
                "col_count": col_count,
                "header": rows[0] if rows else [],
            },
        )


class TsvParser(CsvParser):
    """TSV variant of CsvParser."""

    def __init__(self) -> None:
        super().__init__(delimiter="\t")


register_parser("csv", CsvParser)
register_parser("tsv", TsvParser)


__all__ = ["CsvParser", "TsvParser"]

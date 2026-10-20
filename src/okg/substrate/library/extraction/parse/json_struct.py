"""JSON parser. Wraps stdlib `json` only.

Returns a `ParsedDocument` carrying both:

  - `text`: canonical re-serialization with sort_keys=True so
    content_sha256 is stable across formatting variations.
  - `metadata.structured_data`: the parsed Python object.

Format key: `json`. Malformed input raises `ParseFailed` (no
silent empty result).

Per `add-doc-corpus-extraction-adapter` (2026-04-25).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.parse.dispatch import register_parser


class ParseFailed(SubstrateError):
    code = "parse_failed"


class JsonParser:
    def parse(
        self, content: bytes | str, *, path: str | None = None, **_: Any,
    ) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            byte_count = len(content)
            sha = hashlib.sha256(bytes(content)).hexdigest()
            try:
                parsed = json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as e:
                raise ParseFailed(
                    f"json parse failed: {e}",
                    data={"format": "json", "path": path},
                    fix_hint="check the file is UTF-8 + valid JSON",
                ) from e
        else:
            byte_count = len(content.encode("utf-8"))
            sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
            try:
                parsed = json.loads(content)
            except ValueError as e:
                raise ParseFailed(
                    f"json parse failed: {e}",
                    data={"format": "json", "path": path},
                    fix_hint="check the input is valid JSON",
                ) from e
        canonical = json.dumps(
            parsed, sort_keys=True, ensure_ascii=False, indent=2,
        )
        return ParsedDocument(
            text=canonical,
            metadata={
                "format": "json",
                "byte_count": byte_count,
                "content_sha256": sha,
                "structured_data": parsed,
            },
            chunks=[],
        )


register_parser("json", JsonParser)


__all__ = ["JsonParser", "ParseFailed"]

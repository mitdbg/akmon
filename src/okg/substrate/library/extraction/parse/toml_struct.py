"""TOML parser. Wraps stdlib `tomllib` (Python 3.11+) only.

Returns a `ParsedDocument` carrying:

  - `text`: a stable canonical text form. tomllib doesn't ship
    a writer, so we emit a sorted JSON re-serialization instead
    of round-tripping TOML. content_sha256 is computed over the
    INPUT bytes (not the re-serialization) so it stays
    bit-stable regardless of writer choice.
  - `metadata.structured_data`: the parsed Python object.

Format key: `toml`. Malformed input raises `ParseFailed`.

Per `add-doc-corpus-extraction-adapter` (2026-04-25).
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from typing import Any

from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.parse.dispatch import register_parser
from okg.substrate.library.extraction.parse.json_struct import ParseFailed


class TomlParser:
    def parse(
        self, content: bytes | str, *, path: str | None = None, **_: Any,
    ) -> ParsedDocument:
        if isinstance(content, str):
            encoded = content.encode("utf-8")
        elif isinstance(content, (bytes, bytearray)):
            encoded = bytes(content)
        else:
            raise TypeError(
                f"TomlParser.parse expected str/bytes; got "
                f"{type(content).__name__}"
            )
        byte_count = len(encoded)
        sha = hashlib.sha256(encoded).hexdigest()
        try:
            parsed = tomllib.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as e:
            raise ParseFailed(
                f"toml parse failed: {e}",
                data={"format": "toml", "path": path},
                fix_hint="check the input is UTF-8 + valid TOML",
            ) from e
        canonical = json.dumps(
            parsed, sort_keys=True, ensure_ascii=False,
            indent=2, default=str,
        )
        return ParsedDocument(
            text=canonical,
            metadata={
                "format": "toml",
                "byte_count": byte_count,
                "content_sha256": sha,
                "structured_data": parsed,
            },
            chunks=[],
        )


register_parser("toml", TomlParser)


__all__ = ["TomlParser"]

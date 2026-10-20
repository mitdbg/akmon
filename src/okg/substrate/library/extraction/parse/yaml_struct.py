"""YAML parser. Wraps `pyyaml` only.

Returns a `ParsedDocument` carrying:

  - `text`: canonical YAML re-emission via `yaml.dump` with a SafeDumper
    (sorted keys, default_flow_style=False) — stable for
    content_sha256 of structurally-identical YAML. It is written
    for a reader: characters as written (no unicode escapes), no
    line folding, and a multi-line string as a literal `|` block,
    so stored and searched text holds the prose the author wrote,
    not escape and fold-continuation markers (#2179).
  - `metadata.structured_data`: the parsed Python object
    (always via `safe_load` — no arbitrary tag execution).
  - `metadata.original_text`: the file as written, with its
    comments and key order. The re-emission above drops both, and a
    config comment often says why a value is what it is, so the
    doc-corpus source also chunks this text (#2671).

Format keys: `yaml`, `yml`. Malformed input raises
`ParseFailed`.

Per `add-doc-corpus-extraction-adapter` (2026-04-25).
"""

from __future__ import annotations
from okg.substrate import fastyaml

import hashlib
from typing import Any

import yaml as _yaml

from okg.substrate.library.extraction._protocols import ParsedDocument
from okg.substrate.library.extraction.parse.dispatch import register_parser
from okg.substrate.library.extraction.parse.json_struct import ParseFailed


class _ReadableDumper(_yaml.SafeDumper):
    """SafeDumper that writes a multi-line string as a literal block."""


def _represent_str(dumper: _yaml.SafeDumper, value: str) -> _yaml.Node:
    # The emitter falls back to a quoted scalar on its own when a literal
    # block cannot hold the value (trailing spaces, tabs, control
    # characters). NEL (U+0085) is the exception: written raw, the loader
    # reads it as a line break, so a string holding one stays double-quoted,
    # where it is escaped.
    if "\x85" in value:
        style = '"'
    elif "\n" in value:
        style = "|"
    else:
        style = None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_ReadableDumper.add_representer(str, _represent_str)


def _readable_dump(data: Any) -> str:
    """Sorted, block-style YAML with characters and lines as written."""
    return _yaml.dump(
        data,
        Dumper=_ReadableDumper,
        default_flow_style=False,
        sort_keys=True,
        allow_unicode=True,
        width=float("inf"),
    )


class YamlParser:
    def parse(
        self, content: bytes | str, *, path: str | None = None, **_: Any,
    ) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            byte_count = len(content)
            sha = hashlib.sha256(bytes(content)).hexdigest()
            text_in = content.decode("utf-8", errors="replace")
        else:
            text_in = content
            encoded = content.encode("utf-8")
            byte_count = len(encoded)
            sha = hashlib.sha256(encoded).hexdigest()
        try:
            parsed = fastyaml.safe_load(text_in)
        except _yaml.YAMLError as e:
            raise ParseFailed(
                f"yaml parse failed: {e}",
                data={"format": "yaml", "path": path},
                fix_hint=(
                    "check the input is valid YAML loadable via "
                    "yaml.safe_load (no custom tags)"
                ),
            ) from e
        canonical = _readable_dump(parsed)
        return ParsedDocument(
            text=canonical,
            metadata={
                "format": "yaml",
                "byte_count": byte_count,
                "content_sha256": sha,
                "structured_data": parsed,
                # Only when the file says something: document markers and
                # blank lines alone would become a chunk shared by every
                # empty YAML file.
                **(
                    {"original_text": text_in}
                    if any(
                        line.strip() not in ("", "---", "...")
                        for line in text_in.splitlines()
                    )
                    else {}
                ),
            },
            chunks=[],
        )


register_parser("yaml", YamlParser)
register_parser("yml", YamlParser)


__all__ = ["YamlParser"]

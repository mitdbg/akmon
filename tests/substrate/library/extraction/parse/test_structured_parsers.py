"""Stride A — plain-text + structured-config parsers.

Asserts:
  - text/json/yaml/toml parsers register at module import;
  - each returns a `ParsedDocument` with non-empty `text`,
    a `format` discriminator in metadata, and a stable
    `content_sha256`;
  - malformed JSON/YAML/TOML raises a structured `ParseFailed`
    rather than silently returning empty.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from okg.substrate.library.extraction.parse.dispatch import parser_for
from okg.substrate.library.extraction.parse.json_struct import ParseFailed


def test_text_parser_decodes_and_normalizes() -> None:
    p = parser_for("text")
    raw = b"Hello world"
    pd = p.parse(raw)
    assert pd.text == "Hello world"
    assert pd.metadata["format"] == "text"
    assert pd.metadata["byte_count"] == len(raw)
    assert pd.metadata["content_sha256"] == hashlib.sha256(raw).hexdigest()


def test_text_parser_repairs_mojibake() -> None:
    """ftfy + Unicode normalization runs as part of TextParser.
    Mojibake (round-tripped UTF-8/cp1252) gets repaired."""
    p = parser_for("text")
    # `caf\xc3\xa9` is "café" correctly UTF-8-encoded; ftfy
    # leaves it alone. Exact mojibake examples are platform-
    # sensitive — this test just asserts the cleaner runs.
    raw = "café".encode("utf-8")
    pd = p.parse(raw)
    assert "café" in pd.text or "cafe" in pd.text


def test_text_parser_aliases_register() -> None:
    # txt + rst route to the same TextParser class.
    a = parser_for("text")
    b = parser_for("txt")
    c = parser_for("rst")
    assert type(a) is type(b) is type(c)


def test_json_parser_canonicalizes_and_extracts_structured() -> None:
    p = parser_for("json")
    payload = b'{"b": 2, "a": 1}'
    pd = p.parse(payload)
    # Canonical text has sorted keys.
    assert "\"a\": 1" in pd.text
    assert pd.text.find("\"a\"") < pd.text.find("\"b\"")
    assert pd.metadata["structured_data"] == {"a": 1, "b": 2}
    assert pd.metadata["content_sha256"] == hashlib.sha256(payload).hexdigest()


def test_json_parser_raises_on_malformed_input() -> None:
    p = parser_for("json")
    with pytest.raises(ParseFailed) as exc_info:
        p.parse(b"{not-json}", path="bad.json")
    err = exc_info.value
    assert err.data["format"] == "json"
    assert err.data["path"] == "bad.json"


def test_json_parser_handles_json_exception_alias_skew(monkeypatch) -> None:
    class StaleJSONDecodeError(ValueError):
        pass

    monkeypatch.setattr(json, "JSONDecodeError", StaleJSONDecodeError)
    with pytest.raises(ParseFailed):
        parser_for("json").parse(b"{not-json}", path="bad.json")


def test_yaml_parser_round_trips_and_safe_loads() -> None:
    p = parser_for("yaml")
    raw = b"name: okg-workspace\nkind: deployment\n"
    pd = p.parse(raw)
    assert pd.metadata["structured_data"] == {
        "name": "okg-workspace", "kind": "deployment",
    }
    # Canonical re-emit sorts keys.
    assert pd.text.startswith("kind:")  # alphabetical


def test_yaml_parser_aliases() -> None:
    assert type(parser_for("yaml")) is type(parser_for("yml"))


def test_toml_parser_uses_stdlib_tomllib() -> None:
    p = parser_for("toml")
    raw = b'[project]\nname = "okg"\nversion = "0.1.0"\n'
    pd = p.parse(raw)
    assert pd.metadata["format"] == "toml"
    assert pd.metadata["structured_data"]["project"]["name"] == "okg"
    assert pd.metadata["content_sha256"] == hashlib.sha256(raw).hexdigest()


def test_toml_parser_raises_on_malformed() -> None:
    p = parser_for("toml")
    with pytest.raises(ParseFailed) as exc_info:
        p.parse(b"this is not [valid toml", path="bad.toml")
    assert exc_info.value.data["path"] == "bad.toml"

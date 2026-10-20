"""The source manifest reference lists exactly the keys its JSON Schema has.

The page is found through documentation.yaml and its canonical_id, never by
a hard-coded page path. Each table row whose first cell is a backticked key
path counts as a listed key.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
CANONICAL_ID = "source-manifest-reference"
SCHEMA = ROOT / "schemas" / "source_manifest.schema.json"

#: The placeholder the page uses for a name the author chooses, by the key
#: whose values are named.
PLACEHOLDERS = {"types": "<type>", "fields": "<field>", "links": "<link>", "connections": "<connection>"}
#: Shared definitions the page documents once, under one canonical key; every
#: other use lists only its own key and says it takes the same keys.
CANONICAL_PREFIX = {"read": "read", "auth": "read.auth"}
ROW_KEY = re.compile(r"^\|\s*`([^`]+)`\s*\|", re.MULTILINE)


def _page_text() -> str:
    inventory = yaml.safe_load((ROOT / "documentation.yaml").read_text(encoding="utf-8"))
    for publication in inventory["publications"].values():
        source = ROOT / publication["source_dir"]
        for relative in publication["pages"]:
            text = (source / relative).read_text(encoding="utf-8")
            front = yaml.safe_load(text.split("---\n", 2)[1]) if text.startswith("---\n") else {}
            if isinstance(front, dict) and front.get("canonical_id") == CANONICAL_ID:
                return text
    raise AssertionError(f"no published page has canonical_id {CANONICAL_ID!r}")


def _resolve(schema: dict, node: Any) -> Any:
    while isinstance(node, dict) and "$ref" in node:
        node = schema["$defs"][node["$ref"].rsplit("/", 1)[1]]
    return node


def _schema_keys(schema: dict, node: Any, prefix: str = "") -> set[str]:
    if isinstance(node, dict) and "$ref" in node:
        shared = node["$ref"].rsplit("/", 1)[1]
        if shared in CANONICAL_PREFIX and prefix != CANONICAL_PREFIX[shared]:
            return set()  # documented once, under its canonical key
    node = _resolve(schema, node)
    keys: set[str] = set()
    if not isinstance(node, dict):
        return keys
    for variant in node.get("oneOf") or ():
        keys |= _schema_keys(schema, variant, prefix)
    for name, child in (node.get("properties") or {}).items():
        path = f"{prefix}.{name}" if prefix else name
        keys.add(path)
        keys |= _schema_keys(schema, child, path)
    extra = node.get("additionalProperties")
    if isinstance(extra, dict):
        last = prefix.rsplit(".", 1)[-1]
        path = f"{prefix}.{PLACEHOLDERS[last]}"
        keys.add(path)
        keys |= _schema_keys(schema, extra, path)
    return keys


def _page_keys(text: str) -> set[str]:
    return set(ROW_KEY.findall(text)) - {"Key"}


def test_reference_lists_exactly_the_schema_keys() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    expected = _schema_keys(schema, schema)
    listed = _page_keys(_page_text())
    assert listed == expected, {
        "missing_from_page": sorted(expected - listed),
        "not_in_schema": sorted(listed - expected),
    }


def test_the_key_walk_sees_a_nested_key() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    keys = _schema_keys(schema, schema)
    assert "types.<type>.links.<link>.each" in keys
    assert "types.<type>.fields.<field>.type" in keys
    assert _page_keys("| `a` | x |\n| Key | y |\n") == {"a"}

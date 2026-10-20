"""M3.4 — linker_config template field grammar."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog import (
    ALLOWED_TEMPLATE_FIELDS,
    load_linker_config,
    validate_dst_template,
)
from okg.substrate.errors import SubstrateError


def _write(tmp_path: Path, doc: dict) -> Path:
    p = tmp_path / "linker_config.yaml"
    p.write_text(yaml.safe_dump(doc))
    return p


# ---------- pure validator ----------

def test_allowed_fields_set() -> None:
    assert ALLOWED_TEMPLATE_FIELDS == {"resolved", "value", "type", "src"}


@pytest.mark.parametrize("template", [
    "{resolved}",
    "{value}",
    "{type}",
    "{src}",
    "prefix-{resolved}",
    "{src}|{type}|{value}",
    "static_only",        # no placeholders is fine
    "{resolved}{value}",
])
def test_validate_accepts_allowed_templates(template: str) -> None:
    validate_dst_template(template)


@pytest.mark.parametrize("template", [
    "{parsed_slug}",
    "{Resolved}",          # case-sensitive
    "{ resolved }",        # whitespace inside braces
    "{resolved}{unknown}",
    "{resolved",           # unclosed brace
    "resolved}",           # stray close
    "literal{",
    "}stuff",
])
def test_validate_rejects_disallowed_templates(template: str) -> None:
    with pytest.raises(SubstrateError):
        validate_dst_template(template)


def test_validate_error_names_offending_field() -> None:
    with pytest.raises(SubstrateError) as exc:
        validate_dst_template("{parsed_slug}", where="routes.x.dst_template")
    msg = str(exc.value)
    assert "{parsed_slug}" in msg
    assert "routes.x.dst_template" in msg


# ---------- file loader ----------

def test_load_simple_route(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "routes": {
            "service": {"edge_type": "mentions", "dst_template": "{resolved}"},
        }
    })
    routes = load_linker_config(p)
    assert len(routes) == 1
    r = routes[0]
    assert r.entity_type == "service"
    assert r.src_subtype is None
    assert r.edge_type == "mentions"
    assert r.dst_template == "{resolved}"


def test_load_by_src_subtype(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "routes": {
            "file": {
                "ticket": {"edge_type": "mentions", "dst_template": "{resolved}"},
                "_default": {"edge_type": "references", "dst_template": "{value}"},
            }
        }
    })
    routes = load_linker_config(p)
    assert len(routes) == 2
    by_subtype = {r.src_subtype: r for r in routes}
    assert by_subtype["ticket"].edge_type == "mentions"
    assert by_subtype["_default"].edge_type == "references"


def test_load_includes_validated(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "includes": ["okg.linker_routes.service"],
        "routes": {},
    })
    load_linker_config(p)


def test_load_bad_include_pattern(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "includes": ["my.custom.thing"],
        "routes": {},
    })
    with pytest.raises(SubstrateError, match="okg.linker_routes"):
        load_linker_config(p)


def test_load_rejects_parsed_slug_template(tmp_path: Path) -> None:
    """The tasks.md 3.4 example: a linker_config.yaml with
    `{parsed_slug}` fails at catalog load with a readable error."""
    p = _write(tmp_path, {
        "routes": {
            "service": {
                "edge_type": "mentions",
                "dst_template": "{parsed_slug}",
            },
        }
    })
    with pytest.raises(SubstrateError) as exc:
        load_linker_config(p)
    msg = str(exc.value)
    assert "{parsed_slug}" in msg
    assert "routes.service.dst_template" in msg


def test_load_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "routes": {},
        "novel_top_key": {},
    })
    with pytest.raises(SubstrateError, match="unknown top-level keys"):
        load_linker_config(p)


def test_load_rejects_unknown_route_key(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "routes": {
            "service": {
                "edge_type": "mentions",
                "dst_template": "{resolved}",
                "made_up": True,
            }
        }
    })
    with pytest.raises(SubstrateError, match="unknown keys"):
        load_linker_config(p)


def test_load_rejects_bad_subtype_key(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "routes": {
            "file": {
                "bad-subtype": {
                    "edge_type": "mentions",
                    "dst_template": "{resolved}",
                }
            }
        }
    })
    with pytest.raises(SubstrateError, match="snake_case subtype"):
        load_linker_config(p)


def test_load_rejects_confidence_out_of_range(tmp_path: Path) -> None:
    p = _write(tmp_path, {
        "routes": {
            "service": {
                "edge_type": "mentions",
                "dst_template": "{resolved}",
                "confidence": 2.5,
            }
        }
    })
    with pytest.raises(SubstrateError, match="confidence"):
        load_linker_config(p)


def test_existing_example_loads(tmp_path: Path) -> None:
    """The shipped example linker_config.yaml validates."""
    repo_root = Path(__file__).resolve().parents[2]
    example = repo_root / "schemas/examples/linker_config.yaml"
    if not example.exists():
        pytest.skip(f"{example} not present")
    routes = load_linker_config(example)
    # The example has 4 routes total: service, repo, file/Ticket,
    # file/_default.
    assert len(routes) == 4
    by_key = {(r.entity_type, r.src_subtype): r for r in routes}
    assert ("service", None) in by_key
    assert ("repo", None) in by_key
    assert ("file", "Ticket") in by_key
    assert ("file", "_default") in by_key

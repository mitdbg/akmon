"""Keep current reference pages aligned with live public surfaces.

These checks intentionally compare names and registration metadata, not
sentences.  The reference pages can be rewritten for clarity without
breaking this contract; adding or removing a registered surface cannot pass
without an accompanying documentation change.
"""

from __future__ import annotations

import importlib
import inspect
import re
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = ROOT / "src"


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _mcp_documented(text: str) -> set[str]:
    """Read only the contiguous current-tool table.

    The reference uses the public call signature in the first cell (for
    example ``inspect(target)``), while the registry exposes the bare tool
    name.  Parse the name before the optional signature so the contract
    compares the same identifier on both sides.
    """
    table = text.split("| Tool |", 1)[1].split("\n\n", 1)[0]
    return set(
        re.findall(r"^\| `([a-z][a-z_]*)(?:\([^`]*\))?` \|", table, re.MULTILINE)
    )


def _profile_table(text: str) -> str:
    return text.split("## Built-In Profiles", 1)[1].split(
        "## Composable Ontology Model", 1
    )[0]


def _profile_documented(text: str) -> set[str]:
    return set(re.findall(r"^\| `([^`]+)` \|", _profile_table(text), re.MULTILINE))


def _nested_module_and_class(value: Any) -> tuple[str, str]:
    if isinstance(value, dict):
        if "module" in value or "class" in value:
            return str(value.get("module") or "—"), str(value.get("class") or "—")
        for child in value.values():
            found = _nested_module_and_class(child)
            if found != ("—", "—"):
                return found
    if isinstance(value, list):
        for child in value:
            found = _nested_module_and_class(child)
            if found != ("—", "—"):
                return found
    return "—", "—"


def _source_defaults() -> dict[str, tuple[frozenset[str], str, str]]:
    result: dict[str, tuple[frozenset[str], str, str]] = {}
    profiles: dict[str, set[str]] = {}
    implementations: dict[str, tuple[str, str]] = {}
    for path in sorted(
        (SOURCE_ROOT / "okg/substrate/library/profiles").glob(
            "*/source-defaults/*.yaml"
        )
    ):
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        assert isinstance(raw, dict) and len(raw) == 1, path
        source_id, config = next(iter(raw.items()))
        module_and_class = _nested_module_and_class(config)
        profiles.setdefault(str(source_id), set()).add(path.parent.parent.name)
        previous = implementations.setdefault(str(source_id), module_and_class)
        assert previous == module_and_class, (
            f"source {source_id!r} has inconsistent module/class metadata: "
            f"{previous!r} versus {module_and_class!r}"
        )
    for source_id in sorted(profiles):
        module, klass = implementations[source_id]
        result[source_id] = (frozenset(profiles[source_id]), module, klass)
    return result


def _ontology_manifests() -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    for path in sorted(
        (SOURCE_ROOT / "okg/substrate/library/ontologies").glob("*/module.yaml")
    ):
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        name = str(raw.get("name") or path.parent.name)
        result[name] = (str(raw.get("version") or "—"), str(path.relative_to(ROOT)))
    return result


def _ontology_documented(text: str) -> dict[str, tuple[str, str]]:
    section = text.split("### Ontology module manifest inventory", 1)[1].split(
        "## Profile-To-Ontology Examples", 1
    )[0]
    pattern = re.compile(
        r"^\| `([^`]+)` \| `([^`]+)` \| `([^`]+)` \|$", re.MULTILINE
    )
    return {name: (version, manifest) for name, version, manifest in pattern.findall(section)}


def _sdk_symbols() -> set[str]:
    sys.path.insert(0, str(SOURCE_ROOT))
    from okg import deployment

    return set(deployment.__all__)


def _sdk_missing(text: str, symbols: set[str]) -> set[str]:
    missing: set[str] = set()
    for symbol in symbols:
        token = re.escape(symbol)
        if not re.search(rf"(?:`okg\.deployment\.)?{token}(?:[`(,\s])", text):
            missing.add(symbol)
    return missing


def _error_registry() -> set[str]:
    raw = yaml.safe_load(_text("docs/reference/error_codes.yaml")) or {}
    return {str(row["code"]) for row in raw.get("codes", [])}


def _live_error_codes() -> set[str]:
    from okg.substrate.errors import SubstrateError

    return {
        str(value.code)
        for value in vars(sys.modules[SubstrateError.__module__]).values()
        if inspect.isclass(value)
        and value is not SubstrateError
        and issubclass(value, SubstrateError)
        and getattr(value, "code", None)
        and value.code != "substrate_error"
    }


def test_mcp_reference_matches_registered_read_and_escape_tools() -> None:
    from okg.substrate.mcp.operators import AKMON_READ_TOOLS, QUERY_ESCAPE_TOOLS

    expected = set(AKMON_READ_TOOLS) | set(QUERY_ESCAPE_TOOLS)
    documented = _mcp_documented(_text("docs/reference/mcp.md"))
    assert documented == expected


def test_mcp_reference_detects_a_removed_operator() -> None:
    text = _text("docs/reference/mcp.md")
    expected = _mcp_documented(text)
    removed = next(iter(expected))
    mutated = re.sub(
        rf"^\| `{re.escape(removed)}(?:\([^`]*\))?` \|[^\n]*\n?",
        "",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    assert expected - _mcp_documented(mutated) == {removed}


def test_profile_reference_matches_built_in_profile_manifests() -> None:
    manifest_names = {
        str((yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("name"))
        for path in sorted(
            (SOURCE_ROOT / "okg/substrate/library/profiles").glob("*/profile.yaml")
        )
        if path.parent.name != "_test"
    }
    text = _text("docs/develop/extensions/ontologies.md")
    documented = _profile_documented(text)
    assert documented == manifest_names
    assert "`_test` profile" in text and "internal loader fixture" in text


def test_profile_reference_detects_a_removed_profile() -> None:
    text = _text("docs/develop/extensions/ontologies.md")
    expected = _profile_documented(text)
    removed = next(iter(expected))
    mutated = text.replace(f"| `{removed}` |", "", 1)
    assert expected - _profile_documented(mutated) == {removed}


def test_runnable_source_defaults_name_importable_implementations() -> None:
    for source_id, (_profiles, module, klass) in _source_defaults().items():
        if module == "—" or klass == "—" or "${" in module:
            continue
        loaded = importlib.import_module(module)
        assert hasattr(loaded, klass), (
            f"source {source_id!r} names missing implementation {module}.{klass}"
        )


def test_ontology_reference_matches_module_manifests() -> None:
    expected = _ontology_manifests()
    documented = _ontology_documented(_text("docs/develop/extensions/ontologies.md"))
    assert documented == expected


def test_ontology_reference_detects_a_removed_module() -> None:
    text = _text("docs/develop/extensions/ontologies.md")
    expected = _ontology_manifests()
    module = next(iter(expected))
    mutated = text.replace(f"| `{module}` |", "", 1)
    assert set(expected) - set(_ontology_documented(mutated)) == {module}


def test_sdk_reference_covers_every_public_export() -> None:
    symbols = _sdk_symbols()
    missing = _sdk_missing(_text("docs/reference/sdk-api.md"), symbols)
    assert missing == set()


def test_sdk_reference_detects_a_removed_export() -> None:
    text = _text("docs/reference/sdk-api.md")
    symbols = _sdk_symbols()
    removed = next(iter(symbols))
    mutated = text.replace(removed, "")
    assert removed in _sdk_missing(mutated, {removed})


def test_error_registry_covers_live_substrate_error_exports() -> None:
    assert _live_error_codes() <= _error_registry()


def test_error_reference_detects_a_removed_live_code() -> None:
    live = _live_error_codes()
    registered = _error_registry()
    removed = next(iter(live))
    mutated_registry = registered - {removed}
    assert live - mutated_registry == {removed}

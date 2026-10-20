"""The public surface an external deployment depends on, read from this okg.

A deployment kept outside this repository breaks when okg renames a verb,
drops a flag, closes a `deployment.yaml` key or retires a source class. Until
now each deployment kept its own hand-written list of what it relied on
(archi-crab-okg's `scripts/contract-check.py` hard-codes twelve commands).

:func:`build_contract` reads that surface from the installed package, offline:
no database, no network, nothing written. It is computed live rather than
committed, so a pull request that changes the surface does not also have to
regenerate a shared file (weekly-release design, decision 307 D2). The release
cut stores each tag's contract as a release asset; :func:`diff_contracts`
compares two of them for the release notes and for `okg deployment compat
--against`.

Each section maps an item id to its attributes. An item that disappears, or
whose attributes tighten (a flag becomes required, a choice is dropped), is a
breaking change; a new item is not.

Not covered yet, because okg has no single authoritative source for them:
environment variables, MCP tool parameter schemas (FastMCP derives them at
server build), error codes (the catalogue is not shipped in the wheel), flags
a profile injects into `init`/`install`, and the `okg pact` workflow verbs,
which are this repository's own tooling rather than a deployment surface.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from typing import Any

CONTRACT_SCHEMA = "okg.release_contract.v1"

#: Sections in the order a reader meets them.
SECTIONS: tuple[str, ...] = (
    "cli",
    "exit_codes",
    "deployment_keys",
    "source_registry_keys",
    "reserved_source_params",
    "source_classes",
    "ontology_modules",
    "mcp_tools",
)

#: Verbs left out of the CLI section; see the module docstring.
_EXCLUDED_VERBS = frozenset({"pact"})


def build_contract() -> dict[str, Any]:
    """The public surface of the running okg, as plain JSON data."""
    import okg

    return {
        "schema": CONTRACT_SCHEMA,
        "okg_version": okg.__version__,
        "sections": {
            "cli": _cli_section(),
            "exit_codes": _exit_code_section(),
            "deployment_keys": _deployment_key_section(),
            "source_registry_keys": _source_registry_key_section(),
            "reserved_source_params": _reserved_param_section(),
            "source_classes": _source_class_section(),
            "ontology_modules": _ontology_module_section(),
            "mcp_tools": _mcp_tool_section(),
        },
    }


def render_contract(contract: Mapping[str, Any]) -> str:
    """Stable bytes for one contract: sorted keys, one trailing newline."""
    return json.dumps(contract, indent=1, sort_keys=True) + "\n"


# ------------------------------------------------------- release history

#: Package directory a release wheel bundles earlier releases' contracts in.
BUNDLED_DIR = "_release_contracts"


def contract_asset_name(tag: str) -> str:
    """The GitHub release asset holding the contract of ``tag``."""
    return f"okg-contract-{tag}.json"


def bundled_contract_versions() -> list[str]:
    """Versions whose contract this installed okg carries, oldest first.

    A release wheel carries every earlier release's contract (decision 312);
    the first release, v0.2.0, carries none, and a wheel built outside the
    release cut carries whatever its build found.
    """
    root = _bundled_root()
    if root is None:
        return []
    versions = []
    for entry in root.iterdir():
        name = entry.name
        if name.startswith("okg-contract-v") and name.endswith(".json.gz"):
            versions.append(name[len("okg-contract-v"):-len(".json.gz")])
    return sorted(versions, key=_version_key)


def bundled_contract(version: str) -> dict[str, Any] | None:
    """The bundled contract of release ``version`` (no leading v), or None."""
    import gzip

    root = _bundled_root()
    if root is None:
        return None
    entry = root / f"okg-contract-v{version.removeprefix('v')}.json.gz"
    if not entry.is_file():
        return None
    return json.loads(gzip.decompress(entry.read_bytes()).decode("utf-8"))


def _bundled_root() -> Any:
    from importlib import resources

    try:
        root = resources.files("okg") / BUNDLED_DIR
    except (ModuleNotFoundError, TypeError):
        return None
    return root if root.is_dir() else None


def _version_key(version: str) -> tuple[int, ...]:
    parts = []
    for part in version.split("."):
        parts.append(int(part) if part.isdigit() else -1)
    return tuple(parts)


# --------------------------------------------------------------------- cli


def _cli_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.cli.main import _build_parser

    # argv=[] keeps the tree independent of this process's own sys.argv,
    # which `okg pact` reads to decide whether to show deprecated verbs.
    parser = _build_parser(argv=[])
    items: dict[str, dict[str, Any]] = {}
    # Contract ids start with the parser's own program name.
    _walk_parser(parser, (parser.prog,), items, top=True)
    return items


def _walk_parser(
    parser: argparse.ArgumentParser,
    path: tuple[str, ...],
    items: dict[str, dict[str, Any]],
    *,
    top: bool = False,
) -> None:
    command = " ".join(path)
    if not top:
        items[command] = {"kind": "command"}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, child in sorted(action.choices.items()):
                if top and name in _EXCLUDED_VERBS:
                    continue
                if child is parser:
                    continue
                _walk_parser(child, (*path, name), items)
            continue
        if isinstance(action, argparse._HelpAction):
            continue
        if action.option_strings:
            # One item per spelling: a renamed flag that keeps its old name as
            # a deprecated alias shows up as added, and as breaking only when
            # the alias is finally removed.
            for name in action.option_strings:
                items[f"{command} {name}"] = {
                    "kind": "option",
                    "required": bool(action.required),
                    "choices": _choices(action),
                    "value": _value_mode(action),
                    # Hidden from --help: how deprecated aliases are kept.
                    "hidden": action.help == argparse.SUPPRESS,
                }
        else:
            # Keyed by position, not dest: renaming a dest is invisible to a
            # caller, moving or adding a positional is not.
            position = sum(
                1 for key, item in items.items()
                if item.get("kind") == "positional" and key.startswith(f"{command} <")
            )
            items[f"{command} <{position + 1}>"] = {
                "kind": "positional",
                "required": action.nargs not in ("?", "*"),
                "choices": _choices(action),
                "value": _value_mode(action),
            }


def _value_mode(action: argparse.Action) -> str:
    """How many values the argument takes, in the terms a caller sees."""
    nargs = action.nargs
    if nargs == 0:
        return "none"
    if nargs == "?":
        return "optional"
    if nargs == "*":
        return "any"
    if nargs == "+":
        return "one_or_more"
    if nargs is None:
        return "one"
    return str(nargs)


def _choices(action: argparse.Action) -> list[str] | None:
    if action.choices is None or isinstance(action, argparse._SubParsersAction):
        return None
    return sorted(str(choice) for choice in action.choices)


# -------------------------------------------------------------- exit codes


def _exit_code_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.cli import run_summary

    meanings = (
        ("EXIT_OK", "ok"),
        ("EXIT_FAILED", "refused or failed"),
        ("EXIT_BLOCKED", "a publish ran and was blocked; readers keep the previous version"),
        ("EXIT_CAPACITY_WARNING", "published, with a publish-lock capacity warning"),
    )
    # Only the codes this okg defines: an older release lacks the newer ones.
    items = {
        str(getattr(run_summary, name)): {"meaning": meaning}
        for name, meaning in meanings
        if hasattr(run_summary, name)
    }
    items["2"] = {"meaning": "usage or extension error"}
    return items


# ------------------------------------------------------------------ schemas


def _deployment_key_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.deployments.keys import deployment_json_schema

    schema = deployment_json_schema()
    items: dict[str, dict[str, Any]] = {}
    _flatten_schema(schema, schema, "", items, required=False, refs=())
    return items


def _source_registry_key_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.schema_resources import schemas_dir

    schema = json.loads(
        (schemas_dir() / "source_registry.schema.json").read_text(encoding="utf-8"),
    )
    items: dict[str, dict[str, Any]] = {}
    for contract, ref in (
        ("legacy", "#/$defs/legacy_document"),
        ("strict_v1", "#/$defs/strict_document"),
    ):
        _flatten_schema(
            {"$ref": ref}, schema, f"{contract}:", items, required=False, refs=(),
        )
    return items


def _flatten_schema(
    node: Any,
    root: Mapping[str, Any],
    path: str,
    items: dict[str, dict[str, Any]],
    *,
    required: bool,
    refs: tuple[str, ...],
    conditional: bool = False,
) -> None:
    """Record every property path a JSON Schema allows.

    `*` stands for any mapping key, `[]` for a list element. A `$ref` already
    on the path is not followed again, so recursive schemas terminate.
    """
    if not isinstance(node, Mapping):
        return
    ref = node.get("$ref")
    if isinstance(ref, str):
        if ref in refs:
            return
        _flatten_schema(
            _resolve_ref(root, ref), root, path, items,
            required=required, refs=(*refs, ref), conditional=conditional,
        )
    for key in ("allOf", "anyOf", "oneOf"):
        for branch in node.get(key) or ():
            _flatten_schema(
                branch, root, path, items,
                required=required, refs=refs,
                conditional=conditional or key != "allOf",
            )
    for key in ("then", "else"):
        if key in node:
            _flatten_schema(
                node[key], root, path, items,
                required=required, refs=refs, conditional=True,
            )
    # A `required` list inside an if/then branch holds only when the branch
    # applies, so it never marks a key unconditionally required.
    required_names = set() if conditional else set(node.get("required") or ())
    for name, child in sorted((node.get("properties") or {}).items()):
        child_path = f"{path}.{name}" if path and not path.endswith(":") else f"{path}{name}"
        entry = items.setdefault(child_path, {"required": False, "types": []})
        entry["required"] = entry["required"] or name in required_names
        entry["types"] = sorted(set(entry["types"]) | set(_types(child, root)))
        _flatten_schema(child, root, child_path, items, required=False, refs=refs)
    extra = node.get("additionalProperties")
    if isinstance(extra, Mapping) and path:
        _flatten_schema(extra, root, f"{path}.*", items, required=False, refs=refs)
    element = node.get("items")
    if isinstance(element, Mapping) and path:
        _flatten_schema(element, root, f"{path}[]", items, required=False, refs=refs)


def _types(node: Any, root: Mapping[str, Any], depth: int = 0) -> list[str]:
    if not isinstance(node, Mapping) or depth > 8:
        return []
    if isinstance(node.get("$ref"), str):
        node = _resolve_ref(root, node["$ref"])
    declared = node.get("type")
    if isinstance(declared, str):
        return [declared]
    if isinstance(declared, list):
        return [str(t) for t in declared]
    if "const" in node:
        return ["const"]
    if "enum" in node:
        return ["enum"]
    union: set[str] = set()
    for key in ("anyOf", "oneOf"):
        for branch in node.get(key) or ():
            union.update(_types(branch, root, depth + 1))
    if union:
        return sorted(union)
    # No declared type: any value is accepted.
    return ["any"]


def _resolve_ref(root: Mapping[str, Any], ref: str) -> Any:
    if not ref.startswith("#/"):
        raise ValueError(f"release contract cannot follow external $ref {ref!r}")
    node: Any = root
    for part in ref[2:].split("/"):
        node = node[part]
    return node


# ------------------------------------------------------------ source layer


def _reserved_param_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.ingest.source_params import RESERVED_ADAPTER_PARAM_NAMES

    return {name: {} for name in sorted(RESERVED_ADAPTER_PARAM_NAMES)}


def _source_class_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.sources.profiles import PROFILE_NAMES

    return {name: {} for name in sorted(PROFILE_NAMES)}


def _ontology_module_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.catalog.module_manifest import discover_manifests
    from okg.substrate.catalog.modules_compose import load_modules_root
    from okg.substrate import fastyaml

    items: dict[str, dict[str, Any]] = {}
    for name, manifest_path in sorted(discover_manifests(load_modules_root()).items()):
        manifest = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
        items[name] = {"version": str(manifest.get("version"))}
    return items


def _mcp_tool_section() -> dict[str, dict[str, Any]]:
    from okg.substrate.mcp.operators import ALL_BUILTIN_TOOLS

    return {name: {} for name in sorted(ALL_BUILTIN_TOOLS)}


# --------------------------------------------------------------------- diff


def diff_contracts(
    old: Mapping[str, Any], new: Mapping[str, Any],
) -> dict[str, Any]:
    """What changed from `old` to `new`, split into breaking and not.

    Breaking: an item removed; a flag, positional or key that became
    required; a choice that was dropped. Everything else that differs is
    listed under `changed` or `added`.
    """
    for label, contract in (("old", old), ("new", new)):
        if contract.get("schema") != CONTRACT_SCHEMA:
            raise ValueError(
                f"{label} contract has schema {contract.get('schema')!r}, "
                f"expected {CONTRACT_SCHEMA!r}"
            )
    breaking: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    added: list[dict[str, Any]] = []
    old_sections = old.get("sections") or {}
    new_sections = new.get("sections") or {}
    for section in sorted(set(old_sections) | set(new_sections)):
        before = old_sections.get(section) or {}
        after = new_sections.get(section) or {}
        for item in sorted(set(before) - set(after)):
            breaking.append({"section": section, "item": item, "change": "removed"})
        for item in sorted(set(after) - set(before)):
            added.append({"section": section, "item": item})
        for item in sorted(set(before) & set(after)):
            if before[item] == after[item]:
                continue
            reasons = list(_tightenings(before[item], after[item]))
            described = reasons or list(_loosenings(before[item], after[item]))
            row = {
                "section": section,
                "item": item,
                "change": "; ".join(described) if described else "attributes changed",
                "before": before[item],
                "after": after[item],
            }
            (breaking if reasons else changed).append(row)
    return {
        "from_version": old.get("okg_version"),
        "to_version": new.get("okg_version"),
        "breaking": breaking,
        "changed": changed,
        "added": added,
    }


def _tightenings(before: Mapping[str, Any], after: Mapping[str, Any]) -> Iterable[str]:
    if after.get("required") and not before.get("required"):
        yield "now required"
    old_choices = before.get("choices")
    new_choices = after.get("choices")
    if old_choices is not None and new_choices is not None:
        dropped = sorted(set(old_choices) - set(new_choices))
        if dropped:
            yield "choices dropped: " + ", ".join(dropped)
    elif old_choices is None and new_choices is not None:
        yield "now restricted to choices: " + ", ".join(new_choices)
    old_types = set(before.get("types") or ())
    new_types = set(after.get("types") or ())
    if old_types and new_types and "any" not in new_types and not old_types <= new_types:
        yield "types narrowed: " + ", ".join(sorted(old_types - new_types))
    old_value = before.get("value")
    new_value = after.get("value")
    if old_value is not None and new_value is not None and old_value != new_value:
        if not _value_still_accepts(old_value, new_value):
            yield f"takes {new_value} value(s), was {old_value}"


#: For each value mode, the modes that still accept every call it accepted.
_VALUE_WIDENINGS: dict[str, frozenset[str]] = {
    "none": frozenset({"optional", "any"}),
    "one": frozenset({"optional", "one_or_more", "any"}),
    "optional": frozenset({"any"}),
    "one_or_more": frozenset({"any"}),
    "any": frozenset(),
}


def _value_still_accepts(old: str, new: str) -> bool:
    return new in _VALUE_WIDENINGS.get(old, frozenset())


def _loosenings(before: Mapping[str, Any], after: Mapping[str, Any]) -> Iterable[str]:
    if before.get("required") and not after.get("required"):
        yield "no longer required"
    old_choices = before.get("choices")
    new_choices = after.get("choices")
    if old_choices is not None and new_choices is None:
        yield "no longer restricted to choices"
    elif old_choices is not None and new_choices is not None:
        gained = sorted(set(new_choices) - set(old_choices))
        if gained:
            yield "choices added: " + ", ".join(gained)
    if before.get("hidden") != after.get("hidden") and "hidden" in after:
        yield "now hidden from --help" if after.get("hidden") else "now shown in --help"
    old_types = set(before.get("types") or ())
    new_types = set(after.get("types") or ())
    if new_types - old_types:
        yield "types widened: " + ", ".join(sorted(new_types - old_types))
    if before.get("value") != after.get("value") and after.get("value") is not None:
        yield f"takes {after.get('value')} value(s), was {before.get('value')}"

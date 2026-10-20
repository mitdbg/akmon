"""Config-key structural enrichers for YAML / TOML / JSON.

Per `cleanup-personal-okg-data-quality` C1 item 5. The three
formats all emit `ConfigKey` nodes from top-level keys; the
parsing differs but the emit pattern is identical, so they share
the base + a small per-format `_iter_top_level_keys` helper.

What counts as a "top-level key"
- JSON: keys of the root object. Arrays at the root → zero keys.
- YAML: keys of the root mapping. If multiple YAML documents,
  the first document's keys (PyYAML safe_load with `safe_load_all`
  could pick all; safe_load picks first — we use safe_load).
- TOML: top-level keys + table headers (`[section]`).

Nested keys are NOT recursed into. Goal is to give the graph a
deterministic anchor per file for "this file declares config
key X" queries. Deep walking is a follow-up if needed.

Parsers are pure functions; tested standalone.
"""
from __future__ import annotations
from okg.substrate import fastyaml

import json
import logging
from typing import Iterable

from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers.code_graph._structural import (
    NonPythonStructuralEnricher,
    ParsedEdge,
    ParsedFile,
    ParsedNode,
    fact_id,
    kebab_slug,
)

logger = logging.getLogger(__name__)

YAML_EXTRACTOR_VERSION = "1.0.0"
TOML_EXTRACTOR_VERSION = "1.0.0"
JSON_EXTRACTOR_VERSION = "1.0.0"


# ---------------------------------------------------------------- #
# Pure parsers per format. Each returns iterable of (key, source)  #
# pairs. `source` is a human-readable kind hint ("yaml_root_key",  #
# "toml_table", "json_root_key").                                  #
# ---------------------------------------------------------------- #


def iter_yaml_top_level_keys(text: str) -> Iterable[tuple[str, str]]:
    try:
        import yaml  # PyYAML
    except ImportError as exc:
        logger.warning("PyYAML not available: %s", exc)
        return
    try:
        loaded = fastyaml.safe_load(text)
    except yaml.YAMLError as exc:
        logger.warning("yaml parse error: %s", exc)
        return
    if isinstance(loaded, dict):
        for k in loaded.keys():
            yield (str(k), "yaml_root_key")


def iter_toml_top_level_keys(text: str) -> Iterable[tuple[str, str]]:
    try:
        # Python 3.11+ stdlib
        import tomllib  # type: ignore[import-not-found]
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            logger.warning("no toml library available")
            return
    try:
        loaded = tomllib.loads(text)
    except Exception as exc:  # tomllib.TOMLDecodeError
        logger.warning("toml parse error: %s", exc)
        return
    if isinstance(loaded, dict):
        for k in loaded.keys():
            yield (str(k), "toml_table")


def iter_json_top_level_keys(text: str) -> Iterable[tuple[str, str]]:
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("json parse error: %s", exc)
        return
    if isinstance(loaded, dict):
        for k in loaded.keys():
            yield (str(k), "json_root_key")


# ---------------------------------------------------------------- #
# Pure `parse()` shape used by enrichers.                          #
# ---------------------------------------------------------------- #


def _parse_config_file(
    text: str,
    *,
    repo: str,
    path: str,
    iter_keys: callable,
    source_kind: str,
) -> ParsedFile:
    nodes: list[ParsedNode] = []
    edges: list[ParsedEdge] = []
    file_node_id = f"file:{repo}:{path}"
    seen: set[str] = set()
    for key, kind in iter_keys(text):
        if not key or key in seen:
            continue
        seen.add(key)
        slug = kebab_slug(key, max_len=80)
        config_id = (
            f"configkey:{fact_id([repo, path, slug])}"
        )
        nodes.append(ParsedNode(
            node_id=config_id,
            subtype="config_key",
            attrs={
                "config_id": config_id,
                "repo": repo,
                "path": path,
                "key": key,
                "source_kind": kind,
                "text": key,
            },
        ))
        edges.append(ParsedEdge(
            src=file_node_id,
            dst=config_id,
            edge_type="contains",
            attrs={"key": key, "kind": kind},
        ))
    return ParsedFile(nodes=nodes, edges=edges)


# ---------------------------------------------------------------- #
# Enricher classes.                                                #
# ---------------------------------------------------------------- #


class YamlStructuralEnricher(NonPythonStructuralEnricher):
    name = "yaml_structural"
    # Every parsed node comes from exactly one source file.
    support_policy: DerivedSupportPolicy = "all_members"
    target_subtypes = ("config_key",)
    suffix = ".yaml"  # Also accept .yml via _matches_suffix.
    extractor_version = YAML_EXTRACTOR_VERSION

    def _matches_suffix(self, path: str) -> bool:
        return path.endswith(".yaml") or path.endswith(".yml")

    def parse(
        self, text: str, *, repo: str, path: str,
    ) -> ParsedFile:
        return _parse_config_file(
            text, repo=repo, path=path,
            iter_keys=iter_yaml_top_level_keys,
            source_kind="yaml_root_key",
        )


class TomlStructuralEnricher(NonPythonStructuralEnricher):
    name = "toml_structural"
    # Every parsed node comes from exactly one source file.
    support_policy: DerivedSupportPolicy = "all_members"
    target_subtypes = ("config_key",)
    suffix = ".toml"
    extractor_version = TOML_EXTRACTOR_VERSION

    def parse(
        self, text: str, *, repo: str, path: str,
    ) -> ParsedFile:
        return _parse_config_file(
            text, repo=repo, path=path,
            iter_keys=iter_toml_top_level_keys,
            source_kind="toml_table",
        )


class JsonStructuralEnricher(NonPythonStructuralEnricher):
    name = "json_structural"
    # Every parsed node comes from exactly one source file.
    support_policy: DerivedSupportPolicy = "all_members"
    target_subtypes = ("config_key",)
    suffix = ".json"
    extractor_version = JSON_EXTRACTOR_VERSION

    def parse(
        self, text: str, *, repo: str, path: str,
    ) -> ParsedFile:
        return _parse_config_file(
            text, repo=repo, path=path,
            iter_keys=iter_json_top_level_keys,
            source_kind="json_root_key",
        )

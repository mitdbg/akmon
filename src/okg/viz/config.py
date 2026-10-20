"""Viz configuration loader + merger.

Each deployment may ship a ``viz:`` block (inline in ``deployment.yaml``
or in a separate ``viz.yaml`` file referenced via ``viz_path``).  The
block is merged onto a built-in default shipped with the package.

The config drives:

- Cytoscape stylesheet generation (one rule per subtype / edge-type).
- Layout runner (policies execute in order; earlier passes pin their
  nodes so later ones don't disturb them).
- Time-axis default field.
- Theme colours.

Keeping the config a plain dict (not a Pydantic model) keeps round-
tripping to YAML simple and lets the settings drawer emit a diff
against the defaults without a schema dance.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml

_DEFAULT_CONFIG_PATH = Path(__file__).parent / "default_config.yaml"

# Reserved subtype / edge-type tokens the framework may inject.
# Listed here for discoverability; the ontology loader enforces that
# user-declared names don't collide with the __*__ pattern.
RESERVED_NODE_SUBTYPES = ("__default__", "__meta__", "__remainder__", "__trace_focus__", "__archetype__")
RESERVED_EDGE_TYPES = ("__default__", "__meta_edge__", "__trace__", "__path__")


def load_default_config() -> dict[str, Any]:
    """Return a fresh deepcopy of the built-in default config."""
    with _DEFAULT_CONFIG_PATH.open() as f:
        return fastyaml.safe_load(f) or {}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` onto ``base``.

    Dict keys in ``override`` replace the corresponding subtree in
    ``base``.  Lists are REPLACED, not concatenated — deployments that
    want to add a layout policy declare the full list, not a delta.
    This keeps merge semantics predictable for the user.
    """
    out = deepcopy(base)
    for key, val in override.items():
        if (
            key in out
            and isinstance(out[key], dict)
            and isinstance(val, dict)
        ):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = deepcopy(val)
    return out


def load_viz_config(
    *,
    inline: dict[str, Any] | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Return the merged viz config for a deployment.

    Priority: inline > path > default.  Exactly one of ``inline`` or
    ``path`` should be set (inline wins if both).  Returns the default
    alone when neither is provided.
    """
    default = load_default_config()
    override: dict[str, Any] = {}
    if inline is not None:
        override = inline
    elif path is not None:
        with path.open() as f:
            loaded = fastyaml.safe_load(f)
        if not isinstance(loaded, dict):
            raise ValueError(f"viz config at {path} is not a YAML mapping")
        override = loaded
    return deep_merge(default, override)


def diff_against_default(merged: dict[str, Any]) -> dict[str, Any]:
    """Return the subset of ``merged`` that differs from the default.

    Used by the "Download YAML" action so the committed override is
    minimal — only the fields the user actually changed.  Deep-walks
    both dicts; a value survives to the output iff it differs from
    the corresponding default value.
    """
    default = load_default_config()
    result = _diff(merged, default)
    return {} if result is _UNCHANGED else result


def _diff(override: Any, default: Any) -> Any:
    if isinstance(override, dict) and isinstance(default, dict):
        out: dict[str, Any] = {}
        for k, v in override.items():
            if k not in default:
                out[k] = v
                continue
            sub = _diff(v, default[k])
            if sub is not _UNCHANGED:
                out[k] = sub
        return out if out else _UNCHANGED
    return override if override != default else _UNCHANGED


class _Unchanged:
    def __repr__(self) -> str:
        return "<UNCHANGED>"


_UNCHANGED = _Unchanged()

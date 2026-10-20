"""Fast YAML loading via libyaml's CSafeLoader.

`yaml.safe_load` uses PyYAML's pure-Python parser, which profiling showed is
~8.6x slower than the libyaml C parser over this repo's config YAML. The C
loader (`CSafeLoader`) is a drop-in for `SafeLoader` with identical safe
semantics -- this codebase registers NO custom YAML constructors/representers
(verified), so there is no behavioural difference, only speed.

Hot paths (catalog compose re-parses ~133 module YAMLs per call; deployment
loading parses configs on every DB-backed test) should use `safe_load` from
here instead of `yaml.safe_load`.
"""
from __future__ import annotations

from typing import Any

import yaml

# CSafeLoader when libyaml is available (the normal case); fall back to the
# pure-Python SafeLoader otherwise so behaviour is never worse than the default.
_LOADER = yaml.CSafeLoader if getattr(yaml, "__with_libyaml__", False) else yaml.SafeLoader


def safe_load(stream: Any) -> Any:
    """Drop-in for ``yaml.safe_load`` using libyaml's CSafeLoader when present."""
    return yaml.load(stream, Loader=_LOADER)

"""Shared helpers for substrate scholarly source adapters.

Lifted from `deployments/memex/sources/_common.py` per
`lift-scholarly-sources-to-substrate-library`. Only the generic
helpers used by the scholarly adapters are lifted here. Memex-
corpus-specific helpers (`require_seed_root`, `first_heading`,
`bold_field`, `key_value_lines`, `front_matter`, file-read
helpers used by the wisdom corpus readers) remain in the
deployment's `deployments/memex/sources/_common.py`.

Convention: this module is package-private to substrate scholarly
(leading underscore). External consumers compose the scholarly
module rather than reaching into its private helpers.
"""

from __future__ import annotations

import re


def okg_user_agent() -> str:
    """The HTTP User-Agent the scholarly readers send: `okg/<installed version>`.

    Read from the installed package on every call, so a version bump changes
    only `pyproject.toml` and `uv.lock`, never this string.
    """
    import okg

    return f"okg/{okg.__version__}"


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    slug = _SLUG_RE.sub("-", value.lower()).strip("-")
    return slug or "unknown"

"""academic-researcher profile CLI.

Discovered by the substrate's `cli/profile_cli_loader.py`. Exports
the contract the loader expects: `register_subparsers(sub)` and
`DISPATCH`. The implementation lives in sibling files (`install.py`,
`wizards.py`); this `__init__.py` is the public surface.

Per `retire-pi-prefix-commands`: this code used to live in
`src/okg/substrate/cli/pi.py` and `pi_add.py`. It was relocated
to enforce the substrate/profile boundary — the substrate's CLI
dispatcher no longer references academic-researcher by name.
"""
from __future__ import annotations

from . import install  # noqa: F401  (loads sibling at import time)
from . import wizards  # noqa: F401


# Re-export the canonical contract names. Install.py keeps the
# legacy `build_parsers` name internally; the substrate loader
# accepts either `build_parsers` or `register_subparsers`.
build_parsers = install.build_parsers
register_subparsers = install.build_parsers
DISPATCH = install.DISPATCH

#: Opts this profile into the substrate-owned `install` verb (PACT
#: `define-newcomer-entry-path`). The substrate merges each installable
#: profile's arguments onto one parser and routes `--profile` here, so
#: `install` can be the single front door without the substrate naming
#: academic-researcher — the boundary `retire-pi-prefix-commands` established.
INSTALL_EXTENSION = install.INSTALL_EXTENSION


__all__ = [
    "build_parsers",
    "register_subparsers",
    "DISPATCH",
    "INSTALL_EXTENSION",
]

"""The one argv prefix for starting the running okg CLI as a child process.

Issue #2313. ``okg install`` outside a source checkout ran its catalog-load
and ingest children as ``python -m okg.substrate.cli.main`` with a working
directory inside the installed package (``.../okg/substrate``). ``-m`` puts
the working directory first on ``sys.path``, so ``import mcp`` found the
package's own ``okg/substrate/mcp`` instead of the third-party ``mcp``
library, and from a wheel ``import types`` found ``okg/substrate/types.py``
instead of the standard library. Both failed as a "circular import".

Every okg CLI child therefore starts through :func:`running_okg_argv`:

* ``sys.executable`` is the interpreter running this okg, so no ``PATH``
  entry can select a different okg (issue #2211); and
* ``-P`` (Python 3.11+) keeps the child's working directory off
  ``sys.path``, so no directory the child happens to run in -- an operator
  folder holding ``mcp/`` or ``okg/``, or a directory inside the package --
  can shadow an installed module.

``tests/substrate/cli/test_child_argv.py`` refuses any new
``"-m", "okg.substrate.cli.main"`` argv in ``src/okg`` that bypasses this
helper.
"""
from __future__ import annotations

import sys

OKG_CLI_MODULE = "okg.substrate.cli.main"


def inherited_bytecode_flags() -> list[str]:
    """Carry a sealed service's bytecode isolation into its Python children.

    Issue #2861. The sealed release launcher starts each service with ``-B -X
    pycache_prefix=<private dir>`` so it never reads or writes ``__pycache__``
    inside the sealed tree. A child started with bare ``sys.executable`` would
    drop both. This returns the same flags only when this process runs with
    both set; any other interpreter (a checkout, a test run) is unchanged.
    """
    prefix = sys.pycache_prefix
    if not sys.flags.dont_write_bytecode or not prefix:
        return []
    return ["-B", "-X", f"pycache_prefix={prefix}"]


def running_okg_argv() -> list[str]:
    """argv prefix that re-enters THIS okg with its cwd kept off sys.path."""
    return [sys.executable, *inherited_bytecode_flags(), "-P", "-m", OKG_CLI_MODULE]

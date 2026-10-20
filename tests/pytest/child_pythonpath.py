"""PYTHONPATH for a test's child Python process.

Never copy the parent's `sys.path` into a child. `inject_deployment_path`
prepends a deployment dir and its parent to `sys.path` and keeps them, so
every test that resolves a deployment under its own `tmp_path` adds two
entries. One serial run of `tests/substrate/mcp` grew `sys.path` to 1,300+
entries; copied into PYTHONPATH that passed Linux's 128 KiB per-string limit
and starting the child failed with `OSError: [Errno 7] Argument list too
long`.

The child runs the same interpreter, so it already finds that interpreter's
site-packages and the editable `okg` install on its own. It needs only the
repo roots, any directories the test names, and the PYTHONPATH the parent
was started with.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"


def child_pythonpath(*extra: str | os.PathLike[str]) -> str:
    """Return `extra` dirs, the src root, the repo root, then the inherited
    PYTHONPATH, joined with `os.pathsep`."""
    parts = [str(entry) for entry in extra]
    parts += [str(SRC_ROOT), str(REPO_ROOT), os.environ.get("PYTHONPATH", "")]
    return os.pathsep.join(part for part in parts if part)

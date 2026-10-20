"""Commands and documentation links that okg prints for the person running it.

okg's hints used to say `uv run okg ...` and name repository paths such as
`docs/runbooks/troubleshooting/storage.md`. Both only work inside a source
checkout: someone who installed the wheel has no `uv` project and no `docs/`
folder (release test #3480). These helpers pick the form that works where
okg is actually running:

* inside a source checkout, the repository forms are kept (`uv run okg` is
  the repository's command authority, per `pact/project.yaml`);
* anywhere else, the command is plain `okg` and a document is a link to the
  public documentation source.

Stdlib-only: hints are built on error paths that must not fail themselves.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

#: The command a checkout uses.
CHECKOUT_COMMAND = "uv run okg"
#: The command an installed package (wheel, uv tool, pip) puts on PATH.
INSTALLED_COMMAND = "okg"
#: Where the published documentation source lives. The public repository is
#: the export of this one (release/public-mirror/allowlist.yaml:
#: `public_repo_url`, `public_branch`), and the export keeps `docs/`.
PUBLIC_DOCS_BASE = "https://github.com/mitdbg/akmon/blob/main"


@lru_cache(maxsize=1)
def checkout_root() -> Path | None:
    """The source checkout this okg package runs from, or None.

    True for an editable install (`uv sync` in a clone, which is also what
    install.sh sets up); False for a wheel, whose package lives in
    `site-packages` with no `pyproject.toml` or `docs/` beside it.
    """
    root = Path(__file__).resolve().parents[4]
    package = root / "src" / "okg"
    if (
        (root / "pyproject.toml").is_file()
        and (root / "docs").is_dir()
        and package.is_dir()
        and Path(__file__).resolve().is_relative_to(package)
    ):
        return root
    return None


def okg_prefix() -> str:
    """How to start okg where it is installed: `uv run okg` or `okg`."""
    return CHECKOUT_COMMAND if checkout_root() is not None else INSTALLED_COMMAND


def hint_command(command: Any) -> str:
    """A command's hint text in the form that runs where okg is installed.

    ``command`` comes from ``okg.substrate.cli.command_reference.okg_command``,
    which checks it against the CLI parser (and whose calls a test scans), so
    a hint never spells a verb or flag by hand.
    """
    return str(command.display(prefix=okg_prefix()))


def doc_reference(path: str) -> str:
    """A reference to a repository document the reader can open.

    In a checkout: the repository-relative path when okg runs from the
    checkout's top folder, else the absolute path of that file. Elsewhere:
    the document's link in the public documentation source.
    """
    root = checkout_root()
    if root is None:
        return f"{PUBLIC_DOCS_BASE}/{path}"
    try:
        if Path(os.getcwd()).resolve() == root:
            return path
    except OSError:
        pass
    return str(root / path)

"""Commands and documentation links that okg prints for the person running it.

okg's hints used to say `uv run okg ...` and name repository paths such as
`docs/runbooks/troubleshooting/storage.md`. Both only work inside a source
checkout: someone who installed the wheel has no `uv` project and no `docs/`
folder (release test #3480). These helpers pick the form that works where
okg is actually running:

* inside a source checkout, the repository forms are kept (`uv run okg` is
  the repository's command authority, per `pact/project.yaml`), except in
  a shell that has the checkout's virtual environment activated, where
  plain `okg` already runs this same package (#3891, #3848);
* anywhere else, the command is plain `okg` and a document is a link to the
  public documentation source.

Stdlib-only: hints are built on error paths that must not fail themselves.
"""
from __future__ import annotations

import os
import sys
import sysconfig
from collections.abc import Mapping
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


def plain_okg_runs_this_package(
    environ: Mapping[str, str] | None = None,
    *,
    prefix: str | None = None,
    base_prefix: str | None = None,
    scripts_dir: str | None = None,
) -> bool:
    """True when typing plain `okg` runs the okg that is running now.

    That holds when the shell has the virtual environment of this very
    interpreter activated: `VIRTUAL_ENV` names `sys.prefix`, and the first
    `okg` on `PATH` is that environment's own script. Three cases say no:

    * not in a virtual environment at all;
    * the process was started by `uv run`, which sets `VIRTUAL_ENV` and
      `PATH` for the child only (it also sets `UV_RUN_RECURSION_DEPTH`),
      so the user's own shell has no `okg`;
    * the first `okg` on `PATH` belongs to another environment.
    """
    env = os.environ if environ is None else environ
    venv = env.get("VIRTUAL_ENV")
    if not venv or "UV_RUN_RECURSION_DEPTH" in env:
        return False
    prefix = sys.prefix if prefix is None else prefix
    base_prefix = sys.base_prefix if base_prefix is None else base_prefix
    if prefix == base_prefix:
        return False
    try:
        if Path(venv).resolve() != Path(prefix).resolve():
            return False
        scripts = Path(scripts_dir or sysconfig.get_path("scripts")).resolve()
        # The shell runs the first `okg` it finds on PATH; that one must be
        # this environment's own script.
        for entry in env.get("PATH", "").split(os.pathsep):
            candidate = Path(entry) / INSTALLED_COMMAND
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return Path(entry).resolve() == scripts
        return False
    except OSError:
        return False


def stable_okg_prefix() -> str:
    """The checkout or installed form, whatever shell is running okg.

    For text that is written to a file and read later, such as a generated
    README: it must not change with the shell that happened to write it.
    """
    return CHECKOUT_COMMAND if checkout_root() is not None else INSTALLED_COMMAND


def okg_prefix() -> str:
    """How to start okg where it is installed: `uv run okg` or `okg`.

    A checkout whose virtual environment is activated in the caller's shell
    gets plain `okg`. Caveat: the check looks at this interpreter's
    environment, not at the source tree. A virtual environment shared by
    several worktrees (an editable install points at one of them) runs
    whichever tree is first on `sys.path`, and `okg` in that shell runs the
    same one, so the hint is right for the shell that printed it.
    """
    if checkout_root() is None:
        return INSTALLED_COMMAND
    if plain_okg_runs_this_package():
        return INSTALLED_COMMAND
    return CHECKOUT_COMMAND


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

"""Where okg's source tree is, when the running okg was installed from a wheel.

Some framework data lives beside the package, not inside it: `deployments/`
(the in-tree deployments the Nomos enforce-mode DSN scan must include) and
`ops/environments/` (environment descriptors). In a source checkout that tree
is the directory above `src/okg`. A wheel install has no such directory: the
package sits in site-packages, and a fixed `Path(__file__).parents[N]` walk
lands in the environment's `lib/`, where the files silently are not.

The runtime image installs okg from a wheel and keeps the source tree at
/opt/okg; it declares that with OKG_FRAMEWORK_TREE. This is not
OKG_REPO_ROOT: the chart points that at the synced deployment repository when
repository sync is on, which is not okg's tree.
"""

from __future__ import annotations

import os
from pathlib import Path

from okg.substrate.errors import SubstrateError

FRAMEWORK_TREE_ENV = "OKG_FRAMEWORK_TREE"


class FrameworkTreeUnavailable(SubstrateError):
    """OKG_FRAMEWORK_TREE names a directory that is not okg's source tree."""

    code = "framework_tree_unavailable"


def framework_tree_root() -> Path | None:
    """okg's source tree, or None for a wheel install that declares none.

    OKG_FRAMEWORK_TREE wins and must name a directory holding `src/okg`;
    anything else refuses, so a declared tree never silently drops out.
    Without it, a source checkout (the package's grandparent holds
    pyproject.toml and src/okg) is its own tree.
    """
    configured = os.environ.get(FRAMEWORK_TREE_ENV)
    if configured:
        root = Path(configured).expanduser()
        if not (root / "src" / "okg").is_dir():
            raise FrameworkTreeUnavailable(
                f"{FRAMEWORK_TREE_ENV}={configured!r} is not okg's source tree",
                data={"env": FRAMEWORK_TREE_ENV, "value": configured},
                fix_hint=(
                    f"point {FRAMEWORK_TREE_ENV} at the directory that holds "
                    "okg's src/okg, deployments/ and ops/ (/opt/okg in the "
                    "runtime image), or unset it"
                ),
            )
        return root.resolve()
    import okg

    candidate = Path(okg.__file__).resolve().parents[2]
    if (candidate / "pyproject.toml").is_file() and (candidate / "src" / "okg").is_dir():
        return candidate
    return None

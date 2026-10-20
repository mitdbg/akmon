"""Import a deployment's top-level ``views`` package without cross-test leaks.

``views`` is a top-level package name that every deployment (and every
scaffolded test deployment under ``tmp_path``) may define. A bare
``sys.path.insert`` guarded by ``if p not in sys.path`` loses in two ways:

* a foreign ``views`` already cached in ``sys.modules`` wins over the
  path entry (``views.<name>`` then fails with ``ModuleNotFoundError``), and
  the root conftest eviction only catches packages whose origin sits under a
  ``deployments/`` directory, not ``tmp_path/deployment/views``;
* an earlier test may have left another deployment dir at the front of
  ``sys.path``.

``isolated_deployment_views`` puts the deployment dir first, hides any cached
``views`` modules for the duration, and on exit restores ``sys.path`` and the
previous ``views`` entries exactly, so the test module neither sees nor leaves
pollution.
"""
from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

_PACKAGE = "views"


def _views_entries() -> dict[str, ModuleType]:
    return {
        name: module
        for name, module in sys.modules.items()
        if name == _PACKAGE or name.startswith(f"{_PACKAGE}.")
    }


def _drop_views_entries() -> None:
    for name in list(_views_entries()):
        sys.modules.pop(name, None)


@contextmanager
def isolated_deployment_views(deployment_dir: Path) -> Iterator[ModuleType]:
    """Yield ``deployment_dir``'s ``views`` package, then undo every change.

    Fails loudly if the imported package does not come from
    ``deployment_dir``: a shadowed import must not pass silently.
    """
    dep_dir = Path(deployment_dir).resolve()
    saved_path = list(sys.path)
    saved_modules = _views_entries()
    _drop_views_entries()
    sys.path.insert(0, str(dep_dir))
    importlib.invalidate_caches()
    try:
        package = importlib.import_module(_PACKAGE)
        origin = Path(package.__file__ or "").resolve()
        if origin.parent != dep_dir / _PACKAGE:
            raise ImportError(
                f"`{_PACKAGE}` resolved to {origin}, not {dep_dir / _PACKAGE}"
            )
        yield package
    finally:
        _drop_views_entries()
        sys.modules.update(saved_modules)
        sys.path[:] = saved_path
        importlib.invalidate_caches()

"""The ``owner/name`` of the repository this package was built from.

A code-proof receipt names the repository it proves code in, and the check
refuses a receipt that names any other. The name used to be a literal in the
source. It now comes from the installed package's metadata: the
``repository`` entry of ``[project.urls]`` in ``pyproject.toml``. A fork or a
mirror that publishes the package under another repository changes that one
line and the receipt check follows, instead of refusing every receipt it
writes.

There is no fallback. Missing metadata, a missing ``repository`` entry, or a
url that is not ``https://github.com/<owner>/<name>`` raises
``HomeRepositoryUnavailable`` naming what was found, so a broken install
cannot quietly accept or refuse receipts against a guessed name.
"""

from __future__ import annotations

from functools import lru_cache
from importlib import metadata
import re

DISTRIBUTION = "okg"
_URL_LABEL = "repository"
_GITHUB_URL_RE = re.compile(
    r"^https://github\.com/(?P<owner>[A-Za-z0-9][A-Za-z0-9-]*)/"
    r"(?P<name>[A-Za-z0-9._-]+?)(?:\.git)?/?$"
)


class HomeRepositoryUnavailable(RuntimeError):
    """The package metadata does not name a GitHub repository."""


def parse_repository_url(url: str) -> str:
    """``https://github.com/<owner>/<name>[.git]`` -> ``owner/name``."""
    match = _GITHUB_URL_RE.fullmatch(url.strip())
    if match is None:
        raise HomeRepositoryUnavailable(
            f"package metadata repository url {url!r} is not "
            "https://github.com/<owner>/<name>"
        )
    return f"{match.group('owner')}/{match.group('name')}"


@lru_cache(maxsize=1)
def home_repository() -> str:
    """The ``owner/name`` from the installed package's ``repository`` url."""
    try:
        entries = metadata.metadata(DISTRIBUTION).get_all("Project-URL") or []
    except metadata.PackageNotFoundError as exc:
        raise HomeRepositoryUnavailable(
            f"package {DISTRIBUTION!r} is not installed, so its repository "
            "url cannot be read; install it (uv sync) before checking code proofs"
        ) from exc
    for entry in entries:
        label, _, url = entry.partition(",")
        if label.strip().lower() == _URL_LABEL:
            return parse_repository_url(url)
    raise HomeRepositoryUnavailable(
        f"package {DISTRIBUTION!r} metadata has no {_URL_LABEL!r} project url "
        f"(found {entries!r}); set [project.urls] {_URL_LABEL} in pyproject.toml"
    )

"""Source-supplied citation helpers (PACT `source-supplied-search-citations`).

A citable searchable node carries two presentation attrs supplied by its
source at emit time: ``citation_label`` (a name a user recognizes —
never the graph node id) and optional ``citation_url`` (a truthful
HTTP(S) link to the record as ingested). Search returns them verbatim;
nothing on the read side composes citations from other attributes.

For git-backed sources the label is ``<repo>/<repo-relative-path>`` and
the url — when one can be produced truthfully — is the browser view of
the file at the **immutable commit** read during ingestion, on an
explicitly supported host. Unsupported hosts, unknown
remotes, dirty working-tree files, and detached states yield label-only
citations; a missing link is always preferred over a guessed one.

Supported browser hosts: ``github.com`` (``/blob/<sha>/<path>``) and
GitLab instances — ``gitlab.com`` or any ``gitlab.*`` host —
(``/-/blob/<sha>/<path>``). Other hosts produce no URL.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote

__all__ = [
    "web_base_url",
    "repo_file_url",
    "citation_label",
    "head_commit_sha",
    "dirty_paths",
]

# git@host:group/repo.git  |  ssh://git@host[:port]/group/repo.git
_SCP_REMOTE_RE = re.compile(r"^(?:[\w.-]+@)?([\w.-]+):(?!//)(.+)$")
_SSH_REMOTE_RE = re.compile(
    r"^ssh://(?:[\w.-]+@)?([\w.-]+)(?::\d+)?/(.+)$",
)
# HTTP(S) keeps an explicit port (it IS the web port); ssh/scp ports are
# dropped (an ssh port says nothing about where the web UI listens).
_HTTP_REMOTE_RE = re.compile(
    r"^(https?)://(?:[\w.%-]+(?::[^@/]*)?@)?([\w.-]+)(:\d+)?/(.+)$",
)


def web_base_url(remote_url: Optional[str]) -> Optional[str]:
    """Normalize a git remote to an HTTP(S) repository web base.

    Accepts HTTP(S)/SSH/SCP-style remotes; strips credentials and a
    trailing ``.git`` (an explicit HTTP(S) port is kept — it is the web
    port; ssh ports are dropped). Returns None for empty, local-path,
    single-segment, or otherwise unrecognized remotes (``file://``,
    bare paths, dotless scp aliases like ``cerngit:group/repo``) —
    callers must treat None as "emit no url", never guess.
    """
    raw = (remote_url or "").strip().rstrip("/")
    if not raw or raw.startswith("file://"):
        return None
    m = _HTTP_REMOTE_RE.match(raw)
    if m:
        scheme, host, port, path = (
            m.group(1), m.group(2), m.group(3) or "", m.group(4)
        )
        return _compose_base(scheme, host, port, path)
    for pattern in (_SSH_REMOTE_RE, _SCP_REMOTE_RE):
        m = pattern.match(raw)
        if m:
            host, path = m.group(1), m.group(2)
            # An scp-style "host" with no dot is almost always an
            # ssh-config alias (`cerngit:cms/repo.git`) — the real web
            # host is unknowable from it, so refuse rather than guess.
            if pattern is _SCP_REMOTE_RE and "." not in host:
                return None
            return _compose_base("https", host, "", path)
    return None


def _compose_base(
    scheme: str,
    host: str,
    port: str,
    path: str,
) -> Optional[str]:
    path = path.strip("/")
    if not host or not path or "/" not in path:
        return None
    if path.endswith(".git"):
        path = path[: -len(".git")]
    if not path or "/" not in path:
        return None
    encoded_path = quote(path, safe="/%:@+~.-_")
    return f"{scheme}://{host}{port}/{encoded_path}"


def _host_style(base: str) -> Optional[str]:
    """Browser-path dialect for a supported web host, otherwise None."""
    host = re.sub(r"^https?://", "", base).split("/", 1)[0].split(":", 1)[0]
    if host == "github.com":
        return "github"
    if host == "gitlab.com" or host.startswith("gitlab."):
        return "gitlab"
    return None


def repo_file_url(
    remote_url: Optional[str],
    ref: Optional[str],
    path: Optional[str],
) -> Optional[str]:
    """Browser view of ``path`` at the immutable ``ref``, or None.

    None whenever the remote is unrecognized, the host is unsupported,
    or any part is missing — the caller then emits a label-only citation.
    """
    base = web_base_url(remote_url)
    rel = (path or "").strip().lstrip("/")
    if not base or not ref or not rel:
        return None
    host_style = _host_style(base)
    if host_style is None:
        return None
    blob = "/blob/" if host_style == "github" else "/-/blob/"
    return f"{base}{blob}{quote(ref, safe='')}/{quote(rel, safe='/')}"


def citation_label(repo: Optional[str], path: Optional[str]) -> Optional[str]:
    """User-recognizable label for a repository-backed record:
    ``<repo>/<repo-relative-path>``. None when either part is missing."""
    repo = (repo or "").strip().strip("/")
    rel = (path or "").strip().lstrip("/")
    if not repo or not rel:
        return None
    return f"{repo}/{rel}"


def head_commit_sha(repo_root: Path | str) -> Optional[str]:
    """The checkout's HEAD commit sha — the immutable revision whose
    tree the source is reading. None for non-git corpora or on any git
    failure (never raises)."""
    try:
        from okg.substrate.library.sources._git_helpers import run_git

        sha = run_git(Path(repo_root), ["rev-parse", "HEAD"]).strip()
        return sha or None
    except Exception:  # noqa: BLE001 — any git/IO failure means "no citable revision"
        return None


def dirty_paths(repo_root: Path | str) -> Optional[frozenset[str]]:
    """Repo-relative paths whose working-tree state differs from HEAD
    (modified or untracked). A dirty file's bytes are NOT the bytes at
    any commit, so a commit-pinned url would be untruthful — callers
    emit label-only citations for these. None (distinct from empty)
    when git status itself fails."""
    try:
        from okg.substrate.library.sources._git_helpers import run_git

        out = run_git(
            Path(repo_root),
            ["--no-optional-locks", "status", "--porcelain", "--untracked-files=all"],
        )
        paths: set[str] = set()
        for line in out.splitlines():
            if len(line) > 3:
                entry = line[3:].strip().strip('"')
                # renames render as "old -> new"; the working-tree file is new
                if " -> " in entry:
                    entry = entry.split(" -> ", 1)[1].strip().strip('"')
                paths.add(entry)
        return frozenset(paths)
    except Exception:  # noqa: BLE001 — status failure ⇒ cannot prove cleanliness
        return None

"""Docs must not link into the private repository in ways the public mirror cannot fix.

The public mirror (``release/public-mirror/allowlist.yaml``) rewrites the
repository URL in ``mkdocs.yml`` and ``docs/``. That rewrite keeps a plain
repository link or ``git clone`` of that URL correct in both builds, but it
cannot fix these links to the ``repo_url`` repository or a private sibling
listed in ``PRIVATE_SIBLINGS``:

* a file link (blob, tree, blame, raw.githubusercontent.com): the file may not
  ship, and the branch differs;
* a commit or compare link: the private history does not ship;
* an issue or pull-request link, or the ``owner/repo#123`` shorthand: the
  numbers exist only in the private tracker;
* a ``git clone`` in any form other than ``repo_url`` itself, such as an SSH
  remote, or of a private sibling.

Links to other repositories, including other public repositories of the same
owner, are not this check's business. Refer to files with a backticked
repository path, to private issues as "internal issue N", and clone
``repo_url`` from ``mkdocs.yml``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
# Private repositories of the repo_url owner that docs may name but not link into.
PRIVATE_SIBLINGS = ("okg-deployments",)
CLONE_URL = re.compile(r"(?:https?://|ssh://|git@)[^\s`'\"<>)]+")
LINK_KINDS = {
    "blob": "file link",
    "tree": "file link",
    "blame": "file link",
    "raw": "file link",
    "commit": "commit link",
    "compare": "commit link",
    "issues": "issue link",
    "pull": "issue link",
}


def _mkdocs(root: Path) -> dict:
    return yaml.safe_load((root / "mkdocs.yml").read_text(encoding="utf-8"))


def _repo_url(root: Path) -> str:
    repo_url = str(_mkdocs(root).get("repo_url") or "").rstrip("/")
    if not re.fullmatch(r"https://github\.com/[\w.-]+/[\w.-]+", repo_url):
        raise AssertionError(f"mkdocs.yml repo_url is not a GitHub repository URL: {repo_url!r}")
    return repo_url


def find_private_link_issues(root: Path) -> list[str]:
    """Return one ``path:line: reason`` entry per link the mirror cannot fix."""
    repo_url = _repo_url(root)
    owner, name = repo_url.split("/")[3:5]
    repos = "|".join(re.escape(repo) for repo in (name, *PRIVATE_SIBLINGS))
    slug = rf"{re.escape(owner)}/(?:{repos})"
    rules = (
        re.compile(rf"github\.com/{slug}/(blob|tree|blame|commit|compare|issues|pull)/[^\s)\]>`'\"]*"),
        re.compile(rf"raw\.githubusercontent\.com/{slug}/()[^\s)\]>`'\"]*"),
        re.compile(rf"(?<![\w./-]){slug}#()\d+"),
    )
    clone_repo = re.compile(rf"github\.com[:/]{re.escape(owner)}/([\w.-]+?)(?:\.git)?/?$")
    private = {name, *PRIVATE_SIBLINGS}
    allowed_clone = {repo_url, f"{repo_url}.git"}
    issues: list[str] = []
    for path in sorted((root / "docs").rglob("*")):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = path.relative_to(root).as_posix()
        for number, line in enumerate(text.splitlines(), start=1):
            for index, rule in enumerate(rules):
                for match in rule.finditer(line):
                    kind = ("file link", "issue link")[index - 1] if index else LINK_KINDS[match.group(1)]
                    issues.append(f"{rel}:{number}: private {kind} {match.group(0)}")
            if "git clone" in line:
                for url in CLONE_URL.findall(line.split("git clone", 1)[1]):
                    url = url.rstrip(".,;")
                    target = clone_repo.search(url)
                    if target and target.group(1) in private and url not in allowed_clone:
                        issues.append(f"{rel}:{number}: clone URL {url} is not repo_url {repo_url}")
    return issues


def _repo(tmp_path: Path, page: str) -> Path:
    root = tmp_path / "repo"
    (root / "docs").mkdir(parents=True)
    (root / "mkdocs.yml").write_text("repo_url: https://github.com/mitdbg/okg\n", encoding="utf-8")
    (root / "docs" / "page.md").write_text(page, encoding="utf-8")
    return root


def test_docs_have_no_private_links() -> None:
    assert find_private_link_issues(ROOT) == []


def test_mkdocs_renders_fences_in_lists_and_keeps_language_classes() -> None:
    """SuperFences renders fences inside list items. With Pygments on, every block
    loses its ``class="language-<name>"`` and the theme's highlight.js guesses the
    language instead, so pymdownx.highlight must set use_pygments: false."""
    extensions: dict[str, object] = {}
    for ext in _mkdocs(ROOT).get("markdown_extensions") or []:
        key, value = next(iter(ext.items())) if isinstance(ext, dict) else (ext, None)
        extensions[key] = value or {}
    assert "pymdownx.superfences" in extensions
    assert "pymdownx.highlight" in extensions, "pymdownx.highlight must be configured"
    assert extensions["pymdownx.highlight"].get("use_pygments") is False
    names = list(extensions)
    assert names.index("pymdownx.highlight") < names.index("pymdownx.superfences")


def test_allowed_references_pass(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        "git clone -b dev https://github.com/mitdbg/okg.git\n"
        "git clone https://github.com/mitdbg/okg ~/okg\n"
        "git clone https://github.com/rhysd/actionlint.git\n"
        "git clone https://github.com/mitdbg/palimpzest.git\n"
        "[pz](https://github.com/mitdbg/palimpzest/blob/main/README.md)\n"
        "[okgx](https://github.com/mitdbg/okgx/issues/3)\n"
        "[fix](https://github.com/pallets/click/pull/2800) and pallets/click#2800\n"
        "See `src/okg/AGENTS.md` and internal issue 2478.\n",
    )
    assert find_private_link_issues(root) == []


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        ("[main.py](https://github.com/mitdbg/okg/blob/dev/src/okg/main.py)", "private file link"),
        ("[examples](https://github.com/mitdbg/okg/tree/dev/examples)", "private file link"),
        ("[raw](https://raw.githubusercontent.com/mitdbg/okg/dev/install.sh)", "private file link"),
        ("[fix](https://github.com/mitdbg/okg/commit/40b9042b77)", "private commit link"),
        ("[diff](https://github.com/mitdbg/okg/compare/main...dev)", "private commit link"),
        ("([issue 2478](https://github.com/mitdbg/okg/issues/2478))", "private issue link"),
        ("[#94](https://github.com/mitdbg/okg-deployments/pull/94)", "private issue link"),
        ("Fixed in mitdbg/okg#2478.", "private issue link"),
        ("git clone git@github.com:mitdbg/okg.git", "clone URL"),
        ("git clone git@github.com:mitdbg/okg-deployments.git", "clone URL"),
        ("git clone -b dev https://github.com/mitdbg/okg-deployments", "clone URL"),
    ],
)
def test_private_links_fail(tmp_path: Path, line: str, reason: str) -> None:
    issues = find_private_link_issues(_repo(tmp_path, f"Intro.\n{line}\n"))
    assert len(issues) == 1, issues
    assert issues[0].startswith("docs/page.md:2: ")
    assert reason in issues[0]


def test_non_github_repo_url_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path, "text\n")
    (root / "mkdocs.yml").write_text("repo_url: ''\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="repo_url"):
        find_private_link_issues(root)

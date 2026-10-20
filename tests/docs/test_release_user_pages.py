"""The install and operate route stays followable by someone with a wheel.

The pages a release user is sent to (install, operate, profiles, sources) were
written and checked from inside a checkout, and three whole classes of defect
survived every existing gate:

* ``scripts/lint_cli_references.py`` proves each documented ``okg <verb>``
  exists, and ``scripts/check_doc_commands.py`` denies a list of retired
  forms -- but ``uv run okg install`` passes both, because the verb is real
  and the prefix is not on the denylist. A wheel user has no ``uv`` project,
  so the command they copy cannot run.
* ``scripts/check_operator_walkthrough.py`` drives the ledger steps as
  ``python -m okg.substrate.cli.main`` with the repository as the working
  directory, so no checkout assumption on any page can fail it.
* Nothing parsed a documented configuration block, so the ``chat:`` block the
  chat verbs require could be missing from the documentation entirely (it
  was) and, once written, could rot against the schema unnoticed.

These tests close the three classes on the release-user pages. They say
nothing about the runbooks, which document operating *this repository* from a
checkout, where ``uv run`` is the correct form.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployments.chat_block import ChatManifestError, parse_chat_block

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"

#: Pages under these trees route someone holding the software to a working
#: graph, so a command on one of them is copied by a person who installed a
#: wheel. The set is DERIVED, not listed: a page added to either tree is
#: guarded from the moment it exists, which a hand-kept list cannot promise.
RELEASE_USER_TREES: tuple[str, ...] = ("operate", "use")

#: Extra pages outside those trees that route a newcomer the same way.
RELEASE_USER_EXTRAS: tuple[str, ...] = ("README.md",)

#: The only pages inside those trees that are exempt, each with the reason.
#: An exemption is a claim that the page documents working from a checkout;
#: nothing else justifies one, and the reason is asserted non-empty below.
EXEMPT_PAGES: dict[str, str] = {
    "operate/operator-ui.md": (
        "documents running the read API and the frontend dev server out of a "
        "checkout -- it requires the project's postgres and viz extras and "
        "Node.js with npm, so `uv run` is the correct form on that page"
    ),
}


def release_user_pages() -> tuple[str, ...]:
    found: list[str] = []
    for tree in RELEASE_USER_TREES:
        root = DOCS / tree
        assert root.is_dir(), f"release-user tree missing: {tree}"
        for path in sorted(root.rglob("*.md")):
            relative = path.relative_to(DOCS).as_posix()
            if relative not in EXEMPT_PAGES:
                found.append(relative)
    for relative in RELEASE_USER_EXTRAS:
        assert (DOCS / relative).is_file(), relative
        found.append(relative)
    return tuple(found)


RELEASE_USER_PAGES = release_user_pages()

_FENCE = re.compile(r"^\s{0,3}(?:```|~~~)")
_INLINE_CODE = re.compile(r"`+([^`]+)`+")


def test_the_guarded_set_is_derived_and_every_exemption_is_justified() -> None:
    """A new operator page must not be able to escape the guard."""
    assert len(RELEASE_USER_PAGES) >= 10, RELEASE_USER_PAGES
    # The install route itself is the page this whole file exists for.
    assert "operate/install.md" in RELEASE_USER_PAGES
    for relative, reason in EXEMPT_PAGES.items():
        assert (DOCS / relative).is_file(), f"exemption names no page: {relative}"
        assert "checkout" in reason, (
            f"{relative}: an exemption must say the page documents working "
            f"from a checkout; nothing else justifies one"
        )
        assert relative not in RELEASE_USER_PAGES


def _copyable_lines(text: str) -> list[tuple[int, str]]:
    """Lines a reader copies a command from.

    Two shapes: a line inside a fenced block, and the inline-code spans of a
    table row. Ordinary prose is exempt, because the pages have to be able to
    NAME the in-checkout form in order to explain it.
    """
    out: list[tuple[int, str]] = []
    inside = False
    for number, line in enumerate(text.splitlines(), 1):
        if _FENCE.match(line):
            inside = not inside
            continue
        if inside:
            out.append((number, line))
        elif line.lstrip().startswith("|"):
            out.extend((number, span) for span in _INLINE_CODE.findall(line))
    return out


def _page(relative: str) -> Path:
    path = DOCS / relative
    assert path.is_file(), f"release-user page missing: {relative}"
    return path


@pytest.mark.parametrize("relative", RELEASE_USER_PAGES)
def test_release_user_commands_are_the_installed_form(relative: str) -> None:
    """No copyable command on these pages carries the repository prefix.

    Prose may name ``uv run okg`` to explain the two forms; a code fence or a
    table cell may not, because that is the text a reader runs.
    """
    offenders = [
        f"{relative}:{number}: {line.strip()}"
        for number, line in _copyable_lines(_page(relative).read_text(encoding="utf-8"))
        if "uv run okg" in line
    ]
    assert offenders == [], (
        "a release user has no uv project; write the command as plain `okg` "
        "and say once in prose that `uv run okg` is the in-checkout form:\n"
        + "\n".join(offenders)
    )


def _declared_python_floor() -> tuple[int, int]:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    requires = metadata["project"]["requires-python"]
    match = re.search(r">=\s*(\d+)\.(\d+)", requires)
    assert match, f"cannot read a floor from requires-python={requires!r}"
    return int(match.group(1)), int(match.group(2))


def test_no_page_states_a_python_floor_the_package_refuses() -> None:
    """A documented floor below ``requires-python`` cannot install at all."""
    major, minor = _declared_python_floor()
    refused = {f"{major}.{lower}" for lower in range(0, minor)}
    offenders: list[str] = []
    for path in sorted(DOCS.rglob("*.md")):
        relative = path.relative_to(DOCS).as_posix()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for version in refused:
                if re.search(rf"Python {re.escape(version)}\b", line):
                    offenders.append(f"{relative}:{number}: {line.strip()}")
    assert offenders == [], (
        f"the package declares requires-python >={major}.{minor}; these lines "
        f"name a version it refuses to install on:\n" + "\n".join(offenders)
    )


def test_the_install_page_states_the_real_interpreter_range() -> None:
    page = _page("operate/install.md").read_text(encoding="utf-8")
    requires = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]["requires-python"]
    assert requires in page, (
        f"docs/operate/install.md must quote the package's own "
        f"requires-python ({requires}) so the two cannot drift apart"
    )


def _documented_chat_manifests() -> list[tuple[str, dict]]:
    """Every fenced YAML block under docs/ that declares a ``chat:`` block."""
    found: list[tuple[str, dict]] = []
    fence = re.compile(r"```ya?ml\n(.*?)```", re.S)
    for path in sorted(DOCS.rglob("*.md")):
        relative = path.relative_to(DOCS).as_posix()
        for body in fence.findall(path.read_text(encoding="utf-8")):
            try:
                payload = yaml.safe_load(body)
            except yaml.YAMLError:
                continue
            if isinstance(payload, dict) and "chat" in payload:
                found.append((relative, payload))
    return found


def test_a_complete_chat_block_is_documented() -> None:
    """The chat verbs refuse a deployment with no block, so one must be shown."""
    enabled = [
        relative
        for relative, payload in _documented_chat_manifests()
        if isinstance(payload["chat"], dict) and payload["chat"].get("enabled")
    ]
    assert enabled, (
        "no page documents an enabled chat: block, and no profile scaffolds "
        "one, so chat setup cannot be started from the documentation"
    )


def test_every_documented_chat_block_parses() -> None:
    """A documented block the loader refuses is worse than none at all."""
    failures: list[str] = []
    for relative, payload in _documented_chat_manifests():
        try:
            parse_chat_block(payload, manifest_path=None)
        except ChatManifestError as exc:
            failures.append(f"{relative}: {exc}")
    assert failures == [], "\n".join(failures)

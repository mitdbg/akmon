"""Literate extraction of the workspace-graph quickstart command blocks.

PACT ``gate-repo-deploy-golden-path`` / ``req.golden.executed-quickstart``:
the golden-path lane MUST source its commands from the canonical quickstart
(``docs/runbooks/workspace-graph/setup.md``) so the doc and the gate cannot
diverge silently. This module is that coupling point: it anchors on the
doc's exact section headers, command spellings, and placeholder tokens, and
raises :class:`QuickstartExtractionError` the moment any of them change.
A failed extraction is a FEATURE — it means the doc moved and the executed
gate must be re-pointed deliberately, not silently skipped.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


# The doc pins this literal DSN (also pinned by
# tests/docs/test_workspace_graph_quickstart.py). The golden-path test
# rewrites it wholesale from OKG_GOLDEN_PATH_DSN so the same test runs
# against the local dev Postgres (5433) and the CI lane's sidecar Postgres
# (5548).
DOC_DSN_LITERAL = "postgresql://postgres:okg@127.0.0.1:5433/<db-name>"

SETUP_SECTION = "## 2. Set Up The Deployment"
VERIFY_SECTION = "## 3. Verify The Graph"
SEARCH_SECTION = "## 4. Try A Search"

# Every placeholder the executed blocks are allowed to contain. Substitution
# must consume all of them; anything left over (including a NEW placeholder
# added to the doc) fails extraction loudly.
KNOWN_PLACEHOLDERS = (
    "<deployment>",
    "<repo-root>",
    "<db-name>",
    "<mcp-port>",
    "<deployment-dir>",
)

_PLACEHOLDER_RE = re.compile(r"<[A-Za-z][A-Za-z0-9_-]*>")  # broad on purpose: new placeholder spellings must fail extraction loudly
_BASH_BLOCK_RE = re.compile(r"```bash\n(.*?)```", re.DOTALL)


class QuickstartExtractionError(AssertionError):
    """The quickstart doc no longer matches the executed-gate anchors."""


@dataclass(frozen=True)
class QuickstartBlocks:
    """Raw (unsubstituted) bash blocks lifted from the quickstart doc."""

    install_block: str
    commit_deployment_block: str
    deployment_env_block: str
    status_block: str
    manual_build_block: str
    search_block: str


def _fail(message: str) -> None:
    raise QuickstartExtractionError(
        f"{message}\n"
        "The canonical quickstart (docs/runbooks/workspace-graph/setup.md) "
        "diverged from the executed golden-path gate "
        "(tests/e2e/test_first_run_golden_path.py). Update the gate's "
        "anchors together with the doc — per PACT "
        "gate-repo-deploy-golden-path the doc and the gate must move in "
        "the same change."
    )


def _section(doc_text: str, header: str) -> str:
    """Return the body of ``header`` up to the next ``## `` heading."""
    if header not in doc_text:
        _fail(f"section header {header!r} not found in the quickstart doc")
    body = doc_text.split(header, 1)[1]
    next_heading = re.search(r"^## ", body, re.MULTILINE)
    return body[: next_heading.start()] if next_heading else body


def _bash_blocks(section_body: str, *, header: str) -> list[str]:
    blocks = _BASH_BLOCK_RE.findall(section_body)
    if not blocks:
        _fail(f"section {header!r} contains no ```bash blocks")
    return blocks


def _single_block(
    blocks: list[str], *, anchors: tuple[str, ...], where: str,
) -> str:
    matches = [b for b in blocks if all(a in b for a in anchors)]
    if len(matches) != 1:
        _fail(
            f"expected exactly one bash block in {where} containing all "
            f"of {list(anchors)}; found {len(matches)}"
        )
    return matches[0]


def extract_quickstart_blocks(doc_text: str) -> QuickstartBlocks:
    """Extract the executed command blocks, failing loudly on drift."""
    setup = _section(doc_text, SETUP_SECTION)
    verify = _section(doc_text, VERIFY_SECTION)
    search = _section(doc_text, SEARCH_SECTION)

    # The gate parameterizes the deployment files location exactly the way
    # the doc instructs operators to (add --deployments-root at install, then
    # export the same root as OKG_DEPLOYMENTS_DIR for later commands). If
    # the doc drops that instruction the gate's parameterization is no
    # longer doc-sanctioned — fail.
    if "--deployments-root" not in setup:
        _fail(
            f"{SETUP_SECTION!r} no longer documents --deployments-root"
        )
    if "OKG_DEPLOYMENTS_DIR" not in setup:
        _fail(
            f"{SETUP_SECTION!r} no longer documents OKG_DEPLOYMENTS_DIR"
        )
    # The canary appends --no-publish exactly because the doc offers it.
    if "--no-publish" not in setup:
        _fail(f"{SETUP_SECTION!r} no longer documents --no-publish")

    setup_blocks = _bash_blocks(setup, header=SETUP_SECTION)
    # `okg install` is the documented door (define-newcomer-entry-path
    # req.entry.migrate-the-other-doors). `okg init` is now an alias of it,
    # and this anchor deliberately holds the doc to the door rather than the
    # alias — a quickstart that drifts back to `init` fails extraction.
    install_block = _single_block(
        setup_blocks,
        anchors=(
            "okg install",
            "--profile workspace-graph",
            "--deployment-name <deployment>",
            "--repo-root <repo-root>",
            DOC_DSN_LITERAL,
            "--mcp-port <mcp-port>",
            "--json",
        ),
        where=SETUP_SECTION,
    )
    commit_deployment_block = _single_block(
        setup_blocks,
        anchors=(
            "DEPLOYMENT_DIR=<deployment-dir>",
            'git -C "$DEPLOYMENT_DIR" init',
            'git -C "$DEPLOYMENT_DIR" add -A',
            'git -C "$DEPLOYMENT_DIR" commit',
        ),
        where=SETUP_SECTION,
    )
    deployment_env_block = _single_block(
        setup_blocks,
        anchors=("okg deployment env <deployment> --json",),
        where=SETUP_SECTION,
    )

    verify_blocks = _bash_blocks(verify, header=VERIFY_SECTION)
    status_block = _single_block(
        verify_blocks,
        anchors=("okg --json status --deployment <deployment>",),
        where=VERIFY_SECTION,
    )
    manual_build_block = _single_block(
        verify_blocks,
        anchors=(
            "okg provision",
            "--deployment <deployment>",
            "--publish-once",
            "--json",
        ),
        where=VERIFY_SECTION,
    )
    search_blocks = _bash_blocks(search, header=SEARCH_SECTION)
    search_block = _single_block(
        search_blocks,
        anchors=(
            "export OKG_NOMOS_SUBJECT=subject:human_operator",
            "okg search",
            "--deployment <deployment>",
        ),
        where=SEARCH_SECTION,
    )

    return QuickstartBlocks(
        install_block=install_block,
        commit_deployment_block=commit_deployment_block,
        deployment_env_block=deployment_env_block,
        status_block=status_block,
        manual_build_block=manual_build_block,
        search_block=search_block,
    )


def substitute_placeholders(block: str, mapping: dict[str, str]) -> str:
    """Fill the doc's placeholders; fail loudly on unknown leftovers.

    ``mapping`` keys are the literal placeholder tokens (``<deployment>``,
    ...). The doc's pinned DSN literal must be rewritten by the caller
    BEFORE the generic tokens (pass it in ``mapping`` too) so the embedded
    ``<db-name>`` never half-substitutes.
    """
    unknown_keys = [
        k for k in mapping
        if k not in KNOWN_PLACEHOLDERS and k != DOC_DSN_LITERAL
    ]
    if unknown_keys:
        _fail(f"substitution map has unknown placeholder keys: {unknown_keys}")

    out = block
    if DOC_DSN_LITERAL in mapping:
        out = out.replace(DOC_DSN_LITERAL, mapping[DOC_DSN_LITERAL])
    for token in KNOWN_PLACEHOLDERS:
        if token in mapping:
            out = out.replace(token, mapping[token])

    leftovers = sorted(set(_PLACEHOLDER_RE.findall(out)))
    if leftovers:
        _fail(
            f"executed block still contains unsubstituted placeholder(s) "
            f"{leftovers} after applying the map — the doc grew a "
            f"placeholder this gate does not know:\n{out}"
        )
    return out

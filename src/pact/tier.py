"""Tier computed from the diff. Declared tiers can only raise it.

This is the only tier rule. `pact check`, the CI `pact-validation` job
(`scripts/ci/verdict_policy.py`) and `scripts/ci/agent_preflight.py` all ask it,
so they cannot disagree about whether a change needs a PACT. The tier depends
on paths and line counts only, never on who authored the change.

- 0: only documentation prose or lockfiles.
- 1: tests only, CI workflow and action files, or fewer than
     ``TIER1_MAX_SOURCE_LINES`` changed source lines. CI files are NOT tier 0:
     a workflow edit can delete the very job that enforces this gate, which is
     a change a second pair of eyes should see, so it costs an approval.
- 2: any other source, schema, migration, or formal change.
- 3: never computed; a program declares it.

The tier is not a label: it is what ``pact check --strict`` charges the
change. The **effective** tier is the higher of the declared and the computed
one, and it decides the gate (``APPROVAL_FROM_TIER``, ``PROOF_FROM_TIER``):

- tier 0 requires neither an approval nor proof;
- tier 1 requires an approval record; unproven requirements are a warning;
- tier 2 and 3 require an approval and bound passing evidence for every
  requirement.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

TIER1_MAX_SOURCE_LINES = 300

#: The lowest effective tier at which strict mode requires an approval.
APPROVAL_FROM_TIER = 1
#: The lowest effective tier at which strict mode requires bound passing
#: evidence for every requirement (below it, unproven requirements warn).
PROOF_FROM_TIER = 2

_INSTRUCTION_BASENAMES = frozenset({"AGENTS.md", "CLAUDE.md", "SKILL.md", "GEMINI.md", "CODEOWNERS"})
_INSTRUCTION_PREFIXES = (".claude/", ".codex/", ".cursor/", ".github/copilot", "docs/runbooks/agents/")
_DOCS_TOP_LEVEL = frozenset({"README.md", "CHANGELOG.md"})
_DOCS_SUFFIXES = (".md", ".rst", ".png", ".svg", ".jpg", ".jpeg", ".gif")
_CI_PREFIXES = (".github/workflows/", ".github/actions/")
_CI_EXACT = frozenset({".github/dependabot.yml", ".github/dependabot.yaml"})
_LOCKFILES = frozenset({"uv.lock", "poetry.lock", "Pipfile.lock", "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "Cargo.lock", "go.sum"})
_TEST_PREFIXES = ("tests/", "test/")
_TIER2_MARKERS = ("migrations/", "/schemas/", "formal/", ".tla", ".lean", "ontolog", "catalog")


def _is_instruction(path: str) -> bool:
    return path.rsplit("/", 1)[-1] in _INSTRUCTION_BASENAMES or path.startswith(_INSTRUCTION_PREFIXES)


def ci_arm(path: str) -> bool:
    """A CI workflow or action definition: tier 1, never tier 0.

    These files decide which checks run at all. Making them free would let a
    pull request delete the `pact-validation` job with no approval, which is
    the gate approving itself away.
    """
    p = path.strip().removeprefix("./")
    if not p or _is_instruction(p):
        return False
    return p in _CI_EXACT or p.startswith(_CI_PREFIXES)


def tier0_arm(path: str) -> str | None:
    p = path.strip().removeprefix("./")
    if not p or _is_instruction(p):
        return None
    if p.startswith(".github/"):
        return None
    if p.rsplit("/", 1)[-1] in _LOCKFILES:
        return "dependency"
    if p in _DOCS_TOP_LEVEL or (p.startswith("docs/") and p.endswith(_DOCS_SUFFIXES)):
        return "docs"
    return None


def is_pact_path(path: str) -> bool:
    return path.startswith("pact/")


@dataclass(frozen=True)
class TierVerdict:
    computed: int
    reason: str
    detail: str | None = None


def compute(changed: Iterable[tuple[str, int]]) -> TierVerdict:
    """``changed`` is (path, changed_lines) pairs, pact/ paths excluded by
    the caller or here. Returns the smallest tier the diff admits."""
    rows = [(p, n) for p, n in changed if not is_pact_path(p)]
    if not rows:
        return TierVerdict(0, "pact_only")
    if all(tier0_arm(p) is not None for p, _ in rows):
        return TierVerdict(0, "tier0_arms")
    if any(any(m in p for m in _TIER2_MARKERS) for p, _ in rows):
        return TierVerdict(2, "schema_or_formal_path")
    source = [(p, n) for p, n in rows if not p.startswith(_TEST_PREFIXES) and tier0_arm(p) is None and not ci_arm(p)]
    if not source:
        return TierVerdict(1, "ci_change" if any(ci_arm(p) for p, _ in rows) else "tests_only")
    lines = sum(n for _, n in source)
    if lines < TIER1_MAX_SOURCE_LINES:
        return TierVerdict(1, "small_source_change", detail=f"{lines} source lines")
    return TierVerdict(2, "source_change", detail=f"{lines} source lines")

"""Markdown parsers for OpenSpec proposal directories.

Four file kinds, each parsed into a focused record set:

- ``proposal.md`` → :class:`ProposalDoc` with title + Why/What/Impact prose
- ``tasks.md`` → list of :class:`TaskGroupRecord` with nested tasks + verifies
- ``design.md`` → :class:`DesignDoc` with decisions, alternatives, goals,
  non-goals, risks, stakeholders
- ``specs/<cap>/spec.md`` → list of :class:`SpecDeltaRecord` with nested
  acceptance criteria + scenarios

Plus the canonical capability spec at ``openspec/specs/<cap>/spec.md``,
which has criteria + scenarios but no SpecDelta wrapper.

Tolerant parsing: unrecognized headers are logged and skipped, never
raised. Parsing one file's badness doesn't break ingest of the rest.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from okg.substrate import fastyaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Frontmatter parsing (added for add-substrate-reasoning-module)
# ---------------------------------------------------------------------------

# Current frontmatter schema version for reasoning content kinds
# (openspec/hypotheses/, openspec/observations/, openspec/insights/).
# Bumped whenever the frontmatter contract changes; source adapter
# rejects unknown values loudly per the reasoning module's R13
# mitigation.
CURRENT_FRONTMATTER_SCHEMA_VERSION = 1


class UnknownFrontmatterSchemaVersionError(ValueError):
    """Raised when a reasoning content file declares an unknown
    frontmatter ``schema_version``.

    The source adapter catches this, logs the error, and marks the
    source run as degraded. No silent stale-frontmatter acceptance.
    """

    def __init__(self, path: Path, version: Any) -> None:
        super().__init__(
            f"unknown frontmatter schema_version={version!r} in {path}"
        )
        self.path = path
        self.version = version


def parse_frontmatter(text: str) -> tuple[dict[str, Any] | None, str]:
    """Split YAML frontmatter from body content.

    Frontmatter lives between two ``---`` lines at the very start of
    the document. Returns ``(frontmatter_dict, body)`` if present,
    ``(None, original_text)`` if no frontmatter or if frontmatter is
    malformed (logged but tolerated — non-reasoning content kinds
    without frontmatter degrade silently to "no frontmatter").
    """
    if not text.startswith("---\n"):
        return None, text
    rest = text[4:]
    # Closing marker may be `\n---\n` (middle of file) or `\n---` at EOF
    end_idx = rest.find("\n---\n")
    if end_idx == -1:
        # Try EOF variant
        eof_idx = rest.rfind("\n---")
        if eof_idx == -1 or eof_idx + 4 != len(rest):
            return None, text
        end_idx = eof_idx
        body_start = len(rest)
    else:
        body_start = end_idx + 5  # past `\n---\n`
    frontmatter_text = rest[:end_idx]
    body = rest[body_start:] if body_start < len(rest) else ""
    try:
        fm = fastyaml.safe_load(frontmatter_text) or {}
    except yaml.YAMLError as e:
        logger.warning("Malformed YAML frontmatter: %s", e)
        return None, text
    if not isinstance(fm, dict):
        return None, text
    return fm, body


@dataclass
class ReasoningProposalDoc:
    """Parsed ``openspec/hypotheses/<slug>/proposal.md`` content.

    Subdirectory shape (matches openspec/changes/<slug>/proposal.md).
    """

    frontmatter: dict[str, Any]
    body: str
    file_path: Path


@dataclass
class ReasoningSingleFileDoc:
    """Parsed ``openspec/observations/<slug>.md`` or
    ``openspec/insights/<slug>.md`` content.

    Single-file shape (no subdirectory).
    """

    frontmatter: dict[str, Any]
    body: str
    file_path: Path


def _parse_reasoning_md(
    path: Path,
) -> tuple[dict[str, Any], str] | None:
    """Shared implementation: read the file, split frontmatter,
    validate ``schema_version``. Returns ``(frontmatter, body)`` or
    ``None`` if the file is empty / has no frontmatter. Raises
    :class:`UnknownFrontmatterSchemaVersionError` if ``schema_version``
    is set but not the current value.
    """
    text = _read(path)
    if not text:
        return None
    fm, body = parse_frontmatter(text)
    if fm is None:
        return None
    sv = fm.get("schema_version")
    if sv != CURRENT_FRONTMATTER_SCHEMA_VERSION:
        raise UnknownFrontmatterSchemaVersionError(path, sv)
    return fm, body.strip()


def parse_reasoning_proposal_md(
    path: Path,
) -> ReasoningProposalDoc | None:
    """Parse a hypothesis ``proposal.md`` file. Returns ``None`` for
    empty or non-frontmatter files; raises
    :class:`UnknownFrontmatterSchemaVersionError` for unknown schema
    versions. The source adapter catches and degrades the run.
    """
    parsed = _parse_reasoning_md(path)
    if parsed is None:
        return None
    fm, body = parsed
    return ReasoningProposalDoc(frontmatter=fm, body=body, file_path=path)


def parse_reasoning_single_file_md(
    path: Path,
) -> ReasoningSingleFileDoc | None:
    """Parse a single-file observation or insight markdown file.
    Same semantics as :func:`parse_reasoning_proposal_md`.
    """
    parsed = _parse_reasoning_md(path)
    if parsed is None:
        return None
    fm, body = parsed
    return ReasoningSingleFileDoc(frontmatter=fm, body=body, file_path=path)


def parse_change_frontmatter(path: Path) -> dict[str, Any]:
    """Extract optional YAML frontmatter from an existing OpenSpec
    change's ``proposal.md``.

    Backward-compatible: existing changes without frontmatter return
    an empty dict. New optional fields like ``hypotheses_tested:`` and
    ``project:`` (per the add-substrate-reasoning-module +
    add-substrate-bookkeeping-module children) are read from here.
    """
    text = _read(path)
    if not text:
        return {}
    fm, _ = parse_frontmatter(text)
    return fm or {}


# ---------------------------------------------------------------------------
# Record types
# ---------------------------------------------------------------------------
@dataclass
class ProposalDoc:
    title: str
    why_text: str
    what_changes_text: str
    impact_text: str


@dataclass
class VerifyRecord:
    text: str


@dataclass
class TaskRecord:
    task_number: str  # e.g. "1.3" or "1.3a"
    description: str
    status: str  # "done" | "pending"
    verifies: list[VerifyRecord] = field(default_factory=list)


@dataclass
class TaskGroupRecord:
    group_index: int
    title: str
    tasks: list[TaskRecord] = field(default_factory=list)


@dataclass
class AlternativeRecord:
    text: str


@dataclass
class DecisionRecord:
    decision_number: str  # e.g. "D1"
    title: str
    rationale: str
    alternatives: list[AlternativeRecord] = field(default_factory=list)


@dataclass
class RiskRecord:
    risk_number: str  # e.g. "R1"
    title: str
    mitigation: str


@dataclass
class DesignDoc:
    context: str = ""
    goals: list[str] = field(default_factory=list)
    non_goals: list[str] = field(default_factory=list)
    decisions: list[DecisionRecord] = field(default_factory=list)
    risks: list[RiskRecord] = field(default_factory=list)
    stakeholders: list[str] = field(default_factory=list)


@dataclass
class ScenarioRecord:
    title: str
    text: str
    index: int


@dataclass
class CriterionRecord:
    title: str
    text: str
    scenarios: list[ScenarioRecord] = field(default_factory=list)


@dataclass
class SpecDeltaRecord:
    delta_kind: str  # "added" | "modified" | "removed" | "renamed"
    criteria: list[CriterionRecord] = field(default_factory=list)


@dataclass
class CapabilityDoc:
    purpose: str = ""
    criteria: list[CriterionRecord] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        logger.warning("Cannot read %s: %s", path, e)
        return ""


def _slugify(text: str) -> str:
    """Convert a header line to a stable slug for use in node IDs."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"\s+", "-", slug.strip())
    return slug[:80] or "untitled"


def _section_split(text: str, header_pat: re.Pattern) -> list[tuple[str, str]]:
    """Split markdown text by top-level header matches.

    Returns list of (header_match_string, body_until_next_match).
    """
    matches = list(header_pat.finditer(text))
    out = []
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        out.append((m.group(0).strip(), text[start:end].strip()))
    return out


# ---------------------------------------------------------------------------
# proposal.md
# ---------------------------------------------------------------------------
_PROPOSAL_TITLE_RE = re.compile(r"^# Change:\s*(.+?)\s*$", re.MULTILINE)
_PROPOSAL_SECTION_RE = re.compile(r"^## (Why|What Changes|Impact)\s*$", re.MULTILINE)


def parse_proposal_md(path: Path) -> ProposalDoc | None:
    text = _read(path)
    return parse_proposal_text(text, fallback_title=path.parent.name)


def parse_proposal_text(
    text: str,
    *,
    fallback_title: str = "",
) -> ProposalDoc | None:
    """Parse proposal bytes already captured by an owning source snapshot."""
    if not text:
        return None
    title_m = _PROPOSAL_TITLE_RE.search(text)
    title = title_m.group(1).strip() if title_m else fallback_title

    sections = {
        name: body for name, body in
        ((m.group(0).removeprefix("## ").strip(), body) for m, body in zip(
            _PROPOSAL_SECTION_RE.finditer(text),
            _section_bodies(text, _PROPOSAL_SECTION_RE),
        ))
    }
    return ProposalDoc(
        title=title,
        why_text=sections.get("Why", "")[:5000],
        what_changes_text=sections.get("What Changes", "")[:5000],
        impact_text=sections.get("Impact", "")[:5000],
    )


def _section_bodies(text: str, pat: re.Pattern) -> list[str]:
    matches = list(pat.finditer(text))
    bodies = []
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        bodies.append(text[start:end].strip())
    return bodies


# ---------------------------------------------------------------------------
# tasks.md
# ---------------------------------------------------------------------------
_GROUP_HEADER_RE = re.compile(r"^## (\d+)\.\s+(.+?)\s*$", re.MULTILINE)
_TASK_LINE_RE = re.compile(r"^-\s+\[([\sxX])\]\s+(\d+\.\d+[a-z]?)\s+(.+?)\s*$", re.MULTILINE)
_VERIFY_LINE_RE = re.compile(
    r"^\s+(?:-\s+)?\*\*Verify\*\*[:\s]*\s*(.+?)\s*$",
    re.MULTILINE,
)


def parse_tasks_md(path: Path) -> list[TaskGroupRecord]:
    text = _read(path)
    if not text:
        return []
    sections = _section_split(text, _GROUP_HEADER_RE)
    groups: list[TaskGroupRecord] = []
    for header, body in sections:
        m = _GROUP_HEADER_RE.match(header)
        if not m:
            continue
        group_idx = int(m.group(1))
        title = m.group(2).strip()
        tasks = _parse_tasks_in_group(body)
        groups.append(TaskGroupRecord(group_index=group_idx, title=title, tasks=tasks))
    return groups


def _parse_tasks_in_group(body: str) -> list[TaskRecord]:
    """Parse `- [ ] N.M description` items plus their nested **Verify** bullets."""
    tasks: list[TaskRecord] = []
    # Find each task and the body until the next task
    matches = list(_TASK_LINE_RE.finditer(body))
    for i, m in enumerate(matches):
        check, num, desc = m.group(1), m.group(2), m.group(3).strip()
        status = "done" if check.lower() == "x" else "pending"
        # Body for this task = lines between this match and the next task
        task_body_start = m.end()
        task_body_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        task_body = body[task_body_start:task_body_end]
        verifies = [VerifyRecord(text=vm.group(1).strip()) for vm in _VERIFY_LINE_RE.finditer(task_body)]
        tasks.append(
            TaskRecord(
                task_number=num,
                description=desc,
                status=status,
                verifies=verifies,
            )
        )
    return tasks


# ---------------------------------------------------------------------------
# design.md
# ---------------------------------------------------------------------------
_DESIGN_TOP_HEADER_RE = re.compile(r"^## (Context|Goals\s*/\s*Non-Goals|Decisions|Risks|Stakeholders)\s*$", re.MULTILINE)
_DECISION_RE = re.compile(r"^### (D\d+)[:.\s]+(.+?)\s*$", re.MULTILINE)
_RISK_RE = re.compile(r"^### (R\d+)[:.\s]+(.+?)\s*$", re.MULTILINE)
_ALT_RE = re.compile(r"\*\*Alternative\*\*[:\s]*(.+?)(?=\n\*\*|\n###|\n##|$)", re.DOTALL)
_BULLET_RE = re.compile(r"^-\s+(.+?)\s*$", re.MULTILINE)
_GOALS_HEADER_RE = re.compile(r"^### Goals\s*$", re.MULTILINE)
_NONGOALS_HEADER_RE = re.compile(r"^### Non-Goals\s*$", re.MULTILINE)
_STAKEHOLDERS_HEADER_RE = re.compile(r"^### Stakeholders\s*$", re.MULTILINE)


def parse_design_md(path: Path) -> DesignDoc | None:
    text = _read(path)
    if not text:
        return None

    doc = DesignDoc()

    # Decisions: scattered across the file (some may be inside a "## Decisions"
    # parent section, but D-headers are unique enough to find directly)
    for m in _DECISION_RE.finditer(text):
        num, title = m.group(1), m.group(2).strip()
        body = _body_until_next_section(text, m.end(), [
            _DECISION_RE, _RISK_RE, _DESIGN_TOP_HEADER_RE,
        ])
        # Extract any **Alternative**: blocks within the decision body
        alts = [
            AlternativeRecord(text=am.group(1).strip()[:1000])
            for am in _ALT_RE.finditer(body)
        ]
        # Rationale = body minus alternative blocks
        rationale = _ALT_RE.sub("", body).strip()[:2000]
        doc.decisions.append(
            DecisionRecord(
                decision_number=num,
                title=title,
                rationale=rationale,
                alternatives=alts,
            )
        )

    # Risks
    for m in _RISK_RE.finditer(text):
        num, title = m.group(1), m.group(2).strip()
        body = _body_until_next_section(text, m.end(), [
            _RISK_RE, _DECISION_RE, _DESIGN_TOP_HEADER_RE,
        ])
        doc.risks.append(
            RiskRecord(risk_number=num, title=title, mitigation=body[:2000])
        )

    # Goals / Non-Goals / Stakeholders are bullet lists under their headers
    doc.goals = _bullets_under(text, _GOALS_HEADER_RE)
    doc.non_goals = _bullets_under(text, _NONGOALS_HEADER_RE)
    doc.stakeholders = _bullets_under(text, _STAKEHOLDERS_HEADER_RE)

    return doc


def _body_until_next_section(text: str, start: int, terminators: list[re.Pattern]) -> str:
    """Return text from `start` until the earliest match of any terminator pattern."""
    end = len(text)
    for pat in terminators:
        m = pat.search(text, start)
        if m and m.start() < end:
            end = m.start()
    return text[start:end].strip()


def _bullets_under(text: str, header_pat: re.Pattern) -> list[str]:
    """Find a header, then collect bullet lines (`- ...`) until the next header."""
    m = header_pat.search(text)
    if not m:
        return []
    body_start = m.end()
    # Stop at the next ### or ## header
    next_header = re.search(r"^##+\s", text[body_start:], re.MULTILINE)
    body = text[body_start:body_start + next_header.start()] if next_header else text[body_start:]
    # Pull bullet items
    return [bm.group(1).strip()[:500] for bm in _BULLET_RE.finditer(body)]


# ---------------------------------------------------------------------------
# specs/<cap>/spec.md (delta form, inside a change directory)
# ---------------------------------------------------------------------------
_DELTA_HEADER_RE = re.compile(
    r"^## (ADDED|MODIFIED|REMOVED|RENAMED) Requirements\s*$",
    re.MULTILINE | re.IGNORECASE,
)
_REQUIREMENT_RE = re.compile(r"^### Requirement:\s*(.+?)\s*$", re.MULTILINE)
_SCENARIO_RE = re.compile(r"^#### Scenario:\s*(.+?)\s*$", re.MULTILINE)


def parse_change_spec_md(path: Path) -> list[SpecDeltaRecord]:
    """Parse a delta-form spec.md (inside changes/<id>/specs/<cap>/spec.md)."""
    text = _read(path)
    if not text:
        return []
    deltas: list[SpecDeltaRecord] = []
    sections = _section_split(text, _DELTA_HEADER_RE)
    for header, body in sections:
        m = _DELTA_HEADER_RE.match(header)
        if not m:
            continue
        kind = m.group(1).lower()
        criteria = _parse_criteria_block(body)
        deltas.append(SpecDeltaRecord(delta_kind=kind, criteria=criteria))
    return deltas


def parse_canonical_spec_md(path: Path) -> CapabilityDoc | None:
    """Parse a canonical capability spec at openspec/specs/<cap>/spec.md.

    Format: optional `## Purpose` paragraph, then `### Requirement:` blocks
    each followed by `#### Scenario:` blocks. No SpecDelta wrapper.
    """
    text = _read(path)
    if not text:
        return None
    purpose_m = re.search(r"^## Purpose\s*\n(.+?)(?=^##+\s|\Z)", text, re.MULTILINE | re.DOTALL)
    purpose = purpose_m.group(1).strip()[:2000] if purpose_m else ""
    criteria = _parse_criteria_block(text)
    return CapabilityDoc(purpose=purpose, criteria=criteria)


def _parse_criteria_block(body: str) -> list[CriterionRecord]:
    """Find all `### Requirement:` blocks in `body` and parse their scenarios."""
    criteria: list[CriterionRecord] = []
    matches = list(_REQUIREMENT_RE.finditer(body))
    for i, m in enumerate(matches):
        title = m.group(1).strip()
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        crit_body = body[body_start:body_end]
        # Body until first scenario = the SHALL prose
        first_scenario = _SCENARIO_RE.search(crit_body)
        if first_scenario:
            shall_text = crit_body[:first_scenario.start()].strip()
            scenarios_text = crit_body[first_scenario.start():]
        else:
            shall_text = crit_body.strip()
            scenarios_text = ""
        scenarios = _parse_scenarios(scenarios_text)
        criteria.append(
            CriterionRecord(title=title, text=shall_text[:5000], scenarios=scenarios)
        )
    return criteria


def _parse_scenarios(body: str) -> list[ScenarioRecord]:
    matches = list(_SCENARIO_RE.finditer(body))
    out: list[ScenarioRecord] = []
    for i, m in enumerate(matches):
        title = m.group(1).strip()
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        scenario_body = body[body_start:body_end].strip()
        out.append(ScenarioRecord(title=title, text=scenario_body[:2000], index=i))
    return out


# ---------------------------------------------------------------------------
# Path mention extraction (for cross-module REFERENCES → SourceFile)
# ---------------------------------------------------------------------------
_AT_PATH_RE = re.compile(r"@/([\w./_-]+)")
_LINK_PATH_RE = re.compile(r"\]\(([\w./_-]+\.[\w]+)\)")
_TRAILING_PUNCT = ".,;:!?)"


def extract_path_mentions(text: str) -> list[str]:
    """Find @/path/to/file and [text](path/to/file.ext) mentions."""
    seen: set[str] = set()
    out: list[str] = []
    for m in _AT_PATH_RE.finditer(text):
        p = m.group(1).rstrip(_TRAILING_PUNCT)
        if p not in seen and "/" in p:  # require at least one slash to avoid noise
            seen.add(p)
            out.append(p)
    for m in _LINK_PATH_RE.finditer(text):
        p = m.group(1)
        if p.startswith(("http://", "https://", "#")):
            continue
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# ---------------------------------------------------------------------------
# File hashing for incremental crawl
# ---------------------------------------------------------------------------
def file_sha256(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()

"""Canonical evidence-kind registry.

Per `polish-pact-agent-ux` req.polish.evidence-kinds: evidence kinds were
free strings, so the corpus forked into near-duplicate vocabularies
(pytest/executed_test, graph/graph_report, ...) and `evidence_policy`
matching was exact-string. This module is the single source: the canonical
kinds, the alias map, and the expansion used by policy matching.
`pact/project.yaml` may extend both under an `evidence_kinds:` block;
defaults live here so bare roots still validate. The merge is extend-only:
defaults cannot be removed by project config.

`manual` is deliberately NOT an alias of `manual_visual`: `manual_visual`
is a named reviewer's approval of rendered visual output (the UI gate),
while `manual` is a by-hand non-visual check. Folding them would let a
command transcript satisfy a visual-approval policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

# Canonical vocabulary. Aliases below fold the observed near-duplicates
# into these; anything outside registry+aliases draws an
# `unknown_evidence_kind` validation ERROR (pact-vacuous-gate-close
# req.vacuous.presence): an unregistered kind resolves to no policy slot, so
# a row carrying it can never be proof. Project-observed kinds are adopted by
# extending `pact/project.yaml evidence_kinds`, not by weakening the check.
DEFAULT_CANONICAL_KINDS: frozenset[str] = frozenset({
    "planned_test",
    "executed_test",
    "formal_check",
    "graph_report",
    "published_generation",
    "benchmark_report",
    "design_review",
    "operator_report",
    "manual",
    "manual_visual",
    "ui_visual",
    "ui_interaction",
    "ui_quality_gate",
    "loop_record",
    "waiver",
    # Written by `okg pact evidence approve` (operations.approve_intent): the
    # reviewer's approval of the change's intent digest. It lives in the CODE
    # defaults, not only in a project's registry block, because a verb must
    # never emit a row the validator refuses in a bare root — the round-trip
    # `pact-vacuous-gate-close` req.vacuous.presence requires. Its own kind
    # rather than an alias of `design_review`: an approval record must not
    # stand in for a reviewed design.
    "intent_review",
})

# alias -> canonical. Folds the near-duplicate pairs found in the corpus
# audit plus the historical test/formal spellings the matcher already
# special-cased inline.
DEFAULT_ALIASES: dict[str, str] = {
    "pytest": "executed_test",
    "junit": "executed_test",
    "test": "executed_test",
    "formal": "formal_check",
    "graph": "graph_report",
    "benchmark": "benchmark_report",
    "review": "design_review",
    "report": "operator_report",
}


@dataclass(frozen=True)
class EvidenceKindRegistry:
    canonical: frozenset[str]
    aliases: dict[str, str]
    # Populated when the project.yaml `evidence_kinds:` block is malformed
    # or unreadable; validation surfaces these as warnings so a broken
    # registry edit is never a silent fallback to defaults.
    config_errors: tuple[str, ...] = field(default=())

    def canonicalize(self, kind: str) -> str:
        return self.aliases.get(kind, kind)

    def is_known(self, kind: str) -> bool:
        return kind in self.canonical or kind in self.aliases

    def expand(self, kind: str) -> set[str]:
        """Every spelling equivalent to `kind`: its canonical form plus all
        aliases that resolve to the same canonical form."""
        canon = self.canonicalize(kind)
        return {canon} | {
            alias for alias, target in self.aliases.items() if target == canon
        }


DEFAULT_REGISTRY = EvidenceKindRegistry(
    canonical=DEFAULT_CANONICAL_KINDS,
    aliases=dict(DEFAULT_ALIASES),
)


def _registry_from_settings(settings: dict) -> EvidenceKindRegistry:
    block = settings.get("evidence_kinds")
    if block is None:
        return DEFAULT_REGISTRY
    errors: list[str] = []
    if not isinstance(block, dict):
        return EvidenceKindRegistry(
            canonical=DEFAULT_CANONICAL_KINDS,
            aliases=dict(DEFAULT_ALIASES),
            config_errors=(
                "evidence_kinds must be a mapping with `canonical` and/or "
                f"`aliases`; got {type(block).__name__}",
            ),
        )
    canonical = set(DEFAULT_CANONICAL_KINDS)
    extra_canonical = block.get("canonical", [])
    if isinstance(extra_canonical, list):
        canonical.update(str(k) for k in extra_canonical)
    elif extra_canonical:
        errors.append(
            "evidence_kinds.canonical must be a list; got "
            f"{type(extra_canonical).__name__} (entry ignored)"
        )
    aliases = dict(DEFAULT_ALIASES)
    extra_aliases = block.get("aliases", {})
    if isinstance(extra_aliases, dict):
        aliases.update({str(a): str(c) for a, c in extra_aliases.items()})
    elif extra_aliases:
        errors.append(
            "evidence_kinds.aliases must be a mapping; got "
            f"{type(extra_aliases).__name__} (entry ignored)"
        )
    return EvidenceKindRegistry(
        canonical=frozenset(canonical),
        aliases=aliases,
        config_errors=tuple(errors),
    )


@lru_cache(maxsize=128)
def _load_registry_cached(repo_root: str, mtime_ns: int) -> EvidenceKindRegistry:
    del mtime_ns  # cache key only: a project.yaml edit invalidates the entry
    from okg.substrate.pact.project_settings import load_project_settings

    try:
        settings = load_project_settings(Path(repo_root))
    except Exception as exc:  # noqa: BLE001 - surfaced as config_errors
        return EvidenceKindRegistry(
            canonical=DEFAULT_CANONICAL_KINDS,
            aliases=dict(DEFAULT_ALIASES),
            config_errors=(
                f"pact/project.yaml unreadable ({exc}); evidence-kind "
                "registry running on code defaults",
            ),
        )
    return _registry_from_settings(settings if isinstance(settings, dict) else {})


def load_registry(repo_root: Path | str | None) -> EvidenceKindRegistry:
    """The active registry: project.yaml extensions merged over defaults.

    Cached per (path, project.yaml mtime) so long-running processes (the
    runtime worker projecting PACT alignment every sync cycle) see registry
    edits without a restart. A missing project.yaml is the documented
    default, not a fallback.
    """
    if repo_root is None:
        return DEFAULT_REGISTRY
    root = Path(repo_root).resolve()
    settings_path = root / "pact" / "project.yaml"
    try:
        mtime_ns = settings_path.stat().st_mtime_ns
    except OSError:
        return DEFAULT_REGISTRY
    return _load_registry_cached(str(root), mtime_ns)

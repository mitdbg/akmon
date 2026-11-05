"""Turn a proposal into deployment files for one experiment branch.

Two kinds of change, both written as ordinary deployment files so they go
through the normal publish path:

* ``text``: a ``surface_text`` branch layer (issue #4280) keyed by the
  experiment branch name. The deployment's ``agent_surface.surface_text``
  is added when missing. Served at serve time; the default branch is
  unchanged because the layer applies only to its own branch.
* ``link``: one declarative cross-link rule (``library/linkers/rules.py``),
  a ``DeclarativeLinker`` enricher entry that loads only that rule, and the
  edge narrowing the rule needs, in a bridges schema file. Materialized by
  the publisher's enricher stage over records already in the graph.

Every proposal is validated with the same parser the server or the linker
uses before a file is written, and refused when its new text contains
words copied from the answer key (``leak_check``).
"""
from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from okg.substrate import fastyaml

KINDS = ("text", "link")
#: Files the proposer may create or change, relative to the deployment root.
SURFACE_TEXT_FILE = "surface_text.yaml"
RULES_DIR = "optimizer_rules"
BRIDGE_PREFIX = "schemas/bridges/optimizer_"


class ProposalRefused(ValueError):
    """The proposal cannot be written as given; the message says why."""


@dataclass(frozen=True)
class Proposal:
    kind: str
    branch: str
    #: For ``text``: the layer {tools?, params?, fix_hints?}. For ``link``:
    #: {rule: <one cross-link rule>, narrowing: {src, edge, dst}}.
    change: Mapping[str, Any]
    stage: str
    purpose: str  # "accuracy" or "cost"
    targets: tuple[str, ...] = ()
    expected_effect: str = ""
    rationale: str = ""
    extra: Mapping[str, Any] = field(default_factory=dict)


_BRANCH = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")


def _slug(branch: str) -> str:
    if not _BRANCH.fullmatch(branch):
        raise ProposalRefused(f"branch {branch!r} must match {_BRANCH.pattern}")
    return branch.replace("-", "_")


# ── leaks ──────────────────────────────────────────────────────────────


def _tokens(text: str) -> list[str]:
    return [t.rstrip("._/-") for t in re.findall(r"[a-z0-9][a-z0-9._/-]*", (text or "").lower())]


def _ngrams(text: str, n: int) -> set[tuple[str, ...]]:
    words = _tokens(text)
    return {tuple(words[i:i + n]) for i in range(len(words) - n + 1)}


def leak_check(new_text: str, answer_key: Iterable[str], *, n: int = 4) -> list[str]:
    """Answer-key phrases of ``n`` words that also appear in ``new_text``.

    A served text that repeats the answer key turns the exam into its own
    answer sheet, so any overlap refuses the proposal. Tokens that look
    like ids or numbers count on their own (a ticket id or a value from a
    required fact is a leak even inside a short phrase).
    """
    found: list[str] = []
    have = _ngrams(new_text, n)
    tokens = set(_tokens(new_text))
    for text in answer_key:
        for gram in _ngrams(text, n) & have:
            found.append(" ".join(gram))
        for tok in _tokens(text):
            # Ids and values: a token with a digit (3+ characters), or a hyphenated token that
            # also holds a digit (a ticket key); plain hyphenated words (read-only) are not leaks.
            if tok in tokens and len(tok) >= 3 and any(c.isdigit() for c in tok):
                found.append(tok)
    return sorted(set(found))


def _texts(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for v in value.values():
            yield from _texts(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _texts(v)


# ── text ───────────────────────────────────────────────────────────────


def _manifest(root: Path) -> tuple[Path, str]:
    path = root / "deployment.yaml"
    return path, path.read_text(encoding="utf-8")


def _child_indent(lines: Sequence[str], at: int) -> str:
    """The indentation of the first non-blank, non-comment line under line ``at``."""
    for line in lines[at + 1:]:
        stripped = line.lstrip(" ")
        if not stripped.strip() or stripped.startswith("#"):
            continue
        indent = line[: len(line) - len(stripped)]
        return indent if indent else "  "
    return "  "


def _insert_under(manifest: str, block: str, text_at_indent: Callable[[str], str]) -> str:
    """Insert lines right under the top-level ``block:`` line, at its children's indentation."""
    lines = manifest.splitlines(keepends=True)
    at = next((i for i, line in enumerate(lines) if line.rstrip() == f"{block}:"), None)
    if at is None:
        raise ProposalRefused(f"`{block}:` is not a plain top-level block; change it by hand")
    lines.insert(at + 1, text_at_indent(_child_indent(lines, at)))
    return "".join(lines)


def _check_against_surface(layer: Mapping[str, Any], surface: Mapping[str, Any] | None) -> None:
    """Refuse names the server would refuse at launch: unpublished tools or arguments, unknown codes."""
    from okg.substrate.mcp import surface_text as st

    if surface is not None:
        tools = {t["name"]: set(((t.get("input_schema") or {}).get("properties") or {})) for t in surface.get("tools") or ()}
        bad = [n for n in (layer.get("tools") or {}) if n not in tools]
        for key in layer.get("params") or {}:
            tool, _, arg = key.partition(".")
            if arg not in tools.get(tool, set()):
                bad.append(key)
        if bad:
            raise ProposalRefused(f"not published by this server: {sorted(bad)}; it publishes {sorted(tools)}")
    import okg.substrate.mcp.server  # noqa: F401 - loads the modules that declare refusal codes

    try:
        st.check_fix_hint_codes(st.EffectiveSurfaceText(fix_hints=dict(layer.get("fix_hints") or {})))
    except st.SurfaceTextInvalid as exc:
        raise ProposalRefused(exc.message) from exc


def apply_text(root: Path, proposal: Proposal, surface: Mapping[str, Any] | None = None) -> list[str]:
    from okg.substrate.mcp.surface_text import parse_surface_text

    layer = dict(proposal.change)
    unknown = sorted(set(layer) - {"tools", "params", "fix_hints"})
    if unknown or not layer:
        raise ProposalRefused(f"a text change holds tools, params and/or fix_hints; got {sorted(layer)}")
    _check_against_surface(layer, surface)
    manifest_path, manifest = _manifest(root)
    raw = fastyaml.safe_load(manifest) or {}
    block = raw.get("agent_surface")
    if not isinstance(block, dict):
        raise ProposalRefused("the deployment declares no agent_surface block")
    name = block.get("surface_text") or SURFACE_TEXT_FILE
    path = root / name
    doc = (fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}) if path.is_file() else {}
    branches = dict(doc.get("branches") or {})
    branches[proposal.branch] = layer
    doc = {**doc, "branches": branches}
    parse_surface_text(doc, where=str(path))  # refuses anything the server's parser would refuse
    changed = [name]
    new_manifest = None
    if not block.get("surface_text"):
        new_manifest = _insert_under(manifest, "agent_surface", lambda ind: f"{ind}surface_text: {name}\n")
        reparsed = fastyaml.safe_load(new_manifest) or {}
        if (reparsed.get("agent_surface") or {}).get("surface_text") != name or \
                {k: v for k, v in reparsed["agent_surface"].items() if k != "surface_text"} != block:
            raise ProposalRefused("adding agent_surface.surface_text would change the manifest's meaning")
        changed.append("deployment.yaml")
    path.write_text(yaml.safe_dump(doc, sort_keys=True), encoding="utf-8")
    if new_manifest is not None:
        manifest_path.write_text(new_manifest, encoding="utf-8")
    return changed


# ── link ───────────────────────────────────────────────────────────────


def _narrowing_doc(slug: str, src: str, edge: str, dst: str) -> str:
    return (
        f"# Written by the optimizer proposer for experiment branch {slug}.\n"
        f"id: https://okg.dev/optimizer/bridges/{slug}\n"
        f"name: optimizer_{slug}_narrowings\n"
        "prefixes:\n  linkml: https://w3id.org/linkml/\n"
        "default_range: string\nimports:\n  - linkml:types\n"
        "classes:\n  EdgeNarrowings:\n    annotations:\n      okg_edge_narrowings: \"true\"\n"
        "    attributes:\n"
        f"      {slug}_{src.lower()}_{edge}_{dst.lower()}:\n"
        f"        annotations:\n          edge_archetype: {edge}\n"
        f"          src_subtypes: {src}\n          dst_subtypes: {dst}\n"
    )


def apply_link(root: Path, proposal: Proposal) -> list[str]:
    from okg.substrate.library.linkers.rules import RuleParseError, load_rules

    slug = _slug(proposal.branch)
    rule = proposal.change.get("rule")
    narrowing = proposal.change.get("narrowing")
    if not isinstance(rule, Mapping) or not isinstance(narrowing, Mapping):
        raise ProposalRefused("a link change holds `rule` and `narrowing`")
    try:
        parsed = load_rules({"rules": [dict(rule)]}, source="proposal")
    except RuleParseError as exc:
        raise ProposalRefused(f"the rule does not parse: {exc}") from exc
    src, edge, dst = (str(narrowing.get(k) or "") for k in ("src", "edge", "dst"))
    if not all(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", x) for x in (src, edge, dst)):
        raise ProposalRefused("narrowing src, edge and dst must be plain names")
    if edge != parsed[0].edge:
        raise ProposalRefused(f"narrowing edge {edge!r} is not the rule's edge {parsed[0].edge!r}")
    manifest_path, manifest = _manifest(root)
    before = (fastyaml.safe_load(manifest) or {}).get("enrichers")
    if not isinstance(before, list):
        raise ProposalRefused("the deployment has no top-level enrichers: list")
    entry = {"class": "okg.substrate.library.linkers.declarative.DeclarativeLinker",
             "config": {"name": f"optimizer_{slug}", "rules_dir": f"{RULES_DIR}/{slug}"}}
    new_manifest = _insert_under(manifest, "enrichers", lambda ind: (
        f"{ind}- class: {entry['class']}\n{ind}  config:\n{ind}    name: {entry['config']['name']}\n"
        f"{ind}    rules_dir: {entry['config']['rules_dir']}\n"))
    after = (fastyaml.safe_load(new_manifest) or {}).get("enrichers")
    if after != [entry, *before]:
        raise ProposalRefused("adding the enricher entry would change the manifest's meaning")
    # Every check passed: only now is anything written.
    rules_dir = root / RULES_DIR / slug
    rules_dir.mkdir(parents=True, exist_ok=True)
    (rules_dir / "rule.yaml").write_text(yaml.safe_dump({"rules": [dict(rule)]}, sort_keys=True),
                                          encoding="utf-8")
    (root / f"{BRIDGE_PREFIX}{slug}.yaml").write_text(_narrowing_doc(slug, src, edge, dst), encoding="utf-8")
    manifest_path.write_text(new_manifest, encoding="utf-8")
    return [f"{RULES_DIR}/{slug}/rule.yaml", f"{BRIDGE_PREFIX}{slug}.yaml", "deployment.yaml"]


def apply(root: Path, proposal: Proposal, *, answer_key: Iterable[str] = (),
          surface: Mapping[str, Any] | None = None) -> list[str]:
    """Write ``proposal`` into the deployment at ``root``; the files changed.

    The leak check covers everything the proposal will put on record (the
    change, its rationale and expected effect) and runs before any write.
    """
    if proposal.kind not in KINDS:
        raise ProposalRefused(f"kind must be one of {KINDS}")
    text = "\n".join([*_texts(proposal.change), proposal.rationale, proposal.expected_effect])
    leaks = leak_check(text, answer_key)
    if leaks:
        raise ProposalRefused(f"the proposal repeats the answer key: {leaks[:5]}")
    root = Path(root)
    return apply_text(root, proposal, surface) if proposal.kind == "text" else apply_link(root, proposal)


__all__ = ["BRIDGE_PREFIX", "KINDS", "Proposal", "ProposalRefused", "RULES_DIR", "SURFACE_TEXT_FILE", "apply",
           "leak_check"]

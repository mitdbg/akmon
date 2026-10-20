"""CrossLinkRule schema + loader for the declarative cross-source
linker.

A rule has the shape (after parse):

    CrossLinkRule(
        id="commit_message_subject_starts_with_change_slug",
        src=RuleEndpoint(
            subtypes=("git_commit",),
            field="message_subject",
            fields=None,
            via_edge=None,
        ),
        dst=RuleEndpoint(
            subtypes=("open_spec_change",),
            field="change_id",
            fields=None,
            via=None,
            derive_from_path=None,
        ),
        matcher="subject_prefix",
        edge="references",
        bidirectional=True,
    )

The substrate ships exactly five matcher names — `exact`,
`subject_prefix`, `word_boundary`, `derive_from_path`,
`alias_table` — and the loader rejects unknown names at
construction time so a typo fails loudly.

Per `declarative-cross-source-linker` (2026-04-26).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


__all__ = [
    "CrossLinkRule",
    "RuleEndpoint",
    "ViaEdge",
    "DeriveFromPath",
    "UniqueFieldFallback",
    "MATCHER_NAMES",
    "load_rules_file",
    "load_rules",
    "RuleParseError",
]


MATCHER_NAMES: tuple[str, ...] = (
    "exact",
    "subject_prefix",
    "word_boundary",
    "derive_from_path",
    "alias_table",
)


class RuleParseError(SubstrateError):
    code = "cross_link_rule_parse_error"


@dataclass(frozen=True)
class ViaEdge:
    edge_type: str
    dst_subtype: str


@dataclass(frozen=True)
class DeriveFromPath:
    """Derive a dst lookup key from a regex capture against `attr`.

    Modes:
      - `multi_capture: false` (DEFAULT, backward-compatible):
        Applies the pattern with `re.match()`. The regex is
        **anchored at the start** of `attr`'s value. If your tag
        can appear mid-string (e.g., in a multi-line docstring
        after a newline), prepend `(?s).*` to allow `.` to span
        newlines and shift the anchor past the prefix. ONE match
        per source — if multiple `@implements_concept:` lines
        appear in one docstring, only the LAST one wins (the
        greedy `.*` swallows earlier ones).
      - `multi_capture: true`: Applies the pattern with
        `re.findall()` over the whole `attr` value. ALL matches
        emit edges. Do NOT include `(?s).*` prefix — `findall`
        scans the whole string; the prefix would inadvertently
        constrain to one capture. Use `(?s)` standalone if your
        tag spans newlines.

    Rules using this matcher:
      - `tool_use_event_runs_openspec_command_for_change.yaml`
      - `code_symbol_implements_concept.yaml` (multi_capture)
      - `coordination_to_reasoning.yaml`
      - `sessions_to_openspec.yaml`
    """
    attr: str
    regex: str
    capture_field: str
    multi_capture: bool = False


@dataclass(frozen=True)
class UniqueFieldFallback:
    """Resolve an alias miss by one attribute, but only when unambiguous.

    Only an `alias_table` dst may declare it. A source value that no alias
    matches links to the single live `subtype` node whose `field` equals it;
    when two or more nodes carry that value, nothing links -- one is never
    picked. Issue #1747 uses it so a bare code-symbol name in a document still
    links once `qualified_name` is no longer an alias.
    """
    subtype: str
    field: str


@dataclass(frozen=True)
class RuleEndpoint:
    subtypes: tuple[str, ...]
    field: Optional[str] = None
    fields: Optional[tuple[str, ...]] = None
    via_edge: Optional[ViaEdge] = None
    via: Optional[str] = None
    derive_from_path: Optional[DeriveFromPath] = None
    unique_field_fallback: Optional[UniqueFieldFallback] = None


@dataclass(frozen=True)
class CrossLinkRule:
    id: str
    src: RuleEndpoint
    dst: RuleEndpoint
    matcher: str
    edge: str = "references"
    bidirectional: bool = True


def _coerce_subtypes(value: Any, *, where: str) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(s, str) for s in value):
        return tuple(value)
    raise RuleParseError(
        f"{where}.subtype must be a string or list of strings",
        fix_hint="set `subtype: foo` or `subtype: [foo, bar]`",
    )


def _parse_endpoint(
    raw: dict[str, Any], *, where: str, allow_via: bool,
) -> RuleEndpoint:
    if not isinstance(raw, dict):
        raise RuleParseError(
            f"{where} must be a mapping; got {type(raw).__name__}",
        )
    subtypes: tuple[str, ...] = ()
    if "subtype" in raw:
        subtypes = _coerce_subtypes(raw["subtype"], where=where)
    elif "via" not in raw:
        raise RuleParseError(
            f"{where} must declare `subtype` (or `via: alias_table` "
            f"on dst)",
        )
    field = raw.get("field")
    fields_raw = raw.get("fields")
    fields: Optional[tuple[str, ...]] = None
    if fields_raw is not None:
        if not isinstance(fields_raw, list) or not all(
            isinstance(f, str) for f in fields_raw
        ):
            raise RuleParseError(
                f"{where}.fields must be a list of strings",
            )
        fields = tuple(fields_raw)
    via_edge: Optional[ViaEdge] = None
    if "via_edge" in raw:
        ve = raw["via_edge"]
        if (
            not isinstance(ve, dict)
            or "edge_type" not in ve
            or "dst_subtype" not in ve
        ):
            raise RuleParseError(
                f"{where}.via_edge must be a mapping with "
                f"edge_type and dst_subtype",
            )
        via_edge = ViaEdge(
            edge_type=str(ve["edge_type"]),
            dst_subtype=str(ve["dst_subtype"]),
        )
    via: Optional[str] = None
    if "via" in raw:
        if not allow_via:
            raise RuleParseError(
                f"{where} cannot use `via:` (only dst supports it)",
            )
        via = str(raw["via"])
        if via != "alias_table":
            raise RuleParseError(
                f"{where}.via must be 'alias_table'; got {via!r}",
            )
    derive_from_path: Optional[DeriveFromPath] = None
    if "derive_from_path" in raw:
        d = raw["derive_from_path"]
        if (
            not isinstance(d, dict)
            or "attr" not in d
            or "regex" not in d
            or "capture_field" not in d
        ):
            raise RuleParseError(
                f"{where}.derive_from_path must include attr, "
                f"regex, capture_field",
            )
        derive_from_path = DeriveFromPath(
            attr=str(d["attr"]),
            regex=str(d["regex"]),
            capture_field=str(d["capture_field"]),
            multi_capture=bool(d.get("multi_capture", False)),
        )
    unique_field_fallback: Optional[UniqueFieldFallback] = None
    if "unique_field_fallback" in raw:
        u = raw["unique_field_fallback"]
        if via != "alias_table":
            raise RuleParseError(
                f"{where}.unique_field_fallback needs `via: alias_table`",
            )
        if (
            not isinstance(u, dict)
            or set(u) != {"subtype", "field"}
            or not all(isinstance(u[k], str) and u[k] for k in u)
        ):
            raise RuleParseError(
                f"{where}.unique_field_fallback must be a mapping with "
                f"exactly `subtype` and `field` strings",
            )
        unique_field_fallback = UniqueFieldFallback(
            subtype=u["subtype"], field=u["field"],
        )
    return RuleEndpoint(
        subtypes=subtypes,
        field=field,
        fields=fields,
        via_edge=via_edge,
        via=via,
        derive_from_path=derive_from_path,
        unique_field_fallback=unique_field_fallback,
    )


def _validate_rule(rule: CrossLinkRule) -> None:
    """Enforce the matcher's required fields. Each matcher has a
    minimum shape contract; missing pieces raise loudly so a
    rule typo fails at load, not at first-emit."""
    m = rule.matcher
    if m not in MATCHER_NAMES:
        raise RuleParseError(
            f"rule {rule.id!r}: unknown matcher {m!r}; valid: "
            f"{MATCHER_NAMES}",
        )
    if m == "exact":
        if not (rule.src.field and rule.dst.field):
            raise RuleParseError(
                f"rule {rule.id!r} matcher=exact: both src.field "
                f"and dst.field are required",
            )
    elif m == "subject_prefix":
        if not (rule.src.field and rule.dst.field):
            raise RuleParseError(
                f"rule {rule.id!r} matcher=subject_prefix: both "
                f"src.field and dst.field are required",
            )
    elif m == "word_boundary":
        has_src_text = bool(rule.src.field) or bool(rule.src.fields)
        if not (has_src_text and rule.dst.field):
            raise RuleParseError(
                f"rule {rule.id!r} matcher=word_boundary: "
                f"src.field|fields and dst.field are required",
            )
    elif m == "derive_from_path":
        if rule.src.via_edge is None and rule.src.field is None:
            raise RuleParseError(
                f"rule {rule.id!r} matcher=derive_from_path: src "
                f"needs `via_edge` (path lives on the via dst) or "
                f"`field` (path lives directly on src)",
            )
        if rule.dst.derive_from_path is None or rule.dst.field is None:
            raise RuleParseError(
                f"rule {rule.id!r} matcher=derive_from_path: "
                f"dst needs `derive_from_path` block + `field` "
                f"(the field on dst that the regex capture matches)",
            )
    elif m == "alias_table":
        if not rule.src.field:
            raise RuleParseError(
                f"rule {rule.id!r} matcher=alias_table: "
                f"src.field is required (the value to look up)",
            )
        if rule.dst.via != "alias_table":
            raise RuleParseError(
                f"rule {rule.id!r} matcher=alias_table: "
                f"dst.via must be 'alias_table'",
            )


def load_rules(raw: dict[str, Any], *, source: str) -> list[CrossLinkRule]:
    """Parse a YAML-decoded `{"rules": [...]}` document into
    a list of `CrossLinkRule`. `source` is used in error msgs."""
    rules_raw = raw.get("rules") if isinstance(raw, dict) else None
    if not isinstance(rules_raw, list):
        raise RuleParseError(
            f"{source}: top-level `rules:` list missing",
        )
    out: list[CrossLinkRule] = []
    seen_ids: set[str] = set()
    for i, r in enumerate(rules_raw):
        if not isinstance(r, dict):
            raise RuleParseError(
                f"{source}: rules[{i}] must be a mapping",
            )
        rid = r.get("id")
        if not isinstance(rid, str) or not rid:
            raise RuleParseError(
                f"{source}: rules[{i}] missing string `id`",
            )
        if rid in seen_ids:
            raise RuleParseError(
                f"{source}: duplicate rule id {rid!r}",
            )
        seen_ids.add(rid)
        src = _parse_endpoint(
            r.get("src") or {}, where=f"rules[{rid}].src",
            allow_via=False,
        )
        dst = _parse_endpoint(
            r.get("dst") or {}, where=f"rules[{rid}].dst",
            allow_via=True,
        )
        matcher = r.get("matcher")
        if not isinstance(matcher, str):
            raise RuleParseError(
                f"{source}: rules[{rid}] missing `matcher`",
            )
        edge = r.get("edge") or "references"
        bidirectional = bool(r.get("bidirectional", True))
        rule = CrossLinkRule(
            id=rid,
            src=src,
            dst=dst,
            matcher=matcher,
            edge=str(edge),
            bidirectional=bidirectional,
        )
        _validate_rule(rule)
        out.append(rule)
    return out


def load_rules_file(path: Path) -> list[CrossLinkRule]:
    with path.open() as f:
        raw = fastyaml.safe_load(f) or {}
    return load_rules(raw, source=str(path))

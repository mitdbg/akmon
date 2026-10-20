"""linker_config.yaml validator (M3.4 — template field grammar).

Per design.md and `schemas/linker_config.schema.json`:

  Allowed substitution fields in `dst_template`:
    {resolved}, {value}, {type}, {src}

  Anything else (including a typo like `{parsed_slug}`, or a
  literal `{` / `}`) is rejected at catalog-load time with a
  readable error. There is NO escape form — literal braces are
  not expressible.

The JSON schema's regex is the authoritative grammar; this
loader applies the same rule in Python so that callers that load
linker_config without first running JSON-schema validation still
get the same rejection. Keeping the regex literal-identical to
the schema is the contract; if you change one, change both.

Per the M2.5 design review (extended to M3.4): catalog grammar
errors must surface BEFORE the publisher cycle starts — at
catalog load — so a bad linker_config never reaches Phase A.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError


ALLOWED_TEMPLATE_FIELDS: frozenset[str] = frozenset({
    "resolved", "value", "type", "src",
})


# Authoritative regex — keep textually aligned with
# schemas/linker_config.schema.json #/$defs/simple_route/dst_template/pattern.
# Matches: any sequence of (non-brace chars OR one of the four allowed
# {field} placeholders). Rejects literal { or } and any other {name}.
_TEMPLATE_RE = re.compile(
    r"^([^{}]|\{(resolved|value|type|src)\})+$"
)

# A sub-pattern used to enumerate the {field} occurrences for the
# error message (so we can name the offender).
_FIELD_RE = re.compile(r"\{([^{}]*)\}")

# Subtype name pattern from the schema's by_src_subtype keys.
# Substrate live subtypes are snake_case. Older fixtures used
# PascalCase class names, so both forms are accepted here.
_SUBTYPE_KEY_RE = re.compile(
    r"^([A-Z][A-Za-z0-9_]*|[a-z][a-z0-9_]*|_default)$"
)


def validate_dst_template(template: str, *, where: str = "") -> None:
    """Raise SubstrateError if `template` violates the grammar.

    `where` is a context string included in the error message
    (e.g. `routes.service.dst_template` or
    `routes.file.Ticket.dst_template`).
    """
    if not isinstance(template, str) or not template:
        raise SubstrateError(
            f"linker_config: {where or 'dst_template'} must be a "
            f"non-empty string; got {template!r}",
            data={"where": where, "value": template},
            fix_hint=(
                "set dst_template to a string containing only literal "
                "characters and one or more of: "
                + ", ".join(sorted("{" + f + "}" for f in ALLOWED_TEMPLATE_FIELDS))
            ),
        )
    if _TEMPLATE_RE.fullmatch(template):
        return

    # Build a focused error: which fields are offending?
    offenders: list[str] = []
    for m in _FIELD_RE.finditer(template):
        name = m.group(1)
        if name not in ALLOWED_TEMPLATE_FIELDS:
            offenders.append("{" + name + "}")
    has_loose_brace = bool(re.search(r"\{[^}]*$|^[^{]*\}", template)) or (
        "{" in template and not _FIELD_RE.search(template)
    )

    if offenders:
        msg = (
            f"linker_config: {where or 'dst_template'} contains "
            f"unsupported field(s) {sorted(set(offenders))}; "
            f"allowed: {sorted('{' + f + '}' for f in ALLOWED_TEMPLATE_FIELDS)}"
        )
    elif has_loose_brace:
        msg = (
            f"linker_config: {where or 'dst_template'} contains a "
            f"literal '{{' or '}}'; brace literals are not expressible "
            f"(no escape form). Template: {template!r}"
        )
    else:
        msg = (
            f"linker_config: {where or 'dst_template'} does not match "
            f"the grammar; only {sorted('{' + f + '}' for f in ALLOWED_TEMPLATE_FIELDS)} "
            f"placeholders are accepted. Template: {template!r}"
        )
    raise SubstrateError(
        msg,
        data={"where": where, "template": template,
              "allowed_fields": sorted(ALLOWED_TEMPLATE_FIELDS)},
        fix_hint=(
            "remove unsupported placeholders, or extend the linker "
            "schema in a follow-up proposal (this is a catalog grammar "
            "change, not a config edit)"
        ),
    )


#: The core producer of "known by key, not loaded" link endpoints (issue
#: #3666). It is an `_extractor:` source so the publisher loads and applies
#: its node facts with the extractor's own edges; the leading underscore keeps
#: it apart from every entity type's `_extractor:<type>` edge source.
LINK_ENDPOINT_SOURCE = "_extractor:_link_endpoint"
#: The attribute an endpoint carries, and its one value.
LOAD_STATE_ATTR = "load_state"
NOT_LOADED = "not_loaded"
#: The attributes an endpoint writes besides its key and scope.
ENDPOINT_TEXT_ATTR = "text"
ENDPOINT_RESERVED_ATTRS = frozenset({LOAD_STATE_ATTR, "label", ENDPOINT_TEXT_ATTR})


def endpoint_text(value: str) -> str:
    """An endpoint's searchable text: the key, then what the row is."""
    return f"{value} (known by key, not loaded: the graph holds no record for it)"

_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_ENDPOINT_ID_RE = re.compile(r"^([^{}]|\{value\})+$")


@dataclass(frozen=True)
class LinkEndpointSpec:
    """A route's `on_miss` block: where a link to an unloaded target goes.

    `endpoint_id` is the target's real id with `{value}` substituted, so the
    edge to the endpoint is the very edge the real record keeps when it
    loads. `scope_attr`/`scope_regex` name the attribute that holds the first
    group of `scope_regex` matched against the key (a Jira project, say).
    """

    endpoint_id: str
    subtype: str
    key_attr: str
    scope_attr: Optional[str] = None
    scope_regex: Optional[str] = None

    def node_id(self, value: str) -> str:
        return self.endpoint_id.replace("{value}", value)

    def attrs(self, value: str) -> dict[str, Any]:
        """The endpoint's whole attribute set: key, scope, state, label and
        a text that says what it is, so a search for the key finds it and
        reads, in the row itself, that the record is not loaded."""
        out: dict[str, Any] = {
            self.key_attr: value,
            LOAD_STATE_ATTR: NOT_LOADED,
            "label": value,
            ENDPOINT_TEXT_ATTR: endpoint_text(value),
        }
        if self.scope_attr and self.scope_regex:
            match = re.search(self.scope_regex, value)
            if match is not None and match.group(1):
                out[self.scope_attr] = match.group(1)
        return out


def is_link_endpoint_row(row: Any) -> bool:
    """Whether a graph row is a link endpoint: written by the core producer
    and still marked not loaded. A real record at the same id is not one."""
    if not isinstance(row, dict):
        try:
            row = dict(row)
        except (TypeError, ValueError):
            return False
    if str(row.get("source") or "") != LINK_ENDPOINT_SOURCE:
        return False
    attrs = row.get("attrs")
    return isinstance(attrs, dict) and attrs.get(LOAD_STATE_ATTR) == NOT_LOADED


@dataclass(frozen=True)
class LinkerRoute:
    """One resolved (entity_type, src_subtype?) → route binding.

    `src_subtype` is None for the simple-route shape; populated for
    by-src-subtype routes (including the `_default` key).

    `on_miss` (issue #3666) is off unless the route declares it. It is not
    part of the extractor fingerprint, so switching it on does not
    re-extract every text record; existing misses are backfilled from their
    retry rows (`okg remediate link-endpoints`).
    """

    entity_type: str
    src_subtype: Optional[str]
    edge_type: str
    dst_template: str
    confidence: Optional[float] = None
    on_miss: Optional[LinkEndpointSpec] = None


def _ident(value: Any, *, where: str) -> str:
    if not isinstance(value, str) or not _IDENT_RE.fullmatch(value):
        raise SubstrateError(
            f"linker_config: {where} must be a snake_case name; got {value!r}",
            data={"where": where, "value": value},
        )
    return value


def validate_on_miss(block: Any, *, where: str, dst_template: str) -> LinkEndpointSpec:
    """Validate one route's `on_miss` block (issue #3666)."""
    if not isinstance(block, dict):
        raise SubstrateError(
            f"linker_config: {where} must be a mapping",
            data={"where": where, "value": block},
        )
    allowed = {"endpoint_id", "subtype", "key_attr", "scope"}
    extras = set(block) - allowed
    missing = {"endpoint_id", "subtype", "key_attr"} - set(block)
    if extras or missing:
        raise SubstrateError(
            f"linker_config: {where} needs endpoint_id, subtype and key_attr "
            f"(scope optional); unknown {sorted(extras)}, missing {sorted(missing)}",
            data={"where": where, "extras": sorted(extras), "missing": sorted(missing)},
        )
    if "{resolved}" not in dst_template:
        raise SubstrateError(
            f"linker_config: {where} needs a dst_template with {{resolved}}; "
            "a route that does not resolve never misses",
            data={"where": where, "dst_template": dst_template},
        )
    endpoint_id = block["endpoint_id"]
    if (
        not isinstance(endpoint_id, str)
        or not _ENDPOINT_ID_RE.fullmatch(endpoint_id)
        or "{value}" not in endpoint_id
    ):
        raise SubstrateError(
            f"linker_config: {where}.endpoint_id must contain {{value}} and no "
            f"other placeholder; got {endpoint_id!r}",
            data={"where": where, "value": endpoint_id},
        )
    subtype = _ident(block["subtype"], where=f"{where}.subtype")
    key_attr = _ident(block["key_attr"], where=f"{where}.key_attr")
    scope_attr = scope_regex = None
    scope = block.get("scope")
    if scope is not None:
        if not isinstance(scope, dict) or set(scope) != {"attr", "regex"}:
            raise SubstrateError(
                f"linker_config: {where}.scope must be a mapping with attr and regex",
                data={"where": where, "value": scope},
            )
        scope_attr = _ident(scope["attr"], where=f"{where}.scope.attr")
        if scope_attr == key_attr or scope_attr in ENDPOINT_RESERVED_ATTRS:
            raise SubstrateError(
                f"linker_config: {where}.scope.attr {scope_attr!r} is already "
                "an endpoint attribute",
                data={"where": where, "value": scope_attr},
            )
        scope_regex = scope["regex"]
        try:
            compiled = re.compile(scope_regex) if isinstance(scope_regex, str) else None
        except re.error as exc:
            raise SubstrateError(
                f"linker_config: {where}.scope.regex does not compile: {exc}",
                data={"where": where, "value": scope_regex},
            ) from exc
        if compiled is None or compiled.groups < 1:
            raise SubstrateError(
                f"linker_config: {where}.scope.regex needs one capture group",
                data={"where": where, "value": scope_regex},
            )
    if key_attr in ENDPOINT_RESERVED_ATTRS:
        raise SubstrateError(
            f"linker_config: {where}.key_attr {key_attr!r} is reserved",
            data={"where": where, "value": key_attr},
        )
    return LinkEndpointSpec(
        endpoint_id=endpoint_id,
        subtype=subtype,
        key_attr=key_attr,
        scope_attr=scope_attr,
        scope_regex=scope_regex,
    )


def _validate_simple_route(
    route: dict[str, Any], *, where: str,
) -> tuple[str, str, Optional[float]]:
    if not isinstance(route, dict):
        raise SubstrateError(
            f"linker_config: {where} must be a mapping",
            data={"where": where, "value": route},
        )
    extras = set(route) - {"edge_type", "dst_template", "confidence", "on_miss"}
    if extras:
        raise SubstrateError(
            f"linker_config: {where} has unknown keys "
            f"{sorted(extras)}; allowed: edge_type, dst_template, "
            f"confidence, on_miss",
            data={"where": where, "extras": sorted(extras)},
        )
    if "edge_type" not in route or "dst_template" not in route:
        raise SubstrateError(
            f"linker_config: {where} requires both edge_type and "
            f"dst_template",
            data={"where": where, "got": sorted(route)},
        )
    edge_type = route["edge_type"]
    if not isinstance(edge_type, str) or not edge_type:
        raise SubstrateError(
            f"linker_config: {where}.edge_type must be a non-empty "
            f"string; got {edge_type!r}",
            data={"where": where, "value": edge_type},
        )
    template = route["dst_template"]
    validate_dst_template(template, where=f"{where}.dst_template")
    confidence = route.get("confidence")
    if confidence is not None:
        if not isinstance(confidence, (int, float)) or not (
            0.0 <= float(confidence) <= 1.0
        ):
            raise SubstrateError(
                f"linker_config: {where}.confidence must be a number "
                f"in [0, 1]; got {confidence!r}",
                data={"where": where, "value": confidence},
            )
        confidence = float(confidence)
    return edge_type, template, confidence


def _is_subtype_key(s: str) -> bool:
    return bool(_SUBTYPE_KEY_RE.fullmatch(s))


def load_linker_config(path: str | Path) -> list[LinkerRoute]:
    """Load `linker_config.yaml`, validate, return resolved routes.

    Validation rules applied at load time:

      - Top-level keys must be a subset of {includes, routes}.
      - `includes` (if present) must be a list of strings matching
        `okg.linker_routes.<name>` — this loader does NOT resolve
        includes; it surfaces the names so a higher-level loader
        can fetch and re-validate.
      - Each route is either a `simple_route` (edge_type +
        dst_template + optional confidence) or a `by_src_subtype`
        mapping whose keys are snake_case live subtypes, PascalCase
        class names, or `_default`, with simple_route values.
      - Every dst_template MUST satisfy the four-field grammar.

    On any violation: raises `SubstrateError` with the offending
    location (route key + sub-key path) named in the message.
    """
    p = Path(path)
    with p.open("r") as f:
        doc = fastyaml.safe_load(f) or {}

    if not isinstance(doc, dict):
        raise SubstrateError(
            f"linker_config {p}: top-level must be a mapping; "
            f"got {type(doc).__name__}",
            data={"path": str(p)},
        )
    extras = set(doc) - {"includes", "routes"}
    if extras:
        raise SubstrateError(
            f"linker_config {p}: unknown top-level keys "
            f"{sorted(extras)}; allowed: includes, routes",
            data={"path": str(p), "extras": sorted(extras)},
        )

    includes = doc.get("includes") or []
    if includes and (
        not isinstance(includes, list)
        or not all(isinstance(x, str) for x in includes)
    ):
        raise SubstrateError(
            f"linker_config {p}: 'includes' must be a list of strings",
            data={"path": str(p), "value": includes},
        )
    inc_re = re.compile(r"^okg\.linker_routes\.[a-z_][a-z0-9_]*$")
    for inc in includes:
        if not inc_re.fullmatch(inc):
            raise SubstrateError(
                f"linker_config {p}: include {inc!r} does not match "
                f"'okg.linker_routes.<lowercase_identifier>'",
                data={"path": str(p), "include": inc},
            )

    routes = doc.get("routes") or {}
    if not isinstance(routes, dict):
        raise SubstrateError(
            f"linker_config {p}: 'routes' must be a mapping; "
            f"got {type(routes).__name__}",
            data={"path": str(p)},
        )

    out: list[LinkerRoute] = []
    for entity_type, body in routes.items():
        if not isinstance(body, dict):
            raise SubstrateError(
                f"linker_config {p}: routes.{entity_type} must be a "
                f"mapping",
                data={"path": str(p), "entity_type": entity_type},
            )

        # Distinguish simple_route from by_src_subtype by looking
        # at the keys: simple_route has edge_type/dst_template at
        # the top; by_src_subtype's keys are subtype names.
        if "edge_type" in body or "dst_template" in body:
            # Simple route at this level — no by-subtype branching.
            edge_type, template, confidence = _validate_simple_route(
                body, where=f"routes.{entity_type}",
            )
            out.append(LinkerRoute(
                entity_type=entity_type,
                src_subtype=None,
                edge_type=edge_type,
                dst_template=template,
                confidence=confidence,
                on_miss=_route_on_miss(
                    body, where=f"routes.{entity_type}.on_miss",
                    dst_template=template,
                ),
            ))
        else:
            # by_src_subtype: every key must be a subtype name or _default.
            if not body:
                raise SubstrateError(
                    f"linker_config {p}: routes.{entity_type} must have "
                    f"at least one subtype key (or _default)",
                    data={"path": str(p), "entity_type": entity_type},
                )
            for subtype, sub_route in body.items():
                if not _is_subtype_key(subtype):
                    raise SubstrateError(
                        f"linker_config {p}: routes.{entity_type}.{subtype}"
                        f" must be a snake_case subtype, PascalCase "
                        f"class name, or '_default'",
                        data={
                            "path": str(p),
                            "entity_type": entity_type,
                            "subtype_key": subtype,
                        },
                    )
                edge_type, template, confidence = _validate_simple_route(
                    sub_route, where=f"routes.{entity_type}.{subtype}",
                )
                out.append(LinkerRoute(
                    entity_type=entity_type,
                    src_subtype=subtype,
                    edge_type=edge_type,
                    dst_template=template,
                    confidence=confidence,
                    on_miss=_route_on_miss(
                        sub_route,
                        where=f"routes.{entity_type}.{subtype}.on_miss",
                        dst_template=template,
                    ),
                ))

    return out


def _declares_on_miss(doc: Any) -> bool:
    routes = doc.get("routes") if isinstance(doc, dict) else None
    if not isinstance(routes, dict):
        return False
    for body in routes.values():
        if not isinstance(body, dict):
            continue
        if "on_miss" in body:
            return True
        if any(isinstance(sub, dict) and "on_miss" in sub for sub in body.values()):
            return True
    return False


def link_endpoint_routes(path: str | Path) -> list[LinkerRoute]:
    """The routes of a deployment's `linker_config.yaml` that declare
    `on_miss` (issue #3666), for the catalog-load check.

    A file with no `on_miss` block returns ``[]`` without further reading,
    so the catalog load of a deployment that does not use endpoints is
    unchanged. A file that declares one is loaded and validated whole.
    """
    p = Path(path)
    if not p.is_file():
        return []
    with p.open("r") as f:
        doc = fastyaml.safe_load(f) or {}
    if not _declares_on_miss(doc):
        return []
    return [route for route in load_linker_config(p) if route.on_miss is not None]


def check_link_endpoint_subtypes(
    routes: list[LinkerRoute],
    *,
    subtypes: set[str] | frozenset[str],
    required_attributes: Any,
) -> None:
    """Refuse an `on_miss` the catalog cannot hold (issue #3666).

    `required_attributes(subtype)` returns the attributes the subtype marks
    required, inherited ones included. An endpoint fills only its key
    attribute and, when declared, its scope attribute; a subtype that
    requires any other attribute would get rows that break its own schema,
    and a subtype the catalog lacks would only fail later, at publish.
    """
    for route in routes:
        spec = route.on_miss
        if spec is None:
            continue
        where = (
            f"routes.{route.entity_type}.on_miss"
            if route.src_subtype is None
            else f"routes.{route.entity_type}.{route.src_subtype}.on_miss"
        )
        if spec.subtype not in subtypes:
            raise SubstrateError(
                f"linker_config: {where}.subtype {spec.subtype!r} is not a "
                "subtype of this catalog",
                data={
                    "reason": "unknown_subtype",
                    "where": where,
                    "subtype": spec.subtype,
                },
                fix_hint=(
                    "set on_miss.subtype to a catalog subtype, or remove "
                    "on_miss from the route"
                ),
            )
        filled = {spec.key_attr, *ENDPOINT_RESERVED_ATTRS}
        if spec.scope_attr:
            filled.add(spec.scope_attr)
        unfilled = sorted(set(required_attributes(spec.subtype)) - filled)
        if unfilled:
            raise SubstrateError(
                f"linker_config: {where} writes {spec.subtype!r} endpoints "
                f"that fill only {sorted(filled)}, but {spec.subtype!r} "
                f"requires {unfilled} too",
                data={
                    "reason": "required_attribute_unfilled",
                    "where": where,
                    "subtype": spec.subtype,
                    "filled": sorted(filled),
                    "unfilled": unfilled,
                },
                fix_hint=(
                    "point on_miss.key_attr or on_miss.scope.attr at the "
                    "required attribute, choose a subtype that requires "
                    "only those, or remove on_miss from the route"
                ),
            )


def _route_on_miss(
    route: dict[str, Any], *, where: str, dst_template: str,
) -> Optional[LinkEndpointSpec]:
    if "on_miss" not in route:
        return None
    return validate_on_miss(route["on_miss"], where=where, dst_template=dst_template)

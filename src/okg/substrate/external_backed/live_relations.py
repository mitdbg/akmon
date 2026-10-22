"""Subtype-declared live relations: declaration, binding and load refusals.

Issue #2013, PACT ``live-relations-on-expand``, Sprint 20 charter #2008.

WHAT THIS MODULE IS
-------------------
A subtype declares, in its LinkML class annotations, what an agent may ask
the system BEHIND that node. A deployment binds each declared relation to an
adapter. This module owns the declaration and the binding, and nothing else:

* the **declaration parser** -- :func:`parse_live_relations`, run over a
  class's ``annotations`` block at catalog-composition time;
* the **argument model compiler** -- :func:`compile_args_model`, which turns
  the declared ``args`` into a pydantic model with ``extra="forbid"`` ONCE,
  at deployment load;
* the **binding cross-check** -- :func:`bind_live_relations`, which pairs
  declared relations with ``agent_surface.live_bindings`` and refuses, by
  name, every shape this substrate cannot honour.

Dispatch, the Nomos gate and the call sample live next door in
:mod:`okg.substrate.external_backed.live_dispatch`. Nothing here talks to a
database, a policy bundle or an adapter.

WHY THE ANNOTATION IS A NESTED MAPPING
--------------------------------------
LinkML annotations accept arbitrarily nested mappings, and this repository
already relies on that: ``okg_relation_kind`` and ``okg_evidence_kind_by_attr``
are both parsed as nested mappings by :mod:`okg.substrate.catalog.modules_compose`,
which reads the raw YAML rather than going through ``SchemaView``. A JSON
string was considered as a fallback and is not needed. The shape::

    annotations:
      okg_live_relations:
        hits:
          action: external.opensearch.search
          principal_mode: service
          sample_policy: digest
          retention_class: warm
          deadline_seconds: 10
          args:
            q:    {type: string, required: true}
            size: {type: integer, default: 10}

SERVICE MODE ONLY, THIS SPRINT
------------------------------
``principal_mode`` accepts both declared values, and ``caller`` is REFUSED at
deployment load with :class:`LiveRelationCallerModeUnsupported`. The value
stays in the schema because the follow-up that threads a real caller subject
will need it, and refusing at LOAD rather than at dispatch means a deployment
that declares it learns at start rather than on an agent's first call.

Two preconditions are recorded on that follow-up and must not be skipped:
the resolved principal has to join the probe cache key (today one service
principal per deployment makes the key unambiguous), and the pre-gate
argument validation in the dispatcher is an existence oracle once a real
caller can be denied.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.external_backed.call_sample import (
    RETENTION_CLASSES,
    SAMPLE_POLICIES,
)
from okg.substrate.external_backed.handle import ADAPTER_KINDS

#: The command that recomposes a deployment's catalog, in the shape
#: `okg catalog ownership claim` prints it (`cli/catalog.py`). The operator
#: fills in the deployment and picks the writer scope; `--environment-id` is
#: added when the scope is `environment`.
RECOMPOSE_COMMAND = (
    "okg catalog load --deployment <deployment> "
    "--writer-scope <deployment_root|environment> --apply"
)

#: The annotation key a subtype's LinkML class declares live relations under.
ANNOTATION_KEY = "okg_live_relations"

#: Both are legal to DECLARE. Only `service` is honoured this sprint; see the
#: module docstring and `LiveRelationCallerModeUnsupported`.
PRINCIPAL_MODES = frozenset({"service", "caller"})

#: Argument value types a declaration may use. Deliberately small: every one
#: has an unambiguous JSON-Schema rendering, a stable canonical form for the
#: cache fingerprint, and an obvious pydantic type. Widening this set is a
#: design decision, not a convenience.
ARG_TYPES: Mapping[str, type] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
}

#: Longest `description` a relation may declare. `inspect` prints it beside
#: the arguments for every declared relation (a deployment may declare
#: dozens on one subtype), so it is a sentence or two -- what the relation answers
#: and what not to read into it -- never a manual.
MAX_DESCRIPTION_CHARS = 400

#: Relation and argument names. The same grammar the rest of the catalog uses
#: for declared tokens, so a relation name is always a legal dict key, a legal
#: JSON-Schema property and a legal cache-key component.
_TOKEN = re.compile(r"[a-z][a-z0-9_]{0,63}")

#: A Nomos action, e.g. `external.opensearch.search`. Dotted lower tokens: the
#: gate compares this string against the bundle's rules, so a typo must fail
#: here rather than silently matching no rule and denying forever.
_ACTION = re.compile(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+")


class LiveRelationInvalid(SubstrateError):
    """A live-relation declaration or binding this substrate cannot honour.

    Every subclass below is a LOAD-time refusal. None of them can be reached
    at dispatch, which is the point: a deployment learns that its declaration
    is wrong when it starts, not when an agent makes the first call and gets
    an error nobody is watching for.
    """

    code = "live_relation_invalid"


class LiveRelationArgSchemaInvalid(LiveRelationInvalid):
    code = "live_relation_arg_schema_invalid"


class LiveRelationPrincipalModeInvalid(LiveRelationInvalid):
    code = "live_relation_principal_mode_invalid"


class LiveRelationCallerModeUnsupported(LiveRelationInvalid):
    """`principal_mode: caller` on a relation, which is not served yet.

    Refused rather than downgraded to `service`. Downgrading would run the
    call under the runtime's own principal while the deployment believed it
    was running under the caller's -- a silent privilege change, which is the
    worst possible way to fail this particular check.
    """

    code = "live_relation_caller_mode_unsupported"


class LiveRelationSamplePolicyInvalid(LiveRelationInvalid):
    code = "live_relation_sample_policy_invalid"


class LiveRelationActionMissing(LiveRelationInvalid):
    """A declared relation with no Nomos action.

    There is no default action and there is no ungated live call. A relation
    with nothing to ask the gate about cannot be dispatched at all, so it
    fails load rather than dispatching against a guessed action name.
    """

    code = "live_relation_action_missing"


class LiveRelationUnbound(LiveRelationInvalid):
    code = "live_relation_unbound"


class LiveRelationAdapterKindUnknown(LiveRelationInvalid):
    code = "live_relation_adapter_kind_unknown"


class LiveBindingUndeclaredRelation(LiveRelationInvalid):
    """A `live_bindings` entry naming a relation no subtype declares.

    Refused rather than ignored: an operator who binds a misspelled relation
    has configured an adapter that will never run, and an ignored binding
    gives them no signal at all.
    """

    code = "live_binding_undeclared_relation"


@dataclass(frozen=True)
class LiveRelationArg:
    """One declared argument, with its bounds.

    Every argument is REQUIRED or has a DEFAULT -- never neither. That is
    what lets the compiled model be non-nullable, and non-nullability is what
    stops an explicit `null` defeating a declared default: `{"time_window":
    null}` used to validate against `str | None` and substitute `None`, which
    an adapter would turn into an unbounded scan. The value the operator
    declared applies on ABSENCE; an explicit null is refused by name.

    The bounds below exist because a typed slot is only as safe as its type.
    A `string` with no `maxLength` is an unbounded string in someone's query
    DSL, and an `integer` with no `maximum` is an unbounded page size. They
    are compiled into the model, published by `inspect` so an agent is told
    the limits rather than discovering them by refusal, and enforced at
    dispatch with the violated constraint named.
    """

    name: str
    type_name: str
    required: bool = False
    default: Any = None
    enum: tuple[Any, ...] | None = None
    pattern: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    max_length: int | None = None

    @property
    def python_type(self) -> type:
        return ARG_TYPES[self.type_name]

    def published(self) -> dict[str, Any]:
        """What `inspect` advertises for this argument, bounds included."""
        body: dict[str, Any] = {"type": self.type_name}
        if self.required:
            body["required"] = True
        if self.default is not None:
            body["default"] = self.default
        if self.enum is not None:
            body["enum"] = list(self.enum)
        if self.pattern is not None:
            body["pattern"] = self.pattern
        if self.minimum is not None:
            body["minimum"] = self.minimum
        if self.maximum is not None:
            body["maximum"] = self.maximum
        if self.max_length is not None:
            body["maxLength"] = self.max_length
        return body


@dataclass(frozen=True)
class LiveRelation:
    """One subtype-declared live relation, before it is bound."""

    name: str
    subtype: str
    action: str
    args: tuple[LiveRelationArg, ...] = ()
    principal_mode: str = "service"
    sample_policy: str = "metadata"
    retention_class: str = "warm"
    deadline_seconds: float = 10.0
    #: Operator text `inspect` shows beside the arguments: what the relation
    #: answers and what not to read into it (a quota figure
    #: ranked as capacity because nothing said it is an allocation). At most
    #: :data:`MAX_DESCRIPTION_CHARS`; absent is the normal case.
    description: str | None = None

    def published(self) -> dict[str, Any]:
        """The block `inspect` publishes for this relation.

        This is the COMPILED declaration, not the raw annotation, so what
        `inspect` advertises and what the dispatcher validates against cannot
        drift: they are rendered from the same object.

        A SUBSET of :meth:`to_metadata`. `principal_mode` and
        `deadline_seconds` are operator facts, not things a caller can act
        on, so they are stored but not advertised. `description` is
        advertised only when declared.
        """
        return {
            **(
                {"description": self.description}
                if self.description is not None else {}
            ),
            "action": self.action,
            "args": {arg.name: arg.published() for arg in self.args},
            "sample_policy": self.sample_policy,
            "retention_class": self.retention_class,
        }

    def to_metadata(self) -> dict[str, Any]:
        """The WHOLE compiled declaration, for `okg.subtypes.metadata`.

        The dispatcher rebuilds a `LiveRelation` from this at call time, so
        everything it needs has to survive the round trip -- including the
        two fields `published()` withholds. Storing only the published subset
        was the earlier shape and it could not be dispatched from: the
        deadline and the principal mode were lost, so the call would have
        silently run unbounded under an unstated principal.
        """
        return {
            **self.published(),
            "principal_mode": self.principal_mode,
            "deadline_seconds": self.deadline_seconds,
        }

    @classmethod
    def from_metadata(
        cls, name: str, subtype: str, body: Mapping[str, Any],
    ) -> "LiveRelation":
        """Rebuild a relation from what the catalog stored.

        Refuses a body it cannot rebuild rather than filling in defaults: a
        stored declaration that lost its deadline or its principal mode is a
        catalog written by a different version of this code, and guessing
        would run an external call under assumptions nobody declared.
        """
        missing = [
            key for key in ("action", "principal_mode", "deadline_seconds")
            if body.get(key) is None
        ]
        if missing:
            raise LiveRelationInvalid(
                f"stored live relation {name!r} on subtype {subtype!r} is "
                f"missing {missing}; the catalog was composed by a different "
                f"version of this code. Recompose the catalog rather than "
                f"dispatching against a partial declaration.",
                data={"subtype": subtype, "relation": name,
                      "missing": missing},
                fix_hint=f"run `{RECOMPOSE_COMMAND}` to recompose this catalog",
            )
        mode = str(body["principal_mode"])
        if mode not in PRINCIPAL_MODES:
            raise LiveRelationPrincipalModeInvalid(
                f"stored live relation {name!r} on subtype {subtype!r} "
                f"declares `principal_mode: {mode!r}`, not one of "
                f"{sorted(PRINCIPAL_MODES)}.",
                data={"subtype": subtype, "relation": name,
                      "principal_mode": mode},
            )
        if mode == "caller":
            # The same refusal `parse_live_relations` makes, repeated where a
            # STORED declaration is rebuilt. A catalog composed before that
            # refusal existed still carries `caller`, and rebuilding it
            # without this check would dispatch under the runtime's own
            # principal while the deployment believed otherwise.
            raise LiveRelationCallerModeUnsupported(
                f"stored live relation {name!r} on subtype {subtype!r} "
                f"declares `principal_mode: caller`, which is not served "
                f"yet. Recompose the catalog with `principal_mode: service`.",
                data={"subtype": subtype, "relation": name, "issue": 2013},
                fix_hint="declare `principal_mode: service` and recompose",
            )
        raw_args = body.get("args") or {}
        # The anchoring refusal `_parse_args` makes, repeated here for the
        # same reason as the caller-mode one above: a catalog composed before
        # it existed still stores the unanchored pattern, and rebuilding it
        # unchecked would dispatch an argument the operator believed was
        # restricted.
        for arg_name, arg_body in sorted(raw_args.items()):
            if arg_body.get("pattern") is not None:
                _assert_pattern_anchored(
                    str(arg_body["pattern"]), subtype=subtype,
                    relation=name, name=str(arg_name), where="",
                    stored=True,
                )
        # EVERY declared field is rebuilt, bounds included. Reading only
        # type/required/default meant a relation's `maximum` was validated at
        # composition, published by `inspect`, and then silently dropped when
        # the dispatcher rebuilt it -- so the bound existed everywhere except
        # where it was enforced.
        args = tuple(
            LiveRelationArg(
                name=str(arg_name),
                type_name=str(arg_body.get("type")),
                required=bool(arg_body.get("required", False)),
                default=arg_body.get("default"),
                enum=(
                    tuple(arg_body["enum"])
                    if isinstance(arg_body.get("enum"), (list, tuple))
                    else None
                ),
                pattern=(
                    str(arg_body["pattern"])
                    if arg_body.get("pattern") is not None else None
                ),
                minimum=(
                    float(arg_body["minimum"])
                    if arg_body.get("minimum") is not None else None
                ),
                maximum=(
                    float(arg_body["maximum"])
                    if arg_body.get("maximum") is not None else None
                ),
                max_length=(
                    int(arg_body["maxLength"])
                    if arg_body.get("maxLength") is not None else None
                ),
            )
            # NAME order, the same order `_parse_args` returns. The catalog
            # stores this body as jsonb, which does not keep object-key
            # order, so declaration order cannot survive the round trip.
            for arg_name, arg_body in sorted(raw_args.items())
        )
        return cls(
            name=name,
            subtype=subtype,
            action=str(body["action"]),
            args=args,
            principal_mode=mode,
            sample_policy=str(body.get("sample_policy") or "metadata"),
            retention_class=str(body.get("retention_class") or "warm"),
            deadline_seconds=float(body["deadline_seconds"]),
            # The same refusal the parser makes, on the stored copy: a
            # catalog written by other code must not publish an unbounded
            # or non-text description.
            description=_description(
                body.get("description"), subtype=subtype, relation=name,
                where=" (stored in the catalog)",
            ),
        )


@dataclass(frozen=True)
class LiveBinding:
    """A deployment's binding of one declared relation to an adapter."""

    relation: str
    kind: str
    config: Mapping[str, Any] = field(default_factory=dict)
    secret_refs: tuple[str, ...] = ()
    service_principal: str | None = None
    #: Whose credential the external system uses: `service` or `personal`.
    #: Required for `kind: mcp`, where the credential lives in the MCP
    #: server's container and okg cannot inspect it; refused on every other
    #: kind. See `mcp_adapter.assert_mcp_credentials_admitted`.
    upstream_credential: str | None = None


@dataclass(frozen=True)
class BoundLiveRelation:
    """A declared relation paired with the adapter that serves it."""

    relation: LiveRelation
    binding: LiveBinding

    @property
    def name(self) -> str:
        return self.relation.name


def _token(value: Any, *, label: str, where: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value.strip()):
        raise LiveRelationInvalid(
            f"{label} must be a lower-snake-case token of at most 64 "
            f"characters; got {value!r}.{where}",
            fix_hint=f"rename {label} to match [a-z][a-z0-9_]*",
        )
    return value.strip()


def parse_live_relations(
    annotations: Any,
    *,
    subtype: str,
    where: str = "",
) -> dict[str, LiveRelation]:
    """Compile a class's `okg_live_relations` annotation.

    Returns an empty mapping when the annotation is absent -- a subtype that
    declares no live relations is the normal case and is not an error. A
    PRESENT but malformed annotation always raises: a typo that silently
    produced "no live relations" is the failure this parser exists to stop.

    Written in the shape of `_parse_evidence_annotations`
    (`catalog/modules_compose.py`), which is the in-repo precedent for a
    nested-mapping annotation: validate the shape, refuse a wrong level by
    name, and tokenize identifiers against an explicit grammar.
    """
    if not isinstance(annotations, Mapping):
        return {}
    raw = annotations.get(ANNOTATION_KEY)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise LiveRelationInvalid(
            f"subtype {subtype!r} annotation `{ANNOTATION_KEY}` must be a "
            f"mapping of relation name to declaration; got "
            f"{type(raw).__name__}.{where}",
            data={"subtype": subtype},
            fix_hint=(
                f"write `{ANNOTATION_KEY}:` as a mapping, one key per relation"
            ),
        )

    relations: dict[str, LiveRelation] = {}
    for raw_name, body in raw.items():
        name = _token(
            raw_name, label=f"subtype {subtype!r} live relation name",
            where=where,
        )
        if not isinstance(body, Mapping):
            raise LiveRelationInvalid(
                f"subtype {subtype!r} live relation {name!r} must be a "
                f"mapping; got {type(body).__name__}.{where}",
                data={"subtype": subtype, "relation": name},
            )
        unknown = sorted(set(body) - _RELATION_KEYS)
        if unknown:
            raise LiveRelationInvalid(
                f"subtype {subtype!r} live relation {name!r} has unknown "
                f"key(s) {', '.join(repr(k) for k in unknown)}; accepted "
                f"keys are {sorted(_RELATION_KEYS)}.{where}",
                data={"subtype": subtype, "relation": name,
                      "unknown": unknown},
            )

        action = body.get("action")
        if action is None or (isinstance(action, str) and not action.strip()):
            raise LiveRelationActionMissing(
                f"subtype {subtype!r} live relation {name!r} declares no "
                f"`action`. There is no default Nomos action and no ungated "
                f"live call, so this relation could never be dispatched."
                f"{where}",
                data={"subtype": subtype, "relation": name},
                fix_hint=(
                    "declare `action: <domain>.<system>.<verb>` and allow it "
                    "for the service principal in the deployment's policy "
                    "bundle"
                ),
            )
        if not isinstance(action, str) or not _ACTION.fullmatch(action.strip()):
            raise LiveRelationInvalid(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`action: {action!r}`, which is not a dotted lower-case "
                f"action name. The gate matches this string against the "
                f"policy bundle, so a typo would match no rule and deny "
                f"this relation forever with no other signal.{where}",
                data={"subtype": subtype, "relation": name, "action": action},
                fix_hint="spell the action like `external.opensearch.search`",
            )
        action = action.strip()

        principal_mode = body.get("principal_mode", "service")
        if principal_mode not in PRINCIPAL_MODES:
            raise LiveRelationPrincipalModeInvalid(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`principal_mode: {principal_mode!r}`, which is not one of "
                f"{sorted(PRINCIPAL_MODES)}.{where}",
                data={"subtype": subtype, "relation": name,
                      "principal_mode": principal_mode},
            )
        if principal_mode == "caller":
            raise LiveRelationCallerModeUnsupported(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`principal_mode: caller`. The value is legal in the schema "
                f"and is NOT served yet: no caller subject reaches the live "
                f"gate today, so the call would run under the runtime's own "
                f"service principal while this deployment believed it ran "
                f"under the caller's. Refused rather than silently "
                f"downgraded.{where}",
                data={"subtype": subtype, "relation": name, "issue": 2013},
                fix_hint=(
                    "declare `principal_mode: service` until caller identity "
                    "is threaded through"
                ),
            )

        sample_policy = body.get("sample_policy", "metadata")
        if sample_policy not in SAMPLE_POLICIES:
            raise LiveRelationSamplePolicyInvalid(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`sample_policy: {sample_policy!r}`, which is not one of "
                f"{sorted(SAMPLE_POLICIES)}.{where}",
                data={"subtype": subtype, "relation": name,
                      "sample_policy": sample_policy},
            )
        retention_class = body.get("retention_class", "warm")
        if retention_class not in RETENTION_CLASSES:
            raise LiveRelationSamplePolicyInvalid(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`retention_class: {retention_class!r}`, which is not one "
                f"of {list(RETENTION_CLASSES)}.{where}",
                data={"subtype": subtype, "relation": name,
                      "retention_class": retention_class},
            )

        deadline = body.get("deadline_seconds", 10.0)
        if not isinstance(deadline, (int, float)) or isinstance(deadline, bool):
            raise LiveRelationInvalid(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`deadline_seconds: {deadline!r}`, which is not a number."
                f"{where}",
                data={"subtype": subtype, "relation": name},
            )
        if not (0 < float(deadline) <= 120):
            raise LiveRelationInvalid(
                f"subtype {subtype!r} live relation {name!r} declares "
                f"`deadline_seconds: {deadline}`, outside (0, 120]. An "
                f"unbounded live call holds an agent's turn open on a system "
                f"this graph does not control.{where}",
                data={"subtype": subtype, "relation": name,
                      "deadline_seconds": deadline},
            )

        relations[name] = LiveRelation(
            name=name,
            subtype=subtype,
            action=action,
            args=_parse_args(
                body.get("args"), subtype=subtype, relation=name, where=where,
            ),
            principal_mode=principal_mode,
            sample_policy=sample_policy,
            retention_class=retention_class,
            deadline_seconds=float(deadline),
            description=_description(
                body.get("description"), subtype=subtype, relation=name,
                where=where,
            ),
        )
    return relations


def _description(
    value: Any, *, subtype: str, relation: str, where: str,
) -> str | None:
    """A declared `description`: absent, or text of at most
    :data:`MAX_DESCRIPTION_CHARS` characters. Anything else is refused by
    name rather than published cut or coerced."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise LiveRelationInvalid(
            f"subtype {subtype!r} live relation {relation!r} declares a "
            f"`description` that is not non-empty text (got "
            f"{type(value).__name__}).{where}",
            data={"subtype": subtype, "relation": relation,
                  "field": "description"},
            fix_hint="write `description:` as one or two plain sentences",
        )
    text = value.strip()
    if len(text) > MAX_DESCRIPTION_CHARS:
        raise LiveRelationInvalid(
            f"subtype {subtype!r} live relation {relation!r} declares a "
            f"`description` of {len(text)} characters; at most "
            f"{MAX_DESCRIPTION_CHARS} are allowed, because `inspect` prints "
            f"it for every declared relation.{where}",
            data={"subtype": subtype, "relation": relation,
                  "field": "description", "length": len(text),
                  "limit": MAX_DESCRIPTION_CHARS},
            fix_hint=(
                f"shorten `description` to at most {MAX_DESCRIPTION_CHARS} "
                f"characters"
            ),
        )
    return text


_RELATION_KEYS = frozenset({
    "action", "args", "principal_mode", "sample_policy", "retention_class",
    "deadline_seconds", "description",
})

_ARG_KEYS = frozenset({
    "type", "required", "default",
    # The constraint set, added together rather than one at a time: a schema
    # that can say `type: string` but not `maxLength` invites an operator to
    # believe the type is the bound.
    "enum", "pattern", "minimum", "maximum", "maxLength",
})

#: Which constraints each type accepts. Declaring `maxLength` on an integer
#: is refused rather than ignored -- an ignored constraint reads as an
#: enforced one.
_CONSTRAINTS_BY_TYPE: Mapping[str, frozenset[str]] = {
    "string": frozenset({"enum", "pattern", "maxLength"}),
    "integer": frozenset({"enum", "minimum", "maximum"}),
    "number": frozenset({"enum", "minimum", "maximum"}),
    "boolean": frozenset({"enum"}),
}


def _assert_pattern_compiles(
    pattern: str, *, subtype: str, relation: str, name: str, where: str,
) -> None:
    """Compile a declared pattern through pydantic's own regex engine."""
    from pydantic import ConfigDict, Field, create_model

    try:
        create_model(
            "_PatternProbe",
            __config__=ConfigDict(extra="forbid"),
            value=(str, Field(pattern=pattern)),
        )
    except Exception as exc:  # noqa: BLE001 — any engine complaint is a refusal.
        raise LiveRelationArgSchemaInvalid(
            f"subtype {subtype!r} live relation {relation!r} argument "
            f"{name!r} declares `pattern: {pattern!r}`, which the engine that "
            f"enforces it rejects ({type(exc).__name__}). Python's `re` "
            f"accepts constructs that engine does not, so a pattern is "
            f"compiled here, at load, rather than at server build."
            f"{where}",
            data={"subtype": subtype, "relation": relation, "arg": name},
        ) from exc


#: An inline flag group that turns multi-line mode on, anywhere in the
#: pattern: `(?m)`, `(?im)`, `(?m:...)`. Over-matching `(?-m)` only makes the
#: check stricter, never looser.
_MULTILINE_FLAG = re.compile(r"\(\?[A-Za-z-]*m[A-Za-z-]*[:)]")

#: An inline flag group naming verbose mode: `(?x)`, `(?ix)`, `(?x:...)`.
#: In verbose mode `#` starts a comment and whitespace is ignored, so a
#: comment can hide a `(` from the syntactic check below while the engine
#: sees a top-level `|`. Refused outright; `(?-x)` is refused with it.
_VERBOSE_FLAG = re.compile(r"\(\?[A-Za-z-]*x[A-Za-z-]*[:)]")


def _pattern_anchor_problem(pattern: str) -> str | None:
    """Why `pattern` does not match the WHOLE value, or None when it does.

    pydantic applies `pattern` as a search (as JSON Schema does), so a
    pattern restricts the whole value only when it is pinned at both ends
    and has no top-level alternation. The check is syntactic and
    conservative: `^a$|^b$` is refused although it is anchored, because
    the fix -- `^(?:a|b)$` -- is always available and always clear.
    """
    if _VERBOSE_FLAG.search(pattern) is not None:
        return (
            "it enables verbose mode (`x` flag), where `#` starts a comment "
            "that can hide part of the pattern from this check; write the "
            "pattern without verbose mode"
        )
    body = pattern
    leading = re.match(r"\(\?[A-Za-z]+\)", body)
    if leading:
        body = body[leading.end():]
    # Multi-line mode turns `^` and `$` into LINE anchors, so a value with an
    # embedded newline would pass on one matching line. `\A` and `\z` are
    # text anchors in every mode.
    multiline = _MULTILINE_FLAG.search(pattern) is not None
    starts = ("\\A",) if multiline else ("^", "\\A")
    if not body.startswith(starts):
        return (
            "it does not start with `\\A` (multi-line mode is on)"
            if multiline else "it does not start with `^` or `\\A`"
        )
    # A quantifier on the start anchor makes it optional: `^?[a-z]+$`
    # accepts `!!ab`, because the search skips the anchor and matches `ab`.
    after_anchor = body[2:] if body.startswith("\\A") else body[1:]
    if after_anchor[:1] in ("?", "*", "+", "{"):
        return (
            "its start anchor is followed by a quantifier, which makes the "
            "anchor optional"
        )

    depth = 0
    # Opening index of each character class we are inside. Rust's regex
    # syntax nests classes (`[[a-z]&&[^x]]`), and a `]` in a class's first
    # position (`[]a]`, `[^]a]`) is a literal, not the close.
    classes: list[int] = []
    escaped = False
    last = len(body) - 1
    end_anchor: str | None = None
    for index, char in enumerate(body):
        if escaped:
            escaped = False
            if char == "z" and index == last:
                end_anchor = "\\z"
            continue
        if char == "\\":
            escaped = True
            continue
        if classes:
            first = classes[-1] + 1
            if body[first:first + 1] == "^":
                first += 1
            if char == "[":
                classes.append(index)
            elif char == "]" and index != first:
                classes.pop()
            continue
        if char == "[":
            classes.append(index)
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "|" and depth == 0:
            return (
                "it has a top-level `|`, so each side is searched on its "
                "own; wrap the alternation in `(?:...)` between the anchors"
            )
        elif char == "$" and index == last:
            end_anchor = "$"
    ends = ("\\z",) if multiline else ("$", "\\z")
    if end_anchor not in ends:
        return (
            "it does not end with `\\z` (multi-line mode is on)"
            if multiline else "it does not end with an unescaped `$` or `\\z`"
        )
    return None


def _assert_pattern_anchored(
    pattern: str, *, subtype: str, relation: str, name: str, where: str,
    stored: bool = False,
) -> None:
    """Refuse a pattern that would match a SUBSTRING of the value (#2148).

    `stored` is set when the pattern comes from a catalog rebuilt by
    `from_metadata`: fixing the schema alone leaves the old pattern in the
    catalog, so the hint also names the recompose command.
    """
    problem = _pattern_anchor_problem(pattern)
    if problem is None:
        return
    fix_hint = (
        "anchor the pattern at both ends, e.g. `^(?:...)$` or "
        "`\\A(?:...)\\z`"
    )
    if stored:
        fix_hint += (
            f", then run `{RECOMPOSE_COMMAND}` so the catalog stops storing "
            f"the old pattern"
        )
    raise LiveRelationArgSchemaInvalid(
        f"subtype {subtype!r} live relation {relation!r} argument {name!r} "
        f"declares `pattern: {pattern!r}`, which is not anchored at both "
        f"ends: {problem}. A pattern is applied as a SEARCH, so an "
        f"unanchored one accepts any value that merely CONTAINS a match "
        f"(`[A-Za-z0-9 ]{{1,60}}` accepts `!!bad!!`).{where}",
        data={"subtype": subtype, "relation": relation, "arg": name,
              "pattern": pattern, "reason": "pattern_unanchored"},
        fix_hint=fix_hint,
    )


def _is_declared_type(value: Any, type_name: str) -> bool:
    """Type check that does NOT treat a bool as an integer.

    `isinstance(True, int)` is True in Python, so `default: true` on an
    integer argument passed the check and became 1, and `enum: [true]`
    became [1]. Both are the operator meaning something else entirely.
    """
    if isinstance(value, bool):
        return type_name == "boolean"
    if type_name == "number":
        return isinstance(value, (int, float))
    return isinstance(value, ARG_TYPES[type_name])


def _parse_args(
    raw: Any, *, subtype: str, relation: str, where: str,
) -> tuple[LiveRelationArg, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, Mapping):
        raise LiveRelationArgSchemaInvalid(
            f"subtype {subtype!r} live relation {relation!r} `args` must be "
            f"a mapping of argument name to declaration; got "
            f"{type(raw).__name__}.{where}",
            data={"subtype": subtype, "relation": relation},
        )
    args: list[LiveRelationArg] = []
    for raw_name, body in raw.items():
        name = _token(
            raw_name,
            label=f"subtype {subtype!r} relation {relation!r} argument name",
            where=where,
        )
        if not isinstance(body, Mapping):
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} must be a mapping; got {type(body).__name__}."
                f"{where}",
                data={"subtype": subtype, "relation": relation, "arg": name},
            )
        unknown = sorted(set(body) - _ARG_KEYS)
        if unknown:
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} has unknown key(s) "
                f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
                f"{sorted(_ARG_KEYS)}.{where}",
                data={"subtype": subtype, "relation": relation, "arg": name,
                      "unknown": unknown},
            )
        type_name = body.get("type")
        if type_name not in ARG_TYPES:
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} declares `type: {type_name!r}`, which is not one "
                f"of {sorted(ARG_TYPES)}.{where}",
                data={"subtype": subtype, "relation": relation, "arg": name,
                      "type": type_name},
            )
        required = bool(body.get("required", False))
        default = body.get("default")
        if required and default is not None:
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} is `required` AND carries a `default`. A default "
                f"on a required argument can never be used, so one of the "
                f"two is a mistake and the declaration does not say which."
                f"{where}",
                data={"subtype": subtype, "relation": relation, "arg": name},
                fix_hint="drop `required: true`, or drop the `default`",
            )
        if not required and default is None:
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} is neither `required` nor given a `default`, so "
                f"its value on absence would be null. A null reaching an "
                f"adapter is an UNBOUNDED call -- a missing time window is a "
                f"full scan -- so there is no implicit null: declare the "
                f"argument required, or give it the default you intend."
                f"{where}",
                data={"subtype": subtype, "relation": relation, "arg": name},
                fix_hint="add `required: true`, or add a `default`",
            )
        if default is not None and not _is_declared_type(
            default, str(type_name),
        ):
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} declares `type: {type_name}` but a default of "
                f"type {type(default).__name__}.{where}",
                data={"subtype": subtype, "relation": relation, "arg": name},
            )

        allowed = _CONSTRAINTS_BY_TYPE[str(type_name)]
        declared_constraints = {
            key for key in ("enum", "pattern", "minimum", "maximum",
                            "maxLength")
            if body.get(key) is not None
        }
        misplaced = sorted(declared_constraints - allowed)
        if misplaced:
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} is a {type_name} and declares "
                f"{', '.join(repr(k) for k in misplaced)}, which a "
                f"{type_name} does not take; it accepts {sorted(allowed)}. "
                f"Refused rather than ignored: an ignored constraint reads "
                f"as an enforced one.{where}",
                data={"subtype": subtype, "relation": relation, "arg": name,
                      "misplaced": misplaced},
            )

        max_length_declared = body.get("maxLength")

        enum = body.get("enum")
        if enum is not None:
            if not isinstance(enum, (list, tuple)) or not enum:
                raise LiveRelationArgSchemaInvalid(
                    f"subtype {subtype!r} live relation {relation!r} "
                    f"argument {name!r} declares an empty or non-list "
                    f"`enum`.{where}",
                    data={"subtype": subtype, "relation": relation,
                          "arg": name},
                )
            bad = [v for v in enum
                   if not _is_declared_type(v, str(type_name))]
            if bad:
                raise LiveRelationArgSchemaInvalid(
                    f"subtype {subtype!r} live relation {relation!r} "
                    f"argument {name!r} declares enum values that are not "
                    f"{type_name}: {bad!r}.{where}",
                    data={"subtype": subtype, "relation": relation,
                          "arg": name},
                )
            if default is not None and default not in enum:
                raise LiveRelationArgSchemaInvalid(
                    f"subtype {subtype!r} live relation {relation!r} "
                    f"argument {name!r} has a default {default!r} outside "
                    f"its own enum {list(enum)!r}, so the default could "
                    f"never be accepted.{where}",
                    data={"subtype": subtype, "relation": relation,
                          "arg": name},
                )
            enum = tuple(enum)

        pattern = body.get("pattern")
        if pattern is not None:
            if not isinstance(pattern, str) or not pattern:
                raise LiveRelationArgSchemaInvalid(
                    f"subtype {subtype!r} live relation {relation!r} "
                    f"argument {name!r} declares a non-string or empty "
                    f"`pattern`.{where}",
                    data={"subtype": subtype, "relation": relation,
                          "arg": name},
                )
            # Compiled through THE SAME ENGINE that will enforce it.
            # Validating with Python's `re` accepted patterns pydantic's Rust
            # engine rejects -- a lookahead passes `re.compile` and then
            # blows up inside `create_model`, turning an operator's typo into
            # a crash at server build instead of a named load refusal here.
            _assert_pattern_compiles(
                pattern, subtype=subtype, relation=relation, name=name,
                where=where,
            )
            # An unanchored pattern matches a SUBSTRING in pydantic, so
            # `"[a-z]+"` accepts anything containing a letter. An operator
            # writing it believes the alphabet is restricted, and only
            # `maxLength` bounds anything -- refused here, by name (#2148).
            _assert_pattern_anchored(
                pattern, subtype=subtype, relation=relation, name=name,
                where=where,
            )
            # An unbounded string behind a pattern is refused too:
            # catastrophic backtracking needs length to bite.
            if max_length_declared is None:
                raise LiveRelationArgSchemaInvalid(
                    f"subtype {subtype!r} live relation {relation!r} "
                    f"argument {name!r} declares a `pattern` without a "
                    f"`maxLength`. A pattern over an unbounded string is how "
                    f"a regular expression becomes a denial of service; "
                    f"declare the longest value you mean to accept.{where}",
                    data={"subtype": subtype, "relation": relation,
                          "arg": name},
                    fix_hint="add `maxLength` beside the `pattern`",
                )

        minimum = body.get("minimum")
        maximum = body.get("maximum")
        for label, bound in (("minimum", minimum), ("maximum", maximum)):
            if bound is None:
                continue
            # `float("abc")` raises a bare ValueError that reaches the
            # operator as a traceback, and `float(True)` silently becomes
            # 1.0 -- a bound nobody declared.
            if isinstance(bound, bool) or not isinstance(bound, (int, float)):
                raise LiveRelationArgSchemaInvalid(
                    f"subtype {subtype!r} live relation {relation!r} "
                    f"argument {name!r} declares `{label}: {bound!r}`, which "
                    f"is not a number.{where}",
                    data={"subtype": subtype, "relation": relation,
                          "arg": name, "bound": label},
                    fix_hint=f"set `{label}` to a number, or remove it",
                )
        if (
            minimum is not None and maximum is not None
            and float(minimum) > float(maximum)
        ):
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} declares minimum {minimum} above maximum "
                f"{maximum}, which nothing can satisfy.{where}",
                data={"subtype": subtype, "relation": relation, "arg": name},
            )

        max_length = max_length_declared
        if max_length is not None and (
            not isinstance(max_length, int) or isinstance(max_length, bool)
            or max_length <= 0
        ):
            raise LiveRelationArgSchemaInvalid(
                f"subtype {subtype!r} live relation {relation!r} argument "
                f"{name!r} declares `maxLength: {max_length!r}`, which is "
                f"not a positive integer.{where}",
                data={"subtype": subtype, "relation": relation, "arg": name},
            )

        args.append(LiveRelationArg(
            name=name,
            type_name=str(type_name),
            required=required,
            default=default,
            enum=enum,
            pattern=str(pattern) if pattern is not None else None,
            minimum=float(minimum) if minimum is not None else None,
            maximum=float(maximum) if maximum is not None else None,
            max_length=max_length,
        ))
    # NAME order, not declaration order (issue #2144). The catalog stores the
    # compiled declaration as jsonb, which does not keep object-key order, so
    # the relation `from_metadata` rebuilds for the dispatcher can only ever
    # come back in a canonical order. Returning declaration order here made
    # the composed relation and the rebuilt one compare unequal, and made
    # `inspect`'s argument order depend on which path produced it.
    return tuple(sorted(args, key=lambda arg: arg.name))


def compile_args_model(relation: LiveRelation) -> type:
    """Compile one relation's declared arguments into a strict model.

    ONCE, at deployment load -- not per call. That is what makes "an unknown
    key is refused by name" cheap enough to do on every dispatch, and it is
    what turns a malformed declaration into a load failure rather than a
    first-call surprise.

    `extra="forbid"` is the whole point: pydantic names the offending key in
    its own error, and the dispatcher re-raises it with the accepted key list
    attached.

    EVERY FIELD IS NON-NULLABLE. An optional argument is `T` with a default,
    never `T | None`. The nullable form let an explicit `{"arg": null}`
    validate and substitute `None`, defeating the default the operator
    declared -- a null time window is a full scan. The declared value applies
    on ABSENCE; an explicit null is refused, by name, in
    `validate_live_arguments`.

    Declared bounds are compiled in, so the model refuses an out-of-range
    value rather than handing it to an adapter.
    """
    from typing import Annotated, Literal

    from pydantic import ConfigDict, Field, create_model

    fields: dict[str, Any] = {}
    for arg in relation.args:
        annotation: Any = arg.python_type
        if arg.enum is not None:
            annotation = Literal[tuple(arg.enum)]  # type: ignore[valid-type]
        constraints: dict[str, Any] = {}
        if arg.minimum is not None:
            constraints["ge"] = arg.minimum
        if arg.maximum is not None:
            constraints["le"] = arg.maximum
        if arg.max_length is not None:
            constraints["max_length"] = arg.max_length
        if arg.pattern is not None:
            constraints["pattern"] = arg.pattern
        if constraints:
            annotation = Annotated[annotation, Field(**constraints)]
        fields[arg.name] = (
            annotation, ... if arg.required else arg.default,
        )
    return create_model(  # type: ignore[call-overload]
        f"LiveArgs_{relation.subtype}_{relation.name}",
        __config__=ConfigDict(extra="forbid"),
        **fields,
    )


def parse_live_bindings(
    raw: Any, *, where: str = "",
) -> dict[str, LiveBinding]:
    """Parse `agent_surface.live_bindings` into typed bindings.

    Strict at every level, like the rest of the `agent_surface` block. Note
    what is NOT accepted here: `principal_mode` belongs to the SUBTYPE's
    declaration, not to the deployment's binding. A deployment that could
    raise its own principal mode would be able to widen a relation's
    authority without touching the ontology that declared it.
    """
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise LiveRelationInvalid(
            f"deployment.yaml `agent_surface.live_bindings` must be a "
            f"mapping of relation name to binding; got "
            f"{type(raw).__name__}.{where}",
            fix_hint="declare `live_bindings: {}`",
        )
    bindings: dict[str, LiveBinding] = {}
    for raw_name, body in raw.items():
        name = _token(
            raw_name, label="`agent_surface.live_bindings` key", where=where,
        )
        if not isinstance(body, Mapping):
            raise LiveRelationInvalid(
                f"deployment.yaml `agent_surface.live_bindings.{name}` must "
                f"be a mapping; got {type(body).__name__}.{where}",
                data={"relation": name},
            )
        unknown = sorted(set(body) - _BINDING_KEYS)
        if unknown:
            raise LiveRelationInvalid(
                f"deployment.yaml `agent_surface.live_bindings.{name}` has "
                f"unknown key(s) {', '.join(repr(k) for k in unknown)}; "
                f"accepted keys are {sorted(_BINDING_KEYS)}.{where}",
                data={"relation": name, "unknown": unknown},
            )
        kind = body.get("kind")
        if kind not in ADAPTER_KINDS:
            raise LiveRelationAdapterKindUnknown(
                f"deployment.yaml `agent_surface.live_bindings.{name}.kind` "
                f"is {kind!r}, which is not an adapter kind this substrate "
                f"runs. Known kinds are {sorted(ADAPTER_KINDS)}.{where}",
                data={"relation": name, "kind": kind,
                      "known": sorted(ADAPTER_KINDS)},
                fix_hint=f"set `kind` to one of {sorted(ADAPTER_KINDS)}",
            )
        secret_refs = body.get("secret_refs") or ()
        if not isinstance(secret_refs, (list, tuple)) or not all(
            isinstance(ref, str) and ref.strip() for ref in secret_refs
        ):
            raise LiveRelationInvalid(
                f"deployment.yaml "
                f"`agent_surface.live_bindings.{name}.secret_refs` must be a "
                f"list of non-empty strings.{where}",
                data={"relation": name},
            )
        config = body.get("config") or {}
        if not isinstance(config, Mapping):
            raise LiveRelationInvalid(
                f"deployment.yaml "
                f"`agent_surface.live_bindings.{name}.config` must be a "
                f"mapping; got {type(config).__name__}.{where}",
                data={"relation": name},
            )
        service_principal = body.get("service_principal")
        if service_principal is not None and (
            not isinstance(service_principal, str)
            or not service_principal.strip()
        ):
            raise LiveRelationInvalid(
                f"deployment.yaml "
                f"`agent_surface.live_bindings.{name}.service_principal` "
                f"must be a non-empty string when declared.{where}",
                data={"relation": name},
            )
        upstream_credential = body.get("upstream_credential")
        if kind == "mcp":
            # Declared, because it cannot be detected: the MCP server holds
            # the upstream credential in its own container, out of okg's
            # sight. A missing declaration is refused rather than defaulted,
            # since either default would be a guess about someone's
            # credential.
            if upstream_credential not in UPSTREAM_CREDENTIALS:
                raise LiveRelationInvalid(
                    f"deployment.yaml "
                    f"`agent_surface.live_bindings.{name}.upstream_credential` "
                    f"must be one of {list(UPSTREAM_CREDENTIALS)} for a "
                    f"`kind: mcp` binding; got {upstream_credential!r}. It "
                    f"declares whose credential the MCP server uses "
                    f"upstream, which okg cannot inspect.{where}",
                    data={"relation": name, "field": "upstream_credential",
                          "value": upstream_credential},
                    fix_hint=(
                        "declare `upstream_credential: service` for a "
                        "service account, or `personal` for one person's "
                        "credential (admitted only on a prototype or demo "
                        "deployment)"
                    ),
                )
        elif upstream_credential is not None:
            raise LiveRelationInvalid(
                f"deployment.yaml "
                f"`agent_surface.live_bindings.{name}.upstream_credential` "
                f"is set on a `kind: {kind}` binding. It is meaningful only "
                f"for `kind: mcp`, whose credential lives outside okg, and "
                f"is refused here rather than ignored.{where}",
                data={"relation": name, "field": "upstream_credential",
                      "kind": kind},
                fix_hint="remove `upstream_credential` from this binding",
            )
        bindings[name] = LiveBinding(
            relation=name,
            kind=str(kind),
            config=dict(config),
            secret_refs=tuple(secret_refs),
            service_principal=(
                service_principal.strip() if service_principal else None
            ),
            upstream_credential=(
                str(upstream_credential) if upstream_credential else None
            ),
        )
    return bindings


_BINDING_KEYS = frozenset({
    "kind", "config", "secret_refs", "service_principal",
    "upstream_credential",
})

#: The values a `kind: mcp` binding's `upstream_credential` may take. The
#: manifest parser owns the list; `mcp_adapter` imports it.
UPSTREAM_CREDENTIALS = ("service", "personal")


def bind_live_relations(
    declared: Mapping[str, LiveRelation],
    bindings: Mapping[str, LiveBinding],
    *,
    where: str = "",
) -> dict[str, BoundLiveRelation]:
    """Pair declared relations with their bindings, refusing every gap.

    Both directions are checked, and both are refusals rather than warnings:

    * a DECLARED relation with no binding cannot be dispatched, and a
      deployment that advertises it through `inspect` would be promising an
      agent something that always fails;
    * a BOUND relation nothing declares is an adapter configured to never
      run, which is the operator's typo and not a state to tolerate.
    """
    unbound = sorted(set(declared) - set(bindings))
    if unbound:
        raise LiveRelationUnbound(
            f"live relation(s) {', '.join(repr(n) for n in unbound)} are "
            f"declared by a subtype but bound by no "
            f"`agent_surface.live_bindings` entry, so nothing could serve "
            f"them. `inspect` would advertise a relation that always fails."
            f"{where}",
            data={"unbound": unbound, "bound": sorted(bindings)},
            fix_hint=(
                "add an `agent_surface.live_bindings` entry for each, or "
                "remove the declaration from the subtype"
            ),
        )
    undeclared = sorted(set(bindings) - set(declared))
    if undeclared:
        raise LiveBindingUndeclaredRelation(
            f"`agent_surface.live_bindings` names "
            f"{', '.join(repr(n) for n in undeclared)}, which no enrolled "
            f"subtype declares. The adapter is configured and can never run."
            f" Declared relations are {sorted(declared)}.{where}",
            data={"undeclared": undeclared, "declared": sorted(declared)},
            fix_hint=(
                "fix the spelling, enrol the module that declares the "
                "relation, or remove the binding"
            ),
        )
    return {
        name: BoundLiveRelation(relation=declared[name], binding=bindings[name])
        for name in sorted(declared)
    }


__all__ = [
    "ADAPTER_KINDS",
    "ANNOTATION_KEY",
    "ARG_TYPES",
    "PRINCIPAL_MODES",
    "BoundLiveRelation",
    "LiveBinding",
    "LiveBindingUndeclaredRelation",
    "LiveRelation",
    "LiveRelationActionMissing",
    "LiveRelationAdapterKindUnknown",
    "LiveRelationArg",
    "LiveRelationArgSchemaInvalid",
    "LiveRelationCallerModeUnsupported",
    "LiveRelationInvalid",
    "LiveRelationPrincipalModeInvalid",
    "LiveRelationSamplePolicyInvalid",
    "LiveRelationUnbound",
    "bind_live_relations",
    "compile_args_model",
    "parse_live_bindings",
    "parse_live_relations",
]


def register_sample_policies(
    relations: Mapping[str, "LiveRelation"],
) -> tuple[str, ...]:
    """Register each declared relation's sample policy for dispatch.

    Issue #2011 built the sample store and explicitly left "per-deployment
    policy loading" to this change. Without this call every live dispatch
    fails at the sample write with ``no registered sample policy`` -- which
    is how it was found: the call ran, the adapter ran, and the sample
    refused, so the whole call correctly failed with the out-of-band record
    saying the adapter HAD run. The failure behaviour was right and the
    integration was missing.

    ``graph_read=False`` on every one: a live relation must never be
    sampleable by the MCP tracing wrapper, which has no authorization
    decision of its own and would record a call as if it had been gated.

    ``refetch_query_keys`` is left EMPTY, so a digest sample's re-fetch
    pointer carries only the argument SHAPE. A sample can live 730 days, and
    copying argument values in would persist whatever a caller passed --
    a search term, a person's name -- for that long. A relation that wants a
    value in its pointer opts that key in explicitly, which is a decision for
    whoever declares it, not a default.

    STARTUP ONLY, and it refuses a CONFLICTING redeclaration by name.
    `call_sample.register_relation_sample_policy` documents itself as
    startup-scoped and its `declare()` replaces silently, so calling this per
    request -- which an earlier shape did -- meant one deployment's relation
    could quietly take over a name another deployment, or #2011's own
    hydration policies, had registered. Re-registering the SAME policy is a
    no-op; re-registering a different one for the same name raises.
    """
    from okg.substrate.external_backed.call_sample import (
        RelationSamplePolicy,
        default_registry,
        register_relation_sample_policy,
    )

    registry = default_registry()
    registered: list[str] = []
    for name, relation in sorted(relations.items()):
        wanted = RelationSamplePolicy(
            relation=name,
            policy=relation.sample_policy,
            retention_class=relation.retention_class,
            graph_read=False,
        )
        # `declared_relations` distinguishes a REGISTERED policy from the
        # registry's computed default, which `policy_for` returns for any
        # unknown name. Comparing against `policy_for` alone would treat
        # every first registration as a conflict.
        existing = (
            registry.policy_for(name)
            if name in registry.declared_relations else None
        )
        if existing is not None and existing != wanted:
            raise LiveRelationInvalid(
                f"live relation {name!r} is already registered with a "
                f"different sample policy "
                f"({existing.policy}/{existing.retention_class}, "
                f"graph_read={existing.graph_read}); this declaration asks "
                f"for {wanted.policy}/{wanted.retention_class}. The sample "
                f"registry is process-wide, so the second registration would "
                f"silently change how the first deployment's calls are "
                f"sampled.",
                data={
                    "relation": name,
                    "registered": [existing.policy, existing.retention_class],
                    "declared": [wanted.policy, wanted.retention_class],
                },
                fix_hint=(
                    "rename the relation, or declare the same sample policy "
                    "and retention class in both places"
                ),
            )
        if existing is None:
            register_relation_sample_policy(wanted)
        registered.append(name)
    return tuple(registered)


__all__.append("register_sample_policies")

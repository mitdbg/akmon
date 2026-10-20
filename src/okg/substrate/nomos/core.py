"""Deterministic Nomos policy compiler and bounded evaluator."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.substrate.nomos.vocabulary import (
    EFFECTS,
    NomosVocabularyError,
    validate_policy_graph_vocabulary,
)


_PROTECTED_IDENTITY_FACTS = frozenset({
    "deployment",
    "scope",
    "source",
    "graph_branch",
    "resource_kind",
    "classification",
    "principal_kind",
})


class NomosPolicyError(ValueError):
    """Raised when a policy graph or decision input is invalid."""


@dataclass(frozen=True)
class PolicyRule:
    rule_id: str
    effect: str
    actions: tuple[str, ...]
    subjects: tuple[str, ...] = ("*",)
    match: Mapping[str, Any] = field(default_factory=dict)
    priority: int = 0
    reason_code: str = ""
    obligations: tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "effect": self.effect,
            "actions": list(self.actions),
            "subjects": list(self.subjects),
            "match": _json_safe(self.match),
            "priority": self.priority,
            "reason_code": self.reason_code or self.rule_id,
            "obligations": [_json_safe(item) for item in self.obligations],
        }


@dataclass(frozen=True)
class CompiledPolicyBundle:
    policy_bundle_id: str
    policy_version: str
    policy_fingerprint: str
    compiler_version: str
    source_generation_id: int | None
    scope: Mapping[str, Any]
    default_effect: str
    reason_code_catalog: Mapping[str, str]
    effective_from: str | None
    effective_until: str | None
    compiled_at: str
    rules: tuple[PolicyRule, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_bundle_id": self.policy_bundle_id,
            "policy_version": self.policy_version,
            "policy_fingerprint": self.policy_fingerprint,
            "compiler_version": self.compiler_version,
            "source_generation_id": self.source_generation_id,
            "scope": _json_safe(self.scope),
            "default_effect": self.default_effect,
            "reason_code_catalog": dict(self.reason_code_catalog),
            "effective_from": self.effective_from,
            "effective_until": self.effective_until,
            "compiled_at": self.compiled_at,
            "rules": [rule.to_dict() for rule in self.rules],
        }


@dataclass(frozen=True)
class ConfigEntry:
    config_key: str
    value: Any | None = None
    secret_ref: str | None = None
    classification: str = "internal"
    scope: str = ""
    owner: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        out = {
            "config_key": self.config_key,
            "classification": self.classification,
            "scope": self.scope,
            "owner": self.owner,
            "metadata": _json_safe(self.metadata),
        }
        if self.value is not None:
            out["value"] = _json_safe(self.value)
        if self.secret_ref is not None:
            out["secret_ref"] = self.secret_ref
        return out


@dataclass(frozen=True)
class CompiledConfigSet:
    config_set_id: str
    config_version: str
    config_fingerprint: str
    compiler_version: str
    source_generation_id: int | None
    scope: Mapping[str, Any]
    effective_from: str | None
    effective_until: str | None
    compiled_at: str
    entries: tuple[ConfigEntry, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "config_set_id": self.config_set_id,
            "config_version": self.config_version,
            "config_fingerprint": self.config_fingerprint,
            "compiler_version": self.compiler_version,
            "source_generation_id": self.source_generation_id,
            "scope": _json_safe(self.scope),
            "effective_from": self.effective_from,
            "effective_until": self.effective_until,
            "compiled_at": self.compiled_at,
            "entries": [entry.to_dict() for entry in self.entries],
        }


@dataclass(frozen=True)
class DecisionInput:
    subject: str
    action: str
    resource: Mapping[str, Any]
    classification: str = "internal"
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "action": self.action,
            "resource": _json_safe(self.resource),
            "classification": self.classification,
            "context": _json_safe(self.context),
        }


@dataclass(frozen=True)
class PolicyDecision:
    decision_id: str
    policy_bundle_id: str | None
    policy_version: str | None
    policy_fingerprint: str | None
    input_hash: str
    subject: str
    action: str
    resource: Mapping[str, Any]
    classification: str
    context: Mapping[str, Any]
    effect: str
    reason_code: str
    matched_rule_ids: tuple[str, ...]
    obligations: tuple[Mapping[str, Any], ...]
    enforcement_point: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "policy_bundle_id": self.policy_bundle_id,
            "policy_version": self.policy_version,
            "policy_fingerprint": self.policy_fingerprint,
            "input_hash": self.input_hash,
            "subject": self.subject,
            "action": self.action,
            "resource": _json_safe(self.resource),
            "classification": self.classification,
            "context": _json_safe(self.context),
            "effect": self.effect,
            "reason_code": self.reason_code,
            "matched_rule_ids": list(self.matched_rule_ids),
            "obligations": [_json_safe(item) for item in self.obligations],
            "enforcement_point": self.enforcement_point,
        }


def compile_policy_graph(
    policy_graph: Mapping[str, Any],
    *,
    compiled_at: str = "1970-01-01T00:00:00Z",
    compiler_version: str = "nomos-compiler-v0",
) -> CompiledPolicyBundle:
    """Compile graph-native policy JSON into a deterministic bundle."""
    bundle_id = _required_str(policy_graph, "policy_bundle_id")
    version = _required_str(policy_graph, "policy_version")
    default_effect = _required_str(policy_graph, "default_effect")
    if default_effect not in EFFECTS:
        raise NomosPolicyError(
            f"default_effect must be one of {sorted(EFFECTS)}; "
            f"got {default_effect!r}"
        )
    raw_rules = policy_graph.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise NomosPolicyError("policy graph must contain a non-empty rules list")
    if policy_graph.get("vocabulary_strict"):
        try:
            validate_policy_graph_vocabulary(policy_graph)
        except NomosVocabularyError as exc:
            raise NomosPolicyError(str(exc)) from exc

    seen: set[str] = set()
    rule_signatures: dict[tuple[Any, ...], PolicyRule] = {}
    rules: list[PolicyRule] = []
    reason_catalog: dict[str, str] = {}
    for raw in raw_rules:
        if not isinstance(raw, Mapping):
            raise NomosPolicyError("each policy rule must be an object")
        rule = _parse_rule(raw)
        if rule.rule_id in seen:
            raise NomosPolicyError(f"duplicate policy rule id: {rule.rule_id}")
        seen.add(rule.rule_id)
        signature = (
            rule.priority,
            rule.actions,
            rule.subjects,
            json.dumps(_json_safe(rule.match), sort_keys=True),
        )
        previous = rule_signatures.get(signature)
        if previous is not None and previous.effect != rule.effect:
            raise NomosPolicyError(
                "conflicting policy rules share priority/actions/"
                "subjects/match without a deterministic tie-breaker: "
                f"{previous.rule_id!r} and {rule.rule_id!r}"
            )
        rule_signatures[signature] = rule
        rules.append(rule)
        reason_catalog[rule.reason_code or rule.rule_id] = rule.rule_id
    rules.sort(key=lambda rule: (-rule.priority, rule.rule_id))

    source_generation_id = policy_graph.get("source_generation_id")
    if source_generation_id is not None and not isinstance(
        source_generation_id,
        int,
    ):
        raise NomosPolicyError("source_generation_id must be an integer or null")

    scope = policy_graph.get("scope") or {}
    if not isinstance(scope, Mapping):
        raise NomosPolicyError("scope must be an object when supplied")

    effective_from = _optional_str(policy_graph, "effective_from")
    effective_until = _optional_str(policy_graph, "effective_until")
    fingerprint_payload = {
        "policy_bundle_id": bundle_id,
        "policy_version": version,
        "compiler_version": compiler_version,
        "source_generation_id": source_generation_id,
        "scope": _json_safe(scope),
        "default_effect": default_effect,
        "effective_from": effective_from,
        "effective_until": effective_until,
        "rules": [rule.to_dict() for rule in rules],
    }
    fingerprint = "sha256:" + _sha256_hex(fingerprint_payload)
    return CompiledPolicyBundle(
        policy_bundle_id=bundle_id,
        policy_version=version,
        policy_fingerprint=fingerprint,
        compiler_version=compiler_version,
        source_generation_id=source_generation_id,
        scope=dict(scope),
        default_effect=default_effect,
        reason_code_catalog=reason_catalog,
        effective_from=effective_from,
        effective_until=effective_until,
        compiled_at=compiled_at,
        rules=tuple(rules),
    )


def compile_config_graph(
    config_graph: Mapping[str, Any],
    *,
    compiled_at: str = "1970-01-01T00:00:00Z",
    compiler_version: str = "nomos-config-compiler-v0",
) -> CompiledConfigSet:
    """Compile graph-native configuration JSON into a deterministic set."""
    config_set_id = _required_str(config_graph, "config_set_id")
    version = _required_str(config_graph, "config_version")
    raw_entries = config_graph.get("entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise NomosPolicyError("config graph must contain a non-empty entries list")
    seen: set[str] = set()
    entries: list[ConfigEntry] = []
    for raw in raw_entries:
        if not isinstance(raw, Mapping):
            raise NomosPolicyError("each config entry must be an object")
        entry = _parse_config_entry(raw)
        if entry.config_key in seen:
            raise NomosPolicyError(f"duplicate config key: {entry.config_key}")
        seen.add(entry.config_key)
        entries.append(entry)
    entries.sort(key=lambda entry: entry.config_key)
    source_generation_id = config_graph.get("source_generation_id")
    if source_generation_id is not None and not isinstance(
        source_generation_id,
        int,
    ):
        raise NomosPolicyError("source_generation_id must be an integer or null")
    scope = config_graph.get("scope") or {}
    if not isinstance(scope, Mapping):
        raise NomosPolicyError("scope must be an object when supplied")
    effective_from = _optional_str(config_graph, "effective_from")
    effective_until = _optional_str(config_graph, "effective_until")
    fingerprint_payload = {
        "config_set_id": config_set_id,
        "config_version": version,
        "compiler_version": compiler_version,
        "source_generation_id": source_generation_id,
        "scope": _json_safe(scope),
        "effective_from": effective_from,
        "effective_until": effective_until,
        "entries": [entry.to_dict() for entry in entries],
    }
    fingerprint = "sha256:" + _sha256_hex(fingerprint_payload)
    return CompiledConfigSet(
        config_set_id=config_set_id,
        config_version=version,
        config_fingerprint=fingerprint,
        compiler_version=compiler_version,
        source_generation_id=source_generation_id,
        scope=dict(scope),
        effective_from=effective_from,
        effective_until=effective_until,
        compiled_at=compiled_at,
        entries=tuple(entries),
    )


def evaluate(
    bundle: CompiledPolicyBundle | None,
    decision_input: DecisionInput,
    *,
    enforcement_point: str = "nomos.evaluate",
) -> PolicyDecision:
    """Evaluate a bounded policy input against a compiled bundle."""
    input_hash = "sha256:" + _sha256_hex(decision_input.to_dict())
    if bundle is None:
        decision_seed = {
            "policy_fingerprint": None,
            "input_hash": input_hash,
            "effect": "deny",
            "reason_code": "nomos.default_deny.no_active_bundle",
        }
        return PolicyDecision(
            decision_id="nomos:decision:" + _sha256_hex(decision_seed)[:24],
            policy_bundle_id=None,
            policy_version=None,
            policy_fingerprint=None,
            input_hash=input_hash,
            subject=decision_input.subject,
            action=decision_input.action,
            resource=dict(decision_input.resource),
            classification=decision_input.classification,
            context=dict(decision_input.context),
            effect="deny",
            reason_code="nomos.default_deny.no_active_bundle",
            matched_rule_ids=(),
            obligations=(),
            enforcement_point=enforcement_point,
        )

    if _has_invalid_protected_identity_fact(decision_input):
        effect = "deny"
        reason_code = "nomos.invalid_identity_fact"
        matched_rule_ids = ()
        obligations = ()
    elif (
        matched := next(
            (
                rule
                for rule in bundle.rules
                if _rule_matches(rule, decision_input)
            ),
            None,
        )
    ) is None:
        effect = bundle.default_effect
        reason_code = f"nomos.default_{effect}"
        matched_rule_ids = ()
        obligations = ()
    else:
        effect = matched.effect
        reason_code = matched.reason_code or matched.rule_id
        matched_rule_ids = (matched.rule_id,)
        obligations = matched.obligations

    decision_seed = {
        "policy_fingerprint": bundle.policy_fingerprint,
        "input_hash": input_hash,
        "effect": effect,
        "reason_code": reason_code,
        "matched_rule_ids": list(matched_rule_ids),
    }
    return PolicyDecision(
        decision_id="nomos:decision:" + _sha256_hex(decision_seed)[:24],
        policy_bundle_id=bundle.policy_bundle_id,
        policy_version=bundle.policy_version,
        policy_fingerprint=bundle.policy_fingerprint,
        input_hash=input_hash,
        subject=decision_input.subject,
        action=decision_input.action,
        resource=dict(decision_input.resource),
        classification=decision_input.classification,
        context=dict(decision_input.context),
        effect=effect,
        reason_code=reason_code,
        matched_rule_ids=matched_rule_ids,
        obligations=obligations,
        enforcement_point=enforcement_point,
    )


def load_policy_graph(path: Path) -> Mapping[str, Any]:
    value = _load_json(path)
    if not isinstance(value, Mapping):
        raise NomosPolicyError(f"policy graph must be an object: {path}")
    return value


def load_config_graph(path: Path) -> Mapping[str, Any]:
    value = _load_json(path)
    if not isinstance(value, Mapping):
        raise NomosPolicyError(f"config graph must be an object: {path}")
    return value


def load_bundle(path: Path) -> CompiledPolicyBundle:
    value = _load_json(path)
    if not isinstance(value, Mapping):
        raise NomosPolicyError(f"compiled bundle must be an object: {path}")
    return policy_bundle_from_mapping(value)


def policy_bundle_from_mapping(value: Mapping[str, Any]) -> CompiledPolicyBundle:
    """Load a compiled policy bundle from an already-parsed mapping."""
    rules = tuple(_parse_rule(raw) for raw in value.get("rules") or [])
    fingerprint = _required_str(value, "policy_fingerprint")
    default_effect = _required_str(value, "default_effect")
    if default_effect not in EFFECTS:
        raise NomosPolicyError(f"invalid default_effect in bundle: {default_effect}")
    return CompiledPolicyBundle(
        policy_bundle_id=_required_str(value, "policy_bundle_id"),
        policy_version=_required_str(value, "policy_version"),
        policy_fingerprint=fingerprint,
        compiler_version=_required_str(value, "compiler_version"),
        source_generation_id=value.get("source_generation_id"),
        scope=value.get("scope") or {},
        default_effect=default_effect,
        reason_code_catalog=value.get("reason_code_catalog") or {},
        effective_from=value.get("effective_from"),
        effective_until=value.get("effective_until"),
        compiled_at=_required_str(value, "compiled_at"),
        rules=rules,
    )


def load_config_set(path: Path) -> CompiledConfigSet:
    value = _load_json(path)
    if not isinstance(value, Mapping):
        raise NomosPolicyError(f"compiled config set must be an object: {path}")
    return config_set_from_mapping(value)


def config_set_from_mapping(value: Mapping[str, Any]) -> CompiledConfigSet:
    """Load a compiled config set from an already-parsed mapping."""
    entries = tuple(_parse_config_entry(raw) for raw in value.get("entries") or [])
    fingerprint = _required_str(value, "config_fingerprint")
    return CompiledConfigSet(
        config_set_id=_required_str(value, "config_set_id"),
        config_version=_required_str(value, "config_version"),
        config_fingerprint=fingerprint,
        compiler_version=_required_str(value, "compiler_version"),
        source_generation_id=value.get("source_generation_id"),
        scope=value.get("scope") or {},
        effective_from=value.get("effective_from"),
        effective_until=value.get("effective_until"),
        compiled_at=_required_str(value, "compiled_at"),
        entries=entries,
    )


def load_decision_input(path: Path) -> DecisionInput:
    value = _load_json(path)
    if not isinstance(value, Mapping):
        raise NomosPolicyError(f"decision input must be an object: {path}")
    resource = value.get("resource") or {}
    context = value.get("context") or {}
    if not isinstance(resource, Mapping):
        raise NomosPolicyError("decision input resource must be an object")
    if not isinstance(context, Mapping):
        raise NomosPolicyError("decision input context must be an object")
    return DecisionInput(
        subject=_required_str(value, "subject"),
        action=_required_str(value, "action"),
        resource=resource,
        classification=str(value.get("classification") or "internal"),
        context=context,
    )


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_safe(value), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _parse_rule(raw: Mapping[str, Any]) -> PolicyRule:
    rule_id = _required_str(raw, "rule_id")
    effect = _required_str(raw, "effect")
    if effect not in EFFECTS:
        raise NomosPolicyError(
            f"rule {rule_id!r} effect must be one of {sorted(EFFECTS)}"
        )
    actions = _str_tuple(raw.get("actions"), field_name=f"{rule_id}.actions")
    subjects = _str_tuple(
        raw.get("subjects", ["*"]),
        field_name=f"{rule_id}.subjects",
    )
    match = raw.get("match") or {}
    if not isinstance(match, Mapping):
        raise NomosPolicyError(f"rule {rule_id!r} match must be an object")
    priority = raw.get("priority", 0)
    if not isinstance(priority, int):
        raise NomosPolicyError(f"rule {rule_id!r} priority must be an integer")
    obligations = raw.get("obligations") or []
    if not isinstance(obligations, list) or not all(
        isinstance(item, Mapping) for item in obligations
    ):
        raise NomosPolicyError(
            f"rule {rule_id!r} obligations must be a list of objects"
        )
    return PolicyRule(
        rule_id=rule_id,
        effect=effect,
        actions=actions,
        subjects=subjects,
        match=dict(match),
        priority=priority,
        reason_code=str(raw.get("reason_code") or rule_id),
        obligations=tuple(dict(item) for item in obligations),
    )


def _parse_config_entry(raw: Mapping[str, Any]) -> ConfigEntry:
    config_key = _required_str(raw, "config_key")
    value = raw.get("value")
    secret_ref = _optional_str(raw, "secret_ref")
    secret = bool(raw.get("secret") or raw.get("sensitive"))
    if secret and value is not None:
        raise NomosPolicyError(
            f"config entry {config_key!r} is secret and must use secret_ref"
        )
    if _looks_secret_key(config_key) and value is not None and secret_ref is None:
        raise NomosPolicyError(
            f"config entry {config_key!r} looks secret and must use secret_ref"
        )
    if value is None and secret_ref is None:
        raise NomosPolicyError(
            f"config entry {config_key!r} must define value or secret_ref"
        )
    classification = str(raw.get("classification") or "internal")
    scope = str(raw.get("scope") or "")
    owner = str(raw.get("owner") or "")
    metadata = raw.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        raise NomosPolicyError(f"config entry {config_key!r} metadata must be object")
    return ConfigEntry(
        config_key=config_key,
        value=value,
        secret_ref=secret_ref,
        classification=classification,
        scope=scope,
        owner=owner,
        metadata=dict(metadata),
    )


#: Substring markers that classify a config key as secret-bearing. This is the
#: single shared detector (per require-secret-refs-not-literals): the config
#: compiler, the source-registry write path, and the doctor literal-secret scan
#: all derive their secret-key vocabulary from here. `api_key`-class keys are
#: included so a literal value for `api_key` / `apikey` / `access_key` /
#: provider-named API keys is refused (the prior set omitted them).
SECRET_KEY_MARKERS: tuple[str, ...] = (
    "secret",
    "password",
    "passphrase",
    "token",
    "private_key",
    "credential",
    "api_key",
    "apikey",
    "access_key",
    "accesskey",
    "connection_string",
    "bearer",
)


def looks_secret_key(config_key: str) -> bool:
    """True if `config_key` names a secret-bearing value (shared detector)."""
    lowered = config_key.lower()
    return any(marker in lowered for marker in SECRET_KEY_MARKERS)


def _looks_secret_key(config_key: str) -> bool:
    return looks_secret_key(config_key)


def _has_invalid_protected_identity_fact(
    decision_input: DecisionInput,
) -> bool:
    protected_values = [decision_input.classification]
    for payload in (decision_input.resource, decision_input.context):
        protected_values.extend(
            value
            for key, value in payload.items()
            if key in _PROTECTED_IDENTITY_FACTS
        )
    return any(
        not isinstance(value, str) or not value.strip()
        for value in protected_values
    )


def _rule_matches(rule: PolicyRule, decision_input: DecisionInput) -> bool:
    if "*" not in rule.actions and decision_input.action not in rule.actions:
        return False
    if "*" not in rule.subjects and decision_input.subject not in rule.subjects:
        return False
    facts: dict[str, Any] = {
        "subject": decision_input.subject,
        "action": decision_input.action,
        "classification": decision_input.classification,
    }
    facts.update(decision_input.resource)
    facts.update(decision_input.context)
    for key, expected in rule.match.items():
        if key not in facts:
            return False
        if not _value_matches(facts[key], expected):
            return False
    return True


def _value_matches(actual: Any, expected: Any) -> bool:
    if isinstance(expected, list):
        expected_set = {json.dumps(_json_safe(v), sort_keys=True) for v in expected}
        if isinstance(actual, list):
            actual_set = {json.dumps(_json_safe(v), sort_keys=True) for v in actual}
            return actual_set.issubset(expected_set)
        return json.dumps(_json_safe(actual), sort_keys=True) in expected_set
    if isinstance(actual, list):
        return json.dumps(_json_safe(expected), sort_keys=True) in {
            json.dumps(_json_safe(v), sort_keys=True) for v in actual
        }
    return actual == expected


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.expanduser().read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise NomosPolicyError(f"invalid JSON in {path}: {exc}") from exc


def _required_str(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise NomosPolicyError(f"{key} is required and must be a non-empty string")
    return value


def _optional_str(mapping: Mapping[str, Any], key: str) -> str | None:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise NomosPolicyError(f"{key} must be a non-empty string or null")
    return value


def _str_tuple(value: Any, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise NomosPolicyError(f"{field_name} must be a list of strings")
    result = tuple(str(item) for item in value)
    if not result or any(not item for item in result):
        raise NomosPolicyError(f"{field_name} must be a non-empty list")
    return result


def _sha256_hex(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(v) for v in value]
    return value

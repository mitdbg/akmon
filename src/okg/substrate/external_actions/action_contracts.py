"""Closed action-contract values and RFC 8785-compatible digest helpers.

The implementation intentionally uses a tiny serializer instead of the
process-wide JSON encoder.  JCS sorts object keys by UTF-16 code units and
uses ECMAScript's number representation; both details matter when a digest is
used as an authorization binding.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_SAFE_INTEGER = 9_007_199_254_740_991
REQUEST_BODY_MAX = 262_144
CANONICAL_PAYLOAD_MAX = 131_072
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9._~:/+\-]{16,128}$")
TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:+\-]{1,128}$")
SUBJECT_RE = re.compile(r"^[A-Za-z0-9._:+\-]{1,256}$")
CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
RISK_TIERS = ("low", "moderate", "high", "critical")
OUTCOMES = ("succeeded", "failed", "indeterminate")
REPLAY_MODES = frozenset(
    {"none", "lookup_only", "idempotent_retry", "lookup_or_idempotent_retry"}
)


class ActionContractError(ValueError):
    """A fail-closed contract, payload, or canonicalization refusal."""

    def __init__(self, message: str, *, code: str = "invalid_contract") -> None:
        super().__init__(message)
        self.code = code


def _utf16_key(value: str) -> bytes:
    return value.encode("utf-16-be", "surrogatepass")


def _validate_json(value: Any, *, depth: int = 0, members: list[int] | None = None, arrays: list[int] | None = None, scalars: list[int] | None = None) -> None:
    if depth > 32:
        raise ActionContractError("JSON nesting depth exceeds 32", code="payload_limit")
    if members is None:
        members, arrays, scalars = [0], [0], [0]
    if value is None or isinstance(value, (bool, str, int, float)):
        scalars[0] += 1
        if scalars[0] > 4096:
            raise ActionContractError("JSON scalar count exceeds 4096", code="payload_limit")
        if isinstance(value, str):
            try:
                size = len(value.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise ActionContractError("invalid Unicode scalar", code="malformed_utf8") from exc
            if size > 16_384:
                raise ActionContractError("JSON string exceeds 16 KiB", code="payload_limit")
        elif isinstance(value, int) and not isinstance(value, bool):
            if abs(value) > MAX_SAFE_INTEGER:
                raise ActionContractError("integer is not I-JSON safe", code="unsupported_number")
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ActionContractError("non-finite number", code="unsupported_number")
        return
    if isinstance(value, Mapping):
        members[0] += len(value)
        if members[0] > 512:
            raise ActionContractError("JSON member count exceeds 512", code="payload_limit")
        for key, child in value.items():
            if not isinstance(key, str):
                raise ActionContractError("object keys must be strings", code="invalid_json")
            try:
                key_size = len(key.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise ActionContractError("invalid Unicode scalar", code="malformed_utf8") from exc
            if key_size > 16_384:
                raise ActionContractError("JSON string exceeds 16 KiB", code="payload_limit")
            _validate_json(child, depth=depth + 1, members=members, arrays=arrays, scalars=scalars)
        return
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        arrays[0] += len(value)
        if arrays[0] > 512:
            raise ActionContractError("JSON array item count exceeds 512", code="payload_limit")
        for child in value:
            _validate_json(child, depth=depth + 1, members=members, arrays=arrays, scalars=scalars)
        return
    raise ActionContractError(f"unsupported JSON value {type(value).__name__}", code="invalid_json")


def _number(value: int | float) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if not math.isfinite(value):
        raise ActionContractError("non-finite number", code="unsupported_number")
    if value == 0:
        return "0"
    negative = value < 0
    text = repr(abs(value))
    if "e" not in text and "E" not in text:
        if text.endswith(".0"):
            text = text[:-2]
        return ("-" if negative else "") + text
    mantissa, exponent = re.split("[eE]", text)
    exponent_value = int(exponent)
    digits = mantissa.replace(".", "")
    integer_digits = len(mantissa.split(".", 1)[0])
    decimal_exponent = exponent_value + integer_digits - 1
    # ECMAScript chooses fixed notation for -6 <= n < 21.
    if -6 <= decimal_exponent < 21:
        point = decimal_exponent + 1
        if point <= 0:
            rendered = "0." + ("0" * (-point)) + digits
        elif point >= len(digits):
            rendered = digits + ("0" * (point - len(digits)))
        else:
            rendered = digits[:point] + "." + digits[point:]
        rendered = rendered.rstrip("0").rstrip(".") if "." in rendered else rendered
        return ("-" if negative else "") + rendered
    exponent_sign = "+" if decimal_exponent >= 0 else "-"
    exponent_digits = str(abs(decimal_exponent))
    rendered = digits[0]
    if len(digits) > 1:
        rendered += "." + digits[1:].rstrip("0")
    return ("-" if negative else "") + rendered + "e" + exponent_sign + exponent_digits


def _jcs(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _number(value)
    if isinstance(value, str):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        except (UnicodeEncodeError, ValueError) as exc:
            raise ActionContractError("string cannot be represented as UTF-8", code="malformed_utf8") from exc
    if isinstance(value, Mapping):
        entries = []
        for key in sorted(value, key=_utf16_key):
            if not isinstance(key, str):
                raise ActionContractError("object keys must be strings", code="invalid_json")
            entries.append(_jcs(key) + ":" + _jcs(value[key]))
        return "{" + ",".join(entries) + "}"
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return "[" + ",".join(_jcs(item) for item in value) + "]"
    raise ActionContractError(f"unsupported JSON value {type(value).__name__}", code="invalid_json")


def canonical_json_bytes(value: Any) -> bytes:
    """Return UTF-8 RFC 8785 JCS bytes after I-JSON/size validation."""

    _validate_json(value)
    try:
        encoded = _jcs(value).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ActionContractError("value is not valid UTF-8", code="malformed_utf8") from exc
    return encoded


def canonical_json(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ActionContractError("duplicate JSON object key", code="duplicate_key")
        result[key] = value
    return result


def strict_json_loads(data: bytes | str) -> Any:
    """Parse one strict I-JSON document, rejecting duplicates and NaN."""

    if isinstance(data, bytes):
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ActionContractError("JSON is not UTF-8", code="malformed_utf8") from exc
    if not isinstance(data, str):
        raise ActionContractError("JSON input must be bytes or text", code="invalid_json")
    try:
        value = json.loads(
            data,
            object_pairs_hook=_no_duplicates,
            parse_int=lambda raw: _parse_int(raw),
            parse_float=lambda raw: _parse_float(raw),
            parse_constant=lambda raw: _reject_constant(raw),
        )
    except ActionContractError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActionContractError("malformed JSON", code="invalid_json") from exc
    _validate_json(value)
    return value


def _parse_int(raw: str) -> int:
    result = int(raw)
    if abs(result) > MAX_SAFE_INTEGER:
        raise ActionContractError("integer is not I-JSON safe", code="unsupported_number")
    return result


def _parse_float(raw: str) -> float:
    result = float(raw)
    if not math.isfinite(result):
        raise ActionContractError("non-finite number", code="unsupported_number")
    return result


def _reject_constant(raw: str) -> Any:
    raise ActionContractError(f"non-finite number {raw}", code="unsupported_number")


def digest_bytes(domain: str, data: bytes) -> str:
    if not isinstance(domain, str) or not domain or "\x00" in domain:
        raise ActionContractError("hash domain must be non-empty and NUL-free", code="invalid_domain")
    if not isinstance(data, bytes):
        raise ActionContractError("hash input must be bytes", code="invalid_hash_input")
    try:
        domain_bytes = domain.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ActionContractError("hash domain must be valid UTF-8", code="invalid_domain") from exc
    return "sha256:" + hashlib.sha256(domain_bytes + b"\0" + data).hexdigest()


def digest_value(domain: str, value: Any) -> str:
    return digest_bytes(domain, canonical_json_bytes(value))


def validate_digest(value: str, *, field: str = "digest") -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ActionContractError(f"{field} must be sha256:<64 lowercase hex>", code="invalid_digest")
    return value


def _timestamp(value: datetime | str) -> str:
    if isinstance(value, str):
        if not TIMESTAMP_RE.fullmatch(value):
            raise ActionContractError("timestamp must use six fractional digits and Z", code="invalid_timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ActionContractError("timestamp must be RFC-3339", code="invalid_timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise ActionContractError("timestamp must be datetime or text", code="invalid_timestamp")
    if parsed.tzinfo is None:
        raise ActionContractError("timestamp must include UTC timezone", code="invalid_timestamp")
    parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def idempotency_key_hash(key: str) -> str:
    if not isinstance(key, str) or not IDEMPOTENCY_KEY_RE.fullmatch(key):
        raise ActionContractError("invalid Idempotency-Key", code="invalid_idempotency_key")
    return digest_value("okg-external-action-idempotency-key-v1", key)


def effect_instance_key(deployment: str, proposer_separation_subject: str, submission_idempotency_key_hash: str) -> str:
    validate_digest(submission_idempotency_key_hash, field="submission_idempotency_key_hash")
    return digest_value(
        "okg-external-action-effect-instance-v1",
        {
            "deployment": deployment,
            "proposer_separation_subject": proposer_separation_subject,
            "submission_idempotency_key_hash": submission_idempotency_key_hash,
        },
    )


def submission_content_hash(value: Mapping[str, Any]) -> str:
    fields = ("automation_id", "automation_version", "run_id", "correlation_id", "action_type", "target_ref", "risk_tier", "action_schema_digest", "action_contract_digest", "original_payload_hash", "requested_review_expires_at", "summary_code", "evidence_digests")
    return digest_value("okg-external-action-submission-content-v1", _closed_mapping(value, fields, name="submission_content"))


def _provider_idempotency_digest_bytes(value: Mapping[str, Any]) -> bytes:
    fields = ("deployment", "effect_instance_key", "approved_payload_hash", "action_contract_digest", "provider_connection_contract_digest", "provider_operation_id", "provider_operation_descriptor_digest")
    raw = canonical_json_bytes(_closed_mapping(value, fields, name="provider_idempotency_binding"))
    return hashlib.sha256(b"okg-external-action-effect-v1\0" + raw).digest()


def provider_idempotency_key(value: Mapping[str, Any]) -> str:
    digest = _provider_idempotency_digest_bytes(value)
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def provider_idempotency_key_digest(value: Mapping[str, Any]) -> str:
    """Return the audit rendering of the provider replay-key digest."""

    return "sha256:" + _provider_idempotency_digest_bytes(value).hex()


def target_matches(target: str, constraint: Mapping[str, Any]) -> bool:
    """Evaluate only the closed ``ascii_prefix_decimal_v1`` matcher."""

    if not isinstance(target, str) or not isinstance(constraint, Mapping):
        return False
    if constraint.get("kind") != "ascii_prefix_decimal_v1":
        return False
    try:
        prefix = constraint["prefix"]
        minimum = constraint["decimal_digits_minimum"]
        maximum = constraint["decimal_digits_maximum"]
        if constraint["full_match"] is not True or constraint["ascii_only"] is not True:
            return False
        if constraint["normalization"] != "none" or constraint["input_encoding"] != "UTF-8":
            return False
        if not isinstance(prefix, str) or not isinstance(minimum, int) or not isinstance(maximum, int):
            return False
        if minimum < 0 or maximum < minimum:
            return False
        raw = target.encode("utf-8")
        prefix_bytes = prefix.encode("ascii")
    except (KeyError, UnicodeEncodeError):
        return False
    if not raw.startswith(prefix_bytes):
        return False
    suffix = raw[len(prefix_bytes) :]
    if not (minimum <= len(suffix) <= maximum):
        return False
    if constraint.get("leading_zeroes") == "forbidden" and len(suffix) > 1 and suffix[0] == 0x30:
        return False
    return bool(suffix) and all(0x30 <= byte <= 0x39 for byte in suffix)


def _closed_mapping(value: Mapping[str, Any], fields: Sequence[str], *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ActionContractError(f"{name} must be an object")
    expected = set(fields)
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown or missing:
        raise ActionContractError(f"{name} has unknown or missing fields", code="invalid_contract")
    return {field: value[field] for field in fields}


@dataclass(frozen=True, slots=True)
class ReviewExecutionLimits:
    maximum_execution_window_seconds: int
    maximum_review_lifetime_seconds: int
    reporting_attention_seconds: int
    reconcile_not_before_seconds: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReviewExecutionLimits":
        fields = ("maximum_execution_window_seconds", "maximum_review_lifetime_seconds", "reporting_attention_seconds", "reconcile_not_before_seconds")
        row = _closed_mapping(value, fields, name="review_and_execution_limits")
        maximums = {
            "maximum_execution_window_seconds": 86_400,
            "maximum_review_lifetime_seconds": 604_800,
            "reporting_attention_seconds": 604_800,
            "reconcile_not_before_seconds": 2_592_000,
        }
        for name, number in row.items():
            if type(number) is not int or not 1 <= number <= maximums[name]:
                raise ActionContractError(f"invalid {name}", code="invalid_contract")
        if row["maximum_execution_window_seconds"] > row["maximum_review_lifetime_seconds"]:
            raise ActionContractError("execution window exceeds review lifetime", code="invalid_contract")
        return cls(**row)

    def to_mapping(self) -> dict[str, int]:
        return {
            "maximum_execution_window_seconds": self.maximum_execution_window_seconds,
            "maximum_review_lifetime_seconds": self.maximum_review_lifetime_seconds,
            "reporting_attention_seconds": self.reporting_attention_seconds,
            "reconcile_not_before_seconds": self.reconcile_not_before_seconds,
        }


@dataclass(frozen=True, slots=True)
class ActionContract:
    """The complete immutable registry entry used by a proposal binding."""

    action_type: str
    action_schema_digest: str
    executor_contract_digest: str
    payload_renderer_digest: str
    governed_tool_binding_ref: str
    governed_tool_binding_digest: str
    provider_connection_ref: str
    provider_connection_contract_digest: str
    provider_capability_digest: str
    provider_operation_id: str
    provider_operation_descriptor_digest: str
    provider_replay_mode: str
    target_constraint: Mapping[str, Any]
    minimum_risk_tier: str
    review_and_execution_limits: ReviewExecutionLimits
    diff_field_codes: tuple[str, ...]
    allowed_summary_codes: tuple[str, ...]
    transition_reason_codes: tuple[str, ...]
    outcome_reason_rules: Mapping[str, Mapping[str, Any]]
    payload_schema: Mapping[str, Any] | None = None

    DIGEST_FIELDS = (
        "action_type", "action_schema_digest", "executor_contract_digest", "payload_renderer_digest",
        "governed_tool_binding_ref", "governed_tool_binding_digest", "provider_connection_ref",
        "provider_connection_contract_digest", "provider_capability_digest", "provider_operation_id",
        "provider_operation_descriptor_digest", "provider_replay_mode", "target_constraint",
        "minimum_risk_tier", "review_and_execution_limits", "diff_field_codes", "allowed_summary_codes",
        "transition_reason_codes", "outcome_reason_rules",
    )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, payload_schema: Mapping[str, Any] | None = None) -> "ActionContract":
        row = _closed_mapping(value, cls.DIGEST_FIELDS, name="action_contract")
        for field in (
            "action_schema_digest", "executor_contract_digest", "payload_renderer_digest", "governed_tool_binding_digest",
            "provider_connection_contract_digest", "provider_capability_digest", "provider_operation_descriptor_digest",
        ):
            validate_digest(row[field], field=field)
        if row["provider_replay_mode"] not in REPLAY_MODES:
            raise ActionContractError("unknown provider replay mode", code="invalid_contract")
        if row["minimum_risk_tier"] not in RISK_TIERS:
            raise ActionContractError("unknown minimum risk tier", code="invalid_contract")
        limits = ReviewExecutionLimits.from_mapping(row["review_and_execution_limits"])
        code_sets: dict[str, tuple[str, ...]] = {}
        for name in ("diff_field_codes", "allowed_summary_codes", "transition_reason_codes"):
            values = row[name]
            if not isinstance(values, list) or len(values) > 128 or values != sorted(set(values)):
                raise ActionContractError(f"{name} must be sorted and unique", code="invalid_contract")
            if any(not isinstance(item, str) or not CODE_RE.fullmatch(item) for item in values):
                raise ActionContractError(f"invalid {name} code", code="invalid_contract")
            code_sets[name] = tuple(values)
        rules = row["outcome_reason_rules"]
        if not isinstance(rules, Mapping) or not rules:
            raise ActionContractError("outcome_reason_rules must be nonempty", code="invalid_contract")
        checked_rules: dict[str, Mapping[str, Any]] = {}
        for key, rule in rules.items():
            if not isinstance(key, str) or not CODE_RE.fullmatch(key) or not isinstance(rule, Mapping):
                raise ActionContractError("invalid outcome reason rule", code="invalid_contract")
            allowed = rule.get("allowed_outcomes")
            receipt = rule.get("provider_receipt")
            if set(rule) != {"allowed_outcomes", "provider_receipt"} or not isinstance(allowed, list) or not allowed or receipt not in {"required", "forbidden"}:
                raise ActionContractError("invalid outcome reason rule fields", code="invalid_contract")
            if any(item not in OUTCOMES for item in allowed) or allowed != sorted(set(allowed), key=OUTCOMES.index):
                raise ActionContractError("invalid outcome vocabulary", code="invalid_contract")
            checked_rules[key] = {"allowed_outcomes": list(allowed), "provider_receipt": receipt}
        target = row["target_constraint"]
        target_fields = ("kind", "prefix", "decimal_digits_minimum", "decimal_digits_maximum", "leading_zeroes", "full_match", "normalization", "input_encoding", "ascii_only")
        if not isinstance(target, Mapping) or set(target) != set(target_fields) or target.get("kind") != "ascii_prefix_decimal_v1":
            raise ActionContractError("unsupported target constraint", code="invalid_contract")
        if (
            not isinstance(target["prefix"], str)
            or not isinstance(target["decimal_digits_minimum"], int)
            or not isinstance(target["decimal_digits_maximum"], int)
            or target["decimal_digits_minimum"] < 0
            or target["decimal_digits_maximum"] < target["decimal_digits_minimum"]
            or target["leading_zeroes"] not in {"allowed", "forbidden"}
            or target["full_match"] is not True
            or target["normalization"] != "none"
            or target["input_encoding"] != "UTF-8"
            or target["ascii_only"] is not True
        ):
            raise ActionContractError("invalid target constraint", code="invalid_contract")
        return cls(
            action_type=row["action_type"], action_schema_digest=row["action_schema_digest"],
            executor_contract_digest=row["executor_contract_digest"], payload_renderer_digest=row["payload_renderer_digest"],
            governed_tool_binding_ref=row["governed_tool_binding_ref"], governed_tool_binding_digest=row["governed_tool_binding_digest"],
            provider_connection_ref=row["provider_connection_ref"], provider_connection_contract_digest=row["provider_connection_contract_digest"],
            provider_capability_digest=row["provider_capability_digest"], provider_operation_id=row["provider_operation_id"],
            provider_operation_descriptor_digest=row["provider_operation_descriptor_digest"], provider_replay_mode=row["provider_replay_mode"],
            target_constraint=dict(target), minimum_risk_tier=row["minimum_risk_tier"], review_and_execution_limits=limits,
            diff_field_codes=code_sets["diff_field_codes"], allowed_summary_codes=code_sets["allowed_summary_codes"],
            transition_reason_codes=code_sets["transition_reason_codes"], outcome_reason_rules=checked_rules,
            payload_schema=payload_schema,
        )

    @property
    def action_contract_digest(self) -> str:
        return digest_value("okg-external-action-contract-v1", self.to_mapping())

    def to_mapping(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "action_schema_digest": self.action_schema_digest,
            "executor_contract_digest": self.executor_contract_digest,
            "payload_renderer_digest": self.payload_renderer_digest,
            "governed_tool_binding_ref": self.governed_tool_binding_ref,
            "governed_tool_binding_digest": self.governed_tool_binding_digest,
            "provider_connection_ref": self.provider_connection_ref,
            "provider_connection_contract_digest": self.provider_connection_contract_digest,
            "provider_capability_digest": self.provider_capability_digest,
            "provider_operation_id": self.provider_operation_id,
            "provider_operation_descriptor_digest": self.provider_operation_descriptor_digest,
            "provider_replay_mode": self.provider_replay_mode,
            "target_constraint": dict(self.target_constraint),
            "minimum_risk_tier": self.minimum_risk_tier,
            "review_and_execution_limits": self.review_and_execution_limits.to_mapping(),
            "diff_field_codes": list(self.diff_field_codes),
            "allowed_summary_codes": list(self.allowed_summary_codes),
            "transition_reason_codes": list(self.transition_reason_codes),
            "outcome_reason_rules": {key: dict(value) for key, value in self.outcome_reason_rules.items()},
        }

    def validate_payload(self, payload: Any) -> bytes:
        canonical = canonical_json_bytes(payload)
        if len(canonical) > CANONICAL_PAYLOAD_MAX:
            raise ActionContractError("canonical payload exceeds 128 KiB", code="payload_limit")
        if self.payload_schema is not None:
            try:
                from jsonschema import Draft202012Validator

                errors = sorted(Draft202012Validator(self.payload_schema).iter_errors(payload), key=lambda error: list(error.path))
            except Exception as exc:
                raise ActionContractError("action schema authority unavailable", code="review_authority_unavailable") from exc
            if errors:
                raise ActionContractError("payload does not match action schema", code="invalid_proposal")
        return canonical

    def validate_submission(self, *, target_ref: str, risk_tier: str, summary_code: str, payload: Any, action_contract_digest: str | None = None) -> bytes:
        if action_contract_digest is not None and action_contract_digest != self.action_contract_digest:
            raise ActionContractError("action contract digest mismatch", code="invalid_proposal")
        if not target_matches(target_ref, self.target_constraint):
            raise ActionContractError("target does not match action contract", code="invalid_proposal")
        if risk_tier not in RISK_TIERS or RISK_TIERS.index(risk_tier) < RISK_TIERS.index(self.minimum_risk_tier):
            raise ActionContractError("risk tier is below action contract minimum", code="invalid_proposal")
        if summary_code not in self.allowed_summary_codes:
            raise ActionContractError("summary code is not allowed", code="invalid_proposal")
        return self.validate_payload(payload)


def action_contract_digest(value: Mapping[str, Any]) -> str:
    return digest_value("okg-external-action-contract-v1", dict(value))


def decision_binding_hash(value: Mapping[str, Any]) -> str:
    return digest_value("okg-external-action-decision-binding-v1", dict(value))


def execution_binding_hash(value: Mapping[str, Any]) -> str:
    return digest_value("okg-external-action-execution-binding-v1", dict(value))


def transition_request_hash(value: Mapping[str, Any]) -> str:
    return digest_value("okg-external-action-transition-request-v1", dict(value))


def outcome_request_hash(value: Mapping[str, Any]) -> str:
    return digest_value("okg-external-action-outcome-request-v1", dict(value))


def event_hash(value: Mapping[str, Any]) -> str:
    return digest_value("okg-external-action-event-v1", dict(value))


def format_timestamp(value: datetime | str) -> str:
    return _timestamp(value)


def _deployment_product_ref(value: Any, *, field: str) -> str:
    """Render the #1182 owner/kind/id authority without ambiguity."""

    parts = (getattr(value, "owner", None), getattr(value, "kind", None), getattr(value, "ref", None))
    if any(
        not isinstance(part, str)
        or not part
        or ":" in part
        or not SAFE_IDENTIFIER_RE.fullmatch(part)
        for part in parts
    ):
        raise ActionContractError(f"{field} is not an unambiguous owner:kind:id reference")
    return ":".join(parts)


def _deployment_product_ref_projection(value: Any) -> dict[str, Any]:
    return {
        "owner": value.owner,
        "id": value.ref,
        "kind": value.kind,
        "version": value.version,
        "digest": value.digest,
    }


def _concrete_contract(value: Any, fields: Sequence[str], *, name: str) -> dict[str, Any]:
    return _closed_mapping(value, fields, name=name)


def resolve_action_contracts(
    source: Mapping[str, Any] | str | Path,
    *,
    authored_definition: Any,
    resolved_lock: Any,
) -> dict[str, ActionContract]:
    """Resolve a registry against one authored definition and canonical lock.

    This is a pure authority check.  It reads no secret, imports no provider
    adapter, and can never perform the reviewed external effect.
    """

    try:
        from jsonschema import Draft202012Validator
        from okg.substrate.contracts.deployment_product import (
            AuthoredDeploymentDefinition,
            ResolvedLock,
            authored_definition_digest,
            canonicalize_lock,
            connection_capability_digest,
            connection_contract_digest,
        )
        from .config import load_action_contracts

        raw = load_action_contracts(source)
        definition_input = (
            authored_definition.model_dump(mode="python", round_trip=True)
            if isinstance(authored_definition, AuthoredDeploymentDefinition)
            else authored_definition
        )
        lock_input = (
            resolved_lock.model_dump(mode="python", round_trip=True)
            if isinstance(resolved_lock, ResolvedLock)
            else resolved_lock
        )
        definition = AuthoredDeploymentDefinition.model_validate(definition_input)
        lock = canonicalize_lock(ResolvedLock.model_validate(lock_input))
    except ActionContractError:
        raise
    except Exception as exc:
        raise ActionContractError("deployment-product action authority is invalid") from exc

    if definition.definition_id != lock.definition_id:
        raise ActionContractError("definition and resolved lock identify different deployments")
    definition_digest = authored_definition_digest(definition)
    if definition_digest != lock.definition_digest:
        raise ActionContractError("authored definition digest does not match resolved lock")

    actions = raw.get("actions")
    if not isinstance(actions, list) or not actions:
        raise ActionContractError("action registry must contain actions")
    compiled: dict[str, ActionContract] = {}
    concrete_fields = {
        "action_schema",
        "executor_contract",
        "payload_renderer",
        "provider_operation_descriptor",
    }
    for value in actions:
        if not isinstance(value, Mapping):
            raise ActionContractError("action registry entry must be an object")
        if set(value) != set(ActionContract.DIGEST_FIELDS) | concrete_fields:
            raise ActionContractError("action registry entry has unknown or missing fields")
        contract_values = {field: value[field] for field in ActionContract.DIGEST_FIELDS}

        action_schema = value["action_schema"]
        if not isinstance(action_schema, Mapping):
            raise ActionContractError("action_schema must be an object")
        try:
            Draft202012Validator.check_schema(action_schema)
        except Exception as exc:
            raise ActionContractError("action_schema is not valid JSON Schema") from exc
        if digest_value("okg-external-action-schema-v1", action_schema) != contract_values["action_schema_digest"]:
            raise ActionContractError("action schema digest mismatch")

        executor = _concrete_contract(
            value["executor_contract"],
            ("journal_protocol", "provider_replay_mode", "worker_protocol"),
            name="executor_contract",
        )
        if executor["worker_protocol"] != "okg-external-action-worker/v1" or executor["journal_protocol"] != "okg-external-action-worker-journal/v1":
            raise ActionContractError("executor contract uses an unsupported protocol")
        if executor["provider_replay_mode"] != contract_values["provider_replay_mode"]:
            raise ActionContractError("executor replay mode mismatch")
        if digest_value("okg-external-action-executor-contract-v1", executor) != contract_values["executor_contract_digest"]:
            raise ActionContractError("executor contract digest mismatch")

        renderer = _concrete_contract(
            value["payload_renderer"],
            ("version", "method", "operation", "target", "body"),
            name="payload_renderer",
        )
        if (
            renderer["version"] != 1
            or renderer["method"] not in {"POST", "PUT", "PATCH", "DELETE"}
            or renderer["operation"] != contract_values["provider_operation_id"]
            or renderer["target"] != "target_ref"
            or renderer["body"] != "canonical_payload_bytes"
        ):
            raise ActionContractError("payload renderer semantics are invalid")
        if digest_value("okg-external-action-payload-renderer-v1", renderer) != contract_values["payload_renderer_digest"]:
            raise ActionContractError("payload renderer digest mismatch")

        descriptor = _concrete_contract(
            value["provider_operation_descriptor"],
            (
                "operation_id", "effect_kind", "effect_phase", "required_capabilities",
                "request_media_type", "executor_contract_digest", "payload_renderer_digest",
                "provider_replay_mode",
            ),
            name="provider_operation_descriptor",
        )
        capabilities = descriptor["required_capabilities"]
        if (
            descriptor["operation_id"] != contract_values["provider_operation_id"]
            or descriptor["effect_kind"] != "write"
            or descriptor["effect_phase"] != "execute"
            or descriptor["request_media_type"] != "application/json"
            or descriptor["executor_contract_digest"] != contract_values["executor_contract_digest"]
            or descriptor["payload_renderer_digest"] != contract_values["payload_renderer_digest"]
            or descriptor["provider_replay_mode"] != contract_values["provider_replay_mode"]
            or not isinstance(capabilities, list)
            or not capabilities
            or capabilities != sorted(set(capabilities))
            or any(not isinstance(item, str) or not SAFE_IDENTIFIER_RE.fullmatch(item) for item in capabilities)
        ):
            raise ActionContractError("provider operation descriptor semantics are invalid")
        if digest_value("okg-external-action-provider-operation-v1", descriptor) != contract_values["provider_operation_descriptor_digest"]:
            raise ActionContractError("provider operation descriptor digest mismatch")

        binding_matches = [
            item
            for item in definition.tool_bindings
            if _deployment_product_ref(item.tool_ref, field="governed_tool_binding_ref")
            == contract_values["governed_tool_binding_ref"]
        ]
        if len(binding_matches) != 1 or not binding_matches[0].enabled:
            raise ActionContractError("governed tool binding is missing, duplicate, or disabled")
        binding = binding_matches[0]
        connection_matches = [
            item
            for item in definition.provider_connections
            if _deployment_product_ref(item.reference, field="provider_connection_ref")
            == contract_values["provider_connection_ref"]
        ]
        locked_matches = [
            item
            for item in lock.connection_contracts
            if _deployment_product_ref(item.connection_ref, field="provider_connection_ref")
            == contract_values["provider_connection_ref"]
        ]
        if len(connection_matches) != 1 or len(locked_matches) != 1:
            raise ActionContractError("provider connection authority is missing or duplicate")
        connection, locked = connection_matches[0], locked_matches[0]
        if binding.connection_ref != connection.reference or locked.connection_ref != connection.reference:
            raise ActionContractError("tool binding, connection, and lock references differ")

        observed_contract_digest = connection_contract_digest(connection)
        observed_capability_digest = connection_capability_digest(connection)
        if (
            observed_contract_digest != locked.contract_digest
            or observed_contract_digest != contract_values["provider_connection_contract_digest"]
            or observed_capability_digest != locked.capability_digest
            or observed_capability_digest != contract_values["provider_capability_digest"]
        ):
            raise ActionContractError("provider connection digest authority mismatch")
        required_capabilities = set(capabilities)
        binding_capabilities = set(binding.required_capabilities)
        connection_capabilities = set(connection.capabilities)
        if not binding_capabilities <= connection_capabilities or not required_capabilities <= binding_capabilities or not required_capabilities <= connection_capabilities:
            raise ActionContractError("provider operation capabilities exceed the governed authority")

        binding_projection = {
            "definition_digest": lock.definition_digest,
            "binding_id": binding.binding_id,
            "connection_ref": _deployment_product_ref_projection(binding.connection_ref),
            "enabled": binding.enabled,
            "config": dict(binding.config),
            "readiness_policy": binding.readiness_policy,
            "refresh_identity": binding.refresh_identity,
            "required_capabilities": list(binding.required_capabilities),
            "binding_kind": binding.binding_kind,
            "tool_ref": _deployment_product_ref_projection(binding.tool_ref),
        }
        if digest_value("okg-external-action-governed-tool-binding-v1", binding_projection) != contract_values["governed_tool_binding_digest"]:
            raise ActionContractError("governed tool binding digest mismatch")

        contract = ActionContract.from_mapping(contract_values, payload_schema=action_schema)
        if contract.action_type in compiled:
            raise ActionContractError("duplicate action type")
        compiled[contract.action_type] = contract
    return compiled


__all__ = [
    "ActionContract", "ActionContractError", "ReviewExecutionLimits", "RISK_TIERS", "OUTCOMES",
    "canonical_json", "canonical_json_bytes", "strict_json_loads", "digest_bytes", "digest_value",
    "validate_digest", "format_timestamp", "action_contract_digest", "decision_binding_hash",
    "execution_binding_hash", "transition_request_hash", "outcome_request_hash", "event_hash",
    "effect_instance_key", "idempotency_key_hash", "provider_idempotency_key",
    "provider_idempotency_key_digest", "submission_content_hash",
    "target_matches", "resolve_action_contracts",
]

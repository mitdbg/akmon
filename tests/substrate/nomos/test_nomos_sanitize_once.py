"""Issue #2784: each Nomos decision is sanitized once, and nothing leaks.

The per-row read filter used to clean every decision's resource, context and
obligations five times (the evaluated decision, the outcome, the trace, and the
persist, on top of the request itself). These tests pin the new contract:

* the row's resource is walked exactly once per evaluated decision, and the
  outcome, trace and persist reuse that sanitized value;
* a decision that did not come out of the sanitizer -- built by hand, or copied
  with ``dataclasses.replace`` -- is still sanitized in full before it is
  written, so the reuse can never let a raw value through;
* no secret-looking value appears unsanitized in the persisted row, the
  returned decision, the outcome trace, or the log;
* decision outcomes are the ones the policy engine computes on the sanitized
  input, across a representative policy set.

Only storage is replaced: the active bundle is served from memory and the
batched decision INSERT goes to a cursor that JSON-encodes and keeps each row.
"""
from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import logging
import random
import types
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.nomos import read_filter, runtime_enforcement, store
from okg.substrate.nomos.core import (
    CompiledPolicyBundle,
    DecisionInput,
    compile_policy_graph,
    evaluate,
)
from okg.substrate.nomos.identity_store import (
    batched_decision_persist,
    persist_nomos_decision,
)
from okg.substrate.nomos.runtime_enforcement import (
    NomosPrimitiveCheck,
    NomosRuntimeRequest,
    evaluate_runtime_boundary,
    is_sanitized_policy_decision,
    runtime_surface_mapping_for,
    sanitize_policy_decision,
    sanitize_trace_mapping,
)
from okg.substrate.operator_read import (
    LOCAL_OPERATOR_READ_ACTION,
    LOCAL_OPERATOR_READ_SURFACE,
)

DEPLOYMENT = "sanitize-once-test"
SUBJECT = "subject:reader"
RUNTIME_SURFACE = "graph.read.local_operator"

#: Values the sanitizer must never let through, each for a different reason.
SECRET_LITERALS = (
    "sk-live-2784SECRETVALUE",  # credential-shaped literal
    "ghp_2784SECRETGITHUBTOKEN",  # credential-shaped literal
    "secret://vault/2784-db-password",  # secret reference
    "/Users/alice/2784-private-notes.txt",  # local path
    "123-45-2784",  # SSN-shaped
)
#: A harmless-looking value that is secret only because of its key.
KEYED_SECRET = "plain-value-under-an-api-key-2784"
ALL_SECRETS = (*SECRET_LITERALS, KEYED_SECRET)


def _secret_bag() -> dict[str, Any]:
    return {
        "literal_values": list(SECRET_LITERALS),
        "api_key": KEYED_SECRET,
        "nested": {"password": KEYED_SECRET, "path": SECRET_LITERALS[3]},
    }


def _bundle(*, default: str = "allow", rules: list[dict[str, Any]] | None = None) -> CompiledPolicyBundle:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:sanitize-once",
        "policy_version": "v1",
        "default_effect": default,
        "rules": rules if rules is not None else [
            {
                "rule_id": "audit-graph-objects",
                "effect": "allow",
                "actions": ["*"],
                "subjects": ["*"],
                "match": {"resource_kind": "graph_object"},
                "priority": 2,
                "reason_code": "nomos.test.audit",
                # Obligation parameters come from the policy, not the input:
                # they carry secrets too and must be sanitized as well.
                "obligations": [{"type": "audit", **_secret_bag()}],
            },
            {
                "rule_id": "redact-docs",
                "effect": "redact",
                "actions": ["*"],
                "subjects": ["*"],
                "match": {"source_id": "docs"},
                "priority": 1,
                "reason_code": "nomos.test.redact",
                "obligations": [{"type": "redaction", "token": KEYED_SECRET}],
            },
        ],
    })


class _Cursor:
    def __init__(self, sink: list[str]) -> None:
        self.sink = sink

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        rows = params.get("rows")
        if rows is not None:
            self.sink.extend(json.dumps(item.obj, default=str) for item in rows)
        else:
            self.sink.append(json.dumps(
                {key: getattr(value, "obj", value) for key, value in params.items()},
                default=str,
            ))


class _Conn:
    """Stands in for the connection: records every decision row it is sent."""

    def __init__(self) -> None:
        self.written: list[str] = []

    def cursor(self, **_: Any) -> _Cursor:
        return _Cursor(self.written)

    def commit(self) -> None:
        pass


@pytest.fixture
def storage(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    state = SimpleNamespace(bundle=_bundle(), mode="enforce", outcomes=[])
    real_boundary = store.evaluate_runtime_boundary

    def boundary(*args: Any, **kwargs: Any) -> Any:
        outcome = real_boundary(*args, **kwargs)
        state.outcomes.append(outcome)
        return outcome

    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: True)
    monkeypatch.setattr(read_filter, "apply_effective_attrs_to_rows", lambda conn, rows, **_: rows)
    monkeypatch.setattr(store, "load_active_policy_bundle", lambda conn, **_: state.bundle)
    monkeypatch.setattr(store, "deployment_nomos_enforcement_mode", lambda name: state.mode)
    monkeypatch.setattr(store, "evaluate_runtime_boundary", boundary)
    return state


def _session(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "deployment_name": DEPLOYMENT,
        "generation_id": "gen:test",
        "branch_name": "default",
        "catalog_version_id": None,
        "nomos_subject": SUBJECT,
        "nomos_read_action": LOCAL_OPERATOR_READ_ACTION,
        "nomos_read_surface": LOCAL_OPERATOR_READ_SURFACE,
        "nomos_multi_user": False,
        "nomos_identity_claim": None,
        "nomos_access_context": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _rows(n: int) -> list[dict[str, Any]]:
    sources = ("git", "docs", "jira")
    return [
        {
            "node_id": f"node:{i}",
            "subtype": "doc",
            "source": sources[i % 3],
            "source_record_id": {"key": f"k{i}", **_secret_bag()},
            "source_revision": {"v": 1},
            "attrs": {"name": f"n{i}", "classification": "internal"},
        }
        for i in range(n)
    ]


def _count_walks(monkeypatch: pytest.MonkeyPatch, key: str) -> list[int]:
    """Count how often ``sanitize_trace_value`` descends into ``key``'s value."""
    counter = [0]
    real = runtime_enforcement.sanitize_trace_value

    def counting(value: Any, *, key: str | None = None, depth: int = 0) -> Any:
        if key == watched:
            counter[0] += 1
        return real(value, key=key, depth=depth)

    watched = key
    # The recursion resolves the module global, so nested walks are counted too.
    monkeypatch.setattr(runtime_enforcement, "sanitize_trace_value", counting)
    return counter


def _count_evaluations(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    counter = [0]
    real = runtime_enforcement.evaluate

    def counting(*args: Any, **kwargs: Any) -> Any:
        counter[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(runtime_enforcement, "evaluate", counting)
    return counter


# --------------------------------------------------------------------------- #
# Once per decision                                                            #
# --------------------------------------------------------------------------- #
def test_read_filter_walks_each_decision_resource_once(
    storage: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    walks = _count_walks(monkeypatch, "source_lineage")
    evaluations = _count_evaluations(monkeypatch)
    conn = _Conn()

    visible = read_filter.filter_node_rows_for_session(conn, _session(), _rows(9))

    assert visible, "the fixture policy must leave rows visible"
    assert evaluations[0] > len(visible)  # source + fine-grained targets per row
    # ``source_lineage`` is a top-level resource fact on every evaluation: it
    # is walked when the request is sanitized and never again -- not for the
    # evaluated decision, the outcome, its trace, or the persist. On dev
    # before #2784 this was five walks per evaluation.
    assert walks[0] == evaluations[0]
    assert len(conn.written) == evaluations[0]


def test_policy_obligations_are_sanitized_once_per_decision(
    storage: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # "nested" (a mapping, so one call per walk) exists only in the audit
    # rule's obligation parameters here; the request does not carry it.
    walks = _count_walks(monkeypatch, "nested")
    conn = _Conn()
    with batched_decision_persist(conn):
        outcome = store.evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=DEPLOYMENT,
            scope=f"deployment:{DEPLOYMENT}",
            request=NomosRuntimeRequest(
                surface=RUNTIME_SURFACE,
                subject=SUBJECT,
                resource={"resource_kind": "graph_object", "source_id": "git"},
            ),
            performed_obligations=("audit",),
        )
    assert outcome is not None
    assert outcome.decision.matched_rule_ids == ("audit-graph-objects",)
    assert walks[0] == 1
    assert is_sanitized_policy_decision(outcome.decision)
    assert len(conn.written) == 1


def test_sanitize_policy_decision_reuses_its_own_result() -> None:
    raw = evaluate(
        _bundle(),
        DecisionInput(
            subject=SUBJECT,
            action=LOCAL_OPERATOR_READ_ACTION,
            resource={"resource_kind": "graph_object", **_secret_bag()},
            classification="internal",
            context={"token": KEYED_SECRET},
        ),
    )
    assert not is_sanitized_policy_decision(raw)
    safe = sanitize_policy_decision(raw)
    assert is_sanitized_policy_decision(safe)
    assert sanitize_policy_decision(safe) is safe
    # The stamp is not a field: equality and the serialized form ignore it.
    assert "_nomos_sanitized_decision" not in safe.to_dict()
    assert dataclasses.fields(safe) == dataclasses.fields(raw)


# --------------------------------------------------------------------------- #
# Failure case: an unsanitized decision is never trusted                       #
# --------------------------------------------------------------------------- #
def test_decision_mutated_after_sanitizing_is_sanitized_again_before_persist() -> None:
    outcome = evaluate_runtime_boundary(
        _bundle(),
        NomosRuntimeRequest(
            surface=RUNTIME_SURFACE,
            subject=SUBJECT,
            resource={"resource_kind": "graph_object", "nested": {"path": "ok"}},
        ),
        performed_obligations=("audit",),
    )
    decision = outcome.decision
    assert is_sanitized_policy_decision(decision)
    # In-place edits the stamp cannot see by identity: a new top-level key, a
    # nested dict, the context and an obligation's parameters.
    decision.resource["note"] = SECRET_LITERALS[0]
    decision.resource["nested"]["path"] = SECRET_LITERALS[3]
    decision.context["api_key"] = KEYED_SECRET
    decision.obligations[0]["token"] = SECRET_LITERALS[1]
    assert not is_sanitized_policy_decision(decision)

    conn = _Conn()
    persist_nomos_decision(
        conn,
        deployment_name=DEPLOYMENT,
        decision=decision,
        target={"kind": "test"},
        allow_deferred=False,
    )
    assert len(conn.written) == 1
    _assert_no_secret("persisted row", conn.written)
    _assert_no_secret(
        "trace", [json.dumps(runtime_enforcement.sanitize_decision_trace_mapping(decision))],
    )
    assert '"redacted": "local_path"' in conn.written[0]


def test_each_single_edit_breaks_the_stamp() -> None:
    def fresh() -> Any:
        return evaluate_runtime_boundary(
            _bundle(),
            NomosRuntimeRequest(
                surface=RUNTIME_SURFACE,
                subject=SUBJECT,
                resource={"resource_kind": "graph_object", "nested": {"k": [1, 2]}},
                context={"c": "v"},
            ),
            performed_obligations=("audit",),
        ).decision

    edits = [
        lambda d: d.resource.__setitem__("x", "y"),
        lambda d: d.resource["nested"]["k"].append(3),
        lambda d: d.resource.pop("nested"),
        lambda d: d.context.__setitem__("c", "w"),
        lambda d: d.obligations[0].__setitem__("extra", 1),
        # Mixed key types cannot be fingerprinted: never trusted.
        lambda d: d.resource.__setitem__(1, "int key"),
        # An object that prints like the value it replaced still differs.
        lambda d: d.context.__setitem__("c", _PrintsAs("v")),
    ]
    for edit in edits:
        decision = fresh()
        assert is_sanitized_policy_decision(decision)
        edit(decision)
        assert not is_sanitized_policy_decision(decision), edit


class _PrintsAs:
    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        return self.text


def test_replaced_or_hand_built_decision_is_sanitized_in_full_before_persist() -> None:
    safe = sanitize_policy_decision(evaluate(
        _bundle(),
        DecisionInput(
            subject=SUBJECT,
            action=LOCAL_OPERATOR_READ_ACTION,
            resource={"resource_kind": "graph_object"},
            classification="internal",
            context={},
        ),
    ))
    # Copying a sanitized decision with raw values drops the stamp, so the
    # persist cannot mistake the copy for a sanitized one.
    tampered = dataclasses.replace(
        safe,
        resource={**safe.resource, **_secret_bag()},
        context={"note": SECRET_LITERALS[0]},
        obligations=({"type": "audit", "api_key": KEYED_SECRET},),
    )
    assert not is_sanitized_policy_decision(tampered)
    hand_built = evaluate(
        None,
        DecisionInput(
            subject=SUBJECT,
            action=LOCAL_OPERATOR_READ_ACTION,
            resource=_secret_bag(),
            classification="internal",
            context={"token": KEYED_SECRET},
        ),
    )
    conn = _Conn()
    for decision in (tampered, hand_built):
        persist_nomos_decision(
            conn,
            deployment_name=DEPLOYMENT,
            decision=decision,
            target={"kind": "test", "cookie": KEYED_SECRET},
            allow_deferred=False,
        )
    assert len(conn.written) == 2
    _assert_no_secret("persisted rows", conn.written)
    # The walk really happened: the markers are there in place of the values.
    assert '"redacted": "secret"' in conn.written[0]
    assert '"redacted": "local_path"' in conn.written[1]


# --------------------------------------------------------------------------- #
# No unsanitized value in any output                                           #
# --------------------------------------------------------------------------- #
def _assert_no_secret(where: str, blobs: list[str]) -> None:
    for blob in blobs:
        for secret in ALL_SECRETS:
            assert secret not in blob, f"{secret!r} leaked into {where}"


@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_no_secret_reaches_any_decision_output_on_the_read_path(
    storage: SimpleNamespace,
    caplog: pytest.LogCaptureFixture,
    mode: str,
) -> None:
    storage.mode = mode
    # A default-deny bundle makes the audit run exercise log-and-allow.
    storage.bundle = _bundle(default="deny") if mode == "audit" else _bundle()
    conn = _Conn()
    session = _session(nomos_access_context={
        "context_id": "ctx:2784",
        "policy_context": _secret_bag(),
    })
    with caplog.at_level(logging.DEBUG):
        read_filter.filter_node_rows_for_session(conn, session, _rows(6))
        returned = store.evaluate_with_active_policy(
            conn,
            deployment_name=DEPLOYMENT,
            scope=f"deployment:{DEPLOYMENT}",
            decision_input=DecisionInput(
                subject=SUBJECT,
                action=LOCAL_OPERATOR_READ_ACTION,
                resource={"resource_kind": "graph_object", **_secret_bag()},
                classification="internal",
                context={"token": KEYED_SECRET, "note": SECRET_LITERALS[1]},
            ),
            enforcement_point=RUNTIME_SURFACE,
            target={"kind": "test", "authorization": SECRET_LITERALS[0]},
        )

    assert conn.written and storage.outcomes
    _assert_no_secret("persisted rows", conn.written)
    _assert_no_secret("returned decision", [json.dumps(returned.to_dict())])
    _assert_no_secret(
        "outcome decisions",
        [json.dumps(o.decision.to_dict()) for o in storage.outcomes],
    )
    _assert_no_secret(
        "outcome traces",
        [json.dumps(dict(o.trace), default=str) for o in storage.outcomes],
    )
    _assert_no_secret("log", [record.getMessage() for record in caplog.records])
    if mode == "audit":
        assert any(o.audit_log_and_allow for o in storage.outcomes)


def test_no_secret_reaches_fail_closed_and_primitive_denied_outputs() -> None:
    bundle = _bundle()
    outcomes = [
        # Primitive denial: its metadata lands in the decision context.
        evaluate_runtime_boundary(
            bundle,
            NomosRuntimeRequest(
                surface=RUNTIME_SURFACE,
                subject=SUBJECT,
                resource={"resource_kind": "graph_object", **_secret_bag()},
            ),
            primitive_checks=(
                NomosPrimitiveCheck("p", False, "", {"token": KEYED_SECRET}),
            ),
        ),
        # Malformed request: fail-closed decision built from the raw request.
        evaluate_runtime_boundary(
            bundle,
            NomosRuntimeRequest(surface="not.a.surface", subject=SUBJECT, resource=_secret_bag()),
        ),
        # Missing obligation: the enforced decision rebuilt in the outcome.
        evaluate_runtime_boundary(
            bundle,
            NomosRuntimeRequest(
                surface=RUNTIME_SURFACE,
                subject=SUBJECT,
                resource={"source_id": "docs", **_secret_bag()},
                context={"api_key": KEYED_SECRET},
            ),
            performed_obligations=(),
        ),
    ]
    assert [o.allowed for o in outcomes] == [False, False, False]
    assert outcomes[2].missing_obligations == ("redaction",)
    for outcome in outcomes:
        assert is_sanitized_policy_decision(outcome.decision)
        _assert_no_secret("decision", [json.dumps(outcome.decision.to_dict())])
        _assert_no_secret("trace", [json.dumps(dict(outcome.trace), default=str)])


def test_trace_matches_the_decision_and_does_not_alias_it() -> None:
    outcome = evaluate_runtime_boundary(
        _bundle(),
        NomosRuntimeRequest(
            surface=RUNTIME_SURFACE,
            subject=SUBJECT,
            resource={"resource_kind": "graph_object", **_secret_bag()},
        ),
        performed_obligations=("audit",),
    )
    decision = outcome.decision.to_dict()
    for key in ("resource", "context", "obligations"):
        assert outcome.trace[key] == decision[key]
    outcome.trace["resource"]["nested"]["path"] = "mutated"
    assert outcome.decision.resource["nested"]["path"] != "mutated"


# --------------------------------------------------------------------------- #
# Outcomes unchanged                                                           #
# --------------------------------------------------------------------------- #
def _reference_decision(
    bundle: CompiledPolicyBundle | None,
    request: NomosRuntimeRequest,
) -> Any:
    """The decision as dev built it: sanitize the request, then add defaults."""
    mapping = runtime_surface_mapping_for(request.surface)
    resource = sanitize_trace_mapping(request.resource)
    resource.setdefault("resource_kind", mapping.resource_kind)
    resource.setdefault("nomos_surface", mapping.surface)
    context = sanitize_trace_mapping(request.context)
    context.setdefault("principal_kind", mapping.principal_kind)
    context.setdefault("nomos_boundary", mapping.surface)
    return evaluate(
        bundle,
        DecisionInput(
            subject=request.subject.strip(),
            action=mapping.action,
            resource=resource,
            classification=request.classification or mapping.classification,
            context=context,
        ),
        enforcement_point=mapping.surface,
    )


#: A rule no request matches, so the bundle's default effect decides.
_UNMATCHED_RULE: dict[str, Any] = {
    "rule_id": "never",
    "effect": "deny",
    "actions": ["*"],
    "subjects": ["subject:nobody"],
    "priority": 1,
    "reason_code": "nomos.test.never",
}
_POLICY_SET: list[tuple[str, CompiledPolicyBundle | None]] = [
    ("no-bundle", None),
    ("default-allow", _bundle(rules=[_UNMATCHED_RULE])),
    ("default-deny", _bundle(default="deny", rules=[_UNMATCHED_RULE])),
    ("audit-and-redact", _bundle()),
    ("secret-match", _bundle(default="deny", rules=[{
        "rule_id": "allow-redacted-key",
        "effect": "allow",
        "actions": ["*"],
        "subjects": ["*"],
        # Rules match the sanitized fact, never the raw one.
        "match": {"source_id": "git"},
        "priority": 1,
        "reason_code": "nomos.test.allow",
    }])),
    ("quarantine", _bundle(rules=[{
        "rule_id": "quarantine-jira",
        "effect": "quarantine",
        "actions": ["*"],
        "subjects": ["*"],
        "match": {"source_id": "jira"},
        "priority": 1,
        "reason_code": "nomos.test.quarantine",
        "obligations": [{"type": "quarantine"}],
    }])),
    ("declassify-override-kind", _bundle(rules=[{
        "rule_id": "declassify-custom-kind",
        "effect": "declassify",
        "actions": ["*"],
        "subjects": ["*"],
        # The request sets its own resource_kind: it must win over the default.
        "match": {"resource_kind": "custom_kind"},
        "priority": 1,
        "reason_code": "nomos.test.declassify",
        "obligations": [{"type": "declassification"}],
    }])),
]
_REQUESTS: list[NomosRuntimeRequest] = [
    NomosRuntimeRequest(RUNTIME_SURFACE, SUBJECT, {"source_id": "git"}),
    NomosRuntimeRequest(RUNTIME_SURFACE, SUBJECT, {"source_id": "docs", **_secret_bag()}),
    NomosRuntimeRequest(RUNTIME_SURFACE, SUBJECT, {"source_id": "jira"}, "restricted"),
    NomosRuntimeRequest(
        RUNTIME_SURFACE, SUBJECT, {"resource_kind": "custom_kind"}, None,
        {"principal_kind": "agent", "token": KEYED_SECRET},
    ),
    NomosRuntimeRequest("graph.read.mcp", SUBJECT, {"resource_kind": "graph_object"}),
    NomosRuntimeRequest("graph.export", SUBJECT, {"source_id": "docs"}),
]


@pytest.mark.parametrize("name,bundle", _POLICY_SET, ids=[name for name, _ in _POLICY_SET])
def test_decision_outcomes_match_the_engine_on_the_sanitized_input(
    name: str, bundle: CompiledPolicyBundle | None,
) -> None:
    for request in _REQUESTS:
        for performed in (None, ("audit", "redaction", "quarantine", "declassification")):
            outcome = evaluate_runtime_boundary(
                bundle, request, performed_obligations=performed,
            )
            reference = _reference_decision(bundle, request)
            decided = outcome.decision
            if decided.effect == reference.effect:
                # The engine's own decision: same identity, rule and reason.
                assert decided.decision_id == reference.decision_id, (name, request)
                assert decided.input_hash == reference.input_hash
                assert decided.reason_code == reference.reason_code
                assert decided.matched_rule_ids == reference.matched_rule_ids
                assert decided.resource == reference.resource
                assert decided.context == reference.context
            else:
                # Enforcement overrode the engine (a missing or blocking
                # obligation); the engine's verdict is kept in the context.
                assert decided.input_hash == reference.input_hash
                assert decided.context["policy_decision_id"] == reference.decision_id
                assert decided.context["policy_effect"] == reference.effect
                assert outcome.allowed is False


# --------------------------------------------------------------------------- #
# sanitize_trace_value output is byte-identical with dev                      #
# --------------------------------------------------------------------------- #
def _dev_sanitize_trace_value(value: Any, *, key: str | None = None, depth: int = 0) -> Any:
    """``sanitize_trace_value`` exactly as origin/dev 00424f3e9dad had it."""
    from collections.abc import Mapping, Sequence

    re_mod = runtime_enforcement
    if depth > 8:
        return re_mod._redaction_marker("max_depth", value)
    key_l = (key or "").lower()
    if isinstance(value, bytes | bytearray | memoryview):
        return re_mod._redaction_marker("binary", value)
    if key_l and (re_mod.looks_secret_key(key_l) or key_l in re_mod._AUTHORIZATION_KEYS):
        return re_mod._redaction_marker("secret", value)
    if key_l in re_mod._TRACE_PAYLOAD_KEYS:
        return re_mod._redaction_marker("payload", value)
    if isinstance(value, Mapping):
        return {
            str(k): _dev_sanitize_trace_value(v, key=str(k), depth=depth + 1)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [
            _dev_sanitize_trace_value(item, key=key, depth=depth + 1)
            for item in value
        ]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(re_mod._SECRET_REF_PREFIXES):
            return re_mod._redaction_marker("secret_ref", stripped)
        if re_mod._SSN_RE.search(stripped) or (
            "patient" in stripped.lower()
            and re_mod._DATE_OF_BIRTH_RE.search(stripped)
        ):
            return re_mod._redaction_marker("phi", stripped)
        if re_mod._LOCAL_PATH_RE.search(stripped):
            return re_mod._redaction_marker("local_path", stripped)
        if re_mod._looks_like_credential_literal(stripped):
            return re_mod._redaction_marker("secret", stripped)
        if len(stripped) > 512:
            return {
                "redacted": "long_string",
                "sha256": "sha256:" + re_mod._sha256_hex(stripped),
                "length": len(stripped),
            }
        return value
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        return re_mod._redaction_marker("non_json", type(value).__qualname__)
    return value


class _Str(str):
    def __str__(self) -> str:
        return "sk-live-from-dunder-str"


class _Level(enum.IntEnum):
    HIGH = 3


def _nested(depth: int) -> Any:
    value: Any = SECRET_LITERALS[0]
    for _ in range(depth):
        value = {"level": value}
    return value


_EQUIVALENCE_CASES: list[tuple[str | None, Any]] = [
    (None, 10**5000),  # over the 4,300-digit limit: json.dumps raises
    (None, -(10**5000)),
    (None, 10**4299),
    (None, 7),
    (None, True),
    (None, None),
    (None, 1.5),
    (None, float("nan")),
    (None, float("inf")),
    (None, _Str("  hello ")),
    (None, _Str("sk-live-abc")),
    (None, _Level.HIGH),
    (None, decimal.Decimal("1.10")),
    (None, datetime.datetime(2026, 9, 28, 12, 0)),
    (None, datetime.date(2026, 9, 28)),
    (None, b"raw"),
    (None, bytearray(b"raw")),
    (None, memoryview(b"raw")),
    (None, {"a", "b"}),
    (None, frozenset({1})),
    (None, ("x", SECRET_LITERALS[2], 3)),
    (None, types.MappingProxyType({"b": 1, "a": SECRET_LITERALS[3]})),
    ("API_KEY", "value"),
    ("Authorization", "Bearer abc"),
    ("COOKIE", {"nested": 1}),
    ("Body", "raw payload"),
    ("tags", ["a", SECRET_LITERALS[0], 10**5000]),
    (None, _nested(8)),
    (None, _nested(9)),
    (None, _nested(12)),
    (None, {1: "int key", "1": "str key"}),
    (None, "x" * 513),
    (None, "patient born 1980-02-03"),
    (None, object()),
]


@pytest.mark.parametrize(
    "key,value",
    _EQUIVALENCE_CASES,
    # Explicit ids: pytest cannot render an int over 4,300 digits as an id.
    ids=[f"{index}-{type(value).__name__}" for index, (_, value) in enumerate(_EQUIVALENCE_CASES)],
)
def test_sanitize_trace_value_matches_dev_byte_for_byte(key: str | None, value: Any) -> None:
    new = runtime_enforcement.sanitize_trace_value(value, key=key)
    old = _dev_sanitize_trace_value(value, key=key)
    # json.dumps also proves the output serializes: a huge int must not
    # survive raw, because encoding it raises.
    assert json.dumps(new) == json.dumps(old)


def test_sanitize_trace_value_matches_dev_on_random_values() -> None:
    rng = random.Random(2784)
    scalars: list[Any] = [
        *ALL_SECRETS, "plain", 1, 10**5000, 2.5, float("nan"), True, None, b"b",
        _Str("s"), _Level.HIGH, decimal.Decimal("2"), {"s"}, "y" * 600,
    ]
    keys = ["name", "api_key", "TOKEN", "body", "Authorization", "tags", "nested"]

    def value(depth: int) -> Any:
        roll = rng.random()
        if depth < 11 and roll < 0.25:
            return {rng.choice(keys): value(depth + 1) for _ in range(rng.randint(0, 3))}
        if depth < 11 and roll < 0.35:
            return [value(depth + 1) for _ in range(rng.randint(0, 3))]
        if depth < 11 and roll < 0.4:
            return tuple(value(depth + 1) for _ in range(rng.randint(0, 2)))
        return rng.choice(scalars)

    def outcome(sanitize: Any, item: Any, key: str | None) -> str:
        # A NaN under a secret-looking key makes the marker hash raise, on dev
        # too; that must stay the same failure, not become a different one.
        try:
            return json.dumps(sanitize(item, key=key))
        except ValueError as exc:
            return f"raised ValueError: {exc}"

    for _ in range(3000):
        item, key = value(0), rng.choice([None, *keys])
        assert outcome(runtime_enforcement.sanitize_trace_value, item, key) == outcome(
            _dev_sanitize_trace_value, item, key,
        )

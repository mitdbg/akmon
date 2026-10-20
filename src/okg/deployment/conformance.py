"""Connector conformance kit (#1181 slice 7).

An external repository points this module at one `Connector` and a small
deterministic dataset, and gets one test per check plus a machine-readable
receipt. It imports nothing from `okg.substrate` on the *author's* side:
the author declares a `ConnectorCase` out of names `okg.deployment`
already exports.

Run it, from the external repository::

    pytest -p okg.deployment.conformance --pyargs okg.deployment.conformance \\
        --okg-connector mypackage.conformance:CASE \\
        --okg-conformance-receipt receipt.json

`-p` registers this module as a pytest plugin (the options, the fixtures,
the receipt hook); `--pyargs` collects it as the test module. There is
deliberately **no** ``[project.entry-points.pytest11]`` entry: `okg` is
installed in every environment that has the substrate, so an entry point
would load this plugin — its options and its session hook — into every
unrelated pytest session. Opt-in is the cost of not doing that.

What the kit proves, and what it does not
-----------------------------------------
`CHECKS` is the executable table; `COVERAGE` is the honest map onto the
nine areas issue #1181 names. Three of those nine are not fully provable
here and the receipt says so rather than reporting a pass:

* **lineage** is asserted by the publisher, not by the kit. A connector
  emits `source_record_id` / `source_revision`; whether the published
  generation carries complete lineage is a publish-time assertion and
  belongs to #1185's arm.
* **capability refusal** — denying a connector an undeclared resource —
  is provable only in the brokered tier, which does not exist yet. What
  is provable in-process is *declaration*: every capability the connector
  used must be declared, and every capability it declared must be
  observed. An optional capability the connector does not implement must
  be left undeclared; declaring one and not implementing it **fails**
  rather than skips.
* **narrowing rejection** is checked at the connector boundary: an item
  outside the declared signature must surface as a `RejectedItem`, not
  vanish. The live narrowing catalog is applied at publish.

The **enricher** areas are out of scope: this slice is connectors only,
and slice 5 owns the enricher read surface.

No check skips. A case that cannot prove an area — no secret values for
redaction, no out-of-signature item for narrowing — fails with a reason
naming what the case must declare.
"""
from __future__ import annotations

import dataclasses
import importlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

import pytest

from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.errors import ConnectorConformanceCaseInvalid
from okg.substrate.fact_batch import FactBatchFieldError, FactChunk
from okg.substrate.library.sources.base import (
    SOURCE_RUN_PARTIAL_STATUS,
    ProgressMarker,
)
from okg.substrate.producer_authority import normalize_output_signature

from ._internal.connectors import HEALTH_STATUSES
from ._internal.protocol import ConnectorAdapter, ConnectorError
from ._internal.registration import ConnectorRegistration
from ._version import __version__

#: Receipt document and harness identities. Field names mirror #1185's
#: `ExternalDistributionConformanceReceipt` (`schema_version`, `verdict`,
#: `harness`, `subject`, `status`, `diagnostic_codes`) so a release-level
#: consumer reads one vocabulary; the kit does not import that model.
RECEIPT_SCHEMA = "okg.connector_conformance.receipt/v1"
HARNESS_PROTOCOL = "okg.connector_conformance/v1"

#: `ConnectorContext` inputs whose use requires the matching declared
#: capability. Reading one is the observable: nothing else distinguishes a
#: connector that honours cancellation from one that ignores it.
CAPABILITY_INPUTS = {
    "authoritative_replay": "authoritative_replay",
    "cancellation": "cancelled",
    "change_probe": "change_probe_verdict",
    "progress_fingerprints": "since_progress",
    "scoped_run": "sync_scope",
}

#: `ConnectorRun` fields whose use requires the matching declared
#: capability.
CAPABILITY_OUTPUTS = {
    "checkpoint_resume": "next_checkpoint",
    "record_authority_stream": "record_authority_stream",
    "record_set_authority": "record_set",
    "scoped_replacement": "record_set_replacement_keys",
}


@dataclass(frozen=True)
class ConnectorCase:
    """One connector, its declaration, and the dataset the kit drives it on.

    `dataset` is the deterministic input, keyed by the record id the
    connector reports in `source_record_id["id"]`; its values are digested
    into the receipt so a receipt names the data it was earned on.
    `secret_refs` is what `ConnectorContext.secret_ref` resolves;
    `secret_values` are the actual credential strings, which the kit
    searches for in everything the connector produced.
    `expected_rejections` names the dataset ids that lie outside the
    declared `output_signature` and must come back as `RejectedItem`s.
    """

    connector: Any
    registration: ConnectorRegistration
    dataset: Mapping[str, Any]
    config: Mapping[str, Any] = field(default_factory=dict)
    secret_refs: Mapping[str, str] = field(default_factory=dict)
    secret_values: Sequence[str] = ()
    expected_rejections: Sequence[str] = ()


class _Recorder:
    """A `ConnectorContext` view that records which inputs were read.

    Attribute reads and method calls forward unchanged, which is all the
    landed protocol asks of a connector. It is deliberately *not* a
    `ConnectorContext`, so a connector that `isinstance`-checks its context
    or calls `dataclasses.replace` on it sees the difference; the kit's
    capability check needs the read record, and nothing in the protocol
    sanctions either of those.
    """

    def __init__(self, ctx: Any) -> None:
        self.ctx = ctx
        self.used: set[str] = set()

    def __getattr__(self, name: str) -> Any:
        # `ctx` and `used` live in __dict__, so they never reach here.
        self.used.add(name)
        return getattr(self.ctx, name)


class _Probe:
    """A `Connector` wrapper that keeps the run and what it asked for."""

    def __init__(self, connector: Any) -> None:
        self.connector = connector
        self.name = connector.name
        self.profile = connector.profile
        self.used: set[str] = set()
        self.outcome: Any = None

    def run(self, ctx: Any) -> Any:
        recorder = _Recorder(ctx)
        try:
            self.outcome = self.connector.run(recorder)
            return self.outcome
        finally:
            self.used |= recorder.used


@dataclass
class Drives:
    """Every run the kit performs, executed once and shared by the checks."""

    baseline: Any
    resumed: Any
    cancelled: Any
    probes: list[_Probe]
    credential_error: Optional[Exception]

    def runs(self) -> tuple[Any, ...]:
        return (self.baseline, self.resumed, self.cancelled)

    def outcomes(self) -> tuple[Any, ...]:
        return tuple(p.outcome for p in self.probes if p.outcome is not None)

    def used(self) -> set[str]:
        return set().union(*(p.used for p in self.probes)) if self.probes else set()


def _drive(
    case: ConnectorCase,
    *,
    cursor: Optional[Mapping[str, Any]] = None,
    secrets: Optional[Mapping[str, str]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> tuple[Any, _Probe]:
    """Run the connector once through the SDK adapter the runner drives.

    All six kwargs the runner selects by signature inspection are named,
    so a check exercises the adapter's real call shape.
    """
    probe = _Probe(case.connector)
    adapter = ConnectorAdapter(
        probe,
        config=case.config,
        secrets=dict(case.secret_refs if secrets is None else secrets),
        cancel=cancel,
    )
    return adapter.run(
        "okg-conformance",
        mode="cursor",
        cursor=cursor,
        since_progress=None,
        sync_scope=None,
        change_probe_verdict=None,
        authoritative_replay=False,
    ), probe


def execute(case: ConnectorCase) -> Drives:
    """Perform every drive the checks read. Called once per session."""
    baseline, first = _drive(case)
    resumed, second = _drive(
        case, cursor=baseline.next_cursor,
    )
    polls = {"n": 0}

    def cancel() -> bool:
        polls["n"] += 1
        return polls["n"] > 1

    stopped, third = _drive(case, cancel=cancel)
    error: Optional[Exception] = None
    if case.secret_refs:
        try:
            _drive(case, secrets={})
        except Exception as exc:  # noqa: BLE001 — reported, not swallowed
            error = exc
    return Drives(
        baseline=baseline,
        resumed=resumed,
        cancelled=stopped,
        probes=[first, second, third],
        credential_error=error,
    )


def _body(fact: Any) -> dict[str, Any]:
    if dataclasses.is_dataclass(fact) and not isinstance(fact, type):
        return {f.name: getattr(fact, f.name) for f in dataclasses.fields(fact)}
    return dict(vars(fact))


#: The kit digests each item as the one-item chunk that would carry it, so
#: these two only have to be fixed: a digest is compared with another digest
#: from the same kit, never with a stored one. A publisher picks its own.
_DIGEST_RUN_ID = "00000000-0000-4000-8000-000000000000"
_DIGEST_ORDINAL = 0


def _digests(items: Any) -> list[str]:
    """Digest each item exactly as the publish path would.

    The kit's claim is that a connector's output is stable and that a resume
    does not re-emit it. Both are comparisons between digests, so what
    matters is that the digest is the *publisher's* — the one a `FactChunk`
    computes after freezing and coercing the item. Canonicalizing here with a
    second encoder was how the kit came to refuse a float confidence that
    publish stores without complaint (#1970): the deployment-product lock
    encoder is strict about floats, correctly for lock payloads and wrongly
    for facts.

    `SourceRun.facts` is one mixed stream — `ConnectorAdapter.run`
    flat-extends whatever the connector returned — so this splits it the way
    `SourceRunFactBatchLowerer` does, by type. Getting that wrong is not a
    smaller mistake than the encoder was: handing a `ProgressMarker` over as
    a fact refuses it outright, and sixteen shipped sources yield markers.
    """

    return [_digest(item) for item in items]


def _is_marker(item: Any) -> bool:
    """The one place the kit decides an item is a marker, not content.

    `_digest` routes by this and `_content_digests` filters by it. Two
    copies of the predicate could drift, and the pair only means anything
    while they agree.
    """

    return isinstance(item, ProgressMarker)


def _content_digests(items: Any) -> list[str]:
    """Digest the content items, dropping the progress markers.

    `checkpoint_resume` asks whether a resume re-emitted content the first
    run had already produced. A `ProgressMarker` is not content: it is a
    commit boundary, the source's claim that everything it means to emit for
    `record_id` has been yielded. A connector emitting one stable run-level
    marker per run -- the `calendar_ics` shape, one marker carrying the
    feed's fingerprint -- re-emits the identical marker on a resume, and that
    is what an unchanged feed fingerprint is supposed to do. Intersecting it
    as though it were a fact false-failed the check (#1994).

    No comparison between the two runs' markers is left to make, so none is
    made: an identical marker means the record is unchanged and a different
    fingerprint under the same `record_id` means it changed, and both are
    correct. Every marker is still digested here rather than skipped, so one
    the publisher would refuse still reports as a publish refusal instead of
    passing unexamined.
    """

    content: list[str] = []
    # One pass over the stream, digesting every item and keeping the content
    # ones. A marker's digest is computed and dropped on purpose: computing
    # it is what makes a marker the publisher would refuse report as a
    # publish refusal instead of passing unexamined, and `_determinism`
    # never digests the resumed run, so a marker that appears only on a
    # resume is caught here or nowhere. One pass also means this does not
    # depend on `items` being re-iterable -- a one-shot iterator would leave
    # a second pass comparing nothing, which is a check that silently proves
    # nothing.
    for item in items:
        digest = _digest(item)
        if not _is_marker(item):
            content.append(digest)
    return content


def _digest(item: Any) -> str:
    marker = _is_marker(item)
    try:
        return FactChunk.create(
            run_id=_DIGEST_RUN_ID,
            ordinal=_DIGEST_ORDINAL,
            facts=() if marker else [item],
            progress_markers=[item] if marker else (),
        ).digest
    except (ValueError, TypeError) as exc:
        # Say which kind of failure this is. Without the prefix a publisher's
        # validation refusal — an empty id, a secret-bearing key, a datetime
        # attribute — reads as though `determinism` or `checkpoint_resume`
        # caught the connector being unstable, which it did not.
        #
        # `FactBatchFieldError` subclasses `ValueError`, so it is caught here
        # too. The wider tuple is not defensive: `_freeze_fact` coerces with
        # `float(fact.confidence)`, and a non-numeric confidence raises a
        # bare `ValueError` or `TypeError` from the standard library before
        # any FactBatch check runs. Catching only the FactBatch type let
        # those reach the operator unlabelled.
        raise FactBatchFieldError(
            f"this item would be refused at publish: {exc}"
        ) from exc


def _record_id(fact: Any) -> Any:
    return (getattr(fact, "source_record_id", None) or {}).get("id")


# --- Checks ------------------------------------------------------------------
#
# Each returns None on success, or the reason it failed. Raising is also a
# failure: `run_check` reports the exception rather than letting a broken
# connector abort the session.


def _determinism(case: ConnectorCase, drives: Drives) -> Optional[str]:
    first = _digests(drives.baseline.facts)
    again, _ = _drive(case)
    if first != _digests(again.facts):
        return (
            "two runs over the same dataset produced different facts; the "
            "connector's serialization is not deterministic"
        )
    return None


def _checkpoint_resume(case: ConnectorCase, drives: Drives) -> Optional[str]:
    if drives.baseline.next_cursor is None:
        return (
            "the first run returned no next_checkpoint, so a resume cannot "
            "be proven; return a Checkpoint or drop checkpoint_resume"
        )
    repeated = set(_content_digests(drives.baseline.facts)) & set(
        _content_digests(drives.resumed.facts)
    )
    if repeated:
        return (
            f"resuming from next_checkpoint re-emitted {len(repeated)} fact(s) "
            "the first run had already produced"
        )
    if not drives.resumed.completed_scope:
        return (
            "the resumed run did not report completed_scope over an exhausted "
            "deterministic dataset"
        )
    return None


def _cancellation(case: ConnectorCase, drives: Drives) -> Optional[str]:
    run = drives.cancelled
    if "cancelled" not in drives.probes[2].used:
        return "the connector never called ctx.cancelled(), so it cannot stop"
    if run.next_cursor is not None:
        return "a cancelled run advanced the checkpoint"
    if run.completed_scope:
        return "a cancelled run claimed completed_scope"
    status = getattr(run.health, "status", None)
    if status is None or status == "ok":
        return f"a cancelled run reported health {status!r}; it must not be ok"
    return None


def _health(case: ConnectorCase, drives: Drives) -> Optional[str]:
    """Health vocabulary, and partial never claiming a completed scope.

    Correcting the design note: both conditions are already refused at
    construction — `SourceHealth` rejects an unknown status and
    `ConnectorRun` rejects `partial` beside `completed_scope`. What stays
    reachable is a connector returning a health object the SDK did not
    build, because `ConnectorRun.health` is untyped. That is the break the
    kit's own test drives; the partial arm is defence in depth.
    """
    for run in drives.runs():
        status = getattr(run.health, "status", None)
        if status is None:
            continue
        if status not in HEALTH_STATUSES:
            return f"health status {status!r} is not in HEALTH_STATUSES"
        if status == SOURCE_RUN_PARTIAL_STATUS and run.completed_scope:
            return "a partial run also set completed_scope"
    return None


def _corpus(drives: Drives) -> str:
    parts: list[str] = []
    for run in drives.runs():
        parts.append(json.dumps([_body(f) for f in run.facts], default=str))
        parts.append(str(getattr(run.health, "reason", "")))
    for outcome in drives.outcomes():
        for item in (*outcome.warnings, *outcome.rejected):
            parts.append(json.dumps(_body(item), default=str))
    return "\n".join(parts)


def _redaction(case: ConnectorCase, drives: Drives) -> Optional[str]:
    declared = tuple(case.registration.auth_modes)
    if not case.secret_values:
        if declared and declared != ("none",):
            return (
                f"the registration declares auth modes {list(declared)} but the "
                "case supplied no secret_values, so redaction is unproven"
            )
        return None
    corpus = _corpus(drives)
    for index, secret in enumerate(case.secret_values):
        if secret and secret in corpus:
            return (
                f"secret_values[{index}] appears verbatim in the facts, health "
                "reasons, warnings or rejected items of a run"
            )
        if secret and secret in set(case.secret_refs.values()):
            return f"secret_refs exposes secret_values[{index}] as a reference"
    return None


def _signature(case: ConnectorCase, drives: Drives) -> Optional[str]:
    signature = normalize_output_signature(
        dict(case.registration.output_signature),
        source_name=case.registration.connector_type,
    )
    subtypes = {
        n.get("subtype") for n in signature.get("nodes") or [] if isinstance(n, Mapping)
    }
    edges = {
        e.get("edge_type") for e in signature.get("edges") or [] if isinstance(e, Mapping)
    }
    for run in drives.runs():
        for fact in run.facts:
            subtype = getattr(fact, "subtype", None)
            if subtype is not None and subtype not in subtypes:
                return (
                    f"emitted node subtype {subtype!r} is outside the declared "
                    "output_signature"
                )
            edge_type = getattr(fact, "edge_type", None)
            if edge_type is not None and edge_type not in edges:
                return (
                    f"emitted edge type {edge_type!r} is outside the declared "
                    "output_signature"
                )
    return None


def _credential_failure(case: ConnectorCase, drives: Drives) -> Optional[str]:
    declared = tuple(case.registration.auth_modes)
    if not case.secret_refs:
        if declared and declared != ("none",):
            return (
                f"the registration declares auth modes {list(declared)} but the "
                "case declares no secret_refs to fail on"
            )
        return None
    error = drives.credential_error
    if error is None:
        return (
            "a run whose credential reference could not be resolved completed "
            "instead of failing; the connector never resolved its secret"
        )
    if isinstance(error, ConnectorError):
        return (
            f"an unresolvable credential raised {type(error).__name__}, a "
            "signal the shim turns into a clean partial run; a credential "
            "defect must fail visibly instead"
        )
    return None


def _capability_declaration(case: ConnectorCase, drives: Drives) -> Optional[str]:
    declared = set(case.registration.capabilities)
    used = drives.used()
    observed = {cap for cap, attr in CAPABILITY_INPUTS.items() if attr in used}
    for cap, attr in CAPABILITY_OUTPUTS.items():
        if any(getattr(o, attr, None) for o in drives.outcomes()):
            observed.add(cap)
    if any(
        getattr(o.health, "status", None) == SOURCE_RUN_PARTIAL_STATUS
        for o in drives.outcomes()
    ):
        observed.add("partial_health")
    undeclared = sorted(observed - declared)
    if undeclared:
        return (
            f"the connector used {undeclared} without declaring them in its "
            "registration capabilities"
        )
    unimplemented = sorted(declared - observed)
    if unimplemented:
        return (
            f"the registration declares {unimplemented}, which this run never "
            "exercised; an optional capability must be implemented or left "
            "undeclared"
        )
    return None


def _narrowing_rejection(case: ConnectorCase, drives: Drives) -> Optional[str]:
    expected = set(case.expected_rejections)
    if not expected:
        return (
            "the case declares no expected_rejections, so a rejection cannot "
            "be told from a silent drop; add one dataset item outside the "
            "declared output_signature"
        )
    outcome = drives.probes[0].outcome
    rejected = {r.record_id for r in (outcome.rejected if outcome else ())}
    missing = sorted(expected - rejected)
    if missing:
        return (
            f"items {missing} lie outside the declared signature and were "
            "dropped without a RejectedItem"
        )
    for run in drives.runs():
        for fact in run.facts:
            if _record_id(fact) in expected:
                return (
                    f"item {_record_id(fact)!r} was rejected and also emitted "
                    "as a fact"
                )
    return None


def _sdk_version(case: ConnectorCase, drives: Drives) -> Optional[str]:
    window = case.registration.sdk_range
    if not window.supports(__version__):
        return (
            f"okg.deployment {__version__} is outside the declared range "
            f"[{window.min_version}, {window.max_version_exclusive})"
        )
    return None


def _change_probe(case: ConnectorCase, drives: Drives) -> Optional[str]:
    """The registered reader can say cheaply whether its input changed (#2478).

    `okg deployment add-source` and deployment lint refuse a runnable reader
    whose adapter class declares no sound `change_probe_kind` or never sets
    `self.change_probe`. Without this check the kit passed such a connector,
    so a green receipt did not mean the connector could be added. The class
    is the one the registration names (`module` / `class_name`), constructed
    from the case's `config` the way the registry constructs it.
    """
    import inspect

    from okg.substrate.deployment_lint import _PROBE_EXEMPT_SOURCE_CLASSES
    from okg.substrate.library.sources.change_probe import (
        BANNED_PROBE_KINDS,
        CHANGE_PROBE_KIND_ATTR,
        SOUND_PROBE_KINDS,
    )

    registration = case.registration
    if registration.source_class in _PROBE_EXEMPT_SOURCE_CLASSES:
        return None
    target = f"{registration.module}.{registration.class_name}"
    try:
        cls = getattr(importlib.import_module(registration.module),
                      registration.class_name)
    except (ImportError, AttributeError) as exc:
        return f"the registered reader {target} cannot be imported: {exc}"
    kind = getattr(cls, CHANGE_PROBE_KIND_ATTR, None)
    if not isinstance(kind, str) or not kind:
        return (
            f"{target} declares no {CHANGE_PROBE_KIND_ATTR}; add-source "
            "refuses a reader without a change probe. Declare one of "
            f"{sorted(SOUND_PROBE_KINDS)} (content_hash is the floor) and set "
            "self.change_probe in the adapter"
        )
    if kind in BANNED_PROBE_KINDS or kind not in SOUND_PROBE_KINDS:
        return (
            f"{target} declares {CHANGE_PROBE_KIND_ATTR}={kind!r}, which is "
            f"not a content-sound probe; use one of {sorted(SOUND_PROBE_KINDS)}"
        )
    kwargs = dict(case.config)
    try:
        parameters = inspect.signature(cls).parameters.values()
    except (TypeError, ValueError):
        parameters = None
    if parameters is not None and not any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters
    ):
        # The registry passes only the parameters a constructor names; a
        # `**kwargs` constructor receives the whole config.
        named = {p.name for p in parameters}
        kwargs = {k: v for k, v in kwargs.items() if k in named}
    try:
        adapter = cls(**kwargs)
    except Exception as exc:  # noqa: BLE001 — reported, not swallowed
        return (
            f"{target} could not be constructed from the case config to read "
            f"its change probe: {type(exc).__name__}: {exc}"
        )
    if getattr(adapter, "change_probe", None) is None:
        return (
            f"{target} declares {CHANGE_PROBE_KIND_ATTR}={kind!r} but sets no "
            "self.change_probe, so the runner can never skip an unchanged read"
        )
    return None


#: check id -> (issue area it serves, implementation).
CHECKS: dict[str, tuple[str, Callable[[ConnectorCase, Drives], Optional[str]]]] = {
    "determinism": ("deterministic serialization", _determinism),
    "checkpoint_resume": ("retry/deduplication", _checkpoint_resume),
    "cancellation": ("cancellation", _cancellation),
    "health": ("sdk surface", _health),
    "redaction": ("sdk surface", _redaction),
    "signature_consistency": ("sdk surface", _signature),
    "credential_failure": ("credential failure/rotation", _credential_failure),
    "capability_declaration": ("capability refusal", _capability_declaration),
    "narrowing_rejection": ("narrowing rejection", _narrowing_rejection),
    "sdk_version": ("sdk surface", _sdk_version),
    "change_probe": ("sdk surface", _change_probe),
}

#: The nine areas #1181 names, and what this kit actually does about each.
COVERAGE: tuple[tuple[str, str, str], ...] = (
    ("deterministic serialization", "exercised", "determinism"),
    ("retry/deduplication", "exercised", "checkpoint_resume"),
    ("checkpoint safety", "exercised", "checkpoint_resume, cancellation"),
    ("cancellation", "exercised", "cancellation"),
    (
        "credential failure/rotation",
        "partly exercised",
        "credential_failure proves the failure; rotation is the platform's, "
        "because the SDK hands the connector a reference and never a value",
    ),
    (
        "capability refusal",
        "partly exercised",
        "capability_declaration proves declaration matches use; refusal is "
        "provable only in the brokered tier, which does not exist",
    ),
    (
        "lineage",
        "documented only",
        "the publisher asserts lineage, not the kit",
    ),
    (
        "narrowing rejection",
        "partly exercised",
        "narrowing_rejection proves an out-of-signature item is rejected "
        "visibly; the live narrowing catalog is applied at publish",
    ),
    (
        "enricher zero-output/partial-failure",
        "out of scope",
        "connectors only; slice 5 owns the enricher read surface",
    ),
)


def run_check(case: ConnectorCase, drives: Drives, check: str) -> Optional[str]:
    """Return None when `check` passes, else the reason it failed."""
    try:
        area, fn = CHECKS[check]
    except KeyError:
        raise ConnectorConformanceCaseInvalid(
            f"unknown conformance check {check!r}",
            data={"check": str(check), "known": sorted(CHECKS)},
        ) from None
    try:
        return fn(case, drives)
    except Exception as exc:  # noqa: BLE001 — a raising connector is a failure
        return f"{check} raised {type(exc).__name__}: {exc}"


#: What a check that recorded no result at all is reported as. A check the
#: session never reached did not pass; saying so is the whole point of the
#: receipt, which a release-level consumer reads instead of the pytest log.
DID_NOT_RUN = "did not run"


def receipt(
    case: ConnectorCase,
    results: Mapping[str, Optional[str]],
    *,
    session_failed: bool = False,
) -> dict[str, Any]:
    """The machine-readable record of one conformance session.

    Built over every declared check, never over what happened to be
    recorded: a connector that raises before any check runs must read
    `fail` on all of them, not `pass` on none. `session_failed` carries
    pytest's nonzero exit status, which fails the verdict even when every
    check that did run passed.
    """
    checks = []
    for name in sorted(CHECKS):
        reason = results[name] if name in results else DID_NOT_RUN
        checks.append(
            {
                "check": name,
                "area": CHECKS[name][0],
                "status": "pass" if reason is None else "fail",
                "diagnostic_codes": (
                    [] if reason is None
                    else [f"connector_conformance_{name}_failed"]
                ),
                "detail": reason or "",
            }
        )
    window = case.registration.sdk_range
    items = {
        key: sha256_digest(canonical_json_bytes(case.dataset[key]))
        for key in sorted(case.dataset)
    }
    document = {
        "schema_version": RECEIPT_SCHEMA,
        "verdict": (
            "fail"
            if session_failed or any(c["status"] == "fail" for c in checks)
            else "pass"
        ),
        "harness": {
            "harness_protocol": HARNESS_PROTOCOL,
            "package_version": __version__,
        },
        "subject": {
            "subject_kind": "connector",
            "connector_type": case.registration.connector_type,
            "connector_version": case.registration.version,
            "sdk_version": __version__,
            "sdk_min_version": window.min_version,
            "sdk_max_version_exclusive": window.max_version_exclusive,
        },
        "inputs": {
            "dataset_digest": sha256_digest(canonical_json_bytes(items)),
            "item_count": len(items),
            "item_digests": items,
        },
        "checks": checks,
        "coverage": [
            {"area": area, "coverage": mark, "note": note}
            for area, mark, note in COVERAGE
        ],
    }
    document["receipt_digest"] = sha256_digest(canonical_json_bytes(document))
    return document


def load_case(reference: str) -> ConnectorCase:
    """Resolve ``module.path:attribute`` to a `ConnectorCase`."""
    module_name, separator, attribute = str(reference).partition(":")
    if not separator or not module_name or not attribute:
        raise ConnectorConformanceCaseInvalid(
            f"--okg-connector must be module.path:attribute, got {reference!r}",
            data={"reference": str(reference), "reason": "missing ':'"},
            fix_hint="pass --okg-connector mypackage.conformance:CASE",
        )
    try:
        module = importlib.import_module(module_name)
        value = getattr(module, attribute)
    except (ImportError, AttributeError) as exc:
        raise ConnectorConformanceCaseInvalid(
            f"--okg-connector {reference!r} does not resolve",
            data={"reference": str(reference), "reason": str(exc)},
        ) from exc
    if not isinstance(value, ConnectorCase) and callable(value):
        value = value()
    if not isinstance(value, ConnectorCase):
        raise ConnectorConformanceCaseInvalid(
            f"--okg-connector {reference!r} is not a ConnectorCase",
            data={"reference": str(reference), "reason": type(value).__name__},
        )
    return value


# --- pytest plugin -----------------------------------------------------------
#
# Registered with `-p okg.deployment.conformance`, never through a pytest11
# entry point; see the module docstring.

_SESSION: dict[str, Any] = {}


def pytest_addoption(parser: Any) -> None:
    group = parser.getgroup("okg-conformance")
    group.addoption(
        "--okg-connector",
        default=None,
        help="module.path:attribute naming a ConnectorCase to certify.",
    )
    group.addoption(
        "--okg-conformance-receipt",
        default=None,
        help="path to write the JSON conformance receipt at session end.",
    )


@pytest.fixture(scope="session")
def okg_connector_case(request: Any) -> ConnectorCase:
    """The case under test, named by `--okg-connector`.

    Not overridable. Under `--pyargs okg.deployment.conformance` this
    module is the collected test module, and a fixture defined in the
    collected module shadows one from a `conftest.py` or a `-p` plugin, so
    `--okg-connector` is the only way in.
    """
    reference = request.config.getoption("--okg-connector")
    if not reference:
        raise ConnectorConformanceCaseInvalid(
            "no connector to certify",
            data={"reference": "", "reason": "--okg-connector not given"},
            fix_hint="pass --okg-connector module.path:CASE",
        )
    return load_case(reference)


@pytest.fixture(scope="session")
def okg_conformance_drives(okg_connector_case: ConnectorCase) -> Drives:
    """Every drive, performed once and shared by every check."""
    _SESSION["case"] = okg_connector_case
    return execute(okg_connector_case)


@pytest.mark.parametrize("okg_conformance_check", sorted(CHECKS))
def test_connector_conformance(
    okg_connector_case: ConnectorCase,
    okg_conformance_drives: Drives,
    okg_conformance_check: str,
) -> None:
    reason = run_check(
        okg_connector_case, okg_conformance_drives, okg_conformance_check,
    )
    _SESSION.setdefault("results", {})[okg_conformance_check] = reason
    assert reason is None, f"{okg_conformance_check}: {reason}"


def pytest_sessionfinish(session: Any, exitstatus: Any) -> None:
    """Write the receipt: every declared check, and the session exit."""
    path = session.config.getoption("--okg-conformance-receipt", None)
    case = _SESSION.get("case")
    if not path or case is None:
        return
    document = receipt(
        case, _SESSION.get("results", {}), session_failed=bool(exitstatus),
    )
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)


__all__ = [
    "CAPABILITY_INPUTS",
    "CAPABILITY_OUTPUTS",
    "CHECKS",
    "COVERAGE",
    "DID_NOT_RUN",
    "ConnectorCase",
    "Drives",
    "HARNESS_PROTOCOL",
    "RECEIPT_SCHEMA",
    "execute",
    "load_case",
    "receipt",
    "run_check",
]

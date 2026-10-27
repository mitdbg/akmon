"""Deployment health, projected into the chat banner (PACT
``chat-health-banner-projection``).

The operator sees a degrading deployment on the console's Home. The chat
user — the person actually reading the stale answer — sees nothing. This
module is the projection that closes that gap: it reads the deployment's
health through the SAME machinery every other status surface reads, maps it
onto one severity-mapped Open WebUI banner, and hands
:mod:`okg.substrate.chat.sync` a composed banner list to reconcile.

What this module does NOT do
---------------------------
It does not write. ``ui.banners`` is written exactly once, by
``_sync_instance_ui``, through the same read/diff/write/readback-and-assert
reconciler every other chat surface goes through. No new write path exists.

It does not compute health. ``trust_level``, ``source_freshness``,
``queues``, ``dlq_count`` and ``doctor`` arrive from an
``okg dogfood status`` payload and are projected through
:func:`okg.substrate.chat.source_admin.shared_status_fields` — the umbrella's
one shared status vocabulary (harmonization commitment 9). The console Health
view, the CLI, and this banner therefore cannot name the same condition with
different words, because there is only one projection.

Four states, and probe failure is one of them
---------------------------------------------
=================  =========================================================
state              meaning
=================  =========================================================
``healthy``        ``trust_level == "ready"``. No owned banner.
``degraded``       ``trust_level == "stale_context"``. Severity ``warning``.
``unavailable``    ``trust_level == "unavailable"``. Severity ``error``.
``probe_failed``   The probe raised, timed out, or answered something this
                   module cannot map. NEVER ``healthy``.
=================  =========================================================

``probe_failed`` is the whole point of :func:`observe`. A probe that raises
must not clear the banner, because "the health check broke" and "the
deployment recovered" are different facts and rendering the first as the
second is the silent fallback this repo treats as a P0 defect. With a prior
owned banner the projection HOLDS it verbatim; with none it writes nothing.
Both arms report loudly — the reason rides the sync report's ``health``
block, and ``sync`` records an unsatisfiable clause so the run's top-line
status is qualified rather than a bare ``verified``.

An answer this module cannot MAP is a probe failure too. A payload carrying
an unknown ``trust_level`` is not a healthy payload; defaulting it to any
state would be a default-value-as-fallback.

Fixed vocabulary, structurally
------------------------------
Issue #1183 forbids secrets from entering generated config, and ``ui.banners``
is generated config. Doctor text, connection errors and DSNs carry hostnames
and credentials, so the banner names WHAT degraded and WHERE TO LOOK, never
HOW the probe said it.

That is enforced by SHAPE, not by scrubbing: :class:`HealthReading` carries
only a state from a closed set, a severity from a closed set, condition
labels from a closed set, and an as-of stamp this module generated. The raw
payload never reaches :func:`compose_content`, and a raising probe's message
is captured in :class:`HealthProbeFailure`, which goes to the report
(redacted) and never to a banner. There is no code path along which probe
text can reach banner text.

The ownership marker lives in ``id``
------------------------------------
The recorded census measured the banner object's field set exactly —
``id/type/title/content/dismissible/timestamp``, set and read back VERBATIM
(``okg/substrate/chat/data/ui-knob-census.json``). An
extra key would be unmeasured, and the vendor drops unknown fields silently
elsewhere (``valves/update`` does exactly that), so a marker in a seventh key
could vanish on the way in and take ownership with it. The marker is
therefore a reserved ``id`` prefix, :data:`OWNER_MARKER` — the same field
``_banner_payload`` already relies on round-tripping.

Ownership is total and narrow: a banner whose ``id`` does not start with the
marker is never modified, never reordered out of the list, and never removed.
That includes the deployment's declared ``chat.ui.banner`` (id
:data:`DECLARED_BANNER_ID`), which stays owned by ``chat-sync-projection``.

The as-of stamp is the state's ONSET, not the last probe
--------------------------------------------------------
``req.chat-health-banner-projection.lifecycle`` requires repeated syncs at the
same health state to change NOTHING. A stamp rewritten on every run would
make that unachievable — every run would rewrite the banner and report a
change forever. So the stamp is the moment the reported state was first
observed: the banner says "this condition has held since T", which is a real
staleness bound rather than a re-assertion of liveness.

Continuity is carried in ``timestamp`` (epoch seconds, the vendor's own
bookkeeping field, round-tripped verbatim per the census) and the state
identity is carried in the ``id`` digest. A run whose composed digest equals
the observed owned banner's id keeps that banner's stamp; a run whose digest
differs is a new state and takes the current stamp.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.chat.source_admin import shared_status_fields
from okg.substrate.errors import SubstrateError

__all__ = [
    "CONDITION_LABELS",
    "DECLARED_BANNER_ID",
    "OWNER_MARKER",
    "SEVERITY_BY_STATE",
    "STATE_BY_TRUST_LEVEL",
    "STATE_DEGRADED",
    "STATE_HEALTHY",
    "STATE_NOT_PROBED",
    "STATE_PROBE_FAILED",
    "STATE_UNAVAILABLE",
    "BANNER_FIELDS",
    "NO_CONDITIONS_TEXT",
    "NO_CONSOLE_TEXT",
    "BannerPlan",
    "HealthProbe",
    "HealthProbeError",
    "HealthProbeFailure",
    "HealthReading",
    "banner_id",
    "cli_health_probe",
    "compose_banner_list",
    "compose_content",
    "console_link",
    "health_report_block",
    "is_owned",
    "observe",
    "read_health",
]


# ---------------------------------------------------------------------------
# Vocabulary. Every string a banner can contain originates in this section.
# ---------------------------------------------------------------------------

#: Reserved ``id`` prefix marking a banner this projection owns. Ownership is
#: decided by this and nothing else — never by content matching, never by
#: position in the list.
OWNER_MARKER = "okg-chat-health"

#: The id ``chat-sync-projection`` stamps on the deployment's DECLARED banner
#: (``sync._banner_payload``). Named here so the composition can tell "the
#: manifest's banner" from "an operator's banner" without matching text.
DECLARED_BANNER_ID = "okg-chat-sync"

STATE_HEALTHY = "healthy"
STATE_DEGRADED = "degraded"
STATE_UNAVAILABLE = "unavailable"
#: The probe raised, timed out, or answered unmappably.
STATE_PROBE_FAILED = "probe_failed"
#: No probe was supplied to this run, so health was never read. Reported on
#: every run rather than inferred — "nobody asked" is not "everything is fine".
STATE_NOT_PROBED = "not_probed"

#: ``trust_level`` (``okg dogfood status``) -> banner state. CLOSED: a value
#: outside this table is a probe failure, not a default.
STATE_BY_TRUST_LEVEL: dict[str, str] = {
    "ready": STATE_HEALTHY,
    "stale_context": STATE_DEGRADED,
    "unavailable": STATE_UNAVAILABLE,
}

#: State -> the vendor banner ``type``, which is also the severity WORD the
#: banner text carries. The word is in the text deliberately: the census
#: proves ``type`` round-trips verbatim but never enumerated which values the
#: frontend styles, so a reader must not depend on colour to learn severity.
SEVERITY_BY_STATE: dict[str, str] = {
    STATE_DEGRADED: "warning",
    STATE_UNAVAILABLE: "error",
}

#: The closed set of phrases naming WHAT degraded. Each is a label for a
#: shared-status-vocabulary field, never a rendering of that field's value.
CONDITION_LABELS: dict[str, str] = {
    "generation_absent": "published generation",
    "source_freshness": "source freshness",
    "source_freshness_unreported": "source freshness (not reported)",
    "queues": "queue depth",
    "dlq": "dead-letter queue",
    "dlq_unreported": "dead-letter queue (not reported)",
    "doctor": "doctor checks",
    "doctor_unreported": "doctor checks (not run)",
}

#: Rendered when a degraded state itemizes no condition. Saying "not itemized"
#: is different from saying "nothing is wrong", and the difference matters.
NO_CONDITIONS_TEXT = "not itemized"

#: Rendered when the deployment declares no ``chat.deep_links.operator_ui_base``.
#: ``chat-deep-links`` forbids a guessed base, a same-origin assumption, and an
#: environment fallback: a plausible-looking dead link is worse than plain text.
NO_CONSOLE_TEXT = "Operator console: not declared by this deployment."

#: The console view that says whether the graph is healthy: its Home. MEASURED
#: from the console's own hash grammar: ``consoleLink("home")`` builds
#: ``#console/home`` (``operator_ui/web/src/operator-ui/routes/console/
#: ConsoleRoute.tsx``). The old Health page at ``#health`` was deleted by PACT
#: console-default-remove-old-views; that hash now also opens the console's
#: Home, but a banner names the real route. Per the chat-deep-links base rule
#: the URL is ``{base}#{hash}`` with no invented path segment.
CONSOLE_HEALTH_HASH = "console/home"


class HealthProbeError(SubstrateError):
    """The health probe could not produce a mappable reading.

    Raised by :func:`cli_health_probe` and :func:`read_health`, and caught by
    :func:`observe`, which converts it into the ``probe_failed`` STATE. It is
    an error only in the sense that health is unknown; it never becomes a
    banner and never fails the sync run.
    """

    code = "chat_health_probe_failed"


# ---------------------------------------------------------------------------
# The reading
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HealthReading:
    """One mapped health observation. Closed vocabulary ONLY.

    This is the boundary the fixed-vocabulary requirement rests on. Nothing
    here is derived from probe TEXT: ``state`` and ``severity`` come from
    closed tables, ``conditions`` are labels from :data:`CONDITION_LABELS`,
    and ``trust_level`` is the raw shared-vocabulary term the payload carried
    (kept for the report, and itself one of three closed values).
    """

    state: str
    severity: str | None
    conditions: tuple[str, ...]
    trust_level: str

    @property
    def degraded(self) -> bool:
        return self.state in SEVERITY_BY_STATE

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "severity": self.severity,
            "conditions": list(self.conditions),
            "trust_level": self.trust_level,
        }


@dataclass(frozen=True)
class HealthProbeFailure:
    """Why health is UNKNOWN for this run.

    ``detail`` is redacted before it is stored and is bound for the sync
    report only. It is structurally incapable of reaching a banner: nothing in
    the composition path accepts a :class:`HealthProbeFailure`.
    """

    reason: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {"reason": self.reason, "detail": self.detail}


def read_health(payload: Mapping[str, Any]) -> HealthReading:
    """Map an ``okg dogfood status`` payload onto a :class:`HealthReading`.

    Reads the payload through :func:`shared_status_fields` rather than
    re-deriving anything, so the banner and ``okg dogfood status`` agree on
    both the NAME and the VALUE of every field by construction.

    Raises :class:`HealthProbeError` when ``trust_level`` is absent or outside
    :data:`STATE_BY_TRUST_LEVEL`. An unmappable answer is not a healthy one.
    """
    if not isinstance(payload, Mapping):
        raise HealthProbeError(
            "the health probe returned "
            f"{type(payload).__name__}, not a status mapping. A reading that "
            "cannot be interpreted is not a healthy reading.",
            data={"reason": "probe_payload_not_a_mapping"},
            fix_hint="return the `okg dogfood status --json` payload",
        )
    fields = shared_status_fields(payload)
    trust_level = fields.get("trust_level")
    state = STATE_BY_TRUST_LEVEL.get(str(trust_level)) if trust_level else None
    if state is None:
        raise HealthProbeError(
            f"the health probe reported trust_level {trust_level!r}, which is "
            f"outside the shared status vocabulary "
            f"{sorted(STATE_BY_TRUST_LEVEL)}. An unmapped level is reported as "
            "a probe failure rather than defaulted to a state — a default here "
            "would let an unreadable deployment render as a healthy one.",
            data={
                "reason": "unmapped_trust_level",
                "trust_level": trust_level,
                "known": sorted(STATE_BY_TRUST_LEVEL),
            },
            fix_hint="check `okg dogfood status --json` for this deployment",
        )
    return HealthReading(
        state=state,
        severity=SEVERITY_BY_STATE.get(state),
        conditions=(
            ()
            if state == STATE_HEALTHY
            else _conditions(
                fields,
                runtime_disabled=_runtime_disabled_by_config(payload),
            )
        ),
        trust_level=str(trust_level),
    )


def _runtime_disabled_by_config(payload: Mapping[str, Any]) -> bool:
    """Did ``okg dogfood status`` report the runtime off by configuration?

    Issue #2987: the dead-letter queue belongs to the runtime worker. With
    ``runtime.enabled: false`` there is no runtime queue to report, so its
    absence is not a condition to name. Issue #3958: the same holds for a
    runtime the ``standard`` profile reads as disabled because it is not
    declared (``not_declared_default_disabled``).
    """
    worker = payload.get("runtime_worker")
    return (
        isinstance(worker, Mapping)
        and worker.get("status") in _RUNTIME_OFF_STATUSES
    )


#: ``runtime_worker.status`` values meaning no runtime worker is expected.
_RUNTIME_OFF_STATUSES = frozenset({
    "disabled_by_config",
    "not_declared_default_disabled",
})


def _conditions(
    fields: Mapping[str, Any],
    *,
    runtime_disabled: bool = False,
) -> tuple[str, ...]:
    """Which shared-vocabulary fields are unhealthy, as LABELS.

    Every arm is an explicit predicate over a field the shared vocabulary
    defines. ``queues`` fires only on an explicit ``ok: False``: the dogfood
    payload's queue block carries depths and no verdict, and inventing a depth
    threshold here would be a made-up number presented as a health rule.

    A field the payload did not report gets its own label. "Not reported" and
    "reported healthy" are different facts, and collapsing them is how a
    partially-answering probe comes to read as a clean one.
    """
    labels: list[str] = []
    if fields.get("generation_id") is None:
        labels.append(CONDITION_LABELS["generation_absent"])

    freshness = fields.get("source_freshness")
    if freshness is None:
        labels.append(CONDITION_LABELS["source_freshness_unreported"])
    elif isinstance(freshness, Mapping) and freshness.get("ok") is False:
        labels.append(CONDITION_LABELS["source_freshness"])

    queues = fields.get("queues")
    if isinstance(queues, Mapping) and queues.get("ok") is False:
        labels.append(CONDITION_LABELS["queues"])

    dlq = fields.get("dlq_count")
    if dlq is None:
        if not runtime_disabled:
            labels.append(CONDITION_LABELS["dlq_unreported"])
    elif isinstance(dlq, int) and dlq > 0:
        labels.append(CONDITION_LABELS["dlq"])

    doctor = fields.get("doctor")
    if doctor is None:
        labels.append(CONDITION_LABELS["doctor_unreported"])
    elif isinstance(doctor, Mapping) and doctor.get("ok") is False:
        labels.append(CONDITION_LABELS["doctor"])

    return tuple(labels)


# ---------------------------------------------------------------------------
# The probe
# ---------------------------------------------------------------------------

#: Deployment name -> an ``okg dogfood status`` payload. Injectable so the
#: suites can drive degraded, healthy, and raising probes without a database.
HealthProbe = Callable[[str], Mapping[str, Any]]


def cli_health_probe(
    deployment: str,
    *,
    okg_cli: str | None = None,
    timeout: float = 120.0,
    runner: Callable[[Sequence[str], float], tuple[int, str, str]] | None = None,
) -> Mapping[str, Any]:
    """The real probe: ``okg dogfood status --deployment D --json``.

    argv list, ``shell=False``, exactly as ``review_queue_pipe.run_okg`` reaches
    the substrate.

    A NONZERO EXIT IS NOT A PROBE FAILURE. ``okg dogfood status`` exits 1 for a
    deployment that is not ready, and that run still prints a complete payload
    whose ``trust_level`` is the answer this projection wants. Gating on the
    exit code would turn every degraded deployment — the exact case the banner
    exists for — into "health unknown". The gate is on a payload that parses
    and carries a mappable ``trust_level``, which :func:`read_health` decides.
    """
    # ``okg_cli=None`` is THIS okg (issue #2301), not the first on PATH.
    prefix = running_okg_argv() if okg_cli is None else [okg_cli]
    argv = [*prefix, "dogfood", "status", "--deployment", deployment, "--json"]
    if runner is not None:
        returncode, stdout, stderr = runner(argv, timeout)
    else:
        completed = subprocess.run(  # noqa: S603 - argv list, shell=False
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,
        )
        returncode, stdout, stderr = (
            completed.returncode, completed.stdout, completed.stderr,
        )
    if not (stdout or "").strip():
        raise HealthProbeError(
            "`okg dogfood status --json` printed nothing, so this run has no "
            "health reading at all.",
            data={
                "reason": "probe_empty_output",
                "returncode": returncode,
                "command": " ".join(argv),
            },
            fix_hint=f"run `{' '.join(argv)}` and read the error",
        )
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise HealthProbeError(
            "`okg dogfood status --json` did not print JSON, so this run has "
            "no health reading.",
            data={
                "reason": "probe_output_unparseable",
                "returncode": returncode,
                "command": " ".join(argv),
            },
            fix_hint=f"run `{' '.join(argv)}` and read the output",
        ) from exc
    if not isinstance(payload, Mapping):
        raise HealthProbeError(
            "`okg dogfood status --json` printed JSON that is not a status "
            f"mapping ({type(payload).__name__}).",
            data={"reason": "probe_payload_not_a_mapping"},
            fix_hint=f"run `{' '.join(argv)}` and read the output",
        )
    del stderr  # never rendered: it is the probe's own text, which the
    # fixed-vocabulary rule keeps out of everything this module produces.
    return payload


def observe(
    deployment: str,
    *,
    probe: HealthProbe,
) -> tuple[HealthReading | None, HealthProbeFailure | None]:
    """Run the probe. Exactly one of the two results is not ``None``.

    Every exception is caught, including the ones nobody enumerated. A probe
    is an I/O call into another subsystem, and the PACT names "errors or times
    out" as ONE state: letting an unanticipated exception escape would abort
    the sync run and, with it, the banner-holding behaviour that is the whole
    point of the probe-failure arm. ``BaseException`` is deliberately NOT
    caught — a ``KeyboardInterrupt`` is the operator stopping the run, not a
    deployment reporting on its health.

    The failure's ``detail`` is redacted through the sync module's own
    :func:`~okg.substrate.chat.sync.redact`, which is the same scrubbing every
    other error text in this program passes through.
    """
    from okg.substrate.chat.sync import redact

    try:
        payload = probe(deployment)
        return read_health(payload), None
    except HealthProbeError as exc:
        return None, HealthProbeFailure(
            reason=str(exc.data.get("reason") or "probe_failed"),
            detail=redact(str(exc)),
        )
    except Exception as exc:  # noqa: BLE001 - see docstring
        return None, HealthProbeFailure(
            reason=f"probe_raised:{type(exc).__name__}",
            detail=redact(f"{type(exc).__name__}: {exc}"),
        )


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def is_owned(banner: Any) -> bool:
    """Does this projection own this banner object?

    The single ownership predicate. Everything that is not this is foreign and
    is preserved untouched — including the deployment's declared banner.
    """
    if not isinstance(banner, Mapping):
        return False
    identifier = banner.get("id")
    if not isinstance(identifier, str):
        return False
    return identifier == OWNER_MARKER or identifier.startswith(OWNER_MARKER + ":")


def banner_id(reading: HealthReading) -> str:
    """The owned banner's id: the marker plus a digest of the STATE.

    Two jobs in one field, both required by the census-measured object shape
    (no seventh key is available): it marks ownership, and it makes "the same
    state as last run" decidable without parsing the banner's text.
    """
    material = "|".join(
        (reading.state, reading.severity or "", *reading.conditions)
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    return f"{OWNER_MARKER}:{digest}"


def console_link(operator_ui_base: str | None) -> str | None:
    """``{base}#console/home``, or ``None`` when the deployment declares no base."""
    base = (operator_ui_base or "").strip()
    if not base:
        return None
    return f"{base}#{CONSOLE_HEALTH_HASH}"


def compose_content(
    reading: HealthReading,
    *,
    as_of: int,
    operator_ui_base: str | None,
) -> str:
    """The banner's text, from the fixed vocabulary and NOTHING else.

    Four clauses, exactly the four the PACT's content rule admits: the state,
    its severity, the as-of stamp, and the console deep link. Every substring
    is either a module constant, a closed-set term, a generated ISO timestamp,
    or the deployment's own declared console base.
    """
    conditions = ", ".join(reading.conditions) or NO_CONDITIONS_TEXT
    link = console_link(operator_ui_base)
    console_clause = (
        f"Operator console: {link}" if link is not None else NO_CONSOLE_TEXT
    )
    return (
        f"OKG graph health: {reading.state} ({reading.severity}). "
        f"Affected: {conditions}. "
        f"State as of {_iso(as_of)}. "
        f"{console_clause}"
    )


def _iso(epoch_seconds: int) -> str:
    return (
        datetime.fromtimestamp(epoch_seconds, timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


#: The census-measured banner object field set. Composition and readback both
#: project onto exactly these, so the comparison can converge.
BANNER_FIELDS: tuple[str, ...] = (
    "id", "type", "title", "content", "dismissible", "timestamp",
)


def _normalized(banner: Mapping[str, Any]) -> dict[str, Any]:
    return {name: banner.get(name) for name in BANNER_FIELDS}


def _owned_banner(
    reading: HealthReading, *, as_of: int, operator_ui_base: str | None,
) -> dict[str, Any]:
    return {
        "id": banner_id(reading),
        "type": reading.severity,
        "title": None,
        "content": compose_content(
            reading, as_of=as_of, operator_ui_base=operator_ui_base,
        ),
        "dismissible": True,
        # The as-of stamp, in the vendor's own bookkeeping field. Read back out
        # on the next run so an unchanged state keeps its ONSET rather than
        # being re-stamped into a permanent change report.
        "timestamp": as_of,
    }


@dataclass(frozen=True)
class BannerPlan:
    """The effective desired ``ui.banners`` list, and what happened to it."""

    #: declared + foreign + at most one owned, in that composition order.
    banners: tuple[dict[str, Any], ...]
    #: What the owned slot became: ``raised``, ``held``, ``cleared``, or
    #: ``absent``. Recorded on every arm so "nothing happened" is attributable.
    owned_action: str
    #: How many banners this projection preserved untouched.
    foreign_preserved: int
    #: The owned banner's as-of stamp, when there is one.
    as_of: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "owned_action": self.owned_action,
            "foreign_preserved": self.foreign_preserved,
            "as_of": _iso(self.as_of) if self.as_of is not None else None,
        }


def compose_banner_list(
    observed: Sequence[Any],
    *,
    declared: Sequence[Mapping[str, Any]] = (),
    reading: HealthReading | None,
    failure: HealthProbeFailure | None,
    now: int,
    operator_ui_base: str | None = None,
) -> BannerPlan:
    """The effective desired state: declared banners + at most one owned one.

    This is the seam ``chat-sync-projection`` reconciles against. Composed FROM
    the observed list, because the vendor's ``POST /api/v1/configs/banners``
    replaces the entire list: a desired value that did not carry the foreign
    banners forward would delete them on every run.

    ``failure`` is not None
        The owned slot is left EXACTLY as observed — held if there was one,
        empty if there was not. A probe that broke is never a recovery.

    ``reading`` is healthy
        The owned banner is removed. Only the owned one: the ownership
        predicate is the only thing consulted.

    ``reading`` is degraded or unavailable
        One owned banner is composed. If the state digest matches the observed
        owned banner's id, its as-of stamp is carried forward, so a re-run at
        the same state produces a byte-identical list and reconciles to zero
        changes.

    ``reading`` and ``failure`` both None
        No probe ran. The owned slot is left as observed, exactly as on the
        failure arm — this function never invents a health claim.
    """
    normalized = [
        _normalized(item) for item in observed if isinstance(item, Mapping)
    ]
    declared_ids = {
        str(item.get("id")) for item in declared if isinstance(item, Mapping)
    }
    prior_owned = next((b for b in normalized if is_owned(b)), None)
    foreign = [
        b for b in normalized
        if not is_owned(b) and str(b.get("id")) not in declared_ids
    ]
    declared_normalized = [_normalized(item) for item in declared]

    owned: dict[str, Any] | None
    action: str
    if failure is not None or reading is None:
        # Held verbatim, INCLUDING its original stamp. Re-stamping a held
        # banner would assert that health was re-observed, which is the claim
        # the probe failed to make.
        owned = prior_owned
        action = "held" if prior_owned is not None else "absent"
    elif not reading.degraded:
        owned = None
        action = "cleared" if prior_owned is not None else "absent"
    else:
        candidate_id = banner_id(reading)
        carried = (
            prior_owned.get("timestamp")
            if prior_owned is not None and prior_owned.get("id") == candidate_id
            else None
        )
        as_of = int(carried) if isinstance(carried, int) else now
        owned = _owned_banner(
            reading, as_of=as_of, operator_ui_base=operator_ui_base,
        )
        action = "raised"

    banners = tuple(declared_normalized + foreign + ([owned] if owned else []))
    stamp = owned.get("timestamp") if owned is not None else None
    return BannerPlan(
        banners=banners,
        owned_action=action,
        foreign_preserved=len(foreign),
        as_of=int(stamp) if isinstance(stamp, int) else None,
    )


def health_report_block(
    *,
    reading: HealthReading | None,
    failure: HealthProbeFailure | None,
    plan: BannerPlan | None,
    probed: bool,
) -> dict[str, Any]:
    """The sync report's ``health`` block. Printed on EVERY run.

    Every arm is named. A run that never probed says so; a run whose probe
    failed says so and says why; a healthy run says healthy. There is no arm
    that prints nothing, because silence is what makes a broken health check
    indistinguishable from a healthy deployment.
    """
    if not probed:
        state = STATE_NOT_PROBED
    elif failure is not None:
        state = STATE_PROBE_FAILED
    elif reading is not None:
        state = reading.state
    else:  # pragma: no cover - observe() always returns one or the other
        state = STATE_PROBE_FAILED
    block: dict[str, Any] = {
        "state": state,
        "ok": state == STATE_HEALTHY,
        "probed": probed,
        "reading": reading.to_dict() if reading is not None else None,
        "probe_failure": failure.to_dict() if failure is not None else None,
        "banner": plan.to_dict() if plan is not None else None,
    }
    if not probed:
        block["reason"] = (
            "this run was given no health probe, so deployment health was "
            "never read and no banner was touched. Not probed is not healthy."
        )
    return block

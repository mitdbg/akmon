"""Publisher lock-hold headroom policy: the capacity contract for a publish.

Issue #1291. The publisher runs every statement of a publish cycle under a
session advisory lock bounded by a hard ``statement_timeout``
(:data:`DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS`, 900s). A staging recovery
measured a 881.6s hold against that 900s bound — 18.4 seconds of margin — and
the only consequence was a log warning. A publish that succeeds by 2% of its
timeout is not a healthy release: one more source, one concurrent scan, or one
plan change converts it into a wedged publisher.

This module owns the *policy* so the publisher receipt, the shared publish
proof, and deployment readiness all apply one bar:

    measured lock hold <= hard statement timeout / required safety factor

The default factor is 2.0 (hold must fit in 50% of the hard timeout), matching
the post-mortem recommendation. Issue #1479 moved a PUBLISHER's copy of these
settings into the deployment manifest --
``publisher.hard_statement_timeout_seconds``,
``publisher.lock_hold_headroom_factor`` and
``publisher.allow_raised_statement_timeout`` -- so
``OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS`` and
``OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR`` no longer reach a constructed
publisher. They still configure THIS module's readers, which is what the
clamping below is about.

The bound is the smaller of the timeout the publish ran under and the built-in
900s default, so raising the timeout cannot relax the release bar on its own; a
run that genuinely needs the longer bound (a cold interval rebuild) sets
``OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT=1``, and the verdict records
that the bar was widened. Setting the statement timeout to 0 disables it
entirely and therefore fails this gate as ``unbounded`` -- a behavior change for
any deployment that disabled it.

A publish receipt is the ARTIFACT UNDER JUDGEMENT, never the judge.
:func:`lock_headroom_from_payload` therefore reads the manifest-declared factor
and raised-timeout authorization out of a receipt only when they make the bar
STRICTER, and requires the reader's own
``OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT`` before honouring a bound
above the default. Otherwise a deployment declaring ``lock_hold_headroom_factor:
1.0`` beside a two-hour timeout would pass a two-hour lock hold by writing its
own verdict.

Absence is never agreement. :func:`evaluate_lock_headroom` reports five
distinct states and only two of them are a pass:

``pass``            measured hold is inside the policy bound.
``blocked``         measured hold lacks the required safety factor.
``unmeasured``      no lock hold was measured — the gate cannot clear.
``unbounded``       the hard statement timeout is disabled, so nothing bounds
                    the hold at all — also not a pass.
``not_applicable``  the caller proved no publish work ran under the lock (a
                    skipped/no-source-work cycle, or an already-current
                    generation observed without publishing). Callers must state
                    this explicitly; it is never inferred from a missing field.
"""
from __future__ import annotations

import logging
import math
import os
from collections.abc import Mapping
from typing import Any

LOCK_HEADROOM_SCHEMA = "okg.publisher.lock_headroom.v1"
LOCK_HEADROOM_POLICY_SCHEMA = "okg.publisher.lock_headroom_policy.v1"

# Kept here rather than in publisher.py so readiness and the publish proof can
# resolve the same bound without importing the publisher. publisher.py imports
# this name; the value (900s) is unchanged.
DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS = 900.0
STATEMENT_TIMEOUT_ENV = "OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS"

# hold <= hard timeout / 2 (50% of the bound). The 2026-08 staging post-mortem
# recommends a 2x safety factor on critical publish phases.
DEFAULT_LOCK_HOLD_HEADROOM_FACTOR = 2.0
LOCK_HEADROOM_FACTOR_ENV = "OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR"
# A cold rebuild legitimately raises the statement timeout past 900s (see
# pact/changes/rebuild-af-pubs-interval-storage). Clamping the bar to the 900s
# default with no escape would make its publish proof unpassable, so the raise
# is honored ONLY when the operator names it here, and the verdict records
# that it happened.
RAISED_TIMEOUT_OVERRIDE_ENV = "OKG_PUBLISH_LOCK_HEADROOM_ALLOW_RAISED_TIMEOUT"

PASSING_STATUSES = frozenset({"pass", "not_applicable"})

_FIX_HINT_BLOCKED = (
    "reduce publish lock hold (split the slow phase, index the delta scan, or "
    "shrink the cycle) until it fits the configured headroom; raising "
    f"{LOCK_HEADROOM_FACTOR_ENV} only relaxes the release bar"
)
_FIX_HINT_UNMEASURED = (
    "publish once with a publisher that records timing_details.lock.hold_ms "
    "and re-run this proof; an unmeasured hold cannot demonstrate headroom"
)
_FIX_HINT_UNBOUNDED = (
    "set publisher.hard_statement_timeout_seconds in deployment.yaml back to "
    "a positive bound so a runaway publish statement cannot hold the advisory "
    f"lock indefinitely ({STATEMENT_TIMEOUT_ENV} no longer reaches a "
    "constructed publisher)"
)


def resolve_hard_timeout_ms(env: Mapping[str, str] | None = None) -> int:
    """Resolve the publisher's hard per-statement timeout in milliseconds.

    0 means the bound is disabled. An unparsable override falls back to the
    default and says so in the log rather than failing open silently.
    """
    source = os.environ if env is None else env
    raw = source.get(STATEMENT_TIMEOUT_ENV)
    secs = DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS
    if raw is not None:
        # `inf` and `nan` parse but blow up in int(); a negative value is a
        # typo, not a deliberate "0 disables". Readiness and the publish proof
        # now call this too, so a malformed variable must not turn one
        # publisher crash into a crash in every authority that reads the bound.
        parsed = _as_float(raw)
        if parsed is None or parsed < 0:
            logging.getLogger(__name__).warning(
                "ignoring unusable %s=%r; using the default %.0fs bound",
                STATEMENT_TIMEOUT_ENV, raw,
                DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS,
            )
        else:
            secs = parsed
    if secs <= 0:
        return 0
    return max(1, int(secs * 1000))


def resolve_headroom_factor(
    env: Mapping[str, str] | None = None,
) -> tuple[float, str]:
    """Return ``(required_factor, factor_source)``.

    ``factor_source`` is ``"default"``, ``"env"``, or
    ``"default_after_invalid_env"``. An unusable override never silently
    becomes a weaker bar: it is logged and replaced by the (stricter or equal)
    built-in default, and the receipt records that it happened.
    """
    source = os.environ if env is None else env
    raw = source.get(LOCK_HEADROOM_FACTOR_ENV)
    if raw is None:
        return (DEFAULT_LOCK_HOLD_HEADROOM_FACTOR, "default")
    try:
        factor = float(raw)
    except ValueError:
        factor = 0.0
    # NaN parses and compares False against every bound, so a bare `< 1.0`
    # guard would let `nan` through and make `hold > max_hold` permanently
    # false: the gate would accept the 881.6s hold it exists to refuse.
    if not math.isfinite(factor) or factor < 1.0:
        logging.getLogger(__name__).warning(
            "ignoring unusable %s=%r (must be >= 1.0); using the default %.1fx "
            "publish lock-hold safety factor",
            LOCK_HEADROOM_FACTOR_ENV, raw, DEFAULT_LOCK_HOLD_HEADROOM_FACTOR,
        )
        return (DEFAULT_LOCK_HOLD_HEADROOM_FACTOR, "default_after_invalid_env")
    return (factor, "env")


def headroom_policy(
    *,
    hard_timeout_ms: int | float | None = None,
    hard_timeout_source: str | None = None,
    required_factor: int | float | None = None,
    factor_source: str | None = None,
    allow_raised_timeout: bool | None = None,
    allow_raised_timeout_source: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """The configured capacity policy, independent of any measurement."""
    if hard_timeout_ms is None:
        hard_timeout_ms = resolve_hard_timeout_ms(env)
        # Overwrite unconditionally. The value in hand now came from the
        # environment, so keeping a caller-supplied "deployment_manifest" here
        # would report a provenance this policy did not use.
        hard_timeout_source = "environment_compatibility"
    if required_factor is None:
        factor, resolved_factor_source = resolve_headroom_factor(env)
        factor_source = factor_source or resolved_factor_source
    else:
        parsed_factor = _as_float(required_factor)
        if parsed_factor is None or parsed_factor < 1.0:
            raise ValueError("required_factor must be finite and >= 1.0")
        factor = parsed_factor
        factor_source = factor_source or "explicit"
    hard_ms = _as_float(hard_timeout_ms)
    # The release bar is derived from the SMALLER of the timeout this publish
    # actually ran under and the built-in 900s bound. Otherwise the same knob
    # whose danger this gate exists to bound -- raise the statement timeout --
    # would silently relax the bar: an 881.6s hold would pass at a 3600s
    # timeout. Lowering the timeout still tightens the bar.
    default_ms = DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS * 1000.0
    source = os.environ if env is None else env
    if allow_raised_timeout is None:
        allow_raised = str(
            source.get(RAISED_TIMEOUT_OVERRIDE_ENV) or "",
        ).strip().lower() in {"1", "true", "yes"}
        allow_raised_timeout_source = (
            allow_raised_timeout_source or "environment_compatibility"
        )
    elif not isinstance(allow_raised_timeout, bool):
        raise ValueError("allow_raised_timeout must be a boolean")
    else:
        allow_raised = allow_raised_timeout
        allow_raised_timeout_source = allow_raised_timeout_source or "explicit"
    policy_ms: float | None
    max_hold_ms: float | None
    policy_timeout_source = "running_timeout"
    if hard_ms is None or hard_ms <= 0:
        policy_ms = None
        max_hold_ms = None
        policy_timeout_source = "none"
    elif hard_ms > default_ms and not allow_raised:
        # Never silently widened: the same knob whose danger this gate bounds
        # cannot relax it by itself. An operator who genuinely needs the longer
        # bound (a cold rebuild) sets RAISED_TIMEOUT_OVERRIDE_ENV, and the
        # verdict records that the bar was widened deliberately.
        policy_ms = default_ms
        max_hold_ms = round(policy_ms / factor, 3)
        policy_timeout_source = "clamped_to_default"
    else:
        policy_ms = hard_ms
        max_hold_ms = round(policy_ms / factor, 3)
        if hard_ms > default_ms:
            policy_timeout_source = "raised_timeout_override"
    return {
        "schema": LOCK_HEADROOM_POLICY_SCHEMA,
        "required_factor": factor,
        "factor_source": factor_source,
        "factor_env": LOCK_HEADROOM_FACTOR_ENV,
        "hard_timeout_ms": None if hard_ms is None else round(hard_ms, 3),
        "hard_timeout_source": hard_timeout_source or "explicit",
        "policy_timeout_ms": None if policy_ms is None else round(policy_ms, 3),
        "policy_timeout_source": policy_timeout_source,
        "policy_timeout_env": RAISED_TIMEOUT_OVERRIDE_ENV,
        "allow_raised_timeout": allow_raised,
        "allow_raised_timeout_source": allow_raised_timeout_source,
        "timeout_above_default": bool(
            hard_ms is not None and hard_ms > default_ms
        ),
        "max_hold_ms": max_hold_ms,
    }


def evaluate_lock_headroom(
    *,
    hold_ms: Any,
    hard_timeout_ms: int | float | None = None,
    hard_timeout_source: str | None = None,
    required_factor: int | float | None = None,
    factor_source: str | None = None,
    allow_raised_timeout: bool | None = None,
    allow_raised_timeout_source: str | None = None,
    wait_ms: Any = None,
    slowest_phase: Mapping[str, Any] | None = None,
    not_applicable_reason: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Verdict for one measured publish cycle. ``ok`` is True only for a
    measured hold inside the bound, or an explicitly stated exemption."""
    policy = headroom_policy(
        hard_timeout_ms=hard_timeout_ms,
        hard_timeout_source=hard_timeout_source,
        required_factor=required_factor,
        factor_source=factor_source,
        allow_raised_timeout=allow_raised_timeout,
        allow_raised_timeout_source=allow_raised_timeout_source,
        env=env,
    )
    verdict: dict[str, Any] = {
        "schema": LOCK_HEADROOM_SCHEMA,
        "policy": policy,
        "required_factor": policy["required_factor"],
        "hard_timeout_ms": policy["hard_timeout_ms"],
        "max_hold_ms": policy["max_hold_ms"],
        "policy_timeout_ms": policy["policy_timeout_ms"],
        "hold_ms": _as_float(hold_ms),
        "wait_ms": _as_float(wait_ms),
        "measured": _as_float(hold_ms) is not None,
        "observed_factor": None,
        "remaining_ms": None,
        "slowest_phase": dict(slowest_phase) if slowest_phase else None,
    }
    if not_applicable_reason is not None:
        verdict.update({
            "ok": True,
            "status": "not_applicable",
            "reason": (
                "publish lock-hold headroom does not apply: "
                f"{not_applicable_reason}"
            ),
        })
        return verdict

    hold = verdict["hold_ms"]
    hard = policy["policy_timeout_ms"]
    if hard is None or hard <= 0:
        verdict.update({
            "ok": False,
            "status": "unbounded",
            "reason": (
                "the publisher hard statement timeout is disabled, so the "
                "publish lock hold has no bound to demonstrate headroom "
                "against"
            ),
            "fix_hint": _FIX_HINT_UNBOUNDED,
        })
        return verdict
    if hold is None:
        verdict.update({
            "ok": False,
            "status": "unmeasured",
            "reason": (
                "no publish lock hold was measured, so the required "
                f"{policy['required_factor']}x timeout headroom is unproven"
            ),
            "fix_hint": _FIX_HINT_UNMEASURED,
        })
        return verdict

    if hold < 0:
        verdict.update({
            "ok": False,
            "status": "unmeasured",
            "measured": False,
            "reason": (
                f"the recorded publish lock hold is negative ({hold} ms), so "
                "the measurement is unusable and the required "
                f"{policy['required_factor']}x timeout headroom is unproven"
            ),
            "fix_hint": _FIX_HINT_UNMEASURED,
        })
        return verdict
    verdict["remaining_ms"] = round(hard - hold, 3)
    # Float division throughout: an integer floor would report 1x headroom as
    # 1x for a hold anywhere in the upper half of the bound.
    verdict["observed_factor"] = (
        round(hard / hold, 3) if hold > 0 else None
    )
    max_hold = policy["max_hold_ms"]
    if max_hold is not None and hold > max_hold:
        verdict.update({
            "ok": False,
            "status": "blocked",
            "reason": (
                f"publish held the lock {hold / 1000.0:.1f}s against a "
                f"{hard / 1000.0:.0f}s hard statement timeout; the release bar "
                f"is {policy['required_factor']}x headroom "
                f"({max_hold / 1000.0:.1f}s)"
                + (
                    "; the bound is clamped to the built-in 900s default "
                    f"because the running timeout is longer (set "
                    f"{RAISED_TIMEOUT_OVERRIDE_ENV}=1 to judge against it)"
                    if policy["policy_timeout_source"] == "clamped_to_default"
                    else ""
                )
            ),
            "fix_hint": _FIX_HINT_BLOCKED,
        })
        return verdict
    verdict.update({
        "ok": True,
        "status": "pass",
        "reason": (
            f"publish held the lock {hold / 1000.0:.1f}s, within the "
            f"{policy['required_factor']}x headroom bound "
            f"({(max_hold or 0.0) / 1000.0:.1f}s)"
        ),
    })
    return verdict


def lock_receipts(payload: Any) -> list[Mapping[str, Any]]:
    """Every ``timing_details.lock`` receipt in a publish or ingest payload.

    An ingest report carries one entry per publish it ran. A long hold in the
    first publish must not be hidden by a fast third one, so every receipt is
    collected and the caller judges the worst.
    """
    found: list[Mapping[str, Any]] = []
    if not isinstance(payload, Mapping):
        return found
    details = payload.get("timing_details")
    if isinstance(details, Mapping):
        lock = details.get("lock")
        if isinstance(lock, Mapping):
            found.append(lock)
    for key in ("publish", "last_publish"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            found.extend(lock_receipts(nested))
    publishes = payload.get("publishes")
    if isinstance(publishes, list):
        for item in publishes:
            found.extend(lock_receipts(item))
    return found


def lock_receipt(payload: Any) -> Mapping[str, Any] | None:
    """The receipt whose hold is worst (an unmeasured hold sorts worst).

    Returns None only when the payload carries no lock receipt at all, which
    callers must treat as unproven, never as healthy.
    """
    receipts = lock_receipts(payload)
    if not receipts:
        return None

    def _rank(lock: Mapping[str, Any]) -> tuple[int, float]:
        hold = _as_float(lock.get("hold_ms"))
        # Unmeasured sorts above every measured hold: it cannot clear the bar.
        return (1, 0.0) if hold is None else (0, hold)

    return max(receipts, key=_rank)


def lock_headroom_from_payload(
    payload: Any,
    *,
    not_applicable_reason: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Evaluate the headroom policy against a publish payload's receipt.

    A payload with no lock receipt yields ``unmeasured`` (not a pass), unless
    the caller states an exemption.
    """
    if not_applicable_reason is not None:
        return evaluate_lock_headroom(
            hold_ms=None,
            not_applicable_reason=not_applicable_reason,
            env=env,
        )
    lock = lock_receipt(payload)
    if lock is None:
        verdict = evaluate_lock_headroom(hold_ms=None, env=env)
        verdict["reason"] = (
            "the publish payload carried no lock receipt, so the required "
            f"{verdict['required_factor']}x timeout headroom is unproven"
        )
        return verdict
    recorded = lock.get("headroom")
    recorded_policy = (
        recorded.get("policy")
        if isinstance(recorded, Mapping)
        and isinstance(recorded.get("policy"), Mapping)
        else {}
    )
    hard_timeout_ms = lock.get("hard_timeout_ms")
    if hard_timeout_ms is None and isinstance(recorded, Mapping):
        hard_timeout_ms = recorded.get("hard_timeout_ms")
    # A receipt is the ARTIFACT UNDER JUDGEMENT, never the judge. The publisher
    # that wrote it now takes its factor and its raised-timeout authorization
    # from the deployment manifest, so honouring those verbatim would let a
    # deployment author its own passing verdict: factor 1.0 plus a two-hour
    # timeout would pass a two-hour lock hold, which is the exact hold this
    # module exists to refuse. The receipt may only make the bar STRICTER.
    reader_factor, _reader_factor_source = resolve_headroom_factor(env)
    recorded_factor = _as_float(recorded_policy.get("required_factor"))
    if recorded_factor is None or recorded_factor < reader_factor:
        effective_factor: float | None = None
        effective_factor_source: str | None = None
    else:
        effective_factor = recorded_factor
        effective_factor_source = recorded_policy.get("factor_source")
    # Widening the bound past the 900s compatibility default needs the READER's
    # own authorization as well as the publisher's. Either side may refuse.
    reader_allows_raised = str(
        (os.environ if env is None else env).get(RAISED_TIMEOUT_OVERRIDE_ENV)
        or "",
    ).strip().lower() in {"1", "true", "yes"}
    recorded_allows_raised = recorded_policy.get("allow_raised_timeout")
    if not isinstance(recorded_allows_raised, bool):
        effective_allow_raised: bool | None = None
        effective_allow_raised_source: str | None = None
    else:
        effective_allow_raised = recorded_allows_raised and reader_allows_raised
        effective_allow_raised_source = (
            recorded_policy.get("allow_raised_timeout_source")
            if effective_allow_raised
            else "reader_withheld_raised_timeout"
        )
    verdict = evaluate_lock_headroom(
        hold_ms=lock.get("hold_ms"),
        hard_timeout_ms=hard_timeout_ms,
        hard_timeout_source=(
            recorded_policy.get("hard_timeout_source")
            or ("receipt" if hard_timeout_ms is not None else None)
        ),
        required_factor=effective_factor,
        factor_source=effective_factor_source,
        allow_raised_timeout=effective_allow_raised,
        allow_raised_timeout_source=effective_allow_raised_source,
        wait_ms=lock.get("wait_ms", lock.get("acquire_ms")),
        slowest_phase=(
            lock.get("slowest_phase")
            if isinstance(lock.get("slowest_phase"), Mapping)
            else None
        ),
        env=env,
    )
    # Say where the bound came from: a receipt written by an older publisher
    # carries a hold but no bound, and the reader's environment is only a
    # stand-in for the bound the publish actually ran under.
    verdict["hard_timeout_source"] = (
        (recorded_policy.get("hard_timeout_source")
         if recorded_policy.get("hard_timeout_source") != "explicit" else None)
        or ("receipt" if hard_timeout_ms is not None else "environment")
    )
    return verdict


def _as_float(value: Any) -> float | None:
    """Coerce a measurement, rejecting anything that cannot be compared.

    NaN and infinity survive ``float()`` but compare False against every
    bound, so treating them as measurements would turn corrupt instrumentation
    into a pass. They come back as None, which every caller reads as
    unmeasured.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None

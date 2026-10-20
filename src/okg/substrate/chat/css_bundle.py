"""Deliver a deployment's declared CSS bytes to its instance (PACT
``chat-css-bundle-contract``).

This is a BYTE-DELIVERY surface, and the distinction from every other surface
in this package is the whole point of the module existing separately.

What was measured, and what follows from it
-------------------------------------------
The recorded knob census
(``okg/substrate/chat/data/ui-knob-census.json``,
``knobs[theme.accent]``) measured Open WebUI v0.11.0 as exposing no styling
knob at all: no config key, no environment variable, no admin API, and no
value the instance can be asked for. The one lever is a static file. The
served ``index.html`` links ``/static/custom.css`` UNCONDITIONALLY and reads
no value out of it, which is exactly what makes this delivery rather than
configuration — the instance is never asked, and never answers, what it is
styled like.

So this module claims one thing and nothing more: **the bytes named by
``chat.css_bundle.ref`` are the bytes the instance serves at
``/static/custom.css``.** Every report field below is a fact about bytes. No
function here reads, derives, or reports a styling VALUE, because there is
none to read; ``chat.ui.theme`` remains refused at manifest load
(``deployments/chat_block.py``) for the same measured reason.

**The static directory GOTCHA.** The served directory is
:data:`STATIC_DIR` — ``backend/open_webui/static`` — NOT the frontend build's
``build/static``. The census measured both: a 165-byte file written to
``/app/build/static/custom.css`` still served nothing, and the same file
written to ``/app/backend/open_webui/static/custom.css`` served immediately.
Writing to the wrong one is a silent no-op with a green-looking write.

**No upload API exists.** The census re-verified every inbound route touching
the static directory: ``main.py:2823`` mounts it as ``StaticFiles``, which
serves GET and HEAD and exposes no write verb, and no POST/PUT/PATCH handler
writes into it. Bytes therefore reach the directory through the CONTAINER
RUNTIME — :meth:`okg.substrate.chat.instance.ContainerRuntime.execute`, the
one seam this package already has for reaching inside a running instance
(``instance.vendor_default_store_present`` uses the same one). There is no
volume mount over the static directory in
:meth:`~okg.substrate.chat.instance.InstanceSpec.run_argv`, so a host-side
write has nothing to write to.

**The ``CUSTOM_NAME`` hazard.** ``config.py:184-211`` is a legacy OUTBOUND
branding path: a non-empty ``CUSTOM_NAME`` makes the process fetch
``api.openwebui.com/api/v1/custom/<name>`` at import time and, on HTTP 200,
overwrite static branding assets and reassign the site name from the
response. An instance with it set has a third party writing into the very
directory this module delivers to, so the delivery is refused there rather
than raced. :func:`okg.substrate.chat.instance.build_instance_spec` already
refuses to LAUNCH with it set; this refusal covers the instance somebody
started by hand.

Four states, and why none of them is inferred from another
-----------------------------------------------------------
* **never declared** — no ``chat.css_bundle`` and no recorded ship. The
  styling surface is left strictly alone: nothing is read, nothing is
  executed, nothing is removed.
* **declared** — the bytes are shipped and the readback proves byte identity.
* **drifted** — the served bytes differ from the declared file. Reported, then
  reconverged.
* **retracted** — no declaration, but this deployment RECORDED a ship. The
  bytes are removed and the vendor default is restored. Distinguishing this
  from never-declared is what the ship record exists for: read as
  never-declared, a retraction would leave stale branding on the instance,
  which is precisely the hand-applied hack this contract replaces.

Nothing degrades to a default. Every way this can fail — an unreadable
declared file, an unreadable ship record, an absent container, an exec that
did not exit zero, a readback that did not match — is a named refusal on the
outcome, never a quieter success.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from okg.substrate.errors import SubstrateError

from .instance import CONTAINER_FORBIDDEN_VARS


__all__ = [
    "CONTAINER_CSS_PATH",
    "CSS_BASENAME",
    "CUSTOM_NAME_VAR",
    "MAX_BUNDLE_BYTES",
    "REFUSAL_BUNDLE_TOO_LARGE",
    "REFUSAL_BUNDLE_UNREADABLE",
    "REFUSAL_CONTAINER_ABSENT",
    "REFUSAL_CUSTOM_NAME_SET",
    "REFUSAL_CUSTOM_NAME_UNDETERMINED",
    "REFUSAL_NO_CONTAINER_RUNTIME",
    "REFUSAL_REMOVAL_FAILED",
    "REFUSAL_REMOVAL_UNVERIFIED",
    "REFUSAL_SHIP_FAILED",
    "REFUSAL_SHIP_STATE_UNREADABLE",
    "REFUSAL_SHIP_UNVERIFIED",
    "REFUSAL_STALE_APPROVAL_VERSION",
    "REFUSAL_STATIC_DIR_ABSENT",
    "SERVED_CSS_PATH",
    "SHIP_STATE_FILE",
    "STATE_NOT_DECLARED",
    "STATE_RECONVERGED",
    "STATE_REFUSED",
    "STATE_RETRACTED",
    "STATE_SHIPPED",
    "STATE_VERIFIED",
    "STATIC_DIR",
    "SURFACE_CSS_BUNDLE",
    "ChatCssBundleError",
    "CssBundleOutcome",
    "ShipRecord",
    "forget_ship_record",
    "read_ship_record",
    "record_ship",
    "removal_argv",
    "ship_argv",
    "ship_state_path",
    "sync_css_bundle",
]


class ChatCssBundleError(SubstrateError):
    """The byte-delivery surface could not be read or recorded.

    Reserved for the cases where this module cannot even establish what state
    the surface is in — today, an existing ship record that does not parse.
    Contract refusals (a stale approval version, a set ``CUSTOM_NAME``) are
    NOT raised: they are recorded on the outcome with their removal facts, so
    the caller reports what was cleared as well as what was refused.
    """

    code = "chat_css_bundle_refused"


# ---------------------------------------------------------------------------
# Measured vendor constants
# ---------------------------------------------------------------------------

SURFACE_CSS_BUNDLE = "css_bundle"

#: The census GOTCHA, spelled out: this is ``STATIC_DIR``, the directory the
#: application serves. NOT ``/app/build/static``, which is the frontend build
#: output and where a write changes nothing at all.
STATIC_DIR = "/app/backend/open_webui/static"

CSS_BASENAME = "custom.css"

#: Where the bytes land inside the container.
CONTAINER_CSS_PATH = f"{STATIC_DIR}/{CSS_BASENAME}"

#: The readback. Byte identity here is the ONLY checkable claim; the census
#: recorded ``GET /static/custom.css (bytes)`` as the readback for this
#: mechanism precisely because no value-shaped readback exists.
SERVED_CSS_PATH = f"/static/{CSS_BASENAME}"

#: F3 / the census's silent-override hazard. Read from the instance-lifecycle
#: module rather than restated, so the two cannot drift into disagreeing about
#: which variable is dangerous.
CUSTOM_NAME_VAR = CONTAINER_FORBIDDEN_VARS[0]

#: The bytes ride to the container as base64 in an exec argv (there is no
#: upload API and no mount), and an argv is bounded by ``ARG_MAX``. A bundle
#: over this size is REFUSED by name rather than truncated, split, or written
#: through a shell here-document whose quoting nobody can audit. 64 KiB is
#: comfortably under the smallest ``ARG_MAX`` on the platforms this runs on
#: even after base64's 4/3 expansion.
MAX_BUNDLE_BYTES = 64 * 1024

#: Operator state, beside the instance record
#: (``instance.INSTANCE_STATE_FILE``). It is what makes "this deployment put
#: that file there" a RECORD rather than an inference from the bytes.
SHIP_STATE_FILE = "css-bundle.json"


# ---------------------------------------------------------------------------
# Outcome vocabulary
# ---------------------------------------------------------------------------

#: No declaration and no recorded ship. The surface is untouched.
STATE_NOT_DECLARED = "not_declared"
#: Declared, and the instance was ALREADY serving the declared bytes.
STATE_VERIFIED = "verified"
#: Declared, and this run delivered the bytes.
STATE_SHIPPED = "shipped"
#: Declared, the served bytes disagreed, and this run restored byte identity.
STATE_RECONVERGED = "reconverged"
#: Not declared any more, and this run removed the bytes it had recorded.
STATE_RETRACTED = "retracted"
#: A named refusal. Never a quieter success.
STATE_REFUSED = "refused"

REFUSAL_NO_CONTAINER_RUNTIME = "no_container_runtime"
REFUSAL_CONTAINER_ABSENT = "container_absent"
REFUSAL_CUSTOM_NAME_SET = "custom_name_set"
REFUSAL_CUSTOM_NAME_UNDETERMINED = "custom_name_undetermined"
REFUSAL_STALE_APPROVAL_VERSION = "stale_approval_version"
REFUSAL_STATIC_DIR_ABSENT = "static_dir_absent"
REFUSAL_BUNDLE_UNREADABLE = "bundle_unreadable"
REFUSAL_BUNDLE_TOO_LARGE = "bundle_too_large"
REFUSAL_SHIP_FAILED = "ship_failed"
REFUSAL_SHIP_UNVERIFIED = "ship_unverified"
REFUSAL_REMOVAL_FAILED = "removal_failed"
REFUSAL_REMOVAL_UNVERIFIED = "removal_unverified"
REFUSAL_SHIP_STATE_UNREADABLE = "ship_state_unreadable"


def _sha256(payload: bytes | None) -> str | None:
    if payload is None:
        return None
    return hashlib.sha256(payload).hexdigest()


@dataclass
class CssBundleOutcome:
    """What the delivery surface intended, did, and observed.

    Every field is a fact about BYTES: which file, how many, which digest,
    whether the served digest equals the declared one. There is deliberately
    no field naming a styling value, because the instance exposes none to
    read and a field that could only ever be filled by assumption would be
    the vacuous readback this contract exists to avoid.
    """

    surface: str = SURFACE_CSS_BUNDLE
    state: str = STATE_NOT_DECLARED
    ok: bool = True
    container: str | None = None
    container_path: str = CONTAINER_CSS_PATH
    served_path: str = SERVED_CSS_PATH
    declared_ref: str | None = None
    declared_path: str | None = None
    approved_for_version: str | None = None
    running_version: str | None = None
    declared_sha256: str | None = None
    declared_bytes: int | None = None
    #: What the instance was serving BEFORE this run touched anything.
    observed_before_sha256: str | None = None
    observed_before_bytes: int | None = None
    #: What the instance serves after this run. On a refusal that removed the
    #: owned bytes, this is the proof the vendor default is back.
    observed_sha256: str | None = None
    observed_bytes: int | None = None
    #: The digest the ship record carried on entry, or None when this
    #: deployment had no record of shipping anything.
    recorded_ship_sha256: str | None = None
    byte_identical: bool = False
    drift: bool = False
    drift_detail: str | None = None
    delivered: bool = False
    removed: bool = False
    vendor_default_restored: bool = False
    refusal: str | None = None
    refusal_code: str | None = None
    fix_hint: str | None = None
    notes: list[str] = field(default_factory=list)

    def refuse(
        self, code: str, message: str, *, fix_hint: str | None = None,
    ) -> "CssBundleOutcome":
        self.state = STATE_REFUSED
        self.ok = False
        self.refusal_code = code
        self.refusal = message
        self.fix_hint = fix_hint
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "surface": self.surface,
            "state": self.state,
            "ok": self.ok,
            # Stated on every run so no reader has to infer what kind of
            # promise this surface makes.
            "contract": "byte_delivery",
            "claim": (
                "the bytes named by chat.css_bundle.ref are the bytes the "
                "instance serves at " + self.served_path
            ),
            "semantic_readback": (
                "none: this vendor version exposes no surface that reports "
                "what its styling is, so byte identity at "
                f"{self.served_path} is the only checkable claim"
            ),
            "container": self.container,
            "container_path": self.container_path,
            "served_path": self.served_path,
            "declared_ref": self.declared_ref,
            "declared_path": self.declared_path,
            "approved_for_version": self.approved_for_version,
            "running_version": self.running_version,
            "declared_sha256": self.declared_sha256,
            "declared_bytes": self.declared_bytes,
            "observed_before_sha256": self.observed_before_sha256,
            "observed_before_bytes": self.observed_before_bytes,
            "observed_sha256": self.observed_sha256,
            "observed_bytes": self.observed_bytes,
            "recorded_ship_sha256": self.recorded_ship_sha256,
            "byte_identical": self.byte_identical,
            "drift": self.drift,
            "drift_detail": self.drift_detail,
            "delivered": self.delivered,
            "removed": self.removed,
            "vendor_default_restored": self.vendor_default_restored,
            "refusal": self.refusal,
            "refusal_code": self.refusal_code,
            "fix_hint": self.fix_hint,
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# The ship record: operator state, not an inference from the bytes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ShipRecord:
    """What this deployment last delivered, and where.

    The retraction arm turns on this record and nothing else. Deciding
    ownership from the BYTES instead would be unsound in both directions: a
    file this deployment shipped and somebody then edited is still ours to
    remove, and a file that merely happens to match is not.
    """

    deployment: str
    container: str
    container_path: str
    served_path: str
    ref: str
    approved_for_version: str
    sha256: str
    size: int
    shipped_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "container": self.container,
            "container_path": self.container_path,
            "served_path": self.served_path,
            "ref": self.ref,
            "approved_for_version": self.approved_for_version,
            "sha256": self.sha256,
            "size": self.size,
            "shipped_at": self.shipped_at,
        }


def _state_root(state_root: Path | str | None) -> Path:
    if state_root is not None:
        return Path(state_root)
    from okg.substrate.deployments import user_state_chat_instances_root

    return user_state_chat_instances_root()


def ship_state_path(
    deployment: str, *, state_root: Path | str | None = None,
) -> Path:
    return _state_root(state_root) / deployment / SHIP_STATE_FILE


def read_ship_record(
    deployment: str, *, state_root: Path | str | None = None,
) -> ShipRecord | None:
    """The recorded ship, ``None`` when there is none, else a refusal.

    ``None`` means one thing only: no state file. A file that exists and does
    not parse REFUSES, for the same reason
    :func:`okg.substrate.chat.instance.read_instance` does: read as "this
    deployment never shipped anything", an unreadable record turns a
    retraction into a no-op and leaves the shipped bytes on the instance
    forever.
    """
    path = ship_state_path(deployment, state_root=state_root)
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ChatCssBundleError(
            f"deployment {deployment!r}'s recorded css ship state is "
            f"unreadable: {type(exc).__name__}: {exc}",
            data={
                "reason": REFUSAL_SHIP_STATE_UNREADABLE,
                "deployment": deployment,
                "path": str(path),
            },
            fix_hint=(
                f"repair or delete {path}. It is NOT read as 'this deployment "
                "never shipped a bundle': that reading turns a retraction into "
                "a no-op and leaves the delivered bytes on the instance"
            ),
        ) from exc
    if not isinstance(raw, Mapping):
        raise ChatCssBundleError(
            f"deployment {deployment!r}'s recorded css ship state is a "
            f"{type(raw).__name__}, not an object",
            data={
                "reason": REFUSAL_SHIP_STATE_UNREADABLE,
                "deployment": deployment,
                "path": str(path),
            },
            fix_hint=f"repair or delete {path}",
        )
    missing = [
        key
        for key in (
            "container", "container_path", "served_path", "ref",
            "approved_for_version", "sha256",
        )
        if not isinstance(raw.get(key), str) or not raw[key].strip()
    ]
    if missing:
        raise ChatCssBundleError(
            f"deployment {deployment!r}'s recorded css ship state is missing "
            f"{sorted(missing)}",
            data={
                "reason": REFUSAL_SHIP_STATE_UNREADABLE,
                "deployment": deployment,
                "path": str(path),
                "missing": sorted(missing),
            },
            fix_hint=f"repair or delete {path}",
        )
    return ShipRecord(
        deployment=str(raw.get("deployment") or deployment),
        container=str(raw["container"]),
        container_path=str(raw["container_path"]),
        served_path=str(raw["served_path"]),
        ref=str(raw["ref"]),
        approved_for_version=str(raw["approved_for_version"]),
        sha256=str(raw["sha256"]),
        size=int(raw.get("size") or 0),
        shipped_at=str(raw.get("shipped_at") or ""),
    )


def record_ship(
    record: ShipRecord, *, state_root: Path | str | None = None,
) -> Path:
    path = ship_state_path(record.deployment, state_root=state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(record.as_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def forget_ship_record(
    deployment: str, *, state_root: Path | str | None = None,
) -> None:
    ship_state_path(deployment, state_root=state_root).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# The two container commands
# ---------------------------------------------------------------------------

def ship_argv(payload: bytes) -> list[str]:
    """The argv that writes ``payload`` to :data:`CONTAINER_CSS_PATH`.

    The base64 text and the destination are passed as POSITIONAL ARGUMENTS to
    ``sh``, never interpolated into the script text. That is what makes the
    command structurally incapable of carrying a shell metacharacter out of
    the file's contents: ``$1`` and ``$2`` are values to ``sh``, not source.
    """
    encoded = base64.b64encode(payload).decode("ascii")
    return [
        "sh",
        "-c",
        'printf %s "$1" | base64 -d > "$2"',
        "sh",
        encoded,
        CONTAINER_CSS_PATH,
    ]


def removal_argv() -> list[str]:
    """The argv that removes the delivered file.

    The vendor default for this path is that no file exists: the served
    ``index.html`` links it unconditionally and the image ships nothing there,
    so removing the delivered bytes IS restoring the default.
    """
    return ["rm", "-f", CONTAINER_CSS_PATH]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _vendor_default_served(served: bytes | None) -> bool:
    """True when the instance serves no delivered bytes at the css path.

    Both observed shapes count: the census measured the unconditional link
    being served with an empty body when no file exists, and an absent file
    answers 404. Neither is a delivered stylesheet, and treating one of them
    as "still styled" would make every removal look unverified.
    """
    return served is None or served == b""


# ---------------------------------------------------------------------------
# The verb
# ---------------------------------------------------------------------------

def sync_css_bundle(
    *,
    deployment: str,
    deployment_dir: Path,
    css_bundle: Any,
    running_version: str,
    container: str,
    runtime: Any,
    read_served: Callable[[], bytes | None],
    state_root: Path | str | None = None,
    now: Callable[[], str] | None = None,
) -> CssBundleOutcome:
    """Deliver, verify, reconverge, or retract the deployment's css bytes.

    ``css_bundle`` is the parsed
    :class:`~okg.substrate.deployments.chat_block.ChatCssBundle`, or ``None``
    when the manifest declares none. ``read_served`` returns the bytes the
    instance currently serves at :data:`SERVED_CSS_PATH`, or ``None``.
    ``runtime`` is a
    :class:`~okg.substrate.chat.instance.ContainerRuntime`; it is the only way
    into the static directory, because the vendor exposes no upload API and
    the launch spec mounts nothing over it.

    Returns an outcome. Contract refusals do not raise, because a refusal that
    also REMOVED bytes has to report both facts and an exception carrying only
    a message would report one.
    """
    stamp = now or _now
    outcome = CssBundleOutcome(
        container=container, running_version=running_version,
    )
    record = read_ship_record(deployment, state_root=state_root)
    outcome.recorded_ship_sha256 = record.sha256 if record else None
    declared = css_bundle is not None
    if declared:
        outcome.declared_ref = css_bundle.ref
        outcome.declared_path = str(deployment_dir / css_bundle.ref)
        outcome.approved_for_version = css_bundle.approved_for_version

    # -- never declared: the one state that leaves the surface alone --------
    if not declared and record is None:
        outcome.state = STATE_NOT_DECLARED
        outcome.notes.append(
            "the deployment declares no css bundle and this deployment has no "
            f"recorded delivery, so {SERVED_CSS_PATH} was neither read nor "
            "written: a deployment that never declared a bundle leaves the "
            "instance's static file surface untouched"
        )
        return outcome

    # Everything below WILL touch the instance, so every precondition is
    # checked before anything is written or removed.
    if runtime is None:
        return outcome.refuse(
            REFUSAL_NO_CONTAINER_RUNTIME,
            (
                "the css bundle surface needs a container runtime and none was "
                "supplied. The vendor exposes no upload API for the static "
                f"directory ({STATIC_DIR}) and the launch spec mounts nothing "
                "over it, so bytes reach it only through the runtime. Refused "
                "rather than reported as a surface with nothing to do."
            ),
            fix_hint=(
                "run this through `okg chat sync`, which supplies the runtime, "
                "or drop chat.css_bundle from the manifest"
            ),
        )
    if runtime.inspect(container) is None:
        return outcome.refuse(
            REFUSAL_CONTAINER_ABSENT,
            (
                f"the container {container!r} does not exist, so the static "
                "file surface cannot be read or written. An absent container "
                "is not an instance with nothing delivered to it."
            ),
            fix_hint=f"run `okg chat-instance up --deployment {deployment}`",
        )

    refusal = _refuse_on_custom_name(outcome, runtime, container)
    if refusal is not None:
        return refusal

    # -- declared then retracted -------------------------------------------
    if not declared:
        return _retract(
            outcome,
            deployment=deployment,
            runtime=runtime,
            container=container,
            read_served=read_served,
            state_root=state_root,
        )

    # -- the pin gate -------------------------------------------------------
    if css_bundle.approved_for_version != running_version:
        return _refuse_stale_approval(
            outcome,
            deployment=deployment,
            css_bundle=css_bundle,
            running_version=running_version,
            record=record,
            runtime=runtime,
            container=container,
            read_served=read_served,
            state_root=state_root,
        )

    code, _out, err = runtime.execute(container, ["test", "-d", STATIC_DIR])
    if code != 0:
        return outcome.refuse(
            REFUSAL_STATIC_DIR_ABSENT,
            (
                f"the instance has no {STATIC_DIR} directory (exit {code}: "
                f"{err.strip()[:200]}). That directory is the one the "
                "application serves; the frontend build's build/static is a "
                "different directory where a write changes nothing."
            ),
            fix_hint=(
                "check that the container is the pinned Open WebUI image"
            ),
        )

    declared_path = deployment_dir / css_bundle.ref
    try:
        payload = declared_path.read_bytes()
    except OSError as exc:
        return outcome.refuse(
            REFUSAL_BUNDLE_UNREADABLE,
            (
                f"chat.css_bundle.ref points at {declared_path}, which cannot "
                f"be read ({exc}). A declared file that is missing is a broken "
                "declaration, not an absent one."
            ),
            fix_hint="add the file, or remove chat.css_bundle from the manifest",
        )
    outcome.declared_bytes = len(payload)
    outcome.declared_sha256 = _sha256(payload)
    if len(payload) > MAX_BUNDLE_BYTES:
        return outcome.refuse(
            REFUSAL_BUNDLE_TOO_LARGE,
            (
                f"{declared_path} is {len(payload)} bytes, over the "
                f"{MAX_BUNDLE_BYTES}-byte limit this delivery mechanism can "
                "carry. The bytes travel base64-encoded in a container exec "
                "argv, which the kernel bounds; a larger file is refused "
                "rather than truncated into a stylesheet that parses to "
                "something nobody wrote."
            ),
            fix_hint=f"reduce {declared_path} below {MAX_BUNDLE_BYTES} bytes",
        )

    before = read_served()
    outcome.observed_before_sha256 = _sha256(before)
    outcome.observed_before_bytes = None if before is None else len(before)

    if before == payload:
        outcome.state = STATE_VERIFIED
        outcome.byte_identical = True
        outcome.observed_sha256 = outcome.observed_before_sha256
        outcome.observed_bytes = outcome.observed_before_bytes
        outcome.notes.append(
            f"the instance was already serving the declared bytes at "
            f"{SERVED_CSS_PATH}; nothing was written"
        )
        _record(
            outcome,
            deployment=deployment,
            container=container,
            css_bundle=css_bundle,
            payload=payload,
            state_root=state_root,
            stamp=stamp,
        )
        return outcome

    if record is not None:
        outcome.drift = True
        outcome.drift_detail = (
            f"this deployment delivered {record.sha256} to "
            f"{record.container_path}, and the instance is serving "
            f"{outcome.observed_before_sha256 or 'nothing'}"
        )
    elif before is not None and before != b"":
        # Not our drift — we have no record of delivering anything here — but
        # it is still a disagreement, and overwriting it silently would hide
        # that somebody put a file at this path outside this contract.
        outcome.drift = True
        outcome.drift_detail = (
            f"the instance was serving {outcome.observed_before_sha256} at "
            f"{SERVED_CSS_PATH} with no recorded delivery from this "
            "deployment; the declared bytes replace it"
        )

    code, _out, err = runtime.execute(container, ship_argv(payload))
    if code != 0:
        return outcome.refuse(
            REFUSAL_SHIP_FAILED,
            (
                f"writing {len(payload)} byte(s) to {CONTAINER_CSS_PATH} "
                f"exited {code}: {err.strip()[:400]}"
            ),
            fix_hint="check the container's filesystem permissions",
        )
    outcome.delivered = True

    after = read_served()
    outcome.observed_sha256 = _sha256(after)
    outcome.observed_bytes = None if after is None else len(after)
    outcome.byte_identical = after == payload
    if not outcome.byte_identical:
        return outcome.refuse(
            REFUSAL_SHIP_UNVERIFIED,
            (
                f"the write to {CONTAINER_CSS_PATH} reported success, but "
                f"{SERVED_CSS_PATH} serves "
                f"{outcome.observed_sha256 or 'nothing'} rather than the "
                f"declared {outcome.declared_sha256}. A write that cannot be "
                "read back is never counted as delivered."
            ),
            fix_hint=(
                f"confirm the instance serves {STATIC_DIR} (the frontend "
                "build's build/static is a different directory where a write "
                "changes nothing)"
            ),
        )
    outcome.state = STATE_RECONVERGED if outcome.drift else STATE_SHIPPED
    _record(
        outcome,
        deployment=deployment,
        container=container,
        css_bundle=css_bundle,
        payload=payload,
        state_root=state_root,
        stamp=stamp,
    )
    return outcome


def _record(
    outcome: CssBundleOutcome,
    *,
    deployment: str,
    container: str,
    css_bundle: Any,
    payload: bytes,
    state_root: Path | str | None,
    stamp: Callable[[], str],
) -> None:
    record_ship(
        ShipRecord(
            deployment=deployment,
            container=container,
            container_path=CONTAINER_CSS_PATH,
            served_path=SERVED_CSS_PATH,
            ref=css_bundle.ref,
            approved_for_version=css_bundle.approved_for_version,
            sha256=_sha256(payload) or "",
            size=len(payload),
            shipped_at=stamp(),
        ),
        state_root=state_root,
    )
    outcome.notes.append(
        "recorded this delivery in operator state, so a later run that finds "
        "no declaration can tell a retraction from a deployment that never "
        "declared a bundle"
    )


def _refuse_on_custom_name(
    outcome: CssBundleOutcome, runtime: Any, container: str,
) -> CssBundleOutcome | None:
    """Refuse while a third party owns the directory we deliver into.

    ``printenv NAME`` exits 0 when the variable EXISTS and 1 when it does
    not. Any other exit, or a stderr on the exit-1 arm, is "I could not find
    out" — which is refused rather than read as "it is unset", because the
    unset reading is the one that lets the delivery race a vendor round trip.
    """
    code, out, err = runtime.execute(container, ["printenv", CUSTOM_NAME_VAR])
    if code == 0:
        value = out.strip()
        if value:
            return outcome.refuse(
                REFUSAL_CUSTOM_NAME_SET,
                (
                    f"the instance has {CUSTOM_NAME_VAR} set non-empty. At "
                    "this vendor version a non-empty value makes the process "
                    "fetch api.openwebui.com at import time and OVERWRITE the "
                    f"static branding files under {STATIC_DIR} from a third "
                    "party's response (config.py:184-211). Delivering bytes "
                    "into a directory another party writes would report a "
                    "delivery that the next restart silently replaces, so it "
                    "is refused. Nothing was written and nothing was removed."
                ),
                fix_hint=(
                    f"unset {CUSTOM_NAME_VAR} on the instance and restart it, "
                    "then run this again"
                ),
            )
        outcome.notes.append(
            f"{CUSTOM_NAME_VAR} is present but empty on the instance, which "
            "is the inert case: the vendor's branding fetch is skipped"
        )
        return None
    if code == 1 and not err.strip():
        return None
    return outcome.refuse(
        REFUSAL_CUSTOM_NAME_UNDETERMINED,
        (
            f"could not determine whether the instance has {CUSTOM_NAME_VAR} "
            f"set (exit {code}: {err.strip()[:200]}). An answer this run could "
            "not read is not the same as the variable being unset, and only "
            "the unset reading would let the delivery proceed."
        ),
        fix_hint=(
            f"check that the container is running and that `printenv "
            f"{CUSTOM_NAME_VAR}` can be run in it"
        ),
    )


def _remove_owned_bytes(
    outcome: CssBundleOutcome,
    *,
    deployment: str,
    runtime: Any,
    container: str,
    read_served: Callable[[], bytes | None],
    state_root: Path | str | None,
) -> str | None:
    """Remove the delivered file and PROVE the vendor default is back.

    Returns a refusal code, or ``None`` when the removal is proved. Proof is
    two independent witnesses: the container's own filesystem says the file is
    gone, and the served readback carries no bytes. One alone is not enough —
    a readback that could not reach the instance also returns nothing, which
    would read as a removal that never happened.
    """
    before = read_served()
    outcome.observed_before_sha256 = _sha256(before)
    outcome.observed_before_bytes = None if before is None else len(before)

    code, _out, err = runtime.execute(container, removal_argv())
    if code != 0:
        outcome.refusal = (
            f"removing {CONTAINER_CSS_PATH} exited {code}: "
            f"{err.strip()[:400]}"
        )
        return REFUSAL_REMOVAL_FAILED
    outcome.removed = True

    present, _out, _err = runtime.execute(
        container, ["test", "-e", CONTAINER_CSS_PATH],
    )
    after = read_served()
    outcome.observed_sha256 = _sha256(after)
    outcome.observed_bytes = None if after is None else len(after)
    gone = present != 0
    outcome.vendor_default_restored = gone and _vendor_default_served(after)
    if not outcome.vendor_default_restored:
        outcome.refusal = (
            f"{CONTAINER_CSS_PATH} was removed, but the instance has not "
            f"returned to the vendor default: the file is "
            f"{'gone' if gone else 'STILL PRESENT'} on the container "
            f"filesystem and {outcome.served_path} serves "
            f"{outcome.observed_sha256 or 'nothing'}. Both witnesses are "
            "required, because a readback that could not reach the instance "
            "also returns nothing."
        )
        return REFUSAL_REMOVAL_UNVERIFIED
    forget_ship_record(deployment, state_root=state_root)
    return None


def _retract(
    outcome: CssBundleOutcome,
    *,
    deployment: str,
    runtime: Any,
    container: str,
    read_served: Callable[[], bytes | None],
    state_root: Path | str | None,
) -> CssBundleOutcome:
    """The declaration was removed: take the delivered bytes with it."""
    failure = _remove_owned_bytes(
        outcome,
        deployment=deployment,
        runtime=runtime,
        container=container,
        read_served=read_served,
        state_root=state_root,
    )
    if failure is not None:
        outcome.state = STATE_REFUSED
        outcome.ok = False
        outcome.refusal_code = failure
        outcome.fix_hint = (
            f"remove {CONTAINER_CSS_PATH} by hand and run this again; the ship "
            "record is deliberately kept while the removal is unproved"
        )
        return outcome
    outcome.state = STATE_RETRACTED
    outcome.notes.append(
        "the manifest no longer declares a css bundle and this deployment had "
        f"a recorded delivery of {outcome.recorded_ship_sha256}, so the "
        "delivered file was removed and the vendor default restored. A "
        "retraction is not the same as never having declared one: read that "
        "way, the delivered bytes would have stayed on the instance"
    )
    return outcome


def _refuse_stale_approval(
    outcome: CssBundleOutcome,
    *,
    deployment: str,
    css_bundle: Any,
    running_version: str,
    record: ShipRecord | None,
    runtime: Any,
    container: str,
    read_served: Callable[[], bytes | None],
    state_root: Path | str | None,
) -> CssBundleOutcome:
    """A pin bump invalidates the approval AND clears the served bytes.

    The removal is half the refusal, not a courtesy. The declared bytes
    override vendor build-generated class names that carry no cross-version
    guarantee, so bytes approved against the old pin have no standing on the
    new build; leaving them served while the error is visible only in an
    operator's terminal would style the new build with unapproved bytes for as
    long as nobody reads the run.
    """
    removal_note: str
    if record is None:
        removal_note = (
            "this deployment has no recorded delivery, so there were no owned "
            "bytes to clear"
        )
    else:
        failure = _remove_owned_bytes(
            outcome,
            deployment=deployment,
            runtime=runtime,
            container=container,
            read_served=read_served,
            state_root=state_root,
        )
        if failure is not None:
            outcome.state = STATE_REFUSED
            outcome.ok = False
            outcome.refusal_code = failure
            outcome.fix_hint = (
                f"remove {CONTAINER_CSS_PATH} by hand, then re-approve the "
                "bundle against the running version"
            )
            return outcome
        removal_note = (
            f"the bytes this deployment had delivered ({record.sha256}) were "
            "removed and the vendor default restored for the duration of the "
            "refusal"
        )
    outcome.notes.append(removal_note)
    return outcome.refuse(
        REFUSAL_STALE_APPROVAL_VERSION,
        (
            f"chat.css_bundle.approved_for_version is "
            f"{css_bundle.approved_for_version!r} and the instance is running "
            f"{running_version!r}. The declared bytes override vendor "
            "build-generated class names that carry no compatibility "
            "guarantee across versions, so an approval against the old pin "
            f"says nothing about {running_version!r} and the bytes are not "
            f"carried forward. {removal_note}."
        ),
        fix_hint=(
            f"check {css_bundle.ref} against Open WebUI {running_version} and "
            f"set chat.css_bundle.approved_for_version: {running_version!r} — "
            "editing that field after the re-check IS the re-approval, and it "
            "is versioned in the deployment repo like every other identity "
            "change"
        ),
    )

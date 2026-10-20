"""Upgrade, rollback and retire receipts for the chat layer (slice S5 of #1183).

The chat layer has a lifecycle of its own. Its image can move forward, move
back, and be taken away entirely, and none of those three is a change to the
graph deployment underneath it: the app database is a SEPARATE Postgres
database, proved distinct at stand-up, and the graph keeps serving whether or
not a site exists in front of it. What was missing was any way to write down
that a version change happened, from what to what, and that the thing which
must NOT change did not.

**Nothing here starts or stops a container.** Not the upgrade, not the
rollback, not the retirement. Pulling an image and restarting a container is
the operator's ``docker``/compose step (or ``okg chat-instance up`` /
``okg chat-instance down``), and this module deliberately does not reach for
it: a verb that both performed a change and wrote the evidence for it would be
attesting to its own work. What these receipts do instead is bound that step
from both sides — an upgrade receipt is written from a RECONCILED baseline,
naming the command the operator is expected to run, and a retire receipt is
written only once a live read shows the OKG-owned surfaces are actually gone.
The three documents are inert JSON: no database is opened, no container is
inspected, and the only bytes written are one file under an output directory
that must not already exist.

Three kinds, one document family
--------------------------------

``upgrade``
    Baseline image and the version it is tested at, candidate image and its
    version, the app-database identity shown unchanged, the chat plan's digest
    and the S2 readback that proves the baseline was reconciled when the
    upgrade was recorded. It refuses a candidate outside the tested set
    (:data:`~okg.substrate.chat.instance.TESTED_IMAGES`), an app database whose
    identity would move, a candidate identical to the baseline, and — the arm
    the requirement exists for — a baseline readback carrying deviations. An
    upgrade from an unreconciled instance cannot be reasoned about afterwards:
    every deviation found later is ambiguous between "the upgrade did it" and
    "it was already like that".

``rollback``
    Built FROM an upgrade receipt rather than from two operator-supplied
    image names, so a rollback can only be recorded against an upgrade that
    was recorded, and the pair reads as one story. It refuses when the
    baseline image is no longer in the tested set: an allow-list is a live
    statement about what has been measured, and a version retired from it is
    not a place to go back to.

``retire``
    The chat layer removed while the graph deployment stays up. It is the one
    kind that READS the instance, because its claim is an absence and an
    absence has to be observed: the preset, the MCP connection, its write
    sibling and the deployment's prompt must all be gone. Any one still
    present refuses. An instance that cannot be reached refuses too, and
    separately — "I could not ask" must never be recorded as "it is not
    there", which is the exact shape of failure this arm exists to prevent.

    What it does NOT claim, and a reader must not read into it: that the
    container is gone (it names the ``okg chat-instance down`` an operator is
    expected to run, and stops nothing itself); that the app database was
    dropped (it is deliberately kept -- it holds the chat history); or
    anything about the deep-link valve, which lives on a component installed
    separately from this deployment and is not a surface this deployment owns.

Determinism and secrets
-----------------------
Every receipt is digested over its own body with ``receipt_digest`` removed,
and carries no timestamp and no absolute path, so two runs over identical
inputs write identical bytes — asserted, not asserted about. The only
database identity that appears is a :class:`~okg.substrate.chat.instance.
PgTarget` (host, port, database name), which is what the app-database DSN
resolves TO and never the DSN itself; the connection credential is never read
here at all, and the instance is observed through S1's redacted accessor.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from okg.substrate.chat.instance import (
    ChatInstanceError,
    PgTarget,
    tested_image_version,
)
from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "CHAT_LIFECYCLE_SCHEMA",
    "KIND_RETIRE",
    "KIND_ROLLBACK",
    "KIND_UPGRADE",
    "RECEIPT_FILENAME",
    "ChatImageSideNotTested",
    "ChatLifecycleAppDatabaseChanged",
    "ChatLifecycleBaselineUnreconciled",
    "ChatLifecycleError",
    "ChatLifecycleOutputPathExists",
    "ChatLifecycleSurfaceRemains",
    "load_lifecycle_receipt",
    "observe_okg_surfaces",
    "receipt_digest",
    "retire_receipt",
    "rollback_receipt",
    "upgrade_receipt",
    "write_lifecycle_receipt",
]

CHAT_LIFECYCLE_SCHEMA = "okg.deployment_product.chat_lifecycle_receipt/v1"
RECEIPT_FILENAME = "chat-lifecycle-receipt.json"

KIND_UPGRADE = "upgrade"
KIND_ROLLBACK = "rollback"
KIND_RETIRE = "retire"


class ChatLifecycleError(SubstrateError):
    """A chat lifecycle receipt refused. Nothing was written, and nothing ran."""

    code = "chat_lifecycle_refused"


class ChatLifecycleOutputPathExists(ChatLifecycleError):
    """The named output directory already exists."""

    code = "chat_lifecycle_output_path_exists"


class ChatLifecycleAppDatabaseChanged(ChatLifecycleError):
    """The app database would not be the same one after the change."""

    code = "chat_lifecycle_app_database_changed"


class ChatLifecycleBaselineUnreconciled(ChatLifecycleError):
    """The baseline instance does not match its plan, so it is no baseline."""

    code = "chat_lifecycle_baseline_unreconciled"


class ChatLifecycleSurfaceRemains(ChatLifecycleError):
    """A surface this deployment owns is still configured on the instance."""

    code = "chat_lifecycle_surface_remains"


def receipt_digest(document: Mapping[str, Any]) -> str:
    """The digest over the receipt body, with ``receipt_digest`` removed."""
    body = {k: v for k, v in document.items() if k != "receipt_digest"}
    return sha256_digest(canonical_json_bytes(body))


def _sealed(document: dict[str, Any]) -> dict[str, Any]:
    document["receipt_digest"] = receipt_digest(document)
    return document


#: The three fields that ARE the identity of an app database here.
_TARGET_KEYS = ("host", "port", "database")


def _target(value: Any) -> dict[str, Any]:
    """One app-database identity, as host/port/database and nothing else.

    Both supported representations must carry nonblank host and database
    strings and a real TCP port. Equal incomplete values cannot establish
    unchanged identity. This checks structure, not database reachability.
    """
    if isinstance(value, PgTarget):
        value = value.as_dict()
    if isinstance(value, Mapping) and all(k in value for k in _TARGET_KEYS):
        target = {k: value[k] for k in _TARGET_KEYS}
        if (
            all(
                isinstance(target[key], str) and bool(target[key].strip())
                for key in ("host", "database")
            )
            and isinstance(target["port"], int)
            and not isinstance(target["port"], bool)
            and 1 <= target["port"] <= 65535
        ):
            return target
    raise ChatLifecycleError(
        "an app-database identity was not supplied as a usable target "
        f"({', '.join(_TARGET_KEYS)})",
        data={"reason": "app_database_identity_missing"},
        fix_hint="pass the PgTarget the deployment's app DSN resolves to",
    )


def _require_same_app_database(
    deployment: str,
    kind: str,
    baseline: Any,
    current: Any,
) -> dict[str, Any]:
    """The app database must be the SAME database on both sides.

    Compared as resolved targets, never as DSN strings: two spellings of one
    database are the same database, and the check that matters is whether the
    chat history survives the change, not whether a URL was retyped.
    """
    before, after = _target(baseline), _target(current)
    if before != after:
        raise ChatLifecycleAppDatabaseChanged(
            f"deployment {deployment!r} would move its chat app database from "
            f"{before} to {after} as part of this {kind}. An image change is "
            "not a migration: the vendor tables, and every conversation in "
            "them, live in the database being left behind.",
            data={
                "reason": "app_database_identity_changed",
                "deployment": deployment,
                "kind": kind,
                "baseline": before,
                "candidate": after,
            },
            fix_hint=(
                "point the app-database variable back at the recorded "
                "database, or migrate deliberately as its own change"
            ),
        )
    return before


def _require_reconciled(deployment: str, readback: Mapping[str, Any]) -> dict[str, Any]:
    """The baseline must match its plan before it can be a baseline."""
    if readback.get("schema") != "okg.deployment_product.chat_readback/v1":
        raise ChatLifecycleError(
            "the baseline reading is not a chat readback document",
            data={
                "reason": "readback_schema_unknown",
                "deployment": deployment,
                "schema": readback.get("schema"),
            },
            fix_hint="pass the chat-readback.json `okg chat readback` wrote",
        )
    deviations = list(readback.get("deviations") or [])
    if deviations or not readback.get("ok"):
        raise ChatLifecycleBaselineUnreconciled(
            f"deployment {deployment!r}'s instance deviates from its plan on "
            f"{len(deviations)} key(s), so it is not a baseline. An upgrade "
            "recorded from here could never be reasoned about afterwards: "
            "every deviation found later would be ambiguous between the "
            "upgrade and what was already wrong.",
            data={
                "reason": "baseline_deviates_from_plan",
                "deployment": deployment,
                "deviations": len(deviations),
                "surfaces": sorted({str(d.get("surface")) for d in deviations}),
            },
            fix_hint="`okg chat sync`, re-run `okg chat readback`, then record",
        )
    return {
        "plan_digest": readback.get("plan_digest"),
        "ok": True,
        "openwebui_version": readback.get("openwebui_version"),
        "surfaces": {
            name: entry.get("status")
            for name, entry in sorted((readback.get("surfaces") or {}).items())
            if isinstance(entry, Mapping)
        },
    }


class ChatImageSideNotTested(ChatLifecycleError):
    """An image named by a receipt is not one this program has measured.

    Carries the same code the instance layer raises for the same fact, because
    an operator meeting it at stand-up and an operator meeting it at an upgrade
    have the same problem and the same fix; ``role`` says which side of the
    change was the untested one.
    """

    code = "chat_image_not_tested"


def _image_side(
    image: str, *, role: str, tested_images: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """One side of an image change, refusing an image outside the tested set."""
    try:
        version = tested_image_version(image, tested_images=tested_images)
    except ChatInstanceError as exc:
        raise ChatImageSideNotTested(
            f"the {role} image {image!r} is not in the tested set "
            f"{exc.data.get('tested')}",
            data={**dict(exc.data), "role": role},
            fix_hint=exc.fix_hint,
        ) from exc
    return {"image": image, "version": version, "tested": True}


def _container_step(deployment: str, verb: str) -> dict[str, Any]:
    """The step this receipt does NOT perform, named so nobody assumes it did."""
    return {
        "performed_by_this_verb": False,
        "expected_operator_command": verb,
        "note": (
            "this receipt neither started nor stopped a container; it records "
            "the state read through the admin API on either side of the "
            f"operator's own {verb}"
        ),
    }


def upgrade_receipt(
    deployment: str,
    *,
    baseline_image: str,
    candidate_image: str,
    baseline_app_database: Any,
    candidate_app_database: Any,
    plan_digest: str,
    readback: Mapping[str, Any],
    tested_images: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """Record a chat-image upgrade, from a reconciled baseline.

    Starts nothing and stops nothing. The candidate must be in the tested set,
    the app database must be the same database on both sides, the two images
    must differ, and the baseline readback must be clean.
    """
    candidate = _image_side(
        candidate_image, role="candidate", tested_images=tested_images,
    )
    if baseline_image == candidate_image:
        raise ChatLifecycleError(
            f"deployment {deployment!r}'s baseline and candidate images are "
            f"both {baseline_image!r}, so there is no upgrade to record.",
            data={
                "reason": "candidate_not_distinct",
                "deployment": deployment,
                "image": baseline_image,
            },
            fix_hint="declare a different chat.image, or record nothing",
        )
    app_database = _require_same_app_database(
        deployment, KIND_UPGRADE, baseline_app_database, candidate_app_database,
    )
    reconciled = _require_reconciled(deployment, readback)
    if reconciled["plan_digest"] != plan_digest:
        raise ChatLifecycleError(
            "the baseline reading was taken against a different plan "
            f"({reconciled['plan_digest']!r}) from the one being recorded "
            f"({plan_digest!r}), so it is evidence about another desired state.",
            data={
                "reason": "readback_plan_mismatch",
                "deployment": deployment,
                "readback_plan_digest": reconciled["plan_digest"],
                "plan_digest": plan_digest,
            },
            fix_hint="re-run `okg chat readback --plan` against this plan",
        )
    # The baseline is recorded WITHOUT requiring it to still be tested: an
    # upgrade away from a version that has been retired from the allow-list is
    # exactly the move the allow-list is meant to encourage, and refusing it
    # would strand the deployment on the image it most needs to leave.
    baseline: dict[str, Any] = {"image": baseline_image}
    try:
        baseline["version"] = tested_image_version(
            baseline_image, tested_images=tested_images,
        )
        baseline["tested"] = True
    except ChatInstanceError:
        baseline["version"] = None
        baseline["tested"] = False
    return _sealed({
        "schema": CHAT_LIFECYCLE_SCHEMA,
        "kind": KIND_UPGRADE,
        "deployment": deployment,
        "baseline": baseline,
        "candidate": candidate,
        "app_database": app_database,
        "app_database_unchanged": True,
        "plan_digest": plan_digest,
        "readback": reconciled,
        "container_operation": _container_step(
            deployment,
            f"okg chat-instance up --deployment {deployment} [--install-plan <dir>]",
        ),
    })


def rollback_receipt(
    deployment: str,
    *,
    upgrade: Mapping[str, Any],
    current_app_database: Any,
    tested_images: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """Record a return to the image an upgrade receipt moved away from.

    Reads the upgrade receipt rather than two operator-supplied names, so a
    rollback exists only for an upgrade that was recorded and the pair reads
    as one story. The baseline must STILL be in the tested set.
    """
    if upgrade.get("schema") != CHAT_LIFECYCLE_SCHEMA or (
        upgrade.get("kind") != KIND_UPGRADE
    ):
        raise ChatLifecycleError(
            "a rollback is recorded against an upgrade receipt; the document "
            f"passed is {upgrade.get('schema')!r}/{upgrade.get('kind')!r}.",
            data={
                "reason": "upgrade_receipt_expected",
                "deployment": deployment,
                "kind": upgrade.get("kind"),
            },
            fix_hint="pass the chat-lifecycle-receipt.json of kind upgrade",
        )
    from_image = str((upgrade.get("candidate") or {}).get("image") or "")
    to_image = str((upgrade.get("baseline") or {}).get("image") or "")
    if not from_image or not to_image:
        raise ChatLifecycleError(
            "the upgrade receipt names no image on one side, so there is "
            "nothing to roll back between.",
            data={"reason": "upgrade_receipt_incomplete", "deployment": deployment},
            fix_hint="re-record the upgrade receipt",
        )
    # The refusal this kind exists for. `tested` on the receipt is what was
    # true when the upgrade was written; the allow-list is asked AGAIN here,
    # because a version retired in between is not a place to go back to.
    baseline = _image_side(to_image, role="baseline", tested_images=tested_images)
    app_database = _require_same_app_database(
        deployment, KIND_ROLLBACK, upgrade.get("app_database"), current_app_database,
    )
    return _sealed({
        "schema": CHAT_LIFECYCLE_SCHEMA,
        "kind": KIND_ROLLBACK,
        "deployment": deployment,
        "baseline": baseline,
        "candidate": {
            "image": from_image,
            "version": (upgrade.get("candidate") or {}).get("version"),
            "tested": None,
        },
        "app_database": app_database,
        "app_database_unchanged": True,
        "plan_digest": upgrade.get("plan_digest"),
        "reverses_upgrade": upgrade.get("receipt_digest"),
        "container_operation": _container_step(
            deployment,
            f"okg chat-instance up --deployment {deployment} [--install-plan <dir>]",
        ),
    })


#: The surfaces `okg chat sync` creates and owns on an instance. A retirement
#: is the claim that every one of them is gone, so the list is enumerated here
#: rather than derived from whatever a readback happened to compare.
_OWNED_SURFACES = ("preset", "connection", "write_connection", "prompt")


def observe_okg_surfaces(
    deployment: str, client: Any, *, preset_id: str, mcp_server_id: str,
) -> dict[str, bool]:
    """Read the instance for each OKG-owned surface. True means STILL PRESENT.

    Reads only; every call is a GET. An instance that cannot be reached raises
    rather than answering "absent", because an unreachable instance is the one
    thing that must never be mistaken for a retired one.
    """
    from okg.substrate.chat.sync import WRITE_CONNECTION_SUFFIX

    try:
        registered = {
            str((entry.get("info") or {}).get("id"))
            for entry in client.tool_servers()
        }
        commands = {str(row.get("command")) for row in client.prompts()}
        preset_present = client.model_row(preset_id) is not None
    except SubstrateError as exc:
        raise ChatLifecycleError(
            f"deployment {deployment!r}'s instance could not be read "
            f"({exc.code}), so this run cannot say its surfaces are gone. An "
            "unanswered instance is an unmeasured one, never a retired one.",
            data={
                "reason": "instance_unreadable",
                "deployment": deployment,
                "underlying_code": exc.code,
            },
            fix_hint=(
                "reach the instance and re-run; retire the layer only once a "
                "live read shows the surfaces absent"
            ),
        ) from exc
    return {
        "preset": preset_present,
        "connection": mcp_server_id in registered,
        "write_connection": (
            f"{mcp_server_id}{WRITE_CONNECTION_SUFFIX}" in registered
        ),
        "prompt": f"/okg-{deployment}" in commands,
    }


def retire_receipt(
    deployment: str,
    client: Any,
    *,
    preset_id: str,
    mcp_server_id: str,
    app_database: Any,
) -> dict[str, Any]:
    """Record that the chat layer is retired, the graph deployment untouched.

    Neither stops a container nor drops a database. What it claims is narrower
    and checkable: the four surfaces ``okg chat sync`` owns are absent from the
    instance right now. Any one still present refuses, naming it.
    """
    present = observe_okg_surfaces(
        deployment, client, preset_id=preset_id, mcp_server_id=mcp_server_id,
    )
    remaining = sorted(name for name in _OWNED_SURFACES if present.get(name))
    if remaining:
        raise ChatLifecycleSurfaceRemains(
            f"deployment {deployment!r} still configures {remaining} on the "
            "instance, so the chat layer is not retired. A receipt written "
            "now would say the surfaces are gone while a reader could still "
            "select the preset and call the graph.",
            data={
                "reason": "owned_surface_present",
                "deployment": deployment,
                "remaining": remaining,
            },
            fix_hint=(
                "remove the preset, connections and prompt through the same "
                "admin API `okg chat sync` writes them with, then re-run"
            ),
        )
    return _sealed({
        "schema": CHAT_LIFECYCLE_SCHEMA,
        "kind": KIND_RETIRE,
        "deployment": deployment,
        # Derived from the reading above, not restated: a constant list here
        # would assert the absence rather than report the one measured.
        "surfaces_absent": [n for n in _OWNED_SURFACES if not present[n]],
        "preset_id": preset_id,
        "mcp_server_id": mcp_server_id,
        "app_database": _target(app_database),
        # Stated, and true by construction: this module imports no graph
        # accessor and opens no connection. The app database above is the
        # chat layer's own, proved distinct from the graph's at stand-up.
        "graph_deployment": {
            "touched": False,
            "note": (
                "retiring the chat layer changes nothing in the graph "
                "deployment: the app database is a separate database and no "
                "graph connection is opened by this verb"
            ),
        },
        "container_operation": _container_step(
            deployment, f"okg chat-instance down --deployment {deployment}",
        ),
    })


def write_lifecycle_receipt(
    document: Mapping[str, Any], output_dir: Path | str,
) -> dict[str, Any]:
    """Write the receipt under ``output_dir``, which must NOT already exist.

    Stricter than the plan's rule, and deliberately: a receipt is the record
    of a change that happened once, and writing a second one into a directory
    an earlier run wrote would leave two documents whose order nothing states.
    """
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ChatLifecycleOutputPathExists(
            f"{output_dir} already exists. A lifecycle receipt is written into "
            "a fresh directory so an earlier record cannot be joined, "
            "shadowed or overwritten by a later one.",
            data={"reason": "output_path_exists", "path": str(output_dir)},
            fix_hint="name a directory that does not exist yet",
        )
    output_dir.mkdir(parents=True)
    target = output_dir / RECEIPT_FILENAME
    target.write_bytes(canonical_json_bytes(document) + b"\n")
    return {
        "receipt_path": str(target),
        "kind": document.get("kind"),
        "receipt_digest": document.get("receipt_digest"),
    }


def load_lifecycle_receipt(path: Path | str) -> dict[str, Any]:
    """Read a receipt and check it digests to its own recorded value."""
    path = Path(path)
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ChatLifecycleError(
            f"the receipt at {path} could not be read ({exc}).",
            data={"reason": "receipt_unreadable", "path": str(path)},
            fix_hint="pass the chat-lifecycle-receipt.json a receipt verb wrote",
        ) from exc
    if not isinstance(loaded, Mapping) or loaded.get("schema") != CHAT_LIFECYCLE_SCHEMA:
        raise ChatLifecycleError(
            f"the document at {path} is not a {CHAT_LIFECYCLE_SCHEMA}.",
            data={"reason": "receipt_schema_unknown", "path": str(path)},
            fix_hint="pass the chat-lifecycle-receipt.json a receipt verb wrote",
        )
    receipt = dict(loaded)
    recorded, computed = receipt.get("receipt_digest"), receipt_digest(receipt)
    if recorded != computed:
        raise ChatLifecycleError(
            f"the receipt at {path} does not digest to its recorded value: "
            f"recorded {recorded!r}, computed {computed!r}. It was edited "
            "after it was written.",
            data={
                "reason": "receipt_digest_self_mismatch",
                "path": str(path),
                "recorded": recorded,
                "computed": computed,
            },
            fix_hint="re-record the receipt rather than editing it",
        )
    return receipt

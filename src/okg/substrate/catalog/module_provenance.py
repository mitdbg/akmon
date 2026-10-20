"""Provider provenance for the modules a release was sealed from.

Slice 6 of #1179. Slices 1-5 made catalog composition resolve every
``modules:`` entry through an owner-keyed provider, gave the package
manifest an OKG version window and ``module_declarations``, made a
verified package serve those modules, and joined them into a plan and a
lock. What a release records was still only the catalog fingerprint,
which says what the composed catalog *is* and nothing about where its
modules *came from*: two different packages can compose to one
fingerprint, so a release sealed from one and activated against the
other would read as unchanged.

This module is the record and its three decisions.

**Only a non-built-in module is recorded.** The ``okg`` provider is the
checkout the release already pins by ``code_revision``, so recording it
would add a byte to every release without adding a fact. A release built
from built-ins only therefore carries an empty provenance, and its
``input_id`` is byte-identical to what it was before this existed.

**The record is what the provider resolved to**, not what a manifest
asked for: owner, provider kind, the package release digest the provider
was built from, and the object digests behind that module's files. It
sits inside :class:`SealedReleaseInputs`, so the release digest covers
it.

**Activation re-resolves and compares; it never re-records.** A release
whose recorded provenance disagrees with what the providers now supply
refuses with :class:`ReleaseModuleProvenanceMismatch`, naming the module
and both digests. Silently adopting the newly resolved digests would
make the recorded one decorative.

**An upgrade reports what changed and refuses a silent drop.** Carrying
provenance from the old release to the new gives added, removed, and
digest-changed modules; an upgrade that would leave a packaged owner
supplying nothing refuses rather than quietly falling back to whatever
the built-in library happens to hold.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from okg.substrate.catalog.module_providers import BUILTIN_OWNER
from okg.substrate.errors import SubstrateError


__all__ = [
    "BUILTIN_KIND",
    "MODULE_PROVENANCE_SCHEMA",
    "ReleaseModuleProvenanceMismatch",
    "UpgradeModuleProviderDropped",
    "module_provenance_entries",
    "module_provenance_from_composed",
    "module_provider_changes",
    "refuse_dropped_module_owner",
    "refuse_release_provenance_mismatch",
]


MODULE_PROVENANCE_SCHEMA = "okg.release.module_provenance/v1"
BUILTIN_KIND = "builtin"


class ReleaseModuleProvenanceMismatch(SubstrateError):
    """A sealed release's recorded module provenance no longer resolves.

    Raised before activation binds anything. The payload names the module
    and both sides' digests, so an operator reads which package the
    release was sealed from and which one is installed now.
    """

    code = "release_module_provenance_mismatch"


class UpgradeModuleProviderDropped(SubstrateError):
    """An upgrade would leave a packaged module's owner supplying nothing.

    Refused rather than reported, because the alternative is a deployment
    that composes a smaller catalog from the built-in library and calls
    the result an upgrade.
    """

    code = "upgrade_module_provider_owner_dropped"


def _entry(module_ref: str, origin: Mapping[str, Any]) -> dict[str, Any]:
    owner = str(origin["owner"])
    module_id = module_ref if ":" in module_ref else f"{owner}:{module_ref}"
    return {
        "module_id": module_id,
        "owner": owner,
        "provider_kind": str(origin["provider_kind"]),
        "package_release_digest": origin.get("package_release_digest"),
        "object_digests": [str(item) for item in origin.get("object_digests") or ()],
    }


def module_provenance_from_composed(composed: Any) -> tuple[dict[str, Any], ...]:
    """Read the provider provenance off a composed catalog.

    Returns one entry per non-built-in module, ordered by module id so the
    record is a function of the composition and not of dict insertion.

    **Selection is by owner, and the kind must agree with it.** Filtering
    on ``provider_kind != builtin`` alone read an origin that carried no
    kind at all as non-built-in and sealed ``provider_kind: ""`` into the
    release, which activation would then compare against a real kind and
    refuse. The owner is the identity a provider is keyed by, so it
    decides; an origin whose kind is absent, or disagrees with its owner,
    is a malformed composition and refuses here rather than being sealed.
    """
    origins: Mapping[str, Mapping[str, Any]] = getattr(
        composed, "module_origins", {},
    ) or {}
    entries: list[dict[str, Any]] = []
    for ref, origin in origins.items():
        owner = str(origin.get("owner") or "")
        kind = str(origin.get("provider_kind") or "")
        if not owner or not kind:
            raise ReleaseModuleProvenanceMismatch(
                f"module {ref} resolved without an explicit owner and "
                f"provider kind",
                data={"observed": repr({"ref": ref, **dict(origin)})[:200]},
                fix_hint=(
                    "re-compose the catalog; every resolved module records "
                    "the provider that supplied it"
                ),
            )
        if (owner == BUILTIN_OWNER) != (kind == BUILTIN_KIND):
            raise ReleaseModuleProvenanceMismatch(
                f"module {ref} names owner {owner!r} with provider kind "
                f"{kind!r}, which disagree",
                data={"observed": repr({"ref": ref, **dict(origin)})[:200]},
                fix_hint=(
                    f"the {BUILTIN_OWNER!r} owner is the {BUILTIN_KIND!r} "
                    "provider and no other owner may claim that kind"
                ),
            )
        if owner == BUILTIN_OWNER:
            continue
        entries.append(_entry(ref, origin))
    return tuple(sorted(entries, key=lambda item: item["module_id"]))


def module_provenance_entries(
    raw: Any,
) -> tuple[dict[str, Any], ...]:
    """Normalize a provenance list read back from JSON or a mapping."""
    if not raw:
        return ()
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ReleaseModuleProvenanceMismatch(
            "recorded module provenance is not a list of entries",
            data={"observed": repr(raw)[:200]},
            fix_hint="re-seal the release; its metadata is malformed",
        )
    entries: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping) or "module_id" not in item:
            raise ReleaseModuleProvenanceMismatch(
                "recorded module provenance entry names no module",
                data={"observed": repr(item)[:200]},
                fix_hint="re-seal the release; its metadata is malformed",
            )
        entries.append(
            {
                "module_id": str(item["module_id"]),
                "owner": str(item.get("owner") or ""),
                "provider_kind": str(item.get("provider_kind") or ""),
                "package_release_digest": item.get("package_release_digest"),
                "object_digests": [
                    str(each) for each in item.get("object_digests") or ()
                ],
            }
        )
    return tuple(sorted(entries, key=lambda entry: entry["module_id"]))


def _by_id(entries: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(entry["module_id"]): dict(entry) for entry in entries}


def refuse_release_provenance_mismatch(
    recorded: Iterable[Mapping[str, Any]],
    resolved: Iterable[Mapping[str, Any]],
    *,
    command: str,
) -> None:
    """Refuse when what a release recorded is not what now resolves.

    The comparison is over the whole entry -- owner, provider kind,
    release digest and object digests -- so a package rebuilt with the
    same release digest but different bytes is caught too.
    """
    recorded_by = _by_id(recorded)
    resolved_by = _by_id(resolved)
    for module_id in sorted(set(recorded_by) | set(resolved_by)):
        was = recorded_by.get(module_id)
        now = resolved_by.get(module_id)
        if was == now:
            continue
        if was is None:
            reason = "module_not_in_release"
        elif now is None:
            reason = "module_no_longer_supplied"
        else:
            reason = "provider_digest_changed"
        raise ReleaseModuleProvenanceMismatch(
            f"sealed release module {module_id} was sealed from "
            f"{(was or {}).get('package_release_digest')} and now resolves "
            f"to {(now or {}).get('package_release_digest')}",
            data={
                "phase": "pre_activation",
                "command": command,
                "module_id": module_id,
                "reason": reason,
                "recorded_release_digest": (was or {}).get(
                    "package_release_digest"
                ),
                "resolved_release_digest": (now or {}).get(
                    "package_release_digest"
                ),
                "recorded_object_digests": (was or {}).get("object_digests"),
                "resolved_object_digests": (now or {}).get("object_digests"),
            },
            fix_hint=(
                "install the package the release was sealed from, or seal a "
                "new release against the package that is installed now"
            ),
        )


def module_provider_changes(
    previous: Iterable[Mapping[str, Any]],
    target: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Per-module provider changes between two releases' provenance."""
    was = _by_id(previous)
    now = _by_id(target)
    changed = [
        {
            "module_id": module_id,
            "previous_release_digest": was[module_id].get(
                "package_release_digest"
            ),
            "target_release_digest": now[module_id].get("package_release_digest"),
            "previous_object_digests": was[module_id].get("object_digests"),
            "target_object_digests": now[module_id].get("object_digests"),
        }
        for module_id in sorted(set(was) & set(now))
        if was[module_id] != now[module_id]
    ]
    return {
        "schema": MODULE_PROVENANCE_SCHEMA,
        "added": sorted(set(now) - set(was)),
        "removed": sorted(set(was) - set(now)),
        "digest_changed": changed,
    }


def refuse_dropped_module_owner(
    previous: Iterable[Mapping[str, Any]],
    target: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return the change report, refusing an upgrade that drops an owner.

    A module the target no longer names is a removal an operator may have
    intended. An *owner* that supplied modules before and supplies none
    now is not: nothing in the target says who takes its subtypes over.
    """
    was = _by_id(previous)
    now = _by_id(target)
    changes = module_provider_changes(was.values(), now.values())
    dropped = sorted(
        {entry["owner"] for entry in was.values()}
        - {entry["owner"] for entry in now.values()}
    )
    if dropped:
        raise UpgradeModuleProviderDropped(
            f"upgrade would drop packaged module owner(s) "
            f"{', '.join(dropped)} with nothing supplying their modules",
            data={
                "phase": "pre_upgrade",
                "dropped_owners": dropped,
                "dropped_modules": sorted(
                    module_id
                    for module_id, entry in was.items()
                    if entry["owner"] in set(dropped)
                ),
                "changes": changes,
            },
            fix_hint=(
                "install the owner's distribution for the target revision. "
                "Removing its modules from the deployment manifest does not "
                "clear this refusal: the comparison is against what the "
                "previous release recorded, which still names the owner"
            ),
        )
    return changes

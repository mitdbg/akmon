"""Serve ontology modules out of a verified distribution package.

Slice 4 of #1179. Slice 1 made catalog composition resolve every
`modules:` entry through a provider keyed by owner; slice 3 gave the
package manifest a `module_declarations` list. This joins the two: a
verified package becomes one `ModuleProvider` per declared owner, so a
packaged module composes as a peer of the built-in `okg` library.

**Resolution is by declared id only.** The provider serves exactly the
ids in `module_declarations`; there is no filesystem walk of the package,
no `PYTHONPATH`, and no import path. Bytes that merely sit in the
verified object store are not a module until a declaration names them.

**Nothing here executes package code.** Each declared asset's payload is
read back through the package's own verified reader and written into a
provider-owned staging root -- one directory per owner, one per module --
and composition then reads it with the same `fastyaml.safe_load` it uses
for the in-tree library. Executing a packaged asset is #1181's broker
boundary, not this file's. The containment rule still runs against that
staging root, because it must hold for an operator-supplied root too.
"""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from okg.substrate.catalog.module_providers import (
    ModuleProvider,
    contained_child,
    require_nfc_identity,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "MODULE_FILE_BY_ASSET_KIND",
    "PackagedModuleOwnerCollision",
    "PackagedModuleUnavailable",
    "package_module_providers",
]


# A packaged module is a small closed set of files, named by the declaring
# asset's `kind` rather than by any path inside the package. Asset ids
# cannot carry a path separator (`_OWNER_ID_RE` refuses `/`), so a kind is
# the only honest place for this mapping to live.
MODULE_FILE_BY_ASSET_KIND: Mapping[str, str] = {
    "ontology_schema": "schema.yaml",
    "ontology_module_manifest": "module.yaml",
}

# Composition refuses a module without this file, so the provider refuses
# a declaration that could never supply one.
_REQUIRED_FILE = "schema.yaml"


class PackagedModuleUnavailable(SubstrateError):
    """A package declares a module it cannot supply.

    The declaration exists but the bytes behind it do not: an asset
    reference that names no packaged asset, an asset whose kind is not one
    of a module's files, or a declaration that would produce no
    `schema.yaml`. Each refusal names the module id.
    """

    code = "package_module_unavailable"


class PackagedModuleOwnerCollision(SubstrateError):
    """Two declared identities collide into one staging directory.

    Owner ids and module names are distinct byte strings in the sealed
    manifest, but a case-insensitive filesystem folds `Archi` and `archi`
    onto one directory, and a repeated module id would write into a
    directory another declaration already owns. Either way the staged tree
    would no longer be injective with the declarations that produced it,
    so this refuses and names both identities rather than letting the
    second declaration silently overwrite or merge into the first.
    """

    code = "package_module_owner_collision"


def _refuse(module_id: str, reason: str, **data: object) -> "PackagedModuleUnavailable":
    return PackagedModuleUnavailable(
        f"packaged module {module_id} cannot be supplied: {reason}",
        data={"module_id": module_id, "reason": reason, **data},
        fix_hint=(
            "rebuild the package so the declaration names an asset of a "
            f"module file kind ({', '.join(sorted(MODULE_FILE_BY_ASSET_KIND))})"
        ),
    )


def _directory_identity(path: Path) -> tuple[int, int]:
    """The filesystem's own identity for a directory, not its spelling."""
    info = path.stat()
    return (info.st_dev, info.st_ino)


def _refuse_collision(
    segment: str,
    module_id: str,
    first: str,
    second: str,
    path: Path,
) -> PackagedModuleOwnerCollision:
    return PackagedModuleOwnerCollision(
        f"declared {segment}s {first!r} and {second!r} resolve to one "
        f"staging directory {str(path)!r}",
        data={
            "segment": segment,
            "module_id": module_id,
            "first": first,
            "second": second,
            "staging_path": str(path),
        },
        fix_hint=(
            f"give each declared {segment} a distinct identity; two that "
            "differ only by case collide on a case-insensitive filesystem"
        ),
    )


def package_module_providers(
    package: object,
    staging_root: Path,
) -> dict[str, ModuleProvider]:
    """Build one provider per owner declared by a verified package.

    `package` is a `VerifiedDistributionPackage`. `staging_root` must not
    already exist; this creates it and writes each declared module's
    verified bytes beneath it. The returned providers carry the package's
    release digest, which composition records as the module's origin.
    """
    manifest = package.manifest
    declarations = tuple(getattr(manifest, "module_declarations", ()) or ())
    release_digest = manifest.distribution_release.release_digest

    # Every packaged asset, by its qualified id, so a declared reference
    # that names nothing refuses before a byte is read.
    kind_by_ref = {
        item.asset.qualified_id: item.asset.kind for item in manifest.assets
    }
    # The verified object each declared asset resolves to. Recorded on the
    # provider so a sealed release can pin the bytes a module was composed
    # from, not merely the release that shipped them (#1179 slice 6).
    object_by_ref = {
        item.asset.qualified_id: item.locator.object_digest
        for item in manifest.assets
    }

    staging_root = Path(staging_root)
    # Private and exclusive: the caller owns the directory's lifetime, and a
    # pre-existing path is refused rather than written into. `mkdir(parents=)`
    # applies `mode` to the leaf only, so every intermediate is created
    # explicitly -- an 0o755 parent would expose the staged bytes that the
    # 0o700 leaf is there to protect.
    for parent in reversed(staging_root.parents):
        parent.mkdir(exist_ok=True, mode=0o700)
    staging_root.mkdir(exist_ok=False, mode=0o700)

    declared_names: dict[str, set[str]] = {}
    object_digests: dict[str, set[tuple[str, str]]] = {}
    # Two guards, because one filesystem's answer is not the contract.
    #
    # The case-fold map is decided from the declarations alone, so a package
    # whose owners differ only by case is refused on every filesystem rather
    # than installing on a case-sensitive one and colliding on the operator's
    # laptop. A staged tree that is injective with its declarations here and
    # lossy there is not an identity we can seal.
    #
    # The inode map is the backstop for every other way two names can become
    # one directory -- a case-insensitive filesystem reached by some other
    # spelling, a filesystem that normalizes Unicode, a pre-existing symlink
    # inside the staging root -- and it reads the directory that was actually
    # created rather than the name we asked for.
    owner_by_casefold: dict[str, str] = {}
    owner_by_inode: dict[tuple[int, int], str] = {}
    module_by_path: dict[tuple[int, int], str] = {}
    for declaration in declarations:
        module_id = require_nfc_identity(declaration.module_id, "module_id")
        owner = require_nfc_identity(declaration.owner, "module owner")
        name = declaration.name
        # The owner is a path segment too. A validated manifest cannot carry
        # a traversing owner today, but this file re-checks every other
        # segment it joins, and the staging root may be operator-named.
        owner_root = contained_child(staging_root, owner, owner=owner)
        folded = owner_by_casefold.setdefault(owner.casefold(), owner)
        if folded != owner:
            raise _refuse_collision(
                "owner", module_id, folded, owner, owner_root,
            )
        owner_root.mkdir(exist_ok=True, mode=0o700)
        owner_key = _directory_identity(owner_root)
        claimed = owner_by_inode.setdefault(owner_key, owner)
        if claimed != owner:
            raise _refuse_collision(
                "owner", module_id, claimed, owner, owner_root,
            )
        module_dir = contained_child(owner_root, name, owner=owner)
        folded_name = f"{owner}:{name.casefold()}"
        held_name = owner_by_casefold.setdefault(folded_name, name)
        if held_name != name:
            raise _refuse_collision(
                "module", module_id, held_name, name, module_dir,
            )
        try:
            # Exclusive: a second declaration must never write into a
            # directory the first one owns, whether it repeats the id or
            # merely folds onto it by case.
            module_dir.mkdir(exist_ok=False, mode=0o700)
        except FileExistsError as exc:
            raise _refuse_collision(
                "module",
                module_id,
                module_by_path.get(_directory_identity(module_dir), name),
                module_id,
                module_dir,
            ) from exc
        module_by_path[_directory_identity(module_dir)] = module_id

        written: set[str] = set()
        for ref in declaration.asset_refs:
            ref_id = ref.qualified_id
            kind = kind_by_ref.get(ref_id)
            if kind is None:
                raise _refuse(
                    module_id,
                    "declared asset is absent from the package",
                    asset_ref=ref_id,
                )
            filename = MODULE_FILE_BY_ASSET_KIND.get(kind)
            if filename is None:
                raise _refuse(
                    module_id,
                    "declared asset is not a module file",
                    asset_ref=ref_id,
                    asset_kind=kind,
                )
            if filename in written:
                raise _refuse(
                    module_id,
                    f"two declared assets supply {filename}",
                    asset_ref=ref_id,
                )
            # Read through the package's own verified reader: it reopens,
            # re-verifies, and digest-checks the archive on every call.
            payload = package.read_asset(ref_id)
            contained_child(module_dir, filename, owner=owner).write_bytes(payload)
            written.add(filename)

        if _REQUIRED_FILE not in written:
            raise _refuse(
                module_id,
                f"no declared asset supplies {_REQUIRED_FILE}",
                supplied=sorted(written),
            )
        declared_names.setdefault(owner, set()).add(name)
        for ref in declaration.asset_refs:
            digest = object_by_ref.get(ref.qualified_id)
            if digest is not None:
                object_digests.setdefault(owner, set()).add((name, str(digest)))

    return {
        owner: ModuleProvider(
            owner=owner,
            root=staging_root / owner,
            kind="package",
            declared_modules=frozenset(names),
            release_digest=release_digest,
            object_digests=frozenset(object_digests.get(owner, ())),
        )
        for owner, names in declared_names.items()
    }

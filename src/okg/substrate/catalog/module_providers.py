"""Module providers — who owns the ontology modules a catalog composes.

Catalog composition used to resolve a bare module name against one
directory (`modules_root`), so every module implicitly belonged to the
checkout. External distributions (#1179) need a second source of modules
without giving up the guarantee that one subtype has one owner.

A **module provider** is `(owner, root, kind)`. Providers are a mapping
keyed by owner, not a search path: resolution is exact match, so nothing
falls through and no order decides. Built-in modules under
`src/okg/substrate/library/ontologies/` are owned by `okg`.

A deployment's `modules:` entry is either

  * a bare name — resolves only against the `okg` provider, which is
    what every shipped profile already writes, or
  * an owner-qualified `owner:name`, spelled exactly as
    `okg.substrate.distributions.package._OWNER_ID_RE` requires (one
    colon, no whitespace and no `; , / \\`).

Providers load YAML as data through `fastyaml.safe_load`; nothing here
imports or executes packaged code. Executing packaged assets is #1181's
broker boundary.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Optional
import unicodedata

from okg.substrate.errors import SubstrateError


__all__ = [
    "BUILTIN_OWNER",
    "CrossOwnerSubtypeCollision",
    "ModuleIdentityNotNormalized",
    "ModulePathEscapesProviderRoot",
    "ModuleProvider",
    "ModuleProviderError",
    "PackagedModuleUndeclared",
    "ResolvedModule",
    "require_nfc_identity",
    "builtin_module_providers",
    "normalize_providers",
    "parse_module_ref",
    "providers_from_modules_root",
    "refuse_cross_owner_subtype_collisions",
    "resolve_modules",
]


BUILTIN_OWNER = "okg"

# Mirrors `distributions/package.py:_OWNER_ID_RE` (`owner:name`, one
# colon, no whitespace or separators) — the strict spelling the typed
# asset path already enforces. `_module_id` on the semantic path is
# looser (any string containing a colon); this file follows the strict
# one so the two identities cannot diverge.
_SEGMENT_RE = re.compile(r"^[^\s:;,/\\]+$")
_OWNER_ID_RE = re.compile(r"^[^\s:;,/\\]+:[^\s:;,/\\]+$")
# `.` and `..` match the segment pattern but resolve to a directory outside
# the provider's root. A module name is a directory name, so refuse them.
_RESERVED_SEGMENTS = frozenset({".", ".."})


def _valid_segment(text: str) -> bool:
    return bool(_SEGMENT_RE.match(text)) and text not in _RESERVED_SEGMENTS


class ModuleProviderError(SubstrateError):
    """A module reference names no provider, or is misspelled."""

    code = "module_provider_unresolved"


class CrossOwnerSubtypeCollision(SubstrateError):
    """Two owners declare the same subtype.

    Carries the code the typed-asset path already spells
    (`deployment_product_composition.py`) so operators read one
    collision vocabulary, not two. Same-owner cross-declaration is NOT
    a collision: thirteen subtype names are cross-declared among the
    built-in `okg` modules today and five of six shipped profiles load
    an overlapping pair.
    """

    code = "duplicate_semantic_ownership"


class ModuleIdentityNotNormalized(SubstrateError):
    """A module owner or name is not in Unicode NFC.

    **Refused at declaration, not normalized at comparison.** A packaged
    module id is part of a digest-sealed manifest: the release digest is
    taken over the bytes as written. Normalizing only when two ids are
    compared would let an NFC and an NFD spelling of one owner hash to two
    distinct sealed identities while resolving to one provider key --
    identity that is single-sited but not injective. Refusing the non-NFC
    spelling at the point of declaration keeps one id, one byte string, one
    digest, and one provider.
    """

    code = "module_identity_not_nfc"


class ModulePathEscapesProviderRoot(SubstrateError):
    """A module reference resolves outside its provider's root.

    A provider root supplied by an external distribution is not trusted to
    contain only ordinary directories, so the resolved module directory is
    checked against the resolved root: a symlink, a `..` component, or an
    absolute path that leaves the root refuses here rather than reading a
    schema from wherever it landed.
    """

    code = "module_path_escapes_provider_root"


class PackagedModuleUndeclared(SubstrateError):
    """A provider was asked for a module its package does not declare.

    A package-backed provider serves exactly the ids in the package
    manifest's `module_declarations`. Bytes that happen to sit in the
    verified object store are not a module until a declaration names them,
    so an undeclared name refuses instead of resolving.
    """

    code = "package_module_undeclared"


def require_nfc_identity(text: str, field: str) -> str:
    """Return `text` unchanged, or refuse when it is not Unicode NFC."""
    if unicodedata.is_normalized("NFC", text):
        return text
    raise ModuleIdentityNotNormalized(
        f"{field} {text!r} is not in Unicode NFC",
        data={
            "field": field,
            "value": text,
            "nfc_form": unicodedata.normalize("NFC", text),
        },
        fix_hint=(
            "spell the identity in Unicode NFC; the same characters in a "
            "decomposed form are a different identity here"
        ),
    )


def contained_child(root: Path, name: str, *, owner: str) -> Path:
    """Return `root / name`, refusing when it resolves outside `root`.

    The returned path is the unresolved join, so refusal messages elsewhere
    keep naming the path the operator wrote; only the containment decision
    uses the resolved forms.
    """
    candidate = root / name
    real_root = os.path.realpath(root)
    real_candidate = os.path.realpath(candidate)
    if os.path.commonpath([real_root, real_candidate]) != real_root or (
        real_candidate == real_root
    ):
        raise ModulePathEscapesProviderRoot(
            f"module {name!r} of owner {owner!r} resolves to "
            f"{real_candidate!r}, outside provider root {real_root!r}",
            data={
                "owner": owner,
                "module_name": name,
                "provider_root": real_root,
                "resolved_path": real_candidate,
            },
            fix_hint=(
                "keep the module directory inside the provider root; a "
                "symlink out of the root is not followed"
            ),
        )
    return candidate


@dataclass(frozen=True)
class ModuleProvider:
    """One owner's module source directory.

    `kind` is `builtin` for the in-tree ontology library and `package`
    for a verified distribution object store (#1179 slice 4).

    `declared_modules`, when set, is the closed set of names this provider
    may serve -- the package manifest's `module_declarations` for this
    owner. `None` means "whatever the root contains", which is only
    correct for the in-tree library.

    `release_digest` is the package release digest a `package` provider
    was built from; composition records it as the module's origin.

    `object_digests` pairs each declared module name with the verified
    object digests behind its files. It is a frozenset of pairs rather
    than a mapping so the provider stays hashable and frozen, and it is
    `None` for the in-tree library, whose bytes the release already pins
    by `code_revision`.
    """

    owner: str
    root: Path
    kind: str = "builtin"
    declared_modules: Optional[frozenset[str]] = None
    release_digest: Optional[str] = None
    object_digests: Optional[frozenset[tuple[str, str]]] = None

    def module_object_digests(self, name: str) -> tuple[str, ...]:
        """The object digests behind one module, sorted, possibly empty."""
        if self.object_digests is None:
            return ()
        return tuple(
            sorted(digest for module, digest in self.object_digests if module == name)
        )

    def module_dir(self, name: str) -> Path:
        """Resolve one module name against this provider's root.

        Two refusals, both before any file is read: a name the provider does
        not declare, and a name that resolves outside the root.
        """
        if self.declared_modules is not None and name not in self.declared_modules:
            raise PackagedModuleUndeclared(
                f"module {self.owner}:{name} is not declared by the "
                f"{self.owner} package",
                data={
                    "module_id": f"{self.owner}:{name}",
                    "owner": self.owner,
                    "module_name": name,
                    "declared_modules": sorted(self.declared_modules),
                },
                fix_hint=(
                    "declare the module in the package manifest's "
                    "module_declarations, or drop it from modules:"
                ),
            )
        return contained_child(self.root, name, owner=self.owner)


@dataclass(frozen=True)
class ResolvedModule:
    """A `modules:` entry bound to the provider that supplies it."""

    ref: str
    owner: str
    name: str
    module_dir: Path
    provider_kind: str
    release_digest: Optional[str] = None
    object_digests: tuple[str, ...] = ()

    @property
    def module_id(self) -> str:
        return f"{self.owner}:{self.name}"


def builtin_module_providers() -> dict[str, ModuleProvider]:
    """The one provider every deployment gets: `okg`, the in-tree library."""
    from okg.substrate.catalog.modules_compose import load_modules_root

    return providers_from_modules_root(load_modules_root())


def providers_from_modules_root(root: Path | str) -> dict[str, ModuleProvider]:
    """Wrap a legacy `modules_root=` path as the single `okg` provider."""
    return {
        BUILTIN_OWNER: ModuleProvider(
            owner=BUILTIN_OWNER, root=Path(root), kind="builtin",
        ),
    }


def normalize_providers(
    providers: Optional[Mapping[str, ModuleProvider]] = None,
    modules_root: Optional[Path | str] = None,
) -> dict[str, ModuleProvider]:
    """Resolve the two call shapes into one provider mapping.

    `providers` wins when both are given only if they agree; supplying
    both a provider mapping and a `modules_root` is a caller bug, so it
    refuses rather than silently dropping one.
    """
    if providers is not None and modules_root is not None:
        raise ModuleProviderError(
            "pass either providers= or modules_root=, not both",
            data={"owners": sorted(providers), "modules_root": str(modules_root)},
            fix_hint="drop modules_root= and put the path in a ModuleProvider",
        )
    if providers is not None:
        out: dict[str, ModuleProvider] = {}
        for owner, provider in providers.items():
            if owner != provider.owner:
                raise ModuleProviderError(
                    f"provider keyed {owner!r} declares owner "
                    f"{provider.owner!r}",
                    data={"key": owner, "declared_owner": provider.owner},
                    fix_hint="key each provider by its own owner",
                )
            require_nfc_identity(owner, "provider owner")
            if not _valid_segment(owner):
                raise ModuleProviderError(
                    f"provider owner {owner!r} is not a valid owner segment",
                    data={"owner": owner},
                    fix_hint="use an owner with no whitespace or `:;,/\\`",
                )
            out[owner] = provider
        return out
    if modules_root is not None:
        return providers_from_modules_root(modules_root)
    return builtin_module_providers()


def parse_module_ref(ref: str) -> tuple[str, str]:
    """Split a `modules:` entry into `(owner, name)`.

    A bare name is owner `okg`. An owner-qualified reference must match
    the strict one-colon spelling; anything else refuses here rather
    than resolving to a path that happens to exist.
    """
    text = require_nfc_identity(str(ref), "module reference")
    if ":" not in text:
        if not _valid_segment(text):
            raise ModuleProviderError(
                f"module reference {ref!r} is not a valid module name",
                data={"module_ref": text},
                fix_hint="use `name` for a built-in module or `owner:name`",
            )
        return BUILTIN_OWNER, text
    owner, _, name = text.partition(":")
    if not (_OWNER_ID_RE.match(text) and _valid_segment(owner)
            and _valid_segment(name)):
        raise ModuleProviderError(
            f"module reference {ref!r} is not a valid `owner:name`",
            data={"module_ref": text},
            fix_hint=(
                "use exactly one colon and no whitespace or `;,/\\` "
                "on either side"
            ),
        )
    return owner, name


def resolve_modules(
    module_refs: Iterable[str],
    providers: Mapping[str, ModuleProvider],
) -> dict[str, ResolvedModule]:
    """Bind every `modules:` entry to its provider, keyed by the entry.

    Exact match on owner. An unknown owner refuses and names the owners
    that are available; there is no fallback provider to try next.
    """
    resolved: dict[str, ResolvedModule] = {}
    for ref in module_refs:
        owner, name = parse_module_ref(ref)
        provider = providers.get(owner)
        if provider is None:
            raise ModuleProviderError(
                f"module {ref!r} names owner {owner!r}, which supplies no "
                f"modules here",
                data={
                    "module_ref": str(ref),
                    "owner": owner,
                    "available_owners": sorted(providers),
                },
                fix_hint=(
                    "install the owner's distribution, or use a built-in "
                    "module name"
                ),
            )
        resolved[str(ref)] = ResolvedModule(
            ref=str(ref),
            owner=owner,
            name=name,
            module_dir=provider.module_dir(name),
            provider_kind=provider.kind,
            release_digest=provider.release_digest,
            object_digests=provider.module_object_digests(name),
        )
    return resolved


def refuse_cross_owner_subtype_collisions(
    owners_by_subtype: Mapping[str, Mapping[str, Iterable[str]]],
) -> None:
    """Refuse when one subtype is declared under two different owners.

    `owners_by_subtype` maps subtype → owner → the module references
    that declared it. Same-owner cross-declaration keeps today's
    first-origin-wins merge; only a cross-owner declaration refuses,
    and it refuses before the composed catalog is returned, so no
    catalog or generation state has been mutated.
    """
    offenders = sorted(
        (subtype, sorted(by_owner))
        for subtype, by_owner in owners_by_subtype.items()
        if len(by_owner) > 1
    )
    if not offenders:
        return
    subtype, owners = offenders[0]
    detail = ", ".join(
        f"{name}: " + " and ".join(
            f"{each} ({', '.join(sorted(owners_by_subtype[name][each]))})"
            for each in owner_list
        )
        for name, owner_list in offenders
    )
    raise CrossOwnerSubtypeCollision(
        f"subtype {subtype!r} is declared by owners "
        f"{' and '.join(owners)}; cross-owner subtype ownership is refused "
        f"before any catalog mutation [{detail}]",
        data={
            "phase": "pre_mutation",
            "subtype": subtype,
            "owners": owners,
            "collisions": [
                {
                    "subtype": name,
                    "owners": owner_list,
                    "modules_by_owner": {
                        owner: sorted(owners_by_subtype[name][owner])
                        for owner in owner_list
                    },
                }
                for name, owner_list in offenders
            ],
        },
        fix_hint=(
            "rename the subtype in one owner's module, or drop one of the "
            "two modules from the deployment's modules: list"
        ),
    )

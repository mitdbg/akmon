"""Identifier checksum validators.

Each function takes a string and returns True iff it's a
syntactically- and checksum-valid identifier. Used by
`SqlIdentifierExtractor` to filter regex matches with
checksum-bearing identifiers — substrate's SQL function
returns regex-matching strings; these validators reject
checksum failures Python-side (Postgres can't easily express
Mod-97 / Mod-11).

Per `add-doc-corpus-extraction-adapter` (2026-04-25): the
`VALIDATORS` dict is now a registry-backed view. New
validators can register at runtime via
`register_validator(kind, fn)`; lookup goes through
`validator_for(kind)` (raises `ValidatorNotFound` on miss)
or the legacy `validate(value, kind)` shim (returns True
on miss for back-compat with NULL `validator_kind`
columns).
"""

from __future__ import annotations

from typing import Callable

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction.validators.doi import is_valid_doi
from okg.substrate.library.extraction.validators.isbn import is_valid_isbn
from okg.substrate.library.extraction.validators.orcid import is_valid_orcid


__all__ = [
    "VALIDATORS",
    "ValidatorNotFound",
    "available_validators",
    "is_valid_doi",
    "is_valid_isbn",
    "is_valid_orcid",
    "register_validator",
    "validate",
    "validator_for",
]


class ValidatorNotFound(SubstrateError):
    code = "validator_not_found"


_REGISTRY: dict[str, Callable[[str], bool]] = {}


def register_validator(
    kind: str, fn: Callable[[str], bool],
) -> None:
    """Register a checksum validator for `kind`. Idempotent."""
    _REGISTRY[kind] = fn


def validator_for(kind: str) -> Callable[[str], bool]:
    """Return the validator function registered for `kind`.

    Raises `ValidatorNotFound` on miss with the available list.
    Use this when the caller expects the kind to be registered
    (typed code paths). For the legacy NULL-kind passthrough,
    use `validate(value, kind)` instead.
    """
    fn = _REGISTRY.get(kind)
    if fn is None:
        raise ValidatorNotFound(
            f"no validator registered for kind {kind!r}",
            data={
                "kind": kind,
                "available": sorted(_REGISTRY.keys()),
            },
            fix_hint=(
                "register a validator via "
                "`okg.substrate.library.extraction.validators."
                "register_validator` or use one of the available "
                "kinds"
            ),
        )
    return fn


def available_validators() -> list[str]:
    return sorted(_REGISTRY.keys())


def validate(value: str, validator_kind: str | None) -> bool:
    """Dispatch to the named validator. Unknown kinds (or NULL)
    return True (no-op).

    This shim preserves the pre-2026-04-25 behavior where
    `okg.identifier_patterns.validator_kind` rows with NULL or
    unknown values silently bypass the check. New callers
    should prefer `validator_for(kind)` which raises on miss.
    """
    if validator_kind is None:
        return True
    fn = _REGISTRY.get(validator_kind)
    if fn is None:
        return True
    return fn(value)


def _bootstrap() -> None:
    register_validator("doi_checksum", is_valid_doi)
    register_validator("orcid_checksum", is_valid_orcid)
    register_validator("isbn_checksum", is_valid_isbn)


_bootstrap()


# Back-compat re-export. Kept as a property of the registry so
# downstream code reading VALIDATORS sees the live registry.
class _ValidatorsProxy(dict):
    """Read-only-ish view over the registry. Tests / legacy
    callers that did `from validators import VALIDATORS` keep
    working."""

    def __getitem__(self, key):
        return _REGISTRY[key]

    def __contains__(self, key):
        return key in _REGISTRY

    def __iter__(self):
        return iter(_REGISTRY)

    def __len__(self):
        return len(_REGISTRY)

    def get(self, key, default=None):
        return _REGISTRY.get(key, default)

    def keys(self):
        return _REGISTRY.keys()

    def values(self):
        return _REGISTRY.values()

    def items(self):
        return _REGISTRY.items()


VALIDATORS = _ValidatorsProxy()

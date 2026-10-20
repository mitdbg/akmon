"""Tests for entity-consistent keyed pseudonymization.

Proves stability (same canonical -> same pseudonym across independent service
instances, i.e. across cycles/processes), injectivity by construction, loud
failure when the secret-store key is absent, the absence of any reverse map,
explicit epoch rotation, and fail-closed composition (a raw value never appears
in the pseudonym or leaks through an unresolved/raise-mode cascade).
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Optional

import pytest

from okg.substrate.alias import pseudonymize as pz
from okg.substrate.alias.cascade import AliasResolutionFailed
from okg.substrate.alias.pseudonymize import (
    PseudonymKeyUnavailable,
    PseudonymService,
    UnresolvedEntityHeld,
    load_pseudonym_service,
    pseudonymize_entity,
)

_KEY = b"unit-test-secret-key-material-32b"


@dataclass(frozen=True)
class _Entity:
    value: str
    type: Optional[str] = None


class _ConstResolver:
    """Resolver that always returns a fixed canonical (or None)."""

    def __init__(self, canonical: Optional[str]) -> None:
        self._canonical = canonical

    def resolve(self, entity: object) -> Optional[str]:
        return self._canonical


class _RaisingResolver:
    """Resolver that raises AliasResolutionFailed carrying the raw needle."""

    def resolve(self, entity: object) -> Optional[str]:
        raw = getattr(entity, "value", "")
        raise AliasResolutionFailed(
            f"no backend resolved {raw!r}", data={"needle": raw}
        )


def _service(epoch: str = "e1") -> PseudonymService:
    return PseudonymService(key=_KEY, epoch=epoch)


def test_same_canonical_is_stable_across_instances() -> None:
    # Two independent services (stand-ins for two publish cycles / processes)
    # must agree -- pseudonym stability reduces to keyed determinism.
    a = _service().pseudonym(canonical="person:alice", entity_type="person")
    b = _service().pseudonym(canonical="person:alice", entity_type="person")
    assert a == b


def test_distinct_canonicals_do_not_collide() -> None:
    svc = _service()
    pseudonyms = {
        svc.pseudonym(canonical=f"person:{i}", entity_type="person")
        for i in range(5000)
    }
    assert len(pseudonyms) == 5000  # injective at the fixed 128-bit width


def test_field_encoding_is_unambiguous() -> None:
    # The MAC input must be an injective encoding of (epoch, entity_type,
    # canonical): NUL-delimited concatenation is not, so these boundary-shifted
    # pairs would collide under a naive f-string. They must differ.
    svc = _service()
    assert svc.pseudonym(canonical="\x00rest", entity_type="a") != svc.pseudonym(
        canonical="rest", entity_type="a\x00"
    )
    assert svc.pseudonym(canonical="bc", entity_type="a") != svc.pseudonym(
        canonical="c", entity_type="ab"
    )


def test_missing_or_weak_key_fails_loud() -> None:
    with pytest.raises(PseudonymKeyUnavailable):
        load_pseudonym_service(env={})
    with pytest.raises(PseudonymKeyUnavailable):
        load_pseudonym_service(env={"OKG_PSEUDONYM_HMAC_KEY": ""})
    with pytest.raises(PseudonymKeyUnavailable):  # too short
        load_pseudonym_service(env={"OKG_PSEUDONYM_HMAC_KEY": "short"})


def test_key_loads_from_secret_store_env() -> None:
    svc = load_pseudonym_service(env={"OKG_PSEUDONYM_HMAC_KEY": "sixteen-byte-key"})
    assert svc.key == b"sixteen-byte-key"


def test_no_reverse_map_exists() -> None:
    svc = _service()
    # The real argument is that the service is a pure frozen dataclass holding
    # only the key/width/epoch -- no raw->pseudonym store, and no inverse method.
    assert dataclasses.is_dataclass(svc)
    assert svc.__dataclass_params__.frozen
    assert {f.name for f in dataclasses.fields(svc)} == {"key", "label_width", "epoch"}
    assert not any(
        name for name in dir(pz) if "reverse" in name.lower() or "depseudon" in name.lower()
    )


def test_rotation_is_an_explicit_and_visible_epoch() -> None:
    p1 = _service(epoch="e1").pseudonym(canonical="person:alice", entity_type="person")
    p2 = _service(epoch="e2").pseudonym(canonical="person:alice", entity_type="person")
    assert p1 != p2  # a new epoch forks the pseudonym space
    assert "_e1_" in p1 and "_e2_" in p2  # the fork is visible/auditable


def test_raw_value_is_never_in_the_pseudonym() -> None:
    raw = "alice.mccarthy@secret.example.com"
    out = _service().pseudonym(canonical=raw, entity_type="email")
    assert raw not in out
    assert "alice" not in out and "secret.example" not in out


def test_unresolved_entity_is_held_not_surface_keyed() -> None:
    svc = _service()
    entity = _Entity(value="alice.mccarthy@secret.example.com", type="email")
    # An unresolved entity is HELD, never surface-keyed or passed through raw.
    with pytest.raises(UnresolvedEntityHeld) as exc:
        pseudonymize_entity(svc, _ConstResolver(None), entity)
    assert entity.value not in str(exc.value)  # the hold carries no raw value


def test_raise_mode_exception_is_swallowed_and_held_raw_free() -> None:
    svc = _service()
    entity = _Entity(value="bob.jones@secret.example.com", type="email")
    # A raise-mode cascade embeds the raw needle in its exception; the composition
    # must swallow it and re-raise a raw-free hold, not leak the needle.
    with pytest.raises(UnresolvedEntityHeld) as exc:
        pseudonymize_entity(svc, _RaisingResolver(), entity)
    assert entity.value not in str(exc.value)
    assert "bob.jones" not in str(exc.value)


def test_resolved_and_unresolved_of_same_entity_are_consistent_when_resolved() -> None:
    svc = _service()
    # When the cascade resolves both surface variants to one canonical, the
    # pseudonym is identical regardless of surface form.
    resolver = _ConstResolver("person:alice")
    p_from_full = pseudonymize_entity(svc, resolver, _Entity("A. Alice", "person"))
    p_from_abbrev = pseudonymize_entity(svc, resolver, _Entity("Alice A.", "person"))
    assert p_from_full == p_from_abbrev

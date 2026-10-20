"""Entity-consistent keyed pseudonymization on the alias spine.

The alias cascade (:mod:`okg.substrate.alias.cascade`) resolves a surface form to
a canonical entity; this module maps that canonical to a STABLE, keyed pseudonym
so the same entity carries the same token across every node, edge, generation,
and publish cycle -- joins and edge topology survive -- while the raw value does
not.

Design (fixed by ``add-graph-anonymization-pseudonym-vault``):

* **Injectivity by construction.** The pseudonym token is an HMAC-SHA256 over the
  canonical identity, truncated to :data:`DEFAULT_LABEL_WIDTH` hex chars (128
  bits by default). Collision probability is < 2**-64 even at 10**9 distinct
  entities (birthday bound) -- negligible at graph scale. The width IS the
  injectivity argument; the accompanying test is a smoke check, not the
  guarantee.
* **Keyed, no reverse map.** The HMAC key comes from the deployment secret store
  (:data:`PSEUDONYM_KEY_ENV`); there is NO stored raw->pseudonym mapping and no
  inverse function. De-anonymization is a separate, future, break-glass-gated
  concern. A missing key fails LOUD (never a silent default key).
* **Rotation is an explicit epoch.** The pseudonym is bound to an ``epoch`` label
  that is part of both the HMAC input and the visible pseudonym, so rotating the
  key means bumping the epoch: the fork is deterministic and auditable, never a
  silent re-key.
* **Fail-closed composition.** :func:`pseudonymize_entity` never lets a raw value
  pass through: an unresolved entity is HELD (:class:`UnresolvedEntityHeld`,
  carrying no raw value), not surface-keyed, and a raise-mode resolver's
  raw-bearing exception is swallowed rather than re-raised with its needle.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from typing import Mapping, Optional, Protocol

from okg.substrate.alias.cascade import AliasResolutionFailed
from okg.substrate.errors import SubstrateError

__all__ = [
    "DEFAULT_LABEL_WIDTH",
    "MIN_KEY_BYTES",
    "PSEUDONYM_KEY_ENV",
    "PseudonymKeyUnavailable",
    "PseudonymService",
    "UnresolvedEntityHeld",
    "load_pseudonym_service",
    "pseudonymize_entity",
]

#: Secret-store env var holding the HMAC key material. No default: absence is a
#: loud failure, not a silent fallback key.
PSEUDONYM_KEY_ENV = "OKG_PSEUDONYM_HMAC_KEY"

#: HMAC hexdigest truncation width (hex chars). 32 => 128 bits.
DEFAULT_LABEL_WIDTH = 32

#: Minimum key material length. Rejects trivially-weak keys loudly rather than
#: silently accepting them.
MIN_KEY_BYTES = 16

_LABEL_RE = re.compile(r"[^a-z0-9]+")


class PseudonymKeyUnavailable(SubstrateError):
    code = "pseudonym_key_unavailable"


class UnresolvedEntityHeld(SubstrateError):
    """Raised when an entity does not resolve to a canonical identity.

    The alias cascade could not resolve the entity, so there is no stable
    canonical to key a pseudonym on. Per the ratified design an unresolved
    identifier is HELD (not surface-keyed, not passed through raw); the caller
    holds the fact. The raw value is deliberately NOT included in this error.
    """

    code = "unresolved_entity_held"


class _Resolver(Protocol):
    def resolve(self, entity: object) -> Optional[str]:
        ...


def _label_for(entity_type: Optional[str]) -> str:
    base = _LABEL_RE.sub("", (entity_type or "").lower())
    return base or "pii"


@dataclass(frozen=True)
class PseudonymService:
    """Pure, deterministic keyed pseudonymizer. Holds no raw->pseudonym map."""

    key: bytes
    label_width: int = DEFAULT_LABEL_WIDTH
    epoch: str = "e1"

    def pseudonym(self, *, canonical: str, entity_type: Optional[str] = None) -> str:
        """Return the stable pseudonym for a canonical identity.

        Deterministic in ``(key, epoch, entity_type, canonical)`` and one-way:
        the raw canonical is not recoverable from the output without the key.
        """
        if not isinstance(canonical, str) or not canonical:
            raise ValueError("canonical identity must be a non-empty string")
        # Inject the fields through a JSON array, not NUL-delimited concatenation:
        # the latter is not injective (e.g. entity_type="a\x00" + canonical="b"
        # collides with entity_type="a" + canonical="\x00b"), which would defeat
        # the width-based injectivity argument. JSON's structure + escaping is
        # unambiguous.
        message = json.dumps(
            [self.epoch, entity_type or "", canonical], ensure_ascii=False
        ).encode("utf-8")
        token = hmac.new(self.key, message, hashlib.sha256).hexdigest()[: self.label_width]
        return f"{_label_for(entity_type)}_{self.epoch}_{token}"


def load_pseudonym_service(
    *,
    label_width: int = DEFAULT_LABEL_WIDTH,
    epoch: str = "e1",
    env: Optional[Mapping[str, str]] = None,
) -> PseudonymService:
    """Build a service from the secret-store key, failing LOUD when it is absent."""
    source: Mapping[str, str] = os.environ if env is None else env
    raw = source.get(PSEUDONYM_KEY_ENV)
    if not raw:
        raise PseudonymKeyUnavailable(
            f"pseudonym HMAC key not configured; set {PSEUDONYM_KEY_ENV} "
            "from the deployment secret store (no default key is used)"
        )
    key = raw.encode("utf-8")
    if len(key) < MIN_KEY_BYTES:
        raise PseudonymKeyUnavailable(
            f"{PSEUDONYM_KEY_ENV} is too short ({len(key)} bytes); "
            f"require at least {MIN_KEY_BYTES} bytes of key material"
        )
    return PseudonymService(key=key, label_width=label_width, epoch=epoch)


def pseudonymize_entity(
    service: PseudonymService, resolver: _Resolver, entity: object
) -> str:
    """Resolve ``entity`` through the alias cascade, then pseudonymize (fail-closed).

    An entity the cascade cannot resolve is HELD -- :class:`UnresolvedEntityHeld`
    is raised (carrying no raw value) rather than surface-keying the raw form,
    because surface-keying would silently split one entity into different tokens
    before vs. after resolution improves. Any raw-bearing resolution exception is
    swallowed rather than logged, then re-raised as the raw-free hold.
    """
    try:
        canonical = resolver.resolve(entity)
    except AliasResolutionFailed:
        # A raise-mode cascade embeds the raw needle in its exception; do not let
        # it propagate or reach a log sink.
        canonical = None
    if not canonical:
        raise UnresolvedEntityHeld(
            f"entity of type {getattr(entity, 'type', None)!r} did not resolve "
            "to a canonical identity; held (never surface-keyed or stored raw)"
        )
    return service.pseudonym(
        canonical=canonical, entity_type=getattr(entity, "type", None)
    )

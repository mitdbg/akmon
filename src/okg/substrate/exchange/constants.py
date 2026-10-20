"""Wire constants for Poros v0."""

from __future__ import annotations


POROS_VERSION = "0"
DOMAIN_PREFIX = b"POROS-V0\0"

CANONICALIZATION_ALG = "RFC8785-JCS"
SIGNATURE_ALG = "Ed25519"
DIGEST_ALG = "SHA-256"

GRAPH_SUBMIT_TYPE = "graph.submit"
LEGACY_BUNDLE_SUBMIT_TYPE = "bundle.submit"
ENCRYPTED_GRAPH_SUBMIT_TYPE = "encrypted.graph.submit"
RECEIPT_TYPE = "receipt"

INLINE_SMALL_MAX_BYTES = 100 * 1024
INLINE_MEDIUM_MAX_BYTES = 1024 * 1024
DETACHED_MAX_BYTES = 1024**3
EXCHANGE_CLASSIFICATIONS = frozenset({
    "public",
    "internal",
    "restricted",
    "private",
    "unspecified",
})

DEFAULT_REPLAY_RETENTION_SECONDS = 24 * 60 * 60
DEFAULT_MAX_CLOCK_SKEW_SECONDS = 5 * 60
DEFAULT_MAX_EXPIRY_SECONDS = 24 * 60 * 60

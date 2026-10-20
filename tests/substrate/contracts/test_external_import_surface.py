"""Guard the public SDK names external connectors and producers import.

Each name must stay exported from its public module and must be the same
object as its substrate definition. The source runner isinstance-checks some
of these types, so a wrapper or copy would break connectors that follow the
public SDK.

The table carries the public module because the surface spans more than one:
connector runtime names live in ``okg.deployment``, while the distribution and
release producers live in ``okg.distributions``.

A consumer verifies its own framework imports in its own CI. This test names
no consumer and lists no consumer's private import paths.
"""
import importlib

import pytest

ABSORBED_INTO_SDK = {
    # (substrate module, substrate symbol): (public module, public SDK name)
    ("okg.substrate.library.sources.base", "NodeFact"): ("okg.deployment", "NodeFact"),
    ("okg.substrate.library.sources.base", "EdgeFact"): ("okg.deployment", "EdgeFact"),
    ("okg.substrate.library.sources.base", "ProgressMarker"): ("okg.deployment", "ProgressMarker"),
    ("okg.substrate.library.sources.base", "RecordSet"): ("okg.deployment", "RecordSet"),
    ("okg.substrate.library.sources.base", "RecordNode"): ("okg.deployment", "RecordNode"),
    ("okg.substrate.library.sources.base", "RecordEmission"): ("okg.deployment", "RecordEmission"),
    ("okg.substrate.library.sources.base", "EdgeKey"): ("okg.deployment", "EdgeKey"),
    ("okg.substrate.images.values", "ImageRef"): ("okg.deployment", "ImageRef"),
    (
        "okg.substrate.library.sources.base",
        "SourcePreflightResult",
    ): ("okg.deployment", "PreflightResult"),
    (
        "okg.substrate.library.sources.content_hash_probe",
        "ContentHashProbe",
    ): ("okg.deployment", "ContentHashProbe"),
    (
        "okg.substrate.library.sources.content_hash_probe",
        "file_content_items",
    ): ("okg.deployment", "file_content_items"),
    (
        "okg.substrate.library.sources.mutable_api_probe",
        "MutableApiProbe",
    ): ("okg.deployment", "MutableApiProbe"),
    ("okg.substrate.sources.preflight", "file_ref_preflight"): ("okg.deployment", "file_preflight"),
    (
        "okg.substrate.sources.preflight",
        "credential_env_preflight",
    ): ("okg.deployment", "credential_preflight"),
    ("okg.substrate.sources.preflight", "http_probe_result"): ("okg.deployment", "http_preflight"),
    (
        "okg.substrate.sources.preflight",
        "dependency_preflight",
    ): ("okg.deployment", "dependency_preflight"),
    ("okg.substrate.sources.redaction", "redact_text"): ("okg.deployment", "redact"),
    ("okg.substrate.sources.redaction", "redact_endpoint"): ("okg.deployment", "redact_endpoint"),
    ("okg.substrate.sources.redaction", "redact_mapping"): ("okg.deployment", "redact_mapping"),
    (
        "okg.substrate.distributions.install_bundle",
        "build_bundle",
    ): ("okg.distributions", "build_release_entry"),
}

ABSORBED_PAIRS = [
    (module_name, symbol, public_module, public_name)
    for (module_name, symbol), (public_module, public_name) in sorted(
        ABSORBED_INTO_SDK.items()
    )
]


@pytest.mark.parametrize(
    ("module_name", "symbol", "public_module", "public_name"), ABSORBED_PAIRS
)
def test_absorbed_name_is_reachable_from_the_public_sdk(
    module_name: str, symbol: str, public_module: str, public_name: str
) -> None:
    public = importlib.import_module(public_module)

    assert public_name in public.__all__, (
        f"{public_name} left {public_module}.__all__; external authors "
        f"would have to import {module_name}.{symbol} again."
    )
    substrate_object = getattr(importlib.import_module(module_name), symbol)
    assert getattr(public, public_name) is substrate_object, (
        f"{public_module}.{public_name} is no longer the same object as "
        f"{module_name}.{symbol}. These are aliases on purpose — the source "
        "runner isinstance-checks ProgressMarker and consumes the record-set "
        "types directly, so a wrapper breaks the runner, and a wrapped "
        "producer lets the public and private signatures drift apart."
    )

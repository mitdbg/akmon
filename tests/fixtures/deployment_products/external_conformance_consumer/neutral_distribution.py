"""Neutral external-style distribution declaration used by conformance tests."""

from okg.deployment import ExtensionBatch, ExtensionRequest, extension_entrypoint


@extension_entrypoint(
    "neutral-domain-source",
    kind="connector",
    capabilities=("fixture-read",),
)
def read_fixture(request: ExtensionRequest) -> ExtensionBatch:
    """Return inert fixture metadata without importing OKG implementation modules."""

    return ExtensionBatch(observations={"binding_id": request.binding_id})

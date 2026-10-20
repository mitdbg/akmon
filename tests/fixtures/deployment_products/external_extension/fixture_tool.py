from okg.deployment import ExtensionBatch, extension_entrypoint


@extension_entrypoint(
    "fixture.tool",
    kind="tool",
    capabilities=("provider.metadata.read",),
)
def tool(request):
    return ExtensionBatch(observations={"result": request.payload["query"]})

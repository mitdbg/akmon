from okg.deployment import ExtensionBatch, extension_entrypoint


@extension_entrypoint(
    "fixture.flaky",
    kind="verifier",
    capabilities=(),
)
def flaky(request):
    if request.payload["crash"]:
        raise RuntimeError("simulated child-process crash")
    return ExtensionBatch(observations={"recovered": True})

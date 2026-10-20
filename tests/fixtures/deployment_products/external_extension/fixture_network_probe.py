from okg.deployment import ExtensionBatch, extension_entrypoint


@extension_entrypoint(
    "fixture.network-probe",
    kind="verifier",
    capabilities=("network",),
)
def network_probe(request):
    import socket

    socket.socket().connect(("example.test", 443))
    return ExtensionBatch()

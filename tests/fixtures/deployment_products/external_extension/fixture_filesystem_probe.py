from okg.deployment import ExtensionBatch, extension_entrypoint


@extension_entrypoint(
    "fixture.filesystem-probe",
    kind="verifier",
    capabilities=("filesystem",),
)
def filesystem_probe(request):
    from pathlib import Path

    Path("/etc/passwd").read_text()
    return ExtensionBatch()

from okg.deployment import ExtensionBatch, ExtensionEdge, extension_entrypoint


@extension_entrypoint(
    "fixture.enricher",
    kind="enricher",
    capabilities=("graph.output.propose",),
)
def enricher(request):
    return ExtensionBatch(
        edges=(
            ExtensionEdge(
                src=request.payload["src"],
                dst=request.payload["dst"],
                edge_type="links_to",
                attrs={"binding_id": request.binding_id},
            ),
        ),
    )

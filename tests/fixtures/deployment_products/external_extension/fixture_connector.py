from okg.deployment import ExtensionBatch, ExtensionNode, extension_entrypoint


@extension_entrypoint(
    "fixture.connector",
    kind="connector",
    capabilities=("provider.source.read",),
)
def connector(request):
    item = request.payload["item"]
    return ExtensionBatch(
        nodes=(
            ExtensionNode(
                node_id=f"external:{request.binding_id}:{item['id']}",
                subtype="concept",
                attrs={"label": item["label"], "binding_id": request.binding_id},
            ),
        ),
        observations={"provider_effect": "observe"},
    )

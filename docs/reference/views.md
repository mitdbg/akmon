---
audience: data owner, developer
type: reference
canonical_id: native-views-reference
section: reference
visibility: primary
status: active
owner: documentation
---
# Native views

A native view is a page or table that AKMON builds from the graph at each
generation. The deployment lists its views in `views_registry.yaml`.

## Registry entry keys

| Key | Meaning |
|---|---|
| `view_def_id`, `view_def_factory`, `build_corpus` | Required. The view's id and the `module:attribute` of the code that defines and fills it. |
| `view_name`, `anchor_resolver`, `schedule`, `params`, `llm_client`, `tracking_mode` | Optional. |
| `reads_subtypes`, `reads_edge_types` | Graph types the corpus reads. A view that declares none is assumed to read any graph type. |
| `external_inputs` | What the corpus reads outside the graph: `publish_time`, `source_runs`, or `live:<relation>`. |

An unknown key, or an unknown `external_inputs` value, is refused. `okg
deployment lint` and the upgrade preflight report the same error, naming the
entry, the key and the allowed values, before the publisher could fail to
start. A `build_corpus` that accepts `published_at` is treated as reading
`publish_time`.

## When an earlier output is reused

AKMON reuses a view's output for a new generation only when it can prove the
view's inputs are unchanged. All of these must hold:

- the view's definition is unchanged;
- the earlier output was built for the generation published immediately before
  this one, on the same branch;
- the generation's change set is known, and no declared graph type changed (for
  a view that declares none: nothing in the graph changed);
- the catalog version is the same as that earlier generation's;
- no node the view cites changed;
- the view has no external input.

Otherwise the view is rebuilt. A rebuild whose content equals the earlier
output is stored as a reference to it, so unchanged content takes no new space.

## What a view manifest records

Every manifest records `origin`: the manifest and generation that hold its
content. A view that reads only the graph records
`inputs_valid_through_generation`. A view with external inputs records
`external_inputs_state` for each input and `inputs_as_of`, the oldest input
time. For `live:<relation>`, that is the oldest, over the relation's target
nodes, of each node's newest successful sample at or before the publish time;
nodes with no sample are counted `missing`, and nodes whose newest attempt
failed are counted `failed`.

## Retired views

A view that the current `views_registry.yaml` no longer lists is `retired` in
`okg view list`, the portal views list and `list_view_artifacts`. `okg view
show` refuses it by name. No manifest is deleted.

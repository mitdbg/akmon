---
audience: developer, operator
type: how-to
canonical_id: opensearch-reference-deployment
---
# OpenSearch reference deployment

The `opensearch-reference` deployment is the worked example of modelling a
live external search system in the graph. A local OpenSearch container holds
three seeded indices; the deployment ingests the cluster's **metadata** and
publishes it.

The graph stores no logs. It stores which cluster exists, which indices it
holds, what shape they have, and which services write to them — enough for
an agent to find the right index and then query that system directly.

## Bring it up

```
export OKG_DSN=postgres://.../okg_opensearch_reference   # must already exist
deployments/opensearch-reference/up.sh
```

One command from a clean checkout. It starts the container, waits for
cluster health, seeds the indices, and then provisions the graph: migrate,
apply the catalog, ingest, publish.

It uses `okg provision`, not `okg deployment up`. The latter is the
post-merge refresh verb — "current state to current code" — and its first
step reads the deployment's publish state, so against the empty database a
clean checkout has it fails on a missing table. No subcommand of the okg
CLI starts a container or seeds one either, which is why `up.sh` exists at
all.

`OKG_DSN` must name an existing database called `okg_opensearch_reference`:
the manifest declares that name and provisioning refuses a DSN naming
anything else.

Stop the container when you are finished:

```
deployments/opensearch-reference/up.sh --down
```

## What lands in the graph

| Subtype or edge | Count |
| --- | --- |
| `search_cluster` nodes | 1 |
| `search_index` nodes | 3 |
| `service` nodes | 2 |
| `contains` edges (cluster → index) | 3 |
| `logs_to` edges (service → index) | 4 |

Read one back:

```
okg search --deployment opensearch-reference --query app-logs
```

Each `search_index` node carries the index name and aliases, the current
document count and store size, the retention policy, and a bounded summary
of its field mapping: at most 40 entries of `{name, type}`, flattened to
dotted paths at most four levels deep, the whole list capped at 8 KiB.
`field_count` reports the true number of fields before truncation, so a
reader can see what was left out.

`retention` is a three-state answer, not two. A policy name means one is
attached; `null` with `retention_read: true` means the settings were read
and no policy is attached; `retention_read: false` means the settings could
not be read at all. The seed deliberately leaves `audit-trail-2026.09`
without a policy so all three states are visible in one graph.

## Identity, and what a re-ingest does

An index is keyed by its **UUID**, never its name.

| Change at the cluster | What the graph does |
| --- | --- |
| Index renamed | One attribute updates. Same node, no retraction. |
| Alias rolled over to a new index | A new node appears. Nothing is retracted. |
| Documents written | Counts update. **No new revision** — volatile metrics are deliberately outside the content hash, so ordinary traffic does not churn graph history. |
| Mapping gains a field | A new revision. |
| Index deleted | The node and its edges are retracted, because the run completed full scope. |

A metadata read that fails raises rather than returning an empty scope.
Under `missing_from_completed_scope` an empty completed scope would retract
every node this source owns, so one network timeout must not read as a
cluster-wide deletion.

## Services are declared, not discovered

OpenSearch has no idea which process wrote to an index. The `services` list
in `deployments/opensearch-reference/source_registry.yaml` declares them, and
the source emits one `logs_to` edge per declared (service, index) pair. A
declared index the cluster does not have is refused by name at ingest rather
than quietly dropping the edge.

## The container

Single node, pinned by image digest, capped at 2 GiB, bound to
`127.0.0.1:9209`, with the security plugin and its demo config both off. It
is a local unauthenticated dev container: **there is no credential in the
repository and none is needed.** Pointing the source at a real cluster means
supplying a session with a credential from your own secret store; the
`base_url` parameter in the source registry is the only thing that has to
change.

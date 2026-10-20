# opensearch-reference

The reference integration between the OKG and a live external search
system.

A local single-node OpenSearch container holds three seeded indices. This
deployment ingests the cluster's **metadata** and publishes it: one
`search_cluster` node, one `search_index` node per index, and `logs_to`
edges from the two services declared in `source_registry.yaml`.

The graph stores no logs. It stores which cluster exists, which indices it
holds, what shape they have, and who writes to them — enough for an agent
to find the right index and then ask that system directly.

## One command

```
export OKG_DSN=postgres://.../okg_opensearch_reference   # must already exist
deployments/opensearch-reference/up.sh
```

That starts the container, waits for health, seeds it, then provisions:
migrate, apply the catalog, ingest, publish. `OKG_DSN` must name an
existing database called `okg_opensearch_reference`. Stop it with
`up.sh --down`, which also removes the named volume.

## What lands

| | Count |
| --- | --- |
| `search_cluster` nodes | 1 |
| `search_index` nodes | 3 |
| `service` nodes | 2 |
| `contains` edges (cluster → index) | 3 |
| `logs_to` edges (service → index) | 4 |

`app-logs-2026.09` and `ingest-events-2026.09` carry a retention policy;
`audit-trail-2026.09` deliberately carries none, so the graph shows a
read-and-absent retention beside two present ones. OpenSearch's own
`.`-prefixed bookkeeping indices are out of the declared scope, so the
counts do not depend on which plugins booted.

## The container

Single node, pinned by digest, 2 GiB cap, bound to `127.0.0.1:9209`, and
unauthenticated. It is a local dev container: **there is no credential in
this repository and none is needed.** A real cluster would supply one
through the source's `session` parameter from the environment.

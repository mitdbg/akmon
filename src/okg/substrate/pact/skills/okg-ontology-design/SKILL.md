---
name: okg-ontology-design
description: Extend a deployment's schema — add a subtype, attribute, edge, or narrowing to the ontology and recompose the catalog. Use when the user says "add a node type", "add a subtype/edge", "extend the ontology", "tighten a narrowing", or "a new source needs a new subtype". For publish health checks, edit the deployment's invariants.yaml instead.
---

# Extending the ontology

A deployment's catalog (its record types and allowed relationships) is composed
from three places:

| Piece | Where it lives | Holds |
|---|---|---|
| Deployment schemas | `<deployment>/schemas/*.yaml` | Record types (subtypes) and attributes owned by this deployment |
| Bridges | `<deployment>/schemas/bridges/*.yaml` | Allowed relationships (narrowings) for this deployment, between any loaded types |
| Library modules | `src/okg/substrate/library/ontologies/<module>/` (`module.yaml`, `schema.yaml`, `cross_links/*.yaml`), listed under `modules:` in `deployment.yaml` | Reusable types and relationships shared across deployments |

Schemas are written in LinkML, a YAML schema language: each record type is one
entry under `classes:`. Other terms (archetype, edge archetype, bridge,
cross-links, module) are defined in
`docs/reference/glossary.md`. The reference is
`docs/develop/extensions/ontologies.md`.

## Add a type or relationship to one deployment (default)

1. In `<deployment>/schemas/<name>.yaml`, add a class with
   `annotations: {archetype: <kind>}`, such as `artifact`, `actor`,
   `process`, or `event`, plus its attributes. A class without an archetype
   does not become a record type.
2. Make it searchable. Keyword search reads a record's `text` attribute; when
   the content is in other attributes, list them so they are copied into
   `text`. Without this the records publish but no keyword search finds them:
   ```yaml
       annotations:
         archetype: artifact
         okg_searchable_text_fields: summary,description
   ```
3. In `<deployment>/schemas/bridges/<name>.yaml`, add an `EdgeNarrowings`
   class annotated `okg_edge_narrowings: 'true'`. Each attribute names one
   allowed relationship with `edge_archetype`, `src_subtypes`, and
   `dst_subtypes`. An edge whose triple is not in the active catalog is
   rejected at insert as a `narrowing_violation`. An `EdgeNarrowings` class
   elsewhere under `schemas/` is refused as misplaced.
   `examples/json-record-connector/src/okg_example_records/deployment/schemas/`
   shows both files.
4. Preview, then apply the catalog:
   ```bash
   uv run okg catalog load --deployment <deployment-dir> --json
   uv run okg catalog load --deployment <deployment-dir> --apply --json
   ```
   The preview lists what changes: `subtype_count`, `narrowing_count`, and
   the `bridge_paths` it read. Check that your additions appear before
   applying.
5. Publish a source that emits the new type, then confirm a record of it
   exists and its edges were accepted, with the **okg-query** skill or
   `okg generation describe <generation>`.

## Contribute a reusable library module

Use this path only when the type is reusable outside one deployment:
add `module.yaml` and `schema.yaml` under
`src/okg/substrate/library/ontologies/<module>/`, put narrowings between
modules in `cross_links/*.yaml`, add compose tests under
`tests/substrate/catalog/`, then add the module to a profile's `modules.yaml`
or a deployment's `modules:` list.

Keep each change minimal and justify each new type and relationship. To guard
the new shape on every publish, add an SQL check to the deployment's
`invariants.yaml` (a query that returns the rows breaking the rule).

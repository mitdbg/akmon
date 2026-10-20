---
name: okg-author
description: Extend the academic-researcher schema — add a new subtype, attribute, or narrowing. Triggers on "add a new node type", "track a new attribute", "extend the schema", "add narrowing", "I want to capture X but there's no type for it".
---

# OKG Author

## Goal

Add a new ontology element to the operator's deployment safely: schema change → narrowing → projection → first published instance. The schema is LinkML; the substrate validates at compose time and at insert time.

## Required data

- **Which module** the change belongs to:
  - Communications/email/chat → `communications`
  - Calendar/events → `calendar`
  - PI-specific (Advisee, Grant, Lab, Committee, Meeting…) → `academic_researcher`
  - Concept/paper/author → existing `concepts` / `papers` modules (consider whether refinement is right module)
- **Subtype kind**: new node subtype OR new edge subtype OR new attribute on existing subtype.
- **Cardinality / narrowing**: what subtypes can be parent/child of this edge.
- **Source plan**: which existing source(s) will emit this, or whether a new source is needed.

## Operator surface

Schema editing is FILE editing — AKMON operators are for verification afterward.

- Locate the module's schema: `src/okg/substrate/library/ontologies/<module>/schema.yaml`.
- Add the subtype as a LinkML `class` with `is_a:` parent (typically `Node` or an existing subtype).
- Register the subtype in the module's `module.yaml` under `subtypes:`.
- Declare narrowings in `cross_links/<module>_x_<other>.yaml` — hard-validated at compose.
- Validate locally: `okg ontology validate --module <module>` — must pass before publish.
- Project a test instance:
  - Easiest: add a `notes_dir` markdown file with frontmatter that maps to the new subtype, then `okg ingest --include notes_dir --inline`.
  - Or write a new source adapter (see okg-add-source).
- **Verify the instance landed** (AKMON):
  - `search(subtype_filter=<new_subtype>, limit=5)` returns ≥1.
  - `inspect(target={kind:"node", node_id:<sample>}, select=["subtype","attrs","provenance"])` shows correct shape.
  - `expand(input={node_id:<sample>}, edge_type=<new_edge>)` resolves expected neighbors.

## Output shape

Three sections:
1. **Schema diff**: the LinkML changes (class, slots, narrowings).
2. **Validation**: `okg ontology validate` output + `okg generation describe` for the first publish carrying the new type.
3. **First instance**: the node_id + attrs of the seed instance — proves end-to-end.

## Anti-patterns

- Don't add a subtype without declaring narrowings. Edges that aren't narrowed get rejected at insert as `narrowing_violation` — silent on the read side, fatal on the write side.
- Don't reuse an existing subtype "close enough" — once published, the conflation is hard to undo. Spend the extra schema work up front.
- Don't add attributes as `string` when you mean `datetime` or `uri`. Filters + range queries depend on declared types.
- Don't add a subtype without naming the source that emits it. Schema without producer is dead weight.
- Don't ship `is_a: Node` when a more specific parent (Person, Document, Org) exists — refinement loses cross-module narrowings if the parent is wrong.

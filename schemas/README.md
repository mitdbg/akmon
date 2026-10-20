# OKG authored-file schemas

JSON Schemas for every file a deployment author writes. Print one with
`okg schema show <file>`, for example `okg schema show deployment.yaml`.
`okg deployment bootstrap` validates the files it drafts and stages
against these schemas.

Coverage:

| File | Schema | Round-trip example |
|---|---|---|
| `deployment.yaml` | `deployment.schema.json` (generated) | `examples/deployment.yaml` |
| `bootstrap_plan.yaml` | `bootstrap_plan.schema.json` | `examples/bootstrap_plan/empty.yaml` |
| `source_registry.yaml` | `source_registry.schema.json` | `examples/source_registry.yaml` |
| `linker_config.yaml` | `linker_config.schema.json` | `examples/linker_config.yaml` |
| `aliases.json` | `aliases.schema.json` | `examples/aliases.json` |
| `invariants.yaml` | `invariants.schema.json` | `examples/invariants.yaml` |
| `schemas/classes.yaml` | `classes.schema.json` | `examples/classes.yaml` |
| `schemas/narrowings.yaml` | `narrowings.schema.json` | `examples/narrowings.yaml` |
| `plan.yaml` (reconcile) | `plan.schema.json` | `examples/plan.yaml` |
| a source manifest (`okg deployment add-source --manifest`) | `source_manifest.schema.json` | `examples/source_manifest.yaml` |

Each schema is draft 2020-12. Each has a matching `examples/*` file that
validates against it as a round-trip correctness check.

`deployment.schema.json` is generated from the key table in
`okg.substrate.deployments.keys`, which is also what the deployment loader
reads. Do not edit it by hand: change the table and run
`python scripts/gen_deployment_schema.py`. `--check` exits 1 when the
checked-in file is stale. The schema closes a nested block only where the code
that parses that block refuses unknown keys.

Check an example by hand:

```bash
python -c "import json, jsonschema, yaml; jsonschema.validate(yaml.safe_load(open('examples/deployment.yaml')), json.load(open('deployment.schema.json')))"
```

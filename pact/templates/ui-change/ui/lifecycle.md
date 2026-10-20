# Lifecycle — <change-id>

Per-component / per-token / per-pattern stage transitions and the evidence
backing each. Primer-style ladder.

## Stage ladder

| Stage | Allowed changes | Required to graduate |
|---|---|---|
| **experimental** | anything | proof of concept; lives outside the component library |
| **alpha** | breaking expected | tokens applied; a11y audited; docs+tests; no external runtime deps |
| **beta** | minor breaking with notice | shipped on 2+ surfaces; design review; perf baseline; complete usage guidelines |
| **stable** | additive only | API stable; complete usage guidelines; alternatives discouraged via tooling |
| **deprecated** | none; documented warnings | notice given; documented migration path (codemod when feasible) |
| **removed** | — | — |

## Transitions in this change

| Artifact | Kind | From | To | Evidence |
|---|---|---|---|---|
| `<Component>` | component | (new) | alpha | initial impl; tokens applied; axe passes; Storybook covers variants |

## Graduation evidence

For each promotion, document the evidence the ladder requires (surfaces,
design-review link, perf baseline, usage guidelines). Reviewers check it
against the ladder.

## Migration plan (only if deprecating/removing)

Phased rollout + the codemod path + the rollback path. Reference
pact/ui-change-guide.md § "Deprecation flow per artifact kind".

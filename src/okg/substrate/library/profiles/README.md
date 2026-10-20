# Substrate-library profiles

Profiles shipped with the substrate library. Each profile is a
turnkey-deployment recipe consumed by `okg install --profile <name>`.

Each profile defines:

- The ontology modules the deployment loads (`modules.yaml`).
- The init questions the operator answers at scaffold time.
- Default source-registry entries (with `${var}` interpolation
  from init answers, under `source-defaults/`).
- An invariants preset (`invariants.yaml`).
- A dashboards directory.
- Optional deployment defaults (`deployment-defaults.yaml`) merged into
  scaffolded `deployment.yaml` after scaffolder-owned keys are written.

Profile layout:

```
src/okg/substrate/library/profiles/<name>/
    profile.yaml            # required: schema + init questions
    modules.yaml            # required: top-level `modules:` list
    source-defaults/        # optional: per-source YAML defaults
        <source>.yaml
    invariants.yaml         # required: invariants preset (copied)
    dashboards/             # optional: dashboard YAMLs (copied)
    deployment-defaults.yaml # optional: deployment.yaml defaults
```

The schema is defined by
`src/okg/substrate/catalog/profile.py` (`Profile`,
`InitQuestion`); see that module's docstring for the canonical
schema reference.

`okg install --profile <name>` walks the substrate library, the cwd's
`./profiles/<name>/`, and `$OKG_PROFILES_DIR` to locate `<name>`;
in-tree library profiles are the default. Operators may also drop
deployment-specific profiles under cwd `./profiles/` (which is
typically gitignored — those are operator-local).

## Profiles shipped here

- **`_test/`** — minimal fixture used by the Layer 2A test suite.
  No init questions, two modules. Not for production use.
- **`academic-researcher/`** — installed with `okg install --profile academic-researcher`.
  Personal-OKG for an academic researcher or principal investigator:
  scholarly literature, code_graph, OpenSpec, bookkeeping,
  agent_sessions, the reasoning core, communications and calendar,
  plus the `academic_researcher` ontology module that adds
  lab-management subtypes (Advisee, Lab, Grant, Deadline, Committee,
  Review, Course, TalkEngagement, RecommendationLetter, ActionItem).
  Init questions cover ORCID + CV path + group webpage + advisees
  / grants / letters / calendar / notes dirs. Ships PI-tuned
  invariants (advisee floor, grant milestone overdue, letter
  request outstanding, service load threshold) and the pi-overview
  and researcher-overview dashboards. See
  `docs/use/profiles/academic-researcher.md`.
- **`minimal/`** — the default `okg install` profile (issue #3414). An
  empty starter graph for a team building its own domain graph: one small
  ontology module (`document_starter`), no sources, an empty first
  published version, and Nomos runtime enforcement off. Grow it with
  `okg deployment add-source`.
- **`workspace-graph/`** — Repository-centered engineering graph for one
  software workspace: git/code structure, OpenSpec/PACT, agent sessions,
  test and quality evidence, local compute context, invariants, dashboards,
  and deployment defaults. Use `okg install --profile workspace-graph` for new
  Workspace Graph deployments. The legacy `workspace-graph` template alias
  remains compatibility-only for older automation.

Retired names (`git-code`, `systems-research`, and `academic-pi` and
`cs-researcher`, both replaced by `academic-researcher`) live in `RETIRED_PROFILES`
in `deployment_bootstrap/profile_init.py`; `discover_profile` refuses them
with `profile_retired` before searching. Never reuse a retired name.

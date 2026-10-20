# `deployments/` — the framework's self-graph fixture

The framework repo's `deployments/` directory holds **project-internal** deployments only. The bulk of what used to live here moved to two other homes:

## Where things live now

| Tier | What | Where |
|---|---|---|
| **Framework + profile templates** | substrate code, profile starters (e.g. `academic-researcher`, `workspace-graph`) | this repository |
| **Reference deployments** | `cms`, `af_pubs`, `intersystems`, `memex`, `statgen-analysis` — fully-built domain showcases | a separate reference-deployments repository (not public) |
| **Operator state** | what `okg install` creates on each operator's machine | `${XDG_DATA_HOME:-$HOME/.local/share}/okg/deployments/<name>/` |

## What's in this directory

The framework's **project-internal** deployments — fixtures and self-graphs the repo's own tooling and tests build against this checkout. The benchmark deployments include `beaver-okg` and `synth-bench`; small contract fixtures such as `kubernetes-smoke` live alongside them.

## Operator workflow

You don't put your deployments here. To pull a reference deployment from the reference-deployments repository as a starting point, fetch the cache once, then instantiate by profile name:

```bash
okg fetch-deployments           # clone/update the reference-deployments repository into
                                # ~/.cache/okg/deployments/<source>/
okg install --profile cms \
  --deployment-name my-cms      # resolve the cached `cms` profile and scaffold
                                # a working deployment in your user state
```

`okg install --profile <name>` is the one documented way in and the single profile resolver: it searches your local profile dirs *and* the fetch cache. If the cache is empty it fails loudly (`profile_fetch_failed`) telling you to run `okg fetch-deployments` first — it never silently does nothing. The `init` verb still works and does the same thing; it is an alias that says so when you run it.

## Test workflow

If you're a framework dev and need to test against a known-shape deployment, use `OKG_DEPLOYMENTS_DIR=<some-tmp-path>` to point the substrate at a tmp dir, OR `OKG_USER_HOME=<some-tmp-path>` to relocate the whole user-state tree. Both env vars are honored by every deployment-resolving code path.

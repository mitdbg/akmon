---
audience: data owner, operator
type: overview
canonical_id: profiles-overview
section: use
visibility: primary
status: active
owner: profiles
---
# Choose a profile

A profile is a ready-made starting configuration for one kind of graph. It
picks the kinds of records and relationships the graph accepts, the sources
it suggests, the checks a new version must pass, and the questions setup asks.
You pick one with `okg install --profile <profile>`.

A profile is not a deployment. Installing a profile writes a deployment's
configuration files; the deployment is the actual graph, with its database,
sources, credentials and published versions. You can edit every generated
file before the first import.

## Current profiles

| Profile | Builds a graph of | What `okg install` does with it |
| --- | --- | --- |
| `minimal` (the default) | Nothing yet: an empty graph a team grows with its own sources and record types | Writes the configuration, migrates the database you name, and publishes an empty first version |
| [`workspace-graph`](../../operate/install.md) | One Git repository: files, code, commit history, tests, and local coding-assistant sessions | Writes the configuration, migrates the database you name, reads the repository, and publishes the first version |
| [`academic-researcher`](academic-researcher.md) | Publications, code, projects, grants, teaching, advising and group administration on one workstation | Starts a local PostgreSQL in Docker (or uses one you name), writes the configuration, and registers coding assistants; it imports no data until you approve each source |

`minimal` is the default when you give no `--profile`. It needs a deployment
name and a PostgreSQL database:

```bash
okg install --deployment-name <name> \
  --postgres-dsn <postgresql-dsn> --json
```

The first published version is empty. Add a folder of documents or a GitHub
repository with `okg deployment add-source`; see
[Connect your data](../sources/index.md). It adds the record types each source
needs.

`okg install --help` lists every profile. `academic-researcher` is the only
one with its own setup wizard; `minimal` and `workspace-graph` go through the
general installer, which takes every answer as a command-line flag. A flag
that belongs to the academic-researcher installer, such as `--orcid`, is
refused unless you pass `--profile academic-researcher`. None of these
profiles is a managed production service.

Every profile installs with Nomos runtime checks off (`nomos.runtime_enforcement:
off`), because a fresh deployment declares no access rules. Turn on `audit`
or `enforce` in `deployment.yaml` when it does; see
[Nomos](../../runbooks/security/nomos.md).

Four older profile names are refused with the error code `profile_retired`:
`git-code` (use `workspace-graph`, which ships the same sources),
`systems-research` (no replacement), and `academic-pi` and `cs-researcher`
(use `academic-researcher`, which replaces both).

The `_test` profile in the source tree is a fixture for the project's own
tests, not a user option.

## Preview an install

Start with `--dry-run --json`. It writes nothing and reports what the install
would do:

```bash
okg install --profile workspace-graph --deployment-name <name> \
  --repo-root <repo> --postgres-dsn <postgresql-dsn> --dry-run --json
```

Read `status` first. `dry_run_ok` means the plan is complete: the output lists
the files it would write and, in `would_migrate_and_publish`, whether it would
build the graph. `dry_run_blocked` means an input is missing or wrong:
`blockers` names it and `fix_hint` names the flag that supplies it.

Each setup question of a profile becomes a flag, and `okg install --help`
lists only the common ones. For example, `workspace-graph` also accepts
`--codex-sessions-root <dir>`. The blocker for a missing answer names its
flag.

## Pick by the graph you need

Before choosing, answer five questions:

1. **Questions:** what two or three questions must the graph answer? A profile
   whose record and relationship types cannot express them is a poor fit.
2. **Sources:** which exact accounts, repositories, folders or services are
   involved? A source a profile suggests is only a default.
3. **Record types:** do the profile's types name the things in your domain
   without forcing misleading categories? Each profile lists its graph
   modules in `modules.yaml` under
   `src/okg/substrate/library/profiles/<profile>/`.
4. **Checks:** what must block a new version, and what should only warn? A
   profile's thresholds are starting values.
5. **Ownership:** who owns the database, credentials, backups, upgrades and
   access policy?

Prefer the smallest profile that fits. Unused sources and record types add
review and operating work. If a close profile lacks one concept, extend its
graph model instead of adopting a larger, unrelated profile; see
[Profiles and ontologies](../../develop/extensions/ontologies.md).

## Check the result before using real data

Read the generated `deployment.yaml` and `source_registry.yaml`, and confirm
every source has the intended location, scope and credential reference. Then
run `okg doctor --deployment <name>`. A valid configuration proves its parts
agree; it does not prove a source is reachable or that its contents should be
imported.

After the first import, confirm with `okg status --deployment <name>` that a
version was published, and run a search pinned to it (see
[Query and trace the graph](../search/index.md)). A failed setup, import or
publish leaves the last published version unchanged.

If no profile fits, start from [Plan a deployment](../../operate/plan.md)
and choose only the graph modules and sources you need.

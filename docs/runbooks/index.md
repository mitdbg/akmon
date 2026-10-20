---
audience: operator, developer, maintainer, coding agent
type: overview
canonical_id: runbooks
section: runbooks
visibility: primary
status: active
owner: documentation
---
# Runbooks

A runbook is a bounded recovery or maintenance procedure for a known state. It
assumes you understand the deployment and operation involved.
For the product model, begin with [How AKMON works](../start/concepts.md). For
normal operating states and signals, begin with
[Operating AKMON](../operate/index.md).

For the maintained-knowledge terms the procedures use—working state, derived
result, conflict finding, complete computation, generation-pinned read—see
[How information becomes searchable](../start/data-lifecycle.md) and the
[glossary](../reference/glossary.md). The runbooks route an observed failure;
they do not redefine those lifecycle contracts.

## Establish the state before changing it

Record the deployment name, the command that failed, its complete structured
error, and the last known published `generation_id`. Check deployment health,
publication status, source freshness, queue depth, and dead-letter work. If the
problem involves one record, retain its source identity and trace result.

Capture the current state before retrying:

```bash
uv run okg generation list --deployment <deployment> --json
uv run okg metrics --qa-snapshot --deployment <deployment> --json
uv run okg doctor --deployment <deployment> --json
```

If publication failed, inspect the candidate generation named in the error:

```bash
uv run okg generation describe <generation-id> \
  --deployment <deployment> --json
```

The last published generation is the safe reader state. A failed import,
relationship build, or publication normally calls for repairing the failed
stage and retrying it—not deleting the database or forcing the published
pointer forward. Stop when the deployment cannot be identified, authority is
unclear, a command would affect more than the named deployment, or the
procedure's preconditions do not match the observed state.

## Start from the observed failure

| Observation | First distinction | Procedure |
|---|---|---|
| One expected record is absent | Was it absent at the source, rejected during import, left in working state, or missing only from the query? | [First-run and missing-record triage](troubleshooting/first-run.md) |
| Initial setup did not reach a usable deployment | Did configuration, database setup, source inspection, import, or publication fail first? | [The first run failed](troubleshooting/first-run.md) |
| A candidate exists but readers remain on an older generation | Which publication check or relationship stage blocked it? | [Publication keeps failing](troubleshooting/generation-blocked.md) |
| A source was healthy and then stopped changing | Is its schedule running, is the source position advancing, and can the adapter still read its scope? | [A source stopped updating](troubleshooting/source-sync.md) |
| Imported content is malformed or outside the intended scope | Is the bad information still working state, or was it already published? | [A source produced invalid information](troubleshooting/source-poisoning.md) |
| Pending work keeps growing | Is work merely slow, unable to start, retrying, or terminally failed? | [Runtime queue recovery](troubleshooting/runtime-queue.md) |
| Work exhausted its retries | What task input and error entered the dead-letter queue? | [Runtime queue recovery](troubleshooting/runtime-queue.md) |
| Publication cannot acquire its lock | Is another publisher alive, or is the lease stale? | [Publication is locked](troubleshooting/publisher-lock.md) |
| A graph-vocabulary change stopped midway | Which catalog version owns the branch and published generation? | [A catalog change was interrupted](troubleshooting/ontology-rollback.md) |
| Graph history or host storage is unhealthy | Is the problem retention policy, graph-history storage, or host capacity? | [Storage diagnosis and recovery](troubleshooting/storage.md) |

Each troubleshooting procedure names its stop conditions and expected recovery
signal. Recovery is complete only when the failed signal changes: the source is
current, pending work completes, a new generation publishes, or a pinned read
returns the expected record. A successful command without that observable
change is not evidence of recovery.

## Operate a deployment

Use these for deliberate administrative work rather than incident diagnosis:

- [Adopt an existing deployment](deployments/deployment-bootstrap.md) when the
  files and database already exist but this operator has not established their
  authority and state.
- [Control deployment authority](deployments/authority.md) to inspect producer
  scope, configure bounded automatic admission, or repair catalog ownership.

## Security procedures

- [Operate access policy](security/nomos.md) to inspect and change AKMON policy
  decisions.
- [Connect institutional identity](security/nomos-iam.md) to map an external
  identity system into those decisions.

Do not use a database or deployment recovery procedure to bypass an access
denial. Policy unavailability is a failure, not permission.

## Repository procedures

These procedures maintain this codebase and its contributor-only
Workspace Graph. They are not required to operate an unrelated AKMON
deployment.

- [Set up the Workspace Graph](workspace-graph/setup.md)

## Report an unrecognized deployment failure

When no recovery path matches, generate a redacted GitHub issue preview before
creating anything externally:

```bash
uv run okg deployment bug-report \
  --deployment <name> --repo <owner/name> \
  --summary "Short problem statement" --label bug \
  --json > /tmp/okg-bug-preview.json
```

Inspect the preview for credentials, database URLs, private content, local
paths, and unredacted logs. `--repo` is the issue repository; add
`--deployment-repo` only when the deployment belongs to a different repository.
Do not proceed when the target repository or redaction boundary is unclear.

After review, create the issue from the exact preview:

```bash
uv run okg deployment bug-report \
  --preview-json-file /tmp/okg-bug-preview.json --apply --json
```

Read back the returned repository, issue number, URL, and redaction report.
Close the incident only after the issue exists at the intended target and
contains enough generation, source, queue, doctor, and reproduction evidence
to continue diagnosis without exposing private state.

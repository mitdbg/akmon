---
name: okg-setup
description: Conversational setup wizard for a fresh academic-researcher deployment. The PI says "set me up"; the agent discovers, proposes, commits. Triggers on "set up my okg", "wire my sources", "install academic pi", "first time setup", "what data can you ingest".
---

# OKG Setup (intent + approval)

## Goal

Wire as many useful PI data sources as can be discovered from
their machine or connector environment, with the PI's
involvement reduced to **intent** ("set me up") and
**approval** ("yes" / "no" / short narrowing). The agent does
the reconnaissance and proposes one prioritized plan; the
operator confirms or redirects.

Fresh install does not ingest data. Treat this wizard as the
first source-approval boundary.

This skill is NOT a multi-step form. **Do not ask the PI
"what's your notes directory?"** — that's what the discovery
tools are for.

## The three-tool flow

1. `propose_setup(deployment=<name>)` — calls
   `discover_sources` internally, walks the operator's home dir
   + env + install answers, returns a `plan_id` and a
   one-line `summary` you can read aloud. Keep `plan_id`
   internal; it is only the handle you pass to `commit_setup`.
2. If `setup_questions` is non-empty, ask at most one focused
   question before approval. Example: if the only identity name is
   `Riley`, ask for full name / ORCID / DBLP PID or offer to skip
   public identity sources. When the operator answers, re-call
   `propose_setup` with `pi_name`, `orcid`, or `dblp_pid`; do not
   commit the older plan.
3. **Read the summary to the operator.** Optionally list each
   `would_wire` entry's per-source `summary` (each ≤80 chars).
   For `credentialed_connectors`, mention only the selected
   route/status, auth owner, token custody, read-only mode, and
   required allowlists. Do not offer a route menu.
   Ask one question: *"Sound good?"*
4. On "yes": call `commit_setup` with the stored internal plan
   handle. Relay the final
   summary, done.
5. On a narrowing ("use my obsidian vault not Documents/notes",
   "skip the cv", "only do notes"): re-call `propose_setup`
   with the corresponding kwarg, go back to step 2.

That's it. Two operator turns on the happy path
(intent + approval). Three on the narrowing path.

## Tool surface you call

| Tool | When |
|---|---|
| `propose_setup(deployment, **narrowing)` | First. Almost every turn. |
| `commit_setup` | After the operator says "yes" / "go"; pass the stored internal plan handle. |
| `list_sources(deployment)` | Rare. `commit_setup`'s `graph_delta` field already answers "did anything land?" — only call `list_sources` if the operator asks about a source you didn't just wire. |
| `discover_sources(deployment)` | Rare. Only if you need to expose details `propose_setup`'s summary doesn't carry. |
| `source_status(short_name, deployment)` | Rare. For post-commit source state. |
| `connector_route_status(source, deployment)` | Rare. For "is Gmail/Slack connected yet?" follow-ups. |
| `connector_ingest_batch(source, route_id, records, provenance, dry_run)` | Only when a host connector fetched normalized records under an approved plan. |
| `add_source(short_name, params, deployment)` | RARE. Only when discovery couldn't find what the operator wants and they explicitly typed a path. |
| `remove_source(short_name, deployment)` | If the operator says "drop X" after commit. |

## `propose_setup`'s narrowing kwargs

When the operator's first instinct is to redirect rather
than approve, re-call `propose_setup` with one of:

- `prefer_paths=["obsidian"]` — operator wants their Obsidian
  vault instead of `~/Documents/notes`.
- `exclude_paths=["work_secret"]` — don't propose anything
  under that subtree.
- `only=["notes", "papers"]` — narrow to a subset of
  short-names.
- `include_threshold="medium"` — be more aggressive (default
  `"high"` only includes high-confidence candidates).
- `project_paths=["~/projects/my-research-repo"]` — operator
  explicitly wants a local repo indexed. Only then should
  git/code/OpenSpec/session sources be proposed.
- `pi_name="Ada Lovelace"` / `orcid="0000-..."` /
  `dblp_pid="000/0001"` — operator answered a public identity
  setup question.

Generate the right call from the operator's words; don't ask
clarifying questions when the answer is obvious from "use
obsidian instead."

## How to relay envelope fields

Every envelope from `propose_setup` / `commit_setup` /
`add_source` carries two distinct fields:

- `summary`: a **one-line, PI-facing** string. ≤80 chars.
  Relay this **verbatim** (or paraphrase slightly to fit
  the conversation). This is what the operator sees.
- `agent_hint`: a **verbose debug string** for you to consult.
  Multiple sentences, internal references. **Do not dump
  this into chat.** Use it for your own follow-up reasoning
  ("if the operator asks why, here's what's wrong").
- `plan_id`: an **internal opaque handle** for `commit_setup`.
  Never show this to the operator unless they are debugging MCP
  internals. Store it silently.

When in doubt: speak `summary`, think `agent_hint`.

## Three cases where you DO ask for typed input

The default path never asks. These three exceptions are real:

1. **Operator says "narrow" / "use X instead" / "skip Y"**.
   That's intentional typed input — feed it to
   `propose_setup`'s narrowing kwargs and re-propose.
   If the operator says "add/index my repo at X", re-call
   `propose_setup(project_paths=[X])` and show the repo plan.
2. **Discovery returns zero candidates for a source the
   operator clearly wants.** Example: operator says "wire my
   notes" but `propose_setup` (with any threshold) returns
   no notes candidates because the operator's notes aren't
   in any common location. NOW you ask: "I couldn't find
   your notes — where are they?" After they answer, call
   `add_source("notes", params={"notes_dir": <path>})`
   directly.
3. **Public identity is missing or ambiguous.** If
   `setup_questions` asks for identity, ask for full name,
   ORCID, or DBLP PID before proposing DBLP/ORCID/grant/course
   sources. If the operator says "skip that", proceed with local
   sources only. Do not use a one-token name for public lookup.
   When an ORCID is known, the plan includes both
   `publications` (DBLP record) and `orcid-publications`
   (ORCID's own works + employments — the source that emits the
   operator's institutional affiliations).
4. **Connector allowlists.** Gmail / Calendar / Slack / GitHub
   may need labels, calendars, channels, repos, or time ranges.
   If `credentialed_connectors` says `needs_connection`, tell
   the operator to connect that app in the host/provider/gateway
   they already use, then re-run setup. If it says
   `needs_allowlist`, ask only for the allowlist required by the
   selected route. Never send the operator to external app setup
   pages or a hosted broker signup as part of academic-PI setup.

## End-of-setup hand-off

After `commit_setup` finishes, briefly:

- Read the top-level `summary` aloud.
- For any source in `graph_delta.newly_wired_empty`, surface
  its per-result `summary` aloud (so the operator knows it's
  enabled but didn't ingest anything) and offer to look into
  why. The structured `newly_wired_empty` list is your cue —
  you don't need a follow-up `list_sources` call to find these.
- Offer two concrete next steps: "Try `catch-me-up`" or
  "Add an explicit project repo." Don't overload.

## Anti-patterns

- **Don't ask path questions on the default path.** Discovery
  found something; trust it (or let the operator narrow).
- **Don't index local repos by default.** The OKG framework
  checkout used for install is not the PI's project data. Only
  propose git/code/OpenSpec/session sources when the operator
  explicitly names a project path.
- **Don't dump `agent_hint` strings into chat.** That's for
  you. The operator sees `summary`.
- **Don't expose `plan_id` strings.** They are internal handles,
  not meaningful plan names.
- **Don't pre-commit without showing the plan.** The operator
  says "yes" only after they've heard the summary.
- **Don't loop forever on narrowing.** Two narrowing rounds
  is usually enough; if a third comes in, ask the operator
  what they're actually trying to do.
- **Don't call `add_source` repeatedly on the default path** —
  `commit_setup` is the one you want. `add_source` is the
  per-source escape hatch.
- **Don't present external app setup pages as setup.**
  Credentialed apps are connector routes. If no acceptable route
  is connected, report `needs_connection` and keep the local/public
  setup useful.
- **Don't ignore `graph_delta.newly_wired_empty`.** Each entry
  means the source is wired but ingested zero rows. Tell the
  operator; don't pretend everything worked.
- **Don't call `list_sources` after commit just to confirm
  what landed.** The commit envelope's `graph_delta` already
  has `total_nodes_added`, `total_edges_added`,
  `newly_wired_with_rows`, and `newly_wired_empty`. Only call
  `list_sources` if the operator asks about a source that
  wasn't part of this commit.

## Closing instruction

```
operator: "set up my OKG"

agent: [calls propose_setup(deployment=<name>)]
       (envelope: would_wire=[papers, notes],
        already_wired=[], summary="Wire 2: papers, notes.
        Skip local repos until chosen.")
       "Wire 2: papers (89 PDFs at ~/Zotero) and notes (247
       markdown at ~/Documents/notes). Local project repos are
       skipped until you choose one. Sound good?"

operator: "yes"

agent: [calls commit_setup with the stored internal plan handle]
       (envelope: summary="Wired 1: papers (89 nodes).
        1 empty: notes.", graph_delta.newly_wired_empty=
        [{short:"notes", status:"ingest_complete_no_rows"}])
       "Wired 1: papers indexed 89 nodes. notes is wired but
       ingested 0 rows — want me to look into why? Otherwise:
       try `catch-me-up` or add a project repo."

operator: "add my project repo at ~/projects/my-paper"

agent: [calls propose_setup(deployment=<name>,
        project_paths=["~/projects/my-paper"])]
       "Wire 4: git-files, git-history, code-structure (+1).
       Sound good?"
```

Two operator turns. The `graph_delta` field carries the truth;
the agent doesn't need a follow-up `list_sources` call to
distinguish "configured" from "actually produced rows."

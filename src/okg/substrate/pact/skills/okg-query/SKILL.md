---
name: okg-query
description: Query the OKG (Operational Knowledge Graph) for durable engineering context — prior design, sessions, files, evidence, history. Use when the user says "what does the graph say", "query the okg", "find prior work on X", "okg query", or you need context the filesystem doesn't hold. The OKG is durable context, NOT live code truth — use git/files/tests for current state.
---

# Querying the OKG

The OKG projects this repo (PACTs, OpenSpec history, git, code, agent
sessions, docs, CI) into a published graph you can query. Every read is pinned
to a published `generation_id`.

## The query surfaces

- **CLI — text search + node trace:**
  ```bash
  okg search --deployment <deployment> --query "<text>" --method lexical --limit 10
  okg trace node "<node_id>"          # facts + live state + provenance
  ```
- **Shell-side graph inspection** through a Chronos checkout:
  ```bash
  OKG_DSN=<dsn> uv run python - <<'PY'
  import os, psycopg
  from psycopg.rows import dict_row
  from okg.substrate.graph_store import ChronosGraphStore
  with psycopg.connect(os.environ["OKG_DSN"], row_factory=dict_row) as conn:
      store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
      gen = store.latest_generation(deployment_name="<deployment>", status="published")
      graph = store.checkout_generation(gen.checkpoint_id)
      print(graph.query("SELECT subtype, count(*) AS n FROM okg.graph_nodes GROUP BY subtype ORDER BY n DESC LIMIT 20"))
  PY
  ```
- **Agents (MCP) — the AKMON operator surface:** `inspect`, `search`,
  `expand`, `filter`, `map`, `aggregate`, and `query` (the bounded read-only
  SQL escape hatch over the `okg.v_*` views). Omitted `generation_id`
  calls inherit the session's first pin, so related reads stay consistent.
  A reliable read:
  1. `search(query="<words>", subtype_filter="document_chunk", limit=10)`
     finds passages; path words match `document` and `source_file` nodes.
  2. Read the whole document in reading order:
     `inspect(target={"kind": "text", "node_id": "doc:<repo>:<path>"})`.
     The result is text: while its status line says `incomplete` and ends
     with `next: continuation_token=<token>`, call again with that token.
     `order_basis` says how chunks were ordered and `coverage` whether any
     are missing; each item's `heading_path` places it in its section. A
     `file:` node's text is only its path; inspecting it lists its `doc:`
     nodes under `documents`: read those. A one-hop `expand` returns linked records,
     not chunk text. `select` takes top-level fields (`attrs`).
  3. Stop once the answer is cited from graph text.

There is no `okg query` CLI verb. From a shell, raw SQL is `psql`; the CLI
read verbs are `okg search` and `okg trace`. The `query` operator is the MCP
tool, agent-facing only.

## What the graph is good for
- Mapping a PACT/task to likely files, symbols, sessions, tests.
- Finding prior attempts, failure patterns, handoff evidence.
- Design history — `pact_change` and `open_spec_change` nodes are read-only
  history (OpenSpec authoring is retired; query the graph for its history).

## Example: find prior attempts on a task
The agent_sessions source projects past runs, so you can see what was already
tried before you start:
```bash
okg search --deployment <deployment> --query "<task or symbol>" --subtype agent_session --limit 5
okg trace node "<agent-session-node-id>"   # transcript ref, files touched, outcome
```

## What it is NOT
Not a substitute for reading the current file, `rg`, live `git status`, or
focused tests. Always report the `generation_id` for graph evidence, and call
out stale / inferred / unavailable data explicitly.

See `docs/use/search/index.md` for the read surfaces.

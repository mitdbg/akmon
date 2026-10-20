# {{ deployment_name }} OKG

This deployment indexes `{{ repo_root }}` for coding-agent context: git
history, tracked files, code symbols, package manifests, OpenSpec changes
when available, test evidence, scanner artifacts, profile bundles,
document chunks, and matching Codex sessions.

## Environment

Run these commands from the OKG package checkout.

```bash
export OKG_DSN="postgres://postgres:okg@127.0.0.1:5433/okg"
export OKG_DEPLOYMENTS_DIR="{{ deployments_dir }}"
```

## Catalog

```bash
uv run --extra code --extra quality okg migrate --deployment "{{ deployment_name }}" --apply
uv run --extra code --extra quality okg catalog load --deployment "{{ deployment_dir }}" --apply
uv run --extra code --extra quality okg doctor --check sources --deployment "{{ deployment_name }}"
```

## Publish

```bash
uv run --extra code --extra quality okg ingest --deployment "{{ deployment_name }}" --progress
uv run --extra code --extra quality okg run --once --deployment "{{ deployment_name }}" --apply
uv run --extra code --extra quality okg status --deployment "{{ deployment_name }}"
uv run --extra code --extra quality okg doctor --check coding-context --deployment "{{ deployment_name }}"
```

When a publish completes, inspect the generation and metrics:

```bash
uv run --extra code --extra quality okg generation describe <generation_id>
uv run --extra code --extra quality okg metrics --qa-snapshot --deployment "{{ deployment_name }}"
```

## Aliases

Seed OpenSpec change aliases after the first publish if this project uses
OpenSpec:

```bash
uv run --extra code --extra quality okg alias seed-from-subtype --subtype open_spec_change --field change_id --apply
```

## MCP

Default lexical/alias MCP:

```bash
uv run --extra code --extra quality okg mcp-serve --deployment "{{ deployment_name }}"
```

Deployment entrypoint:

```bash
uv run --extra code --extra quality python "{{ deployment_dir }}/server.py"
```

## Optional Embeddings

Embeddings are disabled by default in `deployment.yaml`. Enable them only
when semantic search is needed: edit the canonical `embeddings` block to name
an explicit backend that actually passes the fixed 2560-dimensional capability
preflight. This repository ships no supported production backend recipe;
`hash` is for deterministic tests/development. Then run the
DBOS runtime worker so `okg_embed_batch` can populate missing node
embeddings:

```bash
uv run --extra code --extra quality okg runtime worker --deployment "{{ deployment_name }}"
uv run --extra code --extra quality okg mcp-serve --deployment "{{ deployment_name }}"
uv run --extra code --extra quality python "{{ deployment_dir }}/server.py"
```

## Updating a deployment made before a template change

`okg init` copies this template's files into the deployment, so later template
changes do not reach an existing deployment. To pick them up, edit your copies.

Python function bodies: under `chunker:` in `extractors.yaml`, add the Python
symbol chunker. Without it, a Python file's chunks hold only its docstrings.
The next ingest re-chunks every file, with no `--reset-cursor` needed:

```yaml
  - format: code
    class: okg.substrate.library.extraction.chunk.python_symbols.PythonSymbolChunker
    config:
      max_chars: 4000
      overlap: 400
```

Build and deploy files: in `source_registry.yaml`, copy the doc-corpus
`include_globs` entries for templates (`*.j2`, `*.tpl`, `*.tmpl`), Dockerfiles,
Makefiles and Jenkinsfiles, together with their `extension_to_format` routes,
from the current template. Files without an extension need the routes.

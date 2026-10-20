---
audience: operator
type: how-to
canonical_id: task-set-up-chat
section: operations
visibility: primary
status: active
owner: documentation
---
# Set up chat

Chat puts a web chat site in front of a published graph. Answers come from the
graph through its search tools. Set it up after
[installing a deployment](install.md) and publishing at least once.

You need a model provider: OpenAI, a local Ollama, or any server that speaks
the OpenAI API.

The chat container image is not published. Build it from this source
(`ops/openwebui/Dockerfile`, described in `ops/openwebui/README.md`), push
it to your registry, and set `chat.image` to
`<your-registry>/okg-openwebui-compat@sha256:<digest>`. Only the images
listed in `TESTED_IMAGES` in `src/okg/substrate/chat/instance.py` are
admitted, so add your build there too, mapped to `"0.11.4"`: the
Dockerfile builds on Open WebUI v0.11.4. `PINNED_VERSION` in the same
file is the version the chat measurements were taken on. The default in
this copy is a placeholder that cannot be pulled.

## 1. Write the chat settings

A new deployment has no chat settings, and every chat command refuses to run
without them. `okg chat setup` writes them for you. `--configure-only`
writes and checks the settings, then stops. It starts nothing and contacts
nothing:

```bash
okg chat setup --deployment <name> --provider openai --model gpt-4o-mini \
  --instance-port 3000 --configure-only
```

The flags:

- `--provider` is `openai`, `ollama` or `openai-compatible`. `openai` calls
  `https://api.openai.com/v1`. `ollama` calls Ollama on this machine through
  `http://host.docker.internal:11434/v1`. `openai-compatible` also needs
  `--provider-base-url`, the address the chat container uses to reach your
  server.
- `--model` is the model the provider serves. It becomes the site's only
  model.
- `--instance-port` is the port the chat site listens on. It must differ
  from the graph endpoint's port. The graph endpoint is `okg mcp-serve`,
  which answers the chat site's searches.
- `--provider-key-env` is the **name** of the environment variable that holds
  your API key. It defaults to `OPENAI_API_KEY` for `openai`. Only the name
  is saved; the key itself is read when the site starts. If you paste a key
  here by mistake, the command refuses and does not repeat what you typed.
- `--mcp-port` is the graph endpoint's port. You need it only when the
  install did not record one.

When `deployment.yaml` declares no search profiles, the command creates one
named `chat`. Without `--search-subtype`, it covers `document` and
`document_chunk`, the records a local folder produces. To search other types,
list every type you want with `--search-subtype`.

A graph built by [Install OKG](install.md) holds mostly files, code and
commits, so name those:

```bash
okg chat setup --deployment <name> --provider openai --model gpt-4o-mini \
  --instance-port 3000 --search-subtype source_file \
  --search-subtype code_symbol --search-subtype git_commit --configure-only
```

On `academic-researcher`, papers are `paper` records, so add them:

```bash
okg chat setup --deployment <name> --provider openai --model gpt-4o-mini \
  --instance-port 3000 --search-subtype paper --search-subtype document \
  --search-subtype document_chunk --configure-only
```

When `deployment.yaml` already declares search profiles, `--search-subtype`
is refused. Pass `--scope <profile>` to pick one; with a single
profile, or a `search.default_profile`, it is picked for you.

The report lists the record types chat will *not* search, so a scope that is
too narrow shows up before anything starts.

The command adds two blocks to `deployment.yaml` and writes a starter system
prompt to `chat/system_prompt.md`, which you can edit. For the first example,
on an install whose graph endpoint is port 5431, the blocks are:

```yaml
search:
  profiles:
    chat:
      subtypes:
      - document
      - document_chunk
chat:
  enabled: true
  ui:
    site_name: <name> graph
  models:
    builtin:
    - gpt-4o-mini
    default: gpt-4o-mini
  preset:
    display_name: <name> graph
    description: Answers grounded in the published graph.
    system_prompt_ref: chat/system_prompt.md
    suggestions:
    - What kinds of records does this graph hold?
  search_profile: chat
  mcp:
    port: 5431
    tools:
    - search
    - inspect
    - expand
    - filter
    - map
    - aggregate
    - query
  provider:
    base_url: https://api.openai.com/v1
    api_key_env: OPENAI_API_KEY
  instance_port: 3000
```

Running the command again changes nothing. It never overwrites existing chat
settings: a flag that disagrees with them is refused with
`chat_block_answer_conflicts`. To start over, delete the `chat:` block.

To check the settings without starting anything:

```bash
okg chat plan --deployment <name> --out <a new directory> --json
```

## 2. Start chat

Set these environment variables, then run the same command without the
settings flags:

- your provider key, in the variable named by `--provider-key-env`
  (`OPENAI_API_KEY` unless you changed it);
- `OKG_CHAT_APP_DATABASE_URL`, the database the chat site stores its own
  data in;
- `OKG_CHAT_WEBUI_SECRET_KEY`, a random secret for the site's sessions;
- `OKG_CHAT_ADMIN_PASSWORD`, the password for the site's admin account.

```bash
okg chat setup --deployment <name>
```

It starts the chat site, creates the admin account (or signs in to it),
registers the provider, starts the graph endpoint if it is not running, and
asks the site one question to prove the answer came through the graph. It
exits 0 only when that answer used the graph. On failure it names the step
that failed and what to fix.

The [CLI reference](../reference/cli.md#okg-chat-plan-sync-and-readback)
describes each step and its refusals.

## How the chat site reaches this machine

The chat site runs in a container. It needs the graph endpoint, which listens
only on this machine's `127.0.0.1`, plus the chat database and a local model
provider when those run here too. `--container-network` on `okg chat setup`
and `okg chat-instance up` picks how the container reaches them:

- `auto`, the default, picks from the platform and the container runtime,
  as below.
- On Linux with podman running on this machine, `auto` uses `pasta`. Podman
  forwards each port the site needs into the container, which dials it as
  `127.0.0.1:<port>`. Those ports are the graph endpoint's, the chat
  database's when it runs here, and the provider's and
  `chat.mcp.live_tools.url`'s when their address is `127.0.0.1`,
  `localhost` or `host.docker.internal`, plus setup's `--mcp-probe-url` and
  `--mcp-instance-url` ports when those are on this machine. The container
  reaches no other port on this machine, and nothing new listens on the
  network.
- On Linux with docker, or with a podman service on another machine, `auto`
  refuses and asks you to choose.
- `host` is never picked for you. The container shares this machine's
  network, so it can reach **every** service on this machine's `127.0.0.1`,
  including other deployments' graph endpoints and databases, not only the
  ones it needs. The chat site is always bound to `127.0.0.1`, and `--host`
  must stay `127.0.0.1`. It does not work with rootless docker, which is
  refused.
- On macOS, `auto` uses `gateway`. The container reaches this machine
  through `host.docker.internal`, as before.

On Linux, `host.docker.internal` inside the container means `127.0.0.1`, so
the `ollama` preset's `http://host.docker.internal:11434/v1` still works.
Under `pasta`, any other `host.docker.internal:<port>` address stops working
unless its port is one of the forwarded ports, for example a connection an
admin adds by hand in the chat site.

Changing the mode, or a port the container needs, means recreating the
container: run `okg chat-instance down`, then set up again. To keep an
instance started before this choice existed, pass
`--container-network gateway`.

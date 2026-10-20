---
audience: developer, operator
type: tutorial
canonical_id: task-first-result
section: get-started
visibility: primary
status: active
owner: documentation
---
# Evaluate AKMON from source

This tutorial builds a disposable graph from two local research records (one
describes a paper, the other its dataset), publishes it, and searches the
published version. Completing it verifies import, publication, and
version-pinned search for this checkout.

This tutorial runs AKMON from a source checkout and uses PostgreSQL. You need
[Git](https://git-scm.com/downloads),
[`uv`](https://docs.astral.sh/uv/getting-started/installation/), and
[Docker with Compose](https://docs.docker.com/engine/install/) or Podman
(see step 2).

## 1. Install AKMON

If you received AKMON as a source archive, unpack it and enter the directory it creates. The commands on this
page call that directory `akmon`:

```bash
tar xzf akmon-<version>.tar.gz
cd akmon
```

Otherwise, clone the repository into a directory named `akmon`:

```bash
git clone -b main https://github.com/mitdbg/akmon.git akmon
cd akmon
```

Every publish records the checkout's exact Git commit, so the directory must
be the top of its own Git repository with no uncommitted files. A clone is.
An unpacked archive is not, and without a commit, verification fails with
"outside a discoverable framework Git checkout". If
`git rev-parse --show-toplevel` does not print this directory (it fails, or
prints a parent repository the archive was unpacked into), make it one
before continuing. The `-c` options let the commit work on a machine with no
Git identity configured:

```bash
git init -b main
git add -A
git -c user.name="AKMON evaluator" -c user.email=evaluator@example.invalid \
  commit -q -m "AKMON source"
```

Then install the project:

```bash
uv sync --extra code --extra quality --extra dev
```

`uv` installs the required Python version and project packages. Keep all
remaining commands in the `akmon` directory. A later `uv sync` keeps only the
extras it names, so if you run another one (for example the development setup
in `CONTRIBUTING.md`), add `--extra code --extra quality` to it or the
packages this tutorial needs are removed.

Every code-structure source, even for a Python-only repository, loads
tree-sitter grammars when it starts. They download on first use (network
access needed) into a per-user cache (`~/.cache` on Linux, `~/Library/Caches`
on macOS). If your home directory is not writable, as on some shared or AFS
hosts, set `TREE_SITTER_LANGUAGE_PACK_CACHE_DIR` to a directory you own and
can write, not group- or other-writable, before ingesting. On Linux,
`XDG_CACHE_HOME` also moves it.

## 2. Start the sample database

Create names for the temporary files and database connection used by this
tutorial:

```bash
export OKG_QUICKSTART_ROOT="$(mktemp -d)"
export OKG_DEPLOYMENTS_DIR="$OKG_QUICKSTART_ROOT/deployments"
export OKG_QUICKSTART_DEPLOYMENT="$OKG_DEPLOYMENTS_DIR/akmon-quickstart"
export OKG_QUICKSTART_DSN="postgresql://postgres:okg@127.0.0.1:5433/okg"
mkdir -p "$OKG_DEPLOYMENTS_DIR"
```

`OKG_DEPLOYMENTS_DIR` is where commands look up a deployment by name. Set it
again in any new shell before using `--deployment akmon-quickstart`.

Make sure Docker is running, then start the repository's PostgreSQL service.
The first build downloads a large base image.

```bash
cd ops/pg
docker compose up -d --build
cd ../..
```

With Podman, run `podman compose up -d --build` instead (it needs
`podman-compose` or `docker-compose` installed), or run `podman-compose`
directly. Make the same substitution in every `docker compose` command on
this page. If `docker` is already Podman's alias (the `podman-docker`
package), the commands work unchanged.

The service listens on `127.0.0.1:5433`, in Compose project `okg-pg`, with
container `okg-pg`.
Do not use these commands with a shared database or an existing deployment.
If port `5433` or a container named `okg-pg` is already in use, start a
separate copy with its own project (so its data volume is separate),
container name and port, instead of the commands above. Keep the three
variables set for every `docker compose` command on this copy:

```bash
export COMPOSE_PROJECT_NAME=akmon-quickstart \
  OKG_PG_CONTAINER=akmon-quickstart-pg OKG_PG_PORT=5434
cd ops/pg
docker compose up -d --build
cd ../..
export OKG_QUICKSTART_DSN="postgresql://postgres:okg@127.0.0.1:${OKG_PG_PORT}/okg"
```

Every later command reads the database address from `OKG_QUICKSTART_DSN`
(the `--dsn` option or `--dsn-env OKG_QUICKSTART_DSN`), so nothing else
changes. To use an AKMON-ready PostgreSQL you already run, skip the service
and set `OKG_QUICKSTART_DSN` to an empty database on it.

The container starts with the PostgreSQL extensions but not the AKMON schema.
Install it:

```bash
uv run okg migrate --dsn "$OKG_QUICKSTART_DSN" --apply
```

## 3. Create the sample deployment

Copy the sample records and create a short description of the graph:

```bash
mkdir -p "$OKG_QUICKSTART_DEPLOYMENT/fixtures"
cp tests/fixtures/deployment_bootstrap/records.json \
  "$OKG_QUICKSTART_DEPLOYMENT/fixtures/records.json"

printf '%s\n' \
  '# Sample records' \
  'A paper and dataset record used to demonstrate import, publication, and search.' \
  > "$OKG_QUICKSTART_ROOT/brief.md"
```

Ask AKMON to inspect the sample and prepare a deployment:

```bash
uv run okg deployment bootstrap plan \
  --deployment "$OKG_QUICKSTART_DEPLOYMENT" \
  --name akmon-quickstart \
  --brief "$OKG_QUICKSTART_ROOT/brief.md" \
  --source records=fixtures/records.json \
  --dsn-env OKG_QUICKSTART_DSN \
  --apply --json

uv run okg deployment bootstrap inspect-source \
  --deployment "$OKG_QUICKSTART_DEPLOYMENT" records --apply --json

uv run okg deployment bootstrap draft \
  --deployment "$OKG_QUICKSTART_DEPLOYMENT" --apply --json
```

The inspection should identify `id` as the record identifier. The draft
command should report that it prepared deployment files. A non-zero exit or a
`bootstrap_*` error means the step failed; do not continue until it succeeds.

The verification publish records the exact deployment revision, so commit the
staged deployment before verifying it:

```bash
git -C "$OKG_QUICKSTART_DEPLOYMENT" init -b main
git -C "$OKG_QUICKSTART_DEPLOYMENT" add -A -- .
git -C "$OKG_QUICKSTART_DEPLOYMENT" commit -m "Stage AKMON quickstart deployment"
git -C "$OKG_QUICKSTART_DEPLOYMENT" status --short
```

## 4. Publish the graph

The sample has no changes that require review. An import with review-required
changes pauses until they are accepted or rejected.

The verification command imports the sample, creates its relationships, runs
the configured checks, and publishes a version:

```bash
uv run okg deployment bootstrap verify \
  --deployment "$OKG_QUICKSTART_DEPLOYMENT" \
  --dsn "$OKG_QUICKSTART_DSN" --apply --json
```

The command returns JSON. A successful result has `ok: true`,
`verification.status` equal to `verified`, a non-empty
`verification.generation_id`, and positive record and relationship deltas.
The top-level `status` describes the whole bootstrap, not this command: its
`stage` is `ready_to_apply` and its `next_action` names the apply command
below. The relevant fields have this shape (other fields omitted):

```json
{
  "ok": true,
  "verification": {
    "status": "verified",
    "generation_id": "<published-version>",
    "node_row_delta": "<positive integer>",
    "edge_row_delta": "<positive integer>"
  },
  "status": {
    "stage": "ready_to_apply",
    "next_action": {
      "command": "okg deployment bootstrap apply --apply"
    }
  }
}
```

The two input records expand into record, finding, document, and document-chunk
nodes plus supporting relationships. Exact deltas can change with the bootstrap
schema, but both must be positive. `verification.generation_id` identifies the
published version you will search. If verification reports a blocked
publication, use the generation identifier in its error details to inspect that
candidate before continuing:

```bash
uv run okg generation describe <generation-id> \
  --deployment akmon-quickstart --dsn "$OKG_QUICKSTART_DSN" --json
```

Verification tests the staged deployment and publishes the sample into its
database, but does not copy the staged configuration into the deployment
directory. Apply copies exactly the files just verified so later commands can
use the deployment name:

```bash
uv run okg deployment bootstrap apply \
  --deployment "$OKG_QUICKSTART_DEPLOYMENT" --apply --json

git -C "$OKG_QUICKSTART_DEPLOYMENT" add .
git -C "$OKG_QUICKSTART_DEPLOYMENT" commit -m "Apply verified AKMON quickstart deployment"
```

## 5. Search the published version

Replace `<generation-id>` with the identifier returned by verification:

```bash
uv run okg search \
  --deployment akmon-quickstart \
  --dsn "$OKG_QUICKSTART_DSN" \
  --query "salinity" --method lexical --limit 10 \
  --generation-id <generation-id> --json
```

The result includes the same `generation_id`, a matching
`pinned_generation_id`, and a `results` list. Each match includes its record
identity and source information. Keeping the query and generation identifier
together makes the result reproducible after a later publication while the
older generation is retained.

Search covers each record's body as well as its title. The draft declares the
record subtype searchable by `title` and `body` and writes each record's
description into `body`, so `--query "measurements"`, a word that appears only
in the descriptions, finds both records.

## Clean up

The next commands permanently delete the tutorial directory and the bundled
PostgreSQL container and volume. Run them only if you used the temporary path
and database from this tutorial. Do not run them for a shared deployment.

```bash
cd ops/pg
docker compose down -v
cd ../..
rm -rf "$OKG_QUICKSTART_ROOT"
```

If you started the separate copy, run `docker compose down -v` with the same
three variables still set; without them it stops the container named
`okg-pg`, which is your existing database. With them set, it removes only
that copy and its volume.

## Continue

- [Connect your data](../use/sources/index.md) to connect and import
  your own source.
- [Query and trace the graph](../use/search/index.md) in an existing deployment.
- [Plan a deployment](../operate/plan.md) before using real data
  or credentials.

---
audience: operator, developer
type: reference
canonical_id: security-model
normative: false
---

# Security and credentials

For graph exports that must preserve structure while removing identifying
values, see [Anonymization](../develop/architecture/privacy.md).


## One rule

**Never store secrets in the repository.** `deployment.yaml`,
`source_registry.yaml`, and every other authored file in a
deployment directory contain only env-variable references — never
raw credentials.

## Deployment DSN references

The substrate's primary secret is the Postgres DSN. `deployment.yaml`
files declare it through `postgres.dsn`, using `${VAR}`
interpolation:

```yaml
postgres:
  dsn: ${ACME_OPS_DSN}
  database: acme_ops
```

The substrate resolves the DSN at connection time by reading the
referenced environment variable. The DSN string (with its password) is never
written to disk by the substrate and never logged. Deployment status commands
report the raw reference, the required env var names, the authority
mode, and a redacted target.

`database` is part of the security contract. Deployment-scoped
commands compare the resolved DSN against the declared database
identity and refuse a mismatch instead of reporting success against
the wrong database. The resolver supports literal DSNs, but authored
deployment files should use env references.

`OKG_DSN` is not a process-wide fallback for deployment commands.
It is used only when the manifest explicitly references it, for
example `postgres.dsn: ${OKG_DSN}`. Commands that do not name a
deployment may still accept `--dsn` or `OKG_DSN` as direct operator
inputs.

## Where the env var comes from

The substrate does not prescribe a secrets manager. It requires only
that every variable referenced by `postgres.dsn` reaches the deployment
command, worker, publisher, or MCP server by startup time.
Three supported patterns, in order of robustness:

### 1. Managed secrets manager (production)

The operator binds the secret to the worker's process
environment from AWS Secrets Manager / GCP Secret Manager /
HashiCorp Vault / Kubernetes secrets + a sidecar.

Recommended binding per managed host:

| Host | Mechanism |
|---|---|
| AWS RDS + ECS/EKS | Secrets Manager secret → Task definition `secrets` block → env var |
| GCP Cloud SQL + Cloud Run | Secret Manager secret → `--set-secrets` → env var |
| Azure + AKS | Azure Key Vault Provider for Secrets Store CSI → env var |
| Self-hosted + systemd | systemd `EnvironmentFile=` pointing at a chmod-600 file outside the repo |

The DSN is rotated via standard managed-secrets rotation. The
substrate does not hot-reload a DSN change — worker restart is
required.

### 2. `.env` file outside the repo (dev, staging)

For local dev and single-host staging:

```sh
# ~/.okg/acme-ops.env (chmod 600, NOT in the repo)
ACME_OPS_DSN=postgres://postgres:okg@localhost:5432/okg
```

Load via `source ~/.okg/acme-ops.env && okg runtime worker
--deployment acme-ops` or via direnv. Never check in an
`.env.example` with a real value; templates contain env-var names
only.

Dogfood/self-hosted deployments can opt into local profile
authority with `postgres.self_hosted_authoritative: true`. In that
mode `.okg/local.env` for the deployment's main checkout wins over
a stale inherited process `OKG_DSN`, and `deployment env` reports
`authority.mode: worktree_main_local_env`.

### 3. `ops/pg/` compose for throwaway local (dev only)

`ops/pg/docker-compose.yaml` (started via `docker compose up` from
the `ops/pg/` directory) uses `POSTGRES_PASSWORD: okg` as a
hardcoded literal — **this is intentional for the local-dev path
and is not a production pattern**. Its README labels it "not a
production target".

## Role-level secrets

A database role is a PostgreSQL identity: a name, optionally a login
password, and the privileges granted to it. Of the processes in the table
below, only `okg mcp-serve` connects as one of the substrate's roles. The
publisher, the runtime worker and the setup commands connect as the user
written in `postgres.dsn`, unchanged.

`okg migrate` creates these roles when they are absent, as `NOLOGIN` roles
with no password, and leaves the attributes of an existing role alone:

- `app_rw`: reads, inserts and updates graph facts and runtime records,
  and owns no table. The MCP server's write connections use it when writes
  are enabled.
- `app_ro`: read-only. No process connects as it; the MCP `query` tool
  switches to it inside a transaction (see [Connection pooling](#connection-pooling)).
- `catalog_admin`: `okg doctor` checks that it exists. No `okg` command
  connects as it.
- `okg_mcp`: the least-privilege role the MCP server reads as.

`okg runtime bootstrap` adds three more `NOLOGIN` roles and grants them
privileges: `okg_substrate`, `okg_dbos`, and `okg_deployment_<name>`. No
`okg` command connects as any of them.

| Process | Database identity |
| --- | --- |
| `okg run --once` (the publisher) | the `postgres.dsn` user |
| `okg runtime worker` (the DBOS runtime worker) | the `postgres.dsn` user |
| `okg migrate`, `okg runtime bootstrap`, `okg catalog load`, `okg doctor` | the `postgres.dsn` user, or the `--dsn` value |
| `okg mcp-serve` | `okg_mcp` for reads; `app_rw` for writes, only when writes are enabled |

**The `postgres.dsn` user must own the substrate's tables.** The publisher,
whether `okg run --once` runs it directly or the runtime worker runs it as a
workflow, writes graph rows and also alters tables: it runs `ALTER TABLE
okg.catalog_versions ADD COLUMN IF NOT EXISTS ...`. PostgreSQL allows that
statement only for the table's owner, a member of the owning role (one that
inherits its privileges), or a superuser, even when the column already
exists. A less-privileged role in `postgres.dsn` does not work:

- As `app_rw`, both `okg run --once` and `okg runtime worker` stop at the
  readiness check with `database_readiness_unreadable` ("permission denied
  for table schema_migration_phases").
- As `okg_substrate` (after a `LOGIN` and a password are added by hand),
  `okg run --once` fails with "must be owner of table catalog_versions".

So do not expect the publisher or the worker to drop to a least-privilege
role. A superuser is not needed, and a superuser in `postgres.dsn` means
the publisher and the worker run as a superuser, which is best avoided. Use
a dedicated role without superuser that owns the tables: the role that ran
`okg migrate`. That role needs extra grants for the migration, listed in
[Install on a workstation](../use/profiles/academic-researcher.md#install-on-a-workstation),
plus `USAGE` on schema `cron` where `pg_cron` is installed. A login that is
a member of the owning role also publishes, with two differences on the
current code:

- The commit-witness trigger checks its writes (see below), and enricher
  writes during a publish are refused.
- Its first publish finishes with a `NotNullViolation` warning from the
  attribute integrity receipt. The published graph is correct, but the
  receipt stays unfinished, so every later publish runs the full integrity
  re-check instead of the fast one: slower, not wrong.

The commit-witness trigger `okg.enforce_commit_witness` refuses a new graph
fact whose `source_program_checksum` (the checksum of the program that
wrote it) is missing from the program ledger. It skips a superuser and the
table's owner, so it does not check writes made as the `postgres.dsn`
owner: such a write is stored even with an empty checksum. Source
admission sets the checksum before it writes. Enricher writes during a
publish currently store it empty.

`okg mcp-serve` is different. It takes the `postgres.dsn` connection
string, replaces the user, and connects as `okg_mcp` to read. With writes
enabled, it also connects as `app_rw`. An environment variable, when set,
replaces the derived string: `OKG_MCP_RO_DSN` for reads and `OKG_MCP_RW_DSN`
for writes. Without an override, and only for a database on `localhost`,
`127.0.0.1`, `::1` or a local socket, the server reads the role's connection
string from the owner-only secrets file described in
[Database role passwords](#database-role-passwords). It no longer falls back
to the fixed passwords earlier releases committed in source, unless
`OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1` is set. For any other host it refuses to
start and names the variable to set.

## Database role passwords

The framework logs in as `okg_mcp` (MCP reads) and `app_rw` (MCP writes). Earlier
releases created these roles, with `app_ro` and `catalog_admin`, with fixed
passwords written in the source, and `okg mcp-serve` used those passwords for
any local database. On a shared host every account could log in as these roles.
Now:

- The bundled Postgres first-boot scripts (`ops/pg/initdb/` and the
  academic-researcher profile's `initdb/`) create the roles without `LOGIN`.
- `okg db role-passwords --apply [--deployment <name>]` gives `okg_mcp` and
  `app_rw` `LOGIN` and a random password each, also replaces the password of
  `app_ro` and `catalog_admin` when they can already log in, and proves each
  login. It writes the role connection strings to
  `$XDG_CONFIG_HOME/okg/role-secrets/<server>-<port>.json`
  (default `~/.config/okg/...`; `OKG_ROLE_SECRETS_FILE` names another path),
  mode 0600 in a 0700 directory. The file is per server, not per database,
  because a role and its password belong to the whole server: every database
  on that server uses the same entries. It never prints a password, and it sends
  PostgreSQL only the SCRAM verifier, so DDL statement logging cannot record
  the password. Run it after `okg migrate --apply`, as the account that runs
  `okg mcp-serve`. It manages local databases only.
- The framework looks up a role's connection string in this order: the override
  variable (`OKG_MCP_RO_DSN`, `OKG_MCP_RW_DSN`), then the secrets file, then
  the built-in password if `OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1`. Otherwise it
  refuses with `role_password_not_provisioned` and names the command above.
  It refuses a secrets file that another account can read or write, that is
  a symlink, or that was written for a different server. `okg mcp-serve`
  proves the resolved password with one login before it starts and refuses
  with `role_password_rejected` when the server no longer accepts it.
- `okg runtime bootstrap` reads `okg_mcp`'s connection string from the same
  file when `OKG_MCP_RO_DSN` is unset, so a volume wipe that recreates the
  role without `LOGIN` is repaired from the recorded password.

`OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1` is for a single-user development machine
whose database no other account can reach. With it set, the framework derives the
built-in passwords again, and `okg db role-passwords --apply
--builtin-dev-passwords` sets them on a fresh database.

**Rotating an existing database.** A database created by an earlier release
still accepts the built-in passwords. `okg doctor` warns with
`builtin_role_password` for every login role that does, naming the fix:

```bash
uv run okg db role-passwords --apply --deployment <deployment>
uv run okg doctor --deployment <deployment> --json   # the warning is gone
```

`okg db role-passwords` without `--apply` reports the same roles without
changing anything. A role named by an override variable is left alone; rotate
it in your secrets manager. The role is one row shared by every database on
the server, so run the rotation once per server, as the account that serves
the deployments on it, and restart `okg mcp-serve` afterwards.

Supervised environment services (`okg environment service install`) manage
the same roles with their own credentials file and pass `OKG_MCP_RO_DSN` and
`OKG_MCP_RW_DSN` to the services they start. Do not run `okg db role-passwords
--apply` against a database those services use: it would change the passwords
under them. Rotate those through the environment service instead.

Production pattern:

- Put the DSN of the table-owning role (not a superuser) in the secrets
  manager and reference it from `postgres.dsn`:

```yaml
postgres:
  dsn: ${ACME_OPS_OWNER_DSN}
  database: acme_ops
```

- Give `okg_mcp` (and `app_rw`, if MCP writes are enabled) `LOGIN` and a
  randomly generated password. Store each password under its own key.
- Set `OKG_MCP_RO_DSN` (and `OKG_MCP_RW_DSN`) in the MCP server's
  environment. `okg runtime bootstrap` reads `OKG_MCP_RO_DSN` and sets
  that role's `LOGIN` and password to match. Do not put role passwords or
  role-specific DSNs in `deployment.yaml`.

## What the substrate logs

The substrate NEVER logs:

- DSN strings, even parsed (the password field is always
  redacted).
- Role passwords.
- Bearer tokens from source-adapter `params:` (those are also
  env-ref'd; see below).
- Any record field a deployment has marked sensitive (see "data
  classification" below).

What it DOES log, for audit:

- The env-var names referenced by `postgres.dsn` (not the values).
- The Postgres host/port/dbname (not the password).
- The role it connected as.

## Source-adapter secrets

A `local-folder` source needs no credential. A `github` source reads
through the GitHub CLI and reuses its login (`gh auth login`); AKMON copies
and stores no GitHub token.

A packaged reader that needs a secret takes the name of an environment
variable, `okg deployment add-source ... --credential NAME`, and stores only
that name. Keep tokens, passwords and database connection strings out of
`--setting` values and out of `source_registry.yaml`. `add-source` refuses a
setting whose name looks like a secret, such as one ending in `token` or
`password`. The check reads only the name, so it does not catch a
connection string stored under a plain name such as `dsn`. See
[Connect your data](../use/sources/index.md#credentials).

## Data classification

Data classification is set in the `nomos:` block of `deployment.yaml`.
`nomos.deployment_posture.allowed_data_classifications` lists the classes the
deployment accepts, and each source's `source_policy.data_classification`
names that source's class. When a source's class is not in the allowed list,
the policy audit reports `nomos.audit.data_classification_not_allowed` and
ingestion stops. There is no top-level `data_classification` key: the manifest
loader refuses unknown top-level keys, so adding one makes every command that
loads the deployment fail. Keeping the local development
database in `ops/pg/` away from production data is the operator's job.

## Connection pooling

A process keeps one connection pool per connection string. Which pools it
opens depends on the process:

- The publisher and the runtime worker share one pool on the
  `postgres.dsn` connection string, and open their direct connections with
  the same string. `runtime.okg_read_pool_size` in `deployment.yaml` sets
  the pool's width. The runtime worker's DBOS engine also connects with that
  string.
- `okg mcp-serve` opens an `okg_mcp` read pool. `okg_mcp` is the dedicated
  least-privilege MCP role. It is NOT a superuser; it holds SELECT across the okg + public
  (Chronos) relations the read surface needs, membership in
  `app_ro` (so the bounded `query` path's internal `run_sql` helper can
  apply its `SET LOCAL ROLE app_ro` downgrade), and writes only to
  runtime records listed in `okg.grant_registry`: the MCP audit and
  telemetry rows, operator call samples (append-only) and their retention
  watermark, Nomos decision and claim upserts, and the external probe cache
  that `inspect(node, hydrate)` overwrites — no fact-table writes, no DDL. It has no privilege on
  `okg.connection_reservations`. Each MCP server records, refreshes and
  releases its own connection reservation through three SECURITY DEFINER
  functions (`okg.connection_reservation_claim`, `_heartbeat`,
  `_release`). They act only on the row whose random holder token the
  calling process presents. They refuse a claim above the MCP maximum or
  beyond the server's connection budget. They sweep only claims idle for
  more than 300 seconds. A custom `OKG_MCP_RO_DSN` role gets this by being
  an inheriting member of `okg_mcp`. The token travels as a bound
  parameter, so `pg_stat_activity` does not show it. A server that logs
  statement parameters writes it to the server log, which only an
  administrator can read. The functions are owned by the NOLOGIN role
  `okg_connection_reservation_owner`, which the first migration creates.
  Roles are cluster-wide, so it outlives any database dropped from that
  cluster. If an MCP server's claim is lost (swept after a laptop sleep,
  or a long database outage), the server logs it and claims again rather
  than exiting, retrying every heartbeat interval. If the claim no longer
  fits the connection budget, the server keeps serving without a recorded
  claim, so other starts count its connections as free. It keeps retrying
  until the claim fits. It warns once with the fix, then at most every 10
  minutes, and logs the recovery. The MCP read path
  (`inspect`/`search`/`query`) reads the physical Chronos tables
  through a checkout, which `app_ro` (view-only) cannot serve.
- Writes from the MCP surface (`add_source`, `commit_setup`,
  `propose`, `record`, `connector_ingest_batch`) are
  operator-gated: disabled unless the server is launched with
  `--allow-writes` / `OKG_MCP_ALLOW_WRITES=1`, and then run over
  a SEPARATE `app_rw` pool. The agent cannot self-grant — a
  tool-call `authority.allow_write` is ignored. academic-researcher
  registers its MCP server write-enabled (setup is agent-driven);
  other deployments default read-only.
- No process opens a pool as `catalog_admin`. `okg catalog load` connects
  as the `postgres.dsn` user.

Only the MCP server's pools use derived role connection strings (`okg_mcp`
for reads, `app_rw` for writes, or `OKG_MCP_RO_DSN` / `OKG_MCP_RW_DSN` when
set). The publisher, the runtime worker and the setup commands use the
`postgres.dsn` user; see [Role-level secrets](#role-level-secrets).

## `okg doctor` secret checks

`okg doctor` runs these checks:

1. `postgres.dsn` exists and all referenced env vars resolve (if
   not, `configure_okg_dsn` is the fix action).
2. The resolved DSN points at the manifest's declared `database`
   identity, or an allowed isolated clone.
3. The resolved DSN reaches a host.
4. The roles `app_rw`, `app_ro` and `catalog_admin` exist (it reports
   `role_missing` otherwise). It does not test their passwords.
5. None of the roles `okg_mcp`, `app_rw`, `app_ro` and `catalog_admin` is
   a superuser (one that is, is a misconfiguration). It does not check the
   `postgres.dsn` user, which the publisher and the runtime worker run as.
   Reports
   `role_superuser` per role and refuses with `role_has_superuser`
   (with an `ALTER ROLE … NOSUPERUSER` fix hint). Implemented in
   `cli/doctor.py`.

Under `OKG_AGENT=1`, each check's output includes a fix hint per
`docs/reference/error_codes.yaml`.

## Unsupported security features

- **Automatic secret rotation.** Operators rotate via their
  managed-secrets manager; the substrate restarts workers to pick
  up rotated values.
- **Encryption at rest beyond PG defaults.** Deployments requiring
  column-level encryption (PHI, export-controlled) use
  `pgcrypto` at their discretion; the substrate does not ship a
  wrapper.
- **TLS enforcement.** The substrate passes through the DSN's TLS settings; it
  does not require `sslmode=verify-full`, and `okg doctor` does not enforce a
  deployment TLS policy. Put the required `sslmode` in the deployment's DSN
  and verify it through the database connection policy.

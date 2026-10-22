---
audience: user, data owner, operator
type: how-to
canonical_id: task-evaluate-akmon
section: operations
visibility: primary
status: active
owner: documentation
---
# Plan a deployment

Before AKMON reads real data, decide where it will run, who will operate it,
and which systems it may access. The [Quickstart](../start/quickstart.md)
builds a first graph; it does not answer these production questions.

## Requirements

- Linux or macOS, with Python 3.12 or 3.13 (`uv` installs it).
- PostgreSQL 17 with the extensions `timescaledb` (2.15 or later), `vector`
  (0.7 or later), `pg_textsearch` (1.0 or later; `okg doctor` recommends
  1.5.1), `pg_trgm`, `fuzzystrmatch` and `btree_gist`, with `timescaledb` and
  `pg_textsearch` in `shared_preload_libraries`, and a database user that can
  create roles and set their passwords. `pg_cron` is optional: AKMON no longer
  schedules work with it, and the install skips it when it is absent. The
  published image `ghcr.io/mitdbg/akmon-postgres:v<version>` (or a build of
  `ops/pg`), run with Docker or podman, has all of it.
- For upgrades, restores and copies: `pg_dump` and `pg_restore` at the
  server's major version or newer.
- Disk: about 10 GB to start (the database image is about 6.5 GB on disk); a graph's
  own size grows with its sources and kept history, see
  [Storage and history](../runbooks/troubleshooting/storage.md).
- For agents on other machines: one port for the HTTP MCP server (8100 by
  default), behind a TLS proxy; the server itself speaks plain HTTP.

## Choose where AKMON will run

| Mode | Use it for | Important limit |
|---|---|---|
| Local example | Learning and smoke tests | No production data, credentials, or network exposure. |
| Local profile | A private workstation or small team trial | The operator owns backups, upgrades, secrets, and access controls. |
| Shared deployment | A team or service | Add TLS at the boundary, managed secrets, backups, retention rules, and a documented operator. |
| Production deployment | A maintained service | Complete the deployment readiness, privacy, recovery, and security review before importing data. |

## Decide before connecting a source

- What systems may AKMON read, and which records are in scope?
- Which data is public, internal, restricted, or regulated?
- Where are credentials stored, and who can rotate them?
- Is traffic protected with TLS at every network boundary?
- Who owns encryption at rest, backups, retention, restore tests, and source
  retirement?
- Which publication checks are technical, and which domain decisions remain
  with the data owner?
- Does the chosen profile match the intended data and operating model, and have
  a documented installation path?

The [security reference](../reference/security.md) explains the current
responsibilities and the limits AKMON does not enforce for you. Read it before
[connecting a source](../use/sources/index.md#connect-the-source).

## Know what publication means

Publication proves the configured checks passed. It does not prove that a
source is scientifically, legally, or operationally correct unless an
explicit domain rule says so. Preserve source evidence and the published
version with any decision or analysis.

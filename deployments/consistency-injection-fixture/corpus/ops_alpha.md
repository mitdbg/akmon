# Alpha Service

## Deployment
Version: 4.2
Memory: 512 MB
Released: 2026-03-14
Certified: 2026-01-15
Uptime: 99.9
Endpoint: https://svc.example/api/v1/alpha
Primary node: DB-01
Backup node: cache01
Runbook: RB-7
Upgrade: planned
Status: active

## Configuration
retry_limit: 5
timeout_s: 30

## Burst mode
timeout_s: 15

## Procedure
1. Stop the ingest daemon.
2. Apply the schema migration.
3. Restart the ingest daemon.
4. Clear the local cache if disk pressure is high.

## Metadata
Reviewed: 2026-05-01
Owner: alpha-team

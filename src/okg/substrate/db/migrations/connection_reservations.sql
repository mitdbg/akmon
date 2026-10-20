-- Host-wide connection reservations.
--
-- Per `unify-connection-budget` req.boot-admission: the per-process tier can
-- compute its own declared demand at boot and refuse if it does not fit, but
-- it CANNOT compute the host total. The MCP server is a separate process
-- started per agent session, several deployments' workers can share one
-- Postgres, and none of them tell each other. A worker that assumes it is
-- alone is wrong under exactly the agent concurrency the budget exists to
-- protect; a worker that assumes the worst case refuses valid deployments.
--
-- Observing `pg_stat_activity` is not sufficient on its own: it shows what is
-- CONNECTED now, so two processes that have declared a width but not yet
-- opened their pools can both be admitted against the same free connections.
-- A reservation is a claim recorded BEFORE the connections are opened, which
-- is what closes that race.
--
-- `application_name` is the key because every substrate pool already stamps
-- one (`okg-pool`, `okg-llm-telemetry`, `okg-projector-batchN`, the MCP
-- server's own), so a refusal can name the holder rather than only the
-- shortfall. `pid` disambiguates two processes sharing a name and lets a
-- stale row be recognised.
--
-- Staleness matters more than exactness here: a crashed worker must not hold
-- its claim forever, or the restart of that same worker is refused against
-- its own corpse. `heartbeat_at` is refreshed by the live process and rows
-- older than the caller's staleness horizon are ignored (and opportunistically
-- deleted) rather than trusted.
--
-- Idempotent: re-applying the migration is a no-op.

CREATE TABLE IF NOT EXISTS okg.connection_reservations (
    application_name  TEXT        NOT NULL,
    pid               INTEGER     NOT NULL,
    deployment        TEXT,
    declared_demand   INTEGER     NOT NULL CHECK (declared_demand >= 0),
    claimed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (application_name, pid)
);

-- The admission read is "sum every non-stale reservation", so the heartbeat is
-- the selective column.
CREATE INDEX IF NOT EXISTS connection_reservations_heartbeat_idx
    ON okg.connection_reservations (heartbeat_at);

COMMENT ON TABLE okg.connection_reservations IS
    'Host-wide connection claims recorded before pools open, so concurrent '
    'boots cannot each be admitted against the same free connections. Keyed '
    'by (application_name, pid); rows staler than the caller''s horizon are '
    'ignored so a crashed process does not hold its claim forever.';

-- Divergence probe (TEST-ONLY): the convergence lane's non-vacuity
-- mutation. Injected through the ordered-migration seams into the
-- RESTORED side's migrate() only, so restored+migrated deliberately
-- stops matching a fresh install — the lane must go red on it, proving
-- the diff engines actually detect drift. Never ships in
-- src/okg/substrate/db/migrations/.
CREATE TABLE IF NOT EXISTS okg.convergence_divergence_probe (
  probe_id   BIGINT PRIMARY KEY,
  planted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

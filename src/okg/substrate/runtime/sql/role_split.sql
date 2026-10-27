-- Postgres role split for the DBOS runtime layer.
--
-- Per `rebuild-substrate-on-dbos` Decision 12 and Phase 1 §2.2.1.
-- Three roles per deployment-database, with belt-and-suspenders
-- enforcement of the substrate / runtime / deployment-author boundary:
--
--   * okg_substrate          — full write on OKG schemas. Used by
--                              Publisher, source-sync runner, bootstrap.
--   * okg_dbos               — write privileges only on the configured
--                              DBOS schema (default `dbos`). Used by
--                              the DBOS runtime itself for workflow
--                              state, step outputs, queue rows.
--   * okg_deployment_<NAME>  — substrate-aware deployment role.
--                              INSERT on okg.node_facts / okg.edge_facts.
--                              Column-level UPDATE (valid_to_gen) on
--                              legacy okg.nodes_live / okg.edges_live when
--                              those tables exist, so bitemporal close works
--                              through the SDK on legacy deployments.
--                              SELECT on okg.* and okg_derived.*.
--                              NO writes on substrate-only state
--                              (graph_generations, source_watermarks,
--                              catalog_*, narrowings, dbos.*).
--
-- The legacy column-level UPDATE on (valid_to_gen) is load-bearing per
-- the Phase 0 review's F11 finding: without it, a deployment that
-- bypasses the SDK can mutate other columns of nodes_live/edges_live,
-- defeating the boundary. Chronos-native deployments dropped those
-- tables; for them the deployment role keeps SELECT plus append-only
-- fact inserts and receives no graph-row UPDATE privilege.
--
-- Substitute :deployment_name and :dbos_schema with deployment-specific
-- values via psycopg parameter binding. This file is a template, not a
-- standalone script — the Python wrapper at
-- src/okg/substrate/runtime/bootstrap.py:apply_role_split formats and
-- executes it against the deployment's DSN.

-- =====================================================================
-- 0. Drift-recovery REVOKEs. Re-running this script must converge the
--    privilege graph to the policy below, even when an operator has
--    granted extra privileges out-of-band. We REVOKE everything the
--    role-policy disallows *before* re-granting what the policy allows.
--
--    These REVOKEs are deliberately broad — they're idempotent
--    (REVOKE of a privilege the role doesn't have is a no-op with a
--    NOTICE) and they make the role's effective privilege set match
--    the policy regardless of how it drifted.
--
--    A separate concern is role-membership drift (e.g. someone ran
--    `GRANT okg_substrate TO okg_dbos`). Memberships are not revoked
--    here because they could be legitimate operator memberships; the
--    provenance-split audit surfaces them as diagnostics so an
--    operator can decide.
-- =====================================================================

-- okg_dbos must not have any privileges on okg.* / okg_derived.*.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_dbos') THEN
        REVOKE ALL ON ALL TABLES IN SCHEMA okg, okg_derived FROM okg_dbos;
        REVOKE ALL ON ALL SEQUENCES IN SCHEMA okg, okg_derived FROM okg_dbos;
        REVOKE ALL ON SCHEMA okg, okg_derived FROM okg_dbos;
        -- Materialize-defaulted future grants: drop the default policy
        -- regardless of which role owns the schema. ALTER DEFAULT
        -- PRIVILEGES is keyed by the granting role; the per-role
        -- form requires running the REVOKE as that granting role,
        -- which a generic bootstrap can't safely do. Operators who
        -- discover lingering default-privilege grants can re-run as
        -- the granting role; the audit will flag the materialized
        -- privileges either way.
    END IF;
END
$$;

-- Per-deployment role: must not have any privileges on the DBOS schema
-- or substrate-only OKG tables. We do these in section 5 below where
-- we have the role name in scope.

-- PUBLIC must not have writes on OKG / DBOS schemas. We do these here
-- since they don't depend on a deployment name.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA okg, okg_derived FROM PUBLIC;

-- =====================================================================
-- 1. Role creation. Idempotent via DO blocks (CREATE ROLE has no
--    IF NOT EXISTS clause).
-- =====================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'okg_substrate'
    ) THEN
        CREATE ROLE okg_substrate;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'okg_dbos'
    ) THEN
        CREATE ROLE okg_dbos;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
         WHERE rolname = format('okg_deployment_%s', :'deployment_name')
    ) THEN
        EXECUTE format(
            'CREATE ROLE %I',
            format('okg_deployment_%s', :'deployment_name')
        );
    END IF;
END
$$;

-- =====================================================================
-- 2. Schema-level grants for okg_substrate.
--    Full write on okg.* and okg_derived.*.
-- =====================================================================

GRANT USAGE  ON SCHEMA okg, okg_derived TO okg_substrate;
GRANT CREATE ON SCHEMA okg, okg_derived TO okg_substrate;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA okg, okg_derived TO okg_substrate;
GRANT USAGE, SELECT, UPDATE
    ON ALL SEQUENCES IN SCHEMA okg, okg_derived TO okg_substrate;

ALTER DEFAULT PRIVILEGES IN SCHEMA okg, okg_derived
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO okg_substrate;
ALTER DEFAULT PRIVILEGES IN SCHEMA okg, okg_derived
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO okg_substrate;

-- =====================================================================
-- 3. Schema-level grants for okg_dbos.
--    Full write only on the configured DBOS schema.
-- =====================================================================

DO $$
DECLARE
    dbos_schema text := :'dbos_schema';
BEGIN
    EXECUTE format(
        'GRANT USAGE, CREATE ON SCHEMA %I TO okg_dbos',
        dbos_schema
    );
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE, DELETE
             ON ALL TABLES IN SCHEMA %I TO okg_dbos',
        dbos_schema
    );
    EXECUTE format(
        'GRANT USAGE, SELECT, UPDATE
             ON ALL SEQUENCES IN SCHEMA %I TO okg_dbos',
        dbos_schema
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I
             GRANT SELECT, INSERT, UPDATE, DELETE
             ON TABLES TO okg_dbos',
        dbos_schema
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I
             GRANT USAGE, SELECT, UPDATE
             ON SEQUENCES TO okg_dbos',
        dbos_schema
    );
END
$$;

-- okg_dbos must NOT have any privileges on okg.* / okg_derived.*.
-- Asserted by the provenance-split audit in Phase 1 §2.5.

-- =====================================================================
-- 4. Per-deployment role: okg_deployment_<NAME>.
--    SELECT on everything, narrow INSERT/UPDATE per the boundary spec.
-- =====================================================================

DO $$
DECLARE
    role_name text := format('okg_deployment_%s', :'deployment_name');
    grant_target text;
BEGIN
    -- Schema usage is required to access any tables.
    EXECUTE format(
        'GRANT USAGE ON SCHEMA okg, okg_derived TO %I',
        role_name
    );

    -- Read everywhere: deployment workflows query the substrate.
    EXECUTE format(
        'GRANT SELECT
             ON ALL TABLES IN SCHEMA okg, okg_derived TO %I',
        role_name
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA okg, okg_derived
             GRANT SELECT ON TABLES TO %I',
        role_name
    );

    -- ...except the PACT ledger. The blanket GRANT above is scoped to a
    -- SCHEMA, so when schema-namespace-qualification moved pact_decisions /
    -- pact_intents / pact_coordination_events out of `public` and into `okg`
    -- it swept them into every deployment role's read set for free. That is a
    -- widening nobody decided: the ledger holds operator verdicts and the
    -- worktree-claim read model, which a deployment workflow has no business
    -- reading, and it was NOT readable before the move. Take it back
    -- explicitly, here, rather than let a namespace change quietly redraw a
    -- privilege boundary.
    FOR grant_target IN
        SELECT unnest(ARRAY[
            'okg.pact_decisions',
            'okg.pact_intents',
            'okg.pact_coordination_events'
        ])
    LOOP
        IF to_regclass(grant_target) IS NOT NULL THEN
            EXECUTE format(
                'REVOKE ALL ON %s FROM %I', grant_target, role_name
            );
        END IF;
    END LOOP;

    -- Insert on the fact tables. node_facts and edge_facts exist in
    -- the current schema; escalation_facts and the cost / correlation
    -- ops tables are added in Phase 2 with the SDK build. Grants for
    -- the Phase 2 tables are conditional on existence so this script
    -- is safe to apply on a Phase-1 substrate as well.
    EXECUTE format(
        'GRANT INSERT ON okg.node_facts, okg.edge_facts TO %I',
        role_name
    );

    -- escalation_facts — append-only fact log.
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relname = 'escalation_facts'
    ) THEN
        EXECUTE format(
            'GRANT INSERT ON okg.escalation_facts TO %I',
            role_name
        );
    END IF;

    -- deployment_cost_ledger — append-only; consume() inserts a row.
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relname = 'deployment_cost_ledger'
    ) THEN
        EXECUTE format(
            'GRANT INSERT ON okg.deployment_cost_ledger TO %I',
            role_name
        );
    END IF;

    -- deployment_cost_caps — UPSERT on cost_budget context entry /
    -- extend(). INSERT + UPDATE table-level (no column restriction
    -- since the only mutable column is the cap value itself).
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relname = 'deployment_cost_caps'
    ) THEN
        EXECUTE format(
            'GRANT INSERT, UPDATE ON okg.deployment_cost_caps TO %I',
            role_name
        );
    END IF;

    -- deployment_correlations — UPSERT on correlate_workflow().
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relname = 'deployment_correlations'
    ) THEN
        EXECUTE format(
            'GRANT INSERT, UPDATE ON okg.deployment_correlations TO %I',
            role_name
        );
    END IF;

    -- Column-level UPDATE on (valid_to_gen) only — bitemporal close
    -- through SDK helpers, no other column mutation. Chronos-native
    -- deployments no longer have nodes_live / edges_live, so guard
    -- these grants by table+column existence.
    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'okg'
           AND table_name = 'nodes_live'
           AND column_name = 'valid_to_gen'
    ) THEN
        EXECUTE format(
            'GRANT UPDATE (valid_to_gen) ON okg.nodes_live TO %I',
            role_name
        );
    END IF;
    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'okg'
           AND table_name = 'edges_live'
           AND column_name = 'valid_to_gen'
    ) THEN
        EXECUTE format(
            'GRANT UPDATE (valid_to_gen) ON okg.edges_live TO %I',
            role_name
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relname = 'escalation_live'
    ) THEN
        EXECUTE format(
            'GRANT UPDATE (valid_to_gen) ON okg.escalation_live TO %I',
            role_name
        );
    END IF;

    -- Sequence usage for SERIAL/BIGSERIAL columns on the fact tables.
    EXECUTE format(
        'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA okg TO %I',
        role_name
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA okg
             GRANT USAGE, SELECT ON SEQUENCES TO %I',
        role_name
    );
END
$$;

-- =====================================================================
-- 5. Negative grants — explicit revokes that backstop the boundary.
--    These run unconditionally on every re-apply so the script
--    converges the per-deployment role's effective privileges to the
--    policy, regardless of drift. REVOKE is idempotent (a REVOKE of a
--    privilege the role doesn't have emits a NOTICE and is a no-op).
-- =====================================================================

DO $$
DECLARE
    role_name text := format('okg_deployment_%s', :'deployment_name');
    dbos_schema text := :'dbos_schema';
    catalog_table text;
BEGIN
    -- No writes on substrate-only OKG tables. Each table is REVOKE'd
    -- guarded by IF EXISTS so the script applies on Phase-1 substrates
    -- that pre-date a particular catalog table.
    FOR catalog_table IN
        SELECT unnest(ARRAY[
            'graph_generations',
            'source_watermarks',
            'narrowings',
            'catalog_versions',
            'catalog_modules',
            'catalog_module_versions',
            'catalog_subtype_versions'
        ])
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg' AND c.relname = catalog_table
        ) THEN
            EXECUTE format(
                'REVOKE INSERT, UPDATE, DELETE, TRUNCATE
                     ON okg.%I FROM %I',
                catalog_table, role_name
            );
        END IF;
    END LOOP;

    -- Column-level UPDATE drift: ensure deployment role has no UPDATE
    -- on any column of legacy nodes_live / edges_live / escalation_live
    -- other than valid_to_gen. Postgres' REVOKE without a column list
    -- revokes table-level AND all column-level UPDATE privileges, so
    -- we REVOKE first then re-issue the narrow column-level GRANT to
    -- converge state. Chronos-native deployments skip the retired live
    -- tables because they are absent by design.
    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'okg'
           AND table_name = 'nodes_live'
           AND column_name = 'valid_to_gen'
    ) THEN
        EXECUTE format(
            'REVOKE UPDATE ON okg.nodes_live FROM %I',
            role_name
        );
        EXECUTE format(
            'GRANT UPDATE (valid_to_gen) ON okg.nodes_live TO %I',
            role_name
        );
    END IF;
    IF EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'okg'
           AND table_name = 'edges_live'
           AND column_name = 'valid_to_gen'
    ) THEN
        EXECUTE format(
            'REVOKE UPDATE ON okg.edges_live FROM %I',
            role_name
        );
        EXECUTE format(
            'GRANT UPDATE (valid_to_gen) ON okg.edges_live TO %I',
            role_name
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relname = 'escalation_live'
    ) THEN
        EXECUTE format(
            'REVOKE UPDATE ON okg.escalation_live FROM %I',
            role_name
        );
        EXECUTE format(
            'GRANT UPDATE (valid_to_gen) ON okg.escalation_live TO %I',
            role_name
        );
    END IF;

    -- No access to dbos schema or tables for the deployment role.
    EXECUTE format(
        'REVOKE ALL ON ALL TABLES IN SCHEMA %I FROM %I',
        dbos_schema, role_name
    );
    EXECUTE format(
        'REVOKE ALL ON ALL SEQUENCES IN SCHEMA %I FROM %I',
        dbos_schema, role_name
    );
    EXECUTE format(
        'REVOKE ALL ON SCHEMA %I FROM %I',
        dbos_schema, role_name
    );
END
$$;

-- =====================================================================
-- 6. Connection limits per role per Phase 1 §2.3.1.
--    8 (read) + 4 (write) = 12 substrate; 6 dbos; 4 deployment burst.
-- =====================================================================

-- A role is one pg_authid row shared by every database on the server, and
-- ALTER ROLE rewrites it without locking it: two deployments bootstrapping
-- at once on one server collide with "tuple concurrently updated" (#1985).
-- Take the lock `okg.substrate.db.role_lock.lock_roles_for_alter` takes --
-- COMMENT ON ROLE with the role's own comment holds its cluster-wide lock
-- until the transaction ends -- in sorted name order, like that helper.
-- apply_role_split sends this file as one simple query, so the whole file
-- is one transaction and the lock covers the ALTER ROLEs below.
DO $$
DECLARE
    role_name text;
BEGIN
    FOR role_name IN
        SELECT r FROM unnest(ARRAY[
            'okg_substrate',
            'okg_dbos',
            format('okg_deployment_%s', :'deployment_name')
        ]) AS r
        ORDER BY r COLLATE "C"
    LOOP
        EXECUTE format(
            'COMMENT ON ROLE %I IS %L',
            role_name,
            (SELECT pg_catalog.shobj_description(oid, 'pg_authid')
               FROM pg_catalog.pg_roles WHERE rolname = role_name)
        );
    END LOOP;
END
$$;

ALTER ROLE okg_substrate CONNECTION LIMIT 12;
ALTER ROLE okg_dbos       CONNECTION LIMIT 6;

DO $$
DECLARE
    role_name text := format('okg_deployment_%s', :'deployment_name');
BEGIN
    EXECUTE format('ALTER ROLE %I CONNECTION LIMIT 4', role_name);
END
$$;

-- =====================================================================
-- 6b. Read-role coverage for runtime-created tables — from the REGISTRY,
--     not from a blanket catch-up (`schema-grant-coherence`,
--     req.scoped-catchup).
--
--     What used to be here, and why it was wrong:
--
--       ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate IN SCHEMA okg
--           GRANT SELECT ON TABLES TO okg_mcp;      -- and again for public
--       GRANT SELECT ON ALL TABLES IN SCHEMA okg TO okg_mcp;
--       GRANT SELECT ON ALL TABLES IN SCHEMA public TO okg_mcp;
--
--     The real problem it was solving is narrow and real: Chronos branch /
--     checkpoint tables (`_chronos_branch_tables` and friends) are created
--     lazily by okg_substrate during ingest/publish, and Postgres default
--     privileges are PER-CREATING-ROLE, so grants issued by the migrate
--     role never covered them. MCP graph reads on a fresh role-split
--     deployment failed with "permission denied for
--     _chronos_branch_tables" after the first publish (three agent
--     sessions, 2026-06-11).
--
--     The fix it reached for was four blanket statements, and they
--     answered a question nobody asked: they also handed okg_mcp SELECT on
--     raw fact tables, source blobs and watermarks — exactly what
--     db/views.sql REVOKEs to enforce the issue-#366 allow-list. Whichever
--     script ran last decided the security boundary. The two ALTER DEFAULT
--     PRIVILEGES statements were the worse half: a default ACL is not
--     visible in any relation's grants, it survives every REVOKE aimed at
--     existing tables, and it silently granted okg_mcp SELECT on every
--     FUTURE okg_substrate-created table.
--
--     Both scripts now apply okg.grant_registry, which names
--     `public._chronos%` as a scoped pattern and nothing broader. Two
--     scripts reading one table commute by construction. The registry is
--     seeded by db/views.sql; on a database where it does not exist yet
--     this block is a no-op and the next migrate applies it.
-- =====================================================================

-- Drift recovery for the default ACLs this script used to create. A default
-- privilege is invisible in every relation's grants, so a deployment that took
-- the old script keeps granting okg_mcp SELECT on each new okg_substrate table
-- until the entry is removed — deleting the statements above converges a FRESH
-- database, not an existing one. db/views.sql carries the same REVOKE; both
-- scripts issuing it is what makes the two orders agree on pg_default_acl, not
-- just on current grants.
--
-- ALTER DEFAULT PRIVILEGES FOR ROLE requires membership in that role. The
-- deployment owner normally has it (it is the role that created okg_substrate);
-- where it does not, skipping is correct — the entry belongs to a role this
-- session cannot speak for, and the provenance-split audit surfaces it.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp')
       AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_substrate') THEN
        BEGIN
            ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate IN SCHEMA okg
                REVOKE SELECT ON TABLES FROM okg_mcp;
            ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate IN SCHEMA public
                REVOKE SELECT ON TABLES FROM okg_mcp;
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING USING MESSAGE =
                'okg: cannot revoke okg_substrate default privileges for '
                'okg_mcp (not a member of okg_substrate); run role_split as a '
                'member of okg_substrate, or re-run migrate';
        END;
    END IF;
END
$$;

-- Converge, do not accumulate — the same revoke-then-apply shape db/views.sql
-- uses, so the two scripts reach identical state in either order rather than
-- merely avoiding conflict.
--
-- The REVOKE is armed by a POPULATED registry, and both halves of that guard
-- are load-bearing:
--   * table absent — a database that has not taken migration 0027. There is
--     nothing to re-grant, so revoking would turn a missing migration into a
--     broken MCP read pool.
--   * table present but EMPTY — schema.sql created it on a fresh install and
--     db/views.sql has not run yet to seed it. This is the reachable order
--     (bootstrap before first migrate), and it is the dangerous one: the
--     function exists, so a presence-only guard would revoke everything and
--     then apply nothing.
-- Applying with no revoke is always safe; revoking with nothing to apply is
-- the outage.
DO $$
BEGIN
    IF to_regprocedure('okg.apply_grant_registry()') IS NOT NULL
       AND to_regclass('okg.grant_registry') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1 FROM okg.grant_registry WHERE classification = 'contract'
        ) THEN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
                REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp;
                REVOKE ALL ON ALL TABLES IN SCHEMA public FROM okg_mcp;
            END IF;
            PERFORM okg.apply_grant_registry();
        END IF;
    END IF;
END
$$;

-- =====================================================================
-- 7. Marker: this script applied successfully for this deployment.
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.role_split_marker (
    deployment_name  text PRIMARY KEY,
    dbos_schema      text NOT NULL,
    applied_at       timestamptz NOT NULL DEFAULT now()
);

INSERT INTO okg.role_split_marker (deployment_name, dbos_schema)
VALUES (:'deployment_name', :'dbos_schema')
ON CONFLICT (deployment_name)
DO UPDATE SET dbos_schema = EXCLUDED.dbos_schema, applied_at = now();

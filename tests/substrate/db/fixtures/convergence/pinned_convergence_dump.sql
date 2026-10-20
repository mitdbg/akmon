--
-- PostgreSQL database dump
--


-- Dumped from database version 17.10 (Ubuntu 17.10-1.pgdg22.04+1)
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: okg; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA okg;


--
-- Name: okg_derived; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA okg_derived;


--
-- Name: btree_gist; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;


--
-- Name: EXTENSION btree_gist; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gist IS 'support for indexing common datatypes in GiST';


--
-- Name: fuzzystrmatch; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS fuzzystrmatch WITH SCHEMA public;


--
-- Name: EXTENSION fuzzystrmatch; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION fuzzystrmatch IS 'determine similarities and distance between strings';


--
-- Name: pg_textsearch; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_textsearch WITH SCHEMA public;


--
-- Name: EXTENSION pg_textsearch; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_textsearch IS 'Full-text search with BM25 ranking';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


--
-- Name: _fill_default_branch_id(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg._fill_default_branch_id() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.branch_id IS NULL AND NEW.deployment_name IS NOT NULL THEN
    INSERT INTO okg.branches
      (deployment_name, name, role, status, metadata)
    VALUES
      (NEW.deployment_name, 'default', 'default', 'active',
       '{"auto_materialized":"_fill_default_branch_id trigger"}'::jsonb)
    ON CONFLICT (deployment_name, name) DO NOTHING;
    SELECT branch_id INTO NEW.branch_id
      FROM okg.branches
     WHERE deployment_name = NEW.deployment_name
       AND name = 'default'
     LIMIT 1;
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: _stamp_searchable_text(text, jsonb, bigint); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg._stamp_searchable_text(p_subtype text, p_attrs jsonb, p_cv bigint) RETURNS jsonb
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $$
DECLARE
  fields    JSONB;
  path_fields JSONB;
  parts     TEXT[] := ARRAY[]::TEXT[];
  field_key TEXT;
  value     TEXT;
  split_value TEXT;
  text_value TEXT;
  joined    TEXT;
BEGIN
  IF p_cv IS NULL THEN
    RETURN p_attrs;
  END IF;
  SELECT s.metadata->'searchable_text_path_fields' INTO path_fields
    FROM okg.subtypes s
   WHERE s.name = p_subtype
     AND s.catalog_version_id = p_cv;
  SELECT s.metadata->'searchable_text_fields' INTO fields
    FROM okg.subtypes s
   WHERE s.name = p_subtype
     AND s.catalog_version_id = p_cv;
  IF fields IS NULL OR jsonb_typeof(fields) <> 'array' THEN
    RETURN p_attrs;
  END IF;
  FOR field_key IN SELECT jsonb_array_elements_text(fields) LOOP
    -- `attrs.text` is itself the derived rollup produced by this
    -- function. Several ontologies include `text` as a fallback
    -- searchable field; treating it like a normal source field makes
    -- restamping non-idempotent (`fresh\n\nold_rollup`). Keep it only
    -- when no other declared string field contributes.
    IF field_key = 'text' THEN
      IF jsonb_typeof(p_attrs->field_key) = 'string' THEN
        -- Same ASCII whitespace set as the loops below and as the Python
        -- mirror. Bare `trim()` here (spaces only) against Python's bare
        -- `.strip()` (all Unicode whitespace) made the two write paths
        -- stamp different text for any `text`-only subtype whose body
        -- begins or ends with a tab or newline — ordinary for a text
        -- blob, and `bookkeeping.ProjectGoal` takes this branch on every
        -- node.
        text_value := btrim(p_attrs->>field_key, E' \t\n\r\f\v');
      END IF;
      CONTINUE;
    END IF;
    -- Per `refine-doc-corpus-extraction` (2026-04-26) W8:
    -- skip array / object jsonb values so multi-valued fields
    -- (e.g. parse_warnings = ["oversize"]) don't get folded
    -- into the BM25-indexed text. Only string scalars
    -- contribute.
    IF jsonb_typeof(p_attrs->field_key) <> 'string' THEN
      CONTINUE;
    END IF;
    value := p_attrs->>field_key;
    -- Order-preserving dedupe: two declared fields can hold the same
    -- value (Document's `title` is seeded from `path`); repeating it only
    -- inflates BM25 term frequency.
    --
    -- `btrim(value, ASCII whitespace)`, not bare `trim(value)` (spaces
    -- only): the Python mirror strips the ASCII whitespace set, and with
    -- a dedupe in play a padding difference no longer just changes
    -- padding — it changes whether a part is dropped, so the two write
    -- paths would stamp structurally different text. See
    -- okg.substrate.searchable_text.ASCII_WHITESPACE.
    value := btrim(value, E' \t\n\r\f\v');
    IF value IS NOT NULL AND length(value) > 0
       AND NOT (value = ANY(parts)) THEN
      parts := parts || value;
    END IF;
  END LOOP;
  -- Per `index-document-source-file-search` (issue #900): fields named in
  -- `searchable_text_path_fields` hold structured identifiers (paths).
  -- The BM25 index is built WITH (text_config='english'), and that parser
  -- lexes a whole slash-path as a SINGLE `file` token, so an interior word
  -- can never match. Append the component-word form alongside the raw
  -- value. Skipped when already present, so subtypes that do not declare
  -- path fields stamp byte-identical text to the pre-change function.
  -- Mirrors okg.substrate.searchable_text.split_path_value.
  IF jsonb_typeof(path_fields) = 'array' THEN
    FOR field_key IN SELECT jsonb_array_elements_text(path_fields) LOOP
      IF field_key = 'text' THEN
        CONTINUE;
      END IF;
      IF jsonb_typeof(p_attrs->field_key) <> 'string' THEN
        CONTINUE;
      END IF;
      value := p_attrs->>field_key;
      IF value IS NULL
         OR length(btrim(value, E' \t\n\r\f\v')) = 0 THEN
        CONTINUE;
      END IF;
      -- `[ \t\n\r\f\v]`, not `\s`: Python's `\s` also matches U+00A0 and
      -- the other Unicode spaces while Postgres' does not, so `\s` here
      -- would make the mirrors disagree on any value with a non-breaking
      -- space. Same set on both sides.
      split_value := btrim(
        regexp_replace(
          regexp_replace(value, '[/\\_.:-]+', ' ', 'g'),
          '[ \t\n\r\f\v]+', ' ', 'g'
        ),
        E' \t\n\r\f\v'
      );
      IF length(split_value) > 0 AND NOT (split_value = ANY(parts)) THEN
        parts := parts || split_value;
      END IF;
    END LOOP;
  END IF;
  IF cardinality(parts) = 0 THEN
    -- `text_value` was already ASCII-trimmed at capture, above.
    IF text_value IS NOT NULL AND length(text_value) > 0 THEN
      RETURN jsonb_set(p_attrs, '{text}', to_jsonb(text_value), true);
    END IF;
    RETURN p_attrs;
  END IF;
  joined := array_to_string(parts, E'\n\n');
  RETURN jsonb_set(p_attrs, '{text}', to_jsonb(joined), true);
END;
$$;


--
-- Name: _stamp_semantic_timestamps(text, jsonb, bigint); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg._stamp_semantic_timestamps(p_subtype text, p_attrs jsonb, p_cv bigint) RETURNS jsonb
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $$
DECLARE
  meta            JSONB;
  src_field       TEXT;
  src_value       TEXT;
  out_attrs       JSONB := p_attrs;
BEGIN
  IF p_cv IS NULL OR p_attrs IS NULL THEN
    RETURN p_attrs;
  END IF;
  SELECT s.metadata INTO meta
    FROM okg.subtypes s
   WHERE s.name = p_subtype
     AND s.catalog_version_id = p_cv;
  IF meta IS NULL THEN
    RETURN p_attrs;
  END IF;

  -- created_at promotion.
  IF jsonb_typeof(meta->'created_at_from') = 'string'
     AND jsonb_typeof(out_attrs->'created_at') IS DISTINCT FROM 'string'
  THEN
    src_field := meta->>'created_at_from';
    IF jsonb_typeof(out_attrs->src_field) = 'string' THEN
      src_value := out_attrs->>src_field;
      IF src_value IS NOT NULL AND length(src_value) > 0 THEN
        out_attrs := jsonb_set(
          out_attrs, '{created_at}', to_jsonb(src_value), true
        );
      END IF;
    END IF;
  END IF;

  -- last_modified_at promotion.
  IF jsonb_typeof(meta->'last_modified_at_from') = 'string'
     AND jsonb_typeof(out_attrs->'last_modified_at') IS DISTINCT FROM 'string'
  THEN
    src_field := meta->>'last_modified_at_from';
    IF jsonb_typeof(out_attrs->src_field) = 'string' THEN
      src_value := out_attrs->>src_field;
      IF src_value IS NOT NULL AND length(src_value) > 0 THEN
        out_attrs := jsonb_set(
          out_attrs, '{last_modified_at}', to_jsonb(src_value), true
        );
      END IF;
    END IF;
  END IF;

  RETURN out_attrs;
END;
$$;


--
-- Name: abort_attribute_integrity_transition(text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.abort_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) RETURNS void
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
             AND backend_pid = pg_backend_pid()
             AND backend_start = (
               SELECT backend_start FROM pg_stat_activity
                WHERE pid = pg_backend_pid()
             )
        $$;


--
-- Name: acquire_history_generation_read_lease(text, text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.acquire_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text, p_checkpoint_id text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg', 'public'
    AS $$
BEGIN
  IF p_pin_name NOT LIKE 'read:%' THEN
    RAISE EXCEPTION 'read lease name must start with read:';
  END IF;
  PERFORM pg_advisory_xact_lock(1326131011, 1);
  DELETE FROM okg.history_generation_pins
   WHERE ctid IN (
     SELECT ctid FROM okg.history_generation_pins
      WHERE pin_name LIKE 'read:%' AND expires_at <= now()
      LIMIT 1000
   );
  IF EXISTS (
       SELECT 1 FROM okg.compacted_generations
        WHERE checkpoint_id = p_checkpoint_id
     )
     OR NOT EXISTS (
       SELECT 1 FROM public._chronos_branch_interval_checkpoints
        WHERE checkpoint_id = p_checkpoint_id
          AND branch_id = p_branch_id
          AND metadata::jsonb ->> 'kind' = 'okg_generation'
          AND metadata::jsonb ->> 'deployment_name' = p_deployment_name
     )
  THEN
    RETURN FALSE;
  END IF;
  INSERT INTO okg.history_generation_pins
    (deployment_name, branch_id, pin_name, checkpoint_id, expires_at)
  VALUES
    (p_deployment_name, p_branch_id, p_pin_name, p_checkpoint_id,
     now() + interval '5 minutes')
  ON CONFLICT (deployment_name, branch_id, pin_name) DO UPDATE
    SET checkpoint_id = EXCLUDED.checkpoint_id,
        expires_at = EXCLUDED.expires_at;
  RETURN TRUE;
END;
$$;


--
-- Name: attribute_integrity_transition_matches(text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.attribute_integrity_transition_matches(p_deployment text, p_branch text, p_namespace text) RETURNS boolean
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
          SELECT EXISTS (
            SELECT 1 FROM okg.attribute_effective_integrity_transitions
             WHERE deployment_name = p_deployment AND branch_id = p_branch
               AND namespace = p_namespace
               AND backend_pid = pg_backend_pid()
               AND backend_start = (
                 SELECT backend_start FROM pg_stat_activity
                  WHERE pid = pg_backend_pid()
               )
          )
        $$;


--
-- Name: begin_attribute_integrity_transition(text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.begin_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
        BEGIN
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace;
          INSERT INTO okg.attribute_effective_integrity_transitions
            (deployment_name, branch_id, namespace, backend_pid, backend_start)
          SELECT p_deployment, p_branch, p_namespace, pg_backend_pid(), backend_start
            FROM pg_stat_activity WHERE pid = pg_backend_pid();
          IF NOT FOUND THEN
            RAISE EXCEPTION 'current backend identity is unavailable';
          END IF;
          RETURN TRUE;
        END
        $$;


--
-- Name: compacted_generation_record(text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.compacted_generation_record(p_generation text, p_deployment_name text DEFAULT NULL::text) RETURNS jsonb
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $_$
  SELECT jsonb_build_object(
    'checkpoint_id', checkpoint_id,
    'target_generation_id', target_generation_id,
    'deployment_name', deployment_name,
    'branch', branch_id,
    'plan_id', plan_id::text,
    'compacted_at', compacted_at,
    'reason', reason
  )
  FROM okg.compacted_generations
  WHERE (
    checkpoint_id = p_generation
    OR (
      p_deployment_name IS NOT NULL
      AND target_generation_id = CASE
        WHEN p_generation ~ '^[0-9]+$' THEN p_generation::bigint
        ELSE NULL
      END
    )
  )
  AND (p_deployment_name IS NULL OR deployment_name = p_deployment_name)
  ORDER BY compacted_at DESC
  LIMIT 1
$_$;


--
-- Name: compute_pagerank(text, double precision, integer, double precision); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.compute_pagerank(edges_sql text, damping double precision DEFAULT 0.85, max_iter integer DEFAULT 50, tol double precision DEFAULT 0.000001) RETURNS TABLE(node_id text, pagerank double precision)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
  n_nodes      BIGINT;
  iter_no      INT  := 0;
  l1_delta     DOUBLE PRECISION;
  dangling_sum DOUBLE PRECISION;
  base         DOUBLE PRECISION;
BEGIN
  -- Materialize the edges. The temp tables auto-drop at function
  -- end (ON COMMIT DROP would conflict with non-tx callers; we
  -- rely on temp-table-per-session lifetimes and explicit DROPs
  -- below).
  CREATE TEMP TABLE _pr_edges (src TEXT, dst TEXT) ON COMMIT DROP;
  EXECUTE format(
    'INSERT INTO _pr_edges (src, dst) '
    'SELECT src::text, dst::text FROM (%s) sub '
    'WHERE src IS NOT NULL AND dst IS NOT NULL',
    edges_sql
  );

  CREATE TEMP TABLE _pr_nodes (
    node_id   TEXT PRIMARY KEY,
    out_deg   BIGINT NOT NULL,
    is_dangle BOOLEAN NOT NULL
  ) ON COMMIT DROP;
  INSERT INTO _pr_nodes (node_id, out_deg, is_dangle)
  SELECT u.node_id,
         coalesce(o.out_deg, 0)               AS out_deg,
         (coalesce(o.out_deg, 0) = 0)         AS is_dangle
    FROM (
      SELECT src AS node_id FROM _pr_edges
      UNION
      SELECT dst FROM _pr_edges
    ) u
    LEFT JOIN (
      SELECT src AS node_id, count(*) AS out_deg
        FROM _pr_edges
       GROUP BY src
    ) o USING (node_id);

  SELECT count(*) INTO n_nodes FROM _pr_nodes;
  IF n_nodes = 0 THEN
    DROP TABLE _pr_edges;
    DROP TABLE _pr_nodes;
    RETURN;
  END IF;

  CREATE TEMP TABLE _pr_curr (node_id TEXT PRIMARY KEY,
                              rank DOUBLE PRECISION NOT NULL) ON COMMIT DROP;
  CREATE TEMP TABLE _pr_next (node_id TEXT PRIMARY KEY,
                              rank DOUBLE PRECISION NOT NULL) ON COMMIT DROP;

  INSERT INTO _pr_curr (node_id, rank)
  SELECT n.node_id, 1.0::float / n_nodes FROM _pr_nodes n;

  LOOP
    EXIT WHEN iter_no >= max_iter;
    iter_no := iter_no + 1;

    SELECT coalesce(sum(c.rank), 0)
      INTO dangling_sum
      FROM _pr_curr c
      JOIN _pr_nodes n USING (node_id)
     WHERE n.is_dangle;

    base := (1.0 - damping) / n_nodes
          + damping * dangling_sum / n_nodes;

    TRUNCATE _pr_next;
    INSERT INTO _pr_next (node_id, rank)
    SELECT n.node_id,
           base + damping * coalesce(inflow.s, 0) AS rank
      FROM _pr_nodes n
      LEFT JOIN (
        SELECT e.dst AS node_id,
               sum(c.rank / nullif(src_n.out_deg, 0)) AS s
          FROM _pr_edges e
          JOIN _pr_curr  c     ON c.node_id     = e.src
          JOIN _pr_nodes src_n ON src_n.node_id = e.src
         WHERE NOT src_n.is_dangle
         GROUP BY e.dst
      ) inflow USING (node_id);

    SELECT coalesce(sum(abs(nxt.rank - cur.rank)), 0)
      INTO l1_delta
      FROM _pr_next nxt
      JOIN _pr_curr cur USING (node_id);

    TRUNCATE _pr_curr;
    INSERT INTO _pr_curr (node_id, rank)
    SELECT node_id, rank FROM _pr_next;

    EXIT WHEN l1_delta < tol;
  END LOOP;

  RETURN QUERY
    SELECT c.node_id, c.rank FROM _pr_curr c;

  DROP TABLE _pr_edges;
  DROP TABLE _pr_nodes;
  DROP TABLE _pr_curr;
  DROP TABLE _pr_next;
END;
$$;


--
-- Name: current_pin(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.current_pin() RETURNS bigint
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $$
DECLARE
  pin_text text;
BEGIN
  pin_text := current_setting('okg.pin_generation', true);
  IF pin_text IS NULL OR pin_text = '' THEN
    RAISE EXCEPTION 'okg.pin_generation GUC not set'
      USING HINT = 'SET LOCAL okg.pin_generation = <generation_id> '
                 || 'before querying any okg.v_* view',
            ERRCODE = 'undefined_parameter';
  END IF;
  RETURN pin_text::bigint;
EXCEPTION WHEN invalid_text_representation THEN
  RAISE EXCEPTION 'okg.pin_generation must be a bigint, got %', pin_text
    USING ERRCODE = 'invalid_parameter_value';
END
$$;


--
-- Name: enforce_commit_witness(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.enforce_commit_witness() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  offending text;
BEGIN
  -- Trusted-base bypass: superuser or the table's owner writes directly.
  IF (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
     OR current_user = (SELECT tableowner FROM pg_tables
                          WHERE schemaname = TG_TABLE_SCHEMA
                            AND tablename = TG_TABLE_NAME)
  THEN
    RETURN NULL;
  END IF;
  -- Runtime role: every inserted row must carry a witness that resolves in the
  -- IR ledger. NULL checksum does not match -> flagged as un-witnessed.
  SELECT n.source_program_checksum INTO offending
    FROM newrows n
    LEFT JOIN okg.ir_programs p
      ON p.program_checksum = n.source_program_checksum
   WHERE p.program_id IS NULL
   LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION
      'commit_witness_missing: durable write into okg.% with unresolvable source_program_checksum %',
      TG_TABLE_NAME, coalesce(offending, '<null>');
  END IF;
  RETURN NULL;
END;
$$;


--
-- Name: finalize_attribute_integrity_receipt(text, text, text, text, text, text, bigint, text, bigint, bigint, bigint, bigint, integer, text, text, numeric, numeric, numeric, numeric, text, bigint); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.finalize_attribute_integrity_receipt(p_receipt_digest text, p_receipt_type text, p_deployment text, p_branch text, p_namespace text, p_generation_id text, p_ordinal bigint, p_prior_receipt_digest text, p_checkpoint_node_count bigint, p_checkpoint_edge_count bigint, p_logical_node_count bigint, p_logical_edge_count bigint, p_schema_version integer, p_schema_digest text, p_migration_digest text, p_logical_payload_seed0 numeric, p_logical_payload_seed1 numeric, p_interval_payload_seed0 numeric, p_interval_payload_seed1 numeric, p_dirty_scope_digest text, p_mutation_epoch bigint) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
        BEGIN
          -- The calling backend must own the live transition token for this
          -- scope; finalization consumes it atomically with the receipt
          -- append and the clean-state compare-and-set on the mutation
          -- epoch.  This function is the only clean-marking authority:
          -- direct DML on the state and receipt tables is revoked from the
          -- runtime and application roles.
          IF NOT okg.finish_attribute_integrity_transition(
            p_deployment, p_branch, p_namespace
          ) THEN
            RAISE EXCEPTION
              'attribute integrity transition changed during finalization';
          END IF;
          INSERT INTO okg.attribute_effective_integrity_receipts (
            receipt_digest, receipt_type, deployment_name, branch_id,
            namespace, generation_id, ordinal, prior_receipt_digest,
            checkpoint_node_count, checkpoint_edge_count,
            logical_node_count, logical_edge_count,
            schema_version, schema_digest, migration_digest,
            logical_payload_seed0, logical_payload_seed1,
            interval_payload_seed0, interval_payload_seed1,
            dirty_scope_digest, mutation_epoch
          ) VALUES (
            p_receipt_digest, p_receipt_type, p_deployment, p_branch,
            p_namespace, p_generation_id, p_ordinal, p_prior_receipt_digest,
            p_checkpoint_node_count, p_checkpoint_edge_count,
            p_logical_node_count, p_logical_edge_count,
            p_schema_version, p_schema_digest, p_migration_digest,
            p_logical_payload_seed0, p_logical_payload_seed1,
            p_interval_payload_seed0, p_interval_payload_seed1,
            p_dirty_scope_digest, p_mutation_epoch
          );
          UPDATE okg.attribute_effective_integrity_state
             SET clean_mutation_epoch = p_mutation_epoch,
                 dirty = FALSE,
                 dirty_reason = 'clean',
                 latest_receipt_digest = p_receipt_digest,
                 updated_at = now()
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
             AND mutation_epoch = p_mutation_epoch
             -- Incremental receipts must extend the scope's current chain
             -- head; an anchor (NULL prior) resets the chain.
             AND (p_prior_receipt_digest IS NULL
                  OR latest_receipt_digest
                     IS NOT DISTINCT FROM p_prior_receipt_digest);
          IF NOT FOUND THEN
            -- Distinguish a chain fork (epoch matched, prior did not)
            -- from the benign transition/epoch race for operability.
            IF EXISTS (
              SELECT 1 FROM okg.attribute_effective_integrity_state
               WHERE deployment_name = p_deployment
                 AND branch_id = p_branch
                 AND namespace = p_namespace
                 AND mutation_epoch = p_mutation_epoch
            ) THEN
              RAISE EXCEPTION
                'attribute integrity receipt chain head moved during finalization';
            END IF;
            RAISE EXCEPTION
              'attribute integrity transition changed during finalization';
          END IF;
          RETURN TRUE;
        END
        $$;


--
-- Name: find_identifier_mentions(text, bigint); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.find_identifier_mentions(p_text text, p_cv bigint) RETURNS TABLE(id_type text, value text, span_start integer, span_end integer)
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $$
DECLARE
  pat_rec  RECORD;
  occ      INTEGER;
  s_pos    INTEGER;  -- 1-indexed start (0 = no match)
  e_pos    INTEGER;  -- 1-indexed pos after match
BEGIN
  FOR pat_rec IN
    SELECT ip.id_type, ip.regex
    FROM okg.identifier_patterns ip
    WHERE ip.catalog_version_id = p_cv
  LOOP
    occ := 1;
    LOOP
      s_pos := regexp_instr(p_text, pat_rec.regex, 1, occ, 0);
      EXIT WHEN s_pos = 0;
      e_pos := regexp_instr(p_text, pat_rec.regex, 1, occ, 1);

      id_type    := pat_rec.id_type;
      value      := substring(p_text FROM s_pos FOR e_pos - s_pos);
      span_start := s_pos - 1;     -- 0-indexed for Python callers
      span_end   := e_pos - 1;     -- exclusive end, 0-indexed
      RETURN NEXT;

      occ := occ + 1;
    END LOOP;
  END LOOP;

  RETURN;
END;
$$;


--
-- Name: finish_attribute_integrity_transition(text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.finish_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
        DECLARE affected INTEGER;
        BEGIN
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
             AND backend_pid = pg_backend_pid()
             AND backend_start = (
               SELECT backend_start FROM pg_stat_activity
                WHERE pid = pg_backend_pid()
             );
          GET DIAGNOSTICS affected = ROW_COUNT;
          RETURN affected = 1;
        END
        $$;


--
-- Name: fuzzy_resolve_alias(text, bigint, real, integer); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.fuzzy_resolve_alias(p_needle text, p_cv bigint, p_threshold real DEFAULT 0.6, p_limit integer DEFAULT 5) RETURNS TABLE(canonical text, similarity real, method text)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
  lev_cap INTEGER := GREATEST(2, length(p_needle) / 4);
BEGIN
  -- Tier 1: exact match
  RETURN QUERY
    SELECT
      a.canonical,
      1.0::real             AS similarity,
      'exact'::text         AS method
    FROM okg.aliases a
    WHERE a.alias = p_needle
      AND a.catalog_version_id = p_cv
    LIMIT p_limit;
  IF FOUND THEN RETURN; END IF;

  -- Tier 2: trigram similarity (uses GIN index)
  RETURN QUERY
    SELECT
      a.canonical,
      similarity(a.alias, p_needle)::real AS similarity,
      'trigram'::text                      AS method
    FROM okg.aliases a
    WHERE a.catalog_version_id = p_cv
      AND a.alias % p_needle
      AND similarity(a.alias, p_needle) >= p_threshold
    ORDER BY similarity(a.alias, p_needle) DESC
    LIMIT p_limit;
  IF FOUND THEN RETURN; END IF;

  -- Tier 3: Levenshtein (capped to avoid O(N*L) on long needles)
  -- fuzzystrmatch's Levenshtein functions reject inputs over
  -- 255 characters. Exact/trigram tiers above can still resolve
  -- long strings; if they miss, skip edit-distance instead of
  -- raising and poisoning the caller's whole batch.
  IF length(p_needle) > 255 THEN RETURN; END IF;

  RETURN QUERY
    SELECT
      a.canonical,
      (
        1.0
        - (
            levenshtein_less_equal(a.alias, p_needle, lev_cap)::real
            / GREATEST(length(a.alias), length(p_needle))::real
          )
      )::real                              AS similarity,
      'levenshtein'::text                  AS method
    FROM okg.aliases a
    WHERE a.catalog_version_id = p_cv
      AND length(a.alias) <= 255
      AND levenshtein_less_equal(a.alias, p_needle, lev_cap) <= lev_cap
    ORDER BY similarity DESC
    LIMIT p_limit;
END;
$$;


--
-- Name: lock_history_compaction_protection(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.lock_history_compaction_protection() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(1326131011, 1);
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;


--
-- Name: mark_attribute_effective_integrity_dirty(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.mark_attribute_effective_integrity_dirty() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
        BEGIN
          -- TRUNCATE has no transition tables, so its invalidation is
          -- necessarily global: every scope's epoch advances and every
          -- other backend's transition token dies.
          UPDATE okg.attribute_effective_integrity_state
             SET mutation_epoch = mutation_epoch + 1,
                 dirty = TRUE,
                 dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ':' || TG_OP,
                 updated_at = now();
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE backend_pid <> pg_backend_pid()
              OR backend_start IS DISTINCT FROM (
                SELECT backend_start FROM pg_stat_activity
                 WHERE pid = pg_backend_pid()
              );
          RETURN NULL;
        END
        $$;


--
-- Name: mark_attribute_effective_integrity_dirty_rows(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
        BEGIN
          -- Scoped invalidation from the statement's transition table
          -- (NEW TABLE for INSERT, OLD TABLE for DELETE; both are named
          -- changed_rows by the trigger). Only the scopes the statement
          -- actually touched lose their clean proof; the interval table
          -- carries the branch, the snapshot table dirties every branch
          -- of the touched namespaces. A zero-row statement dirties
          -- nothing, which is exact.
          IF TG_TABLE_NAME = 'attribute_effective_view_intervals' THEN
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
               SELECT 1 FROM changed_rows c
                WHERE c.namespace = s.namespace AND c.branch_id = s.branch_id
             );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND EXISTS (
                 SELECT 1 FROM changed_rows c
                  WHERE c.namespace = t.namespace
                    AND c.branch_id = t.branch_id
               );
          ELSE
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
               SELECT 1 FROM changed_rows c WHERE c.namespace = s.namespace
             );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND EXISTS (
                 SELECT 1 FROM changed_rows c WHERE c.namespace = t.namespace
               );
          END IF;
          RETURN NULL;
        END
        $$;


--
-- Name: mark_attribute_effective_integrity_dirty_update(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.mark_attribute_effective_integrity_dirty_update() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
        BEGIN
          -- UPDATE scope is the union of the old and new row coordinates:
          -- a row moved across namespaces or branches dirties both sides.
          IF TG_TABLE_NAME = 'attribute_effective_view_intervals' THEN
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
                 SELECT 1 FROM old_rows c
                  WHERE c.namespace = s.namespace
                    AND c.branch_id = s.branch_id
               )
                OR EXISTS (
                 SELECT 1 FROM new_rows c
                  WHERE c.namespace = s.namespace
                    AND c.branch_id = s.branch_id
               );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND (EXISTS (
                     SELECT 1 FROM old_rows c
                      WHERE c.namespace = t.namespace
                        AND c.branch_id = t.branch_id
                   )
                    OR EXISTS (
                     SELECT 1 FROM new_rows c
                      WHERE c.namespace = t.namespace
                        AND c.branch_id = t.branch_id
                   ));
          ELSE
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
                 SELECT 1 FROM old_rows c WHERE c.namespace = s.namespace
               )
                OR EXISTS (
                 SELECT 1 FROM new_rows c WHERE c.namespace = s.namespace
               );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND (EXISTS (
                     SELECT 1 FROM old_rows c
                      WHERE c.namespace = t.namespace
                   )
                    OR EXISTS (
                     SELECT 1 FROM new_rows c
                      WHERE c.namespace = t.namespace
                   ));
          END IF;
          RETURN NULL;
        END
        $$;


--
-- Name: mark_attribute_integrity_scope_dirty(text, text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.mark_attribute_integrity_scope_dirty(p_deployment text, p_branch text, p_namespace text, p_reason text) RETURNS void
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
          UPDATE okg.attribute_effective_integrity_state
             SET mutation_epoch = mutation_epoch + 1,
                 dirty = TRUE,
                 dirty_reason = p_reason,
                 updated_at = now()
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
        $$;


--
-- Name: refresh_pagerank(text, text, double precision, integer, double precision); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.refresh_pagerank(p_scope text, edges_sql text, damping double precision DEFAULT 0.85, max_iter integer DEFAULT 50, tol double precision DEFAULT 0.000001) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
  row_count BIGINT;
BEGIN
  CREATE TEMP TABLE _pr_result (
    node_id  TEXT PRIMARY KEY,
    pagerank DOUBLE PRECISION NOT NULL
  ) ON COMMIT DROP;

  INSERT INTO _pr_result (node_id, pagerank)
  SELECT cp.node_id, cp.pagerank
    FROM okg.compute_pagerank(edges_sql, damping, max_iter, tol) cp;

  INSERT INTO okg.pagerank_scores (scope, node_id, pagerank, refreshed_at)
  SELECT p_scope, r.node_id, r.pagerank, now()
    FROM _pr_result r
    ON CONFLICT (scope, node_id) DO UPDATE
       SET pagerank     = EXCLUDED.pagerank,
           refreshed_at = EXCLUDED.refreshed_at;

  DELETE FROM okg.pagerank_scores p
   WHERE p.scope = p_scope
     AND NOT EXISTS (
       SELECT 1 FROM _pr_result r WHERE r.node_id = p.node_id
     );

  SELECT count(*) INTO row_count
    FROM okg.pagerank_scores
   WHERE scope = p_scope;

  DROP TABLE _pr_result;
  RETURN row_count;
END;
$$;


--
-- Name: register_attribute_integrity_scope(text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.register_attribute_integrity_scope(p_deployment text, p_branch text, p_namespace text) RETURNS void
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $$
          INSERT INTO okg.attribute_effective_integrity_state
            (deployment_name, branch_id, namespace)
          VALUES (p_deployment, p_branch, p_namespace)
          ON CONFLICT (deployment_name, branch_id, namespace) DO NOTHING
        $$;


--
-- Name: reject_attribute_integrity_receipt_mutation(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.reject_attribute_integrity_receipt_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
        BEGIN
          RAISE EXCEPTION 'attribute integrity receipts are append-only';
        END
        $$;


--
-- Name: release_history_generation_read_lease(text, text, text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.release_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg', 'public'
    AS $$
DECLARE
  removed INTEGER;
BEGIN
  IF p_pin_name NOT LIKE 'read:%' THEN
    RETURN FALSE;
  END IF;
  PERFORM pg_advisory_xact_lock(1326131011, 1);
  DELETE FROM okg.history_generation_pins
   WHERE deployment_name = p_deployment_name
     AND branch_id = p_branch_id
     AND pin_name = p_pin_name;
  GET DIAGNOSTICS removed = ROW_COUNT;
  RETURN removed > 0;
END;
$$;


--
-- Name: require_chronos_checkout(text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.require_chronos_checkout(kept_table text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  has_rows boolean;
  chronos_registered boolean;
BEGIN
  -- Registry-driven, not a hardcoded table allow-list: a kept table is
  -- guarded iff it is registered in public._chronos_branch_tables. The
  -- registration check runs FIRST so the SECURITY DEFINER dynamic-SQL probe
  -- below only ever targets a Chronos-registered relation.
  IF to_regclass(kept_table) IS NULL THEN
    RETURN false;
  END IF;
  IF to_regclass('public._chronos_branch_tables') IS NULL THEN
    RETURN false;
  END IF;
  SELECT EXISTS (
    SELECT 1 FROM public._chronos_branch_tables t
     WHERE t.table_name = kept_table
  ) INTO chronos_registered;
  IF NOT chronos_registered THEN
    -- Plain storage: an empty bare read is honest (empty table = empty result).
    RETURN false;
  END IF;
  -- String concatenation (not the format() percent-placeholder helper) is
  -- deliberate: this DDL is executed both through psycopg cur.execute
  -- (migration) and the Chronos db adapter, and the latter percent-
  -- interpolates the statement -- a percent-placeholder anywhere in the body,
  -- even in a comment, is mis-read as a bind placeholder. kept_table is
  -- registry-derived (checked registered above), not caller free-text.
  EXECUTE 'SELECT EXISTS (SELECT 1 FROM ' || kept_table || ')'
     INTO has_rows;
  IF has_rows THEN
    -- Someone materialized rows into the base table: a bare read is honest.
    RETURN false;
  END IF;
  RAISE EXCEPTION USING
    MESSAGE = 'okg kept-table read requires a Chronos checkout: '
      || kept_table
      || ' is a Chronos-registered maintained-derivation surface that is '
      || 'never materialized for bare SQL sessions, so this read would '
      || 'report an empty result even when published generations exist',
    HINT = 'Read maintained-derivation kept tables through a Chronos '
           'checkout: ChronosGraphStore.checkout_generation() in Python, or '
           'the MCP operators, which rewrite reads to a pinned checkout. '
           'Check live counts with `okg --json status --deployment <name>`, '
           'not bare psql.',
    ERRCODE = 'object_not_in_prerequisite_state';
END
$$;


--
-- Name: require_graph_checkout(text); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.require_graph_checkout(graph_table text) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  has_rows boolean;
  chronos_registered boolean;
BEGIN
  -- SECURITY DEFINER (least-privilege read roles see okg.v_* only, not the
  -- underlying tables or the Chronos registry), so the probe target is
  -- allow-listed rather than caller-chosen.
  IF graph_table NOT IN ('okg.graph_nodes', 'okg.graph_edges') THEN
    RAISE EXCEPTION
      'okg.require_graph_checkout only guards okg.graph_nodes / okg.graph_edges; got %',
      graph_table;
  END IF;
  IF to_regclass(graph_table) IS NULL THEN
    RETURN false;
  END IF;
  EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', graph_table)
     INTO has_rows;
  IF has_rows THEN
    RETURN false;
  END IF;
  IF to_regclass('public._chronos_branch_tables') IS NULL THEN
    RETURN false;
  END IF;
  SELECT EXISTS (
    SELECT 1 FROM public._chronos_branch_tables t
     WHERE t.table_name = graph_table
  ) INTO chronos_registered;
  IF NOT chronos_registered THEN
    RETURN false;
  END IF;
  RAISE EXCEPTION USING
    MESSAGE = format(
      'okg graph read requires a Chronos checkout: %s is a '
      'Chronos-registered checkout surface that is never materialized for '
      'bare SQL sessions, so this read would report an empty graph even '
      'when published generations exist',
      graph_table
    ),
    HINT = 'Supported read paths: `okg search` / `okg trace` (CLI); the '
           'MCP operators (inspect/search/expand/filter/map/aggregate and '
           'the bounded query tool), which rewrite graph reads to a pinned '
           'Chronos checkout; or ChronosGraphStore.checkout_generation() '
           'in Python. Check row counts with `okg --json status '
           '--deployment <name>`, not bare psql.',
    ERRCODE = 'object_not_in_prerequisite_state';
END
$$;


--
-- Name: runtime_queue_health(integer); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.runtime_queue_health(p_stale_after_seconds integer DEFAULT 600) RETURNS TABLE(queue_name text, status text, availability text, active_count bigint, pending_count bigint, waiting_count bigint, delayed_count bigint, running_count bigint, failed_count bigint, cancelled_count bigint, dlq_count bigint, terminal_history_count bigint, unknown_active_age_count bigint, oldest_active_age_seconds double precision, newest_active_age_seconds double precision, stale_after_seconds integer, stale_active_work_present boolean, generated_at timestamp with time zone, reason text, workflow_name text)
    LANGUAGE plpgsql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg', 'dbos'
    AS $_$
DECLARE
  has_status boolean;
  has_queue_name boolean;
  has_workflow_name boolean;
  updated_at_type text;
  created_at_type text;
  has_started_at_epoch_ms boolean;
  queue_expr text;
  workflow_expr text;
  observed_expr text;
  started_expr text;
  total_rows bigint;
BEGIN
  IF to_regclass('dbos.workflow_status') IS NULL THEN
    RETURN QUERY
    SELECT
      NULL::text,
      'unavailable'::text,
      'unavailable'::text,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      NULL::double precision,
      NULL::double precision,
      p_stale_after_seconds,
      false,
      now(),
      'dbos.workflow_status missing'::text,
      NULL::text;
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'status'
  ) INTO has_status;
  IF NOT has_status THEN
    RETURN QUERY
    SELECT
      NULL::text,
      'unavailable'::text,
      'unavailable'::text,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      NULL::double precision,
      NULL::double precision,
      p_stale_after_seconds,
      false,
      now(),
      'dbos.workflow_status.status missing'::text,
      NULL::text;
    RETURN;
  END IF;

  EXECUTE 'SELECT count(*)::bigint FROM dbos.workflow_status' INTO total_rows;
  IF total_rows = 0 THEN
    RETURN QUERY
    SELECT
      NULL::text,
      'idle'::text,
      'available'::text,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      NULL::double precision,
      NULL::double precision,
      p_stale_after_seconds,
      false,
      now(),
      NULL::text,
      NULL::text;
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'queue_name'
  ) INTO has_queue_name;
  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'name'
  ) INTO has_workflow_name;
  SELECT data_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'updated_at'
   LIMIT 1
    INTO updated_at_type;
  SELECT data_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'created_at'
   LIMIT 1
    INTO created_at_type;
  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'started_at_epoch_ms'
  ) INTO has_started_at_epoch_ms;

  queue_expr := CASE
    WHEN has_queue_name THEN 'queue_name'
    ELSE 'NULL::text'
  END;
  workflow_expr := CASE
    WHEN has_workflow_name THEN 'name'
    ELSE 'NULL::text'
  END;
  observed_expr := CASE
    WHEN updated_at_type IN (
      'bigint', 'integer', 'numeric', 'double precision', 'real'
    ) THEN 'to_timestamp(updated_at::double precision / 1000.0)'
    WHEN updated_at_type IS NOT NULL THEN 'updated_at::timestamptz'
    WHEN created_at_type IN (
      'bigint', 'integer', 'numeric', 'double precision', 'real'
    ) THEN 'to_timestamp(created_at::double precision / 1000.0)'
    WHEN created_at_type IS NOT NULL THEN 'created_at::timestamptz'
    ELSE 'NULL::timestamptz'
  END;
  started_expr := CASE
    WHEN has_started_at_epoch_ms
    THEN 'started_at_epoch_ms IS NOT NULL AND started_at_epoch_ms > 0'
    ELSE 'false'
  END;

  RETURN QUERY EXECUTE format($runtime_queue_health$
    WITH workflow_rows AS (
      SELECT
        COALESCE(%1$s, 'default')::text AS queue_name,
        NULLIF(%5$s, '')::text AS workflow_name,
        upper(status::text) AS workflow_status,
        %2$s AS observed_at,
        (%3$s) AS started
      FROM dbos.workflow_status
    ),
    grouped AS (
      SELECT
        queue_name,
        workflow_name,
        count(*) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
        )::bigint AS active_count,
        count(*) FILTER (WHERE workflow_status = 'PENDING')::bigint
          AS pending_count,
        count(*) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'DELAYED')
        )::bigint AS waiting_count,
        count(*) FILTER (WHERE workflow_status = 'DELAYED')::bigint
          AS delayed_count,
        count(*) FILTER (
          WHERE workflow_status = 'PENDING' AND started
        )::bigint AS running_count,
        count(*) FILTER (
          WHERE workflow_status IN (
            'ERROR', 'FAILED', 'MAX_RECOVERY_ATTEMPTS',
            'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
          )
        )::bigint AS failed_count,
        count(*) FILTER (WHERE workflow_status = 'CANCELLED')::bigint
          AS cancelled_count,
        count(*) FILTER (
          WHERE workflow_status IN (
            'CANCELLED', 'ERROR', 'FAILED', 'MAX_RECOVERY_ATTEMPTS',
            'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
          )
        )::bigint AS dlq_count,
        count(*) FILTER (
          WHERE workflow_status NOT IN ('ENQUEUED', 'PENDING', 'DELAYED')
        )::bigint AS terminal_history_count,
        count(*) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
            AND observed_at IS NULL
        )::bigint AS unknown_active_age_count,
        min(observed_at) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
        ) AS oldest_active_at,
        max(observed_at) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
        ) AS newest_active_at
      FROM workflow_rows
      GROUP BY queue_name, workflow_name
    )
    SELECT
      queue_name,
      CASE
        WHEN active_count = 0 AND dlq_count = 0 THEN 'idle'
        WHEN active_count > 0 AND unknown_active_age_count > 0
          THEN 'unavailable'
        WHEN dlq_count > 0 AND failed_count > 0 AND cancelled_count > 0
          THEN 'dlq'
        WHEN dlq_count > 0 AND failed_count > 0 THEN 'failed'
        WHEN dlq_count > 0 AND cancelled_count > 0 THEN 'cancelled'
        WHEN dlq_count > 0 THEN 'dlq'
        WHEN oldest_active_at IS NOT NULL
          AND EXTRACT(EPOCH FROM now() - oldest_active_at) > %4$s
          THEN 'stale'
        WHEN running_count > 0 THEN 'running'
        WHEN active_count > 0 THEN 'waiting'
        ELSE 'idle'
      END AS status,
      'available'::text AS availability,
      active_count,
      pending_count,
      waiting_count,
      delayed_count,
      running_count,
      failed_count,
      cancelled_count,
      dlq_count,
      terminal_history_count,
      unknown_active_age_count,
      CASE
        WHEN oldest_active_at IS NULL THEN NULL::double precision
        ELSE EXTRACT(EPOCH FROM now() - oldest_active_at)::double precision
      END AS oldest_active_age_seconds,
      CASE
        WHEN newest_active_at IS NULL THEN NULL::double precision
        ELSE EXTRACT(EPOCH FROM now() - newest_active_at)::double precision
      END AS newest_active_age_seconds,
      %4$s::integer AS stale_after_seconds,
      (
        oldest_active_at IS NOT NULL
        AND EXTRACT(EPOCH FROM now() - oldest_active_at) > %4$s
      ) AS stale_active_work_present,
      now() AS generated_at,
      NULL::text AS reason,
      workflow_name
    FROM grouped
    ORDER BY queue_name, workflow_name NULLS FIRST
  $runtime_queue_health$, queue_expr, observed_expr, started_expr,
  p_stale_after_seconds, workflow_expr);
END;
$_$;


--
-- Name: source_frontier_active_workflows(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.source_frontier_active_workflows() RETURNS TABLE(deployment text, source_name text, registry_source_name text, workflow_id text, workflow_status text, run_mode text, trigger_kind text, active_work_observed_at timestamp with time zone, active_work_age_seconds double precision, active_work_stale boolean, priority integer, deduplication_id text, attribution_state text)
    LANGUAGE plpgsql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $_$
DECLARE
  created_type text;
  updated_type text;
  created_expr text;
  updated_expr text;
  priority_expr text;
BEGIN
  IF to_regclass('dbos.workflow_status') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name IN (
         'workflow_uuid',
         'status',
         'name',
         'queue_name',
         'deduplication_id'
       )
     GROUP BY table_schema, table_name
    HAVING count(*) = 5
  ) THEN
    RETURN;
  END IF;

  SELECT data_type INTO created_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'created_at';
  SELECT data_type INTO updated_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'updated_at';

  created_expr := CASE
    WHEN created_type IN (
      'bigint',
      'integer',
      'numeric',
      'double precision',
      'real'
    ) THEN 'to_timestamp(created_at::double precision / 1000.0)'
    WHEN created_type IS NULL THEN 'NULL::timestamptz'
    ELSE 'created_at::timestamptz'
  END;
  updated_expr := CASE
    WHEN updated_type IN (
      'bigint',
      'integer',
      'numeric',
      'double precision',
      'real'
    ) THEN 'to_timestamp(updated_at::double precision / 1000.0)'
    WHEN updated_type IS NULL THEN 'NULL::timestamptz'
    ELSE 'updated_at::timestamptz'
  END;
  priority_expr := CASE
    WHEN EXISTS (
      SELECT 1
        FROM information_schema.columns
       WHERE table_schema = 'dbos'
         AND table_name = 'workflow_status'
         AND column_name = 'priority'
    ) THEN 'priority::integer'
    ELSE 'NULL::integer'
  END;

  RETURN QUERY EXECUTE format($sql$
    WITH raw AS (
      SELECT
        workflow_uuid::text AS workflow_id,
        upper(status::text) AS workflow_status,
        coalesce(%2$s, %1$s) AS observed_at,
        %3$s AS priority,
        queue_name::text AS queue_name,
        deduplication_id::text AS deduplication_id
      FROM dbos.workflow_status
      WHERE name = 'okg_source_sync'
        AND upper(status::text) IN ('ENQUEUED', 'PENDING', 'DELAYED')
    ),
    decoded AS (
      SELECT
        coalesce(
          nullif(split_part(deduplication_id, ':', 2), ''),
          nullif(
            regexp_replace(
              regexp_replace(queue_name, '^okg-', ''),
              '-source-sync$',
              ''
            ),
            ''
          )
        ) AS deployment,
        CASE
          WHEN deduplication_id LIKE 'sync:%%'
            AND split_part(deduplication_id, ':', 5)
                NOT IN ('', '_poll_batch')
          THEN split_part(deduplication_id, ':', 5)
          ELSE NULL
        END AS registry_source_name,
        workflow_id,
        workflow_status,
        CASE WHEN deduplication_id LIKE 'sync:%%'
          THEN nullif(split_part(deduplication_id, ':', 4), '')
        END AS run_mode,
        CASE WHEN deduplication_id LIKE 'sync:%%'
          THEN nullif(split_part(deduplication_id, ':', 3), '')
        END AS trigger_kind,
        observed_at,
        priority,
        deduplication_id,
        CASE
          WHEN deduplication_id LIKE 'sync:%%'
            AND split_part(deduplication_id, ':', 5)
                NOT IN ('', '_poll_batch')
          THEN 'deduplication_id'
          WHEN deduplication_id LIKE 'sync:%%'
            AND split_part(deduplication_id, ':', 5) = '_poll_batch'
          THEN 'deployment_batch'
          ELSE 'deployment_only'
        END AS attribution_state
      FROM raw
    )
    SELECT
      d.deployment,
      CASE WHEN d.registry_source_name IS NULL THEN NULL
        ELSE d.deployment || '.' || d.registry_source_name
      END AS source_name,
      d.registry_source_name,
      d.workflow_id,
      d.workflow_status,
      d.run_mode,
      d.trigger_kind,
      d.observed_at AS active_work_observed_at,
      CASE WHEN d.observed_at IS NULL THEN NULL
        ELSE extract(epoch FROM (now() - d.observed_at))::double precision
      END AS active_work_age_seconds,
      CASE WHEN d.observed_at IS NULL THEN NULL
        ELSE d.observed_at < now() - interval '10 minutes'
      END AS active_work_stale,
      d.priority,
      d.deduplication_id,
      d.attribution_state
    FROM decoded d
    WHERE d.deployment <> ''
  $sql$, created_expr, updated_expr, priority_expr);
END
$_$;


--
-- Name: source_frontier_state(); Type: FUNCTION; Schema: okg; Owner: -
--

CREATE FUNCTION okg.source_frontier_state() RETURNS TABLE(deployment text, source_name text, registry_source_name text, branch_name text, latest_published_generation_id text, latest_published_at timestamp with time zone, watermark_source text, last_reconcile_at timestamp with time zone, last_reconcile_age_seconds double precision, projection_cursor bigint, progress_observed_at timestamp with time zone, last_run_observed_at timestamp with time zone, last_run_id text, last_run_status text, last_run_mode text, last_run_completed_scope boolean, last_error text, nodes_appended bigint, edges_appended bigint, record_set_state text, record_set_sha256 text, record_set_record_count bigint, record_set_node_count bigint, record_set_edge_count bigint, cursor_blob_count integer, cursor_blob_bytes bigint, active_workflow_id text, workflow_status text, workflow_run_mode text, active_work_stale boolean, active_work_age_seconds double precision, runtime_state text, read_visibility_state text, frontier_state text)
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'okg'
    AS $_$
WITH progress_seen AS (
  SELECT
    p.deployment,
    p.source_name,
    CASE
      WHEN p.source_name LIKE p.deployment || '.%'
      THEN substring(p.source_name FROM length(p.deployment) + 2)
      ELSE p.source_name
    END AS registry_source_name,
    max(p.updated_at) AS progress_observed_at
  FROM okg.source_progress p
  GROUP BY p.deployment, p.source_name
),
latest_runs AS (
  SELECT DISTINCT ON (p.deployment, p.source_name)
    p.deployment,
    p.source_name,
    p.key,
    p.value::jsonb AS payload,
    p.updated_at AS last_run_observed_at
  FROM okg.source_progress p
  WHERE (
      p.key = 'sync.last_run'
      OR p.key LIKE 'sync.run.%'
    )
    AND p.value LIKE '{%'
  ORDER BY
    p.deployment,
    p.source_name,
    CASE WHEN p.key = 'sync.last_run' THEN 0 ELSE 1 END,
    p.updated_at DESC,
    p.key DESC
),
watermark_rows AS (
  SELECT
    w.source,
    CASE
      WHEN w.source LIKE '%.%' THEN split_part(w.source, '.', 1)
      ELSE NULL
    END AS deployment_guess,
    CASE
      WHEN w.source LIKE '%.%'
      THEN substring(w.source FROM position('.' in w.source) + 1)
      ELSE w.source
    END AS registry_source_name,
    w.cutover_watermark,
    w.last_applied_rev,
    w.last_published_rev,
    w.projection_cursor,
    w.last_reconcile_at,
    w.replay_required
  FROM okg.source_watermarks w
),
all_active_work AS (
  SELECT *
  FROM okg.source_frontier_active_workflows()
),
active_work_summary AS (
  SELECT
    deployment,
    count(*) FILTER (WHERE registry_source_name IS NULL)::int
      AS unattributed_active_count,
    count(*) FILTER (
      WHERE registry_source_name IS NULL AND active_work_stale
    )::int AS unattributed_stale_count
  FROM all_active_work
  GROUP BY deployment
),
ranked_active_work AS (
  SELECT
    aw.*,
    row_number() OVER (
      PARTITION BY aw.deployment, aw.registry_source_name
      ORDER BY
        CASE
          WHEN aw.run_mode IN (
            'release_unchanged',
            'release_new',
            'scope_complete',
            'reconcile'
          ) AND NOT coalesce(aw.active_work_stale, false) THEN 0
          WHEN aw.run_mode IN (
            'release_unchanged',
            'release_new',
            'scope_complete',
            'reconcile'
          ) THEN 1
          WHEN NOT coalesce(aw.active_work_stale, false) THEN 2
          ELSE 3
        END,
        CASE aw.workflow_status
          WHEN 'PENDING' THEN 0
          WHEN 'ENQUEUED' THEN 1
          WHEN 'DELAYED' THEN 2
          ELSE 9
        END,
        coalesce(nullif(aw.priority, 0), 100),
        coalesce(aw.active_work_age_seconds, 1.0e18),
        aw.workflow_id
    ) AS active_rank
  FROM all_active_work aw
  WHERE aw.registry_source_name IS NOT NULL
),
active_work AS (
  SELECT *
  FROM ranked_active_work
  WHERE active_rank = 1
),
source_keys AS (
  SELECT
    ps.deployment,
    ps.source_name,
    ps.registry_source_name
  FROM progress_seen ps
  UNION
  SELECT
    wr.deployment_guess AS deployment,
    wr.source AS source_name,
    wr.registry_source_name
  FROM watermark_rows wr
  UNION
  SELECT
    aw.deployment,
    aw.source_name,
    aw.registry_source_name
  FROM active_work aw
),
latest_published AS (
  SELECT DISTINCT ON (g.deployment_name)
    g.deployment_name AS deployment,
    g.id::text AS generation_id,
    g.published_at
  FROM okg.v_generations g
  ORDER BY g.deployment_name, g.published_at DESC NULLS LAST, g.id::text DESC
),
cursor_blobs AS (
  SELECT
    cb.source,
    count(*)::int AS cursor_blob_count,
    sum(cb.size_bytes)::bigint AS cursor_blob_bytes
  FROM okg.source_cursor_blobs cb
  GROUP BY cb.source
)
SELECT
  coalesce(sk.deployment, ps.deployment, aw.deployment) AS deployment,
  coalesce(ps.source_name, wr.source, aw.source_name, sk.source_name)
    AS source_name,
  coalesce(ps.registry_source_name, wr.registry_source_name,
           aw.registry_source_name, sk.registry_source_name)
    AS registry_source_name,
  'default'::text AS branch_name,
  lp.generation_id AS latest_published_generation_id,
  lp.published_at AS latest_published_at,
  wr.source AS watermark_source,
  wr.last_reconcile_at,
  CASE WHEN wr.last_reconcile_at IS NULL THEN NULL
    ELSE extract(epoch FROM (now() - wr.last_reconcile_at))::double precision
  END AS last_reconcile_age_seconds,
  wr.projection_cursor,
  ps.progress_observed_at,
  lr.last_run_observed_at,
  coalesce(
    nullif(lr.payload->>'run_id', ''),
    substring(lr.key FROM 'sync\.run\.(.*)$')
  ) AS last_run_id,
  nullif(coalesce(lr.payload->>'status', lr.payload->>'result'), '')
    AS last_run_status,
  nullif(coalesce(lr.payload->>'run_mode', lr.payload->>'mode'), '')
    AS last_run_mode,
  CASE
    WHEN lr.payload ? 'completed_scope'
    THEN (lr.payload->>'completed_scope')::boolean
    WHEN coalesce(lr.payload->>'run_mode', lr.payload->>'mode')
      IN ('release_unchanged', 'release_new', 'scope_complete', 'reconcile')
    THEN true
    WHEN lr.payload IS NULL THEN NULL
    ELSE false
  END AS last_run_completed_scope,
  nullif(coalesce(
    lr.payload->>'last_error',
    lr.payload->>'error',
    lr.payload->>'failure'
  ), '') AS last_error,
  CASE WHEN lr.payload->>'nodes_appended' ~ '^-?[0-9]+$'
    THEN (lr.payload->>'nodes_appended')::bigint END AS nodes_appended,
  CASE WHEN lr.payload->>'edges_appended' ~ '^-?[0-9]+$'
    THEN (lr.payload->>'edges_appended')::bigint END AS edges_appended,
  CASE
    WHEN wr.last_applied_rev ? 'record_set_ref' AND srb.source IS NULL
      THEN 'external_blob_missing'
    WHEN wr.last_applied_rev ? 'record_set_ref'
      THEN 'external_blob'
    WHEN wr.last_applied_rev ? 'record_set'
      THEN 'inline_cursor'
    WHEN srs.source IS NOT NULL
      THEN 'hot_row'
    WHEN wr.last_applied_rev ? 'record_set_summary'
      THEN 'summary_only'
    ELSE 'unavailable'
  END AS record_set_state,
  wr.last_applied_rev->'record_set_ref'->>'sha256' AS record_set_sha256,
  NULLIF(
    coalesce(
      wr.last_applied_rev->'record_set_summary'->>'record_count',
      srb.record_set_summary->>'record_count',
      srs.record_set_summary->>'record_count'
    ),
    ''
  )::bigint AS record_set_record_count,
  NULLIF(
    coalesce(
      wr.last_applied_rev->'record_set_summary'->>'node_count',
      srb.record_set_summary->>'node_count',
      srs.record_set_summary->>'node_count'
    ),
    ''
  )::bigint AS record_set_node_count,
  NULLIF(
    coalesce(
      wr.last_applied_rev->'record_set_summary'->>'edge_count',
      srb.record_set_summary->>'edge_count',
      srs.record_set_summary->>'edge_count'
    ),
    ''
  )::bigint AS record_set_edge_count,
  cb.cursor_blob_count,
  cb.cursor_blob_bytes,
  aw.workflow_id AS active_workflow_id,
  aw.workflow_status,
  aw.run_mode AS workflow_run_mode,
  aw.active_work_stale,
  aw.active_work_age_seconds,
  CASE
    WHEN aw.workflow_id IS NOT NULL AND aw.active_work_stale THEN 'stale'
    WHEN aw.workflow_id IS NOT NULL THEN 'active'
    WHEN coalesce(aws.unattributed_stale_count, 0) > 0
      THEN 'stale_unattributed'
    WHEN coalesce(aws.unattributed_active_count, 0) > 0
      THEN 'active_unattributed'
    WHEN aw.attribution_state IS NULL THEN 'not_observed'
    ELSE aw.attribution_state
  END AS runtime_state,
  rv.read_visibility_state,
  CASE
    WHEN nullif(coalesce(
      lr.payload->>'last_error',
      lr.payload->>'error',
      lr.payload->>'failure'
    ), '') IS NOT NULL THEN 'blocked'
    WHEN coalesce(wr.replay_required, '{}'::jsonb) <> '{}'::jsonb
      THEN 'blocked'
    WHEN aw.workflow_id IS NOT NULL AND aw.active_work_stale THEN 'blocked'
    WHEN coalesce(aws.unattributed_stale_count, 0) > 0 THEN 'blocked'
    WHEN aw.workflow_id IS NOT NULL THEN 'replaying'
    WHEN coalesce(aws.unattributed_active_count, 0) > 0 THEN 'replaying'
    WHEN rv.read_visibility_state = 'read_visible'
      AND wr.last_reconcile_at IS NOT NULL THEN 'fresh'
    WHEN rv.read_visibility_state = 'read_visible' THEN 'read_visible'
    WHEN wr.source IS NULL AND ps.source_name IS NULL THEN 'unknown'
    ELSE 'partial'
  END AS frontier_state
FROM source_keys sk
LEFT JOIN progress_seen ps
  ON ps.deployment IS NOT DISTINCT FROM sk.deployment
 AND ps.registry_source_name = sk.registry_source_name
LEFT JOIN latest_runs lr
  ON lr.deployment IS NOT DISTINCT FROM ps.deployment
 AND lr.source_name = ps.source_name
LEFT JOIN watermark_rows wr
  ON (
       wr.deployment_guess IS NOT DISTINCT FROM sk.deployment
       AND wr.registry_source_name = sk.registry_source_name
     )
  OR (
       wr.deployment_guess IS NULL
       AND wr.registry_source_name = sk.registry_source_name
     )
LEFT JOIN active_work aw
  ON aw.deployment IS NOT DISTINCT FROM coalesce(sk.deployment, ps.deployment)
 AND aw.registry_source_name = sk.registry_source_name
LEFT JOIN active_work_summary aws
  ON aws.deployment IS NOT DISTINCT FROM coalesce(
       sk.deployment,
       ps.deployment,
       wr.deployment_guess,
       aw.deployment
     )
LEFT JOIN latest_published lp
  ON lp.deployment IS NOT DISTINCT FROM coalesce(
       sk.deployment,
       ps.deployment,
       wr.deployment_guess,
       aw.deployment
     )
LEFT JOIN okg.source_record_sets srs
  ON srs.source = wr.source
LEFT JOIN okg.source_record_set_blobs srb
  ON srb.source = wr.source
 AND srb.sha256 = wr.last_applied_rev->'record_set_ref'->>'sha256'
LEFT JOIN cursor_blobs cb
  ON cb.source = wr.source
LEFT JOIN LATERAL (
  SELECT CASE
    WHEN lp.generation_id IS NULL THEN 'no_published_generation'
    WHEN wr.source IS NULL THEN 'no_watermark'
    WHEN coalesce(wr.replay_required, '{}'::jsonb) <> '{}'::jsonb
      THEN 'replay_required'
    WHEN aw.workflow_id IS NOT NULL THEN 'frontier_advancing'
    WHEN coalesce(aws.unattributed_active_count, 0) > 0
      THEN 'frontier_advancing'
    WHEN coalesce(wr.last_published_rev, '{}'::jsonb) = '{}'::jsonb
      THEN 'unpublished_watermark'
    WHEN wr.last_applied_rev ? 'record_set_ref' AND srb.source IS NULL
      THEN 'record_set_blob_missing'
    ELSE 'read_visible'
  END AS read_visibility_state
) rv ON true
ORDER BY deployment NULLS LAST, registry_source_name, source_name;
$_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _compressed_hypertable_10; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_10 (
);


--
-- Name: _compressed_hypertable_12; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_12 (
);


--
-- Name: _compressed_hypertable_14; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_14 (
);


--
-- Name: _compressed_hypertable_16; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_16 (
);


--
-- Name: _compressed_hypertable_3; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_3 (
);


--
-- Name: _compressed_hypertable_4; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_4 (
);


--
-- Name: _compressed_hypertable_6; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_6 (
);


--
-- Name: _compressed_hypertable_8; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_8 (
);


--
-- Name: admission_envelopes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.admission_envelopes (
    admission_id uuid DEFAULT gen_random_uuid() NOT NULL,
    deployment_name text NOT NULL,
    producer_id text NOT NULL,
    producer_kind text NOT NULL,
    intent text NOT NULL,
    admission_mode text NOT NULL,
    status text NOT NULL,
    catalog_version_id bigint,
    source text,
    run_id uuid,
    target_generation_id bigint,
    idempotency_key text NOT NULL,
    authority jsonb DEFAULT '{}'::jsonb NOT NULL,
    check_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    verdict jsonb DEFAULT '{}'::jsonb NOT NULL,
    workflow_ref jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    decided_at timestamp with time zone,
    published_at timestamp with time zone,
    target_branch_name text,
    target_generation_ref text,
    CONSTRAINT admission_envelopes_admission_mode_check CHECK ((admission_mode = ANY (ARRAY['direct'::text, 'fast_track'::text, 'review'::text, 'quarantine'::text]))),
    CONSTRAINT admission_envelopes_producer_kind_check CHECK ((producer_kind = ANY (ARRAY['source'::text, 'extractor'::text, 'enricher'::text, 'agent'::text, 'human'::text, 'workflow'::text, 'system'::text, 'lens'::text]))),
    CONSTRAINT admission_envelopes_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'accepted'::text, 'rejected'::text, 'published'::text, 'blocked'::text, 'failed'::text, 'quarantined'::text])))
);


--
-- Name: COLUMN admission_envelopes.target_generation_id; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON COLUMN okg.admission_envelopes.target_generation_id IS 'Legacy numeric compatibility generation token. Active Chronos-aware consumers should prefer target_generation_ref.';


--
-- Name: COLUMN admission_envelopes.target_generation_ref; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON COLUMN okg.admission_envelopes.target_generation_ref IS 'Canonical text generation reference for the Chronos generation targeted by this admission envelope.';


--
-- Name: admission_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.admission_events (
    event_id bigint NOT NULL,
    admission_id uuid NOT NULL,
    event_type text NOT NULL,
    actor text,
    reason text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    workflow_ref jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT admission_events_event_type_check CHECK ((event_type = ANY (ARRAY['created'::text, 'duplicate_coalesced'::text, 'submitted'::text, 'accepted'::text, 'rejected'::text, 'quarantined'::text, 'published'::text, 'blocked'::text, 'failed'::text, 'refs_attached'::text])))
);


--
-- Name: admission_events_event_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.admission_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: admission_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.admission_events_event_id_seq OWNED BY okg.admission_events.event_id;


--
-- Name: admission_record_refs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.admission_record_refs (
    ref_id bigint NOT NULL,
    admission_id uuid NOT NULL,
    ref_key text NOT NULL,
    ref_kind text NOT NULL,
    source text,
    run_id uuid,
    fact_id_min bigint,
    fact_id_max bigint,
    record_count bigint DEFAULT 0 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT admission_record_refs_record_count_check CHECK ((record_count >= 0)),
    CONSTRAINT admission_record_refs_ref_kind_check CHECK ((ref_kind = ANY (ARRAY['node_fact_range'::text, 'edge_fact_range'::text, 'payload'::text])))
);


--
-- Name: admission_record_refs_ref_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.admission_record_refs_ref_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: admission_record_refs_ref_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.admission_record_refs_ref_id_seq OWNED BY okg.admission_record_refs.ref_id;


--
-- Name: alias_collisions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.alias_collisions (
    catalog_version_id bigint NOT NULL,
    subtype text NOT NULL,
    field text NOT NULL,
    alias_value text NOT NULL,
    scope text,
    canonicals jsonb NOT NULL,
    distinct_canonical_count integer NOT NULL,
    detected_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: aliases; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.aliases (
    alias text NOT NULL,
    normalized_alias text,
    canonical text NOT NULL,
    scope text,
    catalog_version_id bigint NOT NULL
);


--
-- Name: attribute_effective_attrs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_effective_attrs (
    target_kind text NOT NULL,
    target_id text NOT NULL,
    generation_id text NOT NULL,
    namespace text DEFAULT 'default'::text NOT NULL,
    attr_key text NOT NULL,
    observation_id text NOT NULL,
    value jsonb NOT NULL,
    value_hash text NOT NULL,
    producer text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    dependencies jsonb DEFAULT '{}'::jsonb NOT NULL,
    dependency_fingerprint text,
    confidence double precision,
    materialized_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT attribute_effective_attrs_target_kind_check CHECK ((target_kind = ANY (ARRAY['node'::text, 'edge'::text])))
);


--
-- Name: attribute_effective_integrity_receipts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_effective_integrity_receipts (
    receipt_digest text NOT NULL,
    receipt_type text NOT NULL,
    deployment_name text NOT NULL,
    branch_id text NOT NULL,
    namespace text NOT NULL,
    generation_id text NOT NULL,
    ordinal bigint NOT NULL,
    prior_receipt_digest text,
    checkpoint_node_count bigint NOT NULL,
    checkpoint_edge_count bigint NOT NULL,
    logical_node_count bigint NOT NULL,
    logical_edge_count bigint NOT NULL,
    schema_version integer NOT NULL,
    schema_digest text NOT NULL,
    migration_digest text NOT NULL,
    logical_payload_seed0 numeric NOT NULL,
    logical_payload_seed1 numeric NOT NULL,
    interval_payload_seed0 numeric NOT NULL,
    interval_payload_seed1 numeric NOT NULL,
    dirty_scope_digest text NOT NULL,
    mutation_epoch bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT attribute_effective_integrity_receipts_receipt_type_check CHECK ((receipt_type = ANY (ARRAY['anchor'::text, 'incremental'::text])))
);


--
-- Name: attribute_effective_integrity_state; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_effective_integrity_state (
    deployment_name text NOT NULL,
    branch_id text NOT NULL,
    namespace text NOT NULL,
    mutation_epoch bigint DEFAULT 0 NOT NULL,
    clean_mutation_epoch bigint,
    dirty boolean DEFAULT true NOT NULL,
    dirty_reason text DEFAULT 'unanchored'::text NOT NULL,
    latest_receipt_digest text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: attribute_effective_integrity_transitions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_effective_integrity_transitions (
    deployment_name text NOT NULL,
    branch_id text NOT NULL,
    namespace text NOT NULL,
    backend_pid integer NOT NULL,
    backend_start timestamp with time zone NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: attribute_effective_view; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_effective_view (
    target_kind text NOT NULL,
    target_id text NOT NULL,
    generation_id text NOT NULL,
    namespace text DEFAULT 'default'::text NOT NULL,
    subtype text,
    src text,
    dst text,
    edge_type text,
    raw_attrs jsonb,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    overlay_count integer DEFAULT 0 NOT NULL,
    raw_text_hash text,
    materialized_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT attribute_effective_view_target_kind_check CHECK ((target_kind = ANY (ARRAY['node'::text, 'edge'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.05', autovacuum_vacuum_cost_limit='2000');


--
-- Name: attribute_effective_view_intervals; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_effective_view_intervals (
    target_kind text NOT NULL,
    target_id text NOT NULL,
    namespace text DEFAULT 'default'::text NOT NULL,
    branch_id text NOT NULL,
    from_ord bigint NOT NULL,
    to_ord bigint DEFAULT '9223372036854775807'::bigint NOT NULL,
    subtype text,
    src text,
    dst text,
    edge_type text,
    raw_attrs jsonb,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    overlay_count integer DEFAULT 0 NOT NULL,
    raw_text_hash text,
    materialized_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT attribute_effective_view_intervals_check CHECK ((from_ord < to_ord)),
    CONSTRAINT attribute_effective_view_intervals_target_kind_check CHECK ((target_kind = ANY (ARRAY['node'::text, 'edge'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.05', autovacuum_vacuum_cost_limit='2000');


--
-- Name: attribute_observations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.attribute_observations (
    observation_id text NOT NULL,
    target_kind text NOT NULL,
    target_id text NOT NULL,
    generation_id text NOT NULL,
    namespace text DEFAULT 'default'::text NOT NULL,
    attr_key text NOT NULL,
    value jsonb NOT NULL,
    value_hash text NOT NULL,
    status text NOT NULL,
    producer text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    dependencies jsonb DEFAULT '{}'::jsonb NOT NULL,
    dependency_fingerprint text,
    supersedes text,
    superseded_by text,
    decision jsonb DEFAULT '{}'::jsonb NOT NULL,
    confidence double precision,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT attribute_observations_status_check CHECK ((status = ANY (ARRAY['candidate'::text, 'accepted'::text, 'rejected'::text, 'superseded'::text, 'stale'::text]))),
    CONSTRAINT attribute_observations_target_kind_check CHECK ((target_kind = ANY (ARRAY['node'::text, 'edge'::text])))
);


--
-- Name: branch_merge_requests; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.branch_merge_requests (
    request_id bigint NOT NULL,
    deployment_name text NOT NULL,
    source_branch_id bigint NOT NULL,
    target_branch_id bigint NOT NULL,
    status text NOT NULL,
    predicate jsonb DEFAULT '{}'::jsonb NOT NULL,
    verdict jsonb DEFAULT '{}'::jsonb NOT NULL,
    decided_by text,
    decided_at timestamp with time zone,
    applied_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT branch_merge_requests_check CHECK ((source_branch_id <> target_branch_id)),
    CONSTRAINT branch_merge_requests_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'approved'::text, 'rejected'::text, 'applied'::text, 'withdrawn'::text])))
);


--
-- Name: branch_merge_requests_request_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.branch_merge_requests_request_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: branch_merge_requests_request_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.branch_merge_requests_request_id_seq OWNED BY okg.branch_merge_requests.request_id;


--
-- Name: branches; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.branches (
    branch_id bigint NOT NULL,
    deployment_name text NOT NULL,
    name text NOT NULL,
    role text NOT NULL,
    base_branch_id bigint,
    base_generation_id bigint,
    status text NOT NULL,
    policy_clauses jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT branches_role_check CHECK ((role = ANY (ARRAY['default'::text, 'quarantine'::text, 'experiment'::text]))),
    CONSTRAINT branches_status_check CHECK ((status = ANY (ARRAY['active'::text, 'merged'::text, 'abandoned'::text, 'superseded'::text])))
);


--
-- Name: branches_branch_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.branches_branch_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: branches_branch_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.branches_branch_id_seq OWNED BY okg.branches.branch_id;


--
-- Name: catalog_versions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.catalog_versions (
    catalog_version_id bigint NOT NULL,
    status text NOT NULL,
    applied_at timestamp with time zone,
    diff_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    reconcile_plan jsonb DEFAULT '{}'::jsonb NOT NULL,
    deployment_name text,
    catalog_fingerprint text,
    schema_fingerprint text,
    writer_fingerprint text,
    writer_provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_by_command text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT catalog_versions_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'applied'::text, 'rolled_back'::text])))
);


--
-- Name: catalog_versions_catalog_version_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.catalog_versions_catalog_version_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: catalog_versions_catalog_version_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.catalog_versions_catalog_version_id_seq OWNED BY okg.catalog_versions.catalog_version_id;


--
-- Name: chronos_hot_index_manifest; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.chronos_hot_index_manifest (
    logical_table text NOT NULL,
    physical_table text NOT NULL,
    suffix text NOT NULL,
    full_index_name text NOT NULL,
    hot_index_name text NOT NULL,
    floor numeric(32,0) NOT NULL,
    headroom_generations integer NOT NULL,
    floor_checkpoint_segment_id bigint NOT NULL,
    floor_checkpoint_branch_point numeric(32,0) NOT NULL,
    open_interval_value numeric(32,0) NOT NULL,
    state text NOT NULL,
    index_definition text,
    index_bytes_at_activation bigint,
    failure text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    activated_at timestamp with time zone,
    CONSTRAINT chronos_hot_index_manifest_headroom_generations_check CHECK ((headroom_generations > 0)),
    CONSTRAINT chronos_hot_index_manifest_state_check CHECK ((state = ANY (ARRAY['building'::text, 'proof'::text, 'activating'::text, 'active'::text, 'retired'::text, 'failed'::text])))
);


--
-- Name: TABLE chronos_hot_index_manifest; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON TABLE okg.chronos_hot_index_manifest IS 'Runtime-derived Chronos live-front index authority; floors and open values are observed store facts, never code constants.';


--
-- Name: chronos_native_graph_storage_marker; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.chronos_native_graph_storage_marker (
    enabled_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'chronos_native_graph_storage'::text NOT NULL
);


--
-- Name: compacted_generations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.compacted_generations (
    checkpoint_id text NOT NULL,
    target_generation_id bigint,
    deployment_name text NOT NULL,
    branch_id text NOT NULL,
    plan_id uuid NOT NULL,
    compacted_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'history_retention'::text NOT NULL
);


--
-- Name: consistency_candidate_coverage; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.consistency_candidate_coverage (
    run_id uuid NOT NULL,
    check_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    universe_key text NOT NULL,
    coverage_basis text NOT NULL,
    coverage_complete boolean NOT NULL,
    scoped boolean DEFAULT false NOT NULL,
    scope_subject_count bigint DEFAULT 0 NOT NULL,
    eligible_input_count bigint DEFAULT 0 NOT NULL,
    excluded_input_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    missing_text_count bigint DEFAULT 0 NOT NULL,
    missing_embedding_count bigint DEFAULT 0 NOT NULL,
    block_count bigint DEFAULT 0 NOT NULL,
    candidate_count bigint DEFAULT 0 NOT NULL,
    candidate_count_by_block jsonb DEFAULT '{}'::jsonb NOT NULL,
    largest_block_size bigint DEFAULT 0 NOT NULL,
    p95_block_size numeric(18,4) DEFAULT 0 NOT NULL,
    all_pairs_upper_bound bigint DEFAULT 0 NOT NULL,
    reduction_ratio numeric(18,10) DEFAULT 0 NOT NULL,
    evaluator_invocation_count bigint DEFAULT 0 NOT NULL,
    evaluator_cost jsonb DEFAULT '{}'::jsonb NOT NULL,
    false_negative_audit jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT consistency_candidate_coverage_all_pairs_upper_bound_check CHECK ((all_pairs_upper_bound >= 0)),
    CONSTRAINT consistency_candidate_coverage_block_count_check CHECK ((block_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_candidate_count_check CHECK ((candidate_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_check CHECK (((NOT coverage_complete) OR (coverage_basis = ANY (ARRAY['exact_postgres'::text, 'exact_snapshot'::text])))),
    CONSTRAINT consistency_candidate_coverage_check1 CHECK (((NOT coverage_complete) OR ((missing_text_count = 0) AND (missing_embedding_count = 0)))),
    CONSTRAINT consistency_candidate_coverage_coverage_basis_check CHECK ((coverage_basis = ANY (ARRAY['exact_postgres'::text, 'exact_snapshot'::text, 'heuristic'::text, 'sampled_monitoring'::text, 'incomplete'::text]))),
    CONSTRAINT consistency_candidate_coverage_eligible_input_count_check CHECK ((eligible_input_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_evaluator_invocation_count_check CHECK ((evaluator_invocation_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_largest_block_size_check CHECK ((largest_block_size >= 0)),
    CONSTRAINT consistency_candidate_coverage_missing_embedding_count_check CHECK ((missing_embedding_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_missing_text_count_check CHECK ((missing_text_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_p95_block_size_check CHECK ((p95_block_size >= (0)::numeric)),
    CONSTRAINT consistency_candidate_coverage_reduction_ratio_check CHECK ((reduction_ratio >= (0)::numeric)),
    CONSTRAINT consistency_candidate_coverage_scope_subject_count_check CHECK ((scope_subject_count >= 0)),
    CONSTRAINT consistency_candidate_coverage_scoped_complete_check CHECK ((NOT (coverage_complete AND scoped)))
);


--
-- Name: consistency_candidates; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.consistency_candidates (
    check_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    candidate_id text NOT NULL,
    run_id uuid,
    input_scope text NOT NULL,
    blocking_rule_hash text NOT NULL,
    participant_roles jsonb NOT NULL,
    participant_anchors jsonb NOT NULL,
    predicate_key text NOT NULL,
    generation_policy text NOT NULL,
    coverage_status text NOT NULL,
    exclusion_reason text,
    candidate_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT consistency_candidates_coverage_status_check CHECK ((coverage_status = ANY (ARRAY['candidate'::text, 'excluded'::text, 'not_evaluated'::text])))
);


--
-- Name: consistency_check_definitions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.consistency_check_definitions (
    check_id text NOT NULL,
    deployment_name text NOT NULL,
    predicate_family text NOT NULL,
    candidate_rule_hash text NOT NULL,
    evaluator_ref text NOT NULL,
    evaluator_hash text NOT NULL,
    confidence_type text NOT NULL,
    coverage_contract jsonb DEFAULT '{}'::jsonb NOT NULL,
    severity_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    check_kind text,
    CONSTRAINT consistency_check_definitions_confidence_type_check CHECK ((confidence_type = ANY (ARRAY['deterministic_boolean'::text, 'calibrated_probability'::text, 'thresholded_score'::text, 'ordinal_label'::text]))),
    CONSTRAINT consistency_check_definitions_kind_check CHECK (((check_kind IS NULL) OR (check_kind = ANY (ARRAY['rule_based'::text, 'security'::text, 'statistical'::text, 'semantic'::text]))))
);


--
-- Name: consistency_gate_results; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.consistency_gate_results (
    result_id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    check_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    gate_name text NOT NULL,
    result text NOT NULL,
    blocks_publish boolean NOT NULL,
    reason text NOT NULL,
    open_observation_count bigint DEFAULT 0 NOT NULL,
    newly_opened_count bigint DEFAULT 0 NOT NULL,
    resolved_count bigint DEFAULT 0 NOT NULL,
    is_probe boolean DEFAULT false NOT NULL,
    coverage_complete boolean,
    coverage_basis text,
    budget_ms bigint,
    elapsed_ms bigint DEFAULT 0 NOT NULL,
    preflight jsonb DEFAULT '{}'::jsonb NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT consistency_gate_results_budget_ms_check CHECK (((budget_ms IS NULL) OR (budget_ms >= 0))),
    CONSTRAINT consistency_gate_results_elapsed_ms_check CHECK ((elapsed_ms >= 0)),
    CONSTRAINT consistency_gate_results_newly_opened_count_check CHECK ((newly_opened_count >= 0)),
    CONSTRAINT consistency_gate_results_open_observation_count_check CHECK ((open_observation_count >= 0)),
    CONSTRAINT consistency_gate_results_resolved_count_check CHECK ((resolved_count >= 0)),
    CONSTRAINT consistency_gate_results_result_check CHECK ((result = ANY (ARRAY['evaluated'::text, 'blocked_incomplete'::text, 'blocked_timeout'::text, 'blocked_error'::text])))
);


--
-- Name: console_count_rollup; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.console_count_rollup (
    dimension_kind text NOT NULL,
    dimension_value text NOT NULL,
    item_count bigint NOT NULL
);


--
-- Name: coordination_native_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.coordination_native_events (
    event_id text NOT NULL,
    deployment_name text NOT NULL,
    event_type text NOT NULL,
    change_id text NOT NULL,
    task_id text NOT NULL,
    actor_id text NOT NULL,
    actor_session_id text NOT NULL,
    role_agent_id text,
    workspace_id text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    idempotency_key text NOT NULL,
    expected_revision bigint NOT NULL,
    resulting_revision bigint NOT NULL,
    status text NOT NULL,
    rejection_code text,
    rejection_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT coordination_native_events_status_check CHECK ((status = ANY (ARRAY['accepted'::text, 'rejected'::text])))
);


--
-- Name: coordination_native_state; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.coordination_native_state (
    deployment_name text NOT NULL,
    change_id text NOT NULL,
    task_id text NOT NULL,
    work_item_id text NOT NULL,
    revision bigint DEFAULT 0 NOT NULL,
    current_authority text DEFAULT 'native'::text NOT NULL,
    active_claim_owner text,
    workspace_id text,
    claim_state text DEFAULT 'unclaimed'::text NOT NULL,
    lease_state text DEFAULT 'none'::text NOT NULL,
    lease_fresh boolean DEFAULT false NOT NULL,
    blocked boolean DEFAULT false NOT NULL,
    blocker_reason text,
    proof_attached boolean DEFAULT false NOT NULL,
    proof_refs jsonb DEFAULT '[]'::jsonb NOT NULL,
    review_state text DEFAULT 'none'::text NOT NULL,
    merge_ready boolean DEFAULT false NOT NULL,
    supervisor_finalized boolean DEFAULT false NOT NULL,
    subagents_rolled_up boolean DEFAULT false NOT NULL,
    finalized_state text,
    task_done boolean DEFAULT false NOT NULL,
    last_heartbeat_at timestamp with time zone,
    last_operator_report_kind text,
    last_event_id text,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: database_owner; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.database_owner (
    singleton boolean DEFAULT true NOT NULL,
    owner_deployment_name text NOT NULL,
    db_owner_uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    schema_fingerprint text,
    stamped_at timestamp with time zone DEFAULT now() NOT NULL,
    stamped_by_command text,
    CONSTRAINT database_owner_singleton_check CHECK (singleton)
);


--
-- Name: deployment_apply_runs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.deployment_apply_runs (
    deployment_name text NOT NULL,
    desired_state_digest text NOT NULL,
    plan_id text NOT NULL,
    status text NOT NULL,
    current_stage text,
    desired_state jsonb NOT NULL,
    completed_stages jsonb DEFAULT '[]'::jsonb NOT NULL,
    stage_receipts jsonb DEFAULT '{}'::jsonb NOT NULL,
    error jsonb,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT deployment_apply_runs_status_check CHECK ((status = ANY (ARRAY['applying'::text, 'failed'::text, 'complete'::text])))
);


--
-- Name: deployment_promotion_fences; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.deployment_promotion_fences (
    deployment_name text NOT NULL,
    promotion_id text NOT NULL,
    status text DEFAULT 'fenced'::text NOT NULL,
    schedules_removed integer DEFAULT 0 NOT NULL,
    schedule_snapshot jsonb DEFAULT '[]'::jsonb NOT NULL,
    fenced_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    blocker jsonb,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT deployment_promotion_fences_schedules_removed_check CHECK ((schedules_removed >= 0)),
    CONSTRAINT deployment_promotion_fences_status_check CHECK ((status = ANY (ARRAY['fenced'::text, 'recovery_blocked'::text, 'released'::text])))
);


--
-- Name: deployment_worker_ack; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.deployment_worker_ack (
    deployment_name text NOT NULL,
    desired_state_digest text NOT NULL,
    source_registry_fingerprint text NOT NULL,
    source_fingerprints jsonb,
    runtime_fingerprint text,
    worker_config_fingerprint text,
    service_authority jsonb,
    observed_runtime_authority jsonb,
    manifest_fingerprint text,
    catalog_fingerprint text,
    schema_fingerprint text,
    writer_fingerprint text,
    schedule_fingerprint text,
    active_sources jsonb NOT NULL,
    active_schedules jsonb NOT NULL,
    acknowledged_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: deployment_write_ownership; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.deployment_write_ownership (
    deployment_name text NOT NULL,
    owner_fingerprint text NOT NULL,
    schema_fingerprint text NOT NULL,
    catalog_fingerprint text NOT NULL,
    deployment_root_hash text,
    worktree_root_hash text,
    status text DEFAULT 'active'::text NOT NULL,
    claimed_at timestamp with time zone DEFAULT now() NOT NULL,
    claimed_by_command text,
    last_seen_catalog_version_id bigint,
    manifest_fingerprint text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT deployment_write_ownership_status_check CHECK ((status = ANY (ARRAY['active'::text, 'released'::text])))
);


--
-- Name: derived_artifact_definitions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_artifact_definitions (
    definition_id text NOT NULL,
    artifact_kind text NOT NULL,
    deployment_name text NOT NULL,
    logical_key text NOT NULL,
    compute_hash text NOT NULL,
    output_schema_hash text NOT NULL,
    renderer_hash text NOT NULL,
    dirty_rule_hash text NOT NULL,
    renderer_ref text NOT NULL,
    output_contract jsonb DEFAULT '{}'::jsonb NOT NULL,
    dirty_rule jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT derived_artifact_definitions_artifact_kind_check CHECK ((artifact_kind = ANY (ARRAY['derived_artifact'::text, 'view'::text, 'consistency_check'::text, 'lens'::text])))
);


--
-- Name: derived_artifact_dependencies; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_artifact_dependencies (
    dependency_id uuid DEFAULT gen_random_uuid() NOT NULL,
    downstream_definition_id text NOT NULL,
    upstream_definition_id text NOT NULL,
    upstream_slot_name text DEFAULT 'relation'::text NOT NULL,
    dependency_kind text NOT NULL,
    scope_key text,
    required_compute_hash text,
    required_output_schema_hash text,
    required_renderer_hash text,
    required_dirty_rule_hash text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT derived_artifact_dependencies_check CHECK ((downstream_definition_id <> upstream_definition_id)),
    CONSTRAINT derived_artifact_dependencies_dependency_kind_check CHECK ((dependency_kind = 'manifest_slot'::text))
);


--
-- Name: derived_artifact_runs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_artifact_runs (
    run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    status text NOT NULL,
    trigger_kind text NOT NULL,
    compute_hash text NOT NULL,
    output_schema_hash text NOT NULL,
    renderer_hash text NOT NULL,
    dirty_rule_hash text NOT NULL,
    dependency_manifest_ids uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    output_manifest_id uuid,
    reused_from_run_id uuid,
    total_scopes bigint DEFAULT 0 NOT NULL,
    dirty_scopes bigint DEFAULT 0 NOT NULL,
    dirty_rows bigint DEFAULT 0 NOT NULL,
    reused_scopes bigint DEFAULT 0 NOT NULL,
    fallback_count bigint DEFAULT 0 NOT NULL,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    error jsonb,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT derived_artifact_runs_status_check CHECK ((status = ANY (ARRAY['running'::text, 'completed'::text, 'failed'::text, 'carried_forward'::text, 'blocked'::text]))),
    CONSTRAINT derived_artifact_runs_trigger_kind_check CHECK ((trigger_kind = ANY (ARRAY['dirty'::text, 'full'::text, 'carry_forward'::text, 'manual'::text, 'migration'::text, 'post_publish'::text])))
);


--
-- Name: derived_dirty_rule_audits; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_dirty_rule_audits (
    audit_id uuid DEFAULT gen_random_uuid() NOT NULL,
    dirty_rule_run_id uuid NOT NULL,
    definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    audit_kind text NOT NULL,
    status text NOT NULL,
    expected_scope_count bigint DEFAULT 0 NOT NULL,
    observed_scope_count bigint DEFAULT 0 NOT NULL,
    missed_scope_count bigint DEFAULT 0 NOT NULL,
    missed_scope_keys jsonb DEFAULT '[]'::jsonb NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT derived_dirty_rule_audits_audit_kind_check CHECK ((audit_kind = 'missed_dirty'::text)),
    CONSTRAINT derived_dirty_rule_audits_expected_scope_count_check CHECK ((expected_scope_count >= 0)),
    CONSTRAINT derived_dirty_rule_audits_missed_scope_count_check CHECK ((missed_scope_count >= 0)),
    CONSTRAINT derived_dirty_rule_audits_observed_scope_count_check CHECK ((observed_scope_count >= 0)),
    CONSTRAINT derived_dirty_rule_audits_status_check CHECK ((status = ANY (ARRAY['passed'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: derived_dirty_rule_runs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_dirty_rule_runs (
    dirty_rule_run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    dirty_rule_hash text NOT NULL,
    scope_universe_hash text NOT NULL,
    dirty_query_hash text NOT NULL,
    audit_query_hash text,
    status text NOT NULL,
    fallback_kind text DEFAULT 'none'::text NOT NULL,
    total_scope_count bigint DEFAULT 0 NOT NULL,
    dirty_scope_count bigint DEFAULT 0 NOT NULL,
    fallback_scope_count bigint DEFAULT 0 NOT NULL,
    missed_scope_count bigint DEFAULT 0 NOT NULL,
    elapsed_ms bigint DEFAULT 0 NOT NULL,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    error jsonb,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT derived_dirty_rule_runs_dirty_scope_count_check CHECK ((dirty_scope_count >= 0)),
    CONSTRAINT derived_dirty_rule_runs_elapsed_ms_check CHECK ((elapsed_ms >= 0)),
    CONSTRAINT derived_dirty_rule_runs_fallback_kind_check CHECK ((fallback_kind = ANY (ARRAY['none'::text, 'full_scope_universe'::text]))),
    CONSTRAINT derived_dirty_rule_runs_fallback_scope_count_check CHECK ((fallback_scope_count >= 0)),
    CONSTRAINT derived_dirty_rule_runs_missed_scope_count_check CHECK ((missed_scope_count >= 0)),
    CONSTRAINT derived_dirty_rule_runs_status_check CHECK ((status = ANY (ARRAY['running'::text, 'completed'::text, 'failed'::text, 'audit_failed'::text]))),
    CONSTRAINT derived_dirty_rule_runs_total_scope_count_check CHECK ((total_scope_count >= 0))
);


--
-- Name: derived_dirty_scopes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_dirty_scopes (
    dirty_scope_id bigint NOT NULL,
    dirty_rule_run_id uuid,
    definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    scope_key text NOT NULL,
    dirty_reason text NOT NULL,
    dependency_kind text NOT NULL,
    dependency_ref text NOT NULL,
    dependency_hash text,
    queued_at timestamp with time zone DEFAULT now() NOT NULL,
    consumed_run_id uuid,
    consumed_at timestamp with time zone
);


--
-- Name: derived_dirty_scopes_dirty_scope_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.derived_dirty_scopes_dirty_scope_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: derived_dirty_scopes_dirty_scope_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.derived_dirty_scopes_dirty_scope_id_seq OWNED BY okg.derived_dirty_scopes.dirty_scope_id;


--
-- Name: derived_output_dependency_index; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_output_dependency_index (
    dependency_index_id bigint NOT NULL,
    lineage_envelope_id uuid NOT NULL,
    manifest_id uuid NOT NULL,
    output_id text NOT NULL,
    dependency_kind text NOT NULL,
    dependency_ref text NOT NULL,
    dependency_key_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: derived_output_dependency_index_dependency_index_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.derived_output_dependency_index_dependency_index_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: derived_output_dependency_index_dependency_index_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.derived_output_dependency_index_dependency_index_id_seq OWNED BY okg.derived_output_dependency_index.dependency_index_id;


--
-- Name: derived_output_lineage_envelopes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_output_lineage_envelopes (
    lineage_envelope_id uuid DEFAULT gen_random_uuid() NOT NULL,
    manifest_id uuid NOT NULL,
    definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    output_id text NOT NULL,
    output_kind text NOT NULL,
    lineage_schema text NOT NULL,
    envelope_hash text NOT NULL,
    output_digest text NOT NULL,
    previous_output_digest text,
    component_id text NOT NULL,
    component_hash text NOT NULL,
    invalidation_scope text NOT NULL,
    envelope jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: derived_output_manifests; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_output_manifests (
    manifest_id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    status text NOT NULL,
    required_slots text[] DEFAULT '{}'::text[] NOT NULL,
    manifest_hash text NOT NULL,
    reused_from_manifest_id uuid,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    staged_at timestamp with time zone DEFAULT now() NOT NULL,
    promoted_at timestamp with time zone,
    failed_at timestamp with time zone,
    CONSTRAINT derived_output_manifests_status_check CHECK ((status = ANY (ARRAY['staged'::text, 'completed'::text, 'failed'::text])))
);


--
-- Name: derived_output_slots; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_output_slots (
    slot_id uuid DEFAULT gen_random_uuid() NOT NULL,
    manifest_id uuid NOT NULL,
    slot_name text NOT NULL,
    storage_kind text NOT NULL,
    media_type text NOT NULL,
    artifact_type text NOT NULL,
    renderer_ref text NOT NULL,
    payload_hash text NOT NULL,
    storage_ref jsonb NOT NULL,
    inline_payload jsonb,
    row_count bigint,
    byte_count bigint,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT derived_output_slots_inline_payload_chk CHECK (((storage_kind <> 'inline'::text) OR (inline_payload IS NOT NULL))),
    CONSTRAINT derived_output_slots_storage_kind_check CHECK ((storage_kind = ANY (ARRAY['relation'::text, 'inline'::text, 'graph_projection'::text, 'compatibility'::text])))
);


--
-- Name: derived_relation_registry; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_relation_registry (
    relation_id uuid DEFAULT gen_random_uuid() NOT NULL,
    definition_id text NOT NULL,
    logical_relation_id text NOT NULL,
    schema_name text DEFAULT 'okg_derived'::text NOT NULL,
    physical_name text NOT NULL,
    output_schema_hash text NOT NULL,
    ddl_hash text NOT NULL,
    partition_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    index_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    retention_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    retired_at timestamp with time zone,
    CONSTRAINT derived_relation_registry_status_check CHECK ((status = ANY (ARRAY['active'::text, 'retired'::text])))
);


--
-- Name: derived_run_dependency_bindings; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_run_dependency_bindings (
    binding_id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    downstream_definition_id text NOT NULL,
    upstream_definition_id text NOT NULL,
    upstream_manifest_id uuid NOT NULL,
    upstream_slot_name text NOT NULL,
    upstream_target_generation_id bigint NOT NULL,
    scope_key text,
    required_compute_hash text,
    required_output_schema_hash text,
    required_renderer_hash text,
    required_dirty_rule_hash text,
    bound_compute_hash text NOT NULL,
    bound_output_schema_hash text NOT NULL,
    bound_renderer_hash text NOT NULL,
    bound_dirty_rule_hash text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: derived_schedule_nodes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_schedule_nodes (
    plan_id uuid NOT NULL,
    definition_id text NOT NULL,
    topo_index integer NOT NULL,
    status text NOT NULL,
    run_id uuid,
    dirty_scope_count bigint DEFAULT 0 NOT NULL,
    upstream_binding_count bigint DEFAULT 0 NOT NULL,
    error jsonb,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    CONSTRAINT derived_schedule_nodes_dirty_scope_count_check CHECK ((dirty_scope_count >= 0)),
    CONSTRAINT derived_schedule_nodes_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'running'::text, 'completed'::text, 'carried_forward'::text, 'failed'::text]))),
    CONSTRAINT derived_schedule_nodes_upstream_binding_count_check CHECK ((upstream_binding_count >= 0))
);


--
-- Name: derived_schedule_plans; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.derived_schedule_plans (
    plan_id uuid DEFAULT gen_random_uuid() NOT NULL,
    deployment_name text NOT NULL,
    target_generation_id bigint NOT NULL,
    execution_phase text NOT NULL,
    catalog_version_id bigint,
    graph_hash text NOT NULL,
    status text NOT NULL,
    error jsonb,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT derived_schedule_plans_execution_phase_check CHECK ((execution_phase = ANY (ARRAY['post_publish'::text, 'pre_publish_gate'::text]))),
    CONSTRAINT derived_schedule_plans_status_check CHECK ((status = ANY (ARRAY['running'::text, 'completed'::text, 'failed'::text])))
);


--
-- Name: edge_fact_strandings; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.edge_fact_strandings (
    fact_id bigint NOT NULL,
    first_seen_gen bigint NOT NULL,
    last_retry_gen bigint NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    reason text DEFAULT 'endpoint_invisible_at_projection'::text NOT NULL
);


--
-- Name: edge_facts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.edge_facts (
    fact_id bigint NOT NULL,
    edge_id bigint NOT NULL,
    src text NOT NULL,
    dst text NOT NULL,
    edge_type text NOT NULL,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    provenance text NOT NULL,
    confidence real DEFAULT 1.0 NOT NULL,
    source text NOT NULL,
    source_record_id jsonb NOT NULL,
    source_revision jsonb NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    dedupe_key text NOT NULL,
    run_id uuid NOT NULL,
    op character(1) NOT NULL,
    source_program_checksum text DEFAULT current_setting('okg.source_program_checksum'::text, true),
    CONSTRAINT edge_facts_confidence_check CHECK (((confidence >= (0)::double precision) AND (confidence <= (1)::double precision))),
    CONSTRAINT edge_facts_op_check CHECK ((op = ANY (ARRAY['I'::bpchar, 'R'::bpchar]))),
    CONSTRAINT edge_facts_provenance_check CHECK ((provenance = ANY (ARRAY['explicit'::text, 'derived_deterministic'::text, 'linked_heuristic'::text, 'extracted_model'::text, 'derived_semantic'::text])))
);


--
-- Name: edge_facts_edge_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.edge_facts_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: edge_facts_fact_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.edge_facts_fact_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: edge_facts_fact_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.edge_facts_fact_id_seq OWNED BY okg.edge_facts.fact_id;


--
-- Name: effective_attribute_recovery_attempts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.effective_attribute_recovery_attempts (
    classification_binding_digest text NOT NULL,
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    predecessor_generation_id text NOT NULL,
    expected_classification jsonb NOT NULL,
    stage text NOT NULL,
    authorization_payload jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    consumed_by_generation_id text,
    consumed_at timestamp with time zone,
    CONSTRAINT effective_attribute_recovery_attempts_stage_check CHECK ((stage = ANY (ARRAY['classified'::text, 'snapshot_committed'::text, 'interval_committed'::text, 'prepared'::text])))
);


--
-- Name: enricher_backfill_progress; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.enricher_backfill_progress (
    deployment text NOT NULL,
    branch_name text DEFAULT 'default'::text NOT NULL,
    enricher_name text NOT NULL,
    input_node_id text NOT NULL,
    catalog_version_id bigint DEFAULT 0 NOT NULL,
    profile_fingerprint text NOT NULL,
    transform_checksum text NOT NULL,
    status text DEFAULT 'complete'::text NOT NULL,
    attempts integer DEFAULT 1 NOT NULL,
    emitted_edge_count bigint DEFAULT 0 NOT NULL,
    last_run_id uuid,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT enricher_backfill_progress_attempts_chk CHECK ((attempts > 0)),
    CONSTRAINT enricher_backfill_progress_edge_count_chk CHECK ((emitted_edge_count >= 0)),
    CONSTRAINT enricher_backfill_progress_status_chk CHECK ((status = 'complete'::text))
);


--
-- Name: enricher_progress; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.enricher_progress (
    deployment text NOT NULL,
    enricher_name text NOT NULL,
    fact_watermark bigint DEFAULT 0 NOT NULL,
    edge_watermark bigint DEFAULT 0 NOT NULL,
    generation_id bigint DEFAULT 0 NOT NULL,
    catalog_version_id bigint,
    last_run_at timestamp with time zone DEFAULT now() NOT NULL,
    last_status text DEFAULT 'never'::text NOT NULL,
    lifetime_edges_emitted bigint DEFAULT 0 NOT NULL,
    dormant_ok boolean DEFAULT false NOT NULL,
    consecutive_failure_count bigint DEFAULT 0 NOT NULL,
    CONSTRAINT enricher_progress_last_status_chk CHECK (((last_status = ANY (ARRAY['never'::text, 'success'::text])) OR (last_status ~~ 'failed:%'::text)))
);


--
-- Name: entity_identity_claims; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.entity_identity_claims (
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    subtype text NOT NULL,
    normalized_key text NOT NULL,
    canonical_node_id text NOT NULL,
    raw_name text NOT NULL,
    resolution_method text NOT NULL,
    policy_fingerprint text NOT NULL,
    catalog_version_id bigint,
    graph_token text NOT NULL,
    claimed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: entity_merge_receipts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.entity_merge_receipts (
    merge_id uuid DEFAULT gen_random_uuid() NOT NULL,
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    survivor_node_id text NOT NULL,
    retired_node_id text NOT NULL,
    subtype text NOT NULL,
    pinned_checkpoint text NOT NULL,
    catalog_version_id bigint NOT NULL,
    footprint_digest text NOT NULL,
    plan_digest text NOT NULL,
    plan jsonb NOT NULL,
    retired_snapshot jsonb NOT NULL,
    status text DEFAULT 'awaiting_publish'::text NOT NULL,
    repair_generation_id text,
    actor text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone
);


--
-- Name: entity_resolution_rejections; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.entity_resolution_rejections (
    rejection_id bigint NOT NULL,
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    subtype text NOT NULL,
    normalized_key text NOT NULL,
    raw_name text NOT NULL,
    reason text NOT NULL,
    policy_fingerprint text NOT NULL,
    source_node_ref text,
    occurrences bigint DEFAULT 1 NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: entity_resolution_rejections_rejection_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.entity_resolution_rejections_rejection_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: entity_resolution_rejections_rejection_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.entity_resolution_rejections_rejection_id_seq OWNED BY okg.entity_resolution_rejections.rejection_id;


--
-- Name: envelope_violations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.envelope_violations (
    id bigint NOT NULL,
    deployment text NOT NULL,
    generation_id text,
    producer_id text NOT NULL,
    code text NOT NULL,
    record_kind text,
    detail jsonb NOT NULL,
    poisons_reuse boolean DEFAULT true NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: envelope_violations_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.envelope_violations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: envelope_violations_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.envelope_violations_id_seq OWNED BY okg.envelope_violations.id;


--
-- Name: equivalence_verdicts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.equivalence_verdicts (
    deployment text NOT NULL,
    update_index integer NOT NULL,
    production_generation text NOT NULL,
    reference_ref text NOT NULL,
    comparison text NOT NULL,
    verdict text NOT NULL,
    release_blocking boolean NOT NULL,
    artifact jsonb,
    work_summary jsonb,
    certified_differences jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT equivalence_verdicts_certified_differences_ck CHECK (((certified_differences IS NULL) OR (verdict = 'equal'::text))),
    CONSTRAINT equivalence_verdicts_comparison_ck CHECK ((comparison = ANY (ARRAY['differential_vs_reference'::text, 'baseline_vs_reference'::text]))),
    CONSTRAINT equivalence_verdicts_release_blocking_ck CHECK ((release_blocking = (verdict = 'mismatch'::text))),
    CONSTRAINT equivalence_verdicts_verdict_ck CHECK ((verdict = ANY (ARRAY['equal'::text, 'mismatch'::text, 'excluded'::text])))
);


--
-- Name: external_probe_cache; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.external_probe_cache (
    deployment_name text NOT NULL,
    node_id text NOT NULL,
    backing_fingerprint text NOT NULL,
    field text NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    status text NOT NULL,
    payload jsonb,
    error jsonb,
    trust_label text DEFAULT 'untrusted'::text NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT external_probe_cache_status_check CHECK ((status = ANY (ARRAY['ok'::text, 'unavailable'::text, 'denied'::text, 'error'::text, 'timeout'::text])))
);


--
-- Name: extractor_retry_queue; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.extractor_retry_queue (
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    node_id text NOT NULL,
    reason text DEFAULT 'historical_unresolved'::text NOT NULL,
    enqueued_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: fact_batch_receipts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.fact_batch_receipts (
    admission_id uuid NOT NULL,
    run_id uuid NOT NULL,
    chunk_ordinal integer NOT NULL,
    canonical_digest text NOT NULL,
    target_branch text NOT NULL,
    state text NOT NULL,
    marker_digest text NOT NULL,
    graph_result jsonb DEFAULT '{}'::jsonb NOT NULL,
    branch_token text,
    audit_fact_ranges jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT fact_batch_receipts_canonical_digest_check CHECK ((canonical_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT fact_batch_receipts_chunk_ordinal_check CHECK ((chunk_ordinal >= 0)),
    CONSTRAINT fact_batch_receipts_marker_digest_check CHECK ((marker_digest ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT fact_batch_receipts_state_check CHECK ((state = ANY (ARRAY['prepared'::text, 'applied'::text, 'compensating'::text, 'compensated'::text]))),
    CONSTRAINT fact_batch_receipts_target_branch_check CHECK ((btrim(target_branch) <> ''::text))
);


--
-- Name: federation_connection_requests; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.federation_connection_requests (
    deployment_name text DEFAULT 'default'::text NOT NULL,
    request_id text NOT NULL,
    local_authority_id text NOT NULL,
    requester_authority_id text NOT NULL,
    target_authority_id text NOT NULL,
    requested_action text NOT NULL,
    target_address text NOT NULL,
    requested_visibility text DEFAULT 'public'::text NOT NULL,
    key_fingerprint text NOT NULL,
    discovery_hash text,
    status text DEFAULT 'pending'::text NOT NULL,
    reviewer_identity text,
    nomos_decision_id text,
    nomos_decision_deployment_name text DEFAULT 'default'::text NOT NULL,
    decision_evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT federation_connection_requests_decision_scope_chk CHECK (((nomos_decision_id IS NULL) OR (nomos_decision_deployment_name = deployment_name))),
    CONSTRAINT federation_connection_requests_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'approved'::text, 'denied'::text, 'quarantined'::text])))
);


--
-- Name: federation_discovered_candidates; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.federation_discovered_candidates (
    deployment_name text DEFAULT 'default'::text NOT NULL,
    local_authority_id text NOT NULL,
    remote_authority_id text NOT NULL,
    discovery_hash text NOT NULL,
    document jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT federation_discovered_candidates_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'active'::text, 'disabled'::text, 'revoked'::text])))
);


--
-- Name: federation_mailbox_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.federation_mailbox_events (
    event_id bigint NOT NULL,
    message_id text NOT NULL,
    event_type text NOT NULL,
    sender_authority_id text NOT NULL,
    recipient_authority_id text NOT NULL,
    status text NOT NULL,
    reason_code text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: federation_mailbox_events_event_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.federation_mailbox_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: federation_mailbox_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.federation_mailbox_events_event_id_seq OWNED BY okg.federation_mailbox_events.event_id;


--
-- Name: federation_mailbox_messages; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.federation_mailbox_messages (
    message_id text NOT NULL,
    sender_authority_id text NOT NULL,
    recipient_authority_id text NOT NULL,
    payload_mode text NOT NULL,
    packet_sha256 text NOT NULL,
    body_sha256 text,
    envelope jsonb,
    pointer jsonb,
    object_uri text,
    object_sha256 text,
    size_bytes bigint,
    expires_at timestamp with time zone NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    lease_token text,
    lease_expires_at timestamp with time zone,
    receipt_evidence jsonb,
    denial_evidence jsonb,
    deny_reason_code text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT federation_mailbox_messages_payload_mode_check CHECK ((payload_mode = ANY (ARRAY['inline'::text, 'detached_pointer'::text]))),
    CONSTRAINT federation_mailbox_messages_size_bytes_check CHECK (((size_bytes IS NULL) OR (size_bytes >= 0))),
    CONSTRAINT federation_mailbox_messages_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'leased'::text, 'acknowledged'::text, 'denied'::text])))
);


--
-- Name: federation_peer_trust_pins; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.federation_peer_trust_pins (
    deployment_name text DEFAULT 'default'::text NOT NULL,
    local_authority_id text NOT NULL,
    remote_authority_id text NOT NULL,
    key_fingerprint text NOT NULL,
    allowed_actions text[] DEFAULT '{}'::text[] NOT NULL,
    allowed_boards text[] DEFAULT '{}'::text[] NOT NULL,
    allowed_pull_surfaces text[] DEFAULT '{}'::text[] NOT NULL,
    visibility_tiers text[] DEFAULT '{public}'::text[] NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    nomos_decision_id text,
    nomos_decision_deployment_name text DEFAULT 'default'::text NOT NULL,
    nomos_effect text DEFAULT 'allow'::text NOT NULL,
    decision_evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT federation_peer_trust_pins_decision_scope_chk CHECK (((nomos_decision_id IS NULL) OR (nomos_decision_deployment_name = deployment_name))),
    CONSTRAINT federation_peer_trust_pins_nomos_effect_check CHECK ((nomos_effect = ANY (ARRAY['allow'::text, 'deny'::text, 'review'::text, 'quarantine'::text]))),
    CONSTRAINT federation_peer_trust_pins_status_check CHECK ((status = ANY (ARRAY['active'::text, 'disabled'::text, 'revoked'::text])))
);


--
-- Name: federation_route_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.federation_route_events (
    deployment_name text DEFAULT 'default'::text NOT NULL,
    route_event_id text NOT NULL,
    local_authority_id text NOT NULL,
    remote_authority_id text NOT NULL,
    subject text NOT NULL,
    action text NOT NULL,
    target_address text NOT NULL,
    visibility text DEFAULT 'public'::text NOT NULL,
    decision text NOT NULL,
    reason_code text NOT NULL,
    nomos_decision_id text,
    nomos_decision_deployment_name text DEFAULT 'default'::text NOT NULL,
    trust_pin_fingerprint text,
    live_row_delta jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT federation_route_events_decision_check CHECK ((decision = ANY (ARRAY['allow'::text, 'deny'::text]))),
    CONSTRAINT federation_route_events_decision_scope_chk CHECK (((nomos_decision_id IS NULL) OR (nomos_decision_deployment_name = deployment_name)))
);


--
-- Name: generation_ordinals; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.generation_ordinals (
    checkpoint_id text NOT NULL,
    branch_id text NOT NULL,
    ordinal bigint NOT NULL,
    previous_checkpoint_id text,
    deployment_name text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: graph_edges; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.graph_edges (
    edge_id bigint NOT NULL,
    src text NOT NULL,
    dst text NOT NULL,
    edge_type text NOT NULL,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    provenance text DEFAULT 'explicit'::text NOT NULL,
    confidence real DEFAULT 1.0 NOT NULL,
    source text,
    source_record_id jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_revision jsonb DEFAULT '{}'::jsonb NOT NULL,
    run_id text,
    observed_at timestamp with time zone
);


--
-- Name: graph_generations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.graph_generations (
    generation_id bigint NOT NULL,
    deployment_name text NOT NULL,
    status text NOT NULL,
    fact_watermark bigint,
    edge_fact_watermark bigint,
    catalog_version_id bigint NOT NULL,
    published_at timestamp with time zone,
    invariant_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    excluded_sources jsonb DEFAULT '[]'::jsonb NOT NULL,
    blocked_reason jsonb,
    correlation_id text NOT NULL,
    trace_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    nodes_added bigint DEFAULT 0 NOT NULL,
    nodes_retracted bigint DEFAULT 0 NOT NULL,
    edges_added bigint DEFAULT 0 NOT NULL,
    edges_retracted bigint DEFAULT 0 NOT NULL,
    nomos_policy_bundle_ids text[] DEFAULT ARRAY[]::text[] NOT NULL,
    nomos_policy_fingerprints text[] DEFAULT ARRAY[]::text[] NOT NULL,
    branch_id bigint NOT NULL,
    CONSTRAINT graph_generations_status_chk CHECK ((status = ANY (ARRAY['pending'::text, 'published'::text, 'blocked'::text, 'no_op'::text, 'worker_write'::text])))
);


--
-- Name: graph_generations_generation_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.graph_generations_generation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: graph_generations_generation_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.graph_generations_generation_id_seq OWNED BY okg.graph_generations.generation_id;


--
-- Name: graph_nodes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.graph_nodes (
    node_id text NOT NULL,
    subtype text NOT NULL,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    text_hash text,
    embedding public.vector(2560),
    embedding_model text,
    source text,
    source_record_id jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_revision jsonb DEFAULT '{}'::jsonb NOT NULL,
    run_id text,
    observed_at timestamp with time zone
);


--
-- Name: history_compaction_plans; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.history_compaction_plans (
    plan_id uuid NOT NULL,
    deployment_name text NOT NULL,
    branch_id text NOT NULL,
    status text NOT NULL,
    plan jsonb NOT NULL,
    progress jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT history_compaction_plans_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'applying'::text, 'complete'::text, 'failed'::text])))
);


--
-- Name: history_generation_pins; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.history_generation_pins (
    deployment_name text NOT NULL,
    branch_id text NOT NULL,
    pin_name text NOT NULL,
    checkpoint_id text NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: identifier_patterns; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.identifier_patterns (
    id_type text NOT NULL,
    regex text NOT NULL,
    validator_kind text,
    description text,
    catalog_version_id bigint NOT NULL,
    CONSTRAINT identifier_patterns_validator_kind_check CHECK (((validator_kind IS NULL) OR (validator_kind = ANY (ARRAY['doi_checksum'::text, 'orcid_checksum'::text, 'isbn_checksum'::text]))))
);


--
-- Name: invariants; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.invariants (
    name text NOT NULL,
    severity text NOT NULL,
    scope text NOT NULL,
    source text,
    sql_body text,
    timeout_ms integer,
    kind text DEFAULT 'sql'::text NOT NULL,
    candidates_sql text,
    judge_prompt text,
    judge_model text,
    sample_cap integer,
    fire_on text[],
    catalog_version_id bigint NOT NULL,
    concept_scope jsonb,
    CONSTRAINT invariants_check CHECK (((scope <> 'source'::text) OR (source IS NOT NULL))),
    CONSTRAINT invariants_check1 CHECK ((((kind = 'sql'::text) AND (sql_body IS NOT NULL) AND (candidates_sql IS NULL) AND (judge_prompt IS NULL) AND (judge_model IS NULL) AND (sample_cap IS NULL)) OR ((kind = 'llm_judge'::text) AND (sql_body IS NULL) AND (candidates_sql IS NOT NULL) AND (judge_prompt IS NOT NULL) AND (judge_model IS NOT NULL) AND (sample_cap IS NOT NULL) AND (sample_cap > 0)))),
    CONSTRAINT invariants_kind_check CHECK ((kind = ANY (ARRAY['sql'::text, 'llm_judge'::text]))),
    CONSTRAINT invariants_scope_check CHECK ((scope = ANY (ARRAY['source'::text, 'cross'::text]))),
    CONSTRAINT invariants_severity_check CHECK ((severity = ANY (ARRAY['error'::text, 'warn'::text]))),
    CONSTRAINT invariants_timeout_ms_check CHECK (((timeout_ms IS NULL) OR (timeout_ms > 0)))
);


--
-- Name: ir_invocations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.ir_invocations (
    invocation_id uuid DEFAULT gen_random_uuid() NOT NULL,
    program_id uuid NOT NULL,
    binding_id uuid,
    deployment_name text NOT NULL,
    invocation_kind text NOT NULL,
    branch_name text,
    generation_id bigint,
    status text NOT NULL,
    verdict text,
    reason text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    result_count bigint DEFAULT 0 NOT NULL,
    node_delta_count bigint DEFAULT 0 NOT NULL,
    edge_delta_count bigint DEFAULT 0 NOT NULL,
    cost_proxy double precision,
    quality_score double precision,
    error_count bigint DEFAULT 0 NOT NULL,
    truncation_count bigint DEFAULT 0 NOT NULL,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ir_invocations_invocation_kind_check CHECK ((invocation_kind = ANY (ARRAY['read'::text, 'score'::text, 'compile'::text, 'materialize'::text, 'replay'::text, 'promote'::text, 'rollback'::text, 'gate'::text, 'project'::text, 'enrich'::text, 'commit'::text]))),
    CONSTRAINT ir_invocations_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'rejected'::text, 'skipped'::text])))
);


--
-- Name: ir_program_bindings; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.ir_program_bindings (
    binding_id uuid DEFAULT gen_random_uuid() NOT NULL,
    program_id uuid NOT NULL,
    deployment_name text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    program_role text NOT NULL,
    generation_id bigint,
    branch_name text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ir_program_bindings_owner_kind_check CHECK ((owner_kind = ANY (ARRAY['graph_api_call'::text, 'mcp_call'::text, 'akmon_call'::text, 'source_run'::text, 'projection_batch'::text, 'enricher_run'::text, 'gate_eval'::text, 'lens'::text, 'optimizer_run'::text, 'optimizer_candidate'::text, 'optimizer_evaluation'::text, 'optimizer_promotion'::text]))),
    CONSTRAINT ir_program_bindings_program_role_check CHECK ((program_role = ANY (ARRAY['workload'::text, 'match'::text, 'before'::text, 'install'::text, 'after'::text, 'rollback'::text, 'compiler_input'::text, 'replay'::text, 'source'::text, 'projection'::text, 'enricher'::text, 'gate'::text, 'lens'::text, 'read'::text, 'promotion'::text])))
);


--
-- Name: ir_programs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.ir_programs (
    program_id uuid DEFAULT gen_random_uuid() NOT NULL,
    program_checksum text NOT NULL,
    ir_version text DEFAULT 'okg-ir-v1'::text NOT NULL,
    program_kind text NOT NULL,
    registry_id text,
    registry_version text,
    registry_checksum text,
    effect_summary jsonb DEFAULT '[]'::jsonb NOT NULL,
    determinism text NOT NULL,
    op_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    function_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    cardinality_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_mode text DEFAULT 'full'::text NOT NULL,
    program_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    program_ref text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ir_programs_payload_mode_check CHECK ((payload_mode = ANY (ARRAY['full'::text, 'redacted'::text, 'external_ref'::text]))),
    CONSTRAINT ir_programs_program_kind_check CHECK ((program_kind = ANY (ARRAY['read'::text, 'write'::text, 'analysis'::text, 'compiler_input'::text, 'gate'::text, 'lens'::text, 'source'::text, 'projection'::text, 'replay'::text, 'rollback'::text, 'transform'::text])))
);


--
-- Name: lease_audit; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.lease_audit (
    audit_id bigint NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    deployment_name text NOT NULL,
    scope text NOT NULL,
    event text NOT NULL,
    holder_id text NOT NULL,
    prior_holder_id text,
    lease_seq bigint NOT NULL,
    holder_meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT lease_audit_event_check CHECK ((event = ANY (ARRAY['acquired'::text, 'released'::text, 'stolen'::text, 'lost'::text, 'force_released'::text])))
);


--
-- Name: lease_audit_audit_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.lease_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: lease_audit_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.lease_audit_audit_id_seq OWNED BY okg.lease_audit.audit_id;


--
-- Name: lens_definitions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.lens_definitions (
    definition_id text NOT NULL,
    deployment_name text NOT NULL,
    lens_id text NOT NULL,
    view_definition_id text NOT NULL,
    view_id text NOT NULL,
    lens_contract jsonb DEFAULT '{}'::jsonb NOT NULL,
    putback_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_authority_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    lens_promotion_mapping jsonb
);


--
-- Name: lens_overlay_log; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.lens_overlay_log (
    overlay_id uuid DEFAULT gen_random_uuid() NOT NULL,
    request_id uuid NOT NULL,
    delta_id uuid NOT NULL,
    lens_definition_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    source_authority text NOT NULL,
    source_record_id jsonb NOT NULL,
    expected_source_revision jsonb,
    field_path text[] NOT NULL,
    old_value jsonb,
    new_value jsonb NOT NULL,
    source_delta jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text NOT NULL,
    promoted_admission_id uuid,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    superseded_at timestamp with time zone,
    promoted_at timestamp with time zone,
    CONSTRAINT lens_overlay_log_status_check CHECK ((status = ANY (ARRAY['active'::text, 'superseded'::text, 'promoted'::text, 'superseded_by_promotion'::text])))
);


--
-- Name: lens_putback_deltas; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.lens_putback_deltas (
    delta_id uuid DEFAULT gen_random_uuid() NOT NULL,
    request_id uuid NOT NULL,
    delta_index integer NOT NULL,
    slot_name text NOT NULL,
    scope_key text NOT NULL,
    row_key text NOT NULL,
    field_path text[] NOT NULL,
    expected_value jsonb,
    new_value jsonb NOT NULL,
    source_authority text NOT NULL,
    source_record_id jsonb NOT NULL,
    expected_source_revision jsonb,
    source_delta jsonb DEFAULT '{}'::jsonb NOT NULL,
    validation_status text NOT NULL,
    validation_error jsonb,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lens_putback_deltas_delta_index_check CHECK ((delta_index >= 0)),
    CONSTRAINT lens_putback_deltas_validation_status_check CHECK ((validation_status = ANY (ARRAY['prepared'::text, 'conflict'::text])))
);


--
-- Name: lens_putback_requests; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.lens_putback_requests (
    request_id uuid DEFAULT gen_random_uuid() NOT NULL,
    lens_definition_id text NOT NULL,
    deployment_name text NOT NULL,
    lens_id text NOT NULL,
    base_view_definition_id text NOT NULL,
    base_view_manifest_id uuid NOT NULL,
    target_generation_id bigint NOT NULL,
    actor_id text NOT NULL,
    idempotency_key text,
    status text NOT NULL,
    commit_mode text DEFAULT 'overlay'::text NOT NULL,
    delta_count bigint DEFAULT 0 NOT NULL,
    conflict_count bigint DEFAULT 0 NOT NULL,
    error jsonb,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    prepared_at timestamp with time zone,
    committed_at timestamp with time zone,
    rejected_at timestamp with time zone,
    CONSTRAINT lens_putback_requests_commit_mode_check CHECK ((commit_mode = 'overlay'::text)),
    CONSTRAINT lens_putback_requests_conflict_count_check CHECK ((conflict_count >= 0)),
    CONSTRAINT lens_putback_requests_delta_count_check CHECK ((delta_count >= 0)),
    CONSTRAINT lens_putback_requests_status_check CHECK ((status = ANY (ARRAY['preparing'::text, 'prepared'::text, 'conflict'::text, 'committed_overlay'::text, 'rejected'::text, 'failed'::text, 'promoted'::text])))
);


--
-- Name: link_cache; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.link_cache (
    node_id text NOT NULL,
    text_hash text NOT NULL,
    extractor_fingerprint text DEFAULT 'legacy'::text NOT NULL,
    linked_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: link_retry; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.link_retry (
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    node_id text NOT NULL,
    entity_type text NOT NULL,
    entity_value text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    ready_at timestamp with time zone
);


--
-- Name: llm_calls; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.llm_calls (
    id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    deployment text,
    caller text NOT NULL,
    model text NOT NULL,
    fragment_id text,
    prompt_tokens integer,
    completion_tokens integer,
    total_tokens integer,
    cost_usd numeric(12,8),
    latency_ms integer,
    success boolean DEFAULT true NOT NULL,
    error_kind text,
    generation_id bigint,
    extras jsonb,
    outcome_key_digest text
);


--
-- Name: llm_call_stats; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.llm_call_stats AS
 SELECT deployment,
    caller,
    model,
    date_trunc('day'::text, ts) AS day,
    count(*) AS calls,
    sum(prompt_tokens) AS prompt_tokens,
    sum(completion_tokens) AS completion_tokens,
    sum(cost_usd) AS cost_usd
   FROM okg.llm_calls
  WHERE success
  GROUP BY deployment, caller, model, (date_trunc('day'::text, ts));


--
-- Name: llm_calls_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.llm_calls_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: llm_calls_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.llm_calls_id_seq OWNED BY okg.llm_calls.id;


--
-- Name: mcp_call_log; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.mcp_call_log (
    call_id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    session_id text NOT NULL,
    deployment_name text DEFAULT 'default'::text NOT NULL,
    tool_name text NOT NULL,
    operator_name text,
    replacement_operator text,
    args jsonb DEFAULT '{}'::jsonb NOT NULL,
    cost jsonb DEFAULT '{}'::jsonb NOT NULL,
    pin text,
    elapsed_ms integer,
    result_count integer,
    error_code text
);


--
-- Name: mcp_call_log_call_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.mcp_call_log_call_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mcp_call_log_call_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.mcp_call_log_call_id_seq OWNED BY okg.mcp_call_log.call_id;


--
-- Name: mcp_trace_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.mcp_trace_events (
    event_id bigint NOT NULL,
    session_id text NOT NULL,
    deployment_name text NOT NULL,
    generation_id text,
    tool_name text NOT NULL,
    question_label text,
    request_json jsonb,
    response_json jsonb,
    error_text text,
    duration_ms integer,
    event_time timestamp with time zone DEFAULT now() NOT NULL,
    branch_name text DEFAULT 'default'::text,
    route_id text,
    route_manifest_checksum text
);


--
-- Name: mcp_trace_events_event_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.mcp_trace_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mcp_trace_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.mcp_trace_events_event_id_seq OWNED BY okg.mcp_trace_events.event_id;


--
-- Name: model_outcomes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.model_outcomes (
    deployment text NOT NULL,
    key_digest text NOT NULL,
    step_id text NOT NULL,
    operator_version text NOT NULL,
    model_id text NOT NULL,
    prompt_hash text NOT NULL,
    input_hash text NOT NULL,
    content text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    generation_id text
);


--
-- Name: narrowings; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.narrowings (
    parent_subtype text NOT NULL,
    edge_type text NOT NULL,
    child_subtype text NOT NULL,
    attr_rules jsonb DEFAULT '{}'::jsonb NOT NULL,
    catalog_version_id bigint NOT NULL
);


--
-- Name: node_facts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.node_facts (
    fact_id bigint NOT NULL,
    node_id text NOT NULL,
    subtype text NOT NULL,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    source text NOT NULL,
    source_record_id jsonb NOT NULL,
    source_revision jsonb NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    dedupe_key text NOT NULL,
    run_id uuid NOT NULL,
    op character(1) NOT NULL,
    source_program_checksum text DEFAULT current_setting('okg.source_program_checksum'::text, true),
    CONSTRAINT node_facts_op_check CHECK ((op = ANY (ARRAY['I'::bpchar, 'R'::bpchar])))
);


--
-- Name: node_facts_fact_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.node_facts_fact_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: node_facts_fact_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.node_facts_fact_id_seq OWNED BY okg.node_facts.fact_id;


--
-- Name: node_series; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.node_series (
    sample_id bigint NOT NULL,
    deployment_name text NOT NULL,
    node_id text NOT NULL,
    metric text NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    value double precision,
    source text,
    status text DEFAULT 'ok'::text NOT NULL,
    attrs jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT node_series_status_check CHECK ((status = ANY (ARRAY['ok'::text, 'unavailable'::text, 'error'::text])))
);


--
-- Name: node_series_sample_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.node_series_sample_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: node_series_sample_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.node_series_sample_id_seq OWNED BY okg.node_series.sample_id;


--
-- Name: node_series_watermark; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.node_series_watermark (
    deployment_name text NOT NULL,
    node_id text NOT NULL,
    metric text NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: nomos_config_activations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_config_activations (
    activation_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    activation_id text NOT NULL,
    scope text NOT NULL,
    config_fingerprint text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    activated_by text NOT NULL,
    activation_decision_id text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    activated_at timestamp with time zone DEFAULT now() NOT NULL,
    superseded_at timestamp with time zone,
    CONSTRAINT nomos_config_activations_status_check CHECK ((status = ANY (ARRAY['active'::text, 'superseded'::text, 'disabled'::text])))
);


--
-- Name: nomos_config_activations_activation_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_config_activations_activation_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_config_activations_activation_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_config_activations_activation_row_id_seq OWNED BY okg.nomos_config_activations.activation_row_id;


--
-- Name: nomos_config_sets; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_config_sets (
    config_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    config_set_id text NOT NULL,
    config_version text NOT NULL,
    config_fingerprint text NOT NULL,
    compiler_version text NOT NULL,
    source_generation_id bigint,
    scope jsonb DEFAULT '{}'::jsonb NOT NULL,
    entries jsonb DEFAULT '[]'::jsonb NOT NULL,
    config_set jsonb NOT NULL,
    status text DEFAULT 'compiled'::text NOT NULL,
    effective_from timestamp with time zone,
    effective_until timestamp with time zone,
    compiled_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nomos_config_sets_status_check CHECK ((status = ANY (ARRAY['compiled'::text, 'active'::text, 'disabled'::text, 'superseded'::text])))
);


--
-- Name: nomos_config_sets_config_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_config_sets_config_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_config_sets_config_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_config_sets_config_row_id_seq OWNED BY okg.nomos_config_sets.config_row_id;


--
-- Name: nomos_decisions; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_decisions (
    decision_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    decision_id text NOT NULL,
    policy_bundle_id text,
    policy_version text,
    policy_fingerprint text,
    input_hash text NOT NULL,
    subject text NOT NULL,
    action text NOT NULL,
    resource jsonb DEFAULT '{}'::jsonb NOT NULL,
    classification text NOT NULL,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    effect text NOT NULL,
    reason_code text NOT NULL,
    matched_rule_ids text[] DEFAULT ARRAY[]::text[] NOT NULL,
    obligations jsonb DEFAULT '[]'::jsonb NOT NULL,
    enforcement_point text NOT NULL,
    target jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nomos_decisions_effect_check CHECK ((effect = ANY (ARRAY['allow'::text, 'deny'::text, 'review'::text, 'quarantine'::text, 'redact'::text, 'declassify'::text])))
);


--
-- Name: nomos_decisions_decision_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_decisions_decision_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_decisions_decision_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_decisions_decision_row_id_seq OWNED BY okg.nomos_decisions.decision_row_id;


--
-- Name: nomos_external_claims; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_external_claims (
    claim_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    provider_id text NOT NULL,
    provider_type text NOT NULL,
    issuer text NOT NULL,
    external_subject text NOT NULL,
    audiences text[] DEFAULT ARRAY[]::text[] NOT NULL,
    groups text[] DEFAULT ARRAY[]::text[] NOT NULL,
    roles text[] DEFAULT ARRAY[]::text[] NOT NULL,
    entitlements text[] DEFAULT ARRAY[]::text[] NOT NULL,
    assurance text,
    assurance_values text[] DEFAULT ARRAY[]::text[] NOT NULL,
    mfa_satisfied boolean DEFAULT false NOT NULL,
    realm text,
    service_principal text,
    delegation_chain text[] DEFAULT ARRAY[]::text[] NOT NULL,
    expires_at timestamp with time zone,
    not_before timestamp with time zone,
    auth_time timestamp with time zone,
    claim_fingerprint text NOT NULL,
    verification_status text NOT NULL,
    verification_error text,
    evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    nomos_decision_id text,
    observed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: nomos_external_claims_claim_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_external_claims_claim_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_external_claims_claim_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_external_claims_claim_row_id_seq OWNED BY okg.nomos_external_claims.claim_row_id;


--
-- Name: nomos_identity_providers; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_identity_providers (
    provider_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    provider_id text NOT NULL,
    provider_type text NOT NULL,
    issuer text NOT NULL,
    audiences text[] DEFAULT ARRAY[]::text[] NOT NULL,
    subject_claim text DEFAULT 'sub'::text NOT NULL,
    group_claims text[] DEFAULT ARRAY['groups'::text] NOT NULL,
    role_claims text[] DEFAULT ARRAY['roles'::text] NOT NULL,
    entitlement_claims text[] DEFAULT ARRAY['entitlements'::text] NOT NULL,
    assurance_claim text,
    key_reference text,
    secret_references text[] DEFAULT ARRAY[]::text[] NOT NULL,
    claim_allowlist text[] DEFAULT ARRAY[]::text[] NOT NULL,
    required_assurance text[] DEFAULT ARRAY[]::text[] NOT NULL,
    mfa_required boolean DEFAULT false NOT NULL,
    group_claims_approved boolean DEFAULT false NOT NULL,
    metadata_fingerprint text,
    realm text,
    service_principal text,
    freshness_seconds bigint,
    status text DEFAULT 'active'::text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    effective_from timestamp with time zone,
    effective_until timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nomos_identity_providers_freshness_seconds_check CHECK (((freshness_seconds IS NULL) OR (freshness_seconds > 0))),
    CONSTRAINT nomos_identity_providers_provider_type_check CHECK ((provider_type = ANY (ARRAY['agent_local'::text, 'deployment_extension'::text, 'kerberos_gssapi'::text, 'ldap'::text, 'oidc'::text, 'poros_authority'::text, 'saml'::text, 'scim'::text, 'service_account'::text, 'touchstone_oidc'::text]))),
    CONSTRAINT nomos_identity_providers_status_check CHECK ((status = ANY (ARRAY['active'::text, 'disabled'::text, 'rotating'::text, 'revoked'::text])))
);


--
-- Name: nomos_identity_providers_provider_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_identity_providers_provider_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_identity_providers_provider_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_identity_providers_provider_row_id_seq OWNED BY okg.nomos_identity_providers.provider_row_id;


--
-- Name: nomos_permission_grants; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_permission_grants (
    grant_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    grant_id text NOT NULL,
    subject text NOT NULL,
    actions text[] DEFAULT ARRAY[]::text[] NOT NULL,
    scope text NOT NULL,
    effect text DEFAULT 'allow'::text NOT NULL,
    resource_match jsonb DEFAULT '{}'::jsonb NOT NULL,
    classifications text[] DEFAULT ARRAY['*'::text] NOT NULL,
    priority bigint DEFAULT 0 NOT NULL,
    governing_rule_ids text[] DEFAULT ARRAY[]::text[] NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nomos_permission_grants_effect_check CHECK ((effect = ANY (ARRAY['allow'::text, 'deny'::text, 'review'::text, 'quarantine'::text, 'redact'::text, 'declassify'::text]))),
    CONSTRAINT nomos_permission_grants_status_check CHECK ((status = ANY (ARRAY['active'::text, 'disabled'::text])))
);


--
-- Name: nomos_permission_grants_grant_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_permission_grants_grant_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_permission_grants_grant_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_permission_grants_grant_row_id_seq OWNED BY okg.nomos_permission_grants.grant_row_id;


--
-- Name: nomos_policy_activations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_policy_activations (
    activation_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    activation_id text NOT NULL,
    scope text NOT NULL,
    policy_fingerprint text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    activated_by text NOT NULL,
    activation_decision_id text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    activated_at timestamp with time zone DEFAULT now() NOT NULL,
    superseded_at timestamp with time zone,
    CONSTRAINT nomos_policy_activations_status_check CHECK ((status = ANY (ARRAY['active'::text, 'superseded'::text, 'disabled'::text])))
);


--
-- Name: nomos_policy_activations_activation_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_policy_activations_activation_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_policy_activations_activation_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_policy_activations_activation_row_id_seq OWNED BY okg.nomos_policy_activations.activation_row_id;


--
-- Name: nomos_policy_bundles; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_policy_bundles (
    bundle_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    policy_bundle_id text NOT NULL,
    policy_version text NOT NULL,
    policy_fingerprint text NOT NULL,
    compiler_version text NOT NULL,
    source_generation_id bigint,
    scope jsonb DEFAULT '{}'::jsonb NOT NULL,
    default_effect text NOT NULL,
    reason_code_catalog jsonb DEFAULT '{}'::jsonb NOT NULL,
    bundle jsonb NOT NULL,
    status text DEFAULT 'compiled'::text NOT NULL,
    effective_from timestamp with time zone,
    effective_until timestamp with time zone,
    compiled_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nomos_policy_bundles_default_effect_check CHECK ((default_effect = ANY (ARRAY['allow'::text, 'deny'::text, 'review'::text, 'quarantine'::text, 'redact'::text, 'declassify'::text]))),
    CONSTRAINT nomos_policy_bundles_status_check CHECK ((status = ANY (ARRAY['compiled'::text, 'active'::text, 'disabled'::text, 'superseded'::text])))
);


--
-- Name: nomos_policy_bundles_bundle_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_policy_bundles_bundle_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_policy_bundles_bundle_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_policy_bundles_bundle_row_id_seq OWNED BY okg.nomos_policy_bundles.bundle_row_id;


--
-- Name: nomos_subject_bindings; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.nomos_subject_bindings (
    binding_row_id bigint NOT NULL,
    deployment_name text NOT NULL,
    binding_id text NOT NULL,
    provider_id text NOT NULL,
    issuer text NOT NULL,
    claim_path text NOT NULL,
    claim_value text NOT NULL,
    subject text NOT NULL,
    scope text NOT NULL,
    priority bigint DEFAULT 0 NOT NULL,
    required_assurance text[] DEFAULT ARRAY[]::text[] NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    effective_from timestamp with time zone,
    effective_until timestamp with time zone,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nomos_subject_bindings_status_check CHECK ((status = ANY (ARRAY['active'::text, 'disabled'::text])))
);


--
-- Name: nomos_subject_bindings_binding_row_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.nomos_subject_bindings_binding_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nomos_subject_bindings_binding_row_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.nomos_subject_bindings_binding_row_id_seq OWNED BY okg.nomos_subject_bindings.binding_row_id;


--
-- Name: observation_current; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.observation_current (
    observation_id text NOT NULL,
    check_id text NOT NULL,
    candidate_id text NOT NULL,
    latest_generation_id bigint NOT NULL,
    latest_run_id uuid NOT NULL,
    latest_event_id bigint NOT NULL,
    finding_status text NOT NULL,
    evaluation_status text NOT NULL,
    input_scope text NOT NULL,
    predicate_key text NOT NULL,
    normalized_predicate text NOT NULL,
    normalized_values jsonb DEFAULT '{}'::jsonb NOT NULL,
    participant_roles jsonb NOT NULL,
    participant_anchors jsonb NOT NULL,
    severity text,
    confidence numeric(8,6),
    confidence_type text,
    evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT observation_current_evaluation_status_check CHECK ((evaluation_status = ANY (ARRAY['evaluated'::text, 'not_evaluated'::text, 'coverage_incomplete'::text, 'error'::text]))),
    CONSTRAINT observation_current_finding_status_check CHECK ((finding_status = ANY (ARRAY['absent'::text, 'open'::text, 'resolved'::text])))
);


--
-- Name: observation_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.observation_events (
    event_id bigint NOT NULL,
    observation_id text NOT NULL,
    check_id text NOT NULL,
    candidate_id text NOT NULL,
    target_generation_id bigint NOT NULL,
    run_id uuid NOT NULL,
    event_seq integer NOT NULL,
    finding_event text,
    evaluation_event text,
    input_scope text NOT NULL,
    predicate_key text NOT NULL,
    normalized_predicate text NOT NULL,
    normalized_values jsonb DEFAULT '{}'::jsonb NOT NULL,
    participant_roles jsonb NOT NULL,
    participant_anchors jsonb NOT NULL,
    severity text,
    confidence numeric(8,6),
    confidence_type text,
    evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    coverage_complete boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT observation_events_check CHECK ((((finding_event IS NOT NULL) AND (evaluation_event IS NULL)) OR ((finding_event IS NULL) AND (evaluation_event IS NOT NULL)))),
    CONSTRAINT observation_events_evaluation_event_check CHECK (((evaluation_event IS NULL) OR (evaluation_event = ANY (ARRAY['evaluated'::text, 'not_evaluated'::text, 'coverage_incomplete'::text, 'error'::text])))),
    CONSTRAINT observation_events_finding_event_check CHECK (((finding_event IS NULL) OR (finding_event = ANY (ARRAY['observed'::text, 'resolved'::text, 'reopened'::text]))))
);


--
-- Name: observation_events_event_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.observation_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: observation_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.observation_events_event_id_seq OWNED BY okg.observation_events.event_id;


--
-- Name: ontology_rollup; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.ontology_rollup (
    src_subtype text NOT NULL,
    edge_type text NOT NULL,
    dst_subtype text NOT NULL,
    producer text NOT NULL,
    edge_count bigint NOT NULL
);


--
-- Name: pagerank_scores; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.pagerank_scores (
    scope text NOT NULL,
    node_id text NOT NULL,
    pagerank double precision NOT NULL,
    refreshed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: poros_delivery_attempts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.poros_delivery_attempts (
    attempt_id bigint NOT NULL,
    message_id text NOT NULL,
    sender_authority_id text NOT NULL,
    recipient_authority_id text NOT NULL,
    body_sha256 text NOT NULL,
    envelope_sha256 text NOT NULL,
    transport text DEFAULT 'local_cli'::text NOT NULL,
    status text NOT NULL,
    reason_code text,
    receipt_sha256 text,
    response jsonb DEFAULT '{}'::jsonb NOT NULL,
    attempted_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT poros_delivery_attempts_status_check CHECK ((status = ANY (ARRAY['attempted'::text, 'accepted_for_admission'::text, 'rejected_preflight'::text, 'duplicate'::text, 'failed'::text])))
);


--
-- Name: poros_delivery_attempts_attempt_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.poros_delivery_attempts_attempt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: poros_delivery_attempts_attempt_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.poros_delivery_attempts_attempt_id_seq OWNED BY okg.poros_delivery_attempts.attempt_id;


--
-- Name: poros_detached_object_manifests; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.poros_detached_object_manifests (
    manifest_id bigint NOT NULL,
    message_id text NOT NULL,
    body_sha256 text NOT NULL,
    authority_id text NOT NULL,
    side text NOT NULL,
    object_id text NOT NULL,
    media_type text,
    size_bytes bigint NOT NULL,
    object_sha256 text NOT NULL,
    chunk_count bigint DEFAULT 0 NOT NULL,
    classification text DEFAULT 'public'::text NOT NULL,
    encrypted boolean DEFAULT false NOT NULL,
    manifest jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT poros_detached_object_manifests_chunk_count_check CHECK ((chunk_count >= 0)),
    CONSTRAINT poros_detached_object_manifests_side_check CHECK ((side = ANY (ARRAY['sender'::text, 'receiver'::text]))),
    CONSTRAINT poros_detached_object_manifests_size_bytes_check CHECK ((size_bytes >= 0))
);


--
-- Name: poros_detached_object_manifests_manifest_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.poros_detached_object_manifests_manifest_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: poros_detached_object_manifests_manifest_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.poros_detached_object_manifests_manifest_id_seq OWNED BY okg.poros_detached_object_manifests.manifest_id;


--
-- Name: poros_inbox_messages; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.poros_inbox_messages (
    inbox_id bigint NOT NULL,
    receiver_authority_id text NOT NULL,
    sender_authority_id text NOT NULL,
    message_id text NOT NULL,
    status text NOT NULL,
    reason_code text,
    body_sha256 text,
    signing_key_id text,
    envelope_sha256 text NOT NULL,
    receipt_sha256 text,
    envelope jsonb NOT NULL,
    receipt jsonb,
    admission_candidate jsonb,
    trusted boolean DEFAULT false NOT NULL,
    signature_valid boolean DEFAULT false NOT NULL,
    recipient_valid boolean DEFAULT false NOT NULL,
    body_hash_valid boolean DEFAULT false NOT NULL,
    fresh boolean DEFAULT false NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: poros_inbox_messages_inbox_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.poros_inbox_messages_inbox_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: poros_inbox_messages_inbox_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.poros_inbox_messages_inbox_id_seq OWNED BY okg.poros_inbox_messages.inbox_id;


--
-- Name: poros_incorporation_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.poros_incorporation_events (
    event_id bigint NOT NULL,
    receiver_authority_id text NOT NULL,
    sender_authority_id text NOT NULL,
    message_id text NOT NULL,
    body_sha256 text,
    event_type text NOT NULL,
    status text NOT NULL,
    reason_code text,
    nomos_decision_id text,
    nomos_decision_deployment_name text DEFAULT 'default'::text NOT NULL,
    policy_bundle_id text,
    policy_fingerprint text,
    nomos_input_hash text,
    admission_id uuid,
    receiver_generation_id text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT poros_incorporation_events_event_type_check CHECK ((event_type = ANY (ARRAY['preflight_accepted'::text, 'preflight_rejected'::text, 'duplicate'::text, 'nomos_decided'::text, 'admitted'::text, 'published'::text, 'blocked'::text, 'quarantined'::text, 'review'::text, 'promotion_decided'::text, 'promotion_denied'::text, 'promoted'::text, 'rejection_decided'::text, 'rejection_denied'::text, 'rejected'::text])))
);


--
-- Name: poros_incorporation_events_event_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.poros_incorporation_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: poros_incorporation_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.poros_incorporation_events_event_id_seq OWNED BY okg.poros_incorporation_events.event_id;


--
-- Name: poros_outbox_messages; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.poros_outbox_messages (
    message_id text NOT NULL,
    sender_authority_id text NOT NULL,
    recipient_authority_id text NOT NULL,
    sender_generation_id text NOT NULL,
    graph_id text NOT NULL,
    signing_key_id text NOT NULL,
    body_sha256 text NOT NULL,
    envelope_sha256 text NOT NULL,
    payload_mode text DEFAULT 'inline'::text NOT NULL,
    classification text NOT NULL,
    graph_profile text,
    root_node_ids text[] DEFAULT ARRAY[]::text[] NOT NULL,
    node_count bigint DEFAULT 0 NOT NULL,
    edge_count bigint DEFAULT 0 NOT NULL,
    envelope jsonb NOT NULL,
    body jsonb NOT NULL,
    status text DEFAULT 'created'::text NOT NULL,
    max_delivery_attempts bigint DEFAULT 3 NOT NULL,
    next_retry_at timestamp with time zone,
    last_error text,
    terminal_failure_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT poros_outbox_max_delivery_attempts_chk CHECK ((max_delivery_attempts > 0)),
    CONSTRAINT poros_outbox_messages_edge_count_check CHECK ((edge_count >= 0)),
    CONSTRAINT poros_outbox_messages_max_delivery_attempts_check CHECK ((max_delivery_attempts > 0)),
    CONSTRAINT poros_outbox_messages_node_count_check CHECK ((node_count >= 0)),
    CONSTRAINT poros_outbox_messages_payload_mode_check CHECK ((payload_mode = ANY (ARRAY['inline'::text, 'detached'::text]))),
    CONSTRAINT poros_outbox_messages_status_chk CHECK ((status = ANY (ARRAY['created'::text, 'delivering'::text, 'delivered'::text, 'acknowledged'::text, 'failed'::text, 'retrying'::text, 'terminal_failed'::text])))
);


--
-- Name: poros_replay_cache; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.poros_replay_cache (
    receiver_authority_id text NOT NULL,
    sender_authority_id text NOT NULL,
    message_id text NOT NULL,
    body_sha256 text,
    status text NOT NULL,
    reason_code text,
    receipt_sha256 text,
    expires_at timestamp with time zone NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    duplicate_count bigint DEFAULT 0 NOT NULL,
    CONSTRAINT poros_replay_cache_duplicate_count_check CHECK ((duplicate_count >= 0))
);


--
-- Name: post_publish_projection_status; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.post_publish_projection_status (
    deployment_name text NOT NULL,
    generation_id bigint NOT NULL,
    last_run_at timestamp with time zone DEFAULT now() NOT NULL,
    status text NOT NULL,
    error text,
    CONSTRAINT post_publish_projection_status_error_len_chk CHECK (((error IS NULL) OR (length(error) <= 4096))),
    CONSTRAINT post_publish_projection_status_status_chk CHECK ((status = ANY (ARRAY['success'::text, 'failed'::text])))
);


--
-- Name: producer_profiles; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.producer_profiles (
    profile_id bigint NOT NULL,
    deployment_name text NOT NULL,
    producer_id text NOT NULL,
    producer_kind text NOT NULL,
    trust_label text NOT NULL,
    admission_mode text NOT NULL,
    authority_scope jsonb DEFAULT '{}'::jsonb NOT NULL,
    allowed_record_kinds text[] DEFAULT ARRAY[]::text[] NOT NULL,
    output_signature jsonb DEFAULT '{}'::jsonb NOT NULL,
    policy_clauses jsonb DEFAULT '[]'::jsonb NOT NULL,
    output_scope_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_ref jsonb DEFAULT '{}'::jsonb NOT NULL,
    catalog_version_id bigint,
    profile_fingerprint text NOT NULL,
    status text NOT NULL,
    catalog_evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT producer_profiles_admission_mode_check CHECK ((admission_mode = ANY (ARRAY['direct'::text, 'fast_track'::text, 'review'::text, 'quarantine'::text]))),
    CONSTRAINT producer_profiles_producer_kind_check CHECK ((producer_kind = ANY (ARRAY['source'::text, 'extractor'::text, 'enricher'::text, 'agent'::text, 'human'::text, 'workflow'::text, 'system'::text, 'lens'::text]))),
    CONSTRAINT producer_profiles_status_check CHECK ((status = ANY (ARRAY['active'::text, 'compat'::text, 'disabled'::text])))
);


--
-- Name: producer_profiles_profile_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.producer_profiles_profile_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: producer_profiles_profile_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.producer_profiles_profile_id_seq OWNED BY okg.producer_profiles.profile_id;


--
-- Name: projectable_edge_facts; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.projectable_edge_facts AS
 SELECT NULL::bigint AS fact_id,
    NULL::bigint AS edge_id,
    NULL::text AS src,
    NULL::text AS dst,
    NULL::text AS edge_type,
    '{}'::jsonb AS attrs,
    NULL::text AS provenance,
    NULL::real AS confidence,
    NULL::text AS source,
    '{}'::jsonb AS source_record_id,
    '{}'::jsonb AS source_revision,
    NULL::timestamp with time zone AS observed_at,
    NULL::text AS dedupe_key,
    NULL::uuid AS run_id,
    NULL::character(1) AS op
  WHERE false;


--
-- Name: projectable_node_facts; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.projectable_node_facts AS
 SELECT NULL::bigint AS fact_id,
    NULL::text AS node_id,
    NULL::text AS subtype,
    '{}'::jsonb AS attrs,
    NULL::text AS source,
    '{}'::jsonb AS source_record_id,
    '{}'::jsonb AS source_revision,
    NULL::timestamp with time zone AS observed_at,
    NULL::text AS dedupe_key,
    NULL::uuid AS run_id,
    NULL::character(1) AS op
  WHERE false;


--
-- Name: projection_pending; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.projection_pending (
    pending_id bigint NOT NULL,
    generation_id bigint NOT NULL,
    source text NOT NULL,
    new_cursor bigint NOT NULL,
    batch_index integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: projection_pending_pending_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.projection_pending_pending_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: projection_pending_pending_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.projection_pending_pending_id_seq OWNED BY okg.projection_pending.pending_id;


--
-- Name: projection_remediation_batches; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.projection_remediation_batches (
    idempotency_key text NOT NULL,
    run_id uuid NOT NULL,
    input_cursor text,
    requested_limit integer NOT NULL,
    receipt jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: projection_remediation_runs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.projection_remediation_runs (
    run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    remediation_kind text NOT NULL,
    deployment_name text NOT NULL,
    branch_name text NOT NULL,
    pinned_checkpoint text,
    catalog_version_id bigint NOT NULL,
    collision_snapshot jsonb DEFAULT '[]'::jsonb NOT NULL,
    subtype text,
    field text,
    entity_type text,
    status text DEFAULT 'running'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone
);


--
-- Name: repair_selections; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.repair_selections (
    deployment text NOT NULL,
    generation_id text NOT NULL,
    bucket_key text NOT NULL,
    outcome text NOT NULL,
    winner_claim_id text,
    generator_id text,
    generator_version text,
    basis text,
    distrusted_claim_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    asserted_claim_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    unresolved_reason text,
    carried_from_generation text,
    selection_digest text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: reuse_certificates; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.reuse_certificates (
    deployment text NOT NULL,
    generation_id text NOT NULL,
    step_id text NOT NULL,
    certificate_digest text NOT NULL,
    comparison jsonb NOT NULL,
    dependencies jsonb NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reuse_certificates_comparison_object CHECK ((jsonb_typeof(comparison) = 'object'::text)),
    CONSTRAINT reuse_certificates_dependencies_object CHECK ((jsonb_typeof(dependencies) = 'object'::text)),
    CONSTRAINT reuse_certificates_digest_nonempty CHECK ((certificate_digest <> ''::text))
);


--
-- Name: schema_migration_phases; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.schema_migration_phases (
    migration_id text NOT NULL,
    phase text NOT NULL,
    state text DEFAULT 'in_progress'::text NOT NULL,
    batches_committed bigint DEFAULT 0 NOT NULL,
    watermark jsonb DEFAULT 'null'::jsonb NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT schema_migration_phases_state_check CHECK ((state = ANY (ARRAY['in_progress'::text, 'complete'::text])))
);


--
-- Name: schema_migrations; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.schema_migrations (
    migration_id text NOT NULL,
    checksum text NOT NULL,
    description text NOT NULL,
    destructive boolean DEFAULT false NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    applied_by text DEFAULT CURRENT_USER NOT NULL,
    tool_version text,
    status text DEFAULT 'applied'::text NOT NULL,
    result_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT schema_migrations_status_check CHECK ((status = ANY (ARRAY['applied'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: source_cursor_blobs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.source_cursor_blobs (
    source text NOT NULL,
    cursor_key text NOT NULL,
    sha256 text NOT NULL,
    value jsonb NOT NULL,
    value_text text,
    size_bytes bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_progress; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.source_progress (
    deployment text NOT NULL,
    source_name text NOT NULL,
    key text NOT NULL,
    value text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_record_set_blobs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.source_record_set_blobs (
    source text NOT NULL,
    sha256 text NOT NULL,
    record_set jsonb NOT NULL,
    record_set_text text,
    record_set_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    chronos_proof jsonb,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_record_sets; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.source_record_sets (
    source text NOT NULL,
    record_set jsonb NOT NULL,
    record_set_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    chronos_proof jsonb,
    sha256 text DEFAULT ''::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_watermarks; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.source_watermarks (
    source text NOT NULL,
    cutover_watermark jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_applied_rev jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_published_rev jsonb DEFAULT '{}'::jsonb NOT NULL,
    projection_cursor bigint DEFAULT 0 NOT NULL,
    last_reconcile_at timestamp with time zone,
    replay_required jsonb DEFAULT '{}'::jsonb NOT NULL,
    replay_required_at timestamp with time zone
);


--
-- Name: subtype_ancestors; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.subtype_ancestors (
    catalog_version_id bigint NOT NULL,
    subtype_name text NOT NULL,
    ancestor_name text NOT NULL,
    depth integer NOT NULL
);


--
-- Name: subtypes; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.subtypes (
    name text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    parent_subtype text,
    catalog_version_id bigint NOT NULL
);


--
-- Name: telemetry_artifacts; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_artifacts (
    artifact_id text NOT NULL,
    deployment_name text NOT NULL,
    trace_id text NOT NULL,
    span_id text,
    artifact_kind text NOT NULL,
    path text NOT NULL,
    format text NOT NULL,
    collector text,
    collector_status text NOT NULL,
    unavailable_reason text,
    summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: telemetry_events; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_events (
    event_id bigint NOT NULL,
    event_time timestamp with time zone DEFAULT now() NOT NULL,
    trace_id text NOT NULL,
    span_id text,
    event_name text NOT NULL,
    severity text,
    deployment_name text,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    unavailable_reason text
);


--
-- Name: telemetry_events_event_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.telemetry_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: telemetry_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.telemetry_events_event_id_seq OWNED BY okg.telemetry_events.event_id;


--
-- Name: telemetry_graph_effect_manifests; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_graph_effect_manifests (
    deployment_name text NOT NULL,
    trace_id text NOT NULL,
    span_id text NOT NULL,
    branch_name text DEFAULT 'default'::text NOT NULL,
    producer_key text,
    input_generation_id text,
    output_generation_id text,
    coverage text NOT NULL,
    unavailable_reason text,
    total_count bigint DEFAULT 0 NOT NULL,
    retained_count integer DEFAULT 0 NOT NULL,
    retention_limit integer DEFAULT 0 NOT NULL,
    counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT telemetry_graph_effect_manifests_coverage_check CHECK ((coverage = ANY (ARRAY['exact'::text, 'sampled'::text, 'aggregate_only'::text, 'missing'::text])))
);


--
-- Name: telemetry_graph_effects; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_graph_effects (
    effect_id bigint NOT NULL,
    deployment_name text NOT NULL,
    trace_id text NOT NULL,
    span_id text NOT NULL,
    sequence integer NOT NULL,
    object_kind text NOT NULL,
    object_id text NOT NULL,
    change_kind text NOT NULL,
    object_type text,
    source_id text,
    target_id text,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT telemetry_graph_effects_change_kind_check CHECK ((change_kind = ANY (ARRAY['added'::text, 'updated'::text, 'retracted'::text]))),
    CONSTRAINT telemetry_graph_effects_object_kind_check CHECK ((object_kind = ANY (ARRAY['node'::text, 'edge'::text])))
);


--
-- Name: telemetry_graph_effects_effect_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.telemetry_graph_effects_effect_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: telemetry_graph_effects_effect_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.telemetry_graph_effects_effect_id_seq OWNED BY okg.telemetry_graph_effects.effect_id;


--
-- Name: telemetry_metrics; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_metrics (
    metric_id bigint NOT NULL,
    metric_time timestamp with time zone DEFAULT now() NOT NULL,
    metric_name text NOT NULL,
    metric_kind text NOT NULL,
    value_double double precision,
    value_int bigint,
    unit text,
    deployment_name text NOT NULL,
    branch_name text DEFAULT 'default'::text NOT NULL,
    trace_id text,
    span_id text,
    labels jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: telemetry_metrics_metric_id_seq; Type: SEQUENCE; Schema: okg; Owner: -
--

CREATE SEQUENCE okg.telemetry_metrics_metric_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: telemetry_metrics_metric_id_seq; Type: SEQUENCE OWNED BY; Schema: okg; Owner: -
--

ALTER SEQUENCE okg.telemetry_metrics_metric_id_seq OWNED BY okg.telemetry_metrics.metric_id;


--
-- Name: telemetry_spans; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_spans (
    event_time timestamp with time zone DEFAULT now() NOT NULL,
    trace_id text NOT NULL,
    span_id text NOT NULL,
    parent_span_id text,
    span_name text NOT NULL,
    span_kind text DEFAULT 'internal'::text NOT NULL,
    status_code text DEFAULT 'unset'::text NOT NULL,
    status_message text,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    duration_ms integer,
    deployment_name text NOT NULL,
    branch_name text DEFAULT 'default'::text NOT NULL,
    generation_id text,
    correlation_id text,
    source_name text,
    workflow_id text,
    run_id text,
    tool_name text,
    route_id text,
    session_id text,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    unavailable_reason text
);


--
-- Name: telemetry_traces; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.telemetry_traces (
    trace_id text NOT NULL,
    deployment_name text NOT NULL,
    branch_name text DEFAULT 'default'::text NOT NULL,
    root_name text NOT NULL,
    root_kind text NOT NULL,
    status_code text DEFAULT 'unset'::text NOT NULL,
    status_message text,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    duration_ms integer,
    generation_id text,
    correlation_id text,
    source_name text,
    workflow_id text,
    session_id text,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: trace_backtracks; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.trace_backtracks AS
 WITH ordered AS (
         SELECT mcp_trace_events.session_id,
            mcp_trace_events.tool_name,
            (mcp_trace_events.request_json ->> 'node_id'::text) AS node_id,
            mcp_trace_events.event_time,
            lag((mcp_trace_events.request_json ->> 'node_id'::text)) OVER w AS prev_node_id,
            lag(mcp_trace_events.event_time) OVER w AS prev_event_time
           FROM okg.mcp_trace_events
          WHERE (mcp_trace_events.request_json ? 'node_id'::text)
          WINDOW w AS (PARTITION BY mcp_trace_events.session_id ORDER BY mcp_trace_events.event_time)
        )
 SELECT session_id,
    count(*) AS backtrack_count
   FROM ordered
  WHERE ((node_id = prev_node_id) AND ((event_time - prev_event_time) < '00:01:00'::interval))
  GROUP BY session_id;


--
-- Name: trace_sessions; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.trace_sessions AS
 SELECT session_id,
    deployment_name,
    min(event_time) AS first_call_at,
    max(event_time) AS last_call_at,
    count(*) AS tool_call_count,
    count(DISTINCT tool_name) AS unique_tool_count,
    count(*) FILTER (WHERE (error_text IS NOT NULL)) AS error_count,
    sum(duration_ms) AS total_duration_ms
   FROM okg.mcp_trace_events
  GROUP BY session_id, deployment_name;


--
-- Name: trace_sessions_labeled; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.trace_sessions_labeled AS
 SELECT session_id,
    deployment_name,
    question_label,
    min(event_time) AS first_call_at,
    max(event_time) AS last_call_at,
    count(*) AS tool_call_count,
    count(DISTINCT tool_name) AS unique_tool_count,
    count(*) FILTER (WHERE (error_text IS NOT NULL)) AS error_count,
    sum(duration_ms) AS total_duration_ms
   FROM okg.mcp_trace_events
  GROUP BY session_id, deployment_name, question_label;


--
-- Name: v_admission_envelopes; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_admission_envelopes AS
 SELECT (admission_id)::text AS admission_id,
    deployment_name,
    producer_id,
    producer_kind,
    intent,
    admission_mode,
    status,
    catalog_version_id,
    source,
    (run_id)::text AS run_id,
    target_generation_id,
    idempotency_key,
    authority,
    check_summary,
    verdict,
    workflow_ref,
    created_at,
    updated_at,
    decided_at,
    published_at,
    target_generation_ref,
    NULLIF(btrim(target_generation_ref), ''::text) AS canonical_generation_ref
   FROM okg.admission_envelopes;


--
-- Name: VIEW v_admission_envelopes; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON VIEW okg.v_admission_envelopes IS 'Compact admission lifecycle rows. target_generation_ref is the canonical Chronos generation reference; target_generation_id is a legacy numeric compatibility token.';


--
-- Name: COLUMN v_admission_envelopes.target_generation_id; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON COLUMN okg.v_admission_envelopes.target_generation_id IS 'Legacy numeric compatibility generation token. Prefer target_generation_ref or canonical_generation_ref for active Chronos-aware reads.';


--
-- Name: COLUMN v_admission_envelopes.target_generation_ref; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON COLUMN okg.v_admission_envelopes.target_generation_ref IS 'Canonical text generation reference for the Chronos generation targeted by this admission envelope.';


--
-- Name: COLUMN v_admission_envelopes.canonical_generation_ref; Type: COMMENT; Schema: okg; Owner: -
--

COMMENT ON COLUMN okg.v_admission_envelopes.canonical_generation_ref IS 'Canonical Chronos generation reference normalized from target_generation_ref; NULL when only a legacy numeric compatibility token is available.';


--
-- Name: v_admission_events; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_admission_events AS
 SELECT event_id,
    (admission_id)::text AS admission_id,
    event_type,
    actor,
    reason,
    payload,
    workflow_ref,
    created_at
   FROM okg.admission_events;


--
-- Name: v_admission_record_refs; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_admission_record_refs AS
 SELECT ref_id,
    (admission_id)::text AS admission_id,
    ref_key,
    ref_kind,
    source,
    (run_id)::text AS run_id,
    fact_id_min,
    fact_id_max,
    record_count,
    payload,
    created_at
   FROM okg.admission_record_refs;


--
-- Name: v_nodes; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nodes AS
 SELECT graph_nodes.node_id,
    graph_nodes.subtype,
    graph_nodes.attrs,
    graph_nodes.text_hash,
    NULL::bigint AS valid_from_gen,
    NULL::text AS source_program_checksum
   FROM okg.graph_nodes
UNION ALL
 SELECT NULL::text AS node_id,
    NULL::text AS subtype,
    NULL::jsonb AS attrs,
    NULL::text AS text_hash,
    NULL::bigint AS valid_from_gen,
    NULL::text AS source_program_checksum
  WHERE okg.require_graph_checkout('okg.graph_nodes'::text);


--
-- Name: v_agent_messages; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_agent_messages AS
 SELECT node_id AS message_node_id,
    (attrs ->> 'session_id'::text) AS session_id,
    ('session:'::text || (attrs ->> 'session_id'::text)) AS session_node_id,
    COALESCE(
        CASE
            WHEN ((attrs ->> 'turn_index'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'turn_index'::text))::integer
            ELSE NULL::integer
        END,
        CASE
            WHEN (node_id ~ ':[0-9]+$'::text) THEN ("substring"(node_id, ':([0-9]+)$'::text))::integer
            ELSE NULL::integer
        END) AS turn_index,
        CASE
            WHEN ((attrs ->> 'chapter_index'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'chapter_index'::text))::integer
            ELSE NULL::integer
        END AS chapter_index,
    (attrs ->> 'role'::text) AS role,
    (attrs ->> 'message_kind'::text) AS message_kind,
    "left"(COALESCE((attrs ->> 'content_excerpt'::text), ''::text), 500) AS content_excerpt,
    (attrs ->> 'content_hash'::text) AS content_hash,
        CASE
            WHEN ((attrs ->> 'tool_call_count'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'tool_call_count'::text))::integer
            ELSE NULL::integer
        END AS tool_call_count,
        CASE
            WHEN ((attrs ->> 'timestamp'::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'::text) THEN ((attrs ->> 'timestamp'::text))::timestamp with time zone
            ELSE NULL::timestamp with time zone
        END AS "timestamp"
   FROM okg.v_nodes n
  WHERE (subtype = 'agent_message'::text);


--
-- Name: v_agent_sessions; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_agent_sessions AS
 SELECT node_id AS session_node_id,
    (attrs ->> 'session_id'::text) AS session_id,
    subtype AS session_subtype,
    (attrs ->> 'model'::text) AS model,
    (attrs ->> 'cwd'::text) AS cwd,
    (attrs ->> 'repo'::text) AS repo,
    (attrs ->> 'transcript_path'::text) AS transcript_path,
        CASE
            WHEN (COALESCE((attrs ->> 'started_at'::text), (attrs ->> 'timestamp'::text)) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'::text) THEN (COALESCE((attrs ->> 'started_at'::text), (attrs ->> 'timestamp'::text)))::timestamp with time zone
            ELSE NULL::timestamp with time zone
        END AS started_at,
        CASE
            WHEN ((attrs ->> 'ended_at'::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'::text) THEN ((attrs ->> 'ended_at'::text))::timestamp with time zone
            ELSE NULL::timestamp with time zone
        END AS ended_at,
        CASE
            WHEN (COALESCE((attrs ->> 'last_active_at'::text), (attrs ->> 'timestamp'::text)) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'::text) THEN (COALESCE((attrs ->> 'last_active_at'::text), (attrs ->> 'timestamp'::text)))::timestamp with time zone
            ELSE NULL::timestamp with time zone
        END AS last_active_at,
        CASE
            WHEN ((attrs ->> 'message_count'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'message_count'::text))::integer
            ELSE NULL::integer
        END AS message_count,
        CASE
            WHEN ((attrs ->> 'tool_call_count'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'tool_call_count'::text))::integer
            ELSE NULL::integer
        END AS tool_call_count,
        CASE
            WHEN ((attrs ->> 'input_tokens'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'input_tokens'::text))::bigint
            ELSE NULL::bigint
        END AS input_tokens,
        CASE
            WHEN ((attrs ->> 'output_tokens'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'output_tokens'::text))::bigint
            ELSE NULL::bigint
        END AS output_tokens,
        CASE
            WHEN ((attrs ->> 'cache_read_tokens'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'cache_read_tokens'::text))::bigint
            ELSE NULL::bigint
        END AS cache_read_tokens,
        CASE
            WHEN ((attrs ->> 'cache_creation_tokens'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'cache_creation_tokens'::text))::bigint
            ELSE NULL::bigint
        END AS cache_creation_tokens,
        CASE
            WHEN ((attrs ->> 'total_tokens'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'total_tokens'::text))::bigint
            ELSE NULL::bigint
        END AS total_tokens,
        CASE
            WHEN ((attrs ->> 'cost_usd_estimate'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((attrs ->> 'cost_usd_estimate'::text))::double precision
            ELSE NULL::double precision
        END AS cost_usd_estimate,
    (attrs ->> 'cost_confidence'::text) AS cost_confidence,
    (attrs ->> 'cost_basis'::text) AS cost_basis
   FROM okg.v_nodes n
  WHERE (subtype = ANY (ARRAY['agent_session'::text, 'sub_agent_session'::text]));


--
-- Name: v_aliases; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_aliases AS
 SELECT alias,
    canonical,
    scope
   FROM okg.aliases a
  WHERE (catalog_version_id = ( SELECT max(catalog_versions.catalog_version_id) AS max
           FROM okg.catalog_versions
          WHERE (catalog_versions.status = 'applied'::text)));


--
-- Name: v_branch_layered_edges; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_branch_layered_edges AS
 SELECT edge_id,
    src,
    dst,
    edge_type,
    attrs,
    NULL::bigint AS valid_from_gen,
    NULL::bigint AS valid_to_gen,
    NULL::bigint AS branch_id
   FROM okg.graph_edges;


--
-- Name: v_branch_layered_nodes; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_branch_layered_nodes AS
 SELECT node_id,
    subtype,
    attrs,
    text_hash,
    embedding,
    embedding_model,
    NULL::bigint AS valid_from_gen,
    NULL::bigint AS valid_to_gen,
    NULL::bigint AS last_fact_id,
    NULL::bigint AS branch_id
   FROM okg.graph_nodes;


--
-- Name: v_changes; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_changes AS
 SELECT node_id AS change_id,
    (attrs ->> 'change_id'::text) AS change_slug,
    (attrs ->> 'status'::text) AS status,
    (attrs ->> 'title'::text) AS title,
    (attrs ->> 'archived_date'::text) AS archived_date,
    (attrs ->> 'why_text'::text) AS why_text,
    COALESCE(( SELECT (count(*))::integer AS count
           FROM okg.v_nodes t
          WHERE ((t.subtype = 'proposal_task'::text) AND ((t.attrs ->> 'change_id'::text) = (n.attrs ->> 'change_id'::text)))), 0) AS task_count,
    COALESCE(( SELECT (count(*))::integer AS count
           FROM okg.v_nodes t
          WHERE ((t.subtype = 'acceptance_scenario'::text) AND ((t.attrs ->> 'scenario_id'::text) ~~ (('scn:'::text || (n.attrs ->> 'change_id'::text)) || ':%'::text)))), 0) AS scenario_count
   FROM okg.v_nodes n
  WHERE (subtype = 'open_spec_change'::text);


--
-- Name: v_edges; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_edges AS
 SELECT graph_edges.edge_id,
    graph_edges.src,
    graph_edges.dst,
    graph_edges.edge_type,
    graph_edges.attrs,
    NULL::bigint AS valid_from_gen,
    NULL::text AS source_program_checksum
   FROM okg.graph_edges
UNION ALL
 SELECT NULL::bigint AS edge_id,
    NULL::text AS src,
    NULL::text AS dst,
    NULL::text AS edge_type,
    NULL::jsonb AS attrs,
    NULL::bigint AS valid_from_gen,
    NULL::text AS source_program_checksum
  WHERE okg.require_graph_checkout('okg.graph_edges'::text);


--
-- Name: v_doc_chunks; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_doc_chunks AS
 SELECT c.node_id AS chunk_id,
    e.dst AS doc_id,
    (d.attrs ->> 'path'::text) AS path,
    (d.attrs ->> 'repo'::text) AS repo,
    (c.attrs ->> 'text'::text) AS text,
    COALESCE((e.attrs ->> 'heading_path'::text), (c.attrs ->> 'heading_path'::text)) AS heading_path,
    COALESCE((NULLIF((e.attrs ->> 'char_offset'::text), ''::text))::integer, (NULLIF((c.attrs ->> 'char_offset'::text), ''::text))::integer) AS char_offset,
    COALESCE((NULLIF((e.attrs ->> 'char_length'::text), ''::text))::integer, (NULLIF((c.attrs ->> 'char_length'::text), ''::text))::integer) AS char_length,
    COALESCE((e.attrs ->> 'chunker_name'::text), (c.attrs ->> 'chunker_name'::text)) AS chunker_name,
    (c.attrs ->> 'content_sha256'::text) AS content_sha256,
    (c.attrs ->> 'chunk_kind'::text) AS chunk_kind,
    COALESCE((NULLIF((e.attrs ->> 'char_end'::text), ''::text))::integer, (NULLIF((c.attrs ->> 'char_end'::text), ''::text))::integer) AS char_end,
    COALESCE((e.attrs ->> 'table_grid_status'::text), (c.attrs ->> 'table_grid_status'::text)) AS table_grid_status,
    COALESCE((e.attrs ->> 'table_extraction_lane'::text), (c.attrs ->> 'table_extraction_lane'::text)) AS table_extraction_lane,
    COALESCE((e.attrs ->> 'table_grid_error_code'::text), (c.attrs ->> 'table_grid_error_code'::text)) AS table_grid_error_code
   FROM ((okg.v_nodes c
     JOIN okg.v_edges e ON (((e.src = c.node_id) AND (e.edge_type = 'member_of'::text))))
     JOIN okg.v_nodes d ON (((d.node_id = e.dst) AND (d.subtype = 'document'::text))))
  WHERE (c.subtype = 'document_chunk'::text);


--
-- Name: v_chunk_sequence; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_chunk_sequence AS
 SELECT chunk_id,
    doc_id,
    path,
    char_offset,
    char_length,
    heading_path,
    lag(chunk_id) OVER w AS prev_chunk_id,
    lead(chunk_id) OVER w AS next_chunk_id,
    row_number() OVER w AS chunk_index,
    count(*) OVER (PARTITION BY doc_id) AS doc_chunk_total,
    chunk_kind,
    char_end
   FROM okg.v_doc_chunks
  WINDOW w AS (PARTITION BY doc_id ORDER BY char_offset, chunk_id);


--
-- Name: v_compute_host_metrics; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_compute_host_metrics AS
 WITH latest AS (
         SELECT DISTINCT ON (node_series.deployment_name, node_series.node_id, node_series.metric) node_series.deployment_name,
            node_series.node_id,
            node_series.metric,
            node_series.value AS last_value,
            node_series.observed_at AS last_observed_at
           FROM okg.node_series
          ORDER BY node_series.deployment_name, node_series.node_id, node_series.metric, node_series.observed_at DESC, node_series.sample_id DESC
        )
 SELECT s.deployment_name,
    s.node_id,
    s.metric,
    l.last_value,
    l.last_observed_at,
    (EXTRACT(epoch FROM (now() - l.last_observed_at)))::double precision AS staleness_seconds,
    (count(*))::integer AS samples_total,
    (count(*) FILTER (WHERE (s.observed_at >= (now() - '01:00:00'::interval))))::integer AS samples_1h,
    avg(s.value) FILTER (WHERE (s.observed_at >= (now() - '01:00:00'::interval))) AS avg_1h,
    max(s.value) FILTER (WHERE (s.observed_at >= (now() - '01:00:00'::interval))) AS max_1h,
    min(s.value) FILTER (WHERE (s.observed_at >= (now() - '01:00:00'::interval))) AS min_1h,
    avg(s.value) FILTER (WHERE (s.observed_at >= (now() - '1 day'::interval))) AS avg_1d,
    max(s.value) FILTER (WHERE (s.observed_at >= (now() - '1 day'::interval))) AS max_1d,
    min(s.value) FILTER (WHERE (s.observed_at >= (now() - '1 day'::interval))) AS min_1d,
    avg(s.value) FILTER (WHERE (s.observed_at >= (now() - '7 days'::interval))) AS avg_7d,
    max(s.value) FILTER (WHERE (s.observed_at >= (now() - '7 days'::interval))) AS max_7d,
    min(s.value) FILTER (WHERE (s.observed_at >= (now() - '7 days'::interval))) AS min_7d,
    min(s.observed_at) AS first_observed_at
   FROM (okg.node_series s
     JOIN latest l USING (deployment_name, node_id, metric))
  GROUP BY s.deployment_name, s.node_id, s.metric, l.last_value, l.last_observed_at;


--
-- Name: v_consistency_observations; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_consistency_observations AS
 SELECT oc.observation_id,
    defs.deployment_name,
    oc.check_id,
    defs.check_kind,
    defs.predicate_family,
    oc.candidate_id,
    oc.latest_generation_id,
    (oc.latest_run_id)::text AS latest_run_id,
    oc.latest_event_id,
    oc.finding_status,
    oc.evaluation_status,
    oc.input_scope,
    oc.predicate_key,
    oc.normalized_predicate,
    oc.normalized_values,
    oc.participant_roles,
    oc.participant_anchors,
    oc.severity,
    oc.confidence,
    oc.confidence_type,
    oc.evidence,
    oc.payload,
    oc.updated_at
   FROM (okg.observation_current oc
     JOIN okg.consistency_check_definitions defs ON ((defs.check_id = oc.check_id)));


--
-- Name: v_documents; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_documents AS
 SELECT node_id AS doc_id,
    (attrs ->> 'path'::text) AS path,
    (attrs ->> 'repo'::text) AS repo,
    (attrs ->> 'format'::text) AS format,
        CASE
            WHEN (attrs ? 'byte_count'::text) THEN ((attrs ->> 'byte_count'::text))::bigint
            ELSE NULL::bigint
        END AS byte_count,
    (attrs ->> 'content_sha256'::text) AS content_sha256,
    (attrs ->> 'title'::text) AS title,
    COALESCE(( SELECT (count(*))::integer AS count
           FROM (okg.v_edges e
             JOIN okg.v_nodes c ON (((c.node_id = e.dst) AND (c.subtype = 'document_chunk'::text))))
          WHERE ((e.src = n.node_id) AND (e.edge_type = 'contains'::text))), 0) AS chunk_count
   FROM okg.v_nodes n
  WHERE (subtype = 'document'::text);


--
-- Name: v_edges_history; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_edges_history AS
 SELECT edge_id,
    src,
    dst,
    edge_type,
    attrs,
    NULL::bigint AS valid_from_gen,
    NULL::bigint AS valid_to_gen
   FROM okg.graph_edges;


--
-- Name: v_exchange_boundary_violations; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_exchange_boundary_violations AS
 SELECT 'federation_mailbox_messages'::text AS relation_name,
    federation_mailbox_messages.message_id AS record_id,
    'payload_shape'::text AS violation_code
   FROM okg.federation_mailbox_messages
  WHERE (NOT COALESCE((((federation_mailbox_messages.payload_mode = 'inline'::text) AND (federation_mailbox_messages.envelope IS NOT NULL) AND (federation_mailbox_messages.pointer IS NULL) AND (federation_mailbox_messages.object_uri IS NULL) AND (federation_mailbox_messages.object_sha256 IS NULL) AND (federation_mailbox_messages.size_bytes IS NULL)) OR ((federation_mailbox_messages.payload_mode = 'detached_pointer'::text) AND (federation_mailbox_messages.envelope IS NULL) AND (federation_mailbox_messages.pointer IS NOT NULL) AND (NULLIF(btrim(federation_mailbox_messages.object_uri), ''::text) IS NOT NULL) AND (NULLIF(btrim(federation_mailbox_messages.object_sha256), ''::text) IS NOT NULL) AND (federation_mailbox_messages.size_bytes IS NOT NULL) AND (federation_mailbox_messages.size_bytes > 0))), false))
UNION ALL
 SELECT 'federation_mailbox_messages'::text AS relation_name,
    federation_mailbox_messages.message_id AS record_id,
    'lifecycle_evidence'::text AS violation_code
   FROM okg.federation_mailbox_messages
  WHERE (NOT COALESCE((((federation_mailbox_messages.status = 'pending'::text) AND (federation_mailbox_messages.receipt_evidence IS NULL) AND (federation_mailbox_messages.denial_evidence IS NULL) AND (federation_mailbox_messages.deny_reason_code IS NULL)) OR ((federation_mailbox_messages.status = 'leased'::text) AND (federation_mailbox_messages.lease_token IS NOT NULL) AND (federation_mailbox_messages.lease_expires_at IS NOT NULL) AND (federation_mailbox_messages.receipt_evidence IS NULL) AND (federation_mailbox_messages.denial_evidence IS NULL) AND (federation_mailbox_messages.deny_reason_code IS NULL)) OR ((federation_mailbox_messages.status = 'acknowledged'::text) AND (federation_mailbox_messages.receipt_evidence IS NOT NULL) AND (federation_mailbox_messages.denial_evidence IS NULL) AND (federation_mailbox_messages.deny_reason_code IS NULL)) OR ((federation_mailbox_messages.status = 'denied'::text) AND (federation_mailbox_messages.receipt_evidence IS NULL) AND (federation_mailbox_messages.denial_evidence IS NOT NULL) AND (NULLIF(btrim(federation_mailbox_messages.deny_reason_code), ''::text) IS NOT NULL))), false))
UNION ALL
 SELECT 'poros_inbox_messages'::text AS relation_name,
    ((poros_inbox_messages.receiver_authority_id || ':'::text) || poros_inbox_messages.message_id) AS record_id,
    'status_vocabulary'::text AS violation_code
   FROM okg.poros_inbox_messages
  WHERE (poros_inbox_messages.status <> ALL (ARRAY['accepted_for_admission'::text, 'rejected_preflight'::text, 'duplicate'::text, 'quarantined'::text]))
UNION ALL
 SELECT 'poros_detached_object_manifests'::text AS relation_name,
    ((((((poros_detached_object_manifests.side || ':'::text) || poros_detached_object_manifests.authority_id) || ':'::text) || poros_detached_object_manifests.message_id) || ':'::text) || poros_detached_object_manifests.object_id) AS record_id,
    'classification'::text AS violation_code
   FROM okg.poros_detached_object_manifests
  WHERE (poros_detached_object_manifests.classification <> ALL (ARRAY['public'::text, 'internal'::text, 'restricted'::text, 'private'::text, 'unspecified'::text]))
UNION ALL
 SELECT 'poros_detached_object_manifests'::text AS relation_name,
    ((((((poros_detached_object_manifests.side || ':'::text) || poros_detached_object_manifests.authority_id) || ':'::text) || poros_detached_object_manifests.message_id) || ':'::text) || poros_detached_object_manifests.object_id) AS record_id,
    'manifest_shape'::text AS violation_code
   FROM okg.poros_detached_object_manifests
  WHERE (NOT COALESCE((COALESCE((jsonb_typeof(poros_detached_object_manifests.manifest) = 'object'::text), false) AND COALESCE(((poros_detached_object_manifests.manifest ->> 'object_id'::text) = poros_detached_object_manifests.object_id), false) AND ((NOT (poros_detached_object_manifests.manifest ? 'classification'::text)) OR COALESCE(((poros_detached_object_manifests.manifest ->> 'classification'::text) = poros_detached_object_manifests.classification), false)) AND ((poros_detached_object_manifests.encrypted AND (poros_detached_object_manifests.manifest ? 'ciphertext_sha256'::text) AND COALESCE((jsonb_typeof((poros_detached_object_manifests.manifest -> 'encryption'::text)) = 'object'::text), false) AND COALESCE((poros_detached_object_manifests.object_sha256 = (poros_detached_object_manifests.manifest ->> 'ciphertext_sha256'::text)), false)) OR ((NOT poros_detached_object_manifests.encrypted) AND (NOT (poros_detached_object_manifests.manifest ? 'ciphertext_sha256'::text)) AND (NOT (poros_detached_object_manifests.manifest ? 'encryption'::text)) AND (COALESCE((poros_detached_object_manifests.object_sha256 = (poros_detached_object_manifests.manifest ->> 'cleartext_sha256'::text)), false) OR COALESCE((poros_detached_object_manifests.object_sha256 = (poros_detached_object_manifests.manifest ->> 'sha256'::text)), false))))), false));


--
-- Name: _chronos_branch_interval_checkpoints; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_branch_interval_checkpoints (
    checkpoint_id text NOT NULL,
    branch_id text NOT NULL,
    segment_id integer NOT NULL,
    created_at text NOT NULL,
    metadata text NOT NULL
);


--
-- Name: v_generations; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_generations AS
 SELECT checkpoint_id AS id,
    ((metadata)::jsonb ->> 'deployment_name'::text) AS deployment_name,
    ((metadata)::jsonb ->> 'status'::text) AS status,
    (created_at)::timestamp with time zone AS published_at,
    (((metadata)::jsonb ->> 'catalog_version_id'::text))::bigint AS catalog_version_id,
    (created_at)::timestamp with time zone AS created_at,
    checkpoint_id AS correlation_id
   FROM public._chronos_branch_interval_checkpoints
  WHERE ((((metadata)::jsonb ->> 'kind'::text) = 'okg_generation'::text) AND (((metadata)::jsonb ->> 'status'::text) = 'published'::text));


--
-- Name: v_ir_invocations; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_ir_invocations AS
 SELECT invocation_id,
    program_id,
    binding_id,
    deployment_name,
    invocation_kind,
    branch_name,
    generation_id,
    status,
    verdict,
    reason,
    started_at,
    completed_at,
    result_count,
    node_delta_count,
    edge_delta_count,
    cost_proxy,
    quality_score,
    error_count,
    truncation_count,
    metrics,
    created_at
   FROM okg.ir_invocations;


--
-- Name: v_ir_program_bindings; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_ir_program_bindings AS
 SELECT binding_id,
    program_id,
    deployment_name,
    owner_kind,
    owner_id,
    program_role,
    generation_id,
    branch_name,
    metadata,
    created_at
   FROM okg.ir_program_bindings;


--
-- Name: v_ir_programs; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_ir_programs AS
 SELECT program_id,
    program_checksum,
    ir_version,
    program_kind,
    registry_id,
    registry_version,
    registry_checksum,
    effect_summary,
    determinism,
    op_counts,
    function_counts,
    cardinality_summary,
    payload_mode,
    program_json,
    program_ref,
    created_at
   FROM okg.ir_programs;


--
-- Name: v_mcp_calls; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_mcp_calls AS
 SELECT call_id,
    ts,
    session_id,
    deployment_name,
    tool_name,
    operator_name,
    replacement_operator,
    args,
    cost,
    pin,
    elapsed_ms,
    result_count,
    error_code
   FROM okg.mcp_call_log;


--
-- Name: v_mcp_call_costs; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_mcp_call_costs AS
 SELECT call_id,
    ts,
    session_id,
    deployment_name,
    tool_name,
    COALESCE(operator_name, tool_name) AS operator_name,
    replacement_operator,
    pin,
    elapsed_ms,
    result_count,
    error_code,
        CASE
            WHEN ((cost ->> 'args_bytes'::text) ~ '^-?[0-9]+$'::text) THEN ((cost ->> 'args_bytes'::text))::bigint
            ELSE NULL::bigint
        END AS args_bytes,
        CASE
            WHEN ((cost ->> 'response_bytes'::text) ~ '^-?[0-9]+$'::text) THEN ((cost ->> 'response_bytes'::text))::bigint
            ELSE NULL::bigint
        END AS response_bytes,
        CASE
            WHEN ((cost ->> 'args_token_estimate'::text) ~ '^-?[0-9]+$'::text) THEN ((cost ->> 'args_token_estimate'::text))::bigint
            ELSE NULL::bigint
        END AS args_token_estimate,
        CASE
            WHEN ((cost ->> 'response_token_estimate'::text) ~ '^-?[0-9]+$'::text) THEN ((cost ->> 'response_token_estimate'::text))::bigint
            ELSE NULL::bigint
        END AS response_token_estimate,
        CASE
            WHEN (lower((cost ->> 'truncated'::text)) = ANY (ARRAY['true'::text, 't'::text, '1'::text, 'yes'::text])) THEN true
            WHEN (lower((cost ->> 'truncated'::text)) = ANY (ARRAY['false'::text, 'f'::text, '0'::text, 'no'::text])) THEN false
            ELSE false
        END AS truncated,
        CASE
            WHEN ((cost ->> 'row_count'::text) ~ '^-?[0-9]+$'::text) THEN ((cost ->> 'row_count'::text))::bigint
            ELSE NULL::bigint
        END AS row_count,
    (cost ->> 'query_plan_summary'::text) AS query_plan_summary
   FROM okg.v_mcp_calls;


--
-- Name: v_mcp_operator_costs; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_mcp_operator_costs AS
 SELECT deployment_name,
    operator_name,
    count(*) AS calls,
    (COALESCE(sum(response_bytes), (0)::numeric))::bigint AS response_bytes,
    (COALESCE(sum(args_bytes), (0)::numeric))::bigint AS args_bytes,
    count(*) FILTER (WHERE truncated) AS truncated_calls,
    count(*) FILTER (WHERE (error_code IS NOT NULL)) AS error_count,
    percentile_disc((0.50)::double precision) WITHIN GROUP (ORDER BY elapsed_ms) AS p50_ms,
    percentile_disc((0.90)::double precision) WITHIN GROUP (ORDER BY elapsed_ms) AS p90_ms,
    max(response_bytes) AS max_response_bytes,
    max(ts) AS last_called_at
   FROM okg.v_mcp_call_costs
  GROUP BY deployment_name, operator_name;


--
-- Name: v_nodes_history; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nodes_history AS
 SELECT node_id,
    subtype,
    attrs,
    text_hash,
    NULL::bigint AS valid_from_gen,
    NULL::bigint AS valid_to_gen
   FROM okg.graph_nodes;


--
-- Name: v_nomos_config_activations; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_config_activations AS
 SELECT activation_row_id,
    deployment_name,
    activation_id,
    scope,
    config_fingerprint,
    status,
    activated_by,
    activation_decision_id,
    metadata,
    activated_at,
    superseded_at
   FROM okg.nomos_config_activations;


--
-- Name: v_nomos_config_sets; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_config_sets AS
 SELECT c.config_row_id,
    c.deployment_name,
    c.config_set_id,
    c.config_version,
    c.config_fingerprint,
    c.compiler_version,
    c.source_generation_id,
    c.scope,
    redacted.entries,
        CASE
            WHEN (jsonb_typeof(c.config_set) = 'object'::text) THEN ((c.config_set - 'entries'::text) || jsonb_build_object('entries', redacted.entries))
            ELSE jsonb_build_object('entries', redacted.entries)
        END AS config_set,
    c.status,
    c.effective_from,
    c.effective_until,
    c.compiled_at,
        CASE
            WHEN (c.metadata = '{}'::jsonb) THEN '{}'::jsonb
            ELSE jsonb_build_object('metadata_redacted', true)
        END AS metadata,
    c.created_at,
    c.updated_at
   FROM (okg.nomos_config_sets c
     CROSS JOIN LATERAL ( SELECT COALESCE(jsonb_agg(jsonb_build_object('config_key', (item.entry -> 'config_key'::text), 'classification', (item.entry -> 'classification'::text), 'scope', (item.entry -> 'scope'::text), 'owner', (item.entry -> 'owner'::text), 'metadata_redacted', ((item.entry ? 'metadata'::text) AND ((item.entry -> 'metadata'::text) <> '{}'::jsonb)), 'value_redacted', (item.entry ? 'value'::text), 'secret_ref_redacted', (item.entry ? 'secret_ref'::text)) ORDER BY item.ord), '[]'::jsonb) AS entries
           FROM jsonb_array_elements(c.entries) WITH ORDINALITY item(entry, ord)) redacted);


--
-- Name: v_nomos_decisions; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_decisions AS
 SELECT decision_row_id,
    deployment_name,
    decision_id,
    policy_bundle_id,
    policy_version,
    policy_fingerprint,
    input_hash,
    subject,
    action,
    resource,
    classification,
    context,
    effect,
    reason_code,
    matched_rule_ids,
    obligations,
    enforcement_point,
    target,
    created_at
   FROM okg.nomos_decisions;


--
-- Name: v_nomos_external_claims; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_external_claims AS
 SELECT claim_row_id,
    deployment_name,
    provider_id,
    provider_type,
    issuer,
    external_subject,
    audiences,
    groups,
    roles,
    entitlements,
    assurance,
    assurance_values,
    mfa_satisfied,
    realm,
    service_principal,
    delegation_chain,
    expires_at,
    not_before,
    auth_time,
    claim_fingerprint,
    verification_status,
    verification_error,
    evidence,
    nomos_decision_id,
    observed_at
   FROM okg.nomos_external_claims;


--
-- Name: v_nomos_identity_providers; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_identity_providers AS
 SELECT provider_row_id,
    deployment_name,
    provider_id,
    provider_type,
    issuer,
    audiences,
    subject_claim,
    group_claims,
    role_claims,
    entitlement_claims,
    assurance_claim,
    key_reference,
    secret_references,
    claim_allowlist,
    required_assurance,
    mfa_required,
    group_claims_approved,
    metadata_fingerprint,
    realm,
    service_principal,
    freshness_seconds,
    status,
    metadata,
    effective_from,
    effective_until,
    created_at,
    updated_at
   FROM okg.nomos_identity_providers;


--
-- Name: v_nomos_permission_grants; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_permission_grants AS
 SELECT grant_row_id,
    deployment_name,
    grant_id,
    subject,
    actions,
    scope,
    effect,
    resource_match,
    classifications,
    priority,
    governing_rule_ids,
    status,
    metadata,
    created_at,
    updated_at
   FROM okg.nomos_permission_grants;


--
-- Name: v_nomos_policy_activations; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_policy_activations AS
 SELECT activation_row_id,
    deployment_name,
    activation_id,
    scope,
    policy_fingerprint,
    status,
    activated_by,
    activation_decision_id,
    metadata,
    activated_at,
    superseded_at
   FROM okg.nomos_policy_activations;


--
-- Name: v_nomos_policy_bundles; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_policy_bundles AS
 SELECT bundle_row_id,
    deployment_name,
    policy_bundle_id,
    policy_version,
    policy_fingerprint,
    compiler_version,
    source_generation_id,
    scope,
    default_effect,
    reason_code_catalog,
    bundle,
    status,
    effective_from,
    effective_until,
    compiled_at,
    metadata,
    created_at,
    updated_at
   FROM okg.nomos_policy_bundles;


--
-- Name: v_nomos_subject_bindings; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_nomos_subject_bindings AS
 SELECT binding_row_id,
    deployment_name,
    binding_id,
    provider_id,
    issuer,
    claim_path,
    claim_value,
    subject,
    scope,
    priority,
    required_assurance,
    status,
    effective_from,
    effective_until,
    metadata,
    created_at,
    updated_at
   FROM okg.nomos_subject_bindings;


--
-- Name: v_producer_profiles; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_producer_profiles AS
 SELECT profile_id,
    deployment_name,
    producer_id,
    producer_kind,
    trust_label,
    admission_mode,
    status,
    catalog_version_id,
    profile_fingerprint,
    authority_scope,
    allowed_record_kinds,
    output_signature,
    policy_clauses,
    output_scope_summary,
    source_ref,
    catalog_evidence,
    created_at,
    updated_at
   FROM okg.producer_profiles;


--
-- Name: v_runtime_lock_surface_inventory; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_runtime_lock_surface_inventory AS
 SELECT surface_id,
    surface_kind,
    authority,
    storage_relation,
    owner_column,
    expires_column,
    active_status,
    owner_semantics,
    expiry_semantics,
    cleanup_semantics,
    visibility_view,
    table_backed
   FROM ( VALUES ('publisher_advisory_lock'::text,'session_advisory_lock'::text,'Publisher.cycle DBOS workflow plus Postgres session advisory lock'::text,NULL::text,NULL::text,NULL::text,NULL::text,'Owner is the live backend session holding the advisory lock; no OKG row records a holder.'::text,'No row TTL. The lock is released by explicit unlock or backend session close.'::text,'Inspect publisher workflow/session state; do not recreate okg.publisher_leases.'::text,NULL::text,false), ('catalog_apply_advisory_lock'::text,'session_advisory_lock'::text,'Catalog apply CLI plus Postgres session advisory lock'::text,NULL::text,NULL::text,NULL::text,NULL::text,'Owner is the live backend session applying catalog state; no OKG row records a holder.'::text,'No row TTL. The lock is released by explicit unlock or backend session close.'::text,'Inspect catalog apply process/session state; do not infer ownership from lease_audit.'::text,NULL::text,false), ('legacy_publisher_lease_audit'::text,'retired_audit_log'::text,'Historical audit rows from retired okg.publisher_leases runtime'::text,'okg.lease_audit'::text,'holder_id'::text,NULL::text,NULL::text,'holder_id is historical evidence, not current lock ownership.'::text,'No active expiry. Rows describe past acquire/release/lost events.'::text,'Retained for audit only; current publisher coordination is advisory lock plus DBOS.'::text,NULL::text,true), ('mailbox_message_claim'::text,'table_backed_lease'::text,'Federation mailbox recipient polling claim'::text,'okg.federation_mailbox_messages'::text,'lease_token'::text,'lease_expires_at'::text,'leased'::text,'lease_token is bearer material for the authenticated recipient and must not be exposed by read-side views.'::text,'lease_expires_at bounds the poll claim; expired leased messages are claimable again.'::text,'Polling reclaims expired leased messages with a fresh token; ack/deny require an active token.'::text,'okg.v_runtime_stale_locks'::text,true)) inventory(surface_id, surface_kind, authority, storage_relation, owner_column, expires_column, active_status, owner_semantics, expiry_semantics, cleanup_semantics, visibility_view, table_backed);


--
-- Name: v_runtime_queue_health; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_runtime_queue_health AS
 SELECT queue_name,
    status,
    availability,
    active_count,
    pending_count,
    waiting_count,
    delayed_count,
    running_count,
    failed_count,
    cancelled_count,
    dlq_count,
    terminal_history_count,
    unknown_active_age_count,
    oldest_active_age_seconds,
    newest_active_age_seconds,
    stale_after_seconds,
    stale_active_work_present,
    generated_at,
    reason,
    workflow_name
   FROM okg.runtime_queue_health() runtime_queue_health(queue_name, status, availability, active_count, pending_count, waiting_count, delayed_count, running_count, failed_count, cancelled_count, dlq_count, terminal_history_count, unknown_active_age_count, oldest_active_age_seconds, newest_active_age_seconds, stale_after_seconds, stale_active_work_present, generated_at, reason, workflow_name);


--
-- Name: v_runtime_stale_locks; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_runtime_stale_locks AS
 SELECT 'mailbox_message_claim'::text AS surface_id,
    'okg.federation_mailbox_messages'::text AS storage_relation,
    message_id AS resource_id,
    status AS active_status,
    recipient_authority_id AS owner_ref,
    ((lease_token IS NOT NULL) AND (length(lease_token) > 0)) AS token_present,
    lease_expires_at,
        CASE
            WHEN ((lease_token IS NULL) OR (length(lease_token) = 0)) THEN 'missing_token'::text
            WHEN (lease_expires_at IS NULL) THEN 'missing_expiry'::text
            WHEN (lease_expires_at <= now()) THEN 'expired'::text
            ELSE NULL::text
        END AS stale_reason,
        CASE
            WHEN (lease_expires_at IS NULL) THEN NULL::bigint
            ELSE GREATEST((0)::bigint, (EXTRACT(epoch FROM (now() - lease_expires_at)))::bigint)
        END AS stale_for_seconds,
    'mailbox polling can reclaim expired leased messages with a fresh token; ack/deny require an active token'::text AS cleanup_semantics,
    jsonb_build_object('message_id', message_id, 'sender_authority_id', sender_authority_id, 'recipient_authority_id', recipient_authority_id, 'payload_mode', payload_mode, 'expires_at', expires_at) AS metadata
   FROM okg.federation_mailbox_messages msg
  WHERE ((status = 'leased'::text) AND ((lease_token IS NULL) OR (length(lease_token) = 0) OR (lease_expires_at IS NULL) OR (lease_expires_at <= now())));


--
-- Name: v_source_frontier_state; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_source_frontier_state AS
 SELECT deployment,
    source_name,
    registry_source_name,
    branch_name,
    latest_published_generation_id,
    latest_published_at,
    watermark_source,
    last_reconcile_at,
    last_reconcile_age_seconds,
    projection_cursor,
    progress_observed_at,
    last_run_observed_at,
    last_run_id,
    last_run_status,
    last_run_mode,
    last_run_completed_scope,
    last_error,
    nodes_appended,
    edges_appended,
    record_set_state,
    record_set_sha256,
    record_set_record_count,
    record_set_node_count,
    record_set_edge_count,
    cursor_blob_count,
    cursor_blob_bytes,
    active_workflow_id,
    workflow_status,
    workflow_run_mode,
    active_work_stale,
    active_work_age_seconds,
    runtime_state,
    read_visibility_state,
    frontier_state
   FROM okg.source_frontier_state() source_frontier_state(deployment, source_name, registry_source_name, branch_name, latest_published_generation_id, latest_published_at, watermark_source, last_reconcile_at, last_reconcile_age_seconds, projection_cursor, progress_observed_at, last_run_observed_at, last_run_id, last_run_status, last_run_mode, last_run_completed_scope, last_error, nodes_appended, edges_appended, record_set_state, record_set_sha256, record_set_record_count, record_set_node_count, record_set_edge_count, cursor_blob_count, cursor_blob_bytes, active_workflow_id, workflow_status, workflow_run_mode, active_work_stale, active_work_age_seconds, runtime_state, read_visibility_state, frontier_state);


--
-- Name: v_source_run_diagnostics; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_source_run_diagnostics AS
 WITH progress_runs AS (
         SELECT p.deployment,
            p.source_name AS progress_source_name,
            p.key,
            (p.value)::jsonb AS payload,
            p.updated_at
           FROM okg.source_progress p
          WHERE (p.key ~~ 'sync.stage_timings.run.%'::text)
        ), decoded AS (
         SELECT progress_runs.deployment,
            COALESCE((progress_runs.payload ->> 'source_name'::text), progress_runs.progress_source_name) AS source_name,
            COALESCE((progress_runs.payload ->> 'registry_source_name'::text),
                CASE
                    WHEN (progress_runs.progress_source_name ~~ (progress_runs.deployment || '.%'::text)) THEN SUBSTRING(progress_runs.progress_source_name FROM (length(progress_runs.deployment) + 2))
                    ELSE progress_runs.progress_source_name
                END) AS registry_source_name,
            COALESCE(NULLIF((progress_runs.payload ->> 'run_id'::text), ''::text), "substring"(progress_runs.key, 'sync\.stage_timings\.run\.(.*)$'::text)) AS run_id,
            (progress_runs.payload ->> 'status'::text) AS status,
            (progress_runs.payload ->> 'run_mode'::text) AS run_mode,
            COALESCE((progress_runs.payload ->> 'timing_status'::text), 'unavailable'::text) AS timing_status,
            (progress_runs.payload ->> 'timing_unavailable_reason'::text) AS timing_unavailable_reason,
            progress_runs.updated_at AS observed_at,
                CASE
                    WHEN ((progress_runs.payload ->> 'input_count'::text) ~ '^-?[0-9]+$'::text) THEN ((progress_runs.payload ->> 'input_count'::text))::bigint
                    ELSE NULL::bigint
                END AS input_count,
                CASE
                    WHEN ((progress_runs.payload ->> 'output_count'::text) ~ '^-?[0-9]+$'::text) THEN ((progress_runs.payload ->> 'output_count'::text))::bigint
                    ELSE NULL::bigint
                END AS output_count,
                CASE
                    WHEN ((progress_runs.payload ->> 'nodes_appended'::text) ~ '^-?[0-9]+$'::text) THEN ((progress_runs.payload ->> 'nodes_appended'::text))::bigint
                    ELSE (0)::bigint
                END AS nodes_appended,
                CASE
                    WHEN ((progress_runs.payload ->> 'edges_appended'::text) ~ '^-?[0-9]+$'::text) THEN ((progress_runs.payload ->> 'edges_appended'::text))::bigint
                    ELSE (0)::bigint
                END AS edges_appended,
                CASE
                    WHEN ((progress_runs.payload ->> 'nodes_retracted'::text) ~ '^-?[0-9]+$'::text) THEN ((progress_runs.payload ->> 'nodes_retracted'::text))::bigint
                    ELSE (0)::bigint
                END AS node_retractions,
                CASE
                    WHEN ((progress_runs.payload ->> 'edges_retracted'::text) ~ '^-?[0-9]+$'::text) THEN ((progress_runs.payload ->> 'edges_retracted'::text))::bigint
                    ELSE (0)::bigint
                END AS edge_retractions,
            COALESCE((progress_runs.payload -> 'timings_ms'::text), '{}'::jsonb) AS timings_ms,
            COALESCE((progress_runs.payload -> 'sync_scope'::text), '{}'::jsonb) AS sync_scope,
            COALESCE((progress_runs.payload -> 'source_stats'::text), '{}'::jsonb) AS source_stats
           FROM progress_runs
        )
 SELECT d.deployment,
    d.source_name,
    d.registry_source_name,
    d.run_id,
    d.status,
    d.run_mode,
    d.timing_status,
        CASE
            WHEN (d.timing_status = 'available'::text) THEN d.timing_unavailable_reason
            ELSE COALESCE(d.timing_unavailable_reason, 'stage_timing_payload_unavailable'::text)
        END AS timing_unavailable_reason,
    d.observed_at,
    d.observed_at AS started_at,
    d.observed_at AS finished_at,
    d.input_count,
    d.output_count,
    d.nodes_appended,
    d.edges_appended,
    d.node_retractions,
    d.edge_retractions,
        CASE
            WHEN ((d.timings_ms ->> 'total'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'total'::text))::double precision
            ELSE NULL::double precision
        END AS total_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner'::text))::double precision
            ELSE NULL::double precision
        END AS source_runner_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.adapter_run'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.adapter_run'::text))::double precision
            ELSE NULL::double precision
        END AS adapter_run_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.flush.dedupe_persisted'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.flush.dedupe_persisted'::text))::double precision
            ELSE NULL::double precision
        END AS dedupe_persisted_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.flush.chronos_apply'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.flush.chronos_apply'::text))::double precision
            ELSE NULL::double precision
        END AS chronos_apply_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.flush_chunks'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.flush_chunks'::text))::double precision
            ELSE NULL::double precision
        END AS flush_chunks_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.flush_final'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.flush_final'::text))::double precision
            ELSE NULL::double precision
        END AS flush_final_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.stream_walk'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.stream_walk'::text))::double precision
            ELSE NULL::double precision
        END AS stream_walk_ms,
        CASE
            WHEN ((d.timings_ms ->> 'source_runner.record_set_chronos_verify'::text) ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((d.timings_ms ->> 'source_runner.record_set_chronos_verify'::text))::double precision
            ELSE NULL::double precision
        END AS record_set_chronos_verify_ms,
    slowest.phase AS slowest_phase,
    slowest.ms AS slowest_phase_ms,
    d.timings_ms,
    d.source_stats,
    d.sync_scope
   FROM (decoded d
     LEFT JOIN LATERAL ( SELECT kv.key AS phase,
            (kv.value)::double precision AS ms
           FROM jsonb_each_text(d.timings_ms) kv(key, value)
          WHERE ((kv.value ~ '^-?[0-9]+(\.[0-9]+)?$'::text) AND (kv.key <> ALL (ARRAY['total'::text, 'source_runner.total'::text])))
          ORDER BY (kv.value)::double precision DESC, kv.key
         LIMIT 1) slowest ON (true));


--
-- Name: v_source_runs; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_source_runs AS
 WITH payload_runs AS (
         SELECT sp.source_name,
            ((sp.value)::jsonb ->> 'run_id'::text) AS run_id,
            (NULLIF(((sp.value)::jsonb ->> 'started_at'::text), ''::text))::timestamp with time zone AS started_at,
            (NULLIF(((sp.value)::jsonb ->> 'finished_at'::text), ''::text))::timestamp with time zone AS finished_at,
            COALESCE((((sp.value)::jsonb ->> 'nodes_appended'::text))::integer, 0) AS nodes_appended,
            COALESCE((((sp.value)::jsonb ->> 'edges_appended'::text))::integer, 0) AS edges_appended,
            COALESCE((((sp.value)::jsonb ->> 'nodes_retracted'::text))::integer, 0) AS node_retractions,
            COALESCE((((sp.value)::jsonb ->> 'edges_retracted'::text))::integer, 0) AS edge_retractions
           FROM okg.source_progress sp
          WHERE ((sp.key ~~ 'sync.run.%'::text) AND (sp.value ~~ '{%'::text))
        ), diagnostic_runs AS (
         SELECT v_source_run_diagnostics.source_name,
            v_source_run_diagnostics.run_id,
            v_source_run_diagnostics.started_at,
            v_source_run_diagnostics.finished_at,
            v_source_run_diagnostics.nodes_appended,
            v_source_run_diagnostics.edges_appended,
            v_source_run_diagnostics.node_retractions,
            v_source_run_diagnostics.edge_retractions
           FROM okg.v_source_run_diagnostics
        ), fact_runs AS (
         SELECT node_facts.source,
            node_facts.run_id,
            node_facts.observed_at,
                CASE
                    WHEN (node_facts.op = 'I'::bpchar) THEN 1
                    ELSE 0
                END AS nodes_appended,
            0 AS edges_appended,
                CASE
                    WHEN (node_facts.op = 'R'::bpchar) THEN 1
                    ELSE 0
                END AS node_retractions,
            0 AS edge_retractions
           FROM okg.node_facts
        UNION ALL
         SELECT edge_facts.source,
            edge_facts.run_id,
            edge_facts.observed_at,
            0 AS nodes_appended,
                CASE
                    WHEN (edge_facts.op = 'I'::bpchar) THEN 1
                    ELSE 0
                END AS edges_appended,
            0 AS node_retractions,
                CASE
                    WHEN (edge_facts.op = 'R'::bpchar) THEN 1
                    ELSE 0
                END AS edge_retractions
           FROM okg.edge_facts
        ), fact_agg AS (
         SELECT fact_runs.source AS source_name,
            (fact_runs.run_id)::text AS run_id,
            min(fact_runs.observed_at) AS started_at,
            max(fact_runs.observed_at) AS finished_at,
            (sum(fact_runs.nodes_appended))::integer AS nodes_appended,
            (sum(fact_runs.edges_appended))::integer AS edges_appended,
            (sum(fact_runs.node_retractions))::integer AS node_retractions,
            (sum(fact_runs.edge_retractions))::integer AS edge_retractions
           FROM fact_runs
          WHERE (fact_runs.run_id IS NOT NULL)
          GROUP BY fact_runs.source, fact_runs.run_id
        )
 SELECT COALESCE(p.source_name, d.source_name, f.source_name) AS source_name,
    COALESCE(p.run_id, d.run_id, f.run_id) AS run_id,
    COALESCE(p.started_at, d.started_at, f.started_at) AS started_at,
    COALESCE(p.finished_at, d.finished_at, f.finished_at) AS finished_at,
    (COALESCE((p.nodes_appended)::bigint, d.nodes_appended, (f.nodes_appended)::bigint, (0)::bigint))::integer AS nodes_appended,
    (COALESCE((p.edges_appended)::bigint, d.edges_appended, (f.edges_appended)::bigint, (0)::bigint))::integer AS edges_appended,
    (COALESCE((p.node_retractions)::bigint, d.node_retractions, (f.node_retractions)::bigint, (0)::bigint))::integer AS node_retractions,
    (COALESCE((p.edge_retractions)::bigint, d.edge_retractions, (f.edge_retractions)::bigint, (0)::bigint))::integer AS edge_retractions
   FROM ((payload_runs p
     FULL JOIN diagnostic_runs d ON (((d.source_name = p.source_name) AND (d.run_id = p.run_id))))
     FULL JOIN fact_agg f ON (((f.source_name = COALESCE(p.source_name, d.source_name)) AND (f.run_id = COALESCE(p.run_id, d.run_id)))))
UNION ALL
 SELECT w.source AS source_name,
    NULL::text AS run_id,
    w.last_reconcile_at AS started_at,
    w.last_reconcile_at AS finished_at,
    NULL::integer AS nodes_appended,
    NULL::integer AS edges_appended,
    NULL::integer AS node_retractions,
    NULL::integer AS edge_retractions
   FROM okg.source_watermarks w
  WHERE (NOT (EXISTS ( SELECT 1
           FROM okg.source_progress sp2
          WHERE ((sp2.source_name = w.source) AND ((sp2.key ~~ 'sync.run.%'::text) OR (sp2.key ~~ 'sync.stage_timings.run.%'::text))))));


--
-- Name: v_subtypes; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_subtypes AS
 SELECT n.subtype,
    (count(*))::integer AS live_count,
    s.metadata AS catalog_metadata
   FROM (okg.v_nodes n
     LEFT JOIN okg.subtypes s ON (((s.name = n.subtype) AND (s.catalog_version_id = ( SELECT max(catalog_versions.catalog_version_id) AS max
           FROM okg.catalog_versions
          WHERE (catalog_versions.status = 'applied'::text))))))
  GROUP BY n.subtype, s.metadata;


--
-- Name: v_table_cells; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_table_cells AS
 SELECT ((((c.node_id || ':'::text) || (row_item.value ->> 'row_index'::text)) || ':'::text) || (cell_item.value ->> 'column_index'::text)) AS cell_key,
    c.node_id AS table_occurrence_id,
    e.dst AS doc_id,
    (d.attrs ->> 'path'::text) AS path,
    (d.attrs ->> 'repo'::text) AS repo,
    COALESCE((e.attrs ->> 'table_extraction_lane'::text), (c.attrs ->> 'table_extraction_lane'::text)) AS table_extraction_lane,
    ((row_item.value ->> 'row_index'::text))::integer AS row_index,
    (row_item.value ->> 'row_label'::text) AS row_label,
    ((cell_item.value ->> 'column_index'::text))::integer AS column_index,
    (cell_item.value ->> 'column_header'::text) AS column_header,
    (cell_item.value ->> 'text'::text) AS cell_text,
    ((cell_item.value ->> 'row_span'::text))::integer AS row_span,
    ((cell_item.value ->> 'column_span'::text))::integer AS column_span,
    COALESCE((NULLIF((e.attrs ->> 'char_offset'::text), ''::text))::integer, (NULLIF((c.attrs ->> 'char_offset'::text), ''::text))::integer) AS source_char_offset,
    COALESCE((NULLIF((e.attrs ->> 'char_end'::text), ''::text))::integer, (NULLIF((c.attrs ->> 'char_end'::text), ''::text))::integer) AS source_char_end,
    COALESCE((NULLIF((e.attrs ->> 'source_page_index'::text), ''::text))::integer, (NULLIF((c.attrs ->> 'source_page_index'::text), ''::text))::integer) AS source_page_index,
    COALESCE((e.attrs -> 'source_bbox'::text), (c.attrs -> 'source_bbox'::text)) AS source_bbox,
    COALESCE((e.attrs ->> 'source_content_sha256'::text), (c.attrs ->> 'source_content_sha256'::text)) AS source_content_sha256,
    COALESCE((e.attrs ->> 'cache_signature'::text), (c.attrs ->> 'cache_signature'::text)) AS cache_signature
   FROM ((((okg.v_nodes c
     JOIN okg.v_edges e ON (((e.src = c.node_id) AND (e.edge_type = 'member_of'::text))))
     JOIN okg.v_nodes d ON (((d.node_id = e.dst) AND (d.subtype = 'document'::text))))
     CROSS JOIN LATERAL jsonb_array_elements(((c.attrs -> 'table_grid'::text) -> 'rows'::text)) row_item(value))
     CROSS JOIN LATERAL jsonb_array_elements((row_item.value -> 'cells'::text)) cell_item(value))
  WHERE ((c.subtype = 'document_chunk'::text) AND ((c.attrs ->> 'chunk_kind'::text) = 'table'::text) AND ((c.attrs ->> 'table_grid_status'::text) = 'parsed'::text))
  ORDER BY e.dst, c.node_id, ((row_item.value ->> 'row_index'::text))::integer, ((cell_item.value ->> 'column_index'::text))::integer;


--
-- Name: v_telemetry_span_tree; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_telemetry_span_tree AS
 SELECT trace_id,
    span_id,
    parent_span_id,
    span_name,
    span_kind,
    status_code,
    deployment_name,
    branch_name,
    started_at,
    ended_at,
    duration_ms,
    generation_id,
    source_name,
    workflow_id,
    run_id,
    tool_name,
    route_id,
    session_id,
    attributes,
    payload_summary,
    unavailable_reason
   FROM okg.telemetry_spans;


--
-- Name: v_telemetry_trace_summary; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_telemetry_trace_summary AS
 SELECT t.trace_id,
    t.deployment_name,
    t.branch_name,
    t.root_name,
    t.root_kind,
    t.status_code,
    t.started_at,
    t.ended_at,
    t.duration_ms,
    t.generation_id,
    t.correlation_id,
    t.source_name,
    t.workflow_id,
    t.session_id,
    count(DISTINCT s.span_id) AS span_count,
    max(s.duration_ms) AS slowest_span_ms,
    (array_agg(s.span_name ORDER BY s.duration_ms DESC NULLS LAST))[1] AS slowest_span_name,
    count(DISTINCT a.artifact_id) AS artifact_count
   FROM ((okg.telemetry_traces t
     LEFT JOIN okg.telemetry_spans s ON (((s.deployment_name = t.deployment_name) AND (s.trace_id = t.trace_id))))
     LEFT JOIN okg.telemetry_artifacts a ON (((a.deployment_name = t.deployment_name) AND (a.trace_id = t.trace_id))))
  GROUP BY t.trace_id, t.deployment_name, t.branch_name, t.root_name, t.root_kind, t.status_code, t.started_at, t.ended_at, t.duration_ms, t.generation_id, t.correlation_id, t.source_name, t.workflow_id, t.session_id;


--
-- Name: v_tool_use_events; Type: VIEW; Schema: okg; Owner: -
--

CREATE VIEW okg.v_tool_use_events AS
 SELECT node_id AS tool_event_node_id,
    (attrs ->> 'event_id'::text) AS event_id,
    (attrs ->> 'session_id'::text) AS session_id,
    ('session:'::text || (attrs ->> 'session_id'::text)) AS session_node_id,
        CASE
            WHEN ((attrs ->> 'turn_index'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'turn_index'::text))::integer
            ELSE NULL::integer
        END AS turn_index,
        CASE
            WHEN ((attrs ->> 'block_index'::text) ~ '^-?[0-9]+$'::text) THEN ((attrs ->> 'block_index'::text))::integer
            ELSE NULL::integer
        END AS block_index,
    (attrs ->> 'tool_name'::text) AS tool_name,
    (attrs ->> 'mcp_server'::text) AS mcp_server,
    (attrs ->> 'mcp_deployment'::text) AS mcp_deployment,
    (attrs ->> 'mcp_operator'::text) AS mcp_operator,
    (attrs ->> 'file_path'::text) AS file_path,
    "left"(COALESCE((attrs ->> 'command'::text), ''::text), 500) AS command_excerpt,
    "left"(COALESCE((attrs ->> 'input_excerpt'::text), ''::text), 500) AS input_excerpt,
        CASE
            WHEN ((attrs ->> 'timestamp'::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'::text) THEN ((attrs ->> 'timestamp'::text))::timestamp with time zone
            ELSE NULL::timestamp with time zone
        END AS "timestamp"
   FROM okg.v_nodes n
  WHERE (subtype = 'tool_use_event'::text);


--
-- Name: verifier_runs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.verifier_runs (
    run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    deployment text NOT NULL,
    candidate_id text NOT NULL,
    model text NOT NULL,
    prompt_version text NOT NULL,
    verifier_version integer NOT NULL,
    input_tokens integer NOT NULL,
    output_tokens integer NOT NULL,
    cost_usd numeric(10,6) NOT NULL,
    verdict text,
    confidence numeric(4,3),
    emitted_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: visual_caption_failures; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.visual_caption_failures (
    visual_id text NOT NULL,
    vision_signature text NOT NULL,
    fail_count integer DEFAULT 0 NOT NULL,
    last_error text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: visual_crop_blobs; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.visual_crop_blobs (
    sha256 text NOT NULL,
    bytes bytea NOT NULL,
    size_bytes bigint NOT NULL,
    mime_type text DEFAULT 'image/png'::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: work_ledger; Type: TABLE; Schema: okg; Owner: -
--

CREATE TABLE okg.work_ledger (
    deployment text NOT NULL,
    generation_id text NOT NULL,
    mode text DEFAULT 'differential'::text NOT NULL,
    category text NOT NULL,
    validation boolean DEFAULT false NOT NULL,
    measured boolean DEFAULT true NOT NULL,
    work_units bigint,
    model_calls integer,
    prompt_tokens bigint,
    completion_tokens bigint,
    total_tokens bigint,
    cost_usd numeric(18,8),
    cause_counts jsonb,
    detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT work_ledger_category_ck CHECK ((category = ANY (ARRAY['effect_analysis'::text, 'certificate_validation'::text, 'deterministic_execution'::text, 'model_calls'::text, 'propagation'::text, 'repair_solving'::text]))),
    CONSTRAINT work_ledger_measured_units_ck CHECK ((measured = (work_units IS NOT NULL))),
    CONSTRAINT work_ledger_mode_ck CHECK ((mode = ANY (ARRAY['differential'::text, 'complete_reference'::text, 'dependency_baseline'::text]))),
    CONSTRAINT work_ledger_validation_ck CHECK ((validation = (mode <> 'differential'::text)))
);


--
-- Name: _chronos_b_interval_okg_console_count_rollup_257f8bc5b1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1 (
    dimension_kind text NOT NULL,
    dimension_value text NOT NULL,
    item_count bigint,
    live_lo numeric(32,0) NOT NULL,
    live_hi numeric(32,0) NOT NULL,
    writer_segment_id integer NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT _chronos_b_interval_okg_console_count_rollup_257f8bc5b1_check CHECK ((live_lo < live_hi))
);


--
-- Name: _chronos_b_interval_okg_graph_edges_d84ac83c2b; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_b_interval_okg_graph_edges_d84ac83c2b (
    edge_id bigint NOT NULL,
    src text,
    dst text,
    edge_type text,
    attrs jsonb,
    provenance text,
    confidence real,
    source text,
    source_record_id jsonb,
    source_revision jsonb,
    run_id text,
    observed_at timestamp with time zone,
    live_lo numeric(32,0) NOT NULL,
    live_hi numeric(32,0) NOT NULL,
    writer_segment_id integer NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT _chronos_b_interval_okg_graph_edges_d84ac83c2b_check CHECK ((live_lo < live_hi))
);


--
-- Name: _chronos_b_interval_okg_graph_nodes_1c8e1fc23f; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f (
    node_id text NOT NULL,
    subtype text,
    attrs jsonb,
    text_hash text,
    embedding public.vector,
    embedding_model text,
    source text,
    source_record_id jsonb,
    source_revision jsonb,
    run_id text,
    observed_at timestamp with time zone,
    live_lo numeric(32,0) NOT NULL,
    live_hi numeric(32,0) NOT NULL,
    writer_segment_id integer NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT _chronos_b_interval_okg_graph_nodes_1c8e1fc23f_check CHECK ((live_lo < live_hi))
);


--
-- Name: _chronos_b_interval_okg_ontology_rollup_b039672993; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_b_interval_okg_ontology_rollup_b039672993 (
    src_subtype text NOT NULL,
    edge_type text NOT NULL,
    dst_subtype text NOT NULL,
    producer text NOT NULL,
    edge_count bigint,
    live_lo numeric(32,0) NOT NULL,
    live_hi numeric(32,0) NOT NULL,
    writer_segment_id integer NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT _chronos_b_interval_okg_ontology_rollup_b039672993_check CHECK ((live_lo < live_hi))
);


--
-- Name: _chronos_branch_indexes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_branch_indexes (
    backend text NOT NULL,
    index_name text NOT NULL,
    table_name text NOT NULL,
    columns text NOT NULL
);


--
-- Name: _chronos_branch_interval_branches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_branch_interval_branches (
    branch_id text NOT NULL,
    current_segment_id integer NOT NULL,
    parent_branch_id text,
    child_count integer DEFAULT 0 NOT NULL,
    branch_kind text DEFAULT 'mutable'::text NOT NULL,
    created_at text NOT NULL,
    metadata text NOT NULL
);


--
-- Name: _chronos_branch_interval_segment_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public._chronos_branch_interval_segment_id_seq
    AS integer
    START WITH 2
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: _chronos_branch_interval_segments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_branch_interval_segments (
    segment_id integer NOT NULL,
    parent_segment_id integer,
    owner_branch_id text,
    segment_kind text DEFAULT 'mutable'::text NOT NULL,
    live_lo numeric(32,0) NOT NULL,
    live_hi numeric(32,0) NOT NULL,
    branch_point numeric(32,0) NOT NULL,
    created_at text NOT NULL,
    metadata text NOT NULL,
    CONSTRAINT _chronos_branch_interval_segments_check CHECK ((live_lo <= branch_point)),
    CONSTRAINT _chronos_branch_interval_segments_check1 CHECK ((branch_point < live_hi))
);


--
-- Name: _chronos_branch_tables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._chronos_branch_tables (
    table_name text NOT NULL,
    physical_table text NOT NULL,
    pk_columns text NOT NULL,
    columns text NOT NULL,
    column_defs text NOT NULL,
    backend text NOT NULL
);


--
-- Name: admission_events event_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_events ALTER COLUMN event_id SET DEFAULT nextval('okg.admission_events_event_id_seq'::regclass);


--
-- Name: admission_record_refs ref_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_record_refs ALTER COLUMN ref_id SET DEFAULT nextval('okg.admission_record_refs_ref_id_seq'::regclass);


--
-- Name: branch_merge_requests request_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branch_merge_requests ALTER COLUMN request_id SET DEFAULT nextval('okg.branch_merge_requests_request_id_seq'::regclass);


--
-- Name: branches branch_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branches ALTER COLUMN branch_id SET DEFAULT nextval('okg.branches_branch_id_seq'::regclass);


--
-- Name: catalog_versions catalog_version_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.catalog_versions ALTER COLUMN catalog_version_id SET DEFAULT nextval('okg.catalog_versions_catalog_version_id_seq'::regclass);


--
-- Name: derived_dirty_scopes dirty_scope_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_scopes ALTER COLUMN dirty_scope_id SET DEFAULT nextval('okg.derived_dirty_scopes_dirty_scope_id_seq'::regclass);


--
-- Name: derived_output_dependency_index dependency_index_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_dependency_index ALTER COLUMN dependency_index_id SET DEFAULT nextval('okg.derived_output_dependency_index_dependency_index_id_seq'::regclass);


--
-- Name: edge_facts fact_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.edge_facts ALTER COLUMN fact_id SET DEFAULT nextval('okg.edge_facts_fact_id_seq'::regclass);


--
-- Name: entity_resolution_rejections rejection_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_resolution_rejections ALTER COLUMN rejection_id SET DEFAULT nextval('okg.entity_resolution_rejections_rejection_id_seq'::regclass);


--
-- Name: envelope_violations id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.envelope_violations ALTER COLUMN id SET DEFAULT nextval('okg.envelope_violations_id_seq'::regclass);


--
-- Name: federation_mailbox_events event_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_mailbox_events ALTER COLUMN event_id SET DEFAULT nextval('okg.federation_mailbox_events_event_id_seq'::regclass);


--
-- Name: graph_generations generation_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.graph_generations ALTER COLUMN generation_id SET DEFAULT nextval('okg.graph_generations_generation_id_seq'::regclass);


--
-- Name: lease_audit audit_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lease_audit ALTER COLUMN audit_id SET DEFAULT nextval('okg.lease_audit_audit_id_seq'::regclass);


--
-- Name: llm_calls id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.llm_calls ALTER COLUMN id SET DEFAULT nextval('okg.llm_calls_id_seq'::regclass);


--
-- Name: mcp_call_log call_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.mcp_call_log ALTER COLUMN call_id SET DEFAULT nextval('okg.mcp_call_log_call_id_seq'::regclass);


--
-- Name: mcp_trace_events event_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.mcp_trace_events ALTER COLUMN event_id SET DEFAULT nextval('okg.mcp_trace_events_event_id_seq'::regclass);


--
-- Name: node_facts fact_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.node_facts ALTER COLUMN fact_id SET DEFAULT nextval('okg.node_facts_fact_id_seq'::regclass);


--
-- Name: node_series sample_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.node_series ALTER COLUMN sample_id SET DEFAULT nextval('okg.node_series_sample_id_seq'::regclass);


--
-- Name: nomos_config_activations activation_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_activations ALTER COLUMN activation_row_id SET DEFAULT nextval('okg.nomos_config_activations_activation_row_id_seq'::regclass);


--
-- Name: nomos_config_sets config_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_sets ALTER COLUMN config_row_id SET DEFAULT nextval('okg.nomos_config_sets_config_row_id_seq'::regclass);


--
-- Name: nomos_decisions decision_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_decisions ALTER COLUMN decision_row_id SET DEFAULT nextval('okg.nomos_decisions_decision_row_id_seq'::regclass);


--
-- Name: nomos_external_claims claim_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_external_claims ALTER COLUMN claim_row_id SET DEFAULT nextval('okg.nomos_external_claims_claim_row_id_seq'::regclass);


--
-- Name: nomos_identity_providers provider_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_identity_providers ALTER COLUMN provider_row_id SET DEFAULT nextval('okg.nomos_identity_providers_provider_row_id_seq'::regclass);


--
-- Name: nomos_permission_grants grant_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_permission_grants ALTER COLUMN grant_row_id SET DEFAULT nextval('okg.nomos_permission_grants_grant_row_id_seq'::regclass);


--
-- Name: nomos_policy_activations activation_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_activations ALTER COLUMN activation_row_id SET DEFAULT nextval('okg.nomos_policy_activations_activation_row_id_seq'::regclass);


--
-- Name: nomos_policy_bundles bundle_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_bundles ALTER COLUMN bundle_row_id SET DEFAULT nextval('okg.nomos_policy_bundles_bundle_row_id_seq'::regclass);


--
-- Name: nomos_subject_bindings binding_row_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_subject_bindings ALTER COLUMN binding_row_id SET DEFAULT nextval('okg.nomos_subject_bindings_binding_row_id_seq'::regclass);


--
-- Name: observation_events event_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_events ALTER COLUMN event_id SET DEFAULT nextval('okg.observation_events_event_id_seq'::regclass);


--
-- Name: poros_delivery_attempts attempt_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_delivery_attempts ALTER COLUMN attempt_id SET DEFAULT nextval('okg.poros_delivery_attempts_attempt_id_seq'::regclass);


--
-- Name: poros_detached_object_manifests manifest_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_detached_object_manifests ALTER COLUMN manifest_id SET DEFAULT nextval('okg.poros_detached_object_manifests_manifest_id_seq'::regclass);


--
-- Name: poros_inbox_messages inbox_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_inbox_messages ALTER COLUMN inbox_id SET DEFAULT nextval('okg.poros_inbox_messages_inbox_id_seq'::regclass);


--
-- Name: poros_incorporation_events event_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_incorporation_events ALTER COLUMN event_id SET DEFAULT nextval('okg.poros_incorporation_events_event_id_seq'::regclass);


--
-- Name: producer_profiles profile_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.producer_profiles ALTER COLUMN profile_id SET DEFAULT nextval('okg.producer_profiles_profile_id_seq'::regclass);


--
-- Name: projection_pending pending_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.projection_pending ALTER COLUMN pending_id SET DEFAULT nextval('okg.projection_pending_pending_id_seq'::regclass);


--
-- Name: telemetry_events event_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_events ALTER COLUMN event_id SET DEFAULT nextval('okg.telemetry_events_event_id_seq'::regclass);


--
-- Name: telemetry_graph_effects effect_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_graph_effects ALTER COLUMN effect_id SET DEFAULT nextval('okg.telemetry_graph_effects_effect_id_seq'::regclass);


--
-- Name: telemetry_metrics metric_id; Type: DEFAULT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_metrics ALTER COLUMN metric_id SET DEFAULT nextval('okg.telemetry_metrics_metric_id_seq'::regclass);


--
-- Data for Name: hypertable; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.hypertable (id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size, compression_state, compressed_hypertable_id, status) FROM stdin;
3	_timescaledb_internal	_compressed_hypertable_3	_timescaledb_internal	_hyper_3	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
1	okg	node_facts	_timescaledb_internal	_hyper_1	1	_timescaledb_functions	calculate_chunk_interval	0	1	3	0
4	_timescaledb_internal	_compressed_hypertable_4	_timescaledb_internal	_hyper_4	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
2	okg	edge_facts	_timescaledb_internal	_hyper_2	1	_timescaledb_functions	calculate_chunk_interval	0	1	4	0
6	_timescaledb_internal	_compressed_hypertable_6	_timescaledb_internal	_hyper_6	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
5	okg	mcp_trace_events	_timescaledb_internal	_hyper_5	1	_timescaledb_functions	calculate_chunk_interval	0	1	6	0
8	_timescaledb_internal	_compressed_hypertable_8	_timescaledb_internal	_hyper_8	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
7	okg	telemetry_spans	_timescaledb_internal	_hyper_7	1	_timescaledb_functions	calculate_chunk_interval	0	1	8	0
10	_timescaledb_internal	_compressed_hypertable_10	_timescaledb_internal	_hyper_10	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
9	okg	telemetry_events	_timescaledb_internal	_hyper_9	1	_timescaledb_functions	calculate_chunk_interval	0	1	10	0
12	_timescaledb_internal	_compressed_hypertable_12	_timescaledb_internal	_hyper_12	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
11	okg	telemetry_metrics	_timescaledb_internal	_hyper_11	1	_timescaledb_functions	calculate_chunk_interval	0	1	12	0
14	_timescaledb_internal	_compressed_hypertable_14	_timescaledb_internal	_hyper_14	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
13	okg	node_series	_timescaledb_internal	_hyper_13	1	_timescaledb_functions	calculate_chunk_interval	0	1	14	0
16	_timescaledb_internal	_compressed_hypertable_16	_timescaledb_internal	_hyper_16	0	_timescaledb_functions	calculate_chunk_interval	0	2	\N	0
15	okg	mcp_call_log	_timescaledb_internal	_hyper_15	1	_timescaledb_functions	calculate_chunk_interval	0	1	16	0
\.


--
-- Data for Name: bgw_job; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.bgw_job (id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, fixed_schedule, initial_start, hypertable_id, config, check_schema, check_name, timezone) FROM stdin;
1000	Retention Policy [1000]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	1	{"drop_after": "180 days", "hypertable_id": 1}	_timescaledb_functions	policy_retention_check	\N
1001	Retention Policy [1001]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	2	{"drop_after": "180 days", "hypertable_id": 2}	_timescaledb_functions	policy_retention_check	\N
1002	Columnstore Policy [1002]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	1	{"hypertable_id": 1, "compress_after": "30 days"}	_timescaledb_functions	policy_compression_check	\N
1003	Columnstore Policy [1003]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	2	{"hypertable_id": 2, "compress_after": "30 days"}	_timescaledb_functions	policy_compression_check	\N
1004	Columnstore Policy [1004]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	5	{"hypertable_id": 5, "compress_after": "7 days"}	_timescaledb_functions	policy_compression_check	\N
1005	Retention Policy [1005]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	5	{"drop_after": "30 days", "hypertable_id": 5}	_timescaledb_functions	policy_retention_check	\N
1006	Columnstore Policy [1006]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	7	{"hypertable_id": 7, "compress_after": "7 days"}	_timescaledb_functions	policy_compression_check	\N
1007	Retention Policy [1007]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	7	{"drop_after": "30 days", "hypertable_id": 7}	_timescaledb_functions	policy_retention_check	\N
1008	Columnstore Policy [1008]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	9	{"hypertable_id": 9, "compress_after": "7 days"}	_timescaledb_functions	policy_compression_check	\N
1009	Retention Policy [1009]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	9	{"drop_after": "30 days", "hypertable_id": 9}	_timescaledb_functions	policy_retention_check	\N
1010	Columnstore Policy [1010]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	11	{"hypertable_id": 11, "compress_after": "7 days"}	_timescaledb_functions	policy_compression_check	\N
1011	Retention Policy [1011]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	11	{"drop_after": "30 days", "hypertable_id": 11}	_timescaledb_functions	policy_retention_check	\N
1012	Columnstore Policy [1012]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	13	{"hypertable_id": 13, "compress_after": "7 days"}	_timescaledb_functions	policy_compression_check	\N
1013	Retention Policy [1013]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	13	{"drop_after": "30 days", "hypertable_id": 13}	_timescaledb_functions	policy_retention_check	\N
1014	Columnstore Policy [1014]	12:00:00	00:00:00	-1	01:00:00	_timescaledb_functions	policy_compression	postgres	t	f	\N	15	{"hypertable_id": 15, "compress_after": "7 days"}	_timescaledb_functions	policy_compression_check	\N
1015	Retention Policy [1015]	1 day	00:05:00	-1	00:05:00	_timescaledb_functions	policy_retention	postgres	t	f	\N	15	{"drop_after": "90 days", "hypertable_id": 15}	_timescaledb_functions	policy_retention_check	\N
\.


--
-- Data for Name: chunk; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name, compressed_chunk_id, status, osm_chunk, creation_time) FROM stdin;
\.


--
-- Data for Name: chunk_column_stats; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.chunk_column_stats (id, hypertable_id, chunk_id, column_name, range_start, range_end, valid) FROM stdin;
\.


--
-- Data for Name: compression_chunk_size; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.compression_chunk_size (chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size, uncompressed_index_size, compressed_heap_size, compressed_toast_size, compressed_index_size, numrows_pre_compression, numrows_post_compression, numrows_frozen_immediately) FROM stdin;
\.


--
-- Data for Name: compression_settings; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.compression_settings (relid, compress_relid, segmentby, orderby, orderby_desc, orderby_nullsfirst, index) FROM stdin;
okg.node_facts	\N	{node_id}	{observed_at}	{t}	{t}	[{"type": "minmax", "column": "observed_at", "source": "orderby"}, {"type": "firstlast", "column": "observed_at", "source": "orderby"}]
okg.edge_facts	\N	{src,edge_type}	{observed_at}	{t}	{t}	[{"type": "minmax", "column": "observed_at", "source": "orderby"}, {"type": "firstlast", "column": "observed_at", "source": "orderby"}]
okg.mcp_trace_events	\N	{deployment_name}	{event_time}	{t}	{t}	[{"type": "minmax", "column": "event_time", "source": "orderby"}, {"type": "firstlast", "column": "event_time", "source": "orderby"}]
okg.telemetry_spans	\N	{deployment_name}	{event_time}	{t}	{t}	[{"type": "minmax", "column": "event_time", "source": "orderby"}, {"type": "firstlast", "column": "event_time", "source": "orderby"}]
okg.telemetry_events	\N	{event_name}	{event_time}	{t}	{t}	[{"type": "minmax", "column": "event_time", "source": "orderby"}, {"type": "firstlast", "column": "event_time", "source": "orderby"}]
okg.telemetry_metrics	\N	{metric_name}	{metric_time}	{t}	{t}	[{"type": "minmax", "column": "metric_time", "source": "orderby"}, {"type": "firstlast", "column": "metric_time", "source": "orderby"}]
okg.node_series	\N	{deployment_name}	{observed_at}	{t}	{t}	[{"type": "minmax", "column": "observed_at", "source": "orderby"}, {"type": "firstlast", "column": "observed_at", "source": "orderby"}]
okg.mcp_call_log	\N	{tool_name}	{ts}	{t}	{t}	[{"type": "minmax", "column": "ts", "source": "orderby"}, {"type": "firstlast", "column": "ts", "source": "orderby"}]
\.


--
-- Data for Name: continuous_agg; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_agg (mat_hypertable_id, raw_hypertable_id, parent_mat_hypertable_id, user_view_schema, user_view_name, partial_view_schema, partial_view_name, direct_view_schema, direct_view_name, materialized_only, schema_change_timestamp) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_bucket_function; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_bucket_function (mat_hypertable_id, bucket_func, bucket_width, bucket_origin, bucket_offset, bucket_timezone, bucket_fixed_width) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_hypertable_invalidation_log; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value, greatest_modified_value) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_invalidation_threshold; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_invalidation_threshold (hypertable_id, watermark) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_jobs_refresh_ranges; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges (materialization_id, start_range, end_range, pid, job_id, created_at) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_materialization_invalidation_log; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_materialization_ranges; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_materialization_ranges (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
\.


--
-- Data for Name: continuous_aggs_watermark; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark) FROM stdin;
\.


--
-- Data for Name: dimension; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.dimension (id, hypertable_id, column_name, column_type, aligned, num_slices, partitioning_func_schema, partitioning_func, interval_length, compress_interval_length, integer_now_func_schema, integer_now_func) FROM stdin;
1	1	observed_at	timestamp with time zone	t	\N	\N	\N	604800000000	\N	\N	\N
2	2	observed_at	timestamp with time zone	t	\N	\N	\N	604800000000	\N	\N	\N
3	5	event_time	timestamp with time zone	t	\N	\N	\N	86400000000	\N	\N	\N
4	7	event_time	timestamp with time zone	t	\N	\N	\N	86400000000	\N	\N	\N
5	9	event_time	timestamp with time zone	t	\N	\N	\N	86400000000	\N	\N	\N
6	11	metric_time	timestamp with time zone	t	\N	\N	\N	86400000000	\N	\N	\N
7	13	observed_at	timestamp with time zone	t	\N	\N	\N	86400000000	\N	\N	\N
8	15	ts	timestamp with time zone	t	\N	\N	\N	604800000000	\N	\N	\N
\.


--
-- Data for Name: dimension_slice; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.dimension_slice (id, chunk_id, dimension_id, range_start, range_end) FROM stdin;
\.


--
-- Data for Name: metadata; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.metadata (key, value, include_in_telemetry) FROM stdin;
install_timestamp	2026-07-20 14:40:18.216328+00	t
timescaledb_version	2.28.2	f
\.


--
-- Data for Name: tablespace; Type: TABLE DATA; Schema: _timescaledb_catalog; Owner: -
--

COPY _timescaledb_catalog.tablespace (id, hypertable_id, tablespace_name) FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_10; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_10  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_12; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_12  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_14; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_14  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_16; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_16  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_3; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_3  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_4; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_4  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_6; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_6  FROM stdin;
\.


--
-- Data for Name: _compressed_hypertable_8; Type: TABLE DATA; Schema: _timescaledb_internal; Owner: -
--

COPY _timescaledb_internal._compressed_hypertable_8  FROM stdin;
\.


--
-- Data for Name: admission_envelopes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.admission_envelopes (admission_id, deployment_name, producer_id, producer_kind, intent, admission_mode, status, catalog_version_id, source, run_id, target_generation_id, idempotency_key, authority, check_summary, verdict, workflow_ref, created_at, updated_at, decided_at, published_at, target_branch_name, target_generation_ref) FROM stdin;
\.


--
-- Data for Name: admission_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.admission_events (event_id, admission_id, event_type, actor, reason, payload, workflow_ref, created_at) FROM stdin;
\.


--
-- Data for Name: admission_record_refs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.admission_record_refs (ref_id, admission_id, ref_key, ref_kind, source, run_id, fact_id_min, fact_id_max, record_count, payload, created_at) FROM stdin;
\.


--
-- Data for Name: alias_collisions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.alias_collisions (catalog_version_id, subtype, field, alias_value, scope, canonicals, distinct_canonical_count, detected_at) FROM stdin;
\.


--
-- Data for Name: aliases; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.aliases (alias, normalized_alias, canonical, scope, catalog_version_id) FROM stdin;
\.


--
-- Data for Name: attribute_effective_attrs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_effective_attrs (target_kind, target_id, generation_id, namespace, attr_key, observation_id, value, value_hash, producer, provenance, dependencies, dependency_fingerprint, confidence, materialized_at) FROM stdin;
\.


--
-- Data for Name: attribute_effective_integrity_receipts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_effective_integrity_receipts (receipt_digest, receipt_type, deployment_name, branch_id, namespace, generation_id, ordinal, prior_receipt_digest, checkpoint_node_count, checkpoint_edge_count, logical_node_count, logical_edge_count, schema_version, schema_digest, migration_digest, logical_payload_seed0, logical_payload_seed1, interval_payload_seed0, interval_payload_seed1, dirty_scope_digest, mutation_epoch, created_at) FROM stdin;
\.


--
-- Data for Name: attribute_effective_integrity_state; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_effective_integrity_state (deployment_name, branch_id, namespace, mutation_epoch, clean_mutation_epoch, dirty, dirty_reason, latest_receipt_digest, updated_at) FROM stdin;
\.


--
-- Data for Name: attribute_effective_integrity_transitions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_effective_integrity_transitions (deployment_name, branch_id, namespace, backend_pid, backend_start, started_at) FROM stdin;
\.


--
-- Data for Name: attribute_effective_view; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_effective_view (target_kind, target_id, generation_id, namespace, subtype, src, dst, edge_type, raw_attrs, attrs, overlay_count, raw_text_hash, materialized_at) FROM stdin;
\.


--
-- Data for Name: attribute_effective_view_intervals; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_effective_view_intervals (target_kind, target_id, namespace, branch_id, from_ord, to_ord, subtype, src, dst, edge_type, raw_attrs, attrs, overlay_count, raw_text_hash, materialized_at) FROM stdin;
\.


--
-- Data for Name: attribute_observations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.attribute_observations (observation_id, target_kind, target_id, generation_id, namespace, attr_key, value, value_hash, status, producer, provenance, dependencies, dependency_fingerprint, supersedes, superseded_by, decision, confidence, observed_at, updated_at) FROM stdin;
\.


--
-- Data for Name: branch_merge_requests; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.branch_merge_requests (request_id, deployment_name, source_branch_id, target_branch_id, status, predicate, verdict, decided_by, decided_at, applied_at, created_at, metadata) FROM stdin;
\.


--
-- Data for Name: branches; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.branches (branch_id, deployment_name, name, role, base_branch_id, base_generation_id, status, policy_clauses, created_at, closed_at, metadata) FROM stdin;
1	bootstrap	default	default	\N	\N	active	{}	2026-08-04 04:48:08.096883+00	\N	{"auto_materialized": "_fill_default_branch_id trigger"}
\.


--
-- Data for Name: catalog_versions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.catalog_versions (catalog_version_id, status, applied_at, diff_summary, reconcile_plan, deployment_name, catalog_fingerprint, schema_fingerprint, writer_fingerprint, writer_provenance, created_by_command, created_at) FROM stdin;
1	applied	2026-08-04 04:48:08.096883+00	{"reason": "initial seed at okg migrate"}	{"reconcile": []}	\N	\N	\N	\N	{}	\N	2026-08-04 04:48:08.096883+00
\.


--
-- Data for Name: chronos_hot_index_manifest; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.chronos_hot_index_manifest (logical_table, physical_table, suffix, full_index_name, hot_index_name, floor, headroom_generations, floor_checkpoint_segment_id, floor_checkpoint_branch_point, open_interval_value, state, index_definition, index_bytes_at_activation, failure, created_at, activated_at) FROM stdin;
\.


--
-- Data for Name: chronos_native_graph_storage_marker; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.chronos_native_graph_storage_marker (enabled_at, reason) FROM stdin;
2026-08-04 04:48:08.096883+00	chronos_native_graph_storage
\.


--
-- Data for Name: compacted_generations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.compacted_generations (checkpoint_id, target_generation_id, deployment_name, branch_id, plan_id, compacted_at, reason) FROM stdin;
\.


--
-- Data for Name: consistency_candidate_coverage; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.consistency_candidate_coverage (run_id, check_id, target_generation_id, universe_key, coverage_basis, coverage_complete, scoped, scope_subject_count, eligible_input_count, excluded_input_counts, missing_text_count, missing_embedding_count, block_count, candidate_count, candidate_count_by_block, largest_block_size, p95_block_size, all_pairs_upper_bound, reduction_ratio, evaluator_invocation_count, evaluator_cost, false_negative_audit, payload, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: consistency_candidates; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.consistency_candidates (check_id, target_generation_id, candidate_id, run_id, input_scope, blocking_rule_hash, participant_roles, participant_anchors, predicate_key, generation_policy, coverage_status, exclusion_reason, candidate_payload, created_at) FROM stdin;
\.


--
-- Data for Name: consistency_check_definitions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.consistency_check_definitions (check_id, deployment_name, predicate_family, candidate_rule_hash, evaluator_ref, evaluator_hash, confidence_type, coverage_contract, severity_policy, created_at, updated_at, check_kind) FROM stdin;
\.


--
-- Data for Name: consistency_gate_results; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.consistency_gate_results (result_id, run_id, check_id, target_generation_id, gate_name, result, blocks_publish, reason, open_observation_count, newly_opened_count, resolved_count, is_probe, coverage_complete, coverage_basis, budget_ms, elapsed_ms, preflight, details, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: console_count_rollup; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.console_count_rollup (dimension_kind, dimension_value, item_count) FROM stdin;
\.


--
-- Data for Name: coordination_native_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.coordination_native_events (event_id, deployment_name, event_type, change_id, task_id, actor_id, actor_session_id, role_agent_id, workspace_id, payload, observed_at, idempotency_key, expected_revision, resulting_revision, status, rejection_code, rejection_reason, created_at) FROM stdin;
\.


--
-- Data for Name: coordination_native_state; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.coordination_native_state (deployment_name, change_id, task_id, work_item_id, revision, current_authority, active_claim_owner, workspace_id, claim_state, lease_state, lease_fresh, blocked, blocker_reason, proof_attached, proof_refs, review_state, merge_ready, supervisor_finalized, subagents_rolled_up, finalized_state, task_done, last_heartbeat_at, last_operator_report_kind, last_event_id, attrs, updated_at) FROM stdin;
\.


--
-- Data for Name: database_owner; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.database_owner (singleton, owner_deployment_name, db_owner_uuid, schema_fingerprint, stamped_at, stamped_by_command) FROM stdin;
\.


--
-- Data for Name: deployment_apply_runs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.deployment_apply_runs (deployment_name, desired_state_digest, plan_id, status, current_stage, desired_state, completed_stages, stage_receipts, error, started_at, updated_at, completed_at) FROM stdin;
\.


--
-- Data for Name: deployment_promotion_fences; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.deployment_promotion_fences (deployment_name, promotion_id, status, schedules_removed, schedule_snapshot, fenced_at, updated_at, blocker, metadata) FROM stdin;
\.


--
-- Data for Name: deployment_worker_ack; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.deployment_worker_ack (deployment_name, desired_state_digest, source_registry_fingerprint, source_fingerprints, runtime_fingerprint, worker_config_fingerprint, service_authority, observed_runtime_authority, manifest_fingerprint, catalog_fingerprint, schema_fingerprint, writer_fingerprint, schedule_fingerprint, active_sources, active_schedules, acknowledged_at) FROM stdin;
\.


--
-- Data for Name: deployment_write_ownership; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.deployment_write_ownership (deployment_name, owner_fingerprint, schema_fingerprint, catalog_fingerprint, deployment_root_hash, worktree_root_hash, status, claimed_at, claimed_by_command, last_seen_catalog_version_id, manifest_fingerprint, metadata) FROM stdin;
\.


--
-- Data for Name: derived_artifact_definitions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_artifact_definitions (definition_id, artifact_kind, deployment_name, logical_key, compute_hash, output_schema_hash, renderer_hash, dirty_rule_hash, renderer_ref, output_contract, dirty_rule, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: derived_artifact_dependencies; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_artifact_dependencies (dependency_id, downstream_definition_id, upstream_definition_id, upstream_slot_name, dependency_kind, scope_key, required_compute_hash, required_output_schema_hash, required_renderer_hash, required_dirty_rule_hash, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: derived_artifact_runs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_artifact_runs (run_id, definition_id, target_generation_id, status, trigger_kind, compute_hash, output_schema_hash, renderer_hash, dirty_rule_hash, dependency_manifest_ids, output_manifest_id, reused_from_run_id, total_scopes, dirty_scopes, dirty_rows, reused_scopes, fallback_count, metrics, error, started_at, completed_at) FROM stdin;
\.


--
-- Data for Name: derived_dirty_rule_audits; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_dirty_rule_audits (audit_id, dirty_rule_run_id, definition_id, target_generation_id, audit_kind, status, expected_scope_count, observed_scope_count, missed_scope_count, missed_scope_keys, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: derived_dirty_rule_runs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_dirty_rule_runs (dirty_rule_run_id, definition_id, target_generation_id, dirty_rule_hash, scope_universe_hash, dirty_query_hash, audit_query_hash, status, fallback_kind, total_scope_count, dirty_scope_count, fallback_scope_count, missed_scope_count, elapsed_ms, metrics, error, started_at, completed_at) FROM stdin;
\.


--
-- Data for Name: derived_dirty_scopes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_dirty_scopes (dirty_scope_id, dirty_rule_run_id, definition_id, target_generation_id, scope_key, dirty_reason, dependency_kind, dependency_ref, dependency_hash, queued_at, consumed_run_id, consumed_at) FROM stdin;
\.


--
-- Data for Name: derived_output_dependency_index; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_output_dependency_index (dependency_index_id, lineage_envelope_id, manifest_id, output_id, dependency_kind, dependency_ref, dependency_key_hash, created_at) FROM stdin;
\.


--
-- Data for Name: derived_output_lineage_envelopes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_output_lineage_envelopes (lineage_envelope_id, manifest_id, definition_id, target_generation_id, output_id, output_kind, lineage_schema, envelope_hash, output_digest, previous_output_digest, component_id, component_hash, invalidation_scope, envelope, created_at) FROM stdin;
\.


--
-- Data for Name: derived_output_manifests; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_output_manifests (manifest_id, run_id, definition_id, target_generation_id, status, required_slots, manifest_hash, reused_from_manifest_id, metadata, staged_at, promoted_at, failed_at) FROM stdin;
\.


--
-- Data for Name: derived_output_slots; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_output_slots (slot_id, manifest_id, slot_name, storage_kind, media_type, artifact_type, renderer_ref, payload_hash, storage_ref, inline_payload, row_count, byte_count, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: derived_relation_registry; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_relation_registry (relation_id, definition_id, logical_relation_id, schema_name, physical_name, output_schema_hash, ddl_hash, partition_policy, index_policy, retention_policy, status, created_at, retired_at) FROM stdin;
\.


--
-- Data for Name: derived_run_dependency_bindings; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_run_dependency_bindings (binding_id, run_id, downstream_definition_id, upstream_definition_id, upstream_manifest_id, upstream_slot_name, upstream_target_generation_id, scope_key, required_compute_hash, required_output_schema_hash, required_renderer_hash, required_dirty_rule_hash, bound_compute_hash, bound_output_schema_hash, bound_renderer_hash, bound_dirty_rule_hash, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: derived_schedule_nodes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_schedule_nodes (plan_id, definition_id, topo_index, status, run_id, dirty_scope_count, upstream_binding_count, error, started_at, completed_at) FROM stdin;
\.


--
-- Data for Name: derived_schedule_plans; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.derived_schedule_plans (plan_id, deployment_name, target_generation_id, execution_phase, catalog_version_id, graph_hash, status, error, started_at, completed_at) FROM stdin;
\.


--
-- Data for Name: edge_fact_strandings; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.edge_fact_strandings (fact_id, first_seen_gen, last_retry_gen, retry_count, reason) FROM stdin;
\.


--
-- Data for Name: edge_facts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.edge_facts (fact_id, edge_id, src, dst, edge_type, attrs, provenance, confidence, source, source_record_id, source_revision, observed_at, dedupe_key, run_id, op, source_program_checksum) FROM stdin;
\.


--
-- Data for Name: effective_attribute_recovery_attempts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.effective_attribute_recovery_attempts (classification_binding_digest, deployment_name, branch_name, predecessor_generation_id, expected_classification, stage, authorization_payload, created_at, updated_at, consumed_by_generation_id, consumed_at) FROM stdin;
\.


--
-- Data for Name: enricher_backfill_progress; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.enricher_backfill_progress (deployment, branch_name, enricher_name, input_node_id, catalog_version_id, profile_fingerprint, transform_checksum, status, attempts, emitted_edge_count, last_run_id, metadata, completed_at, updated_at) FROM stdin;
\.


--
-- Data for Name: enricher_progress; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.enricher_progress (deployment, enricher_name, fact_watermark, edge_watermark, generation_id, catalog_version_id, last_run_at, last_status, lifetime_edges_emitted, dormant_ok, consecutive_failure_count) FROM stdin;
\.


--
-- Data for Name: entity_identity_claims; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.entity_identity_claims (deployment_name, branch_name, subtype, normalized_key, canonical_node_id, raw_name, resolution_method, policy_fingerprint, catalog_version_id, graph_token, claimed_at) FROM stdin;
\.


--
-- Data for Name: entity_merge_receipts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.entity_merge_receipts (merge_id, deployment_name, branch_name, survivor_node_id, retired_node_id, subtype, pinned_checkpoint, catalog_version_id, footprint_digest, plan_digest, plan, retired_snapshot, status, repair_generation_id, actor, applied_at, published_at) FROM stdin;
\.


--
-- Data for Name: entity_resolution_rejections; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.entity_resolution_rejections (rejection_id, deployment_name, branch_name, subtype, normalized_key, raw_name, reason, policy_fingerprint, source_node_ref, occurrences, first_seen_at, last_seen_at) FROM stdin;
\.


--
-- Data for Name: envelope_violations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.envelope_violations (id, deployment, generation_id, producer_id, code, record_kind, detail, poisons_reuse, recorded_at) FROM stdin;
\.


--
-- Data for Name: equivalence_verdicts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.equivalence_verdicts (deployment, update_index, production_generation, reference_ref, comparison, verdict, release_blocking, artifact, work_summary, certified_differences, recorded_at) FROM stdin;
\.


--
-- Data for Name: external_probe_cache; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.external_probe_cache (deployment_name, node_id, backing_fingerprint, field, observed_at, expires_at, status, payload, error, trust_label, provenance, updated_at) FROM stdin;
\.


--
-- Data for Name: extractor_retry_queue; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.extractor_retry_queue (deployment_name, branch_name, node_id, reason, enqueued_at) FROM stdin;
\.


--
-- Data for Name: fact_batch_receipts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.fact_batch_receipts (admission_id, run_id, chunk_ordinal, canonical_digest, target_branch, state, marker_digest, graph_result, branch_token, audit_fact_ranges, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: federation_connection_requests; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.federation_connection_requests (deployment_name, request_id, local_authority_id, requester_authority_id, target_authority_id, requested_action, target_address, requested_visibility, key_fingerprint, discovery_hash, status, reviewer_identity, nomos_decision_id, nomos_decision_deployment_name, decision_evidence, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: federation_discovered_candidates; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.federation_discovered_candidates (deployment_name, local_authority_id, remote_authority_id, discovery_hash, document, status, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: federation_mailbox_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.federation_mailbox_events (event_id, message_id, event_type, sender_authority_id, recipient_authority_id, status, reason_code, payload, created_at) FROM stdin;
\.


--
-- Data for Name: federation_mailbox_messages; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.federation_mailbox_messages (message_id, sender_authority_id, recipient_authority_id, payload_mode, packet_sha256, body_sha256, envelope, pointer, object_uri, object_sha256, size_bytes, expires_at, status, lease_token, lease_expires_at, receipt_evidence, denial_evidence, deny_reason_code, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: federation_peer_trust_pins; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.federation_peer_trust_pins (deployment_name, local_authority_id, remote_authority_id, key_fingerprint, allowed_actions, allowed_boards, allowed_pull_surfaces, visibility_tiers, status, nomos_decision_id, nomos_decision_deployment_name, nomos_effect, decision_evidence, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: federation_route_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.federation_route_events (deployment_name, route_event_id, local_authority_id, remote_authority_id, subject, action, target_address, visibility, decision, reason_code, nomos_decision_id, nomos_decision_deployment_name, trust_pin_fingerprint, live_row_delta, created_at) FROM stdin;
\.


--
-- Data for Name: generation_ordinals; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.generation_ordinals (checkpoint_id, branch_id, ordinal, previous_checkpoint_id, deployment_name, created_at) FROM stdin;
\.


--
-- Data for Name: graph_edges; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.graph_edges (edge_id, src, dst, edge_type, attrs, provenance, confidence, source, source_record_id, source_revision, run_id, observed_at) FROM stdin;
\.


--
-- Data for Name: graph_generations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.graph_generations (generation_id, deployment_name, status, fact_watermark, edge_fact_watermark, catalog_version_id, published_at, invariant_summary, excluded_sources, blocked_reason, correlation_id, trace_id, created_at, nodes_added, nodes_retracted, edges_added, edges_retracted, nomos_policy_bundle_ids, nomos_policy_fingerprints, branch_id) FROM stdin;
0	bootstrap	published	\N	\N	1	2026-08-04 04:48:08.096883+00	{"reason": "bootstrap generation zero"}	[]	\N	bootstrap:generation:0	\N	2026-08-04 04:48:08.096883+00	0	0	0	0	{}	{}	1
\.


--
-- Data for Name: graph_nodes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.graph_nodes (node_id, subtype, attrs, text_hash, embedding, embedding_model, source, source_record_id, source_revision, run_id, observed_at) FROM stdin;
\.


--
-- Data for Name: history_compaction_plans; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.history_compaction_plans (plan_id, deployment_name, branch_id, status, plan, progress, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: history_generation_pins; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.history_generation_pins (deployment_name, branch_id, pin_name, checkpoint_id, expires_at, created_at) FROM stdin;
\.


--
-- Data for Name: identifier_patterns; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.identifier_patterns (id_type, regex, validator_kind, description, catalog_version_id) FROM stdin;
\.


--
-- Data for Name: invariants; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.invariants (name, severity, scope, source, sql_body, timeout_ms, kind, candidates_sql, judge_prompt, judge_model, sample_cap, fire_on, catalog_version_id, concept_scope) FROM stdin;
\.


--
-- Data for Name: ir_invocations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.ir_invocations (invocation_id, program_id, binding_id, deployment_name, invocation_kind, branch_name, generation_id, status, verdict, reason, started_at, completed_at, result_count, node_delta_count, edge_delta_count, cost_proxy, quality_score, error_count, truncation_count, metrics, created_at) FROM stdin;
\.


--
-- Data for Name: ir_program_bindings; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.ir_program_bindings (binding_id, program_id, deployment_name, owner_kind, owner_id, program_role, generation_id, branch_name, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: ir_programs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.ir_programs (program_id, program_checksum, ir_version, program_kind, registry_id, registry_version, registry_checksum, effect_summary, determinism, op_counts, function_counts, cardinality_summary, payload_mode, program_json, program_ref, created_at) FROM stdin;
\.


--
-- Data for Name: lease_audit; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.lease_audit (audit_id, at, deployment_name, scope, event, holder_id, prior_holder_id, lease_seq, holder_meta) FROM stdin;
\.


--
-- Data for Name: lens_definitions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.lens_definitions (definition_id, deployment_name, lens_id, view_definition_id, view_id, lens_contract, putback_policy, source_authority_policy, created_at, updated_at, lens_promotion_mapping) FROM stdin;
\.


--
-- Data for Name: lens_overlay_log; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.lens_overlay_log (overlay_id, request_id, delta_id, lens_definition_id, target_generation_id, source_authority, source_record_id, expected_source_revision, field_path, old_value, new_value, source_delta, status, promoted_admission_id, metadata, created_at, superseded_at, promoted_at) FROM stdin;
\.


--
-- Data for Name: lens_putback_deltas; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.lens_putback_deltas (delta_id, request_id, delta_index, slot_name, scope_key, row_key, field_path, expected_value, new_value, source_authority, source_record_id, expected_source_revision, source_delta, validation_status, validation_error, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: lens_putback_requests; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.lens_putback_requests (request_id, lens_definition_id, deployment_name, lens_id, base_view_definition_id, base_view_manifest_id, target_generation_id, actor_id, idempotency_key, status, commit_mode, delta_count, conflict_count, error, metadata, created_at, prepared_at, committed_at, rejected_at) FROM stdin;
\.


--
-- Data for Name: link_cache; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.link_cache (node_id, text_hash, extractor_fingerprint, linked_at) FROM stdin;
\.


--
-- Data for Name: link_retry; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.link_retry (deployment_name, branch_name, node_id, entity_type, entity_value, recorded_at, ready_at) FROM stdin;
\.


--
-- Data for Name: llm_calls; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.llm_calls (id, ts, deployment, caller, model, fragment_id, prompt_tokens, completion_tokens, total_tokens, cost_usd, latency_ms, success, error_kind, generation_id, extras, outcome_key_digest) FROM stdin;
\.


--
-- Data for Name: mcp_call_log; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.mcp_call_log (call_id, ts, session_id, deployment_name, tool_name, operator_name, replacement_operator, args, cost, pin, elapsed_ms, result_count, error_code) FROM stdin;
\.


--
-- Data for Name: mcp_trace_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.mcp_trace_events (event_id, session_id, deployment_name, generation_id, tool_name, question_label, request_json, response_json, error_text, duration_ms, event_time, branch_name, route_id, route_manifest_checksum) FROM stdin;
\.


--
-- Data for Name: model_outcomes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.model_outcomes (deployment, key_digest, step_id, operator_version, model_id, prompt_hash, input_hash, content, recorded_at, generation_id) FROM stdin;
\.


--
-- Data for Name: narrowings; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.narrowings (parent_subtype, edge_type, child_subtype, attr_rules, catalog_version_id) FROM stdin;
\.


--
-- Data for Name: node_facts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.node_facts (fact_id, node_id, subtype, attrs, source, source_record_id, source_revision, observed_at, dedupe_key, run_id, op, source_program_checksum) FROM stdin;
\.


--
-- Data for Name: node_series; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.node_series (sample_id, deployment_name, node_id, metric, observed_at, value, source, status, attrs) FROM stdin;
\.


--
-- Data for Name: node_series_watermark; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.node_series_watermark (deployment_name, node_id, metric, first_observed_at, updated_at) FROM stdin;
\.


--
-- Data for Name: nomos_config_activations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_config_activations (activation_row_id, deployment_name, activation_id, scope, config_fingerprint, status, activated_by, activation_decision_id, metadata, activated_at, superseded_at) FROM stdin;
\.


--
-- Data for Name: nomos_config_sets; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_config_sets (config_row_id, deployment_name, config_set_id, config_version, config_fingerprint, compiler_version, source_generation_id, scope, entries, config_set, status, effective_from, effective_until, compiled_at, metadata, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: nomos_decisions; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_decisions (decision_row_id, deployment_name, decision_id, policy_bundle_id, policy_version, policy_fingerprint, input_hash, subject, action, resource, classification, context, effect, reason_code, matched_rule_ids, obligations, enforcement_point, target, created_at) FROM stdin;
\.


--
-- Data for Name: nomos_external_claims; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_external_claims (claim_row_id, deployment_name, provider_id, provider_type, issuer, external_subject, audiences, groups, roles, entitlements, assurance, assurance_values, mfa_satisfied, realm, service_principal, delegation_chain, expires_at, not_before, auth_time, claim_fingerprint, verification_status, verification_error, evidence, nomos_decision_id, observed_at) FROM stdin;
\.


--
-- Data for Name: nomos_identity_providers; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_identity_providers (provider_row_id, deployment_name, provider_id, provider_type, issuer, audiences, subject_claim, group_claims, role_claims, entitlement_claims, assurance_claim, key_reference, secret_references, claim_allowlist, required_assurance, mfa_required, group_claims_approved, metadata_fingerprint, realm, service_principal, freshness_seconds, status, metadata, effective_from, effective_until, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: nomos_permission_grants; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_permission_grants (grant_row_id, deployment_name, grant_id, subject, actions, scope, effect, resource_match, classifications, priority, governing_rule_ids, status, metadata, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: nomos_policy_activations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_policy_activations (activation_row_id, deployment_name, activation_id, scope, policy_fingerprint, status, activated_by, activation_decision_id, metadata, activated_at, superseded_at) FROM stdin;
\.


--
-- Data for Name: nomos_policy_bundles; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_policy_bundles (bundle_row_id, deployment_name, policy_bundle_id, policy_version, policy_fingerprint, compiler_version, source_generation_id, scope, default_effect, reason_code_catalog, bundle, status, effective_from, effective_until, compiled_at, metadata, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: nomos_subject_bindings; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.nomos_subject_bindings (binding_row_id, deployment_name, binding_id, provider_id, issuer, claim_path, claim_value, subject, scope, priority, required_assurance, status, effective_from, effective_until, metadata, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: observation_current; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.observation_current (observation_id, check_id, candidate_id, latest_generation_id, latest_run_id, latest_event_id, finding_status, evaluation_status, input_scope, predicate_key, normalized_predicate, normalized_values, participant_roles, participant_anchors, severity, confidence, confidence_type, evidence, payload, updated_at) FROM stdin;
\.


--
-- Data for Name: observation_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.observation_events (event_id, observation_id, check_id, candidate_id, target_generation_id, run_id, event_seq, finding_event, evaluation_event, input_scope, predicate_key, normalized_predicate, normalized_values, participant_roles, participant_anchors, severity, confidence, confidence_type, evidence, payload, coverage_complete, created_at) FROM stdin;
\.


--
-- Data for Name: ontology_rollup; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.ontology_rollup (src_subtype, edge_type, dst_subtype, producer, edge_count) FROM stdin;
\.


--
-- Data for Name: pagerank_scores; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.pagerank_scores (scope, node_id, pagerank, refreshed_at) FROM stdin;
\.


--
-- Data for Name: poros_delivery_attempts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.poros_delivery_attempts (attempt_id, message_id, sender_authority_id, recipient_authority_id, body_sha256, envelope_sha256, transport, status, reason_code, receipt_sha256, response, attempted_at, completed_at) FROM stdin;
\.


--
-- Data for Name: poros_detached_object_manifests; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.poros_detached_object_manifests (manifest_id, message_id, body_sha256, authority_id, side, object_id, media_type, size_bytes, object_sha256, chunk_count, classification, encrypted, manifest, created_at) FROM stdin;
\.


--
-- Data for Name: poros_inbox_messages; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.poros_inbox_messages (inbox_id, receiver_authority_id, sender_authority_id, message_id, status, reason_code, body_sha256, signing_key_id, envelope_sha256, receipt_sha256, envelope, receipt, admission_candidate, trusted, signature_valid, recipient_valid, body_hash_valid, fresh, received_at, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: poros_incorporation_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.poros_incorporation_events (event_id, receiver_authority_id, sender_authority_id, message_id, body_sha256, event_type, status, reason_code, nomos_decision_id, nomos_decision_deployment_name, policy_bundle_id, policy_fingerprint, nomos_input_hash, admission_id, receiver_generation_id, payload, created_at) FROM stdin;
\.


--
-- Data for Name: poros_outbox_messages; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.poros_outbox_messages (message_id, sender_authority_id, recipient_authority_id, sender_generation_id, graph_id, signing_key_id, body_sha256, envelope_sha256, payload_mode, classification, graph_profile, root_node_ids, node_count, edge_count, envelope, body, status, max_delivery_attempts, next_retry_at, last_error, terminal_failure_reason, created_at, expires_at, updated_at) FROM stdin;
\.


--
-- Data for Name: poros_replay_cache; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.poros_replay_cache (receiver_authority_id, sender_authority_id, message_id, body_sha256, status, reason_code, receipt_sha256, expires_at, first_seen_at, last_seen_at, duplicate_count) FROM stdin;
\.


--
-- Data for Name: post_publish_projection_status; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.post_publish_projection_status (deployment_name, generation_id, last_run_at, status, error) FROM stdin;
\.


--
-- Data for Name: producer_profiles; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.producer_profiles (profile_id, deployment_name, producer_id, producer_kind, trust_label, admission_mode, authority_scope, allowed_record_kinds, output_signature, policy_clauses, output_scope_summary, source_ref, catalog_version_id, profile_fingerprint, status, catalog_evidence, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: projection_pending; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.projection_pending (pending_id, generation_id, source, new_cursor, batch_index, created_at) FROM stdin;
\.


--
-- Data for Name: projection_remediation_batches; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.projection_remediation_batches (idempotency_key, run_id, input_cursor, requested_limit, receipt, created_at) FROM stdin;
\.


--
-- Data for Name: projection_remediation_runs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.projection_remediation_runs (run_id, remediation_kind, deployment_name, branch_name, pinned_checkpoint, catalog_version_id, collision_snapshot, subtype, field, entity_type, status, created_at, completed_at) FROM stdin;
\.


--
-- Data for Name: repair_selections; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.repair_selections (deployment, generation_id, bucket_key, outcome, winner_claim_id, generator_id, generator_version, basis, distrusted_claim_ids, asserted_claim_ids, unresolved_reason, carried_from_generation, selection_digest, recorded_at) FROM stdin;
\.


--
-- Data for Name: reuse_certificates; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.reuse_certificates (deployment, generation_id, step_id, certificate_digest, comparison, dependencies, recorded_at) FROM stdin;
\.


--
-- Data for Name: schema_migration_phases; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.schema_migration_phases (migration_id, phase, state, batches_committed, watermark, started_at, updated_at) FROM stdin;
\.


--
-- Data for Name: schema_migrations; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.schema_migrations (migration_id, checksum, description, destructive, applied_at, applied_by, tool_version, status, result_metadata) FROM stdin;
0003_deployment_promotion_fence	2189be32b689869225a5b75b23ae8f99cf733b79d5ec250f518edfb3817fcd86	ordered migration 0003_deployment_promotion_fence	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0004_effective_view_raw_attrs_nullable	5f8a20155caccb9ab729f2b47050b51d38b59bce03ec1c0290c4fd0fdc1f450d	ordered migration 0004_effective_view_raw_attrs_nullable	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0005_chronos_hot_index_manifest	b14694f70cf1653ec3b63f9a9e72eb382ee976bc61b065d7fffa8c7e001e84f3	ordered migration 0005_chronos_hot_index_manifest	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0006_chronos_history_compaction	5327c5f4d060cb995ff4de87b37b7272b6ab48701f4ad0cb68fac2ccca198bbf	ordered migration 0006_chronos_history_compaction	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0007_chronos_history_read_coordination	218c26e043f32c181047eb8452506333524741bdd1e76732f31afe531b65c423	ordered migration 0007_chronos_history_read_coordination	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0008_history_read_lease_role_api	a6ba9ad926fe4e53e2ea9253c3d8818fb314788367a2c1b5099f329b997d052c	ordered migration 0008_history_read_lease_role_api	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0009_compacted_generation_read_api	bdf7190e2824ea9b217709dd5285f9b90e45a5b0d24e942cd9f9f5a37b5a8a9b	ordered migration 0009_compacted_generation_read_api	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0010_compacted_generation_safe_numeric	2c85effe7d9852422bcf0934ea4e5e7a32dba7f02de6696e0f1dea4dcd09671e	ordered migration 0010_compacted_generation_safe_numeric	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0011_compacted_generation_deployment_scope	f4fdfae1e43a7c83f02cb6f9f836f8a7812b294ebf6f7307d2222b3aaaae67e4	ordered migration 0011_compacted_generation_deployment_scope	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0012_history_read_lease_scope	45024de03ba4fd5660bad2e262789db119cc359094fc214e3ce8d226ce34ed74	ordered migration 0012_history_read_lease_scope	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0013_attribute_integrity_receipts	bce81a578940dafa440c305dd7393f6531d4e0afcb6b0673e16b9a9172fc4f80	ordered migration 0013_attribute_integrity_receipts	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0014_attribute_integrity_transition_token	930c12cd8072b672525ad68706a5fc341d7e53884bd5df66b3e37ceb140086e6	ordered migration 0014_attribute_integrity_transition_token	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0015_attribute_integrity_authority_hardening	8c473680bd28f1bfb00fd51b11b9eaa0046b2383e598463ad7d2e3fa9b8acfb1	ordered migration 0015_attribute_integrity_authority_hardening	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0016_effective_attribute_recovery_attempt	b20a23bbffac69374d15b208055b4d45b85b29ca4437047cd97453d14e8f1b8e	ordered migration 0016_effective_attribute_recovery_attempt	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0017_effective_attribute_recovery_consumption	26aa2fefe4b3af82d01aa8d5fda9e0391f956e74e75e07d53fa745c96987af5a	ordered migration 0017_effective_attribute_recovery_consumption	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0018_promotion_fence_release_receipt	c4e39374f546a8bdf1470a6623d73e1d67a5f38b17fdc3ffb05ced3ae0c9fcf4	ordered migration 0018_promotion_fence_release_receipt	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "ordered_runner"}
0000_schema_snapshot	4127e8902773cb6c359b232efe968388698ce5bc26040032f031441984980183	current schema.sql and views.sql snapshot	f	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"statements_executed": 710, "executable_schema_checksum": "36862789152e04a9fa6614567e501617e6d558f3fddc7ccc7e6dabc8373365f3"}
0002_remove_job_runtime_subsystem	d9eb400b8ca520f4eedd878ddb14e3e208a2cb33ef562a5cc441a8b92e032b40	ordered migration 0002_remove_job_runtime_subsystem	t	2026-08-04 04:48:08.096883+00	postgres	\N	applied	{"applied_via": "schema_baseline", "postcondition": "job_runtime_subsystem_absent"}
\.


--
-- Data for Name: source_cursor_blobs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.source_cursor_blobs (source, cursor_key, sha256, value, value_text, size_bytes, updated_at) FROM stdin;
\.


--
-- Data for Name: source_progress; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.source_progress (deployment, source_name, key, value, updated_at) FROM stdin;
\.


--
-- Data for Name: source_record_set_blobs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.source_record_set_blobs (source, sha256, record_set, record_set_text, record_set_summary, chronos_proof, updated_at) FROM stdin;
\.


--
-- Data for Name: source_record_sets; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.source_record_sets (source, record_set, record_set_summary, chronos_proof, sha256, updated_at) FROM stdin;
\.


--
-- Data for Name: source_watermarks; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.source_watermarks (source, cutover_watermark, last_applied_rev, last_published_rev, projection_cursor, last_reconcile_at, replay_required, replay_required_at) FROM stdin;
\.


--
-- Data for Name: subtype_ancestors; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.subtype_ancestors (catalog_version_id, subtype_name, ancestor_name, depth) FROM stdin;
\.


--
-- Data for Name: subtypes; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.subtypes (name, metadata, parent_subtype, catalog_version_id) FROM stdin;
\.


--
-- Data for Name: telemetry_artifacts; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_artifacts (artifact_id, deployment_name, trace_id, span_id, artifact_kind, path, format, collector, collector_status, unavailable_reason, summary, created_at) FROM stdin;
\.


--
-- Data for Name: telemetry_events; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_events (event_id, event_time, trace_id, span_id, event_name, severity, deployment_name, attributes, unavailable_reason) FROM stdin;
\.


--
-- Data for Name: telemetry_graph_effect_manifests; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_graph_effect_manifests (deployment_name, trace_id, span_id, branch_name, producer_key, input_generation_id, output_generation_id, coverage, unavailable_reason, total_count, retained_count, retention_limit, counts, recorded_at) FROM stdin;
\.


--
-- Data for Name: telemetry_graph_effects; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_graph_effects (effect_id, deployment_name, trace_id, span_id, sequence, object_kind, object_id, change_kind, object_type, source_id, target_id, recorded_at) FROM stdin;
\.


--
-- Data for Name: telemetry_metrics; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_metrics (metric_id, metric_time, metric_name, metric_kind, value_double, value_int, unit, deployment_name, branch_name, trace_id, span_id, labels) FROM stdin;
\.


--
-- Data for Name: telemetry_spans; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_spans (event_time, trace_id, span_id, parent_span_id, span_name, span_kind, status_code, status_message, started_at, ended_at, duration_ms, deployment_name, branch_name, generation_id, correlation_id, source_name, workflow_id, run_id, tool_name, route_id, session_id, attributes, payload_summary, unavailable_reason) FROM stdin;
\.


--
-- Data for Name: telemetry_traces; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.telemetry_traces (trace_id, deployment_name, branch_name, root_name, root_kind, status_code, status_message, started_at, ended_at, duration_ms, generation_id, correlation_id, source_name, workflow_id, session_id, attributes, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: verifier_runs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.verifier_runs (run_id, deployment, candidate_id, model, prompt_version, verifier_version, input_tokens, output_tokens, cost_usd, verdict, confidence, emitted_at) FROM stdin;
\.


--
-- Data for Name: visual_caption_failures; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.visual_caption_failures (visual_id, vision_signature, fail_count, last_error, updated_at) FROM stdin;
\.


--
-- Data for Name: visual_crop_blobs; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.visual_crop_blobs (sha256, bytes, size_bytes, mime_type, updated_at) FROM stdin;
\.


--
-- Data for Name: work_ledger; Type: TABLE DATA; Schema: okg; Owner: -
--

COPY okg.work_ledger (deployment, generation_id, mode, category, validation, measured, work_units, model_calls, prompt_tokens, completion_tokens, total_tokens, cost_usd, cause_counts, detail, recorded_at) FROM stdin;
\.


--
-- Data for Name: _chronos_b_interval_okg_console_count_rollup_257f8bc5b1; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1 (dimension_kind, dimension_value, item_count, live_lo, live_hi, writer_segment_id, deleted) FROM stdin;
\.


--
-- Data for Name: _chronos_b_interval_okg_graph_edges_d84ac83c2b; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_b_interval_okg_graph_edges_d84ac83c2b (edge_id, src, dst, edge_type, attrs, provenance, confidence, source, source_record_id, source_revision, run_id, observed_at, live_lo, live_hi, writer_segment_id, deleted) FROM stdin;
\.


--
-- Data for Name: _chronos_b_interval_okg_graph_nodes_1c8e1fc23f; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f (node_id, subtype, attrs, text_hash, embedding, embedding_model, source, source_record_id, source_revision, run_id, observed_at, live_lo, live_hi, writer_segment_id, deleted) FROM stdin;
\.


--
-- Data for Name: _chronos_b_interval_okg_ontology_rollup_b039672993; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_b_interval_okg_ontology_rollup_b039672993 (src_subtype, edge_type, dst_subtype, producer, edge_count, live_lo, live_hi, writer_segment_id, deleted) FROM stdin;
\.


--
-- Data for Name: _chronos_branch_indexes; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_branch_indexes (backend, index_name, table_name, columns) FROM stdin;
\.


--
-- Data for Name: _chronos_branch_interval_branches; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_branch_interval_branches (branch_id, current_segment_id, parent_branch_id, child_count, branch_kind, created_at, metadata) FROM stdin;
main	2	\N	1	mutable	2026-08-04T04:48:08.687740+00:00	{}
default	3	main	0	mutable	2026-08-04T04:48:08.937584+00:00	{}
\.


--
-- Data for Name: _chronos_branch_interval_checkpoints; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_branch_interval_checkpoints (checkpoint_id, branch_id, segment_id, created_at, metadata) FROM stdin;
\.


--
-- Data for Name: _chronos_branch_interval_segments; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_branch_interval_segments (segment_id, parent_segment_id, owner_branch_id, segment_kind, live_lo, live_hi, branch_point, created_at, metadata) FROM stdin;
1	\N	main	mutable	0	10000000000000000000000000000000	5000000000000000000000000000000	2026-08-04T04:48:08.686498+00:00	{}
4	1	\N	fork_base	0	1	0	2026-08-04T04:48:08.937584+00:00	{}
2	4	main	mutable	3162277660168380	10000000000000000000000000000000	5000000000000001581138830084190	2026-08-04T04:48:08.937584+00:00	{}
3	4	default	mutable	1	3162277660168380	1581138830084190	2026-08-04T04:48:08.937584+00:00	{}
\.


--
-- Data for Name: _chronos_branch_tables; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public._chronos_branch_tables (table_name, physical_table, pk_columns, columns, column_defs, backend) FROM stdin;
okg.graph_nodes	_chronos_b_interval_okg_graph_nodes_1c8e1fc23f	["node_id"]	["node_id", "subtype", "attrs", "text_hash", "embedding", "embedding_model", "source", "source_record_id", "source_revision", "run_id", "observed_at"]	["\\"node_id\\" TEXT", "\\"subtype\\" TEXT", "\\"attrs\\" JSONB", "\\"text_hash\\" TEXT", "\\"embedding\\" vector", "\\"embedding_model\\" TEXT", "\\"source\\" TEXT", "\\"source_record_id\\" JSONB", "\\"source_revision\\" JSONB", "\\"run_id\\" TEXT", "\\"observed_at\\" TIMESTAMPTZ"]	interval
okg.graph_edges	_chronos_b_interval_okg_graph_edges_d84ac83c2b	["edge_id"]	["edge_id", "src", "dst", "edge_type", "attrs", "provenance", "confidence", "source", "source_record_id", "source_revision", "run_id", "observed_at"]	["\\"edge_id\\" BIGINT", "\\"src\\" TEXT", "\\"dst\\" TEXT", "\\"edge_type\\" TEXT", "\\"attrs\\" JSONB", "\\"provenance\\" TEXT", "\\"confidence\\" REAL", "\\"source\\" TEXT", "\\"source_record_id\\" JSONB", "\\"source_revision\\" JSONB", "\\"run_id\\" TEXT", "\\"observed_at\\" TIMESTAMPTZ"]	interval
okg.ontology_rollup	_chronos_b_interval_okg_ontology_rollup_b039672993	["src_subtype", "edge_type", "dst_subtype", "producer"]	["src_subtype", "edge_type", "dst_subtype", "producer", "edge_count"]	["\\"src_subtype\\" TEXT", "\\"edge_type\\" TEXT", "\\"dst_subtype\\" TEXT", "\\"producer\\" TEXT", "\\"edge_count\\" BIGINT"]	interval
okg.console_count_rollup	_chronos_b_interval_okg_console_count_rollup_257f8bc5b1	["dimension_kind", "dimension_value"]	["dimension_kind", "dimension_value", "item_count"]	["\\"dimension_kind\\" TEXT", "\\"dimension_value\\" TEXT", "\\"item_count\\" BIGINT"]	interval
\.


--
-- Name: bgw_job_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: -
--

SELECT pg_catalog.setval('_timescaledb_catalog.bgw_job_id_seq', 1015, true);


--
-- Name: chunk_column_stats_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: -
--

SELECT pg_catalog.setval('_timescaledb_catalog.chunk_column_stats_id_seq', 1, false);


--
-- Name: chunk_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: -
--

SELECT pg_catalog.setval('_timescaledb_catalog.chunk_id_seq', 1, false);


--
-- Name: dimension_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: -
--

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_id_seq', 8, true);


--
-- Name: dimension_slice_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: -
--

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_slice_id_seq', 1, false);


--
-- Name: hypertable_id_seq; Type: SEQUENCE SET; Schema: _timescaledb_catalog; Owner: -
--

SELECT pg_catalog.setval('_timescaledb_catalog.hypertable_id_seq', 16, true);


--
-- Name: admission_events_event_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.admission_events_event_id_seq', 1, false);


--
-- Name: admission_record_refs_ref_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.admission_record_refs_ref_id_seq', 1, false);


--
-- Name: branch_merge_requests_request_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.branch_merge_requests_request_id_seq', 1, false);


--
-- Name: branches_branch_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.branches_branch_id_seq', 2, true);


--
-- Name: catalog_versions_catalog_version_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.catalog_versions_catalog_version_id_seq', 1, true);


--
-- Name: derived_dirty_scopes_dirty_scope_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.derived_dirty_scopes_dirty_scope_id_seq', 1, false);


--
-- Name: derived_output_dependency_index_dependency_index_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.derived_output_dependency_index_dependency_index_id_seq', 1, false);


--
-- Name: edge_facts_edge_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.edge_facts_edge_id_seq', 1, false);


--
-- Name: edge_facts_fact_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.edge_facts_fact_id_seq', 1, false);


--
-- Name: entity_resolution_rejections_rejection_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.entity_resolution_rejections_rejection_id_seq', 1, false);


--
-- Name: envelope_violations_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.envelope_violations_id_seq', 1, false);


--
-- Name: federation_mailbox_events_event_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.federation_mailbox_events_event_id_seq', 1, false);


--
-- Name: graph_generations_generation_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.graph_generations_generation_id_seq', 1, true);


--
-- Name: lease_audit_audit_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.lease_audit_audit_id_seq', 1, false);


--
-- Name: llm_calls_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.llm_calls_id_seq', 1, false);


--
-- Name: mcp_call_log_call_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.mcp_call_log_call_id_seq', 1, false);


--
-- Name: mcp_trace_events_event_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.mcp_trace_events_event_id_seq', 1, false);


--
-- Name: node_facts_fact_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.node_facts_fact_id_seq', 1, false);


--
-- Name: node_series_sample_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.node_series_sample_id_seq', 1, false);


--
-- Name: nomos_config_activations_activation_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_config_activations_activation_row_id_seq', 1, false);


--
-- Name: nomos_config_sets_config_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_config_sets_config_row_id_seq', 1, false);


--
-- Name: nomos_decisions_decision_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_decisions_decision_row_id_seq', 1, false);


--
-- Name: nomos_external_claims_claim_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_external_claims_claim_row_id_seq', 1, false);


--
-- Name: nomos_identity_providers_provider_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_identity_providers_provider_row_id_seq', 1, false);


--
-- Name: nomos_permission_grants_grant_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_permission_grants_grant_row_id_seq', 1, false);


--
-- Name: nomos_policy_activations_activation_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_policy_activations_activation_row_id_seq', 1, false);


--
-- Name: nomos_policy_bundles_bundle_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_policy_bundles_bundle_row_id_seq', 1, false);


--
-- Name: nomos_subject_bindings_binding_row_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.nomos_subject_bindings_binding_row_id_seq', 1, false);


--
-- Name: observation_events_event_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.observation_events_event_id_seq', 1, false);


--
-- Name: poros_delivery_attempts_attempt_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.poros_delivery_attempts_attempt_id_seq', 1, false);


--
-- Name: poros_detached_object_manifests_manifest_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.poros_detached_object_manifests_manifest_id_seq', 1, false);


--
-- Name: poros_inbox_messages_inbox_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.poros_inbox_messages_inbox_id_seq', 1, false);


--
-- Name: poros_incorporation_events_event_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.poros_incorporation_events_event_id_seq', 1, false);


--
-- Name: producer_profiles_profile_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.producer_profiles_profile_id_seq', 1, false);


--
-- Name: projection_pending_pending_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.projection_pending_pending_id_seq', 1, false);


--
-- Name: telemetry_events_event_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.telemetry_events_event_id_seq', 1, false);


--
-- Name: telemetry_graph_effects_effect_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.telemetry_graph_effects_effect_id_seq', 1, false);


--
-- Name: telemetry_metrics_metric_id_seq; Type: SEQUENCE SET; Schema: okg; Owner: -
--

SELECT pg_catalog.setval('okg.telemetry_metrics_metric_id_seq', 1, false);


--
-- Name: _chronos_branch_interval_segment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public._chronos_branch_interval_segment_id_seq', 4, true);


--
-- Name: admission_envelopes admission_envelopes_deployment_name_producer_id_idempotency_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_envelopes
    ADD CONSTRAINT admission_envelopes_deployment_name_producer_id_idempotency_key UNIQUE (deployment_name, producer_id, idempotency_key);


--
-- Name: admission_envelopes admission_envelopes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_envelopes
    ADD CONSTRAINT admission_envelopes_pkey PRIMARY KEY (admission_id);


--
-- Name: admission_envelopes admission_envelopes_target_generation_ref_nonempty_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.admission_envelopes
    ADD CONSTRAINT admission_envelopes_target_generation_ref_nonempty_chk CHECK (((target_generation_ref IS NULL) OR (length(btrim(target_generation_ref)) > 0))) NOT VALID;


--
-- Name: admission_events admission_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_events
    ADD CONSTRAINT admission_events_pkey PRIMARY KEY (event_id);


--
-- Name: admission_record_refs admission_record_refs_admission_id_ref_key_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_record_refs
    ADD CONSTRAINT admission_record_refs_admission_id_ref_key_key UNIQUE (admission_id, ref_key);


--
-- Name: admission_record_refs admission_record_refs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_record_refs
    ADD CONSTRAINT admission_record_refs_pkey PRIMARY KEY (ref_id);


--
-- Name: alias_collisions alias_collisions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.alias_collisions
    ADD CONSTRAINT alias_collisions_pkey PRIMARY KEY (catalog_version_id, subtype, field, alias_value);


--
-- Name: aliases aliases_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.aliases
    ADD CONSTRAINT aliases_pkey PRIMARY KEY (alias, catalog_version_id);


--
-- Name: attribute_effective_attrs attribute_effective_attrs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_attrs
    ADD CONSTRAINT attribute_effective_attrs_pkey PRIMARY KEY (target_kind, target_id, generation_id, namespace, attr_key);


--
-- Name: attribute_effective_integrity_receipts attribute_effective_integrity_receipts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_integrity_receipts
    ADD CONSTRAINT attribute_effective_integrity_receipts_pkey PRIMARY KEY (receipt_digest);


--
-- Name: attribute_effective_integrity_state attribute_effective_integrity_state_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_integrity_state
    ADD CONSTRAINT attribute_effective_integrity_state_pkey PRIMARY KEY (deployment_name, branch_id, namespace);


--
-- Name: attribute_effective_integrity_transitions attribute_effective_integrity_transitions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_integrity_transitions
    ADD CONSTRAINT attribute_effective_integrity_transitions_pkey PRIMARY KEY (deployment_name, branch_id, namespace);


--
-- Name: attribute_effective_view_intervals attribute_effective_view_intervals_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_view_intervals
    ADD CONSTRAINT attribute_effective_view_intervals_pkey PRIMARY KEY (target_kind, target_id, namespace, branch_id, from_ord);


--
-- Name: attribute_effective_view attribute_effective_view_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_view
    ADD CONSTRAINT attribute_effective_view_pkey PRIMARY KEY (target_kind, target_id, generation_id, namespace);


--
-- Name: attribute_observations attribute_observations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_observations
    ADD CONSTRAINT attribute_observations_pkey PRIMARY KEY (observation_id);


--
-- Name: branch_merge_requests branch_merge_requests_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branch_merge_requests
    ADD CONSTRAINT branch_merge_requests_pkey PRIMARY KEY (request_id);


--
-- Name: branches branches_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branches
    ADD CONSTRAINT branches_pkey PRIMARY KEY (branch_id);


--
-- Name: catalog_versions catalog_versions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.catalog_versions
    ADD CONSTRAINT catalog_versions_pkey PRIMARY KEY (catalog_version_id);


--
-- Name: chronos_hot_index_manifest chronos_hot_index_manifest_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.chronos_hot_index_manifest
    ADD CONSTRAINT chronos_hot_index_manifest_pkey PRIMARY KEY (physical_table, suffix, hot_index_name);


--
-- Name: compacted_generations compacted_generations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.compacted_generations
    ADD CONSTRAINT compacted_generations_pkey PRIMARY KEY (checkpoint_id);


--
-- Name: consistency_candidate_coverage consistency_candidate_coverage_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_candidate_coverage
    ADD CONSTRAINT consistency_candidate_coverage_pkey PRIMARY KEY (run_id);


--
-- Name: consistency_candidates consistency_candidates_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_candidates
    ADD CONSTRAINT consistency_candidates_pkey PRIMARY KEY (check_id, target_generation_id, candidate_id);


--
-- Name: consistency_check_definitions consistency_check_definitions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_check_definitions
    ADD CONSTRAINT consistency_check_definitions_pkey PRIMARY KEY (check_id);


--
-- Name: consistency_gate_results consistency_gate_results_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_gate_results
    ADD CONSTRAINT consistency_gate_results_pkey PRIMARY KEY (result_id);


--
-- Name: consistency_gate_results consistency_gate_results_run_id_gate_name_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_gate_results
    ADD CONSTRAINT consistency_gate_results_run_id_gate_name_key UNIQUE (run_id, gate_name);


--
-- Name: console_count_rollup console_count_rollup_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.console_count_rollup
    ADD CONSTRAINT console_count_rollup_pkey PRIMARY KEY (dimension_kind, dimension_value);


--
-- Name: coordination_native_events coordination_native_events_deployment_name_actor_id_change__key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.coordination_native_events
    ADD CONSTRAINT coordination_native_events_deployment_name_actor_id_change__key UNIQUE (deployment_name, actor_id, change_id, task_id, idempotency_key);


--
-- Name: coordination_native_events coordination_native_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.coordination_native_events
    ADD CONSTRAINT coordination_native_events_pkey PRIMARY KEY (event_id);


--
-- Name: coordination_native_state coordination_native_state_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.coordination_native_state
    ADD CONSTRAINT coordination_native_state_pkey PRIMARY KEY (deployment_name, change_id, task_id);


--
-- Name: database_owner database_owner_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.database_owner
    ADD CONSTRAINT database_owner_pkey PRIMARY KEY (singleton);


--
-- Name: deployment_apply_runs deployment_apply_runs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.deployment_apply_runs
    ADD CONSTRAINT deployment_apply_runs_pkey PRIMARY KEY (deployment_name, desired_state_digest);


--
-- Name: deployment_promotion_fences deployment_promotion_fences_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.deployment_promotion_fences
    ADD CONSTRAINT deployment_promotion_fences_pkey PRIMARY KEY (deployment_name);


--
-- Name: deployment_worker_ack deployment_worker_ack_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.deployment_worker_ack
    ADD CONSTRAINT deployment_worker_ack_pkey PRIMARY KEY (deployment_name);


--
-- Name: deployment_write_ownership deployment_write_ownership_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.deployment_write_ownership
    ADD CONSTRAINT deployment_write_ownership_pkey PRIMARY KEY (deployment_name);


--
-- Name: derived_artifact_definitions derived_artifact_definitions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_definitions
    ADD CONSTRAINT derived_artifact_definitions_pkey PRIMARY KEY (definition_id);


--
-- Name: derived_artifact_dependencies derived_artifact_dependencies_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_dependencies
    ADD CONSTRAINT derived_artifact_dependencies_pkey PRIMARY KEY (dependency_id);


--
-- Name: derived_artifact_runs derived_artifact_runs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_runs
    ADD CONSTRAINT derived_artifact_runs_pkey PRIMARY KEY (run_id);


--
-- Name: derived_dirty_rule_audits derived_dirty_rule_audits_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_rule_audits
    ADD CONSTRAINT derived_dirty_rule_audits_pkey PRIMARY KEY (audit_id);


--
-- Name: derived_dirty_rule_runs derived_dirty_rule_runs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_rule_runs
    ADD CONSTRAINT derived_dirty_rule_runs_pkey PRIMARY KEY (dirty_rule_run_id);


--
-- Name: derived_dirty_scopes derived_dirty_scopes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_scopes
    ADD CONSTRAINT derived_dirty_scopes_pkey PRIMARY KEY (dirty_scope_id);


--
-- Name: derived_output_dependency_index derived_output_dependency_ind_lineage_envelope_id_dependenc_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_dependency_index
    ADD CONSTRAINT derived_output_dependency_ind_lineage_envelope_id_dependenc_key UNIQUE (lineage_envelope_id, dependency_kind, dependency_ref);


--
-- Name: derived_output_dependency_index derived_output_dependency_index_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_dependency_index
    ADD CONSTRAINT derived_output_dependency_index_pkey PRIMARY KEY (dependency_index_id);


--
-- Name: derived_output_lineage_envelopes derived_output_lineage_envelopes_manifest_id_output_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_lineage_envelopes
    ADD CONSTRAINT derived_output_lineage_envelopes_manifest_id_output_id_key UNIQUE (manifest_id, output_id);


--
-- Name: derived_output_lineage_envelopes derived_output_lineage_envelopes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_lineage_envelopes
    ADD CONSTRAINT derived_output_lineage_envelopes_pkey PRIMARY KEY (lineage_envelope_id);


--
-- Name: derived_output_manifests derived_output_manifests_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_manifests
    ADD CONSTRAINT derived_output_manifests_pkey PRIMARY KEY (manifest_id);


--
-- Name: derived_output_slots derived_output_slots_manifest_id_slot_name_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_slots
    ADD CONSTRAINT derived_output_slots_manifest_id_slot_name_key UNIQUE (manifest_id, slot_name);


--
-- Name: derived_output_slots derived_output_slots_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_slots
    ADD CONSTRAINT derived_output_slots_pkey PRIMARY KEY (slot_id);


--
-- Name: derived_relation_registry derived_relation_registry_definition_id_logical_relation_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_relation_registry
    ADD CONSTRAINT derived_relation_registry_definition_id_logical_relation_id_key UNIQUE (definition_id, logical_relation_id, output_schema_hash);


--
-- Name: derived_relation_registry derived_relation_registry_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_relation_registry
    ADD CONSTRAINT derived_relation_registry_pkey PRIMARY KEY (relation_id);


--
-- Name: derived_relation_registry derived_relation_registry_schema_name_physical_name_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_relation_registry
    ADD CONSTRAINT derived_relation_registry_schema_name_physical_name_key UNIQUE (schema_name, physical_name);


--
-- Name: derived_run_dependency_bindings derived_run_dependency_bindings_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_run_dependency_bindings
    ADD CONSTRAINT derived_run_dependency_bindings_pkey PRIMARY KEY (binding_id);


--
-- Name: derived_schedule_nodes derived_schedule_nodes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_schedule_nodes
    ADD CONSTRAINT derived_schedule_nodes_pkey PRIMARY KEY (plan_id, definition_id);


--
-- Name: derived_schedule_nodes derived_schedule_nodes_plan_id_topo_index_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_schedule_nodes
    ADD CONSTRAINT derived_schedule_nodes_plan_id_topo_index_key UNIQUE (plan_id, topo_index);


--
-- Name: derived_schedule_plans derived_schedule_plans_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_schedule_plans
    ADD CONSTRAINT derived_schedule_plans_pkey PRIMARY KEY (plan_id);


--
-- Name: edge_fact_strandings edge_fact_strandings_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.edge_fact_strandings
    ADD CONSTRAINT edge_fact_strandings_pkey PRIMARY KEY (fact_id);


--
-- Name: edge_facts edge_facts_dedupe_key_observed_at_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.edge_facts
    ADD CONSTRAINT edge_facts_dedupe_key_observed_at_key UNIQUE (dedupe_key, observed_at);


--
-- Name: edge_facts edge_facts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.edge_facts
    ADD CONSTRAINT edge_facts_pkey PRIMARY KEY (fact_id, observed_at);


--
-- Name: effective_attribute_recovery_attempts effective_attribute_recovery_attempts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.effective_attribute_recovery_attempts
    ADD CONSTRAINT effective_attribute_recovery_attempts_pkey PRIMARY KEY (classification_binding_digest);


--
-- Name: enricher_backfill_progress enricher_backfill_progress_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.enricher_backfill_progress
    ADD CONSTRAINT enricher_backfill_progress_pkey PRIMARY KEY (deployment, branch_name, enricher_name, input_node_id, catalog_version_id, profile_fingerprint, transform_checksum);


--
-- Name: enricher_progress enricher_progress_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.enricher_progress
    ADD CONSTRAINT enricher_progress_pkey PRIMARY KEY (deployment, enricher_name);


--
-- Name: entity_identity_claims entity_identity_claims_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_identity_claims
    ADD CONSTRAINT entity_identity_claims_pkey PRIMARY KEY (deployment_name, branch_name, subtype, normalized_key);


--
-- Name: entity_merge_receipts entity_merge_receipts_deployment_name_branch_name_plan_dige_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_merge_receipts
    ADD CONSTRAINT entity_merge_receipts_deployment_name_branch_name_plan_dige_key UNIQUE (deployment_name, branch_name, plan_digest);


--
-- Name: entity_merge_receipts entity_merge_receipts_deployment_name_branch_name_retired_n_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_merge_receipts
    ADD CONSTRAINT entity_merge_receipts_deployment_name_branch_name_retired_n_key UNIQUE (deployment_name, branch_name, retired_node_id);


--
-- Name: entity_merge_receipts entity_merge_receipts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_merge_receipts
    ADD CONSTRAINT entity_merge_receipts_pkey PRIMARY KEY (merge_id);


--
-- Name: entity_resolution_rejections entity_resolution_rejections_deployment_name_branch_name_su_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_resolution_rejections
    ADD CONSTRAINT entity_resolution_rejections_deployment_name_branch_name_su_key UNIQUE (deployment_name, branch_name, subtype, normalized_key, policy_fingerprint, reason);


--
-- Name: entity_resolution_rejections entity_resolution_rejections_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.entity_resolution_rejections
    ADD CONSTRAINT entity_resolution_rejections_pkey PRIMARY KEY (rejection_id);


--
-- Name: envelope_violations envelope_violations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.envelope_violations
    ADD CONSTRAINT envelope_violations_pkey PRIMARY KEY (id);


--
-- Name: equivalence_verdicts equivalence_verdicts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.equivalence_verdicts
    ADD CONSTRAINT equivalence_verdicts_pkey PRIMARY KEY (deployment, production_generation, comparison);


--
-- Name: external_probe_cache external_probe_cache_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.external_probe_cache
    ADD CONSTRAINT external_probe_cache_pkey PRIMARY KEY (deployment_name, node_id, backing_fingerprint, field);


--
-- Name: extractor_retry_queue extractor_retry_queue_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.extractor_retry_queue
    ADD CONSTRAINT extractor_retry_queue_pkey PRIMARY KEY (deployment_name, branch_name, node_id);


--
-- Name: fact_batch_receipts fact_batch_receipts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.fact_batch_receipts
    ADD CONSTRAINT fact_batch_receipts_pkey PRIMARY KEY (admission_id, run_id, chunk_ordinal);


--
-- Name: federation_connection_requests federation_connection_requests_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_connection_requests
    ADD CONSTRAINT federation_connection_requests_pkey PRIMARY KEY (deployment_name, request_id);


--
-- Name: federation_discovered_candidates federation_discovered_candidates_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_discovered_candidates
    ADD CONSTRAINT federation_discovered_candidates_pkey PRIMARY KEY (deployment_name, local_authority_id, remote_authority_id);


--
-- Name: federation_mailbox_events federation_mailbox_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_mailbox_events
    ADD CONSTRAINT federation_mailbox_events_pkey PRIMARY KEY (event_id);


--
-- Name: federation_mailbox_messages federation_mailbox_lease_shape_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.federation_mailbox_messages
    ADD CONSTRAINT federation_mailbox_lease_shape_chk CHECK (((status <> 'leased'::text) OR ((lease_token IS NOT NULL) AND (length(lease_token) > 0) AND (lease_expires_at IS NOT NULL)))) NOT VALID;


--
-- Name: federation_mailbox_messages federation_mailbox_lifecycle_evidence_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.federation_mailbox_messages
    ADD CONSTRAINT federation_mailbox_lifecycle_evidence_chk CHECK ((((status = 'pending'::text) AND (receipt_evidence IS NULL) AND (denial_evidence IS NULL) AND (deny_reason_code IS NULL)) OR ((status = 'leased'::text) AND (lease_token IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (receipt_evidence IS NULL) AND (denial_evidence IS NULL) AND (deny_reason_code IS NULL)) OR ((status = 'acknowledged'::text) AND (receipt_evidence IS NOT NULL) AND (denial_evidence IS NULL) AND (deny_reason_code IS NULL)) OR ((status = 'denied'::text) AND (receipt_evidence IS NULL) AND (denial_evidence IS NOT NULL) AND (NULLIF(btrim(deny_reason_code), ''::text) IS NOT NULL)))) NOT VALID;


--
-- Name: federation_mailbox_messages federation_mailbox_messages_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_mailbox_messages
    ADD CONSTRAINT federation_mailbox_messages_pkey PRIMARY KEY (message_id);


--
-- Name: federation_mailbox_messages federation_mailbox_payload_shape_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.federation_mailbox_messages
    ADD CONSTRAINT federation_mailbox_payload_shape_chk CHECK ((((payload_mode = 'inline'::text) AND (envelope IS NOT NULL) AND (pointer IS NULL) AND (object_uri IS NULL) AND (object_sha256 IS NULL) AND (size_bytes IS NULL)) OR ((payload_mode = 'detached_pointer'::text) AND (envelope IS NULL) AND (pointer IS NOT NULL) AND (NULLIF(btrim(object_uri), ''::text) IS NOT NULL) AND (NULLIF(btrim(object_sha256), ''::text) IS NOT NULL) AND (size_bytes IS NOT NULL) AND (size_bytes > 0)))) NOT VALID;


--
-- Name: federation_peer_trust_pins federation_peer_trust_pins_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_peer_trust_pins
    ADD CONSTRAINT federation_peer_trust_pins_pkey PRIMARY KEY (deployment_name, local_authority_id, remote_authority_id, key_fingerprint);


--
-- Name: federation_route_events federation_route_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_route_events
    ADD CONSTRAINT federation_route_events_pkey PRIMARY KEY (deployment_name, route_event_id);


--
-- Name: generation_ordinals generation_ordinals_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.generation_ordinals
    ADD CONSTRAINT generation_ordinals_pkey PRIMARY KEY (checkpoint_id);


--
-- Name: graph_edges graph_edges_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.graph_edges
    ADD CONSTRAINT graph_edges_pkey PRIMARY KEY (edge_id);


--
-- Name: graph_generations graph_generations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.graph_generations
    ADD CONSTRAINT graph_generations_pkey PRIMARY KEY (generation_id);


--
-- Name: graph_nodes graph_nodes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.graph_nodes
    ADD CONSTRAINT graph_nodes_pkey PRIMARY KEY (node_id);


--
-- Name: history_compaction_plans history_compaction_plans_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.history_compaction_plans
    ADD CONSTRAINT history_compaction_plans_pkey PRIMARY KEY (plan_id);


--
-- Name: history_generation_pins history_generation_pins_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.history_generation_pins
    ADD CONSTRAINT history_generation_pins_pkey PRIMARY KEY (deployment_name, branch_id, pin_name);


--
-- Name: identifier_patterns identifier_patterns_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.identifier_patterns
    ADD CONSTRAINT identifier_patterns_pkey PRIMARY KEY (id_type, regex, catalog_version_id);


--
-- Name: invariants invariants_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.invariants
    ADD CONSTRAINT invariants_pkey PRIMARY KEY (name, catalog_version_id);


--
-- Name: ir_invocations ir_invocations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_invocations
    ADD CONSTRAINT ir_invocations_pkey PRIMARY KEY (invocation_id);


--
-- Name: ir_program_bindings ir_program_bindings_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_program_bindings
    ADD CONSTRAINT ir_program_bindings_pkey PRIMARY KEY (binding_id);


--
-- Name: ir_programs ir_programs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_programs
    ADD CONSTRAINT ir_programs_pkey PRIMARY KEY (program_id);


--
-- Name: ir_programs ir_programs_program_checksum_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_programs
    ADD CONSTRAINT ir_programs_program_checksum_key UNIQUE (program_checksum);


--
-- Name: lease_audit lease_audit_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lease_audit
    ADD CONSTRAINT lease_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: lens_definitions lens_definitions_deployment_name_lens_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_definitions
    ADD CONSTRAINT lens_definitions_deployment_name_lens_id_key UNIQUE (deployment_name, lens_id);


--
-- Name: lens_definitions lens_definitions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_definitions
    ADD CONSTRAINT lens_definitions_pkey PRIMARY KEY (definition_id);


--
-- Name: lens_overlay_log lens_overlay_log_delta_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_delta_id_key UNIQUE (delta_id);


--
-- Name: lens_overlay_log lens_overlay_log_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_pkey PRIMARY KEY (overlay_id);


--
-- Name: lens_putback_deltas lens_putback_deltas_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_deltas
    ADD CONSTRAINT lens_putback_deltas_pkey PRIMARY KEY (delta_id);


--
-- Name: lens_putback_deltas lens_putback_deltas_request_id_delta_index_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_deltas
    ADD CONSTRAINT lens_putback_deltas_request_id_delta_index_key UNIQUE (request_id, delta_index);


--
-- Name: lens_putback_requests lens_putback_requests_lens_definition_id_actor_id_idempoten_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_requests
    ADD CONSTRAINT lens_putback_requests_lens_definition_id_actor_id_idempoten_key UNIQUE (lens_definition_id, actor_id, idempotency_key);


--
-- Name: lens_putback_requests lens_putback_requests_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_requests
    ADD CONSTRAINT lens_putback_requests_pkey PRIMARY KEY (request_id);


--
-- Name: link_cache link_cache_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.link_cache
    ADD CONSTRAINT link_cache_pkey PRIMARY KEY (node_id);


--
-- Name: link_retry link_retry_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.link_retry
    ADD CONSTRAINT link_retry_pkey PRIMARY KEY (deployment_name, branch_name, node_id, entity_type, entity_value);


--
-- Name: llm_calls llm_calls_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.llm_calls
    ADD CONSTRAINT llm_calls_pkey PRIMARY KEY (id);


--
-- Name: mcp_call_log mcp_call_log_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.mcp_call_log
    ADD CONSTRAINT mcp_call_log_pkey PRIMARY KEY (ts, call_id);


--
-- Name: mcp_trace_events mcp_trace_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.mcp_trace_events
    ADD CONSTRAINT mcp_trace_events_pkey PRIMARY KEY (event_time, event_id);


--
-- Name: model_outcomes model_outcomes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.model_outcomes
    ADD CONSTRAINT model_outcomes_pkey PRIMARY KEY (deployment, key_digest);


--
-- Name: narrowings narrowings_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.narrowings
    ADD CONSTRAINT narrowings_pkey PRIMARY KEY (parent_subtype, edge_type, child_subtype, catalog_version_id);


--
-- Name: node_facts node_facts_dedupe_key_observed_at_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.node_facts
    ADD CONSTRAINT node_facts_dedupe_key_observed_at_key UNIQUE (dedupe_key, observed_at);


--
-- Name: node_facts node_facts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.node_facts
    ADD CONSTRAINT node_facts_pkey PRIMARY KEY (fact_id, observed_at);


--
-- Name: node_series node_series_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.node_series
    ADD CONSTRAINT node_series_pkey PRIMARY KEY (observed_at, sample_id);


--
-- Name: node_series_watermark node_series_watermark_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.node_series_watermark
    ADD CONSTRAINT node_series_watermark_pkey PRIMARY KEY (deployment_name, node_id, metric);


--
-- Name: nomos_config_activations nomos_config_activations_deployment_name_activation_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_activations
    ADD CONSTRAINT nomos_config_activations_deployment_name_activation_id_key UNIQUE (deployment_name, activation_id);


--
-- Name: nomos_config_activations nomos_config_activations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_activations
    ADD CONSTRAINT nomos_config_activations_pkey PRIMARY KEY (activation_row_id);


--
-- Name: nomos_config_sets nomos_config_sets_deployment_name_config_fingerprint_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_sets
    ADD CONSTRAINT nomos_config_sets_deployment_name_config_fingerprint_key UNIQUE (deployment_name, config_fingerprint);


--
-- Name: nomos_config_sets nomos_config_sets_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_sets
    ADD CONSTRAINT nomos_config_sets_pkey PRIMARY KEY (config_row_id);


--
-- Name: nomos_decisions nomos_decisions_deployment_name_decision_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_decisions
    ADD CONSTRAINT nomos_decisions_deployment_name_decision_id_key UNIQUE (deployment_name, decision_id);


--
-- Name: nomos_decisions nomos_decisions_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_decisions
    ADD CONSTRAINT nomos_decisions_pkey PRIMARY KEY (decision_row_id);


--
-- Name: nomos_external_claims nomos_external_claims_deployment_name_provider_id_claim_fin_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_external_claims
    ADD CONSTRAINT nomos_external_claims_deployment_name_provider_id_claim_fin_key UNIQUE (deployment_name, provider_id, claim_fingerprint);


--
-- Name: nomos_external_claims nomos_external_claims_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_external_claims
    ADD CONSTRAINT nomos_external_claims_pkey PRIMARY KEY (claim_row_id);


--
-- Name: nomos_identity_providers nomos_identity_providers_deployment_name_provider_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_identity_providers
    ADD CONSTRAINT nomos_identity_providers_deployment_name_provider_id_key UNIQUE (deployment_name, provider_id);


--
-- Name: nomos_identity_providers nomos_identity_providers_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_identity_providers
    ADD CONSTRAINT nomos_identity_providers_pkey PRIMARY KEY (provider_row_id);


--
-- Name: nomos_permission_grants nomos_permission_grants_deployment_name_grant_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_permission_grants
    ADD CONSTRAINT nomos_permission_grants_deployment_name_grant_id_key UNIQUE (deployment_name, grant_id);


--
-- Name: nomos_permission_grants nomos_permission_grants_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_permission_grants
    ADD CONSTRAINT nomos_permission_grants_pkey PRIMARY KEY (grant_row_id);


--
-- Name: nomos_policy_activations nomos_policy_activations_deployment_name_activation_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_activations
    ADD CONSTRAINT nomos_policy_activations_deployment_name_activation_id_key UNIQUE (deployment_name, activation_id);


--
-- Name: nomos_policy_activations nomos_policy_activations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_activations
    ADD CONSTRAINT nomos_policy_activations_pkey PRIMARY KEY (activation_row_id);


--
-- Name: nomos_policy_bundles nomos_policy_bundles_deployment_name_policy_fingerprint_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_bundles
    ADD CONSTRAINT nomos_policy_bundles_deployment_name_policy_fingerprint_key UNIQUE (deployment_name, policy_fingerprint);


--
-- Name: nomos_policy_bundles nomos_policy_bundles_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_bundles
    ADD CONSTRAINT nomos_policy_bundles_pkey PRIMARY KEY (bundle_row_id);


--
-- Name: nomos_subject_bindings nomos_subject_bindings_deployment_name_binding_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_subject_bindings
    ADD CONSTRAINT nomos_subject_bindings_deployment_name_binding_id_key UNIQUE (deployment_name, binding_id);


--
-- Name: nomos_subject_bindings nomos_subject_bindings_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_subject_bindings
    ADD CONSTRAINT nomos_subject_bindings_pkey PRIMARY KEY (binding_row_id);


--
-- Name: observation_current observation_current_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_current
    ADD CONSTRAINT observation_current_pkey PRIMARY KEY (observation_id);


--
-- Name: observation_events observation_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_events
    ADD CONSTRAINT observation_events_pkey PRIMARY KEY (event_id);


--
-- Name: observation_events observation_events_run_id_event_seq_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_events
    ADD CONSTRAINT observation_events_run_id_event_seq_key UNIQUE (run_id, event_seq);


--
-- Name: ontology_rollup ontology_rollup_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ontology_rollup
    ADD CONSTRAINT ontology_rollup_pkey PRIMARY KEY (src_subtype, edge_type, dst_subtype, producer);


--
-- Name: pagerank_scores pagerank_scores_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.pagerank_scores
    ADD CONSTRAINT pagerank_scores_pkey PRIMARY KEY (scope, node_id);


--
-- Name: poros_delivery_attempts poros_delivery_attempts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_delivery_attempts
    ADD CONSTRAINT poros_delivery_attempts_pkey PRIMARY KEY (attempt_id);


--
-- Name: poros_detached_object_manifests poros_detached_manifest_classification_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.poros_detached_object_manifests
    ADD CONSTRAINT poros_detached_manifest_classification_chk CHECK ((classification = ANY (ARRAY['public'::text, 'internal'::text, 'restricted'::text, 'private'::text, 'unspecified'::text]))) NOT VALID;


--
-- Name: poros_detached_object_manifests poros_detached_manifest_shape_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.poros_detached_object_manifests
    ADD CONSTRAINT poros_detached_manifest_shape_chk CHECK ((COALESCE((jsonb_typeof(manifest) = 'object'::text), false) AND COALESCE(((manifest ->> 'object_id'::text) = object_id), false) AND ((NOT (manifest ? 'classification'::text)) OR COALESCE(((manifest ->> 'classification'::text) = classification), false)) AND ((encrypted AND (manifest ? 'ciphertext_sha256'::text) AND COALESCE((jsonb_typeof((manifest -> 'encryption'::text)) = 'object'::text), false) AND COALESCE((object_sha256 = (manifest ->> 'ciphertext_sha256'::text)), false)) OR ((NOT encrypted) AND (NOT (manifest ? 'ciphertext_sha256'::text)) AND (NOT (manifest ? 'encryption'::text)) AND (COALESCE((object_sha256 = (manifest ->> 'cleartext_sha256'::text)), false) OR COALESCE((object_sha256 = (manifest ->> 'sha256'::text)), false)))))) NOT VALID;


--
-- Name: poros_detached_object_manifests poros_detached_object_manifes_side_authority_id_message_id__key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_detached_object_manifests
    ADD CONSTRAINT poros_detached_object_manifes_side_authority_id_message_id__key UNIQUE (side, authority_id, message_id, object_id);


--
-- Name: poros_detached_object_manifests poros_detached_object_manifests_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_detached_object_manifests
    ADD CONSTRAINT poros_detached_object_manifests_pkey PRIMARY KEY (manifest_id);


--
-- Name: poros_inbox_messages poros_inbox_messages_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_inbox_messages
    ADD CONSTRAINT poros_inbox_messages_pkey PRIMARY KEY (inbox_id);


--
-- Name: poros_inbox_messages poros_inbox_messages_receiver_authority_id_message_id_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_inbox_messages
    ADD CONSTRAINT poros_inbox_messages_receiver_authority_id_message_id_key UNIQUE (receiver_authority_id, message_id);


--
-- Name: poros_inbox_messages poros_inbox_messages_status_chk; Type: CHECK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE okg.poros_inbox_messages
    ADD CONSTRAINT poros_inbox_messages_status_chk CHECK ((status = ANY (ARRAY['accepted_for_admission'::text, 'rejected_preflight'::text, 'duplicate'::text, 'quarantined'::text]))) NOT VALID;


--
-- Name: poros_incorporation_events poros_incorporation_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_incorporation_events
    ADD CONSTRAINT poros_incorporation_events_pkey PRIMARY KEY (event_id);


--
-- Name: poros_outbox_messages poros_outbox_messages_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_outbox_messages
    ADD CONSTRAINT poros_outbox_messages_pkey PRIMARY KEY (message_id);


--
-- Name: poros_replay_cache poros_replay_cache_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_replay_cache
    ADD CONSTRAINT poros_replay_cache_pkey PRIMARY KEY (receiver_authority_id, sender_authority_id, message_id);


--
-- Name: post_publish_projection_status post_publish_projection_status_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.post_publish_projection_status
    ADD CONSTRAINT post_publish_projection_status_pkey PRIMARY KEY (deployment_name, generation_id);


--
-- Name: producer_profiles producer_profiles_deployment_name_producer_id_profile_finge_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.producer_profiles
    ADD CONSTRAINT producer_profiles_deployment_name_producer_id_profile_finge_key UNIQUE (deployment_name, producer_id, profile_fingerprint);


--
-- Name: producer_profiles producer_profiles_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.producer_profiles
    ADD CONSTRAINT producer_profiles_pkey PRIMARY KEY (profile_id);


--
-- Name: projection_pending projection_pending_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.projection_pending
    ADD CONSTRAINT projection_pending_pkey PRIMARY KEY (pending_id);


--
-- Name: projection_remediation_batches projection_remediation_batches_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.projection_remediation_batches
    ADD CONSTRAINT projection_remediation_batches_pkey PRIMARY KEY (idempotency_key);


--
-- Name: projection_remediation_runs projection_remediation_runs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.projection_remediation_runs
    ADD CONSTRAINT projection_remediation_runs_pkey PRIMARY KEY (run_id);


--
-- Name: projection_remediation_runs projection_remediation_runs_scope_pin_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.projection_remediation_runs
    ADD CONSTRAINT projection_remediation_runs_scope_pin_key UNIQUE (remediation_kind, deployment_name, branch_name, subtype, field, entity_type, pinned_checkpoint, catalog_version_id);


--
-- Name: repair_selections repair_selections_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.repair_selections
    ADD CONSTRAINT repair_selections_pkey PRIMARY KEY (deployment, generation_id, bucket_key);


--
-- Name: reuse_certificates reuse_certificates_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.reuse_certificates
    ADD CONSTRAINT reuse_certificates_pkey PRIMARY KEY (deployment, generation_id, step_id);


--
-- Name: schema_migration_phases schema_migration_phases_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.schema_migration_phases
    ADD CONSTRAINT schema_migration_phases_pkey PRIMARY KEY (migration_id, phase);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (migration_id);


--
-- Name: source_cursor_blobs source_cursor_blobs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.source_cursor_blobs
    ADD CONSTRAINT source_cursor_blobs_pkey PRIMARY KEY (source, cursor_key, sha256);


--
-- Name: source_progress source_progress_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.source_progress
    ADD CONSTRAINT source_progress_pkey PRIMARY KEY (deployment, source_name, key);


--
-- Name: source_record_set_blobs source_record_set_blobs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.source_record_set_blobs
    ADD CONSTRAINT source_record_set_blobs_pkey PRIMARY KEY (source, sha256);


--
-- Name: source_record_sets source_record_sets_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.source_record_sets
    ADD CONSTRAINT source_record_sets_pkey PRIMARY KEY (source);


--
-- Name: source_watermarks source_watermarks_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.source_watermarks
    ADD CONSTRAINT source_watermarks_pkey PRIMARY KEY (source);


--
-- Name: subtype_ancestors subtype_ancestors_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.subtype_ancestors
    ADD CONSTRAINT subtype_ancestors_pkey PRIMARY KEY (catalog_version_id, subtype_name, ancestor_name);


--
-- Name: subtypes subtypes_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.subtypes
    ADD CONSTRAINT subtypes_pkey PRIMARY KEY (name, catalog_version_id);


--
-- Name: telemetry_artifacts telemetry_artifacts_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_artifacts
    ADD CONSTRAINT telemetry_artifacts_pkey PRIMARY KEY (artifact_id);


--
-- Name: telemetry_events telemetry_events_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_events
    ADD CONSTRAINT telemetry_events_pkey PRIMARY KEY (event_time, event_id);


--
-- Name: telemetry_graph_effect_manifests telemetry_graph_effect_manifests_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_graph_effect_manifests
    ADD CONSTRAINT telemetry_graph_effect_manifests_pkey PRIMARY KEY (deployment_name, trace_id, span_id);


--
-- Name: telemetry_graph_effects telemetry_graph_effects_deployment_name_trace_id_span_id_se_key; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_graph_effects
    ADD CONSTRAINT telemetry_graph_effects_deployment_name_trace_id_span_id_se_key UNIQUE (deployment_name, trace_id, span_id, sequence);


--
-- Name: telemetry_graph_effects telemetry_graph_effects_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_graph_effects
    ADD CONSTRAINT telemetry_graph_effects_pkey PRIMARY KEY (effect_id);


--
-- Name: telemetry_metrics telemetry_metrics_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_metrics
    ADD CONSTRAINT telemetry_metrics_pkey PRIMARY KEY (metric_time, metric_id);


--
-- Name: telemetry_spans telemetry_spans_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_spans
    ADD CONSTRAINT telemetry_spans_pkey PRIMARY KEY (event_time, trace_id, span_id);


--
-- Name: telemetry_traces telemetry_traces_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.telemetry_traces
    ADD CONSTRAINT telemetry_traces_pkey PRIMARY KEY (deployment_name, trace_id);


--
-- Name: verifier_runs verifier_runs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.verifier_runs
    ADD CONSTRAINT verifier_runs_pkey PRIMARY KEY (run_id);


--
-- Name: visual_caption_failures visual_caption_failures_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.visual_caption_failures
    ADD CONSTRAINT visual_caption_failures_pkey PRIMARY KEY (visual_id, vision_signature);


--
-- Name: visual_crop_blobs visual_crop_blobs_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.visual_crop_blobs
    ADD CONSTRAINT visual_crop_blobs_pkey PRIMARY KEY (sha256);


--
-- Name: work_ledger work_ledger_pkey; Type: CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.work_ledger
    ADD CONSTRAINT work_ledger_pkey PRIMARY KEY (deployment, generation_id, mode, category);


--
-- Name: _chronos_b_interval_okg_console_count_rollup_257f8bc5b1 _chronos_b_interval_okg_console_count_rollup_257f8bc5b1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1
    ADD CONSTRAINT _chronos_b_interval_okg_console_count_rollup_257f8bc5b1_pkey PRIMARY KEY (dimension_kind, dimension_value, live_lo);


--
-- Name: _chronos_b_interval_okg_graph_edges_d84ac83c2b _chronos_b_interval_okg_graph_edges_d84ac83c2b_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_b_interval_okg_graph_edges_d84ac83c2b
    ADD CONSTRAINT _chronos_b_interval_okg_graph_edges_d84ac83c2b_pkey PRIMARY KEY (edge_id, live_lo);


--
-- Name: _chronos_b_interval_okg_graph_nodes_1c8e1fc23f _chronos_b_interval_okg_graph_nodes_1c8e1fc23f_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f
    ADD CONSTRAINT _chronos_b_interval_okg_graph_nodes_1c8e1fc23f_pkey PRIMARY KEY (node_id, live_lo);


--
-- Name: _chronos_b_interval_okg_ontology_rollup_b039672993 _chronos_b_interval_okg_ontology_rollup_b039672993_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_b_interval_okg_ontology_rollup_b039672993
    ADD CONSTRAINT _chronos_b_interval_okg_ontology_rollup_b039672993_pkey PRIMARY KEY (src_subtype, edge_type, dst_subtype, producer, live_lo);


--
-- Name: _chronos_branch_indexes _chronos_branch_indexes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_branch_indexes
    ADD CONSTRAINT _chronos_branch_indexes_pkey PRIMARY KEY (backend, index_name);


--
-- Name: _chronos_branch_interval_branches _chronos_branch_interval_branches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_branch_interval_branches
    ADD CONSTRAINT _chronos_branch_interval_branches_pkey PRIMARY KEY (branch_id);


--
-- Name: _chronos_branch_interval_checkpoints _chronos_branch_interval_checkpoints_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_branch_interval_checkpoints
    ADD CONSTRAINT _chronos_branch_interval_checkpoints_pkey PRIMARY KEY (checkpoint_id);


--
-- Name: _chronos_branch_interval_segments _chronos_branch_interval_segments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_branch_interval_segments
    ADD CONSTRAINT _chronos_branch_interval_segments_pkey PRIMARY KEY (segment_id);


--
-- Name: _chronos_branch_tables _chronos_branch_tables_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._chronos_branch_tables
    ADD CONSTRAINT _chronos_branch_tables_pkey PRIMARY KEY (table_name);


--
-- Name: admission_envelopes_deployment_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_deployment_status_idx ON okg.admission_envelopes USING btree (deployment_name, status, created_at DESC);


--
-- Name: admission_envelopes_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_generation_idx ON okg.admission_envelopes USING btree (target_generation_id) WHERE (target_generation_id IS NOT NULL);


--
-- Name: admission_envelopes_generation_ref_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_generation_ref_idx ON okg.admission_envelopes USING btree (target_generation_ref) WHERE (target_generation_ref IS NOT NULL);


--
-- Name: admission_envelopes_pending_lens_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_pending_lens_idx ON okg.admission_envelopes USING btree (deployment_name, target_branch_name, created_at) WHERE ((producer_kind = 'lens'::text) AND (status = ANY (ARRAY['pending'::text, 'accepted'::text])));


--
-- Name: admission_envelopes_producer_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_producer_idx ON okg.admission_envelopes USING btree (deployment_name, producer_kind, producer_id);


--
-- Name: admission_envelopes_source_run_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_source_run_idx ON okg.admission_envelopes USING btree (source, run_id) WHERE (run_id IS NOT NULL);


--
-- Name: admission_envelopes_target_branch_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_envelopes_target_branch_idx ON okg.admission_envelopes USING btree (deployment_name, target_branch_name) WHERE (target_branch_name IS NOT NULL);


--
-- Name: admission_events_admission_created_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_events_admission_created_idx ON okg.admission_events USING btree (admission_id, created_at DESC);


--
-- Name: admission_events_type_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_events_type_idx ON okg.admission_events USING btree (event_type, created_at DESC);


--
-- Name: admission_record_refs_admission_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_record_refs_admission_idx ON okg.admission_record_refs USING btree (admission_id, ref_kind);


--
-- Name: admission_record_refs_source_run_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX admission_record_refs_source_run_idx ON okg.admission_record_refs USING btree (source, run_id) WHERE (run_id IS NOT NULL);


--
-- Name: aliases_alias_trgm_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX aliases_alias_trgm_idx ON okg.aliases USING gin (alias public.gin_trgm_ops);


--
-- Name: aliases_normalized_lookup_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX aliases_normalized_lookup_idx ON okg.aliases USING btree (catalog_version_id, scope, normalized_alias) WHERE (normalized_alias IS NOT NULL);


--
-- Name: attribute_effective_integrity_receipt_scope_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_integrity_receipt_scope_idx ON okg.attribute_effective_integrity_receipts USING btree (deployment_name, branch_id, namespace, ordinal DESC);


--
-- Name: attribute_effective_intervals_attrs_gin_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_intervals_attrs_gin_idx ON okg.attribute_effective_view_intervals USING gin (attrs jsonb_path_ops);


--
-- Name: attribute_effective_intervals_edge_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_intervals_edge_idx ON okg.attribute_effective_view_intervals USING btree (branch_id, namespace, edge_type, from_ord, to_ord, target_id) WHERE (target_kind = 'edge'::text);


--
-- Name: attribute_effective_intervals_node_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_intervals_node_idx ON okg.attribute_effective_view_intervals USING btree (branch_id, namespace, subtype, from_ord, to_ord, target_id) WHERE (target_kind = 'node'::text);


--
-- Name: attribute_effective_intervals_open_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_intervals_open_idx ON okg.attribute_effective_view_intervals USING btree (branch_id, namespace, target_kind, target_id) WHERE (to_ord = '9223372036854775807'::bigint);


--
-- Name: attribute_effective_view_attrs_gin_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_view_attrs_gin_idx ON okg.attribute_effective_view USING gin (attrs jsonb_path_ops);


--
-- Name: attribute_effective_view_edge_type_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_view_edge_type_idx ON okg.attribute_effective_view USING btree (generation_id, namespace, edge_type, target_id) WHERE (target_kind = 'edge'::text);


--
-- Name: attribute_effective_view_generation_namespace_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_view_generation_namespace_idx ON okg.attribute_effective_view USING btree (generation_id, namespace);


--
-- Name: attribute_effective_view_node_subtype_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX attribute_effective_view_node_subtype_idx ON okg.attribute_effective_view USING btree (generation_id, namespace, subtype, target_id) WHERE (target_kind = 'node'::text);


--
-- Name: branch_merge_requests_source_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX branch_merge_requests_source_idx ON okg.branch_merge_requests USING btree (deployment_name, source_branch_id, status, created_at DESC);


--
-- Name: branch_merge_requests_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX branch_merge_requests_target_idx ON okg.branch_merge_requests USING btree (deployment_name, target_branch_id, status, created_at DESC);


--
-- Name: branches_deployment_name_uidx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX branches_deployment_name_uidx ON okg.branches USING btree (deployment_name, name);


--
-- Name: branches_deployment_role_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX branches_deployment_role_status_idx ON okg.branches USING btree (deployment_name, role, status, branch_id);


--
-- Name: catalog_versions_deployment_fingerprint_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX catalog_versions_deployment_fingerprint_idx ON okg.catalog_versions USING btree (deployment_name, status, catalog_fingerprint, catalog_version_id DESC);


--
-- Name: catalog_versions_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX catalog_versions_status_idx ON okg.catalog_versions USING btree (status);


--
-- Name: chronos_hot_index_manifest_one_active_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX chronos_hot_index_manifest_one_active_idx ON okg.chronos_hot_index_manifest USING btree (physical_table, suffix) WHERE (state = ANY (ARRAY['activating'::text, 'active'::text]));


--
-- Name: compacted_generations_target_id_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX compacted_generations_target_id_idx ON okg.compacted_generations USING btree (deployment_name, target_generation_id) WHERE (target_generation_id IS NOT NULL);


--
-- Name: consistency_candidate_coverage_check_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_candidate_coverage_check_idx ON okg.consistency_candidate_coverage USING btree (check_id, target_generation_id, coverage_complete);


--
-- Name: consistency_candidates_run_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_candidates_run_idx ON okg.consistency_candidates USING btree (run_id);


--
-- Name: consistency_candidates_scope_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_candidates_scope_idx ON okg.consistency_candidates USING btree (check_id, target_generation_id, input_scope);


--
-- Name: consistency_check_definitions_dep_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_check_definitions_dep_idx ON okg.consistency_check_definitions USING btree (deployment_name, predicate_family);


--
-- Name: consistency_check_definitions_kind_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_check_definitions_kind_idx ON okg.consistency_check_definitions USING btree (deployment_name, check_kind) WHERE (check_kind IS NOT NULL);


--
-- Name: consistency_gate_results_blocked_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_gate_results_blocked_idx ON okg.consistency_gate_results USING btree (check_id, gate_name, created_at DESC) WHERE blocks_publish;


--
-- Name: consistency_gate_results_check_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX consistency_gate_results_check_idx ON okg.consistency_gate_results USING btree (check_id, gate_name, target_generation_id);


--
-- Name: coordination_native_events_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX coordination_native_events_status_idx ON okg.coordination_native_events USING btree (deployment_name, status, event_type, observed_at DESC);


--
-- Name: coordination_native_events_task_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX coordination_native_events_task_idx ON okg.coordination_native_events USING btree (deployment_name, change_id, task_id, observed_at DESC);


--
-- Name: coordination_native_state_workspace_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX coordination_native_state_workspace_idx ON okg.coordination_native_state USING btree (deployment_name, workspace_id) WHERE (workspace_id IS NOT NULL);


--
-- Name: derived_artifact_definitions_logical_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_artifact_definitions_logical_idx ON okg.derived_artifact_definitions USING btree (deployment_name, artifact_kind, logical_key);


--
-- Name: derived_artifact_dependencies_downstream_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_artifact_dependencies_downstream_idx ON okg.derived_artifact_dependencies USING btree (downstream_definition_id, upstream_definition_id);


--
-- Name: derived_artifact_dependencies_uidx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX derived_artifact_dependencies_uidx ON okg.derived_artifact_dependencies USING btree (downstream_definition_id, upstream_definition_id, upstream_slot_name, COALESCE(scope_key, ''::text));


--
-- Name: derived_artifact_runs_completed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_artifact_runs_completed_idx ON okg.derived_artifact_runs USING btree (definition_id, completed_at DESC) WHERE (status = ANY (ARRAY['completed'::text, 'carried_forward'::text]));


--
-- Name: derived_artifact_runs_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_artifact_runs_target_idx ON okg.derived_artifact_runs USING btree (definition_id, target_generation_id, status);


--
-- Name: derived_dirty_rule_audits_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_dirty_rule_audits_target_idx ON okg.derived_dirty_rule_audits USING btree (definition_id, target_generation_id, status);


--
-- Name: derived_dirty_rule_runs_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_dirty_rule_runs_target_idx ON okg.derived_dirty_rule_runs USING btree (definition_id, target_generation_id, status);


--
-- Name: derived_dirty_scopes_dedupe_uidx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX derived_dirty_scopes_dedupe_uidx ON okg.derived_dirty_scopes USING btree (definition_id, target_generation_id, scope_key, dependency_kind, dependency_ref);


--
-- Name: derived_dirty_scopes_unconsumed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_dirty_scopes_unconsumed_idx ON okg.derived_dirty_scopes USING btree (definition_id, target_generation_id, queued_at) WHERE (consumed_run_id IS NULL);


--
-- Name: derived_output_dependency_lookup_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_dependency_lookup_idx ON okg.derived_output_dependency_index USING btree (dependency_kind, dependency_ref, manifest_id);


--
-- Name: derived_output_dependency_manifest_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_dependency_manifest_idx ON okg.derived_output_dependency_index USING btree (manifest_id, output_id);


--
-- Name: derived_output_lineage_manifest_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_lineage_manifest_idx ON okg.derived_output_lineage_envelopes USING btree (manifest_id, output_id);


--
-- Name: derived_output_lineage_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_lineage_target_idx ON okg.derived_output_lineage_envelopes USING btree (definition_id, target_generation_id, output_kind);


--
-- Name: derived_output_manifests_completed_latest_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_manifests_completed_latest_idx ON okg.derived_output_manifests USING btree (definition_id, target_generation_id DESC, promoted_at DESC) INCLUDE (manifest_id, run_id) WHERE (status = 'completed'::text);


--
-- Name: derived_output_manifests_completed_uidx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX derived_output_manifests_completed_uidx ON okg.derived_output_manifests USING btree (definition_id, target_generation_id) WHERE (status = 'completed'::text);


--
-- Name: derived_output_manifests_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_manifests_target_idx ON okg.derived_output_manifests USING btree (definition_id, target_generation_id, status);


--
-- Name: derived_output_slots_manifest_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_output_slots_manifest_idx ON okg.derived_output_slots USING btree (manifest_id, storage_kind);


--
-- Name: derived_relation_registry_definition_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_relation_registry_definition_idx ON okg.derived_relation_registry USING btree (definition_id, logical_relation_id, status);


--
-- Name: derived_run_dependency_bindings_run_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_run_dependency_bindings_run_idx ON okg.derived_run_dependency_bindings USING btree (run_id);


--
-- Name: derived_run_dependency_bindings_uidx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX derived_run_dependency_bindings_uidx ON okg.derived_run_dependency_bindings USING btree (run_id, upstream_definition_id, upstream_slot_name, COALESCE(scope_key, ''::text));


--
-- Name: derived_run_dependency_bindings_upstream_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_run_dependency_bindings_upstream_idx ON okg.derived_run_dependency_bindings USING btree (upstream_definition_id, upstream_target_generation_id);


--
-- Name: derived_schedule_nodes_definition_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_schedule_nodes_definition_idx ON okg.derived_schedule_nodes USING btree (definition_id, status);


--
-- Name: derived_schedule_plans_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX derived_schedule_plans_target_idx ON okg.derived_schedule_plans USING btree (deployment_name, target_generation_id, execution_phase, status);


--
-- Name: edge_fact_strandings_by_first_seen; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_fact_strandings_by_first_seen ON okg.edge_fact_strandings USING btree (first_seen_gen);


--
-- Name: edge_fact_strandings_by_last_retry; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_fact_strandings_by_last_retry ON okg.edge_fact_strandings USING btree (last_retry_gen);


--
-- Name: edge_facts_dedupe_probe_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_dedupe_probe_idx ON okg.edge_facts USING btree (source, dedupe_key, op) WHERE (source ~~ '\_enricher:%'::text);


--
-- Name: edge_facts_dst_edge_observed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_dst_edge_observed_idx ON okg.edge_facts USING btree (dst, edge_type, observed_at DESC);


--
-- Name: edge_facts_observed_at_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_observed_at_idx ON okg.edge_facts USING btree (observed_at DESC);


--
-- Name: edge_facts_project_i_fact_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_project_i_fact_idx ON okg.edge_facts USING btree (fact_id, edge_id) INCLUDE (src, dst, edge_type, attrs) WHERE (op = 'I'::bpchar);


--
-- Name: edge_facts_run_id_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_run_id_idx ON okg.edge_facts USING btree (run_id, fact_id);


--
-- Name: edge_facts_source_op_src_edge_dst_observed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_source_op_src_edge_dst_observed_idx ON okg.edge_facts USING btree (source, op, src, edge_type, dst, observed_at DESC);


--
-- Name: edge_facts_source_record_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_source_record_idx ON okg.edge_facts USING btree (source, ((source_record_id)::text), ((source_revision)::text));


--
-- Name: edge_facts_source_run_observed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_source_run_observed_idx ON okg.edge_facts USING btree (source, run_id, observed_at DESC);


--
-- Name: edge_facts_src_edge_dst_observed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_src_edge_dst_observed_idx ON okg.edge_facts USING btree (src, edge_type, dst, observed_at DESC);


--
-- Name: edge_facts_workload_fact_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX edge_facts_workload_fact_idx ON okg.edge_facts USING btree (fact_id DESC) WHERE ((source !~~ 'materializer:%'::text) AND (source !~~ 'derived_artifact:%'::text));


--
-- Name: enricher_backfill_progress_lookup_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX enricher_backfill_progress_lookup_idx ON okg.enricher_backfill_progress USING btree (deployment, branch_name, enricher_name, status, updated_at DESC);


--
-- Name: enricher_progress_deployment_last_run_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX enricher_progress_deployment_last_run_idx ON okg.enricher_progress USING btree (deployment, last_run_at DESC);


--
-- Name: entity_identity_claims_unique_mint_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX entity_identity_claims_unique_mint_idx ON okg.entity_identity_claims USING btree (deployment_name, branch_name, subtype, canonical_node_id) WHERE (resolution_method = 'mint'::text);


--
-- Name: envelope_violations_deployment_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX envelope_violations_deployment_generation_idx ON okg.envelope_violations USING btree (deployment, generation_id);


--
-- Name: equivalence_verdicts_deployment_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX equivalence_verdicts_deployment_generation_idx ON okg.equivalence_verdicts USING btree (deployment, production_generation);


--
-- Name: external_probe_cache_node_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX external_probe_cache_node_idx ON okg.external_probe_cache USING btree (deployment_name, node_id);


--
-- Name: fact_batch_receipts_recovery_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX fact_batch_receipts_recovery_idx ON okg.fact_batch_receipts USING btree (state, updated_at) WHERE (state = ANY (ARRAY['prepared'::text, 'compensating'::text]));


--
-- Name: federation_candidates_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_candidates_status_idx ON okg.federation_discovered_candidates USING btree (deployment_name, local_authority_id, status, updated_at DESC);


--
-- Name: federation_connection_requests_local_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_connection_requests_local_idx ON okg.federation_connection_requests USING btree (deployment_name, local_authority_id, status, created_at DESC);


--
-- Name: federation_connection_requests_target_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_connection_requests_target_idx ON okg.federation_connection_requests USING btree (deployment_name, target_authority_id, target_address, status);


--
-- Name: federation_mailbox_events_message_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_mailbox_events_message_idx ON okg.federation_mailbox_events USING btree (message_id, created_at DESC);


--
-- Name: federation_mailbox_events_recipient_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_mailbox_events_recipient_idx ON okg.federation_mailbox_events USING btree (recipient_authority_id, created_at DESC);


--
-- Name: federation_mailbox_lease_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_mailbox_lease_idx ON okg.federation_mailbox_messages USING btree (status, lease_expires_at) WHERE (status = 'leased'::text);


--
-- Name: federation_mailbox_packet_hash_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_mailbox_packet_hash_idx ON okg.federation_mailbox_messages USING btree (packet_sha256);


--
-- Name: federation_mailbox_recipient_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_mailbox_recipient_status_idx ON okg.federation_mailbox_messages USING btree (recipient_authority_id, status, expires_at, created_at);


--
-- Name: federation_mailbox_sender_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_mailbox_sender_idx ON okg.federation_mailbox_messages USING btree (sender_authority_id, created_at DESC);


--
-- Name: federation_peer_trust_pins_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_peer_trust_pins_status_idx ON okg.federation_peer_trust_pins USING btree (deployment_name, local_authority_id, remote_authority_id, status, updated_at DESC);


--
-- Name: federation_route_events_decision_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_route_events_decision_idx ON okg.federation_route_events USING btree (deployment_name, decision, reason_code, created_at DESC);


--
-- Name: federation_route_events_local_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX federation_route_events_local_idx ON okg.federation_route_events USING btree (deployment_name, local_authority_id, remote_authority_id, created_at DESC);


--
-- Name: generation_ordinals_branch_ordinal; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX generation_ordinals_branch_ordinal ON okg.generation_ordinals USING btree (branch_id, ordinal);


--
-- Name: graph_generations_branch_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX graph_generations_branch_status_idx ON okg.graph_generations USING btree (branch_id, status, generation_id DESC);


--
-- Name: graph_generations_deployment_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX graph_generations_deployment_status_idx ON okg.graph_generations USING btree (deployment_name, status, generation_id DESC);


--
-- Name: graph_generations_one_pending_per_branch_uidx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX graph_generations_one_pending_per_branch_uidx ON okg.graph_generations USING btree (deployment_name, branch_id) WHERE (status = 'pending'::text);


--
-- Name: graph_generations_status_published_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX graph_generations_status_published_idx ON okg.graph_generations USING btree (status, published_at DESC);


--
-- Name: graph_generations_trace_id_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX graph_generations_trace_id_idx ON okg.graph_generations USING btree (trace_id) WHERE (trace_id IS NOT NULL);


--
-- Name: history_generation_pins_checkpoint_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX history_generation_pins_checkpoint_idx ON okg.history_generation_pins USING btree (deployment_name, branch_id, checkpoint_id);


--
-- Name: identifier_patterns_cv_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX identifier_patterns_cv_idx ON okg.identifier_patterns USING btree (catalog_version_id);


--
-- Name: idx_attribute_effective_attrs_observation; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX idx_attribute_effective_attrs_observation ON okg.attribute_effective_attrs USING btree (observation_id);


--
-- Name: idx_attribute_effective_attrs_target; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX idx_attribute_effective_attrs_target ON okg.attribute_effective_attrs USING btree (target_kind, target_id, generation_id, namespace);


--
-- Name: idx_attribute_observations_current_unique; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX idx_attribute_observations_current_unique ON okg.attribute_observations USING btree (target_kind, target_id, generation_id, namespace, attr_key) WHERE ((status = 'accepted'::text) AND (superseded_by IS NULL));


--
-- Name: idx_attribute_observations_dependency; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX idx_attribute_observations_dependency ON okg.attribute_observations USING btree (dependency_fingerprint) WHERE (dependency_fingerprint IS NOT NULL);


--
-- Name: idx_attribute_observations_target; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX idx_attribute_observations_target ON okg.attribute_observations USING btree (target_kind, target_id, generation_id, namespace, attr_key, status);


--
-- Name: ir_invocations_deployment_kind_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_invocations_deployment_kind_idx ON okg.ir_invocations USING btree (deployment_name, invocation_kind, status, created_at DESC);


--
-- Name: ir_invocations_pin_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_invocations_pin_idx ON okg.ir_invocations USING btree (deployment_name, branch_name, generation_id, invocation_kind);


--
-- Name: ir_invocations_program_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_invocations_program_idx ON okg.ir_invocations USING btree (program_id, created_at DESC);


--
-- Name: ir_program_bindings_dedupe_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX ir_program_bindings_dedupe_idx ON okg.ir_program_bindings USING btree (program_id, deployment_name, owner_kind, owner_id, program_role, COALESCE(generation_id, ('-1'::integer)::bigint), COALESCE(branch_name, ''::text));


--
-- Name: ir_program_bindings_owner_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_program_bindings_owner_idx ON okg.ir_program_bindings USING btree (deployment_name, owner_kind, owner_id, program_role);


--
-- Name: ir_program_bindings_program_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_program_bindings_program_idx ON okg.ir_program_bindings USING btree (program_id, created_at DESC);


--
-- Name: ir_programs_kind_created_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_programs_kind_created_idx ON okg.ir_programs USING btree (program_kind, created_at DESC);


--
-- Name: ir_programs_registry_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX ir_programs_registry_idx ON okg.ir_programs USING btree (registry_id, registry_version, registry_checksum);


--
-- Name: lease_audit_recent_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lease_audit_recent_idx ON okg.lease_audit USING btree (at DESC, deployment_name);


--
-- Name: lens_definitions_view_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lens_definitions_view_idx ON okg.lens_definitions USING btree (deployment_name, view_definition_id);


--
-- Name: lens_overlay_log_lens_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lens_overlay_log_lens_idx ON okg.lens_overlay_log USING btree (lens_definition_id, target_generation_id, status, created_at DESC);


--
-- Name: lens_overlay_log_promotion_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lens_overlay_log_promotion_idx ON okg.lens_overlay_log USING btree (promoted_admission_id) WHERE (promoted_admission_id IS NOT NULL);


--
-- Name: lens_overlay_log_row_anchor_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lens_overlay_log_row_anchor_idx ON okg.lens_overlay_log USING btree (lens_definition_id, target_generation_id, status, ((source_delta ->> 'slot_name'::text)), ((source_delta ->> 'scope_key'::text)), ((source_delta ->> 'row_key'::text)));


--
-- Name: lens_putback_deltas_request_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lens_putback_deltas_request_idx ON okg.lens_putback_deltas USING btree (request_id, validation_status);


--
-- Name: lens_putback_requests_lens_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX lens_putback_requests_lens_idx ON okg.lens_putback_requests USING btree (lens_definition_id, target_generation_id, status, created_at DESC);


--
-- Name: link_retry_entity_value_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX link_retry_entity_value_idx ON okg.link_retry USING btree (entity_value) WHERE (ready_at IS NULL);


--
-- Name: link_retry_ready_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX link_retry_ready_idx ON okg.link_retry USING btree (deployment_name, branch_name, node_id) WHERE (ready_at IS NOT NULL);


--
-- Name: llm_calls_deployment_caller_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX llm_calls_deployment_caller_ts_idx ON okg.llm_calls USING btree (deployment, caller, ts DESC);


--
-- Name: llm_calls_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX llm_calls_ts_idx ON okg.llm_calls USING btree (ts DESC);


--
-- Name: mcp_call_log_deployment_operator_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_call_log_deployment_operator_ts_idx ON okg.mcp_call_log USING btree (deployment_name, operator_name, ts DESC);


--
-- Name: mcp_call_log_error_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_call_log_error_ts_idx ON okg.mcp_call_log USING btree (ts DESC) WHERE (error_code IS NOT NULL);


--
-- Name: mcp_call_log_operator_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_call_log_operator_ts_idx ON okg.mcp_call_log USING btree (operator_name, ts DESC);


--
-- Name: mcp_call_log_session_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_call_log_session_ts_idx ON okg.mcp_call_log USING btree (session_id, ts DESC);


--
-- Name: mcp_call_log_tool_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_call_log_tool_ts_idx ON okg.mcp_call_log USING btree (tool_name, ts DESC);


--
-- Name: mcp_call_log_ts_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_call_log_ts_idx ON okg.mcp_call_log USING btree (ts DESC);


--
-- Name: mcp_trace_branch_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_trace_branch_time_idx ON okg.mcp_trace_events USING btree (branch_name, event_time);


--
-- Name: mcp_trace_deployment_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_trace_deployment_time_idx ON okg.mcp_trace_events USING btree (deployment_name, event_time);


--
-- Name: mcp_trace_events_event_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_trace_events_event_time_idx ON okg.mcp_trace_events USING btree (event_time DESC);


--
-- Name: mcp_trace_route_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_trace_route_time_idx ON okg.mcp_trace_events USING btree (route_id, event_time) WHERE (route_id IS NOT NULL);


--
-- Name: mcp_trace_session_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_trace_session_time_idx ON okg.mcp_trace_events USING btree (session_id, event_time);


--
-- Name: mcp_trace_tool_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX mcp_trace_tool_time_idx ON okg.mcp_trace_events USING btree (tool_name, event_time);


--
-- Name: model_outcomes_recorded_at_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX model_outcomes_recorded_at_idx ON okg.model_outcomes USING btree (deployment, recorded_at DESC);


--
-- Name: node_facts_dedupe_probe_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_dedupe_probe_idx ON okg.node_facts USING btree (source, dedupe_key, op) WHERE (source ~~ '\_enricher:%'::text);


--
-- Name: node_facts_node_id_observed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_node_id_observed_idx ON okg.node_facts USING btree (node_id, observed_at DESC);


--
-- Name: node_facts_observed_at_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_observed_at_idx ON okg.node_facts USING btree (observed_at DESC);


--
-- Name: node_facts_run_id_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_run_id_idx ON okg.node_facts USING btree (run_id, fact_id);


--
-- Name: node_facts_source_record_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_source_record_idx ON okg.node_facts USING btree (source, ((source_record_id)::text), ((source_revision)::text));


--
-- Name: node_facts_source_run_observed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_source_run_observed_idx ON okg.node_facts USING btree (source, run_id, observed_at DESC);


--
-- Name: node_facts_workload_fact_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_facts_workload_fact_idx ON okg.node_facts USING btree (fact_id DESC) WHERE ((source !~~ 'materializer:%'::text) AND (source !~~ 'derived_artifact:%'::text));


--
-- Name: node_series_node_metric_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_series_node_metric_time_idx ON okg.node_series USING btree (deployment_name, node_id, metric, observed_at DESC);


--
-- Name: node_series_observed_at_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX node_series_observed_at_idx ON okg.node_series USING btree (observed_at DESC);


--
-- Name: nomos_config_active_scope_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX nomos_config_active_scope_idx ON okg.nomos_config_activations USING btree (deployment_name, scope) WHERE (status = 'active'::text);


--
-- Name: nomos_config_sets_deployment_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_config_sets_deployment_idx ON okg.nomos_config_sets USING btree (deployment_name, config_set_id, status);


--
-- Name: nomos_decisions_deployment_created_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_decisions_deployment_created_idx ON okg.nomos_decisions USING btree (deployment_name, created_at DESC);


--
-- Name: nomos_decisions_effect_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_decisions_effect_idx ON okg.nomos_decisions USING btree (deployment_name, effect, created_at DESC);


--
-- Name: nomos_decisions_input_hash_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_decisions_input_hash_idx ON okg.nomos_decisions USING btree (input_hash);


--
-- Name: nomos_external_claims_decision_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_external_claims_decision_idx ON okg.nomos_external_claims USING btree (nomos_decision_id) WHERE (nomos_decision_id IS NOT NULL);


--
-- Name: nomos_external_claims_fingerprint_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_external_claims_fingerprint_idx ON okg.nomos_external_claims USING btree (claim_fingerprint);


--
-- Name: nomos_external_claims_provider_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_external_claims_provider_idx ON okg.nomos_external_claims USING btree (deployment_name, provider_id, observed_at DESC);


--
-- Name: nomos_identity_providers_deployment_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_identity_providers_deployment_idx ON okg.nomos_identity_providers USING btree (deployment_name, provider_id, status);


--
-- Name: nomos_permission_grants_subject_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_permission_grants_subject_idx ON okg.nomos_permission_grants USING btree (deployment_name, subject, scope, status);


--
-- Name: nomos_policy_active_scope_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE UNIQUE INDEX nomos_policy_active_scope_idx ON okg.nomos_policy_activations USING btree (deployment_name, scope) WHERE (status = 'active'::text);


--
-- Name: nomos_policy_bundles_deployment_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_policy_bundles_deployment_idx ON okg.nomos_policy_bundles USING btree (deployment_name, policy_bundle_id, status);


--
-- Name: nomos_subject_bindings_provider_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_subject_bindings_provider_idx ON okg.nomos_subject_bindings USING btree (deployment_name, provider_id, issuer, scope, status);


--
-- Name: nomos_subject_bindings_subject_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX nomos_subject_bindings_subject_idx ON okg.nomos_subject_bindings USING btree (deployment_name, subject, scope, status);


--
-- Name: observation_current_check_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX observation_current_check_status_idx ON okg.observation_current USING btree (check_id, finding_status, evaluation_status);


--
-- Name: observation_events_check_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX observation_events_check_idx ON okg.observation_events USING btree (check_id, target_generation_id, candidate_id);


--
-- Name: observation_events_observation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX observation_events_observation_idx ON okg.observation_events USING btree (observation_id, target_generation_id, event_seq);


--
-- Name: pagerank_scores_scope_pagerank_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX pagerank_scores_scope_pagerank_idx ON okg.pagerank_scores USING btree (scope, pagerank DESC);


--
-- Name: poros_delivery_attempts_message_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_delivery_attempts_message_idx ON okg.poros_delivery_attempts USING btree (message_id, attempted_at DESC);


--
-- Name: poros_detached_manifest_message_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_detached_manifest_message_idx ON okg.poros_detached_object_manifests USING btree (message_id, body_sha256);


--
-- Name: poros_inbox_body_hash_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_inbox_body_hash_idx ON okg.poros_inbox_messages USING btree (body_sha256) WHERE (body_sha256 IS NOT NULL);


--
-- Name: poros_inbox_receiver_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_inbox_receiver_status_idx ON okg.poros_inbox_messages USING btree (receiver_authority_id, status, received_at DESC);


--
-- Name: poros_inbox_sender_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_inbox_sender_idx ON okg.poros_inbox_messages USING btree (sender_authority_id, received_at DESC);


--
-- Name: poros_incorporation_decision_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_incorporation_decision_idx ON okg.poros_incorporation_events USING btree (nomos_decision_id) WHERE (nomos_decision_id IS NOT NULL);


--
-- Name: poros_incorporation_message_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_incorporation_message_idx ON okg.poros_incorporation_events USING btree (message_id, created_at DESC);


--
-- Name: poros_outbox_body_hash_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_outbox_body_hash_idx ON okg.poros_outbox_messages USING btree (body_sha256);


--
-- Name: poros_outbox_graph_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_outbox_graph_idx ON okg.poros_outbox_messages USING btree (graph_id, created_at DESC);


--
-- Name: poros_outbox_recipient_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_outbox_recipient_status_idx ON okg.poros_outbox_messages USING btree (recipient_authority_id, status, created_at DESC);


--
-- Name: poros_outbox_retry_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_outbox_retry_idx ON okg.poros_outbox_messages USING btree (status, next_retry_at, updated_at);


--
-- Name: poros_outbox_sender_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_outbox_sender_generation_idx ON okg.poros_outbox_messages USING btree (sender_authority_id, sender_generation_id, created_at DESC);


--
-- Name: poros_replay_cache_expires_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX poros_replay_cache_expires_idx ON okg.poros_replay_cache USING btree (expires_at);


--
-- Name: post_publish_projection_status_failed_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX post_publish_projection_status_failed_idx ON okg.post_publish_projection_status USING btree (deployment_name, last_run_at DESC) WHERE (status = 'failed'::text);


--
-- Name: producer_profiles_catalog_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX producer_profiles_catalog_idx ON okg.producer_profiles USING btree (catalog_version_id) WHERE (catalog_version_id IS NOT NULL);


--
-- Name: producer_profiles_deployment_status_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX producer_profiles_deployment_status_idx ON okg.producer_profiles USING btree (deployment_name, producer_id, status);


--
-- Name: projection_pending_gen_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX projection_pending_gen_idx ON okg.projection_pending USING btree (generation_id);


--
-- Name: repair_selections_deployment_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX repair_selections_deployment_generation_idx ON okg.repair_selections USING btree (deployment, generation_id);


--
-- Name: reuse_certificates_deployment_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX reuse_certificates_deployment_generation_idx ON okg.reuse_certificates USING btree (deployment, generation_id);


--
-- Name: reuse_certificates_deployment_step_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX reuse_certificates_deployment_step_idx ON okg.reuse_certificates USING btree (deployment, step_id);


--
-- Name: source_progress_adapter_state_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX source_progress_adapter_state_idx ON okg.source_progress USING btree (deployment, source_name, key) WHERE (key !~~ 'sync.%'::text);


--
-- Name: source_progress_deployment_updated_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX source_progress_deployment_updated_idx ON okg.source_progress USING btree (deployment, updated_at DESC);


--
-- Name: subtype_ancestors_by_ancestor; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX subtype_ancestors_by_ancestor ON okg.subtype_ancestors USING btree (catalog_version_id, ancestor_name);


--
-- Name: telemetry_artifacts_trace_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_artifacts_trace_idx ON okg.telemetry_artifacts USING btree (deployment_name, trace_id, created_at DESC);


--
-- Name: telemetry_events_event_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_events_event_time_idx ON okg.telemetry_events USING btree (event_time DESC);


--
-- Name: telemetry_events_name_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_events_name_time_idx ON okg.telemetry_events USING btree (event_name, event_time DESC);


--
-- Name: telemetry_events_trace_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_events_trace_time_idx ON okg.telemetry_events USING btree (trace_id, event_time);


--
-- Name: telemetry_graph_effects_trace_sequence_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_graph_effects_trace_sequence_idx ON okg.telemetry_graph_effects USING btree (deployment_name, trace_id, span_id, sequence);


--
-- Name: telemetry_metrics_deployment_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_metrics_deployment_time_idx ON okg.telemetry_metrics USING btree (deployment_name, metric_time DESC);


--
-- Name: telemetry_metrics_metric_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_metrics_metric_time_idx ON okg.telemetry_metrics USING btree (metric_time DESC);


--
-- Name: telemetry_metrics_name_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_metrics_name_time_idx ON okg.telemetry_metrics USING btree (metric_name, metric_time DESC);


--
-- Name: telemetry_metrics_trace_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_metrics_trace_idx ON okg.telemetry_metrics USING btree (trace_id) WHERE (trace_id IS NOT NULL);


--
-- Name: telemetry_spans_deployment_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_spans_deployment_time_idx ON okg.telemetry_spans USING btree (deployment_name, event_time DESC);


--
-- Name: telemetry_spans_event_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_spans_event_time_idx ON okg.telemetry_spans USING btree (event_time DESC);


--
-- Name: telemetry_spans_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_spans_generation_idx ON okg.telemetry_spans USING btree (generation_id) WHERE (generation_id IS NOT NULL);


--
-- Name: telemetry_spans_tool_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_spans_tool_time_idx ON okg.telemetry_spans USING btree (tool_name, event_time DESC) WHERE (tool_name IS NOT NULL);


--
-- Name: telemetry_spans_trace_time_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_spans_trace_time_idx ON okg.telemetry_spans USING btree (trace_id, event_time);


--
-- Name: telemetry_traces_deployment_started_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_traces_deployment_started_idx ON okg.telemetry_traces USING btree (deployment_name, started_at DESC);


--
-- Name: telemetry_traces_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX telemetry_traces_generation_idx ON okg.telemetry_traces USING btree (generation_id) WHERE (generation_id IS NOT NULL);


--
-- Name: verifier_runs_candidate_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX verifier_runs_candidate_idx ON okg.verifier_runs USING btree (deployment, candidate_id);


--
-- Name: verifier_runs_deployment_prompt_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX verifier_runs_deployment_prompt_idx ON okg.verifier_runs USING btree (deployment, prompt_version, emitted_at);


--
-- Name: work_ledger_deployment_generation_idx; Type: INDEX; Schema: okg; Owner: -
--

CREATE INDEX work_ledger_deployment_generation_idx ON okg.work_ledger USING btree (deployment, generation_id);


--
-- Name: _chronos_idx_interval_branches_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX _chronos_idx_interval_branches_parent ON public._chronos_branch_interval_branches USING btree (parent_branch_id);


--
-- Name: idx__chronos_b_interval_okg_console_count_rollup_257f8bc5b1_pk_; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_console_count_rollup_257f8bc5b1_pk_ ON public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1 USING btree (dimension_kind, dimension_value, live_hi);


--
-- Name: idx__chronos_b_interval_okg_console_count_rollup_257f8bc5b1_wri; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_console_count_rollup_257f8bc5b1_wri ON public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1 USING btree (writer_segment_id, dimension_kind, dimension_value);


--
-- Name: idx__chronos_b_interval_okg_graph_edges_d84ac83c2b_pk_hi; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_graph_edges_d84ac83c2b_pk_hi ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (edge_id, live_hi);


--
-- Name: idx__chronos_b_interval_okg_graph_edges_d84ac83c2b_writer_segme; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_graph_edges_d84ac83c2b_writer_segme ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (writer_segment_id, edge_id);


--
-- Name: idx__chronos_b_interval_okg_graph_nodes_1c8e1fc23f_pk_hi; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_graph_nodes_1c8e1fc23f_pk_hi ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (node_id, live_hi);


--
-- Name: idx__chronos_b_interval_okg_graph_nodes_1c8e1fc23f_writer_segme; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_graph_nodes_1c8e1fc23f_writer_segme ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (writer_segment_id, node_id);


--
-- Name: idx__chronos_b_interval_okg_ontology_rollup_b039672993_pk_hi; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_ontology_rollup_b039672993_pk_hi ON public._chronos_b_interval_okg_ontology_rollup_b039672993 USING btree (src_subtype, edge_type, dst_subtype, producer, live_hi);


--
-- Name: idx__chronos_b_interval_okg_ontology_rollup_b039672993_writer_s; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx__chronos_b_interval_okg_ontology_rollup_b039672993_writer_s ON public._chronos_b_interval_okg_ontology_rollup_b039672993 USING btree (writer_segment_id, src_subtype, edge_type, dst_subtype, producer);


--
-- Name: idx_okg_chronos_01b7ff4c2e03_edge_identity_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_01b7ff4c2e03_edge_identity_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (src, edge_type, live_lo, live_hi, dst) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_206495205bf1_node_live_lo_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_206495205bf1_node_live_lo_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (live_lo) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_33eed6294b5a_node_source_node_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_33eed6294b5a_node_source_node_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (source, node_id, live_lo, live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_412e64b30a6e_node_subtype_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_412e64b30a6e_node_subtype_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (subtype, live_lo, live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_4cd7cf8b5a08_edge_live_lo_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_4cd7cf8b5a08_edge_live_lo_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (live_lo) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_5009a5b8c8ee_node_id_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_5009a5b8c8ee_node_id_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (node_id, live_lo, live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_5ac0d478aec1_edge_source_run_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_5ac0d478aec1_edge_source_run_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (source, run_id, live_lo, live_hi, src, edge_type, dst) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_5dfd7248f35a_edge_delta_segment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_5dfd7248f35a_edge_delta_segment ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (live_lo, live_hi, edge_id, deleted);


--
-- Name: idx_okg_chronos_74f601774964_edge_live_hi_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_74f601774964_edge_live_hi_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_77b9470dea8a_edge_source_order_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_77b9470dea8a_edge_source_order_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (source, src, edge_type, dst, live_lo, live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_88cbaa229e5c_node_live_hi_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_88cbaa229e5c_node_live_hi_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_8bf435d3fd64_edge_source_edgeid_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_8bf435d3fd64_edge_source_edgeid_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (source, edge_id, live_lo, live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_8fc1cb84e69b_node_text_bm25_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_8fc1cb84e69b_node_text_bm25_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING bm25 (((attrs ->> 'text'::text))) WITH (text_config=english) WHERE ((deleted = false) AND (attrs ? 'text'::text));


--
-- Name: idx_okg_chronos_a4d0df2a82f6_edge_id_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_a4d0df2a82f6_edge_id_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (edge_id, live_lo, live_hi) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_beec741f2b87_node_source_run_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_beec741f2b87_node_source_run_visible ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (source, run_id, live_lo, live_hi, node_id) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_c74ad8350dd6_edge_dst_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_c74ad8350dd6_edge_dst_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (dst, live_lo, live_hi, edge_id) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_ca34590ce629_edge_source_dst_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_ca34590ce629_edge_source_dst_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (source, dst, live_lo, live_hi, edge_id) WHERE (deleted = false);


--
-- Name: idx_okg_chronos_d95ec590a9f2_node_delta_segment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_d95ec590a9f2_node_delta_segment ON public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f USING btree (live_lo, live_hi, node_id, deleted);


--
-- Name: idx_okg_chronos_e8999133f528_edge_src_visible; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_okg_chronos_e8999133f528_edge_src_visible ON public._chronos_b_interval_okg_graph_edges_d84ac83c2b USING btree (src, live_lo, live_hi, edge_id) WHERE (deleted = false);


--
-- Name: attribute_effective_view_intervals attribute_integrity_dirty_truncate_3b377cd88c; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_dirty_truncate_3b377cd88c AFTER TRUNCATE ON okg.attribute_effective_view_intervals FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty();


--
-- Name: attribute_effective_view attribute_integrity_dirty_truncate_db2b19c366; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_dirty_truncate_db2b19c366 AFTER TRUNCATE ON okg.attribute_effective_view FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty();


--
-- Name: attribute_effective_integrity_receipts attribute_integrity_receipts_immutable; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_receipts_immutable BEFORE DELETE OR UPDATE OR TRUNCATE ON okg.attribute_effective_integrity_receipts FOR EACH STATEMENT EXECUTE FUNCTION okg.reject_attribute_integrity_receipt_mutation();


--
-- Name: attribute_effective_view_intervals attribute_integrity_scoped_delete_3b377cd88c; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_scoped_delete_3b377cd88c AFTER DELETE ON okg.attribute_effective_view_intervals REFERENCING OLD TABLE AS changed_rows FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows();


--
-- Name: attribute_effective_view attribute_integrity_scoped_delete_db2b19c366; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_scoped_delete_db2b19c366 AFTER DELETE ON okg.attribute_effective_view REFERENCING OLD TABLE AS changed_rows FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows();


--
-- Name: attribute_effective_view_intervals attribute_integrity_scoped_insert_3b377cd88c; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_scoped_insert_3b377cd88c AFTER INSERT ON okg.attribute_effective_view_intervals REFERENCING NEW TABLE AS changed_rows FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows();


--
-- Name: attribute_effective_view attribute_integrity_scoped_insert_db2b19c366; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_scoped_insert_db2b19c366 AFTER INSERT ON okg.attribute_effective_view REFERENCING NEW TABLE AS changed_rows FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows();


--
-- Name: attribute_effective_view_intervals attribute_integrity_scoped_update_3b377cd88c; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_scoped_update_3b377cd88c AFTER UPDATE ON okg.attribute_effective_view_intervals REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty_update();


--
-- Name: attribute_effective_view attribute_integrity_scoped_update_db2b19c366; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER attribute_integrity_scoped_update_db2b19c366 AFTER UPDATE ON okg.attribute_effective_view REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty_update();


--
-- Name: edge_facts enforce_commit_witness; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER enforce_commit_witness AFTER INSERT ON okg.edge_facts REFERENCING NEW TABLE AS newrows FOR EACH STATEMENT EXECUTE FUNCTION okg.enforce_commit_witness();


--
-- Name: node_facts enforce_commit_witness; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER enforce_commit_witness AFTER INSERT ON okg.node_facts REFERENCING NEW TABLE AS newrows FOR EACH STATEMENT EXECUTE FUNCTION okg.enforce_commit_witness();


--
-- Name: graph_generations graph_generations_fill_branch_id_tg; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER graph_generations_fill_branch_id_tg BEFORE INSERT OR UPDATE OF deployment_name, branch_id ON okg.graph_generations FOR EACH ROW EXECUTE FUNCTION okg._fill_default_branch_id();


--
-- Name: history_generation_pins history_generation_pins_coordination; Type: TRIGGER; Schema: okg; Owner: -
--

CREATE TRIGGER history_generation_pins_coordination BEFORE INSERT OR DELETE OR UPDATE ON okg.history_generation_pins FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection();


--
-- Name: _chronos_branch_interval_branches chronos_branch_history_coordination; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER chronos_branch_history_coordination BEFORE INSERT OR DELETE OR UPDATE ON public._chronos_branch_interval_branches FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection();


--
-- Name: _chronos_branch_interval_checkpoints chronos_checkpoint_history_coordination; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER chronos_checkpoint_history_coordination BEFORE INSERT OR DELETE OR UPDATE ON public._chronos_branch_interval_checkpoints FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection();


--
-- Name: admission_envelopes admission_envelopes_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_envelopes
    ADD CONSTRAINT admission_envelopes_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: admission_events admission_events_admission_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_events
    ADD CONSTRAINT admission_events_admission_id_fkey FOREIGN KEY (admission_id) REFERENCES okg.admission_envelopes(admission_id) ON DELETE CASCADE;


--
-- Name: admission_record_refs admission_record_refs_admission_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.admission_record_refs
    ADD CONSTRAINT admission_record_refs_admission_id_fkey FOREIGN KEY (admission_id) REFERENCES okg.admission_envelopes(admission_id) ON DELETE CASCADE;


--
-- Name: alias_collisions alias_collisions_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.alias_collisions
    ADD CONSTRAINT alias_collisions_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: aliases aliases_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.aliases
    ADD CONSTRAINT aliases_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: attribute_effective_integrity_receipts attribute_effective_integrity_receipt_prior_receipt_digest_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.attribute_effective_integrity_receipts
    ADD CONSTRAINT attribute_effective_integrity_receipt_prior_receipt_digest_fkey FOREIGN KEY (prior_receipt_digest) REFERENCES okg.attribute_effective_integrity_receipts(receipt_digest);


--
-- Name: branch_merge_requests branch_merge_requests_source_branch_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branch_merge_requests
    ADD CONSTRAINT branch_merge_requests_source_branch_id_fkey FOREIGN KEY (source_branch_id) REFERENCES okg.branches(branch_id) ON DELETE CASCADE;


--
-- Name: branch_merge_requests branch_merge_requests_target_branch_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branch_merge_requests
    ADD CONSTRAINT branch_merge_requests_target_branch_id_fkey FOREIGN KEY (target_branch_id) REFERENCES okg.branches(branch_id) ON DELETE CASCADE;


--
-- Name: branches branches_base_branch_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.branches
    ADD CONSTRAINT branches_base_branch_id_fkey FOREIGN KEY (base_branch_id) REFERENCES okg.branches(branch_id) ON DELETE SET NULL;


--
-- Name: compacted_generations compacted_generations_plan_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.compacted_generations
    ADD CONSTRAINT compacted_generations_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES okg.history_compaction_plans(plan_id);


--
-- Name: consistency_candidate_coverage consistency_candidate_coverage_check_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_candidate_coverage
    ADD CONSTRAINT consistency_candidate_coverage_check_id_fkey FOREIGN KEY (check_id) REFERENCES okg.consistency_check_definitions(check_id);


--
-- Name: consistency_candidate_coverage consistency_candidate_coverage_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_candidate_coverage
    ADD CONSTRAINT consistency_candidate_coverage_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: consistency_candidates consistency_candidates_check_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_candidates
    ADD CONSTRAINT consistency_candidates_check_id_fkey FOREIGN KEY (check_id) REFERENCES okg.consistency_check_definitions(check_id);


--
-- Name: consistency_candidates consistency_candidates_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_candidates
    ADD CONSTRAINT consistency_candidates_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: consistency_check_definitions consistency_check_definitions_check_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_check_definitions
    ADD CONSTRAINT consistency_check_definitions_check_id_fkey FOREIGN KEY (check_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: consistency_gate_results consistency_gate_results_check_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_gate_results
    ADD CONSTRAINT consistency_gate_results_check_id_fkey FOREIGN KEY (check_id) REFERENCES okg.consistency_check_definitions(check_id);


--
-- Name: consistency_gate_results consistency_gate_results_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.consistency_gate_results
    ADD CONSTRAINT consistency_gate_results_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: derived_artifact_dependencies derived_artifact_dependencies_downstream_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_dependencies
    ADD CONSTRAINT derived_artifact_dependencies_downstream_definition_id_fkey FOREIGN KEY (downstream_definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_artifact_dependencies derived_artifact_dependencies_upstream_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_dependencies
    ADD CONSTRAINT derived_artifact_dependencies_upstream_definition_id_fkey FOREIGN KEY (upstream_definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_artifact_runs derived_artifact_runs_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_runs
    ADD CONSTRAINT derived_artifact_runs_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_artifact_runs derived_artifact_runs_reused_from_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_artifact_runs
    ADD CONSTRAINT derived_artifact_runs_reused_from_run_id_fkey FOREIGN KEY (reused_from_run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: derived_dirty_rule_audits derived_dirty_rule_audits_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_rule_audits
    ADD CONSTRAINT derived_dirty_rule_audits_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_dirty_rule_audits derived_dirty_rule_audits_dirty_rule_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_rule_audits
    ADD CONSTRAINT derived_dirty_rule_audits_dirty_rule_run_id_fkey FOREIGN KEY (dirty_rule_run_id) REFERENCES okg.derived_dirty_rule_runs(dirty_rule_run_id) ON DELETE CASCADE;


--
-- Name: derived_dirty_rule_runs derived_dirty_rule_runs_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_rule_runs
    ADD CONSTRAINT derived_dirty_rule_runs_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_dirty_scopes derived_dirty_scopes_consumed_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_scopes
    ADD CONSTRAINT derived_dirty_scopes_consumed_run_id_fkey FOREIGN KEY (consumed_run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: derived_dirty_scopes derived_dirty_scopes_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_scopes
    ADD CONSTRAINT derived_dirty_scopes_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_dirty_scopes derived_dirty_scopes_dirty_rule_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_dirty_scopes
    ADD CONSTRAINT derived_dirty_scopes_dirty_rule_run_id_fkey FOREIGN KEY (dirty_rule_run_id) REFERENCES okg.derived_dirty_rule_runs(dirty_rule_run_id);


--
-- Name: derived_output_dependency_index derived_output_dependency_index_lineage_envelope_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_dependency_index
    ADD CONSTRAINT derived_output_dependency_index_lineage_envelope_id_fkey FOREIGN KEY (lineage_envelope_id) REFERENCES okg.derived_output_lineage_envelopes(lineage_envelope_id) ON DELETE CASCADE;


--
-- Name: derived_output_dependency_index derived_output_dependency_index_manifest_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_dependency_index
    ADD CONSTRAINT derived_output_dependency_index_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES okg.derived_output_manifests(manifest_id) ON DELETE CASCADE;


--
-- Name: derived_output_lineage_envelopes derived_output_lineage_envelopes_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_lineage_envelopes
    ADD CONSTRAINT derived_output_lineage_envelopes_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_output_lineage_envelopes derived_output_lineage_envelopes_manifest_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_lineage_envelopes
    ADD CONSTRAINT derived_output_lineage_envelopes_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES okg.derived_output_manifests(manifest_id) ON DELETE CASCADE;


--
-- Name: derived_output_manifests derived_output_manifests_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_manifests
    ADD CONSTRAINT derived_output_manifests_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_output_manifests derived_output_manifests_reused_from_manifest_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_manifests
    ADD CONSTRAINT derived_output_manifests_reused_from_manifest_id_fkey FOREIGN KEY (reused_from_manifest_id) REFERENCES okg.derived_output_manifests(manifest_id);


--
-- Name: derived_output_manifests derived_output_manifests_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_manifests
    ADD CONSTRAINT derived_output_manifests_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: derived_output_slots derived_output_slots_manifest_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_output_slots
    ADD CONSTRAINT derived_output_slots_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES okg.derived_output_manifests(manifest_id) ON DELETE CASCADE;


--
-- Name: derived_relation_registry derived_relation_registry_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_relation_registry
    ADD CONSTRAINT derived_relation_registry_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_run_dependency_bindings derived_run_dependency_bindings_downstream_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_run_dependency_bindings
    ADD CONSTRAINT derived_run_dependency_bindings_downstream_definition_id_fkey FOREIGN KEY (downstream_definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_run_dependency_bindings derived_run_dependency_bindings_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_run_dependency_bindings
    ADD CONSTRAINT derived_run_dependency_bindings_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id) ON DELETE CASCADE;


--
-- Name: derived_run_dependency_bindings derived_run_dependency_bindings_upstream_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_run_dependency_bindings
    ADD CONSTRAINT derived_run_dependency_bindings_upstream_definition_id_fkey FOREIGN KEY (upstream_definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_run_dependency_bindings derived_run_dependency_bindings_upstream_manifest_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_run_dependency_bindings
    ADD CONSTRAINT derived_run_dependency_bindings_upstream_manifest_id_fkey FOREIGN KEY (upstream_manifest_id) REFERENCES okg.derived_output_manifests(manifest_id);


--
-- Name: derived_schedule_nodes derived_schedule_nodes_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_schedule_nodes
    ADD CONSTRAINT derived_schedule_nodes_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: derived_schedule_nodes derived_schedule_nodes_plan_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_schedule_nodes
    ADD CONSTRAINT derived_schedule_nodes_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES okg.derived_schedule_plans(plan_id) ON DELETE CASCADE;


--
-- Name: derived_schedule_nodes derived_schedule_nodes_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.derived_schedule_nodes
    ADD CONSTRAINT derived_schedule_nodes_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: fact_batch_receipts fact_batch_receipts_admission_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.fact_batch_receipts
    ADD CONSTRAINT fact_batch_receipts_admission_id_fkey FOREIGN KEY (admission_id) REFERENCES okg.admission_envelopes(admission_id) ON DELETE CASCADE;


--
-- Name: federation_connection_requests federation_connection_requests_nomos_decision_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_connection_requests
    ADD CONSTRAINT federation_connection_requests_nomos_decision_id_fkey FOREIGN KEY (nomos_decision_deployment_name, nomos_decision_id) REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT;


--
-- Name: federation_mailbox_events federation_mailbox_events_message_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_mailbox_events
    ADD CONSTRAINT federation_mailbox_events_message_id_fkey FOREIGN KEY (message_id) REFERENCES okg.federation_mailbox_messages(message_id) ON DELETE CASCADE;


--
-- Name: federation_peer_trust_pins federation_peer_trust_pins_nomos_decision_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_peer_trust_pins
    ADD CONSTRAINT federation_peer_trust_pins_nomos_decision_id_fkey FOREIGN KEY (nomos_decision_deployment_name, nomos_decision_id) REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT;


--
-- Name: federation_route_events federation_route_events_nomos_decision_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.federation_route_events
    ADD CONSTRAINT federation_route_events_nomos_decision_id_fkey FOREIGN KEY (nomos_decision_deployment_name, nomos_decision_id) REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT;


--
-- Name: graph_generations graph_generations_branch_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.graph_generations
    ADD CONSTRAINT graph_generations_branch_id_fkey FOREIGN KEY (branch_id) REFERENCES okg.branches(branch_id);


--
-- Name: graph_generations graph_generations_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.graph_generations
    ADD CONSTRAINT graph_generations_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: identifier_patterns identifier_patterns_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.identifier_patterns
    ADD CONSTRAINT identifier_patterns_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: invariants invariants_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.invariants
    ADD CONSTRAINT invariants_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: ir_invocations ir_invocations_binding_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_invocations
    ADD CONSTRAINT ir_invocations_binding_id_fkey FOREIGN KEY (binding_id) REFERENCES okg.ir_program_bindings(binding_id) ON DELETE SET NULL;


--
-- Name: ir_invocations ir_invocations_program_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_invocations
    ADD CONSTRAINT ir_invocations_program_id_fkey FOREIGN KEY (program_id) REFERENCES okg.ir_programs(program_id) ON DELETE CASCADE;


--
-- Name: ir_program_bindings ir_program_bindings_program_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.ir_program_bindings
    ADD CONSTRAINT ir_program_bindings_program_id_fkey FOREIGN KEY (program_id) REFERENCES okg.ir_programs(program_id) ON DELETE CASCADE;


--
-- Name: lens_definitions lens_definitions_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_definitions
    ADD CONSTRAINT lens_definitions_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: lens_definitions lens_definitions_view_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_definitions
    ADD CONSTRAINT lens_definitions_view_definition_id_fkey FOREIGN KEY (view_definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: lens_overlay_log lens_overlay_log_delta_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_delta_id_fkey FOREIGN KEY (delta_id) REFERENCES okg.lens_putback_deltas(delta_id);


--
-- Name: lens_overlay_log lens_overlay_log_lens_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_lens_definition_id_fkey FOREIGN KEY (lens_definition_id) REFERENCES okg.lens_definitions(definition_id);


--
-- Name: lens_overlay_log lens_overlay_log_promoted_admission_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_promoted_admission_id_fkey FOREIGN KEY (promoted_admission_id) REFERENCES okg.admission_envelopes(admission_id);


--
-- Name: lens_overlay_log lens_overlay_log_request_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_request_id_fkey FOREIGN KEY (request_id) REFERENCES okg.lens_putback_requests(request_id);


--
-- Name: lens_putback_deltas lens_putback_deltas_request_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_deltas
    ADD CONSTRAINT lens_putback_deltas_request_id_fkey FOREIGN KEY (request_id) REFERENCES okg.lens_putback_requests(request_id) ON DELETE CASCADE;


--
-- Name: lens_putback_requests lens_putback_requests_base_view_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_requests
    ADD CONSTRAINT lens_putback_requests_base_view_definition_id_fkey FOREIGN KEY (base_view_definition_id) REFERENCES okg.derived_artifact_definitions(definition_id);


--
-- Name: lens_putback_requests lens_putback_requests_base_view_manifest_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_requests
    ADD CONSTRAINT lens_putback_requests_base_view_manifest_id_fkey FOREIGN KEY (base_view_manifest_id) REFERENCES okg.derived_output_manifests(manifest_id);


--
-- Name: lens_putback_requests lens_putback_requests_lens_definition_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.lens_putback_requests
    ADD CONSTRAINT lens_putback_requests_lens_definition_id_fkey FOREIGN KEY (lens_definition_id) REFERENCES okg.lens_definitions(definition_id);


--
-- Name: narrowings narrowings_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.narrowings
    ADD CONSTRAINT narrowings_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: nomos_config_activations nomos_config_activations_deployment_name_config_fingerprin_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_config_activations
    ADD CONSTRAINT nomos_config_activations_deployment_name_config_fingerprin_fkey FOREIGN KEY (deployment_name, config_fingerprint) REFERENCES okg.nomos_config_sets(deployment_name, config_fingerprint) ON DELETE RESTRICT;


--
-- Name: nomos_external_claims nomos_external_claims_nomos_decision_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_external_claims
    ADD CONSTRAINT nomos_external_claims_nomos_decision_id_fkey FOREIGN KEY (deployment_name, nomos_decision_id) REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT;


--
-- Name: nomos_policy_activations nomos_policy_activations_deployment_name_policy_fingerprin_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.nomos_policy_activations
    ADD CONSTRAINT nomos_policy_activations_deployment_name_policy_fingerprin_fkey FOREIGN KEY (deployment_name, policy_fingerprint) REFERENCES okg.nomos_policy_bundles(deployment_name, policy_fingerprint) ON DELETE RESTRICT;


--
-- Name: observation_current observation_current_check_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_current
    ADD CONSTRAINT observation_current_check_id_fkey FOREIGN KEY (check_id) REFERENCES okg.consistency_check_definitions(check_id);


--
-- Name: observation_current observation_current_latest_event_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_current
    ADD CONSTRAINT observation_current_latest_event_id_fkey FOREIGN KEY (latest_event_id) REFERENCES okg.observation_events(event_id);


--
-- Name: observation_current observation_current_latest_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_current
    ADD CONSTRAINT observation_current_latest_run_id_fkey FOREIGN KEY (latest_run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: observation_events observation_events_check_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_events
    ADD CONSTRAINT observation_events_check_id_fkey FOREIGN KEY (check_id) REFERENCES okg.consistency_check_definitions(check_id);


--
-- Name: observation_events observation_events_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.observation_events
    ADD CONSTRAINT observation_events_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.derived_artifact_runs(run_id);


--
-- Name: poros_delivery_attempts poros_delivery_attempts_message_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_delivery_attempts
    ADD CONSTRAINT poros_delivery_attempts_message_id_fkey FOREIGN KEY (message_id) REFERENCES okg.poros_outbox_messages(message_id) ON DELETE CASCADE;


--
-- Name: poros_incorporation_events poros_incorporation_events_admission_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_incorporation_events
    ADD CONSTRAINT poros_incorporation_events_admission_id_fkey FOREIGN KEY (admission_id) REFERENCES okg.admission_envelopes(admission_id) ON DELETE SET NULL;


--
-- Name: poros_incorporation_events poros_incorporation_events_nomos_decision_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.poros_incorporation_events
    ADD CONSTRAINT poros_incorporation_events_nomos_decision_id_fkey FOREIGN KEY (nomos_decision_deployment_name, nomos_decision_id) REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT;


--
-- Name: producer_profiles producer_profiles_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.producer_profiles
    ADD CONSTRAINT producer_profiles_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: projection_remediation_batches projection_remediation_batches_run_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.projection_remediation_batches
    ADD CONSTRAINT projection_remediation_batches_run_id_fkey FOREIGN KEY (run_id) REFERENCES okg.projection_remediation_runs(run_id);


--
-- Name: subtype_ancestors subtype_ancestors_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.subtype_ancestors
    ADD CONSTRAINT subtype_ancestors_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: subtypes subtypes_catalog_version_id_fkey; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.subtypes
    ADD CONSTRAINT subtypes_catalog_version_id_fkey FOREIGN KEY (catalog_version_id) REFERENCES okg.catalog_versions(catalog_version_id);


--
-- Name: subtypes subtypes_parent_fk; Type: FK CONSTRAINT; Schema: okg; Owner: -
--

ALTER TABLE ONLY okg.subtypes
    ADD CONSTRAINT subtypes_parent_fk FOREIGN KEY (parent_subtype, catalog_version_id) REFERENCES okg.subtypes(name, catalog_version_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA public TO okg_mcp;


--
-- Name: SCHEMA okg; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA okg TO app_rw;
GRANT USAGE ON SCHEMA okg TO catalog_admin;
GRANT USAGE ON SCHEMA okg TO app_ro;
GRANT USAGE ON SCHEMA okg TO okg_mcp;


--
-- Name: SCHEMA okg_derived; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA okg_derived TO app_rw;


--
-- Name: FUNCTION _fill_default_branch_id(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg._fill_default_branch_id() TO okg_mcp;


--
-- Name: FUNCTION _stamp_searchable_text(p_subtype text, p_attrs jsonb, p_cv bigint); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg._stamp_searchable_text(p_subtype text, p_attrs jsonb, p_cv bigint) TO okg_mcp;


--
-- Name: FUNCTION _stamp_semantic_timestamps(p_subtype text, p_attrs jsonb, p_cv bigint); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg._stamp_semantic_timestamps(p_subtype text, p_attrs jsonb, p_cv bigint) TO okg_mcp;


--
-- Name: FUNCTION abort_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.abort_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.abort_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) TO okg_substrate;


--
-- Name: FUNCTION acquire_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text, p_checkpoint_id text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.acquire_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text, p_checkpoint_id text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.acquire_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text, p_checkpoint_id text) TO app_rw;
GRANT ALL ON FUNCTION okg.acquire_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text, p_checkpoint_id text) TO app_ro;
GRANT ALL ON FUNCTION okg.acquire_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text, p_checkpoint_id text) TO okg_mcp;


--
-- Name: FUNCTION attribute_integrity_transition_matches(p_deployment text, p_branch text, p_namespace text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.attribute_integrity_transition_matches(p_deployment text, p_branch text, p_namespace text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.attribute_integrity_transition_matches(p_deployment text, p_branch text, p_namespace text) TO okg_substrate;


--
-- Name: FUNCTION begin_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.begin_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.begin_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) TO okg_substrate;


--
-- Name: FUNCTION compacted_generation_record(p_generation text, p_deployment_name text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.compacted_generation_record(p_generation text, p_deployment_name text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.compacted_generation_record(p_generation text, p_deployment_name text) TO app_rw;
GRANT ALL ON FUNCTION okg.compacted_generation_record(p_generation text, p_deployment_name text) TO app_ro;
GRANT ALL ON FUNCTION okg.compacted_generation_record(p_generation text, p_deployment_name text) TO okg_mcp;


--
-- Name: FUNCTION compute_pagerank(edges_sql text, damping double precision, max_iter integer, tol double precision); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.compute_pagerank(edges_sql text, damping double precision, max_iter integer, tol double precision) TO app_rw;
GRANT ALL ON FUNCTION okg.compute_pagerank(edges_sql text, damping double precision, max_iter integer, tol double precision) TO okg_mcp;


--
-- Name: FUNCTION current_pin(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.current_pin() TO app_ro;
GRANT ALL ON FUNCTION okg.current_pin() TO okg_mcp;


--
-- Name: FUNCTION enforce_commit_witness(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.enforce_commit_witness() TO okg_mcp;


--
-- Name: FUNCTION finalize_attribute_integrity_receipt(p_receipt_digest text, p_receipt_type text, p_deployment text, p_branch text, p_namespace text, p_generation_id text, p_ordinal bigint, p_prior_receipt_digest text, p_checkpoint_node_count bigint, p_checkpoint_edge_count bigint, p_logical_node_count bigint, p_logical_edge_count bigint, p_schema_version integer, p_schema_digest text, p_migration_digest text, p_logical_payload_seed0 numeric, p_logical_payload_seed1 numeric, p_interval_payload_seed0 numeric, p_interval_payload_seed1 numeric, p_dirty_scope_digest text, p_mutation_epoch bigint); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.finalize_attribute_integrity_receipt(p_receipt_digest text, p_receipt_type text, p_deployment text, p_branch text, p_namespace text, p_generation_id text, p_ordinal bigint, p_prior_receipt_digest text, p_checkpoint_node_count bigint, p_checkpoint_edge_count bigint, p_logical_node_count bigint, p_logical_edge_count bigint, p_schema_version integer, p_schema_digest text, p_migration_digest text, p_logical_payload_seed0 numeric, p_logical_payload_seed1 numeric, p_interval_payload_seed0 numeric, p_interval_payload_seed1 numeric, p_dirty_scope_digest text, p_mutation_epoch bigint) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.finalize_attribute_integrity_receipt(p_receipt_digest text, p_receipt_type text, p_deployment text, p_branch text, p_namespace text, p_generation_id text, p_ordinal bigint, p_prior_receipt_digest text, p_checkpoint_node_count bigint, p_checkpoint_edge_count bigint, p_logical_node_count bigint, p_logical_edge_count bigint, p_schema_version integer, p_schema_digest text, p_migration_digest text, p_logical_payload_seed0 numeric, p_logical_payload_seed1 numeric, p_interval_payload_seed0 numeric, p_interval_payload_seed1 numeric, p_dirty_scope_digest text, p_mutation_epoch bigint) TO okg_substrate;


--
-- Name: FUNCTION find_identifier_mentions(p_text text, p_cv bigint); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.find_identifier_mentions(p_text text, p_cv bigint) TO app_rw;
GRANT ALL ON FUNCTION okg.find_identifier_mentions(p_text text, p_cv bigint) TO okg_mcp;


--
-- Name: FUNCTION finish_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.finish_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.finish_attribute_integrity_transition(p_deployment text, p_branch text, p_namespace text) TO okg_substrate;


--
-- Name: FUNCTION fuzzy_resolve_alias(p_needle text, p_cv bigint, p_threshold real, p_limit integer); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.fuzzy_resolve_alias(p_needle text, p_cv bigint, p_threshold real, p_limit integer) TO app_rw;
GRANT ALL ON FUNCTION okg.fuzzy_resolve_alias(p_needle text, p_cv bigint, p_threshold real, p_limit integer) TO okg_mcp;


--
-- Name: FUNCTION lock_history_compaction_protection(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.lock_history_compaction_protection() TO okg_mcp;


--
-- Name: FUNCTION mark_attribute_effective_integrity_dirty(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty() TO okg_mcp;


--
-- Name: FUNCTION mark_attribute_effective_integrity_dirty_rows(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_rows() TO okg_mcp;


--
-- Name: FUNCTION mark_attribute_effective_integrity_dirty_update(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_update() TO okg_mcp;


--
-- Name: FUNCTION mark_attribute_integrity_scope_dirty(p_deployment text, p_branch text, p_namespace text, p_reason text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.mark_attribute_integrity_scope_dirty(p_deployment text, p_branch text, p_namespace text, p_reason text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.mark_attribute_integrity_scope_dirty(p_deployment text, p_branch text, p_namespace text, p_reason text) TO okg_substrate;


--
-- Name: FUNCTION refresh_pagerank(p_scope text, edges_sql text, damping double precision, max_iter integer, tol double precision); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.refresh_pagerank(p_scope text, edges_sql text, damping double precision, max_iter integer, tol double precision) TO app_rw;
GRANT ALL ON FUNCTION okg.refresh_pagerank(p_scope text, edges_sql text, damping double precision, max_iter integer, tol double precision) TO okg_mcp;


--
-- Name: FUNCTION register_attribute_integrity_scope(p_deployment text, p_branch text, p_namespace text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.register_attribute_integrity_scope(p_deployment text, p_branch text, p_namespace text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.register_attribute_integrity_scope(p_deployment text, p_branch text, p_namespace text) TO okg_substrate;


--
-- Name: FUNCTION reject_attribute_integrity_receipt_mutation(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.reject_attribute_integrity_receipt_mutation() TO okg_mcp;


--
-- Name: FUNCTION release_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text); Type: ACL; Schema: okg; Owner: -
--

REVOKE ALL ON FUNCTION okg.release_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text) FROM PUBLIC;
GRANT ALL ON FUNCTION okg.release_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text) TO app_rw;
GRANT ALL ON FUNCTION okg.release_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text) TO app_ro;
GRANT ALL ON FUNCTION okg.release_history_generation_read_lease(p_deployment_name text, p_branch_id text, p_pin_name text) TO okg_mcp;


--
-- Name: FUNCTION require_chronos_checkout(kept_table text); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.require_chronos_checkout(kept_table text) TO okg_mcp;


--
-- Name: FUNCTION require_graph_checkout(graph_table text); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.require_graph_checkout(graph_table text) TO okg_mcp;


--
-- Name: FUNCTION runtime_queue_health(p_stale_after_seconds integer); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.runtime_queue_health(p_stale_after_seconds integer) TO app_ro;
GRANT ALL ON FUNCTION okg.runtime_queue_health(p_stale_after_seconds integer) TO okg_mcp;


--
-- Name: FUNCTION source_frontier_active_workflows(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.source_frontier_active_workflows() TO app_ro;
GRANT ALL ON FUNCTION okg.source_frontier_active_workflows() TO okg_mcp;


--
-- Name: FUNCTION source_frontier_state(); Type: ACL; Schema: okg; Owner: -
--

GRANT ALL ON FUNCTION okg.source_frontier_state() TO app_ro;
GRANT ALL ON FUNCTION okg.source_frontier_state() TO okg_mcp;


--
-- Name: TABLE _compressed_hypertable_10; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE _timescaledb_internal._compressed_hypertable_10 TO app_rw;
GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_10 TO okg_mcp;


--
-- Name: TABLE _compressed_hypertable_12; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE _timescaledb_internal._compressed_hypertable_12 TO app_rw;
GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_12 TO okg_mcp;


--
-- Name: TABLE _compressed_hypertable_14; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_14 TO app_rw;


--
-- Name: TABLE _compressed_hypertable_16; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_16 TO app_rw;
GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_16 TO okg_mcp;


--
-- Name: TABLE _compressed_hypertable_3; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE _timescaledb_internal._compressed_hypertable_3 TO app_rw;


--
-- Name: TABLE _compressed_hypertable_4; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE _timescaledb_internal._compressed_hypertable_4 TO app_rw;


--
-- Name: TABLE _compressed_hypertable_6; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE _timescaledb_internal._compressed_hypertable_6 TO app_rw;
GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_6 TO okg_mcp;


--
-- Name: TABLE _compressed_hypertable_8; Type: ACL; Schema: _timescaledb_internal; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE _timescaledb_internal._compressed_hypertable_8 TO app_rw;
GRANT SELECT,INSERT ON TABLE _timescaledb_internal._compressed_hypertable_8 TO okg_mcp;


--
-- Name: TABLE admission_envelopes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.admission_envelopes TO app_rw;


--
-- Name: TABLE admission_events; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.admission_events TO app_rw;


--
-- Name: SEQUENCE admission_events_event_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.admission_events_event_id_seq TO app_rw;


--
-- Name: TABLE admission_record_refs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.admission_record_refs TO app_rw;


--
-- Name: SEQUENCE admission_record_refs_ref_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.admission_record_refs_ref_id_seq TO app_rw;


--
-- Name: TABLE alias_collisions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.alias_collisions TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.alias_collisions TO catalog_admin;


--
-- Name: TABLE aliases; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.aliases TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.aliases TO catalog_admin;
GRANT SELECT ON TABLE okg.aliases TO okg_mcp;


--
-- Name: TABLE attribute_effective_attrs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.attribute_effective_attrs TO app_rw;
GRANT SELECT ON TABLE okg.attribute_effective_attrs TO okg_mcp;


--
-- Name: TABLE attribute_effective_integrity_receipts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.attribute_effective_integrity_receipts TO app_rw;
GRANT SELECT ON TABLE okg.attribute_effective_integrity_receipts TO okg_substrate;
GRANT SELECT ON TABLE okg.attribute_effective_integrity_receipts TO okg_mcp;


--
-- Name: TABLE attribute_effective_integrity_state; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.attribute_effective_integrity_state TO app_rw;
GRANT SELECT ON TABLE okg.attribute_effective_integrity_state TO okg_substrate;
GRANT SELECT ON TABLE okg.attribute_effective_integrity_state TO okg_mcp;


--
-- Name: TABLE attribute_effective_view; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE ON TABLE okg.attribute_effective_view TO app_rw;
GRANT SELECT ON TABLE okg.attribute_effective_view TO okg_mcp;


--
-- Name: TABLE attribute_effective_view_intervals; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.attribute_effective_view_intervals TO okg_mcp;


--
-- Name: TABLE attribute_observations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.attribute_observations TO app_rw;
GRANT SELECT ON TABLE okg.attribute_observations TO okg_mcp;


--
-- Name: SEQUENCE branch_merge_requests_request_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.branch_merge_requests_request_id_seq TO app_rw;


--
-- Name: SEQUENCE branches_branch_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.branches_branch_id_seq TO app_rw;


--
-- Name: TABLE catalog_versions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.catalog_versions TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.catalog_versions TO catalog_admin;
GRANT SELECT ON TABLE okg.catalog_versions TO okg_mcp;


--
-- Name: SEQUENCE catalog_versions_catalog_version_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.catalog_versions_catalog_version_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.catalog_versions_catalog_version_id_seq TO catalog_admin;


--
-- Name: TABLE compacted_generations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.compacted_generations TO app_rw;


--
-- Name: TABLE consistency_candidate_coverage; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.consistency_candidate_coverage TO app_rw;


--
-- Name: TABLE consistency_candidates; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.consistency_candidates TO app_rw;


--
-- Name: TABLE consistency_check_definitions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.consistency_check_definitions TO app_rw;


--
-- Name: TABLE consistency_gate_results; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.consistency_gate_results TO app_rw;


--
-- Name: TABLE derived_artifact_definitions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_artifact_definitions TO app_rw;


--
-- Name: TABLE derived_artifact_dependencies; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_artifact_dependencies TO app_rw;


--
-- Name: TABLE derived_artifact_runs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_artifact_runs TO app_rw;


--
-- Name: TABLE derived_dirty_rule_audits; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_dirty_rule_audits TO app_rw;


--
-- Name: TABLE derived_dirty_rule_runs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_dirty_rule_runs TO app_rw;


--
-- Name: TABLE derived_dirty_scopes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_dirty_scopes TO app_rw;


--
-- Name: SEQUENCE derived_dirty_scopes_dirty_scope_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.derived_dirty_scopes_dirty_scope_id_seq TO app_rw;


--
-- Name: TABLE derived_output_dependency_index; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_output_dependency_index TO app_rw;


--
-- Name: SEQUENCE derived_output_dependency_index_dependency_index_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.derived_output_dependency_index_dependency_index_id_seq TO app_rw;


--
-- Name: TABLE derived_output_lineage_envelopes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_output_lineage_envelopes TO app_rw;


--
-- Name: TABLE derived_output_manifests; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_output_manifests TO app_rw;


--
-- Name: TABLE derived_output_slots; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_output_slots TO app_rw;


--
-- Name: TABLE derived_relation_registry; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_relation_registry TO app_rw;


--
-- Name: TABLE derived_run_dependency_bindings; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_run_dependency_bindings TO app_rw;


--
-- Name: TABLE derived_schedule_nodes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_schedule_nodes TO app_rw;


--
-- Name: TABLE derived_schedule_plans; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.derived_schedule_plans TO app_rw;


--
-- Name: TABLE edge_facts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.edge_facts TO app_rw;


--
-- Name: SEQUENCE edge_facts_edge_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.edge_facts_edge_id_seq TO app_rw;


--
-- Name: SEQUENCE edge_facts_fact_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.edge_facts_fact_id_seq TO app_rw;


--
-- Name: TABLE enricher_backfill_progress; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.enricher_backfill_progress TO app_rw;


--
-- Name: TABLE enricher_progress; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.enricher_progress TO app_rw;


--
-- Name: SEQUENCE entity_resolution_rejections_rejection_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.entity_resolution_rejections_rejection_id_seq TO app_rw;


--
-- Name: TABLE envelope_violations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.envelope_violations TO app_rw;


--
-- Name: TABLE equivalence_verdicts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE ON TABLE okg.equivalence_verdicts TO app_rw;


--
-- Name: TABLE external_probe_cache; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.external_probe_cache TO app_rw;


--
-- Name: TABLE extractor_retry_queue; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.extractor_retry_queue TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.extractor_retry_queue TO catalog_admin;


--
-- Name: TABLE fact_batch_receipts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.fact_batch_receipts TO app_rw;


--
-- Name: SEQUENCE federation_mailbox_events_event_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.federation_mailbox_events_event_id_seq TO app_rw;


--
-- Name: TABLE generation_ordinals; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.generation_ordinals TO okg_mcp;


--
-- Name: TABLE graph_edges; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.graph_edges TO projection_remediator;
GRANT SELECT ON TABLE okg.graph_edges TO okg_mcp;


--
-- Name: TABLE graph_generations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.graph_generations TO app_rw;
GRANT SELECT ON TABLE okg.graph_generations TO okg_mcp;


--
-- Name: SEQUENCE graph_generations_generation_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.graph_generations_generation_id_seq TO app_rw;


--
-- Name: TABLE graph_nodes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.graph_nodes TO projection_remediator;
GRANT SELECT ON TABLE okg.graph_nodes TO okg_mcp;


--
-- Name: TABLE history_compaction_plans; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.history_compaction_plans TO app_rw;


--
-- Name: TABLE history_generation_pins; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.history_generation_pins TO app_rw;


--
-- Name: TABLE identifier_patterns; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.identifier_patterns TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.identifier_patterns TO catalog_admin;
GRANT SELECT ON TABLE okg.identifier_patterns TO okg_mcp;


--
-- Name: TABLE invariants; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.invariants TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.invariants TO catalog_admin;


--
-- Name: TABLE ir_invocations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.ir_invocations TO app_rw;


--
-- Name: TABLE ir_program_bindings; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.ir_program_bindings TO app_rw;


--
-- Name: TABLE ir_programs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.ir_programs TO app_rw;


--
-- Name: SEQUENCE lease_audit_audit_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.lease_audit_audit_id_seq TO app_rw;


--
-- Name: TABLE link_cache; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.link_cache TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.link_cache TO catalog_admin;


--
-- Name: TABLE link_retry; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.link_retry TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.link_retry TO catalog_admin;


--
-- Name: TABLE llm_calls; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.llm_calls TO app_rw;


--
-- Name: TABLE llm_call_stats; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.llm_call_stats TO app_rw;


--
-- Name: TABLE mcp_call_log; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.mcp_call_log TO app_rw;
GRANT SELECT,INSERT ON TABLE okg.mcp_call_log TO okg_mcp;


--
-- Name: SEQUENCE mcp_call_log_call_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.mcp_call_log_call_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.mcp_call_log_call_id_seq TO okg_mcp;


--
-- Name: TABLE mcp_trace_events; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.mcp_trace_events TO app_rw;
GRANT SELECT,INSERT ON TABLE okg.mcp_trace_events TO okg_mcp;


--
-- Name: SEQUENCE mcp_trace_events_event_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.mcp_trace_events_event_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.mcp_trace_events_event_id_seq TO okg_mcp;


--
-- Name: TABLE model_outcomes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE ON TABLE okg.model_outcomes TO app_rw;


--
-- Name: TABLE narrowings; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.narrowings TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.narrowings TO catalog_admin;
GRANT SELECT ON TABLE okg.narrowings TO okg_mcp;


--
-- Name: TABLE node_facts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.node_facts TO app_rw;


--
-- Name: SEQUENCE node_facts_fact_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.node_facts_fact_id_seq TO app_rw;


--
-- Name: TABLE node_series; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.node_series TO app_rw;


--
-- Name: SEQUENCE node_series_sample_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.node_series_sample_id_seq TO app_rw;


--
-- Name: TABLE node_series_watermark; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.node_series_watermark TO app_rw;


--
-- Name: SEQUENCE nomos_config_activations_activation_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_config_activations_activation_row_id_seq TO app_rw;


--
-- Name: SEQUENCE nomos_config_sets_config_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_config_sets_config_row_id_seq TO app_rw;


--
-- Name: TABLE nomos_decisions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_decisions TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_decisions TO okg_mcp;


--
-- Name: SEQUENCE nomos_decisions_decision_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_decisions_decision_row_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.nomos_decisions_decision_row_id_seq TO okg_mcp;


--
-- Name: TABLE nomos_external_claims; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_external_claims TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_external_claims TO okg_mcp;


--
-- Name: SEQUENCE nomos_external_claims_claim_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_external_claims_claim_row_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.nomos_external_claims_claim_row_id_seq TO okg_mcp;


--
-- Name: TABLE nomos_identity_providers; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_identity_providers TO app_rw;
GRANT SELECT ON TABLE okg.nomos_identity_providers TO okg_mcp;


--
-- Name: SEQUENCE nomos_identity_providers_provider_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_identity_providers_provider_row_id_seq TO app_rw;


--
-- Name: TABLE nomos_permission_grants; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_permission_grants TO app_rw;
GRANT SELECT ON TABLE okg.nomos_permission_grants TO okg_mcp;


--
-- Name: SEQUENCE nomos_permission_grants_grant_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_permission_grants_grant_row_id_seq TO app_rw;


--
-- Name: TABLE nomos_policy_activations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.nomos_policy_activations TO okg_mcp;


--
-- Name: SEQUENCE nomos_policy_activations_activation_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_policy_activations_activation_row_id_seq TO app_rw;


--
-- Name: TABLE nomos_policy_bundles; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.nomos_policy_bundles TO okg_mcp;


--
-- Name: SEQUENCE nomos_policy_bundles_bundle_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_policy_bundles_bundle_row_id_seq TO app_rw;


--
-- Name: TABLE nomos_subject_bindings; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.nomos_subject_bindings TO app_rw;
GRANT SELECT ON TABLE okg.nomos_subject_bindings TO okg_mcp;


--
-- Name: SEQUENCE nomos_subject_bindings_binding_row_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.nomos_subject_bindings_binding_row_id_seq TO app_rw;


--
-- Name: TABLE observation_current; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.observation_current TO app_rw;


--
-- Name: TABLE observation_events; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.observation_events TO app_rw;


--
-- Name: SEQUENCE observation_events_event_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.observation_events_event_id_seq TO app_rw;


--
-- Name: SEQUENCE poros_delivery_attempts_attempt_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.poros_delivery_attempts_attempt_id_seq TO app_rw;


--
-- Name: SEQUENCE poros_detached_object_manifests_manifest_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.poros_detached_object_manifests_manifest_id_seq TO app_rw;


--
-- Name: SEQUENCE poros_inbox_messages_inbox_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.poros_inbox_messages_inbox_id_seq TO app_rw;


--
-- Name: SEQUENCE poros_incorporation_events_event_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.poros_incorporation_events_event_id_seq TO app_rw;


--
-- Name: TABLE producer_profiles; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.producer_profiles TO app_rw;
GRANT SELECT ON TABLE okg.producer_profiles TO okg_mcp;


--
-- Name: SEQUENCE producer_profiles_profile_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.producer_profiles_profile_id_seq TO app_rw;


--
-- Name: TABLE projectable_edge_facts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.projectable_edge_facts TO app_rw;


--
-- Name: TABLE projectable_node_facts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.projectable_node_facts TO app_rw;


--
-- Name: SEQUENCE projection_pending_pending_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.projection_pending_pending_id_seq TO app_rw;


--
-- Name: TABLE projection_remediation_batches; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.projection_remediation_batches TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.projection_remediation_batches TO catalog_admin;


--
-- Name: TABLE projection_remediation_runs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.projection_remediation_runs TO app_rw;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.projection_remediation_runs TO catalog_admin;


--
-- Name: TABLE repair_selections; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE ON TABLE okg.repair_selections TO app_rw;


--
-- Name: TABLE reuse_certificates; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE ON TABLE okg.reuse_certificates TO app_rw;


--
-- Name: TABLE schema_migrations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.schema_migrations TO app_rw;
GRANT SELECT ON TABLE okg.schema_migrations TO okg_mcp;


--
-- Name: TABLE source_progress; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.source_progress TO app_rw;
GRANT SELECT ON TABLE okg.source_progress TO okg_mcp;


--
-- Name: TABLE source_watermarks; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.source_watermarks TO app_rw;
GRANT SELECT ON TABLE okg.source_watermarks TO okg_mcp;


--
-- Name: TABLE subtypes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.subtypes TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.subtypes TO catalog_admin;
GRANT SELECT ON TABLE okg.subtypes TO okg_mcp;


--
-- Name: TABLE telemetry_artifacts; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_artifacts TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_artifacts TO okg_mcp;


--
-- Name: TABLE telemetry_events; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_events TO app_rw;
GRANT SELECT,INSERT ON TABLE okg.telemetry_events TO okg_mcp;


--
-- Name: SEQUENCE telemetry_events_event_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.telemetry_events_event_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.telemetry_events_event_id_seq TO okg_mcp;


--
-- Name: TABLE telemetry_graph_effect_manifests; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_graph_effect_manifests TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_graph_effect_manifests TO okg_mcp;


--
-- Name: TABLE telemetry_graph_effects; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.telemetry_graph_effects TO app_rw;
GRANT SELECT,INSERT,DELETE ON TABLE okg.telemetry_graph_effects TO okg_mcp;


--
-- Name: SEQUENCE telemetry_graph_effects_effect_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.telemetry_graph_effects_effect_id_seq TO app_rw;


--
-- Name: TABLE telemetry_metrics; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_metrics TO app_rw;
GRANT SELECT,INSERT ON TABLE okg.telemetry_metrics TO okg_mcp;


--
-- Name: SEQUENCE telemetry_metrics_metric_id_seq; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE okg.telemetry_metrics_metric_id_seq TO app_rw;
GRANT SELECT,USAGE ON SEQUENCE okg.telemetry_metrics_metric_id_seq TO okg_mcp;


--
-- Name: TABLE telemetry_spans; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_spans TO app_rw;
GRANT SELECT,INSERT ON TABLE okg.telemetry_spans TO okg_mcp;


--
-- Name: TABLE telemetry_traces; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_traces TO app_rw;
GRANT SELECT,INSERT,UPDATE ON TABLE okg.telemetry_traces TO okg_mcp;


--
-- Name: TABLE v_admission_envelopes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_admission_envelopes TO app_ro;


--
-- Name: TABLE v_admission_events; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_admission_events TO app_ro;


--
-- Name: TABLE v_admission_record_refs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_admission_record_refs TO app_ro;


--
-- Name: TABLE v_nodes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nodes TO app_ro;


--
-- Name: TABLE v_agent_messages; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_agent_messages TO app_ro;


--
-- Name: TABLE v_agent_sessions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_agent_sessions TO app_ro;


--
-- Name: TABLE v_aliases; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_aliases TO app_ro;


--
-- Name: TABLE v_branch_layered_edges; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_branch_layered_edges TO app_ro;


--
-- Name: TABLE v_branch_layered_nodes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_branch_layered_nodes TO app_ro;


--
-- Name: TABLE v_changes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_changes TO app_ro;


--
-- Name: TABLE v_edges; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_edges TO app_ro;


--
-- Name: TABLE v_doc_chunks; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_doc_chunks TO app_ro;


--
-- Name: TABLE v_chunk_sequence; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_chunk_sequence TO app_ro;


--
-- Name: TABLE v_compute_host_metrics; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_compute_host_metrics TO app_ro;


--
-- Name: TABLE v_consistency_observations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_consistency_observations TO app_ro;


--
-- Name: TABLE v_documents; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_documents TO app_ro;


--
-- Name: TABLE v_edges_history; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_edges_history TO app_ro;


--
-- Name: TABLE v_exchange_boundary_violations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_exchange_boundary_violations TO app_ro;


--
-- Name: TABLE _chronos_branch_interval_checkpoints; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_branch_interval_checkpoints TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_branch_interval_checkpoints TO okg_mcp;


--
-- Name: TABLE v_generations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_generations TO app_ro;


--
-- Name: TABLE v_ir_invocations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_ir_invocations TO app_ro;


--
-- Name: TABLE v_ir_program_bindings; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_ir_program_bindings TO app_ro;


--
-- Name: TABLE v_ir_programs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_ir_programs TO app_ro;


--
-- Name: TABLE v_mcp_calls; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_mcp_calls TO app_ro;


--
-- Name: TABLE v_mcp_call_costs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_mcp_call_costs TO app_ro;


--
-- Name: TABLE v_mcp_operator_costs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_mcp_operator_costs TO app_ro;


--
-- Name: TABLE v_nodes_history; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nodes_history TO app_ro;


--
-- Name: TABLE v_nomos_config_activations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_config_activations TO app_ro;


--
-- Name: TABLE v_nomos_config_sets; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_config_sets TO app_ro;


--
-- Name: TABLE v_nomos_decisions; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_decisions TO app_ro;


--
-- Name: TABLE v_nomos_external_claims; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_external_claims TO app_ro;


--
-- Name: TABLE v_nomos_identity_providers; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_identity_providers TO app_ro;


--
-- Name: TABLE v_nomos_permission_grants; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_permission_grants TO app_ro;


--
-- Name: TABLE v_nomos_policy_activations; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_policy_activations TO app_ro;


--
-- Name: TABLE v_nomos_policy_bundles; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_policy_bundles TO app_ro;


--
-- Name: TABLE v_nomos_subject_bindings; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_nomos_subject_bindings TO app_ro;


--
-- Name: TABLE v_producer_profiles; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_producer_profiles TO app_ro;


--
-- Name: TABLE v_runtime_lock_surface_inventory; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_runtime_lock_surface_inventory TO app_ro;


--
-- Name: TABLE v_runtime_queue_health; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_runtime_queue_health TO app_ro;


--
-- Name: TABLE v_runtime_stale_locks; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_runtime_stale_locks TO app_ro;


--
-- Name: TABLE v_source_frontier_state; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_source_frontier_state TO app_ro;


--
-- Name: TABLE v_source_run_diagnostics; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_source_run_diagnostics TO app_ro;


--
-- Name: TABLE v_source_runs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_source_runs TO app_ro;


--
-- Name: TABLE v_subtypes; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_subtypes TO app_ro;


--
-- Name: TABLE v_table_cells; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_table_cells TO app_ro;


--
-- Name: TABLE v_telemetry_span_tree; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_telemetry_span_tree TO app_ro;


--
-- Name: TABLE v_telemetry_trace_summary; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_telemetry_trace_summary TO app_ro;


--
-- Name: TABLE v_tool_use_events; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT ON TABLE okg.v_tool_use_events TO app_ro;


--
-- Name: TABLE verifier_runs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.verifier_runs TO app_rw;


--
-- Name: TABLE visual_caption_failures; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE okg.visual_caption_failures TO app_rw;


--
-- Name: TABLE visual_crop_blobs; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT ON TABLE okg.visual_crop_blobs TO app_rw;


--
-- Name: TABLE work_ledger; Type: ACL; Schema: okg; Owner: -
--

GRANT SELECT,INSERT,DELETE ON TABLE okg.work_ledger TO app_rw;


--
-- Name: TABLE _chronos_b_interval_okg_console_count_rollup_257f8bc5b1; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1 TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_b_interval_okg_console_count_rollup_257f8bc5b1 TO okg_mcp;


--
-- Name: TABLE _chronos_b_interval_okg_graph_edges_d84ac83c2b; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_b_interval_okg_graph_edges_d84ac83c2b TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_b_interval_okg_graph_edges_d84ac83c2b TO okg_mcp;


--
-- Name: TABLE _chronos_b_interval_okg_graph_nodes_1c8e1fc23f; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f TO okg_mcp;


--
-- Name: TABLE _chronos_b_interval_okg_ontology_rollup_b039672993; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_b_interval_okg_ontology_rollup_b039672993 TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_b_interval_okg_ontology_rollup_b039672993 TO okg_mcp;


--
-- Name: TABLE _chronos_branch_indexes; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_branch_indexes TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_branch_indexes TO okg_mcp;


--
-- Name: TABLE _chronos_branch_interval_branches; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_branch_interval_branches TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_branch_interval_branches TO okg_mcp;


--
-- Name: SEQUENCE _chronos_branch_interval_segment_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON SEQUENCE public._chronos_branch_interval_segment_id_seq TO projection_remediator;


--
-- Name: TABLE _chronos_branch_interval_segments; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_branch_interval_segments TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_branch_interval_segments TO okg_mcp;


--
-- Name: TABLE _chronos_branch_tables; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public._chronos_branch_tables TO projection_remediator;
GRANT SELECT ON TABLE public._chronos_branch_tables TO okg_mcp;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: okg; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA okg GRANT ALL ON FUNCTIONS TO okg_mcp;


--
-- PostgreSQL database dump complete
--

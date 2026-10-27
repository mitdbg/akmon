SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '15min';

-- PACT lane-s-storage-redesign-s2 (storage redesign stage S2, design section
-- 5): the overlay materialization marker and the overlay validity ranges.
--
-- okg.attribute_overlay_generations holds one row per generation and
-- namespace whose overlays were materialized, also when the generation has
-- none, so a pinned read on the graph read path can tell "no overlays" from
-- "never materialized" and refuses the second by name.
-- okg.attribute_overlay_ranges holds the same overlay rows as
-- okg.attribute_effective_attrs, as validity ranges over branch ordinals.
--
-- Additive: two new tables and three indexes; nothing existing is rewritten
-- or dropped, and okg.attribute_effective_attrs is still written (and copied
-- forward) exactly as before. schema.sql and attributes._SCHEMA_STATEMENTS
-- author the same DDL, so on a fresh install every CREATE here is a no-op.
--
-- Backfill (markers only): a marker for every generation whose overlay step is
-- proved by its rows -- overlay rows in okg.attribute_effective_attrs, rows in
-- the effective-view snapshot, or interval rows stabbed at its ordinal (both
-- views are built after the overlay step in the same publish transaction). A
-- generation with none of these proofs gets no marker. Ranges are not built
-- here: the first publish on each branch finds its ranges behind and rebuilds
-- them by replaying the per-generation rows in ordinal order
-- (attributes.rebuild_overlay_ranges_for_branch), so one routine owns them.
--
-- okg_mcp's SELECT grants are rows in views.sql's grant registry and in
-- attributes._OKG_MCP_EFFECTIVE_VIEW_GRANTS.

CREATE TABLE IF NOT EXISTS okg.attribute_overlay_generations (
  generation_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  previous_generation_id TEXT,
  overlay_rows BIGINT NOT NULL,
  chronos_branch_id TEXT,
  ordinal BIGINT,
  ranges_synced BOOLEAN NOT NULL DEFAULT FALSE,
  source TEXT NOT NULL CHECK (source IN ('materializer', 'backfill')),
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (generation_id, namespace)
);
CREATE INDEX IF NOT EXISTS idx_attribute_overlay_generations_branch
  ON okg.attribute_overlay_generations (chronos_branch_id, ordinal);
CREATE TABLE IF NOT EXISTS okg.attribute_overlay_ranges (
  chronos_branch_id TEXT NOT NULL,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  attr_key TEXT NOT NULL,
  from_ord BIGINT NOT NULL,
  to_ord BIGINT NOT NULL,
  observation_id TEXT NOT NULL,
  value JSONB NOT NULL,
  value_hash TEXT NOT NULL,
  PRIMARY KEY (chronos_branch_id, target_kind, target_id, namespace, attr_key, from_ord),
  CHECK (from_ord < to_ord)
);
CREATE INDEX IF NOT EXISTS idx_attribute_overlay_ranges_key
  ON okg.attribute_overlay_ranges
  (chronos_branch_id, target_kind, namespace, attr_key, from_ord, target_id);
CREATE INDEX IF NOT EXISTS idx_attribute_overlay_ranges_open
  ON okg.attribute_overlay_ranges (chronos_branch_id, namespace)
  WHERE to_ord = 9223372036854775807;

DO $overlay_marker_backfill$
BEGIN
  IF to_regclass('okg.attribute_effective_attrs') IS NOT NULL THEN
    INSERT INTO okg.attribute_overlay_generations
      (generation_id, namespace, overlay_rows, source)
    SELECT generation_id, namespace, count(*), 'backfill'
      FROM okg.attribute_effective_attrs
     GROUP BY generation_id, namespace
    ON CONFLICT (generation_id, namespace) DO NOTHING;
  END IF;
  IF to_regclass('okg.attribute_effective_view') IS NOT NULL THEN
    INSERT INTO okg.attribute_overlay_generations
      (generation_id, namespace, overlay_rows, source)
    SELECT DISTINCT generation_id, namespace, 0::bigint, 'backfill'
      FROM okg.attribute_effective_view
    ON CONFLICT (generation_id, namespace) DO NOTHING;
  END IF;
  IF to_regclass('okg.generation_ordinals') IS NOT NULL THEN
    IF to_regclass('okg.attribute_effective_view_intervals') IS NOT NULL THEN
      INSERT INTO okg.attribute_overlay_generations
        (generation_id, namespace, overlay_rows, source)
      SELECT o.checkpoint_id, 'default', 0::bigint, 'backfill'
        FROM okg.generation_ordinals o
       WHERE EXISTS (
               SELECT 1
                 FROM okg.attribute_effective_view_intervals i
                WHERE i.branch_id = o.branch_id
                  AND i.namespace = 'default'
                  AND i.from_ord <= o.ordinal
                  AND o.ordinal < i.to_ord)
      ON CONFLICT (generation_id, namespace) DO NOTHING;
    END IF;
    UPDATE okg.attribute_overlay_generations m
       SET chronos_branch_id = o.branch_id, ordinal = o.ordinal
      FROM okg.generation_ordinals o
     WHERE o.checkpoint_id = m.generation_id
       AND m.chronos_branch_id IS NULL;
  END IF;
END
$overlay_marker_backfill$;

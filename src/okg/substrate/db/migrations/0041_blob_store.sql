SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- PACT first-class-images (issue #3046): the general blob store okg.blobs
-- and its image-only view okg.image_blobs.
--
-- Additive and idempotent: one new table, one index, one view and one app_rw
-- grant; nothing existing is read, rewritten or dropped. visual_crop_blobs
-- and the source cursor blobs are untouched. schema.sql authors the same
-- DDL, so on a fresh install every statement here is a no-op.
--
-- okg_mcp's SELECT on okg.image_blobs is NOT granted here. It is a row in
-- views.sql's okg_grant_registry_seed block, which migrate applies after the
-- ordered migrations, so the view exists by the time the registry runs.
-- okg_mcp holds nothing on okg.blobs itself.

CREATE TABLE IF NOT EXISTS okg.blobs (
  id            TEXT        PRIMARY KEY,
  kind          TEXT        NOT NULL,
  bytes         BYTEA,
  sha256        TEXT,
  mime_type     TEXT,
  size_bytes    BIGINT,
  error         TEXT,
  meta          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT blobs_kind_known CHECK (kind IN ('image')),
  -- Exactly one of bytes and error; the byte facts exist only with bytes.
  CONSTRAINT blobs_payload_shape CHECK (
    (bytes IS NOT NULL AND error IS NULL AND sha256 IS NOT NULL
      AND mime_type IS NOT NULL AND size_bytes IS NOT NULL)
    OR (bytes IS NULL AND error IS NOT NULL AND sha256 IS NULL
      AND mime_type IS NULL AND size_bytes IS NULL)
  )
);
CREATE INDEX IF NOT EXISTS blobs_kind_last_used_idx
  ON okg.blobs (kind, last_used_at);

CREATE OR REPLACE VIEW okg.image_blobs WITH (security_barrier = true) AS
SELECT id, bytes, sha256, mime_type, size_bytes, error, meta,
       created_at, last_used_at
  FROM okg.blobs
 WHERE kind = 'image';

DO $blob_store_app_rw_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON okg.blobs TO app_rw;
  END IF;
END
$blob_store_app_rw_grants$;

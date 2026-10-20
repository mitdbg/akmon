-- Store large source record-set authority in bounded pieces (issue #3008).
--
-- Additive and idempotent. A record set above the JSONB threshold used to be
-- one record_set_text value sent in one INSERT; past 1 GB PostgreSQL refuses
-- the message. The source runner now writes ordered pieces into
-- okg.source_record_set_blob_pieces and a header row in
-- okg.source_record_set_blobs last, in one transaction. Existing rows are not
-- rewritten: a header with no piece_write_id reads as before. schema.sql and
-- the runner's runtime bootstrap carry the same definitions.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE okg.source_record_set_blobs
  ADD COLUMN IF NOT EXISTS piece_write_id UUID,
  ADD COLUMN IF NOT EXISTS piece_count INTEGER,
  ADD COLUMN IF NOT EXISTS piece_bytes BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.source_record_set_blobs'::regclass
       AND conname = 'source_record_set_blobs_piece_write_id_key'
  ) THEN
    ALTER TABLE okg.source_record_set_blobs
      ADD CONSTRAINT source_record_set_blobs_piece_write_id_key
      UNIQUE (piece_write_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.source_record_set_blobs'::regclass
       AND conname = 'source_record_set_blobs_pieces_shape'
  ) THEN
    ALTER TABLE okg.source_record_set_blobs
      ADD CONSTRAINT source_record_set_blobs_pieces_shape CHECK (
        (piece_write_id IS NULL AND piece_count IS NULL
          AND piece_bytes IS NULL)
        OR (piece_write_id IS NOT NULL AND piece_count > 0
          AND piece_bytes >= 0 AND record_set_text IS NULL)
      );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS okg.source_record_set_blob_pieces (
  write_id  UUID    NOT NULL,
  piece_no  INTEGER NOT NULL CHECK (piece_no >= 0),
  source    TEXT    NOT NULL,
  piece     TEXT    NOT NULL,
  PRIMARY KEY (write_id, piece_no),
  CONSTRAINT source_record_set_blob_pieces_header_fkey
    FOREIGN KEY (write_id)
    REFERENCES okg.source_record_set_blobs (piece_write_id)
    ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED
);

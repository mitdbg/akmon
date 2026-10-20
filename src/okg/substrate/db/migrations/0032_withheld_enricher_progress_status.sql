-- Admit a withheld enricher run into the enricher_progress status vocabulary
-- (issue 1528, follow-up to 1330).
--
-- Additive and idempotent. schema.sql carries the same current-state
-- definition for fresh installs; this file upgrades deployments that already
-- applied 0031_bounded_record_authority. No row is rewritten: the new prefix
-- widens what is accepted and nothing existing becomes invalid.
--
-- Why a new prefix rather than reusing 'failed:': the publisher now holds an
-- enricher whose declared input source is in admission quarantine. Its
-- watermark must not advance -- the same discipline as a failure -- but it did
-- NOT fail, and `okg doctor --check enricher-liveness` reports every
-- `last_status LIKE 'failed:%'` row as a failing enricher. Reusing that prefix
-- would make a correctly-withheld enricher indistinguishable from a broken one.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE okg.enricher_progress
  DROP CONSTRAINT IF EXISTS enricher_progress_last_status_chk;

ALTER TABLE okg.enricher_progress
  ADD CONSTRAINT enricher_progress_last_status_chk
    CHECK (last_status IN ('never','success')
        OR last_status LIKE 'failed:%'
        OR last_status LIKE 'withheld:%');

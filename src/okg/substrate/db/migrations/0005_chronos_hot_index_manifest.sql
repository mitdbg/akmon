SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS okg.chronos_hot_index_manifest (
    logical_table text NOT NULL,
    physical_table text NOT NULL,
    suffix text NOT NULL,
    full_index_name text NOT NULL,
    hot_index_name text NOT NULL,
    floor numeric(32, 0) NOT NULL,
    headroom_generations integer NOT NULL CHECK (headroom_generations > 0),
    floor_checkpoint_segment_id bigint NOT NULL,
    floor_checkpoint_branch_point numeric(32, 0) NOT NULL,
    open_interval_value numeric(32, 0) NOT NULL,
    state text NOT NULL CHECK (
        state IN ('building', 'proof', 'activating', 'active', 'retired', 'failed')
    ),
    index_definition text,
    index_bytes_at_activation bigint,
    failure text,
    created_at timestamptz NOT NULL DEFAULT now(),
    activated_at timestamptz,
    PRIMARY KEY (physical_table, suffix, hot_index_name)
);

CREATE UNIQUE INDEX IF NOT EXISTS chronos_hot_index_manifest_one_active_idx
    ON okg.chronos_hot_index_manifest (physical_table, suffix)
    WHERE state IN ('activating', 'active');

COMMENT ON TABLE okg.chronos_hot_index_manifest IS
    'Runtime-derived Chronos live-front index authority; floors and open values are observed store facts, never code constants.';

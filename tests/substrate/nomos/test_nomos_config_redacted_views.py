from __future__ import annotations

import psycopg
import pytest

from okg.substrate.nomos import compile_config_graph, upsert_config_set


pytestmark = pytest.mark.postgres


def test_app_readable_config_view_redacts_raw_entries(clean_runtime: str) -> None:
    config_set = compile_config_graph({
        "config_set_id": "nomos:test:redacted-config",
        "config_version": "v1",
        "scope": {"deployment": "nomos-redaction"},
        "entries": [
            {
                "config_key": "runtime.max_batch",
                "value": 2000,
                "classification": "internal",
                "metadata": {
                    "debug_token": "metadata-secret",
                    "endpoint": "https://internal.example/config",
                },
            },
            {
                "config_key": "runtime.openai_token",
                "secret_ref": "secret://okg/openai-token",
                "classification": "restricted",
                "secret": True,
            },
        ],
    })

    with psycopg.connect(clean_runtime) as conn:
        upsert_config_set(
            conn,
            deployment_name="nomos-redaction",
            config_set=config_set,
            status="compiled",
            metadata={
                "operator_note": "row-metadata-secret",
                "raw_config_url": "https://internal.example/raw-config",
            },
        )
        conn.commit()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entries::text, config_set::text, metadata::text
                  FROM okg.nomos_config_sets
                 WHERE deployment_name = 'nomos-redaction'
                """
            )
            raw_entries, raw_config, raw_metadata = cur.fetchone()
            assert '"value": 2000' in raw_entries
            assert "metadata-secret" in raw_entries
            assert "secret://okg/openai-token" in raw_config
            assert "row-metadata-secret" in raw_metadata

            cur.execute(
                """
                SELECT entries::text, config_set::text, metadata::text
                  FROM okg.v_nomos_config_sets
                 WHERE deployment_name = 'nomos-redaction'
                """
            )
            redacted_entries, redacted_config, redacted_metadata = cur.fetchone()
            redacted_text = redacted_entries + redacted_config + redacted_metadata

            assert "runtime.max_batch" in redacted_text
            assert "runtime.openai_token" in redacted_text
            assert '"value":' not in redacted_text
            assert '"secret_ref":' not in redacted_text
            assert '"metadata":' not in redacted_text
            assert "2000" not in redacted_text
            assert "metadata-secret" not in redacted_text
            assert "https://internal.example/config" not in redacted_text
            assert "row-metadata-secret" not in redacted_text
            assert "https://internal.example/raw-config" not in redacted_text
            assert "secret://okg/openai-token" not in redacted_text
            assert '"metadata_redacted": true' in redacted_text
            assert '"value_redacted": true' in redacted_text
            assert '"secret_ref_redacted": true' in redacted_text

            cur.execute(
                """
                SELECT
                  has_table_privilege(
                    'app_ro', 'okg.v_nomos_config_sets'::regclass, 'SELECT'
                  ),
                  has_table_privilege(
                    'app_ro', 'okg.nomos_config_sets'::regclass, 'SELECT'
                  )
                """
            )
            view_readable, table_readable = cur.fetchone()
            assert view_readable is True
            assert table_readable is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET ROLE app_ro")
            cur.execute(
                """
                SELECT entries::text, config_set::text, metadata::text
                  FROM okg.v_nomos_config_sets
                 WHERE deployment_name = 'nomos-redaction'
                """
            )
            app_entries, app_config, app_metadata = cur.fetchone()
            app_text = app_entries + app_config + app_metadata
            assert "runtime.max_batch" in app_text
            assert "metadata-secret" not in app_text
            assert "row-metadata-secret" not in app_text
            assert "secret://okg/openai-token" not in app_text
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cur.execute(
                    """
                    SELECT entries::text
                      FROM okg.nomos_config_sets
                     WHERE deployment_name = 'nomos-redaction'
                    """
                )

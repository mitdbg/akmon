"""harden-okg-ci-runtime / req.tracks-live (doctor cleanliness).

Feature-specific doctor probes must skip gracefully when their feature is not
provisioned on a deployment, instead of failing the whole doctor. okg-ci's
hand-built DB lacks the okg_dbos role-split and the okg.escalation_live table,
so the provenance-split audit and escalation-backlog probe were reporting
spurious failures (doctor ok=False).
"""
from __future__ import annotations

import psycopg

from okg.substrate.cli.doctor import _doctor_probe_feature_absent


def test_missing_role_is_feature_absent() -> None:
    reason = _doctor_probe_feature_absent(
        psycopg.errors.UndefinedObject('role "okg_dbos" does not exist'),
    )
    assert reason is not None and "role" in reason


def test_missing_relation_is_feature_absent() -> None:
    reason = _doctor_probe_feature_absent(
        psycopg.errors.UndefinedTable(
            'relation "okg.escalation_live" does not exist',
        ),
    )
    assert reason is not None and "relation" in reason


def test_sqlstate_classification_for_wrapped_errors() -> None:
    class _Role(Exception):
        sqlstate = "42704"  # undefined_object

    class _Rel(Exception):
        sqlstate = "42P01"  # undefined_table

    assert _doctor_probe_feature_absent(_Role()) is not None
    assert _doctor_probe_feature_absent(_Rel()) is not None


def test_unrelated_errors_are_not_feature_absent() -> None:
    assert _doctor_probe_feature_absent(ValueError("boom")) is None

    class _Other(Exception):
        sqlstate = "23505"  # unique_violation — a real failure, not a skip

    assert _doctor_probe_feature_absent(_Other()) is None

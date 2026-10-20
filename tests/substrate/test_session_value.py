from __future__ import annotations

from okg.substrate.mcp.session import Session


def test_session_as_dict_exposes_public_pin_fields_only() -> None:
    """`Session.as_dict()` MUST expose only the public pin fields,
    not bookkeeping internals like `db_fingerprint`.

    The public pin is `(deployment_name, generation_id, branch)`
    plus the catalog version that pin resolves to. Under Chronos,
    branch ids are stable branch names rather than legacy integer
    rows from `okg.branches`.
    """
    session = Session(
        deployment_name="repo-cleanup",
        generation_id=244,
        catalog_version_id=10,
        db_fingerprint="repo_cleanup:cluster",
    )

    assert session.as_dict() == {
        "deployment_name": "repo-cleanup",
        "generation_id": 244,
        "catalog_version_id": 10,
        "branch_name": "default",
        "branch_id": "default",
    }

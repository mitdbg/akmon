"""The bring-up MCP subject hint tells the truth about record (issue #2157).

``okg provision`` / ``okg deployment up`` print ``mcp_subject_hint``. It used
to describe the read subject only, and it was false for ``record``: the hint
said MCP servers present ``subject:mcp_client`` while the record boundary
resolved the OS login. Reads and record now resolve one subject, so the hint
states it once and says whether the baseline this manifest seeds lets that
subject write. Each case is checked against the policy graph the SAME manifest
compiles to, so the hint cannot drift from the grant it describes.
"""

from __future__ import annotations

import pytest

from okg.substrate.nomos import compile_policy_graph, mcp_subject_hint_for_manifest
from okg.substrate.nomos.read_grants import (
    MCP_CLIENT_SUBJECT,
    granted_subjects_for_surface,
)
from okg.substrate.nomos.store import deployment_baseline_policy_graph


def _manifest(
    *,
    read: list[str],
    write: list[str],
    mcp_write_default: str | None,
) -> dict:
    posture: dict = {
        "deployment_class": "dogfood_workspace",
        "read_principals": read,
        "write_principals": write,
        "export_policy": "no_export",
        "secret_handling": "secret_ref_required",
        "live_call_default": "deny",
    }
    if mcp_write_default is not None:
        posture["mcp_write_default"] = mcp_write_default
    return {
        "name": "hint",
        "nomos": {"runtime_enforcement": "enforce", "deployment_posture": posture},
    }


def _baseline_grants_record_to_mcp_client(manifest: dict) -> bool:
    bundle = compile_policy_graph(
        deployment_baseline_policy_graph(
            manifest,
            deployment_name="hint",
            scope="deployment:hint",
            activated_by="test",
        )
    )
    return MCP_CLIENT_SUBJECT in granted_subjects_for_surface(bundle, "mcp.record")


@pytest.mark.parametrize(
    ("write", "mcp_write_default", "record_allowed"),
    [
        (["mcp_client", "okg_service"], "review", True),
        (["okg_service"], "review", False),
        (["mcp_client", "okg_service"], None, False),
        (["mcp_client", "okg_service"], "deny", False),
    ],
)
def test_hint_says_whether_record_is_granted_to_the_read_subject(
    write: list[str], mcp_write_default: str | None, record_allowed: bool,
) -> None:
    manifest = _manifest(
        read=["mcp_client", "okg_service"],
        write=write,
        mcp_write_default=mcp_write_default,
    )
    assert _baseline_grants_record_to_mcp_client(manifest) is record_allowed

    hint = mcp_subject_hint_for_manifest(manifest)

    assert "subject:mcp_client" in hint
    assert "for reads and record alike" in hint
    assert "okg mcp-serve --allow-writes" in hint
    if record_allowed:
        assert "record is admitted for review as subject:mcp_client" in hint
        assert "denied" not in hint
    else:
        assert "record is denied under this baseline" in hint
        assert "write_principals" in hint
        assert "OKG_MCP_NOMOS_SUBJECT" in hint


def test_hint_without_mcp_client_read_names_record_as_denied_too() -> None:
    manifest = _manifest(
        read=["okg_service"], write=["okg_service"], mcp_write_default="review",
    )
    hint = mcp_subject_hint_for_manifest(manifest)
    assert "grants no subject:mcp_client" in hint
    assert "MCP reads and record will be denied" in hint
    assert "OKG_MCP_NOMOS_SUBJECT" in hint


def test_hint_reads_the_grants_off_the_compiled_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The hint follows the baseline policy, not a restatement of its rule.

    Issue #2343: the hint used to re-derive the ``mcp.record`` grant from the
    manifest (``write_principals`` + ``mcp_write_default``), so a change to
    the rule in ``store.deployment_baseline_policy_graph`` would leave the
    hint stale. Here the baseline builder is swapped for one that grants
    ``mcp.record`` to mcp_client although the manifest does not ask for it;
    a hint read off the policy says record is admitted, a restated one says
    it is denied.
    """
    from okg.substrate.nomos import read_grants

    manifest = _manifest(
        read=["mcp_client", "okg_service"],
        write=["okg_service"],
        mcp_write_default="deny",
    )
    def _baseline_with_record_grant(manifest, **kwargs):
        graph = deployment_baseline_policy_graph(manifest, **kwargs)
        graph["rules"] = [
            *graph["rules"],
            {
                "rule_id": "test_grant_record_to_mcp_client",
                "effect": "allow",
                "priority": 100,
                "subjects": [MCP_CLIENT_SUBJECT],
                "actions": ["mcp.record"],
                "match": {},
                "reason_code": "nomos.test.record_grant",
            },
        ]
        return graph

    monkeypatch.setattr(
        read_grants,
        "deployment_baseline_policy_graph",
        _baseline_with_record_grant,
        raising=False,
    )
    hint = mcp_subject_hint_for_manifest(manifest)
    assert "record is admitted for review as subject:mcp_client" in hint


def test_hint_for_a_manifest_without_a_posture_names_the_gap() -> None:
    hint = mcp_subject_hint_for_manifest({"name": "hint", "nomos": {}})
    assert "declares no nomos.deployment_posture" in hint
    assert "OKG_MCP_NOMOS_SUBJECT" in hint

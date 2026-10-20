"""Admin gating at the visibility and enforcement layers.

``task.chat-source-admin.admin-gating``. Two layers, and the weaker one
must not be mistaken for the stronger.

*Visibility* is the Open WebUI per-model access grant: a non-admin does
not SEE the admin preset in the picker.

*Enforcement* is a caller who reaches the tools anyway being refused.
The server has no per-caller notion today — source-admin tools are gated
process-wide by ``allow_writes``, caller-supplied authority is ignored
for them, the Nomos hook fires only for ``record:`` actions, and
``_write_refusal`` is dead code for tools that were never registered. So
the per-caller typed refusal is BLOCKED on ``mcp-principal-mapping``,
and what is buildable on ``mcp-http-auth`` alone is transport plus
visibility.

Hiding a control is not denying it. These tests therefore assert that
even PERFECT visibility evidence leaves ``requirement_satisfied`` False.

And the visibility layer is weaker still than "the admin group": Open
WebUI keys its access-control bypass on the caller's ROLE, so an INSTANCE
admin outside the deployment's admin group sees the preset by vendor
design. Every verdict carries that bound so no reader can cite a passing
visibility readback against those callers.

Every arm here is an ABSENCE claim, so every arm carries its own positive
control. An absence observed through a query that could not have shown
presence is worth nothing — which is what an empty, errored, or
unauthenticated model listing is.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest
import yaml

from okg.substrate.chat.source_admin import (
    AUTH_REQUIRED,
    ENFORCEMENT_INTERIM_LABEL,
    PER_CALLER_ARM_BLOCKED_ON,
    VISIBILITY_BOUNDS_ROLE_NOT_GROUP,
    ChatSourceAdminError,
    GatingEvidence,
    LiveToolSurface,
    digest_registry_file,
    evaluate_gating_evidence,
    parse_openai_compat_model_ids,
)
from okg.substrate.mcp.operators import AKMON_READ_TOOLS, SOURCE_ADMIN_TOOLS


ADMIN_PRESET_ID = "okg-workspace-admin"
#: The preset a non-admin IS entitled to see: the positive control for
#: every visibility absence claim below.
ENTITLED_PRESET_ID = "okg-workspace-user"
FIXTURE_DEPLOYMENT = "chat-source-admin-fixture"


def _readonly_surface(
    *, tool_names: tuple[str, ...] | None = None,
) -> LiveToolSurface:
    return LiveToolSurface(
        endpoint="http://127.0.0.1:8100/mcp",
        tool_names=(
            tuple(AKMON_READ_TOOLS) if tool_names is None else tool_names
        ),
        auth_posture=AUTH_REQUIRED,
        read_back_at="2026-08-04T00:00:00+00:00",
    )


def _evidence(**overrides: Any) -> GatingEvidence:
    base: dict[str, Any] = {
        "non_admin_model_ids": (ENTITLED_PRESET_ID,),
        "admin_preset_id": ADMIN_PRESET_ID,
        "non_admin_entitled_preset_id": ENTITLED_PRESET_ID,
        "readonly_surface": _readonly_surface(),
        "admin_endpoint_status": 401,
        "registry_digest_before": "a" * 64,
        "registry_digest_after": "a" * 64,
        "registry_path_reported_by_server": (
            "/deployments/okg-workspace/source_registry.yaml"
        ),
    }
    base.update(overrides)
    return GatingEvidence(**base)


# ---------------------------------------------------------------------------
# The interim, honestly labeled
# ---------------------------------------------------------------------------

def test_interim_evidence_is_labeled_transport_plus_visibility() -> None:
    verdict = evaluate_gating_evidence(_evidence())
    assert verdict.label == ENFORCEMENT_INTERIM_LABEL
    assert verdict.label == "transport-plus-visibility"


def test_full_visibility_and_transport_evidence_still_does_not_satisfy() -> None:
    """The strongest interim evidence available, and it is not enough.

    Every observable arm passes here: the preset is hidden, the nine are
    absent from the read-only endpoint, the admin endpoint 401s, and the
    registry is byte-identical. ``requirement_satisfied`` is still False,
    because the requirement's second layer is a per-caller refusal that
    nothing in this evidence exercises.
    """
    verdict = evaluate_gating_evidence(_evidence())
    assert verdict.visibility_ok is True
    assert verdict.visibility_positive_control_ok is True
    assert verdict.tools_absent_on_readonly_endpoint is True
    assert verdict.transport_refusal_ok is True
    assert verdict.registry_unchanged is True
    assert verdict.requirement_satisfied is False
    assert verdict.per_caller_arm == f"blocked:{PER_CALLER_ARM_BLOCKED_ON}"
    assert any(
        PER_CALLER_ARM_BLOCKED_ON in finding for finding in verdict.findings
    )


# ---------------------------------------------------------------------------
# ROLE, not group: what a passing visibility arm does NOT bound
# ---------------------------------------------------------------------------

def test_every_verdict_carries_the_role_not_group_bound() -> None:
    """A per-model grant is not an admin boundary at v0.11.0.

    ``BYPASS_ADMIN_ACCESS_CONTROL`` defaults True (config.py:2062-2068)
    and ``utils/access_control/__init__.py:163`` returns True for any
    caller whose role is ``admin``, so the group grant governs NON-ADMIN
    visibility only. Carried on the verdict so this readback cannot be
    cited against non-group instance admins.
    """
    verdict = evaluate_gating_evidence(_evidence())
    assert verdict.visibility_bounds == VISIBILITY_BOUNDS_ROLE_NOT_GROUP
    assert "BYPASS_ADMIN_ACCESS_CONTROL" in verdict.visibility_bounds
    assert "role" in verdict.visibility_bounds.lower()
    assert any(
        finding == VISIBILITY_BOUNDS_ROLE_NOT_GROUP
        for finding in verdict.findings
    )


# ---------------------------------------------------------------------------
# The visibility absence, and the query that could have shown presence
# ---------------------------------------------------------------------------

def test_an_empty_model_listing_does_not_prove_the_preset_hidden() -> None:
    """The positive control, and the failure it exists to catch.

    An empty, errored, or unauthenticated ``GET /api/v1/models`` contains
    no preset id at all, so "the admin preset is not in it" is true of a
    response that could never have shown any preset. Without the control
    that scores as perfect visibility.
    """
    verdict = evaluate_gating_evidence(_evidence(non_admin_model_ids=()))
    assert verdict.visibility_positive_control_ok is False
    assert verdict.visibility_ok is False
    assert any(
        "could not have shown the admin preset" in finding
        for finding in verdict.findings
    )


def test_a_listing_missing_only_the_entitled_preset_fails_the_control() -> None:
    """Non-vacuity: the control is about THIS id, not about non-emptiness."""
    verdict = evaluate_gating_evidence(
        _evidence(non_admin_model_ids=("some-other-model",)),
    )
    assert verdict.visibility_positive_control_ok is False
    assert verdict.visibility_ok is False


def test_the_visibility_read_is_parsed_from_the_openai_compat_shape() -> None:
    """`GET /api/v1/models` returns {"data": [...]} (main.py:840,887).

    Reading it as a bare array yields nothing and makes every preset look
    absent, so the ids fed to the verdict come from the parser that
    refuses that shape.
    """
    captured = {"data": [{"id": ENTITLED_PRESET_ID}, {"id": "okg-notes"}]}
    ids = parse_openai_compat_model_ids(captured)
    verdict = evaluate_gating_evidence(_evidence(non_admin_model_ids=ids))
    assert verdict.visibility_positive_control_ok is True
    assert verdict.visibility_ok is True
    # POSITIVE CONTROL for the parser itself: the same call over a
    # listing that DOES carry the admin preset reports it visible.
    leaked = parse_openai_compat_model_ids(
        {"data": [{"id": ENTITLED_PRESET_ID}, {"id": ADMIN_PRESET_ID}]},
    )
    assert evaluate_gating_evidence(
        _evidence(non_admin_model_ids=leaked),
    ).visibility_ok is False


def test_mislabeled_evidence_is_refused() -> None:
    with pytest.raises(ChatSourceAdminError) as excinfo:
        evaluate_gating_evidence(_evidence(label="admin-only enforcement"))
    assert ENFORCEMENT_INTERIM_LABEL in str(excinfo.value)


# ---------------------------------------------------------------------------
# Absent, not hidden
# ---------------------------------------------------------------------------

def test_nine_tools_absent_from_the_read_only_endpoint() -> None:
    verdict = evaluate_gating_evidence(_evidence())
    assert verdict.tools_absent_on_readonly_endpoint is True
    observed = set(_readonly_surface().tool_names)
    assert observed & set(SOURCE_ADMIN_TOOLS) == set()
    # Non-vacuous: the readback is not empty, so "absent" is a fact
    # about an observed surface.
    assert observed


def test_empty_readonly_readback_is_unavailable_not_absent() -> None:
    """An empty tools/list is a failed readback, not proof of absence.

    Without this arm, an endpoint that answered nothing would score as
    "the nine are absent" — an absent reference set read as agreement.
    """
    verdict = evaluate_gating_evidence(
        _evidence(readonly_surface=_readonly_surface(tool_names=()))
    )
    assert verdict.tools_absent_on_readonly_endpoint is False
    assert any("EMPTY" in finding for finding in verdict.findings)


def test_source_admin_tool_on_the_readonly_endpoint_fails_the_arm() -> None:
    verdict = evaluate_gating_evidence(
        _evidence(
            readonly_surface=_readonly_surface(
                tool_names=tuple(AKMON_READ_TOOLS) + ("add_source",),
            )
        )
    )
    assert verdict.tools_absent_on_readonly_endpoint is False
    assert any("add_source" in finding for finding in verdict.findings)


# ---------------------------------------------------------------------------
# The transport refusal and its effect
# ---------------------------------------------------------------------------

def test_unobserved_admin_endpoint_status_is_not_a_refusal() -> None:
    verdict = evaluate_gating_evidence(_evidence(admin_endpoint_status=None))
    assert verdict.transport_refusal_ok is False
    assert any("not exercised" in finding for finding in verdict.findings)


def test_a_hidden_but_reachable_preset_id_produces_no_registry_effect() -> None:
    """The refusal is proven to have had no effect, not assumed to.

    A non-admin that knows the preset id and both endpoints still leaves
    the source registry byte-identical.
    """
    verdict = evaluate_gating_evidence(
        _evidence(non_admin_model_ids=(ENTITLED_PRESET_ID,))
    )
    assert verdict.registry_unchanged is True
    changed = evaluate_gating_evidence(
        _evidence(registry_digest_after="b" * 64)
    )
    assert changed.registry_unchanged is False
    assert any(
        "the source registry changed" in finding
        for finding in changed.findings
    )


def test_digests_taken_at_no_server_reported_path_are_not_evidence() -> None:
    verdict = evaluate_gating_evidence(
        _evidence(registry_path_reported_by_server="  ")
    )
    assert verdict.registry_unchanged is False
    assert any(
        "no server-reported path" in finding for finding in verdict.findings
    )


def test_visibility_failure_is_reported() -> None:
    verdict = evaluate_gating_evidence(
        _evidence(
            non_admin_model_ids=(ENTITLED_PRESET_ID, ADMIN_PRESET_ID),
        )
    )
    # The control passed, so this absence-arm failure is a fact about the
    # preset rather than about the listing.
    assert verdict.visibility_positive_control_ok is True
    assert verdict.visibility_ok is False
    assert any(ADMIN_PRESET_ID in finding for finding in verdict.findings)


# ---------------------------------------------------------------------------
# The digest, taken at the path the server reports
# ---------------------------------------------------------------------------

def test_digest_of_a_missing_registry_refuses(tmp_path: Path) -> None:
    """Two missing files must not digest equal and read as "unchanged"."""
    with pytest.raises(ChatSourceAdminError) as excinfo:
        digest_registry_file(tmp_path / "source_registry.yaml")
    assert excinfo.value.data["reason"] == "missing"


def test_digest_changes_with_content(tmp_path: Path) -> None:
    path = tmp_path / "source_registry.yaml"
    path.write_text("sources: {}\n")
    before = digest_registry_file(path)
    path.write_text("sources: {papers: {enabled_by_default: true}}\n")
    assert digest_registry_file(path) != before


# ---------------------------------------------------------------------------
# The server reports the path itself
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        return None

    def fetchone(self) -> Any:
        return None


class _FakeConn:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


@pytest.fixture
def fixture_deployment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    dep_dir = tmp_path / "deployments" / FIXTURE_DEPLOYMENT
    dep_dir.mkdir(parents=True)
    (dep_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"papers": {"enabled_by_default": True}}})
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._repo_root_from_cwd",
        lambda: tmp_path,
    )
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-state"))
    yield dep_dir


def test_list_sources_reports_the_registry_path_it_writes(
    fixture_deployment: Path,
) -> None:
    """The digest path comes from the SERVER, not from the test.

    Two different files can both be called ``source_registry.yaml``;
    digesting one proves nothing about the other, so the before/after
    digests are taken at the path ``list_sources`` reports.
    """
    from okg.substrate.mcp.source_lifecycle import list_sources

    envelope = list_sources(_FakeConn(), FIXTURE_DEPLOYMENT)
    assert envelope["status"] == "ok"
    reported = envelope["registry_path"]
    assert Path(reported) == fixture_deployment / "source_registry.yaml"
    assert digest_registry_file(reported)

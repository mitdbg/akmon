"""Deployment health, projected into the chat banner.

PACT ``chat-health-banner-projection``:
``req.chat-health-banner-projection.lifecycle`` (degrade, hold, recover;
foreign banners survive; fixed vocabulary only) and
``req.chat-health-banner-projection.probe-honesty`` (a failed probe is its own
state, never a recovery).

Every assertion is on the WIRE. The banner list is read back off
:class:`~tests.substrate.chat.fake_openwebui.FakeInstance`'s own
``GET /api/v1/configs/banners`` route and the write count is read off the
instance's recorded request log, so a projection that composed the right list
and posted the wrong one — or posted the right one twice — fails here. Nothing
asserts on a value the projection kept to itself.

The health probe is INJECTED, which is the point: the degraded payloads below
carry a DSN and an internal hostname in exactly the places a real
``okg dogfood status`` carries them (a doctor check message, a blocker
message), and the raising probe raises with a DSN in its message. The banner
must never contain either.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.chat import health_banner
from okg.substrate.chat.data import chat_data_dir
from okg.substrate.chat.health_banner import (
    OWNER_MARKER,
    STATE_DEGRADED,
    STATE_HEALTHY,
    STATE_NOT_PROBED,
    STATE_PROBE_FAILED,
    STATE_UNAVAILABLE,
    HealthProbeError,
    is_owned,
)
from okg.substrate.chat.sync import (
    SURFACE_INSTANCE_UI,
    InstanceClient,
    SyncInputs,
    sync,
)
from okg.substrate.deployments.chat_block import (
    ChatUiCensusUnavailable,
    parse_chat_block,
)

from .fake_openwebui import FakeInstance

REPO_ROOT = Path(__file__).resolve().parents[3]

TOKEN = "fake-admin-token-value-0123456789"
BASE_URL = "http://instance.test"
SYSTEM_PROMPT = "Use the OKG tools. Quote node ids."
CONSOLE_BASE = "http://console.test"
USER_GROUP = "okg-users"

LIVE_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "query", "search")

#: A fake DSN and the internal hostname inside it. Both are planted in every
#: degraded probe answer below, in the two fields a real ``okg dogfood status``
#: actually carries connection text in: a doctor failed-check message and a
#: blocker message. Neither may ever reach a banner: ``ui.banners`` is
#: generated config, which issue #1183 forbids secrets from entering.
LEAKY_DSN = (
    "postgresql://okg_admin:hunter2-not-a-real-password"
    "@db.internal.invalid:5432/okg_workspace"
)
LEAKY_HOST = "db.internal.invalid"
LEAKY_USER = "okg_admin"

#: The operator's own banner. No ownership marker anywhere in it, which is the
#: only thing that decides whether the projection may touch it.
OPERATOR_BANNER: dict[str, Any] = {
    "id": "operator-maintenance-notice",
    "type": "info",
    "title": None,
    "content": "Planned maintenance Saturday 02:00-04:00 UTC.",
    "dismissible": True,
    "timestamp": 0,
}

#: Two fixed clock readings. Run-to-run the stamp must NOT advance while the
#: state is unchanged, so a second clock value is what makes the idempotence
#: assertion non-vacuous.
T1 = 1_770_000_000
T2 = 1_770_090_000


# ---------------------------------------------------------------------------
# Probe payloads: shaped like `okg dogfood status --json`
# ---------------------------------------------------------------------------

def _healthy_payload() -> dict[str, Any]:
    return {
        "trust_level": "ready",
        "read_authority": {
            "ok": True,
            "generation_id": "gen:2026-08-17T00:00:00Z/7",
            "generation_status": "published",
        },
        "source_freshness": {"ok": True, "stale": [], "missing": []},
        "queues": {"substrate": {"depth": 0}, "source_sync": {}},
        "doctor": {"ok": True, "failed_checks": []},
        "warnings": [
            {
                "id": "sources.sync_liveness",
                "status": "warning",
                "details": {"dlq_count": 0},
            },
        ],
    }


def _degraded_payload(*, trust_level: str = "stale_context") -> dict[str, Any]:
    """A degraded answer carrying a DSN exactly where a real one carries it."""
    return {
        "trust_level": trust_level,
        "read_authority": {
            "ok": True,
            "generation_id": "gen:2026-08-17T00:00:00Z/7",
            "generation_status": "published",
        },
        "source_freshness": {"ok": False, "stale": ["git"], "missing": []},
        "queues": {"substrate": {"depth": 41}, "source_sync": {}},
        "doctor": {
            "ok": False,
            "failed_checks": [
                {
                    "id": "database.connectivity",
                    "message": (
                        f"could not connect to {LEAKY_DSN}: password "
                        f"authentication failed for user {LEAKY_USER!r} "
                        f"on host {LEAKY_HOST}"
                    ),
                },
            ],
        },
        "blockers": [
            {
                "id": "database.connectivity",
                "status": "blocker",
                "message": f"psql: could not translate host name {LEAKY_HOST}",
            },
        ],
        "warnings": [
            {
                "id": "sources.sync_liveness",
                "status": "warning",
                "details": {"dlq_count": 4},
            },
        ],
    }


def _probe(payload: dict[str, Any]):
    def probe(_deployment: str) -> dict[str, Any]:
        return payload

    return probe


def _raising_probe(exc: BaseException):
    """A probe that fails the way a real one fails: with the DSN in the text."""

    def probe(_deployment: str) -> dict[str, Any]:
        raise exc

    return probe


def _connection_error() -> OSError:
    return OSError(
        f"connection to server at {LEAKY_HOST} (10.4.2.9), port 5432 failed "
        f"while probing {LEAKY_DSN}"
    )


# ---------------------------------------------------------------------------
# Fixture deployment (the shape the neighbouring sync suites use)
# ---------------------------------------------------------------------------

def _raw_manifest(*, banner: str | None = "Answers are pinned to one published generation.") -> dict[str, Any]:
    ui: dict[str, Any] = {
        "site_name": "Fixture Site",
        "features": {
            "web_search": False,
            "image_generation": False,
            "code_interpreter": False,
            "arena": False,
            "channels": False,
            "byok": False,
            "context_compaction": True,
        },
    }
    if banner is not None:
        ui["banner"] = banner
    return {
        "name": "fixture",
        # UNMASKED, so `query` is not posture-excluded and the tool set stays
        # stable across these arms — this suite is about banners, not curation.
        "nomos": {"runtime_enforcement": "audit"},
        "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        "chat": {
            "enabled": True,
            "ui": ui,
            "models": {"builtin": ["base/model-a"], "default": "base/model-a"},
            "preset": {
                "display_name": "Fixture Preset",
                "description": "Ask the fixture graph.",
                "system_prompt_ref": "chat/system_prompt.md",
                "suggestions": ["What changed last week?"],
            },
            "search_profile": "analyst",
            "mcp": {"port": 8100, "tools": list(LIVE_TOOLS)},
            "access": {"groups": [USER_GROUP]},
            "deep_links": {
                "operator_ui_base": CONSOLE_BASE,
                "chat_site_base": "http://chat.test",
            },
        },
    }


def _unmasked_catalog() -> Any:
    class _Catalog:
        subtype_metadata: dict[str, dict[str, Any]] = {"document": {"other": 1}}

    return _Catalog()


def _make_inputs(tmp_path: Path, *, manifest: dict[str, Any]) -> SyncInputs:
    dep_dir = tmp_path / "deployment"
    (dep_dir / "chat").mkdir(parents=True, exist_ok=True)
    (dep_dir / "chat" / "system_prompt.md").write_text(SYSTEM_PROMPT, encoding="utf-8")
    chat = parse_chat_block(manifest, manifest_path=dep_dir / "deployment.yaml")
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep_dir,
        raw_manifest=manifest,
        chat=chat,
        mcp_instructions_path=None,
        chat_data_dir=chat_data_dir(),
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=tmp_path / "state",
        catalog_loader=_unmasked_catalog,
        agent_tools={},
    )


def _instance(*, banners: list[dict[str, Any]] | None = None) -> FakeInstance:
    instance = FakeInstance(webui_name="Fixture Site")
    instance.add_group(USER_GROUP, "group-users")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    if banners is not None:
        instance.config["ui.banners"] = [dict(b) for b in banners]
    return instance


def _tools(_endpoint: str, _token_env: str | None) -> tuple[str, ...]:
    return LIVE_TOOLS


def _run(
    inputs: SyncInputs,
    instance: FakeInstance,
    *,
    probe: Any,
    now: int,
) -> Any:
    return sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=_tools,
        probe_health=probe,
        now=lambda: now,
    )


# ---------------------------------------------------------------------------
# Readback helpers. Every one of these reads the INSTANCE, not the report.
# ---------------------------------------------------------------------------

def _banners(instance: FakeInstance) -> list[dict[str, Any]]:
    """The banner list as the instance's own readback route serves it."""
    client = InstanceClient(BASE_URL, TOKEN, opener=instance.opener)
    return [dict(b) for b in client.banners()]


def _owned(instance: FakeInstance) -> list[dict[str, Any]]:
    return [b for b in _banners(instance) if is_owned(b)]


def _banner_writes(instance: FakeInstance) -> int:
    return sum(
        1
        for method, path in instance.requests
        if method == "POST" and path == "/api/v1/configs/banners"
    )


# ---------------------------------------------------------------------------
# req.lifecycle -- scn.raise-clear: degrade, hold, recover
# ---------------------------------------------------------------------------

def test_degrade_hold_recover_with_an_operator_banner_present(tmp_path: Path) -> None:
    """The PACT scenario, end to end, against one instance.

    Given an operator-authored banner, sync runs degraded, degraded, healthy.
    Each arm is asserted on the readback: the raised banner, the unchanged
    second run, the cleared third — and the operator's banner surviving all
    three, which is the clause an ownership bug breaks first.
    """
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance(banners=[OPERATOR_BANNER])

    # -- run 1: degraded -> one owned banner appears -----------------------
    first = _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    assert first.ok, first.to_dict()

    after_first = _banners(instance)
    owned = [b for b in after_first if is_owned(b)]
    assert len(owned) == 1, after_first
    assert owned[0]["type"] == "warning"
    assert first.health["state"] == STATE_DEGRADED
    assert first.health["reading"]["severity"] == "warning"
    assert first.health["banner"]["owned_action"] == "raised"

    # The operator's banner is byte-identical to what was seeded.
    assert OPERATOR_BANNER in after_first
    # And the deployment's declared banner is still there too.
    assert any(
        b["id"] == health_banner.DECLARED_BANNER_ID for b in after_first
    ), after_first

    writes_after_first = _banner_writes(instance)
    assert writes_after_first >= 1

    # -- run 2: still degraded -> nothing changes --------------------------
    second = _run(inputs, instance, probe=_probe(_degraded_payload()), now=T2)
    assert second.ok, second.to_dict()

    ui = second.surfaces[SURFACE_INSTANCE_UI]
    assert ui.changes == 0, ui.diff
    banner_entry = next(e for e in ui.diff if e["key"] == "banner")
    assert banner_entry["changed"] is False
    assert banner_entry["verified"] is True
    # No second POST. The clock advanced from T1 to T2 and the banner did not,
    # which is the whole content of "repeated syncs at the same health state
    # change nothing": the as-of stamp is the state's ONSET, not the last probe.
    assert _banner_writes(instance) == writes_after_first
    assert _banners(instance) == after_first
    assert _owned(instance)[0]["timestamp"] == T1

    # -- run 3: healthy -> the owned banner, and only it, is removed -------
    third = _run(inputs, instance, probe=_probe(_healthy_payload()), now=T2)
    assert third.ok, third.to_dict()

    after_third = _banners(instance)
    assert _owned(instance) == []
    assert third.health["state"] == STATE_HEALTHY
    assert third.health["ok"] is True
    assert third.health["banner"]["owned_action"] == "cleared"
    # Both non-owned banners survived the clear.
    assert OPERATOR_BANNER in after_third
    assert any(b["id"] == health_banner.DECLARED_BANNER_ID for b in after_third)


def test_the_raised_banner_carries_only_the_fixed_vocabulary(tmp_path: Path) -> None:
    """acc.lifecycle: a DSN-bearing probe answer must not leak into the banner.

    The probe payload carries the DSN, the database user, and the internal
    hostname in a doctor failed-check message and a blocker message — the two
    fields a real ``okg dogfood status`` carries connection text in. The banner
    names WHAT degraded and WHERE TO LOOK, never HOW the probe said it.
    """
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance(banners=[OPERATOR_BANNER])

    report = _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    assert report.ok

    banner = _owned(instance)[0]
    content = banner["content"]

    # -- the leak assertion, over the WHOLE banner object ------------------
    serialized = json.dumps(banner)
    for secret in (LEAKY_DSN, LEAKY_HOST, LEAKY_USER, "5432", "password", "psql"):
        assert secret not in serialized, (secret, serialized)
    # And over the whole list, so a leak into a neighbouring object fails too.
    assert LEAKY_DSN not in json.dumps(_banners(instance))

    # -- the four admitted clauses are all present -------------------------
    assert "OKG graph health: degraded (warning)" in content
    assert "source freshness" in content
    assert "dead-letter queue" in content
    assert "doctor checks" in content
    assert "State as of 2026-02-02T02:40:00Z" in content
    assert f"Operator console: {CONSOLE_BASE}#console/home" in content

    # The ownership marker is IN the banner object, in the census-measured
    # field set, and the object carries no seventh key the vendor could drop.
    assert banner["id"].startswith(OWNER_MARKER + ":")
    assert set(banner) == set(health_banner.BANNER_FIELDS)


def test_unavailable_maps_to_the_error_severity(tmp_path: Path) -> None:
    """The severity mapping is a table, not a guess: two levels, two severities."""
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance()

    report = _run(
        inputs,
        instance,
        probe=_probe(_degraded_payload(trust_level="unavailable")),
        now=T1,
    )
    assert report.ok
    assert report.health["state"] == STATE_UNAVAILABLE
    banner = _owned(instance)[0]
    assert banner["type"] == "error"
    assert "OKG graph health: unavailable (error)" in banner["content"]
    assert LEAKY_DSN not in json.dumps(banner)


def test_a_health_banner_is_raised_without_a_declared_manifest_banner(
    tmp_path: Path,
) -> None:
    """The projection does not depend on the deployment declaring a banner.

    It also must not claim the census `banner` knob it never drove from the
    manifest: `driven_knobs` is read off the diff, and reporting a knob the
    deployment never declared would overstate what the manifest asked for.
    """
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(banner=None))
    instance = _instance(banners=[OPERATOR_BANNER])

    report = _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    assert report.ok

    after = _banners(instance)
    assert len(_owned(instance)) == 1
    assert OPERATOR_BANNER in after
    assert not any(b["id"] == health_banner.DECLARED_BANNER_ID for b in after)
    assert "banner" not in report.driven_knobs


def test_at_most_one_owned_banner_survives_a_duplicated_stamp(
    tmp_path: Path,
) -> None:
    """Two owned banners collapse to one; the operator's is still untouched.

    Not hypothetical: an interrupted earlier run, or a hand-edited config, can
    leave two. "One health banner at most per deployment" has to be enforced
    by the composition rather than assumed.
    """
    stale_owned = {
        "id": f"{OWNER_MARKER}:0000000000000000",
        "type": "error",
        "title": None,
        "content": "OKG graph health: unavailable (error). Affected: doctor checks.",
        "dismissible": True,
        "timestamp": 1,
    }
    other_owned = dict(stale_owned, id=f"{OWNER_MARKER}:1111111111111111")
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance(banners=[stale_owned, OPERATOR_BANNER, other_owned])

    report = _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    assert report.ok

    after = _banners(instance)
    assert len(_owned(instance)) == 1
    assert _owned(instance)[0]["timestamp"] == T1
    assert OPERATOR_BANNER in after


# ---------------------------------------------------------------------------
# req.probe-honesty -- a failed probe is its own state
# ---------------------------------------------------------------------------

def test_probe_error_with_a_prior_banner_holds_it_and_reports_loudly(
    tmp_path: Path,
) -> None:
    """scn.probe-error: the owned banner survives, and the failure is named.

    The probe raises with the DSN in its message, which is the realistic shape:
    a connection failure quotes the connection string. The held banner must
    still be the one the degraded run wrote, unchanged — including its stamp,
    because re-stamping would assert a re-observation the probe never made.
    """
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance(banners=[OPERATOR_BANNER])

    _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    raised = _banners(instance)
    writes_before = _banner_writes(instance)

    report = _run(
        inputs, instance, probe=_raising_probe(_connection_error()), now=T2,
    )

    # The run still completes: a flaky probe must not take the chat surface
    # down with it. But it can never print a bare `verified`.
    assert report.ok is True
    assert report.status == "verified_with_unsatisfiable_clauses"

    # -- the banner is HELD, byte for byte --------------------------------
    assert _banners(instance) == raised
    assert len(_owned(instance)) == 1
    assert _owned(instance)[0]["timestamp"] == T1
    assert _banner_writes(instance) == writes_before

    # -- and the failure is NAMED -----------------------------------------
    assert report.health["state"] == STATE_PROBE_FAILED
    assert report.health["ok"] is False
    assert report.health["banner"]["owned_action"] == "held"
    failure = report.health["probe_failure"]
    assert failure is not None
    assert failure["reason"] == "probe_raised:OSError"
    clauses = [
        c for c in report.unsatisfiable if c["clause"] == "chat.health_banner"
    ]
    assert len(clauses) == 1
    assert "did not answer" in clauses[0]["reason"]

    # The probe's own text reached the REPORT (redacted) and not the banner.
    assert LEAKY_DSN not in json.dumps(_banners(instance))
    assert LEAKY_HOST not in json.dumps(_banners(instance))


def test_probe_error_with_no_prior_banner_writes_nothing_and_reports_loudly(
    tmp_path: Path,
) -> None:
    """scn.probe-error-clean: nothing is written, and the failure is named."""
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(banner=None))
    instance = _instance(banners=[OPERATOR_BANNER])

    report = _run(
        inputs, instance, probe=_raising_probe(_connection_error()), now=T1,
    )

    assert report.ok is True
    assert report.status == "verified_with_unsatisfiable_clauses"
    assert _owned(instance) == []
    assert _banners(instance) == [OPERATOR_BANNER]
    assert _banner_writes(instance) == 0

    assert report.health["state"] == STATE_PROBE_FAILED
    assert report.health["banner"]["owned_action"] == "absent"
    assert report.health["probe_failure"]["reason"] == "probe_raised:OSError"
    assert any(
        c["clause"] == "chat.health_banner" for c in report.unsatisfiable
    )


def test_a_probe_answer_that_cannot_be_mapped_is_a_failure_not_a_recovery(
    tmp_path: Path,
) -> None:
    """An unknown `trust_level` holds the banner instead of clearing it.

    The dangerous shape: the probe ANSWERS, so nothing raises, but the answer
    carries a level outside the shared status vocabulary. Defaulting it to any
    state would be a default-value-as-fallback, and defaulting it to healthy
    would clear a banner on a deployment nobody measured.
    """
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance(banners=[OPERATOR_BANNER])

    _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    raised = _banners(instance)

    unmapped = dict(_healthy_payload(), trust_level="probably_fine")
    report = _run(inputs, instance, probe=_probe(unmapped), now=T2)

    assert report.health["state"] == STATE_PROBE_FAILED
    assert report.health["probe_failure"]["reason"] == "unmapped_trust_level"
    assert _banners(instance) == raised
    assert report.status == "verified_with_unsatisfiable_clauses"


def test_a_missing_trust_level_is_a_failure_not_a_recovery(
    tmp_path: Path,
) -> None:
    """The absent-field twin of the arm above. Absence is not agreement."""
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance()

    _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    raised = _banners(instance)

    silent = {k: v for k, v in _healthy_payload().items() if k != "trust_level"}
    report = _run(inputs, instance, probe=_probe(silent), now=T2)

    assert report.health["state"] == STATE_PROBE_FAILED
    assert report.health["probe_failure"]["reason"] == "unmapped_trust_level"
    assert _banners(instance) == raised


def test_a_run_with_no_probe_says_so_rather_than_saying_healthy(
    tmp_path: Path,
) -> None:
    """`not_probed` is a fifth answer, reported, and it touches no banner.

    A library caller that supplies no probe gets a named reason in the report.
    The failure mode this forbids is the quiet one: a health block that is
    absent, or that reads `healthy`, when nothing was ever measured.
    """
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest())
    instance = _instance(banners=[OPERATOR_BANNER])

    _run(inputs, instance, probe=_probe(_degraded_payload()), now=T1)
    raised = _banners(instance)

    report = sync(
        inputs,
        InstanceClient(BASE_URL, TOKEN, opener=instance.opener),
        probe_tools=_tools,
    )

    assert report.health["state"] == STATE_NOT_PROBED
    assert report.health["ok"] is False
    assert report.health["probed"] is False
    assert "Not probed is not healthy." in report.health["reason"]
    assert _banners(instance) == raised


# ---------------------------------------------------------------------------
# Unit-level guards on the pieces the harness arms lean on
# ---------------------------------------------------------------------------

class TestOwnership:
    def test_only_the_marker_decides_ownership(self) -> None:
        assert is_owned({"id": OWNER_MARKER}) is True
        assert is_owned({"id": f"{OWNER_MARKER}:abc"}) is True
        assert is_owned(OPERATOR_BANNER) is False
        assert is_owned({"id": health_banner.DECLARED_BANNER_ID}) is False
        # Content that merely LOOKS like the projection's is not owned.
        assert is_owned({
            "id": "operator-copy",
            "content": "OKG graph health: degraded (warning).",
        }) is False
        assert is_owned({"id": None}) is False
        assert is_owned("not-a-banner") is False

    def test_the_state_digest_changes_with_the_state(self) -> None:
        degraded = health_banner.read_health(_degraded_payload())
        worse = health_banner.read_health(
            _degraded_payload(trust_level="unavailable")
        )
        assert health_banner.banner_id(degraded) != health_banner.banner_id(worse)
        # Same reading, same id: this is what makes idempotence decidable
        # without parsing the banner's text.
        again = health_banner.read_health(_degraded_payload())
        assert health_banner.banner_id(degraded) == health_banner.banner_id(again)


class TestReading:
    def test_the_three_shared_trust_levels_map_to_three_states(self) -> None:
        assert health_banner.read_health(_healthy_payload()).state == STATE_HEALTHY
        assert health_banner.read_health(_degraded_payload()).state == STATE_DEGRADED
        assert health_banner.read_health(
            _degraded_payload(trust_level="unavailable")
        ).state == STATE_UNAVAILABLE

    def test_conditions_are_labels_never_the_underlying_message(self) -> None:
        reading = health_banner.read_health(_degraded_payload())
        assert reading.conditions == (
            "source freshness", "dead-letter queue", "doctor checks",
        )
        assert LEAKY_DSN not in " ".join(reading.conditions)

    def test_an_unreported_field_gets_its_own_label(self) -> None:
        """Not-reported and reported-healthy are different facts."""
        payload = {k: v for k, v in _degraded_payload().items() if k != "doctor"}
        payload["warnings"] = []
        reading = health_banner.read_health(payload)
        assert "doctor checks (not run)" in reading.conditions
        assert "dead-letter queue (not reported)" in reading.conditions

    def test_a_healthy_reading_itemizes_no_conditions(self) -> None:
        assert health_banner.read_health(_healthy_payload()).conditions == ()

    @pytest.mark.parametrize("level", ["", None, "ok", "READY", "green"])
    def test_an_unmapped_level_raises_rather_than_defaulting(self, level: Any) -> None:
        payload = dict(_healthy_payload(), trust_level=level)
        with pytest.raises(HealthProbeError) as err:
            health_banner.read_health(payload)
        assert err.value.data["reason"] in {
            "unmapped_trust_level", "probe_payload_not_a_mapping",
        }


class TestConsoleLink:
    def test_the_link_is_the_declared_base_plus_the_measured_hash(self) -> None:
        assert health_banner.console_link(CONSOLE_BASE) == f"{CONSOLE_BASE}#console/home"

    def test_an_undeclared_base_produces_plain_text_never_a_guess(self) -> None:
        assert health_banner.console_link(None) is None
        assert health_banner.console_link("  ") is None
        reading = health_banner.read_health(_degraded_payload())
        content = health_banner.compose_content(
            reading, as_of=T1, operator_ui_base=None,
        )
        assert health_banner.NO_CONSOLE_TEXT in content
        assert "http" not in content


def test_a_census_that_stops_admitting_the_banner_refuses_the_raise(
    tmp_path: Path,
) -> None:
    """F10 applied to the projection, not only to the manifest.

    The health banner reaches the instance through the same census `banner`
    knob, so a re-recorded census that no longer admits it must REFUSE rather
    than write a value nobody can read back. Tested through the health path
    specifically: the manifest here declares no banner, so the manifest-side
    admission check never fires and only the projection's own check can.
    """
    shrunk = tmp_path / "chat-data"
    shrunk.mkdir()
    (shrunk / "ui-knob-census.json").write_text(
        json.dumps({
            "openwebui_version": "0.11.4",
            # Everything the fixture manifest declares is admitted, so the
            # manifest-side check passes and the ONLY thing that can refuse is
            # the projection's own admission check for the banner it raises.
            "knobs": [
                {
                    "manifest_field": "chat.ui.site_name",
                    "mechanism": "env",
                    "drivable": True,
                },
                *(
                    {
                        "manifest_field": f"chat.ui.features.{leaf}",
                        "mechanism": "api",
                        "drivable": True,
                    }
                    for leaf in (
                        "web_search", "image_generation", "code_interpreter",
                        "arena", "channels", "byok", "context_compaction",
                    )
                ),
            ],
        }),
        encoding="utf-8",
    )
    inputs = _make_inputs(tmp_path, manifest=_raw_manifest(banner=None))
    inputs = SyncInputs(**{**inputs.__dict__, "chat_data_dir": shrunk})

    with pytest.raises(ChatUiCensusUnavailable) as err:
        _run(inputs, _instance(), probe=_probe(_degraded_payload()), now=T1)
    assert err.value.data["reason"] == "knob_outside_census"
    assert err.value.data["requested"] == ["banner"]


class TestCliProbe:
    """The real probe's two traps, without touching a real deployment."""

    def test_a_nonzero_exit_is_still_a_reading(self) -> None:
        """`okg dogfood status` exits 1 for a degraded deployment.

        Gating on the exit code would turn every degraded deployment — the
        exact case the banner exists for — into "health unknown".
        """
        payload = _degraded_payload()

        def runner(argv, _timeout):
            # Issue #2301: THIS okg, not the first `okg` on PATH.
            assert argv[:4] == running_okg_argv()
            assert argv[4:6] == ["dogfood", "status"]
            assert "--json" in argv
            return 1, json.dumps(payload), "some stderr"

        observed = health_banner.cli_health_probe("fixture", runner=runner)
        assert health_banner.read_health(observed).state == STATE_DEGRADED

    def test_empty_or_unparseable_output_is_a_probe_failure(self) -> None:
        for stdout in ("", "   ", "not json", "[]"):
            with pytest.raises(HealthProbeError):
                health_banner.cli_health_probe(
                    "fixture", runner=lambda _a, _t, s=stdout: (0, s, ""),
                )

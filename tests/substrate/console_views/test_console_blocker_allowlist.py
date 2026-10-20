"""What the console's attempts read will and will not hand a browser.

PACT operator-console-live-updates. No database: these drive the composer's
own withholding functions directly, so the fast lane runs them. They were
inside the postgres-marked provenance module, where `not postgres` skipped
them -- the safety rules nothing else enforces were the rules nothing ran.

The two payloads below are the shapes real writers produce:

* a gate EXCEPTION summary (publisher.py 7256-7273), merged into
  `blocked_reason` by `_merge_consistency_blocked_reason` (publisher.py 7282).
  Its `details.error` is `str(exc)`, and a psycopg failure there names the
  host, the IP and the role.
* a transform-gate violation block (consistency/transform_gate.py 1141-1175
  and 1004-1030), whose `details` carry graph row ids under ten different
  keys.

This read is authorized at the operator surface, not per record, so neither
may be served.
"""
from __future__ import annotations

import json
from typing import Any

from okg.substrate.console_views import generations as g

# --------------------------------------------------------------------------- #
# the payloads real writers produce                                            #
# --------------------------------------------------------------------------- #
PSYCOPG_MESSAGE = (
    'connection to server at "10.42.7.19", port 5432 failed: FATAL:  '
    'password authentication failed for user "okg_writer"'
)

GATE_EXCEPTION: dict[str, Any] = {
    "reason": "consistency_publish_gate_blocked",
    "failed_consistency_gates": [
        {
            "check_id": "transform.consistency.no_dangling.v4",
            "gate_name": "transform_consistency_gate",
            "target_generation_id": 1789958889115510,
            "result": "blocked_error",
            "blocks_publish": True,
            "reason": "gate_exception",
            "open_observation_count": 0,
            "newly_opened_count": 0,
            "resolved_count": 0,
            "coverage_complete": None,
            "coverage_basis": None,
            "budget_ms": None,
            "elapsed_ms": 91,
            "details": {
                "error_type": "OperationalError",
                "error": PSYCOPG_MESSAGE,
                "gate_pass": 1,
            },
        },
    ],
}

TRANSFORM_VIOLATIONS: dict[str, Any] = {
    "reason": "consistency_publish_gate_blocked",
    "failed_consistency_gates": [
        {
            "check_id": "transform.consistency.v4",
            "gate_name": "transform_consistency_gate",
            "result": "blocked",
            "blocks_publish": True,
            "reason": "violations",
            "elapsed_ms": 204,
            "details": {
                "dangling_edge_ids": ["edge:leak-1", "edge:leak-2"],
                "pruned_dangling_edge_ids": ["edge:leak-3"],
                "orphan_retracted_derived_edge_ids": ["edge:leak-4"],
                "narrowing_violation_edge_ids": ["edge:leak-5"],
                "node_subtype_violation_node_ids": ["node:leak-6"],
                "cardinality_violation_edge_ids": ["edge:leak-7"],
                "derived_node_cleanup": {
                    "blocking_node_ids": ["node:leak-8"],
                    "blocking_declarations": [{"node_id": "node:leak-9"}],
                    "unsupported_node_ids": ["node:leak-10"],
                    "retracted_node_ids": ["node:leak-11"],
                },
            },
        },
    ],
}

#: Every substring that must never reach a browser out of the two payloads.
FORBIDDEN = (
    "10.42.7.19", "5432", "okg_writer", "password authentication",
    "OperationalError", "connection to server",
    "leak-1", "leak-4", "leak-8", "leak-11", "edge:", "node:",
)


def _served(blocked_reason: Any) -> str:
    detail, oversize = g._blocked_detail(blocked_reason)
    return json.dumps({"detail": detail, "oversize": oversize}, default=str)


# --------------------------------------------------------------------------- #
# nothing sensitive survives                                                   #
# --------------------------------------------------------------------------- #
def test_a_gate_exception_serves_no_host_ip_role_or_message() -> None:
    served = _served(GATE_EXCEPTION)
    for forbidden in FORBIDDEN:
        assert forbidden not in served, f"{forbidden!r} reached the payload: {served}"
    # What an operator acts on survives.
    gate = json.loads(served)["detail"]["failed_consistency_gates"][0]
    assert gate["gate_name"] == "transform_consistency_gate"
    assert gate["result"] == "blocked_error"
    assert gate["reason"] == "gate_exception"
    assert gate["blocks_publish"] is True
    assert gate["elapsed_ms"] == 91
    # And the withholding is stated, with the kind and size but no content.
    withheld = {row["key"]: row for row in gate["withheld"]}
    assert withheld["details"]["withheld_reason"] == "exception_text"
    assert withheld["details"]["kind"] == "object"


def test_a_transform_gate_block_serves_no_graph_row_id() -> None:
    served = _served(TRANSFORM_VIOLATIONS)
    for forbidden in FORBIDDEN:
        assert forbidden not in served, f"{forbidden!r} reached the payload: {served}"
    gate = json.loads(served)["detail"]["failed_consistency_gates"][0]
    assert gate["result"] == "blocked"
    assert gate["elapsed_ms"] == 204
    assert any(row["key"] == "details" for row in gate["withheld"])


def test_an_unknown_blocker_shape_is_withheld_whole() -> None:
    """Fail closed. An unrecognised shape is not walked field by field: its
    unknown fields are exactly the ones nobody has checked."""
    detail, _ = g._blocked_detail({
        "reason": "something_nobody_has_reviewed",
        "host": "10.0.0.1",
        "rows": ["node:leak-99"],
    })
    served = json.dumps(detail)
    assert "10.0.0.1" not in served
    assert "leak-99" not in served
    assert detail["withheld_whole"] is True
    assert detail["withheld_reason"] == "unknown_blocker_shape"
    # The reason code itself is code-shaped, so it is named.
    assert detail["reason"] == "something_nobody_has_reviewed"


def test_a_sample_is_withheld_whatever_type_it_holds() -> None:
    """The denylist this replaced only caught a LIST under `sample`."""
    for value in (["node:leak-1"], {"node": "node:leak-2"}, "node:leak-3"):
        detail, _ = g._blocked_detail({
            "reason": "consistency_publish_gate_blocked",
            "failed_consistency_gates": [{"gate_name": "g", "sample": value}],
        })
        served = json.dumps(detail)
        assert "leak" not in served, f"{value!r} leaked: {served}"


def test_an_unknown_key_name_is_never_echoed() -> None:
    """A key name can itself name a source or a record type."""
    detail, _ = g._blocked_detail({
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [
            {"gate_name": "g", "cmssw_releases_violation_ids": ["x"]},
        ],
    })
    served = json.dumps(detail)
    assert "cmssw_releases" not in served
    gate = detail["failed_consistency_gates"][0]
    assert gate["withheld"][0]["key"] == "unknown_key"


def test_a_string_on_an_allowlisted_key_must_still_be_code_shaped() -> None:
    """A field that normally holds `no_violations` can hold a sentence the day
    a writer changes, and a sentence can quote a row or a path."""
    detail, _ = g._blocked_detail({
        "reason": "checkpoint_attestation_failed",
        "attestor_id": "deployment.attestor",
        "code": "attestation_invalid",
        "detail": "receipt for /Users/example/okg/deployments/x.yaml is missing",
    })
    served = json.dumps(detail)
    assert "/Users/example" not in served
    assert detail["attestor_id"] == "deployment.attestor"
    assert detail["code"] == "attestation_invalid"
    assert any(
        row["key"] == "detail" and row["withheld_reason"] == "not_code_shaped"
        for row in detail["withheld"]
    )


def test_the_checks_section_uses_the_same_allowlist() -> None:
    """A gate cannot be safe in one place and unsafe in the other."""
    gate = g.safe_gate_summary(GATE_EXCEPTION["failed_consistency_gates"][0])
    served = json.dumps(gate)
    for forbidden in FORBIDDEN:
        assert forbidden not in served, forbidden
    assert gate["gate_name"] == "transform_consistency_gate"

    # A gate that PASSED is served too: the checks section shows those.
    passed = g.safe_gate_summary({
        "check_id": "transform.consistency.v4",
        "gate_name": "transform_consistency_gate",
        "result": "evaluated",
        "reason": "no_violations",
        "blocks_publish": False,
        "elapsed_ms": 173,
        "coverage_complete": False,
        "details": {"cardinality_violation_edge_ids": []},
        "preflight": {"catalog_equivalence": {}},
    })
    assert passed["result"] == "evaluated"
    assert passed["reason"] == "no_violations"
    assert passed["blocks_publish"] is False
    assert passed["elapsed_ms"] == 173
    # details and preflight are withheld even when they are empty: the rule is
    # the key, not the size it happens to have on this deployment.
    withheld = {row["key"] for row in passed["withheld"]}
    assert withheld == {"details", "preflight"}


# --------------------------------------------------------------------------- #
# the size cap and the view-run error                                          #
# --------------------------------------------------------------------------- #
def test_an_oversized_blocker_is_withheld_whole_with_its_size() -> None:
    """The cap is measured on what the ALLOWLIST would serve.

    It has to be driven with servable content now: an unservable field is
    withheld before the size is taken, so a megabyte of exception text no
    longer reaches the cap at all -- it is a marker by then. What can still
    grow is the number of gates, each of them servable.
    """
    many = {
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [
            {
                "check_id": f"transform.consistency.gate_{index:04d}.v4",
                "gate_name": "transform_consistency_gate",
                "result": "blocked",
                "reason": "violations",
                "blocks_publish": True,
                "elapsed_ms": index,
            }
            for index in range(400)
        ],
    }
    detail, oversize = g._blocked_detail(many)
    assert detail is None
    assert oversize["reason"] == "blocked_detail_too_large"
    assert oversize["bytes"] > g._BLOCKED_DETAIL_MAX_BYTES
    assert oversize["keys"] == ["failed_consistency_gates", "reason"]
    assert "gate_0100" not in json.dumps(oversize)


def test_unservable_content_is_withheld_before_the_size_is_taken() -> None:
    """A megabyte of exception text never reaches the cap: by then it is a
    marker, so the blocker is still served and still says what was withheld."""
    detail, oversize = g._blocked_detail({
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [
            {"gate_name": "g", "details": {"error": "x" * 1_000_000}},
        ],
    })
    assert oversize is None
    served = json.dumps(detail)
    assert "xxxx" not in served
    assert len(served) < 1000
    gate = detail["failed_consistency_gates"][0]
    assert gate["withheld"][0]["key"] == "details"
    assert gate["withheld"][0]["size"] == 1


def test_a_view_run_s_message_and_traceback_are_withheld() -> None:
    safe = g._safe_view_run_error({
        "type": "ProgrammingError",
        "code": "42P01",
        "message": 'relation "okg.x" does not exist at /Users/example/okg/src/a.py',
        "traceback": ['File "/Users/example/okg/src/b.py", line 3', "raise"],
    })
    served = json.dumps(safe)
    assert "/Users/example" not in served
    assert "does not exist" not in served
    assert safe["type"] == "ProgrammingError"
    assert safe["code"] == "42P01"
    assert safe["error_text_withheld"] is True
    assert safe["withheld_keys"] == ["message", "traceback"]

    raw = g._safe_view_run_error("boom at /Users/example/x.py")
    assert "/Users/example" not in json.dumps(raw)
    assert raw["error_text_withheld"] is True


# --------------------------------------------------------------------------- #
# a code shape narrow enough that a location cannot wear it                     #
# --------------------------------------------------------------------------- #
def test_a_dsn_or_a_path_on_an_allowlisted_key_is_withheld() -> None:
    """The first version of the pattern allowed `/` and `@` and ran to 200
    characters, which admits `postgres://okg:pw@db.example:5432/okg` and
    `/Users/example/x` character for character. An allowlisted key is not a
    promise about what a writer put there."""
    for key, value in (
        ("detail", "postgres://okg:pw@db.example:5432/okg"),
        ("reason", "postgres://okg:pw@db.example:5432/okg"),
        ("detail", "/Users/example/okg/deployments/x.yaml"),
        ("branch_name", "/Users/example/okg"),
        ("branch_name", "C:/Users/example"),
        ("detail", "C:\\Users\\example"),
        ("detail", "~/.okg/local.env"),
        ("reason", "x" * 81),
    ):
        assert not g._is_servable_string(key, value), f"{key}={value!r} was served"


def test_a_real_code_gate_name_decision_id_and_branch_are_served() -> None:
    for key, value in (
        ("reason", "no_violations"),
        ("reason", "checkpoint_attestation_failed"),
        ("gate_name", "transform_consistency_gate"),
        ("check_id", "transform.consistency.no_dangling+narrowing.v4"),
        ("nomos_decision_id", "4764b74f-171d-4d83-aa6b-0235f88e0bb4"),
        ("coverage_basis", "chronos_dirty_edges+incident_dirty_nodes:cardinality"),
        # A branch name is the one value that legitimately holds a slash.
        ("branch_name", "feature/x"),
        ("branch_name", "default"),
    ):
        assert g._is_servable_string(key, value), f"{key}={value!r} was withheld"


def test_a_dsn_through_the_composer_is_withheld_with_a_marker() -> None:
    detail, _ = g._blocked_detail({
        "reason": "checkpoint_attestation_failed",
        "attestor_id": "deployment.attestor",
        "detail": "postgres://okg:pw@db.example:5432/okg",
    })
    served = json.dumps(detail)
    assert "db.example" not in served
    assert "pw@" not in served
    assert detail["attestor_id"] == "deployment.attestor"
    assert any(
        row["key"] == "detail" and row["withheld_reason"] == "not_code_shaped"
        for row in detail["withheld"]
    )


# --------------------------------------------------------------------------- #
# a producer run's error: the KEPT scalars go through the same shape filter     #
# --------------------------------------------------------------------------- #
def test_a_kept_error_scalar_that_is_a_path_is_withheld_not_served() -> None:
    """`reason` is on the keep list; that is not a promise about what a
    writer put there.

    psycopg's own connection failure writes exactly this string, and it names
    a filesystem socket path. Every kept scalar goes through the same
    `_is_servable_string` filter the gate and blocker scalars use, so a key
    cannot be safe in one composer and unsafe in another.
    """
    safe = g._safe_view_run_error({
        "reason": "could not connect to server at /tmp/.s.PGSQL.5432",
    })
    served = json.dumps(safe)
    assert "/tmp/.s.PGSQL.5432" not in served
    assert "reason" not in safe
    assert safe["error_text_withheld"] is True
    assert safe["withheld_keys"] == ["reason"]
    assert safe["withheld_reason"] == "error_may_carry_message_or_traceback"


def test_every_kept_error_scalar_is_shape_filtered_not_just_reason() -> None:
    for key in ("type", "error_type", "code", "reason", "status", "kind"):
        safe = g._safe_view_run_error({key: "postgres://okg:pw@db.example:5432/okg"})
        assert key not in safe, f"{key} served a DSN"
        assert "db.example" not in json.dumps(safe)


def test_a_servable_error_scalar_still_reaches_the_page() -> None:
    safe = g._safe_view_run_error({"type": "ProgrammingError", "code": "42P01"})
    assert safe == {"type": "ProgrammingError", "code": "42P01"}


def test_the_error_s_own_reason_is_not_overwritten_by_the_withheld_marker() -> None:
    """A code-shaped `reason` is the failure's own. The marker saying the
    message was withheld is a DIFFERENT fact and gets its own key: serving
    the marker's text under `reason` would state a reason the run never
    recorded."""
    safe = g._safe_view_run_error({
        "reason": "gate_refused",
        "message": "boom at /Users/example/x.py",
    })
    assert safe["reason"] == "gate_refused"
    assert safe["withheld_reason"] == "error_may_carry_message_or_traceback"


def test_an_unknown_error_key_name_is_never_echoed() -> None:
    """The file's own rule for `_withheld` applies here too: an unrecognised
    key name leaks the vocabulary of whatever wrote it, which can itself name
    a source or a record type. `message` and `traceback` are the error
    writers' own fixed vocabulary and stay named."""
    safe = g._safe_view_run_error({
        "type": "ProgrammingError",
        "okg_source_archiv3_secret_table": "x",
        "message": "boom",
    })
    assert "okg_source_archiv3_secret_table" not in json.dumps(safe)
    assert safe["withheld_keys"] == ["message", "unknown_key"]


# --------------------------------------------------------------------------- #
# "could not look" is not "looked and found none"                              #
# --------------------------------------------------------------------------- #
def test_an_attempt_with_no_target_generation_says_it_could_not_look() -> None:
    """An attempt whose metadata records no `target_generation_id` cannot be
    joined to the runs table at all. Reporting that as an empty list would
    say the deployment recorded no producer run for it, which is a claim
    nothing measured."""
    fields = g._view_runs_fields({}, None)
    assert fields["view_runs"] == []
    assert fields["view_runs_unavailable"] == {
        "reason": "target_generation_id_not_recorded",
    }

    for bad in ("12", True, 1.5, {"id": 12}):
        assert g._view_runs_fields({}, bad)["view_runs_unavailable"] == {
            "reason": "target_generation_id_not_recorded",
        }


def test_an_attempt_that_was_looked_up_and_has_no_run_is_not_unavailable() -> None:
    fields = g._view_runs_fields({}, 41)
    assert fields["view_runs"] == []
    assert fields["view_runs_total"] == 0
    assert fields["view_runs_unavailable"] is None

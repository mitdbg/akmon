"""The one typed publish result (`okg.substrate.publish_outcome`)."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okg.substrate import publish_outcome as po

GEN = "okg:demo:gen:new"
PRIOR = "okg:demo:gen:prior"


def test_new_version_is_published_even_with_zero_delta() -> None:
    """#2472: the version id, not the delta, proves a publish."""
    outcome = po.classify_publish({
        "status": "published", "generation_id": GEN,
        "predecessor_generation_id": PRIOR, "noop": True,
        "nodes_added": 0, "edges_added": 0,
    })
    assert outcome.kind == po.PUBLISHED
    assert outcome.generation_id == GEN
    assert outcome.previous_generation_id == PRIOR
    assert outcome.readers_current is True


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(
            {"status": "published", "generation_id": PRIOR, "noop": True,
             "reused_generation": True},
            id="publisher-reuse-path",
        ),
        pytest.param(
            {"status": "published", "generation_id": PRIOR,
             "predecessor_generation_id": PRIOR},
            id="same-id-as-predecessor",
        ),
    ],
)
def test_same_version_is_no_change(entry) -> None:
    outcome = po.classify_publish(entry)
    assert outcome.kind == po.NO_CHANGE
    assert outcome.generation_id == PRIOR
    assert outcome.readers_current is True


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(
            {"status": "published", "generation_id": GEN, "noop": True,
             "predecessor_generation_id": None},
            id="minted-path-reports-no-predecessor",
        ),
        pytest.param(
            {"status": "published", "generation_id": GEN, "noop": True},
            id="no-predecessor-field",
        ),
    ],
)
def test_first_version_with_no_predecessor_is_published(entry) -> None:
    """An empty install's first publish mints a version with no predecessor.

    The publisher reports ``noop`` (nothing was added) and no predecessor
    (there is none). That is a new version, not a reuse: the reuse path
    always says ``reused_generation``. Calling it ``no_change`` made
    ``okg init`` refuse every fresh install whose sources all wait for setup
    (first-run-golden-path, academic-researcher).
    """
    outcome = po.classify_publish(entry)
    assert outcome.kind == po.PUBLISHED
    assert outcome.generation_id == GEN
    assert outcome.previous_generation_id is None


def test_published_with_a_different_predecessor_and_no_noop_is_published() -> None:
    outcome = po.classify_publish({
        "status": "published", "generation_id": GEN,
        "predecessor_generation_id": PRIOR, "nodes_added": 3,
    })
    assert outcome.kind == po.PUBLISHED


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param({"status": "blocked", "generation_id": GEN}, id="status"),
        pytest.param({"status": "gate_blocked", "generation_id": GEN}, id="gate"),
        pytest.param(
            {"status": "published", "generation_id": GEN,
             "blocked_reason": {"dlq": {}}},
            id="published-with-reason",
        ),
        pytest.param(
            {"status": "published", "generation_id": GEN,
             "phase_b_blocked": True},
            id="phase-b",
        ),
    ],
)
def test_blocked_wins_over_published(entry) -> None:
    outcome = po.classify_publish(entry)
    assert outcome.kind == po.BLOCKED
    assert outcome.generation_id == GEN
    assert outcome.readers_current is False


def test_blocked_names_its_failed_checks_sorted() -> None:
    outcome = po.classify_publish({
        "status": "blocked", "generation_id": GEN,
        "blocked_reason": {"zeta_gate": {"n": 1}, "alpha_gate": {"n": 2}},
    })
    assert outcome.failed_checks == ("alpha_gate", "zeta_gate")


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (["b", "a"], ("a", "b")),
        ("dlq_bound", ("dlq_bound",)),
        (None, ()),
        ("", ()),
    ],
)
def test_failed_checks_accepts_every_reason_shape(reason, expected) -> None:
    assert po.failed_checks(reason) == expected


def test_ingest_skip_entry_is_skipped_with_its_reason() -> None:
    outcome = po.classify_publish({
        "label": "commit", "status": "skipped", "publish_skipped": True,
        "reason": "no_source_work",
    })
    assert outcome.kind == po.SKIPPED
    assert outcome.reason == po.NO_SOURCE_WORK
    assert outcome.generation_id is None
    assert outcome.readers_current is False


def test_ingest_failure_entry_is_failed_with_its_error() -> None:
    outcome = po.classify_publish({
        "label": "commit", "status": "fail", "code": "substrate_error",
        "error": "repository provenance inputs are not committed",
        "fix_hint": "commit them",
    })
    assert outcome.kind == po.FAILED
    assert outcome.reason == "repository provenance inputs are not committed"
    assert outcome.fix_hint == "commit them"


@pytest.mark.parametrize("entry", [None, {}, {"status": "weird"}, {"status": ""}])
def test_missing_or_unrecognized_entry_is_unknown(entry) -> None:
    assert po.classify_publish(entry).kind == po.UNKNOWN


def test_describe_command_names_the_version_and_deployment() -> None:
    assert po.describe_generation_command(GEN, "demo") == (
        f"okg generation describe {GEN} --deployment demo --json"
    )


def test_module_imports_nothing_from_okg() -> None:
    tree = ast.parse(Path(po.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("okg"), node.module
        elif isinstance(node, ast.Import):
            assert not any(a.name.startswith("okg") for a in node.names)


def test_run_summary_uses_the_shared_classifier() -> None:
    from okg.substrate.cli import run_summary

    entry = {
        "status": "published", "generation_id": GEN,
        "predecessor_generation_id": PRIOR, "noop": True,
    }
    assert run_summary.publish_outcome(entry) == po.classify_publish(entry).kind
    assert run_summary.BLOCKED_STATUSES is po.BLOCKED_STATUSES

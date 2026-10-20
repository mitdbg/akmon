from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact import validation
from okg.substrate.pact.markdown_view import check_markdown_view
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import intent_digest, save_manifest
from tests.pact.test_cli import _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _manifest() -> PactManifest:
    return PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must validate.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "validates",
                        "when": ["validation runs"],
                        "then": ["validation passes"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "planned test exists",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_intent_review.py::test_intent_review_is_digest_bound",
            }
        ],
    )


def test_intent_review_is_digest_bound(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    manifest = _manifest()
    save_manifest(root, manifest)
    expected_digest = intent_digest(manifest)

    assert main([
        "pact",
        "--root",
        str(root),
        "view",
        "demo",
        "--format=markdown",
        "--json",
    ]) == 0
    _json_out(capsys)
    assert main([
        "pact",
        "--root",
        str(root),
        "evidence",
        "approve",
        "demo",
        "--reviewer",
        "reviewer@example.com",
        "--notes",
        "intent accepted",
        "--json",
    ]) == 0
    _json_out(capsys)

    data = yaml.safe_load((root / "pact" / "changes" / "demo" / "pact.yaml").read_text())
    review = data["evidence"][0]
    assert review["kind"] == "intent_review"
    assert review["data"]["reviewer"] == "reviewer@example.com"
    assert review["data"]["reviewed_digest"] == expected_digest

    checked = check_markdown_view(root, "demo")
    assert checked["status"] == "fresh"

    assert main([
        "pact",
        "--root",
        str(root),
        "gate",
        "pre-implementation",
        "demo",
        "--json",
    ]) == 0


def test_implementation_gate_requires_intent_review(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    save_manifest(root, _manifest())

    assert main([
        "pact",
        "--root",
        str(root),
        "gate",
        "pre-implementation",
        "demo",
        "--json",
    ]) == 1
    payload = _json_out(capsys)
    assert any(
        issue["code"] == "implementation_gate_missing_intent_review"
        for issue in payload["validation"]["errors"]
    )


def test_missing_intent_review_does_not_compute_a_manifest_digest(
    monkeypatch,
) -> None:
    manifest = _manifest()

    def unexpected_digest(_manifest):
        raise AssertionError("a missing review cannot match any digest")

    monkeypatch.setattr(validation, "intent_digest", unexpected_digest)

    assert validation._has_fresh_intent_review(manifest) is False

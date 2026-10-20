"""Every invocation writes one usage line; `pact usage` reproduces the measures."""

from __future__ import annotations

import json
from pathlib import Path

from pact import usage
from pact.cli import main
from _v5_repo import MANIFEST, commit_all, make_repo, write_manifest


def test_one_line_per_invocation_with_required_fields(tmp_path: Path, monkeypatch, capsys) -> None:
    root = make_repo(tmp_path)
    # Tier 2: strict charges both an approval and proof, so one refusal
    # carries both classes. (At tier 1 an unproven requirement only warns.)
    write_manifest(root, {**MANIFEST, "tier": 2})
    commit_all(root, "m")
    log = tmp_path / "usage.jsonl"
    monkeypatch.setenv("PACT_USAGE_LOG", str(log))
    monkeypatch.chdir(root)
    main(["check", "demo", "--json"])
    main(["check", "demo", "--strict", "--json"])
    main(["check", "missing", "--json"])
    capsys.readouterr()
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(rows) == 3
    for row in rows:
        assert {"at", "verb", "change", "verdict", "blocked_by", "classes", "duration_ms", "head", "origin", "caller"} <= set(row)
    assert rows[0]["verdict"] == "recorded"
    assert rows[1]["verdict"] == "refused" and "missing_proof" in rows[1]["classes"] and "approval" in rows[1]["classes"]
    assert rows[2]["classes"] == ["manifest_syntax"]
    assert rows[0]["head"] and rows[0]["caller"] in {"local", "ci"}


def test_usage_report_reproduces_measures(tmp_path: Path) -> None:
    log = tmp_path / "usage.jsonl"
    lines = [
        {"verb": "check", "change": "a", "verdict": "recorded", "blocked_by": [], "classes": [], "duration_ms": 10, "caller": "local"},
        {"verb": "check", "change": "a", "verdict": "refused", "blocked_by": ["approval_missing"], "classes": ["approval"], "duration_ms": 30, "caller": "ci"},
        {"verb": "evidence attach", "change": "b", "verdict": "recorded", "blocked_by": [], "classes": [], "duration_ms": 20, "caller": "local"},
        {"verb": "check", "change": "b", "verdict": "refused", "blocked_by": ["evidence_digest_mismatch:0"], "classes": ["stale_receipt"], "duration_ms": 40, "caller": "local"},
    ]
    log.write_text("\n".join(json.dumps(r) for r in lines) + "\n", encoding="utf-8")
    report = usage.report([log])
    assert report["invocations"] == 4 and report["changes"] == 2
    assert report["verdicts"] == {"recorded": 2, "refused": 2}
    assert report["refusal_rate"] == 0.5
    assert report["refusal_classes"] == {"approval": 1, "stale_receipt": 1}
    assert report["verbs"] == {"check": 3, "evidence attach": 1}
    assert report["duration_ms_median"] == 30
    assert report["callers"] == {"local": 3, "ci": 1}


def test_off_switch_writes_nothing(tmp_path: Path, monkeypatch, capsys) -> None:
    root = make_repo(tmp_path)
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    monkeypatch.chdir(root)
    main(["check", "missing", "--json"])
    capsys.readouterr()
    assert not (root / ".okg" / "pact-usage.jsonl").exists()


def test_origin_url_never_carries_credentials() -> None:
    assert usage._public_origin("https://x-access-token:ghs_SECRET@github.example.com/example-org/example-repo.git") == "https://github.example.com/example-org/example-repo.git"
    assert usage._public_origin("git@github.com:example-org/example-repo.git") == "git@github.com:example-org/example-repo.git"
    assert usage._public_origin("ssh://user:pw@host/repo.git") == "ssh://host/repo.git"
    assert usage._public_origin(None) is None


def test_classify_tokens() -> None:
    assert usage.classify("evidence_digest_mismatch:0") == "stale_receipt"
    assert usage.classify("unproven_requirement:req.x") == "missing_proof"
    assert usage.classify("approval_stale") == "approval"
    assert usage.classify("tier_below_computed:2") == "tier"
    assert usage.classify("invalid_manifest") == "manifest_syntax"
    assert usage.classify("verb_removed:gate") == "other"

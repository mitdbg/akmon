"""The merge-boundary authority surface for a PACT owned by another repository.

Issue #1424. A separate deployments repository holds a valid PACT with green checks and
a reachable decision ledger, and `gate pre-merge` still refuses it: the
registered sync root indexes only the core repository's `pact/` tree, so the
downstream manifest has no graph projection and no event-plane intent, and
publishing the downstream checkout to create one is correctly forbidden.

These cases pin the third surface and, more importantly, its edges: the
declaration is the only selector and never falls back, the authority
repository is pinned by tracked policy rather than by the declaration's own
strings, the receipt path is a write target a manifest chooses and so is
refused before any write when it escapes the checkout, every binding fault
names its own blocker, the decision ledger is still required, and the
provenance and the limits are reported whether the result accepts or refuses.

No database and no graph: the ledger read is replaced, which is also one of
the cases under test.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact import decisions as pact_decisions
from okg.substrate.pact import envelope
from okg.substrate.pact import external_authority as ext
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact import workflow_authority as wa
from okg.substrate.pact.store import (
    intent_digest as compute_intent_digest,
    load_manifest_file,
    load_yaml_mapping,
)


EXTERNAL_REPO = "example-org/example-deployments"
AUTHORITY_REPO = "example-org/example-repo"
RECEIPT_PATH = ".okg/pact-external-approval.json"
CHANGE_ID = "downstream-change"


def _git(root: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout.strip()


def _seed(tmp_path: Path, *, declare: Any = "default", pin: Any = AUTHORITY_REPO) -> Path:
    """A tiny external repository holding one PACT.

    The receipt directory is deliberately NOT gitignored. `git status` in its
    default untracked mode collapses an untracked directory to a single
    `?? .okg/` line, so a fixture that ignored the directory would hide
    whether the cleanliness answer can ever be reached.
    """
    root = tmp_path / "downstream"
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    settings = (
        "pact_version: 4\n"
        "project: okg-deployments\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n"
    )
    if pin is not None:
        settings += f"external_authority:\n  authority_repository: {pin}\n"
    (root / "pact" / "project.yaml").write_text(settings, encoding="utf-8")
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    pact_operations.create_fast_path_change(
        root,
        CHANGE_ID,
        title="Downstream change",
        intent="The downstream contract must hold.",
        acceptance="The observable check.",
        test_id=(
            "tests/pact/test_external_repository_authority.py"
            "::TestReceiptBinding::test_a_bound_receipt_verifies"
        ),
    )
    if declare != "skip":
        _declare(root, declare)
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "downstream@example.invalid")
    _git(root, "config", "user.name", "Downstream")
    _git(root, "remote", "add", "origin", f"git@github.com:{EXTERNAL_REPO}.git")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "seed")
    return root


def _manifest_path(root: Path) -> Path:
    active = root / "pact" / "changes" / CHANGE_ID / "pact.yaml"
    return active if active.is_file() else root / "pact" / "archive" / CHANGE_ID / "pact.yaml"


def _declaration(**overrides: Any) -> dict[str, Any]:
    declaration = {
        "kind": ext.AUTHORITY_KIND,
        "repository": EXTERNAL_REPO,
        "authority_repository": AUTHORITY_REPO,
        "receipt": RECEIPT_PATH,
    }
    declaration.update(overrides)
    return declaration


def _declare(root: Path, declaration: Any) -> None:
    path = _manifest_path(root)
    document = load_yaml_mapping(path)
    if declaration == "default":
        declaration = _declaration()
    metadata = dict(document["change"].get("method_metadata") or {})
    metadata[ext.DECLARATION_KEY] = declaration
    document["change"]["method_metadata"] = metadata
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _receipt_payload(root: Path, **overrides: Any) -> dict[str, Any]:
    manifest = load_manifest_file(_manifest_path(root))
    payload = {
        "change_id": manifest.change.id,
        "repository": EXTERNAL_REPO,
        "authority_repository": AUTHORITY_REPO,
        "head_sha": _git(root, "rev-parse", "HEAD"),
        "tree_sha": _git(root, "rev-parse", "HEAD^{tree}"),
        "intent_digest": compute_intent_digest(manifest),
        "generation_id": "okg:okg-workspace:gen:4211",
        "deployment": "okg-workspace",
        "recorded_by": "riley",
        "recorded_at": "2026-09-03T00:00:00+00:00",
    }
    payload.update(overrides)
    payload["receipt_digest"] = ext.receipt_digest(payload)
    return payload


def _write_receipt(root: Path, **overrides: Any) -> dict[str, Any]:
    """A well-formed receipt bound to the live checkout, plus any overrides."""
    payload = _receipt_payload(root, **overrides)
    path = root / RECEIPT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def _cli(root: Path, argv: tuple[str, ...]) -> tuple[int, dict[str, Any]]:
    import io
    from contextlib import redirect_stdout

    from okg.substrate.cli.main import main

    stream = io.StringIO()
    with redirect_stdout(stream):
        rc = main(["pact", "--root", str(root), *argv, "--json"])
    return rc, json.loads(stream.getvalue().splitlines()[-1])


def _verify(root: Path) -> dict[str, Any]:
    manifest = load_manifest_file(_manifest_path(root))
    return ext.verify_external_authority(
        root,
        manifest,
        intent_digest=compute_intent_digest(manifest),
    )


def _blockers(root: Path) -> list[str]:
    return list(_verify(root)["blockers"])


class TestDeclarationSelectsTheSurface:
    """Presence is the selector, and there is no fallback from it."""

    def test_a_malformed_declaration_refuses_instead_of_falling_back(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path, declare="not-a-mapping")

        def forbidden(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError(
                "a declared external surface must never consult the "
                "sync-root graph projection"
            )

        monkeypatch.setattr(pact_cli, "_graph_projection_status", forbidden)
        manifest = load_manifest_file(_manifest_path(root))

        status = pact_cli._merge_authority_status(
            root, manifest, compute_intent_digest(manifest)
        )

        assert status["authority_scope"] == ext.AUTHORITY_SCOPE
        assert status["ok"] is False
        assert "external_declaration_invalid" in status["blockers"]

    def test_an_undeclared_manifest_keeps_the_graph_projection_arm(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path, declare="skip")
        sentinel = {"authority_scope": "file_projection", "ok": True}
        monkeypatch.setattr(
            pact_cli, "_graph_projection_status", lambda manifest, *a, **kw: sentinel
        )
        manifest = load_manifest_file(_manifest_path(root))

        status = pact_cli._merge_authority_status(
            root, manifest, compute_intent_digest(manifest)
        )

        assert status is sentinel

    def test_the_external_arm_never_reports_a_graph_projection_as_fresh(
        self, tmp_path: Path
    ) -> None:
        """`fresh` would claim a projection this arm did not read."""
        root = _seed(tmp_path)
        _write_receipt(root)

        status = _verify(root)

        assert status["ok"] is True
        assert status["status"] == "not_projected"
        assert status["stale"] is None


class TestDeclarationFaults:
    """Every declaration blocker is reachable and named."""

    def test_each_declaration_fault_names_its_own_blocker(
        self, tmp_path: Path
    ) -> None:
        cases: list[tuple[Any, str]] = [
            ("not-a-mapping", "external_declaration_invalid"),
            (
                {"kind": ext.AUTHORITY_KIND, "repository": EXTERNAL_REPO},
                "external_declaration_incomplete",
            ),
            (
                _declaration(extra="smuggled"),
                "external_declaration_unknown_field",
            ),
            (
                _declaration(kind="something_else"),
                "external_declaration_unknown_kind",
            ),
            (
                _declaration(repository="not a repo slug"),
                "external_declaration_invalid_repository",
            ),
            (
                _declaration(receipt="../escape.json"),
                "external_declaration_invalid_receipt_path",
            ),
            (
                _declaration(receipt="/etc/passwd"),
                "external_declaration_invalid_receipt_path",
            ),
            (
                _declaration(receipt=".git/description"),
                "external_declaration_invalid_receipt_path",
            ),
        ]
        for index, (declaration, expected) in enumerate(cases):
            root = _seed(tmp_path / f"case{index}", declare=declaration)
            assert expected in _blockers(root), (expected, declaration)


class TestReceiptBinding:
    """Exact identity, fail closed, one named blocker per fault."""

    def test_a_bound_receipt_verifies(self, tmp_path: Path) -> None:
        root = _seed(tmp_path)
        _write_receipt(root)

        status = _verify(root)

        assert status["ok"] is True, status["blockers"]
        assert status["projected_generation_id"] == "okg:okg-workspace:gen:4211"

    def test_each_binding_fault_names_its_own_blocker(self, tmp_path: Path) -> None:
        root = _seed(tmp_path)
        # Each case is RESEALED, so it tests the field it names rather than
        # collapsing onto a broken self digest. The broken-seal case is tested
        # separately, below.
        cases: list[tuple[dict[str, Any], str]] = [
            ({"change_id": "other-change"}, "external_receipt_change_mismatch"),
            (
                {"repository": "someone/else"},
                "external_receipt_repository_mismatch",
            ),
            (
                {"authority_repository": "someone/else"},
                "external_receipt_authority_repository_mismatch",
            ),
            (
                {"intent_digest": "sha256:" + "0" * 64},
                "external_receipt_intent_digest_mismatch",
            ),
            ({"head_sha": "a" * 40}, "external_receipt_head_mismatch"),
            ({"head_sha": "not-a-sha"}, "external_receipt_head_malformed"),
            ({"tree_sha": "b" * 40}, "external_receipt_tree_mismatch"),
            ({"tree_sha": "not-a-sha"}, "external_receipt_tree_malformed"),
            ({"generation_id": ""}, "external_receipt_generation_unpinned"),
            ({"recorded_by": ""}, "external_receipt_provenance_incomplete"),
            ({"deployment": ""}, "external_receipt_provenance_incomplete"),
            ({"recorded_at": ""}, "external_receipt_provenance_incomplete"),
        ]
        for overrides, expected in cases:
            _write_receipt(root, **overrides)
            assert expected in _blockers(root), expected

        # A tampered field with the ORIGINAL seal left in place is a broken
        # self digest, which is the whole point of sealing it.
        _write_receipt(root)
        sealed = json.loads((root / RECEIPT_PATH).read_text(encoding="utf-8"))
        sealed["generation_id"] = "okg:okg-workspace:gen:9999"
        (root / RECEIPT_PATH).write_text(json.dumps(sealed), encoding="utf-8")
        assert "external_receipt_digest_mismatch" in _blockers(root)

        # A seal that is not even a sha256 token.
        sealed = _write_receipt(root)
        sealed["receipt_digest"] = "nonsense"
        (root / RECEIPT_PATH).write_text(json.dumps(sealed), encoding="utf-8")
        assert "external_receipt_digest_malformed" in _blockers(root)

        # A field outside the sealed set is refused rather than ignored.
        sealed = _write_receipt(root)
        sealed["extra"] = "smuggled"
        (root / RECEIPT_PATH).write_text(json.dumps(sealed), encoding="utf-8")
        assert "external_receipt_unknown_field" in _blockers(root)

        # A field missing from the sealed set.
        sealed = _write_receipt(root)
        del sealed["deployment"]
        (root / RECEIPT_PATH).write_text(json.dumps(sealed), encoding="utf-8")
        assert "external_receipt_incomplete" in _blockers(root)

        # Not JSON at all, and absent entirely.
        (root / RECEIPT_PATH).write_text("{ not json", encoding="utf-8")
        assert "external_receipt_unreadable" in _blockers(root)
        (root / RECEIPT_PATH).unlink()
        assert "external_receipt_missing" in _blockers(root)

        # A dirty worktree under an otherwise valid receipt.
        _write_receipt(root)
        (root / "pact" / "AGENTS.md").write_text("# edited\n", encoding="utf-8")
        assert "external_receipt_worktree_not_clean" in _blockers(root)

    def test_editing_the_manifest_invalidates_the_receipt(
        self, tmp_path: Path
    ) -> None:
        root = _seed(tmp_path)
        _write_receipt(root)
        assert _verify(root)["ok"] is True

        path = _manifest_path(root)
        document = load_yaml_mapping(path)
        document["change"]["title"] = "Downstream change, revised"
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        _git(root, "commit", "-qam", "revise")
        _write_receipt(root, intent_digest="sha256:" + "1" * 64)

        assert "external_receipt_intent_digest_mismatch" in _blockers(root)

    def test_a_committed_receipt_is_refused(self, tmp_path: Path) -> None:
        """It would sit inside the tree whose hash it binds."""
        root = _seed(tmp_path)
        _write_receipt(root)
        _git(root, "add", "-f", RECEIPT_PATH)
        _git(root, "commit", "-qm", "track the receipt")
        _write_receipt(root)

        assert "external_receipt_is_tracked" in _blockers(root)


class TestTheReceiptPathIsAWriteTargetAManifestChooses:
    """A manifest is authored content; its `receipt` path must not aim a write."""

    def test_a_dot_git_path_is_refused_before_anything_is_written(
        self, tmp_path: Path
    ) -> None:
        root = _seed(tmp_path, declare=_declaration(receipt=".git/description"))
        before = (root / ".git" / "description").read_text(encoding="utf-8")
        manifest = load_manifest_file(_manifest_path(root))

        written = ext.write_receipt(
            root,
            manifest,
            intent_digest=compute_intent_digest(manifest),
            generation_id="okg:okg-workspace:gen:1",
            deployment="okg-workspace",
            recorded_by="riley",
            recorded_at="2026-09-03T00:00:00+00:00",
        )

        assert written["written"] is False
        assert "external_declaration_invalid_receipt_path" in written["blockers"]
        assert (root / ".git" / "description").read_text(encoding="utf-8") == before

    def test_a_symlink_out_of_the_checkout_is_refused_and_not_followed(
        self, tmp_path: Path
    ) -> None:
        outside = tmp_path / "outside.json"
        outside.write_text("untouched", encoding="utf-8")
        root = _seed(tmp_path)
        (root / ".okg").mkdir(parents=True, exist_ok=True)
        (root / RECEIPT_PATH).symlink_to(outside)
        manifest = load_manifest_file(_manifest_path(root))

        written = ext.write_receipt(
            root,
            manifest,
            intent_digest=compute_intent_digest(manifest),
            generation_id="okg:okg-workspace:gen:1",
            deployment="okg-workspace",
            recorded_by="riley",
            recorded_at="2026-09-03T00:00:00+00:00",
        )

        assert written["written"] is False
        assert "external_receipt_path_escapes_repository" in written["blockers"]
        assert outside.read_text(encoding="utf-8") == "untouched"
        assert "external_receipt_path_escapes_repository" in _blockers(root)

    def test_a_tracked_receipt_path_is_refused_before_writing(
        self, tmp_path: Path
    ) -> None:
        root = _seed(tmp_path)
        (root / ".okg").mkdir(parents=True, exist_ok=True)
        (root / RECEIPT_PATH).write_text("committed content", encoding="utf-8")
        _git(root, "add", "-f", RECEIPT_PATH)
        _git(root, "commit", "-qm", "track the receipt")
        manifest = load_manifest_file(_manifest_path(root))

        written = ext.write_receipt(
            root,
            manifest,
            intent_digest=compute_intent_digest(manifest),
            generation_id="okg:okg-workspace:gen:1",
            deployment="okg-workspace",
            recorded_by="riley",
            recorded_at="2026-09-03T00:00:00+00:00",
        )

        assert written["written"] is False
        assert "external_receipt_is_tracked" in written["blockers"]
        assert (root / RECEIPT_PATH).read_text(encoding="utf-8") == "committed content"


class TestLedgerAndScopePreserved:
    """The surface replaces the projection, not the decision."""

    def _inputs(self, **overrides: Any) -> wa.WorkflowAuthorityInput:
        base: dict[str, Any] = {
            "file_fresh": True,
            "graph_required": True,
            "graph_ready": False,
            "graph_fresh": False,
            "ledger_fresh": True,
            "ledger_approval": wa.LEDGER_APPROVAL_APPROVED,
            "generation_pin_matches": False,
            "evidence_artifacts_fresh": True,
            "pact_coordination_agree": True,
            "projection_authority": ext.AUTHORITY_SCOPE,
            "external_authority_allowed": True,
            "external_receipt_bound": True,
        }
        base.update(overrides)
        return wa.WorkflowAuthorityInput(**base)

    def test_a_bound_receipt_replaces_only_the_projection(self) -> None:
        decision = wa.evaluate_workflow_authority(self._inputs())

        assert decision.accepted is True
        assert decision.blockers == ()

    def test_a_bound_receipt_without_a_ledger_approval_still_refuses(self) -> None:
        absent = wa.evaluate_workflow_authority(
            self._inputs(ledger_approval=wa.LEDGER_APPROVAL_ABSENT)
        )
        unreachable = wa.evaluate_workflow_authority(
            self._inputs(
                ledger_fresh=False,
                ledger_approval=wa.LEDGER_APPROVAL_NOT_JUDGED,
            )
        )

        assert "no_ledger_approval" in absent.blockers
        assert absent.accepted is False
        assert "ledger_not_fresh" in unreachable.blockers
        assert unreachable.accepted is False

    def test_an_unbound_receipt_refuses_and_is_not_a_deferral(self) -> None:
        decision = wa.evaluate_workflow_authority(
            self._inputs(external_receipt_bound=False)
        )

        assert "external_receipt_not_bound" in decision.blockers
        assert wa.projection_only(decision.blockers) is False

    def test_an_undeclarable_surface_is_not_an_unknown_authority(self) -> None:
        decision = wa.evaluate_workflow_authority(
            self._inputs(external_authority_allowed=False)
        )

        assert "external_authority_not_allowed" in decision.blockers
        assert decision.accepted is False

    def test_the_authority_repository_cannot_declare_the_surface_for_itself(
        self, tmp_path: Path
    ) -> None:
        root = _seed(tmp_path, declare=_declaration(repository=AUTHORITY_REPO))
        _write_receipt(root, repository=AUTHORITY_REPO)

        assert (
            "external_authority_is_the_authority_repository" in _blockers(root)
        )

    def test_the_bypass_spelling_is_refused(self, tmp_path: Path) -> None:
        """Naming a DIFFERENT authority repository is the bypass.

        `repository: <home>` beside `authority_repository: <other>`
        makes the declaration's own two strings differ, which is all the
        earlier guard compared. The pin is tracked policy, so it does not
        move when the manifest says so.
        """
        root = _seed(
            tmp_path,
            declare=_declaration(
                repository=AUTHORITY_REPO,
                authority_repository="example-org/some-other-repo",
            ),
        )
        _write_receipt(
            root,
            repository=AUTHORITY_REPO,
            authority_repository="example-org/some-other-repo",
        )

        blockers = _blockers(root)

        assert "external_declaration_authority_repository_not_pinned" in blockers
        assert "external_authority_is_the_authority_repository" in blockers

    def test_the_live_origin_remote_is_checked_against_the_pin(
        self, tmp_path: Path
    ) -> None:
        """A checkout OF the authority repository cannot use the surface."""
        root = _seed(tmp_path)
        _write_receipt(root)
        assert _verify(root)["ok"] is True
        _git(root, "remote", "set-url", "origin", f"git@github.com:{AUTHORITY_REPO}.git")

        assert (
            "external_authority_is_the_authority_repository" in _blockers(root)
        )

    def test_an_unpinned_authority_repository_refuses(self, tmp_path: Path) -> None:
        """No tracked pin is a refusal, never a pass."""
        root = _seed(tmp_path, pin=None)
        _write_receipt(root)

        assert "external_authority_repository_unconfigured" in _blockers(root)


class TestTaskCompletionUsesTheSameSurface:
    """`gate task-done` must not read a projection that cannot exist.

    The merge boundary and task completion are separate gates with separate
    bars, but they answer the same question about WHICH surface carries graph
    authority. Leaving task completion on the sync-root projection would have
    left the external PACT refused at a different gate for the same absent
    projection.
    """

    def _graph_demanding_manifest(self, root: Path) -> None:
        """Make the task actually demand graph authority."""
        path = _manifest_path(root)
        document = load_yaml_mapping(path)
        document["requirements"][0]["evidence_policy"]["required"] = ["graph_report"]
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        _git(root, "commit", "-qam", "demand graph evidence")

    def test_the_receipt_is_the_authority_for_task_completion_too(
        self, tmp_path: Path
    ) -> None:
        root = _seed(tmp_path)
        self._graph_demanding_manifest(root)
        _write_receipt(root)
        manifest = load_manifest_file(_manifest_path(root))

        status = pact_operations._workflow_graph_projection_status(root, manifest)

        assert status["authority_scope"] == ext.AUTHORITY_SCOPE
        assert status["ok"] is True

    def test_a_broken_receipt_blocks_task_completion(self, tmp_path: Path) -> None:
        root = _seed(tmp_path)
        self._graph_demanding_manifest(root)
        _write_receipt(root, head_sha="a" * 40)
        manifest = load_manifest_file(_manifest_path(root))

        status = pact_operations._workflow_graph_projection_status(root, manifest)

        assert status["ok"] is False
        assert "external_receipt_head_mismatch" in status["blockers"]


class TestTheFormalModelGapIsDisclosed:
    """The TLA model does not model this arm yet, and that is stated in code.

    Extending `formal/pact/WorkflowAuthority.tla` moves the module and config
    digests that `pact-no-decision-status-truth` pins its formal evidence
    against, so the model change belongs to the change that can re-run TLC and
    re-ingest that proof. Re-sealing it by hand would forge the receipt. This
    case fails if the disclosure is deleted while the gap is still real, so the
    gap cannot go quiet.
    """

    def test_the_gap_is_named_where_the_arm_is_implemented(self) -> None:
        formal = Path(__file__).resolve().parents[2] / "formal" / "pact"
        model = (formal / "WorkflowAuthority.tla").read_text(encoding="utf-8")
        source = Path(wa.__file__).read_text(encoding="utf-8")

        if "external_repository_receipt" in model:
            pytest.skip("the model now carries the arm; the disclosure is spent")
        assert "KNOWN GAP" in source
        assert "WorkflowAuthority.tla" in source


class TestTheRealAcceptancePath:
    """The one case that must accept, through the real gate function."""

    def _archive(self, root: Path) -> None:
        source = root / "pact" / "changes" / CHANGE_ID
        target = root / "pact" / "archive" / CHANGE_ID
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))
        _git(root, "add", "-A")
        _git(root, "commit", "-qm", "archive")

    def _stub_ledger(
        self,
        monkeypatch: pytest.MonkeyPatch,
        digest: str,
    ) -> None:
        class _Conn:
            def close(self) -> None:
                return None

        approval = SimpleNamespace(
            verdict="approve",
            intent_digest=digest,
            to_state=None,
            principal="riley",
            decided_at="2026-09-03T00:00:00Z",
        )
        monkeypatch.setattr(
            pact_decisions, "require_ledger_connection", lambda **kw: _Conn()
        )
        monkeypatch.setattr(
            pact_decisions, "load_decisions", lambda conn, change_id: [approval]
        )
        monkeypatch.setattr(pact_decisions, "load_intents", lambda conn: [])
        monkeypatch.setattr(pact_decisions, "load_decision_rows", lambda conn: [])
        monkeypatch.setattr(
            pact_decisions, "load_coordination_events", lambda conn, change_id: []
        )
        monkeypatch.setattr(
            pact_cli,
            "_event_plane_authority_provenance",
            lambda root: {"event_plane_authority_allowed": False, "mode": "stub"},
        )

    def test_a_written_receipt_and_a_ledger_approval_accept(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path)
        self._archive(root)
        _write_receipt(root)
        manifest = load_manifest_file(_manifest_path(root))
        digest = compute_intent_digest(manifest)
        self._stub_ledger(monkeypatch, digest)

        def forbidden(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("the external arm must not read the projection")

        monkeypatch.setattr(pact_cli, "_graph_projection_status", forbidden)

        gate = pact_cli._ledger_decision_gate(root, CHANGE_ID)

        assert gate["status"] == "approved"
        authority = gate["workflow_authority"]
        assert authority["blockers"] == []
        assert authority["accepted"] is True
        assert gate["ok"] is True
        assert authority["external_authority"]["ok"] is True
        assert authority["graph_projection"]["status"] == "not_projected"

    def test_the_same_state_with_a_broken_receipt_refuses(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The approval is identical; only the receipt binding changes."""
        root = _seed(tmp_path)
        self._archive(root)
        _write_receipt(root, head_sha="a" * 40)
        manifest = load_manifest_file(_manifest_path(root))
        self._stub_ledger(monkeypatch, compute_intent_digest(manifest))

        gate = pact_cli._ledger_decision_gate(root, CHANGE_ID)

        authority = gate["workflow_authority"]
        assert gate["ok"] is False
        assert authority["accepted"] is False
        assert "external_receipt_not_bound" in authority["blockers"]
        assert "worktree_head_mismatch" in authority["blockers"]


class TestDecideProducesTheReceipt:
    """Production is tied to the recorded decision, and never silent."""

    @pytest.fixture(autouse=True)
    def _configured_graph_deployment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The receipt names the repository's configured PACT graph deployment;
        # there is no built-in default (#2823).
        monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "okg-workspace")

    def test_no_configured_deployment_is_surfaced_not_defaulted(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path)
        monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT")
        monkeypatch.chdir(tmp_path)

        out = pact_cli._record_external_receipt(root, CHANGE_ID, self._decided())

        receipt = out["external_authority_receipt"]
        assert receipt["written"] is False
        assert receipt["blockers"] == ["external_receipt_deployment_unconfigured"]
        assert "graph_projection.deployment" in receipt["reason"]
        assert not (root / RECEIPT_PATH).exists()

    def _decided(self, principal: str | None = "riley") -> dict[str, Any]:
        result = envelope.recorded(text="recorded: downstream-change approve")
        result["recorded"] = {"change": CHANGE_ID, "principal": principal}
        return result

    def test_a_declared_manifest_gets_a_receipt(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path)
        monkeypatch.setenv(
            "OKG_PACT_EXTERNAL_GENERATION_ID", "okg:okg-workspace:gen:4211"
        )

        out = pact_cli._record_external_receipt(root, CHANGE_ID, self._decided())

        assert out["external_authority_receipt"]["written"] is True
        assert (root / RECEIPT_PATH).is_file()
        # The receipt the command produced must be the one the gate accepts.
        assert _verify(root)["ok"] is True, _blockers(root)

    def test_an_undeclared_manifest_is_untouched(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path, declare="skip")
        monkeypatch.setenv(
            "OKG_PACT_EXTERNAL_GENERATION_ID", "okg:okg-workspace:gen:4211"
        )

        out = pact_cli._record_external_receipt(root, CHANGE_ID, self._decided())

        assert "external_authority_receipt" not in out
        assert not (root / RECEIPT_PATH).exists()

    def test_a_refused_decision_produces_nothing(self, tmp_path: Path) -> None:
        root = _seed(tmp_path)
        refused = envelope.refused(
            text="refused", message="no", blocked_by=["nope"]
        )

        out = pact_cli._record_external_receipt(root, CHANGE_ID, refused)

        assert "external_authority_receipt" not in out
        assert not (root / RECEIPT_PATH).exists()

    def test_a_failed_write_is_surfaced_not_buried(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An approval with no receipt is not a finished approval.

        Before this, the missing generation pin left `verdict: recorded` and
        exit 0 with the failure buried in a nested field, so the next thing
        the operator saw was the merge gate refusing for a reason nothing had
        announced.
        """
        root = _seed(tmp_path)
        monkeypatch.delenv("OKG_PACT_EXTERNAL_GENERATION_ID", raising=False)

        out = pact_cli._record_external_receipt(root, CHANGE_ID, self._decided())

        assert envelope.verdict_of(out) == envelope.DEFERRED
        assert "external_receipt_generation_unpinned" in out["blocked_by"]
        assert any(
            "OKG_PACT_EXTERNAL_GENERATION_ID" in command
            for command in out["next_commands"]
        )
        assert not (root / RECEIPT_PATH).exists()

    def test_a_decision_with_no_principal_writes_nothing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = _seed(tmp_path)
        monkeypatch.setenv(
            "OKG_PACT_EXTERNAL_GENERATION_ID", "okg:okg-workspace:gen:4211"
        )

        out = pact_cli._record_external_receipt(root, CHANGE_ID, self._decided(None))

        assert out["external_authority_receipt"]["written"] is False
        assert not (root / RECEIPT_PATH).exists()


class TestVisibleProvenance:
    """The surface explains itself on every result, accepted ones included."""

    def test_a_verified_receipt_still_reports_what_it_did_not_check(
        self, tmp_path: Path
    ) -> None:
        root = _seed(tmp_path)
        _write_receipt(root)

        status = _verify(root)

        assert status["ok"] is True
        assert status["attested"] is True
        assert status["verified_against_graph"] is False
        assert status["unverified"] == list(ext.UNVERIFIED_BY_RECEIPT)
        # The self digest detects edits, not authorship: say so.
        assert (
            "receipt_was_produced_by_a_recorded_decision"
            in ext.UNVERIFIED_BY_RECEIPT
        )
        assert status["provenance"]["receipt"]["generation_id"]
        assert status["provenance"]["checkout"]["head_sha"]

    def test_the_verdict_carries_scope_provenance_and_limits(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Through the real merge gate, with the ledger unreachable.

        An unreachable ledger is itself one of the contract's cases: the
        external surface must not rescue a merge the ledger did not approve.
        """
        root = _seed(tmp_path)
        _write_receipt(root)

        def no_ledger(**kwargs: Any) -> Any:
            raise SubstrateError(
                "decision ledger DSN is not configured",
                data={"cause": "no_dsn_configured"},
            )

        monkeypatch.setattr(pact_decisions, "require_ledger_connection", no_ledger)

        gate = pact_cli._ledger_decision_gate(root, CHANGE_ID)

        assert gate["ok"] is False
        assert gate["status"] == "ledger_unreachable"
        block = gate["workflow_authority"]["external_authority"]
        assert block["authority_scope"] == ext.AUTHORITY_SCOPE
        assert block["ok"] is True
        assert block["unverified"] == list(ext.UNVERIFIED_BY_RECEIPT)
        assert block["provenance"]["declaration"]["repository"] == EXTERNAL_REPO
        blockers = gate["workflow_authority"]["blockers"]
        assert "ledger_dsn_not_configured" in blockers
        assert not [b for b in blockers if b.startswith("graph_not_")]

    def test_the_gate_arm_lifts_the_block_and_names_the_scope(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`gate pre-merge` names the surface even when it ACCEPTS."""
        root = _seed(tmp_path)
        _write_receipt(root)
        block = {
            "authority_scope": ext.AUTHORITY_SCOPE,
            "ok": True,
            "blockers": [],
            "provenance": {"declaration": {"repository": EXTERNAL_REPO}},
            "attested": True,
            "verified_against_graph": False,
            "unverified": list(ext.UNVERIFIED_BY_RECEIPT),
        }

        def approved(root_arg: Path, change_id: str, capture: Any = None):
            if capture is not None:
                capture["ledger_consulted"] = True
                capture["decisions"] = []
            return envelope.component(
                ok=True,
                status="approved",
                intent_digest="sha256:" + "2" * 64,
                principal="riley",
                decided_at="2026-09-03T00:00:00Z",
                workflow_authority={
                    "accepted": True,
                    "blockers": [],
                    "projection_only": False,
                    "required_live_checks": [],
                    "external_authority": block,
                },
            )

        monkeypatch.setattr(pact_cli, "_ledger_decision_gate", approved)
        monkeypatch.setattr(
            pact_cli,
            "_simple_gate",
            lambda root_arg, change, name, lifecycle_gate=None: envelope.recorded(
                text=f"gate {name} {change}: recorded"
            ),
        )
        rc, payload = _cli(root, ("gate", "pre-merge", CHANGE_ID))

        assert rc == 0
        assert payload["verdict"] == envelope.RECORDED
        assert ext.AUTHORITY_SCOPE in payload["text"]
        assert payload["external_authority"]["unverified"] == list(
            ext.UNVERIFIED_BY_RECEIPT
        )
        assert payload["external_authority"]["verified_against_graph"] is False

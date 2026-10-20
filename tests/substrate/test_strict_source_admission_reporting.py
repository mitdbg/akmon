"""Lint reports what the strict path refuses, and the refusal names the field.

Issue #2126, findings F13 and F14. Before this, `okg deployment lint` returned
`ok: true` on a registry whose every source the next `okg source add --apply`
would refuse, and the refusal the operator did get read

    cause: ValueError: <redacted>: source 'apple_calendar_local': registry
           profile 'discovery_crawl' does not match class profile 'mutable_api'

with no file, no field, and the one word naming the defect eaten by the secret
redactor -- `source_class_profile_mismatch` is 29 characters of lower-case
snake_case, and `redact_text` replaces any 24-plus-character token.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

import okg.substrate.cli.main as cli
from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    planned_writes,
)
from okg.substrate.deployment_lint import (
    lint_deployment,
    source_activation_probe_findings,
)
from okg.substrate.ingest.adapter_factory import plan_source_registry_bindings
from okg.substrate.sources.guided_add import (
    _admission_cause,
    promote_registry_to_strict,
    _import_fix_hint,
    _lint_blocker,
    _lint_cause,
)
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    admit_source_registry_document,
)


MANIFEST = """\
deployment: strictlint
source_registry: source_registry.yaml
"""

REGISTRY = """\
admission_contract: strict_v1
sources:
  contradicts_its_reader:
    module: okg.substrate.library.sources.gmail_api
    class: GmailApiSource
    ownership_id: strictlint.contradicts_its_reader
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [message_id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params: {}
  authors_a_reserved_param:
    module: okg.substrate.library.sources.concept_aliases
    class: ConceptAliasesSource
    ownership_id: strictlint.authors_a_reserved_param
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [alias_key]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params:
      aliases_path: /strictlint/concept_aliases.yaml
      dsn: postgresql://okg@127.0.0.1:5999/strictlint
"""


@pytest.fixture()
def deployment(tmp_path: Path) -> Path:
    directory = tmp_path / "strictlint"
    directory.mkdir()
    (directory / "deployment.yaml").write_text(MANIFEST, encoding="utf-8")
    (directory / "source_registry.yaml").write_text(REGISTRY, encoding="utf-8")
    return directory


def _codes(findings: list[dict]) -> dict[str, dict]:
    return {str(finding["code"]): finding for finding in findings}


def test_lint_blocks_on_a_source_class_its_reader_contradicts(
    deployment: Path,
) -> None:
    report = lint_deployment(deployment, live=False)

    assert report["ok"] is False
    finding = _codes(report["findings"])[
        "deployment.source_registry.class_profile_mismatch"
    ]
    assert finding["severity"] == "blocker"
    # The operator has to be told which file, which key, and what to write.
    assert finding["location"]["path"] == str(
        deployment / "source_registry.yaml"
    )
    assert finding["location"]["pointer"] == (
        "/sources/contradicts_its_reader/source_class"
    )
    assert "'discovery_crawl'" in finding["observed"]
    assert "GmailApiSource.profile == 'mutable_api'" in finding["observed"]
    assert finding["expected"] == "source_class == 'mutable_api'"
    assert "source_registry.yaml" in finding["next_action"]


def test_lint_blocks_on_a_registry_authored_substrate_param(
    deployment: Path,
) -> None:
    report = lint_deployment(deployment, live=False)

    finding = _codes(report["findings"])[
        "deployment.source_registry.param_reserved"
    ]
    assert finding["severity"] == "blocker"
    assert finding["location"]["pointer"] == (
        "/sources/authors_a_reserved_param/params/dsn"
    )
    assert "delete params.dsn" in finding["next_action"]


def test_lint_stays_green_when_the_reader_and_the_registry_agree(
    tmp_path: Path,
) -> None:
    """The check must not fire on a correct entry, or it is not a check.

    Same reader as the mismatch case above, with the `mutable_api` tuple its
    class declares.
    """
    directory = tmp_path / "strictlint"
    directory.mkdir()
    (directory / "deployment.yaml").write_text(MANIFEST, encoding="utf-8")
    (directory / "source_registry.yaml").write_text(
        REGISTRY.replace(
            "    source_class: discovery_crawl\n"
            "    record_identity_kind: scoped_locator\n"
            "    record_identity_fields: [message_id]\n"
            "    source_revision_kind: content_hash\n",
            "    source_class: mutable_api\n"
            "    record_identity_kind: remote_id\n"
            "    record_identity_fields: [message_id]\n"
            "    source_revision_kind: version_token\n",
        ).replace(
            "      dsn: postgresql://okg@127.0.0.1:5999/strictlint\n",
            "",
        ),
        encoding="utf-8",
    )

    report = lint_deployment(directory, live=False)

    codes = _codes(report["findings"])
    assert "deployment.source_registry.class_profile_mismatch" not in codes
    assert "deployment.source_registry.param_reserved" not in codes


_STRICT_ADMISSION_CODES = frozenset({
    "deployment.source_registry.class_profile_mismatch",
    "deployment.source_registry.param_reserved",
    "deployment.source_registry.class_profile_invalid",
})


def _blockers(report: dict) -> set[tuple[str, str]]:
    return {
        (str(finding["code"]), str(finding["location"].get("pointer")))
        for finding in report["findings"]
        if finding["severity"] == "blocker"
    }


def _legacy_deployment(tmp_path: Path, registry_text: str) -> Path:
    directory = tmp_path / "strictlint"
    directory.mkdir()
    (directory / "deployment.yaml").write_text(MANIFEST, encoding="utf-8")
    (directory / "source_registry.yaml").write_text(
        registry_text.replace("admission_contract: strict_v1\n", ""),
        encoding="utf-8",
    )
    return directory


def _lint_as_add_source_validates(directory: Path) -> dict:
    """Lint the document the add-source save step writes.

    `promote_registry_to_strict` is the function `_save_definition` calls
    before the validate step runs `okg deployment lint`.
    """
    registry_path = directory / "source_registry.yaml"
    promoted, added = promote_registry_to_strict(
        yaml.safe_load(registry_path.read_text(encoding="utf-8")),
    )
    assert added, "the fixture must start as a legacy document"
    registry_path.write_text(
        yaml.safe_dump(promoted, sort_keys=False), encoding="utf-8",
    )
    return lint_deployment(directory, live=False)


def test_lint_blocks_a_legacy_registry_exactly_as_add_source_will(
    tmp_path: Path,
) -> None:
    """Issue #2150: lint said ok on a registry the next add-source refused.

    Severity followed the document's own `admission_contract`, and a
    scaffolded registry carries none, so every strict refusal was a warning.
    The first `okg deployment add-source` promotes the document to
    `strict_v1` and validates it with the same lint, which then blocked on
    sources the operator never touched. Lint on the legacy document must now
    give the verdict the validate step gives after the promotion.
    """
    directory = _legacy_deployment(tmp_path, REGISTRY)

    before = lint_deployment(directory, live=False)

    assert before["ok"] is False
    legacy_blockers = _blockers(before)
    assert {
        ("deployment.source_registry.class_profile_mismatch",
         "/sources/contradicts_its_reader/source_class"),
        ("deployment.source_registry.param_reserved",
         "/sources/authors_a_reserved_param/params/dsn"),
    } <= legacy_blockers
    for finding in before["findings"]:
        if finding["code"] in _STRICT_ADMISSION_CODES:
            # The operator is told why a registry the runtime runs is refused.
            assert "strict_v1" in finding["why"]

    after = _lint_as_add_source_validates(directory)

    assert after["ok"] is False
    assert _blockers(after) == legacy_blockers


def _scaffold_academic_researcher(tmp_path: Path) -> Path:
    """Write the academic-researcher scaffold exactly as an install renders it.

    `planned_writes` is the scaffold's own renderer; the registry it writes
    carries no `admission_contract`, like every installed deployment.
    """
    profile = load_profile(
        Path(__file__).resolve().parents[2]
        / "src/okg/substrate/library/profiles/academic-researcher/profile.yaml"
    )
    plan = build_plan(
        profile=profile,
        deployment_name="lintmatch",
        deployments_root=tmp_path,
        raw_answers={
            "deployment_name": "lintmatch",
            "postgres_dsn": "postgresql://okg@127.0.0.1:5999/lintmatch",
        },
    )
    for write in planned_writes(plan):
        path = Path(write.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(write.text, encoding="utf-8")
    return tmp_path / "lintmatch"


def test_a_scaffolded_registry_add_source_accepts_passes_lint(
    tmp_path: Path,
) -> None:
    """No false red: a correct legacy registry lints ok before and after."""
    directory = _scaffold_academic_researcher(tmp_path)

    before = lint_deployment(directory, live=False)
    after = _lint_as_add_source_validates(directory)

    assert before["ok"] is True, _blockers(before)
    assert after["ok"] is True, _blockers(after)


def test_a_scaffolded_registry_with_a_pre_2136_param_fails_lint(
    tmp_path: Path,
) -> None:
    """The installed-base case from issue #2150, on a real scaffold.

    A deployment scaffolded before #2136 keeps `params.dsn` on the sources
    that change cleaned. Lint used to report `ok: true` on it; the validate
    step after the add-source promotion refused. Both now refuse, on the
    same entry.
    """
    directory = _scaffold_academic_researcher(tmp_path)
    registry_path = directory / "source_registry.yaml"
    document = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert "admission_contract" not in document
    document["sources"]["concept_aliases"].setdefault("params", {})[
        "dsn"
    ] = "${OKG_DSN}"
    registry_path.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8",
    )

    before = lint_deployment(directory, live=False)
    after = _lint_as_add_source_validates(directory)

    expected = {(
        "deployment.source_registry.param_reserved",
        "/sources/concept_aliases/params/dsn",
    )}
    assert before["ok"] is False
    assert _blockers(before) == expected
    assert _blockers(after) == expected


def test_the_activation_seam_reports_the_same_refusals(
    deployment: Path,
) -> None:
    """The guided flow asks this seam before it promotes the registry.

    `guided_add._save_definition` rewrites the document as `strict_v1`, after
    which `okg ingest` plans every entry. The seam has to carry these codes or
    the flow still learns about them only from the import step.
    """
    registry_path = deployment / "source_registry.yaml"
    import yaml

    sources = yaml.safe_load(registry_path.read_text())["sources"]

    findings = source_activation_probe_findings(
        "contradicts_its_reader",
        sources["contradicts_its_reader"],
        deployment_dir=deployment,
        registry_path=registry_path,
    )

    assert "deployment.source_registry.class_profile_mismatch" in {
        str(finding["code"]) for finding in findings
    }


def test_the_import_refusal_keeps_its_code_and_names_the_registry_file() -> None:
    registry_path = Path("/deployments/walkpi/source_registry.yaml")
    cause = _admission_cause(
        "source_class_profile_mismatch: source 'apple_calendar_local' at "
        "/sources/apple_calendar_local/source_class: registry profile "
        "'discovery_crawl' does not match class profile 'mutable_api'",
        registry_path,
    )

    assert "<redacted>" not in cause
    assert cause.startswith("source_class_profile_mismatch: ")
    assert (
        f"at {registry_path}#/sources/apple_calendar_local/source_class:"
    ) in cause


def test_the_import_refusal_still_redacts_a_secret_in_the_cause() -> None:
    """Keeping the code must not widen what reaches the operator's terminal."""
    cause = _admission_cause(
        "source_param_reserved: source 'x' at /sources/x/params/dsn: "
        "connection to postgresql://okg:hunter2@db:5432/okg failed",
        Path("/deployments/walkpi/source_registry.yaml"),
    )

    assert "hunter2" not in cause
    assert cause.startswith("source_param_reserved: ")


def test_a_cause_with_no_diagnostic_code_is_redacted_whole() -> None:
    cause = _admission_cause(
        "postgresql://okg:hunter2@db:5432/okg is unreachable",
        Path("/deployments/walkpi/source_registry.yaml"),
    )

    assert "hunter2" not in cause


def test_the_import_fix_hint_points_at_the_registry_only_when_a_field_failed(
) -> None:
    """A lock timeout is not a registry defect.

    Observed from a wheel: the first `okg deployment add-source --apply` on a
    fixed academic-researcher install reaches the import step and fails with
    `LockNotAvailable` (issue #2123, the ingest deadlock). Telling the
    operator to edit a field that message never named sends them to the
    wrong file.
    """
    registry_path = Path("/deployments/walkpi/source_registry.yaml")

    assert _import_fix_hint(
        "LockNotAvailable: canceling statement due to lock timeout",
        registry_path,
    ) == "fix the named cause, then re-run the same command"

    assert str(registry_path) in _import_fix_hint(
        "source_param_reserved: source 'x' at /sources/x/params/dsn: "
        "params authors substrate-owned keys ['dsn']",
        registry_path,
    )


def test_the_validate_step_names_the_blocker_instead_of_the_deployment() -> None:
    cause = _lint_cause({
        "code": "deployment.source_registry.class_profile_mismatch",
        "path": "/deployments/walkpi/source_registry.yaml",
        "pointer": "/sources/dblp/source_class",
        "observed": "source_class == 'discovery_crawl'",
        "next_action": "set source_class to 'mutable_api'",
    })

    assert cause.startswith(
        "deployment.source_registry.class_profile_mismatch at "
    )
    assert (
        "/deployments/walkpi/source_registry.yaml#/sources/dblp/source_class"
    ) in cause


def test_the_validate_step_falls_back_when_lint_reported_no_blocker() -> None:
    assert _lint_cause({}) == (
        "the deployment does not validate with this source"
    )


#: A deployment-local reader whose `profile` is a string the substrate has no
#: profile for. Five library readers shipped exactly this defect
#: (`profile = "artifact_import"`); they are fixed, so the only way to exercise
#: the rule now is a deployment's own module -- which is the case lint's
#: AST-only contract exists for.
LOCAL_READER = '''\
class InventedProfileSource:
    profile = "artifact_import"
    probe_kind = "scope_complete"

    def __init__(self, notes_path: str) -> None:
        self.notes_path = notes_path
'''

LOCAL_REGISTRY = """\
admission_contract: strict_v1
sources:
  names_an_unreal_profile:
    module: local_reader
    class: InventedProfileSource
    ownership_id: strictlint.names_an_unreal_profile
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [notes_path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params:
      notes_path: /strictlint/notes
"""


def test_lint_blocks_when_the_reader_profile_is_not_a_profile_at_all(
    tmp_path: Path,
) -> None:
    """`class_profile_invalid` is a different defect from a mismatch.

    A mismatch names two real profiles and the operator can pick one. This
    names a string the matrix has never heard of, so no registry edit can
    make the source importable -- the fix is in the reader. The rule had no
    test: deleting the finding left every suite green (issue #2126 review,
    mutation M6).
    """
    directory = tmp_path / "strictlint"
    directory.mkdir()
    (directory / "deployment.yaml").write_text(MANIFEST, encoding="utf-8")
    (directory / "source_registry.yaml").write_text(
        LOCAL_REGISTRY, encoding="utf-8",
    )
    (directory / "local_reader.py").write_text(LOCAL_READER, encoding="utf-8")

    report = lint_deployment(directory, live=False)

    codes = _codes(report["findings"])
    finding = codes["deployment.source_registry.class_profile_invalid"]
    assert report["ok"] is False
    assert finding["severity"] == "blocker"
    # The defect is the class attribute, so the pointer names `/class`, not
    # `/source_class` -- editing source_class here would fix nothing.
    assert finding["location"]["pointer"] == (
        "/sources/names_an_unreal_profile/class"
    )
    assert finding["observed"] == (
        "InventedProfileSource.profile == 'artifact_import'"
    )
    assert "InventedProfileSource" in finding["next_action"]
    # A source the matrix cannot place is not also a mismatch: reporting both
    # would tell the operator to edit the registry, which cannot help.
    assert "deployment.source_registry.class_profile_mismatch" not in codes


def test_the_strict_refusal_points_at_the_field_not_just_the_source(
    deployment: Path,
) -> None:
    """The JSON pointer comes from the real gate, not a hand-built string.

    `_adapter_admission_error` is the whole of finding F14 on the adapter
    side, and dropping its `field` suffix left every suite green (issue #2126
    review, mutation M10) because the reporting tests fed `_admission_cause`
    a literal. This provokes the refusal through
    `plan_source_registry_bindings`, the call `okg ingest` makes.
    """
    registry_path = deployment / "source_registry.yaml"
    document = yaml.safe_load(registry_path.read_text(encoding="utf-8"))

    def _plan(name: str) -> str:
        admission = admit_source_registry_document(
            {
                "admission_contract": STRICT_ADMISSION_CONTRACT,
                "sources": {name: dict(document["sources"][name])},
            },
            registry_path=registry_path,
        )
        with pytest.raises(ValueError) as refusal:
            plan_source_registry_bindings(
                admission,
                dsn="postgresql://okg@127.0.0.1:5999/strictlint",
                deployment="strictlint",
            )
        return str(refusal.value)

    mismatch = _plan("contradicts_its_reader")
    assert mismatch.startswith("source_class_profile_mismatch: ")
    assert (
        "at /sources/contradicts_its_reader/source_class:" in mismatch
    ), mismatch

    reserved = _plan("authors_a_reserved_param")
    assert "at /sources/authors_a_reserved_param/params/dsn:" in reserved, (
        reserved
    )


def test_the_validate_step_reads_a_real_lint_payload(
    deployment: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`_lint_blocker` parses what the CLI actually prints.

    `_lint_cause` was unit-tested with a handmade dict, so making
    `_lint_blocker` return `{}` left every suite green (issue #2126 review,
    mutation M14) and the operator would have seen the generic "the
    deployment does not validate with this source" for a defect lint had
    located exactly. This runs the real `okg deployment lint --json` command
    and hands `_lint_blocker` the bytes it printed.
    """
    code = cli.main(["deployment", "lint", str(deployment), "--json"])
    printed = capsys.readouterr().out

    assert code != 0, "the fixture registry must fail lint for this to mean anything"
    proc = subprocess.CompletedProcess(
        ["okg", "deployment", "lint", str(deployment), "--json"],
        code,
        stdout=printed,
        stderr="",
    )

    payload = json.loads(printed)
    blocking = [
        finding for finding in payload["findings"]
        if finding.get("severity") == "blocker"
    ]
    # The fixture has to reach the rules this change added, or the test is
    # measuring the manifest rules instead.
    assert {
        "deployment.source_registry.class_profile_mismatch",
        "deployment.source_registry.param_reserved",
    } <= {str(finding["code"]) for finding in blocking}
    first = blocking[0]

    blocker = _lint_blocker(proc)

    assert blocker == {
        "code": str(first["code"]),
        "path": str(first.get("location", {}).get("path") or ""),
        "pointer": str(first.get("location", {}).get("pointer") or ""),
        "observed": str(first.get("observed") or ""),
        "next_action": str(first.get("next_action") or ""),
    }
    # The whole point of parsing it: the operator sees the located defect,
    # not just that the deployment did not validate.
    cause = _lint_cause(blocker)
    assert cause.startswith(blocker["code"])
    assert cause != "the deployment does not validate with this source"

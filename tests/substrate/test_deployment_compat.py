"""`okg deployment compat`: an offline answer to "does this deployment work
with the okg that is running?" (weekly-release design, decision 307 D3)."""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.deployment_compat import check_deployment_compat
from okg.substrate.release_contract import (
    CONTRACT_SCHEMA,
    build_contract,
    render_contract,
)


def _deployment(
    tmp_path: Path,
    *,
    adapter_source: str,
    params: str = "{}",
    manifest_extra: str = "",
) -> Path:
    """A legacy-registry deployment whose one source lives in the deployment.

    The module name is unique per test so an import from one test never
    satisfies another.
    """
    module = f"compat_adapters_{uuid.uuid4().hex[:8]}"
    dep = tmp_path / "compatdep"
    dep.mkdir(parents=True)
    (dep / "__init__.py").write_text("")
    (dep / f"{module}.py").write_text(adapter_source)
    (dep / "deployment.yaml").write_text(
        "name: compatdep\n"
        "source_registry: source_registry.yaml\n"
        + manifest_extra
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  notes:\n"
        f"    module: {module}\n"
        "    class: NotesAdapter\n"
        "    ownership_id: compatdep.notes\n"
        f"    params: {params}\n"
    )
    return dep


ADAPTER = """\
class NotesAdapter:
    def __init__(self, *, root: str, dsn: str = "", source_name: str = ""):
        self.root = root
"""


@pytest.fixture
def clean_lint(monkeypatch):
    """Stub `deployment lint` to a clean report.

    Lint has its own tests; stubbing it lets these tests reach the verdicts
    compat adds (compatible, unknown) without a fully authored deployment.
    """
    import okg.substrate.deployment_lint as deployment_lint

    def _clean(deployment, **_kw):  # noqa: ANN001, ANN202
        return {
            "deployment": "compatdep", "deployment_dir": str(deployment),
            "ok": True, "findings": [],
        }

    monkeypatch.setattr(deployment_lint, "lint_deployment", _clean)


def _codes(report: dict) -> set[str]:
    return {finding["code"] for finding in report["findings"]}


def test_matching_source_raises_no_source_finding(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")

    report = check_deployment_compat(dep)

    source_codes = {c for c in _codes(report) if c.startswith("compat.source.")}
    assert source_codes == set()
    assert report["schema"] == "okg.deployment_compat.v1"
    assert report["okg_version"]


def test_renamed_class_is_a_blocker_naming_the_classes_that_exist(
    tmp_path: Path,
) -> None:
    """archi-crab-okg's CMSSWReleaseSource -> CMSSWReleaseAdapter rename."""
    dep = _deployment(
        tmp_path,
        adapter_source=ADAPTER.replace("NotesAdapter", "NotesSource"),
        params="{root: /data}",
    )

    report = check_deployment_compat(dep)

    missing = [
        f for f in report["findings"] if f["code"] == "compat.source.class_missing"
    ]
    assert len(missing) == 1
    assert missing[0]["severity"] == "blocker"
    assert missing[0]["details"]["classes_in_module"] == ["NotesSource"]
    assert report["verdict"] == "incompatible"
    assert report["exit_code"] == 1


def test_class_hint_uses_the_module_exports(tmp_path: Path) -> None:
    """A module that re-exports its adapters (okg's own code_graph does)."""
    dep = _deployment(
        tmp_path,
        adapter_source=(
            "from collections import OrderedDict as NotesSource\n"
            "class _Private: ...\n"
            "__all__ = ['NotesSource', '_Private', 'not_a_class']\n"
            "not_a_class = 1\n"
        ),
    )

    report = check_deployment_compat(dep)

    missing = [
        f for f in report["findings"] if f["code"] == "compat.source.class_missing"
    ]
    assert missing[0]["details"]["classes_in_module"] == ["NotesSource"]


def test_module_that_does_not_import_is_a_blocker(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, adapter_source="import not_a_real_package_x\n")

    report = check_deployment_compat(dep)

    assert "compat.source.import_failed" in _codes(report)
    assert report["exit_code"] == 1


def test_reserved_and_unknown_params_are_reported(tmp_path: Path) -> None:
    dep = _deployment(
        tmp_path,
        adapter_source=ADAPTER,
        params="{root: /data, dsn: 'postgresql://x', colour: blue}",
    )

    report = check_deployment_compat(dep)

    codes = _codes(report)
    assert "compat.source.source_param_reserved" in codes
    assert "compat.source.source_param_unconsumed" in codes


def test_commands_file_flags_a_command_this_okg_refuses(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")
    commands = tmp_path / "commands.txt"
    commands.write_text(
        "# scripts/smoke.sh\n"
        "okg status --deployment compatdep --json\n"
        "uv run okg ingest --deployment compatdep --no-such-flag\n"
        "okg pact check something\n"
    )

    report = check_deployment_compat(dep, commands_file=commands)

    refused = [
        f for f in report["findings"] if f["code"] == "compat.cli.command_refused"
    ]
    assert len(refused) == 1
    assert "line 3" in refused[0]["message"]
    assert "--no-such-flag" in refused[0]["message"]
    assert "compat.cli.not_checked" in _codes(report)
    assert report["exit_code"] == 1


def test_against_lists_breaking_changes_touching_this_deployment(
    tmp_path: Path,
) -> None:
    """An older contract that had a flag this deployment's scripts use."""
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")
    old = build_contract()
    old["okg_version"] = "0.0.9"
    old["sections"]["cli"]["okg status --legacy-summary"] = {
        "kind": "option", "required": False, "choices": None, "value": "none",
    }
    old["sections"]["cli"]["okg doctor --retired"] = {
        "kind": "option", "required": False, "choices": None, "value": "none",
    }
    old_path = tmp_path / "old-contract.json"
    old_path.write_text(render_contract(old))
    commands = tmp_path / "commands.txt"
    commands.write_text("okg status --deployment compatdep --legacy-summary\n")

    report = check_deployment_compat(
        dep, commands_file=commands, against=old_path,
    )

    since = report["breaking_since"]
    assert since["from_version"] == "0.0.9"
    assert since["breaking_count"] == 2
    assert [row["item"] for row in since["affecting_this_deployment"]] == [
        "okg status --legacy-summary",
    ]
    assert report["exit_code"] == 1  # the command itself no longer parses


def test_a_clean_deployment_is_compatible(tmp_path: Path, clean_lint) -> None:
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")

    report = check_deployment_compat(dep)

    assert report["findings"] == []
    assert report["verdict"] == "compatible"
    assert report["exit_code"] == 0
    assert report["ok"] is True


def test_a_check_that_cannot_run_makes_the_verdict_unknown(
    tmp_path: Path, clean_lint,
) -> None:
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")
    bad_contract = tmp_path / "not-a-contract.json"
    bad_contract.write_text(json.dumps({"schema": "something.else"}))

    report = check_deployment_compat(dep, against=bad_contract)

    assert [f["code"] for f in report["findings"]] == ["compat.against.could_not_run"]
    assert report["verdict"] == "unknown"
    assert report["exit_code"] == 2


def test_a_missing_deployment_is_unknown_not_incompatible(tmp_path: Path) -> None:
    report = check_deployment_compat(tmp_path / "nowhere")

    assert [f["code"] for f in report["findings"]] == ["compat.deployment.not_found"]
    assert report["exit_code"] == 2


def test_nomos_audit_runs_only_when_the_deployment_runs_nomos(
    tmp_path: Path, clean_lint, monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    off = _deployment(tmp_path / "off", adapter_source=ADAPTER, params="{root: /data}")
    on = _deployment(
        tmp_path / "on", adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="nomos:\n  runtime_enforcement: enforce\n",
    )

    off_report = check_deployment_compat(off)
    on_report = check_deployment_compat(on)

    assert not [c for c in _codes(off_report) if c.startswith("compat.nomos.")]
    nomos = [f for f in on_report["findings"] if f["check"] == "nomos"]
    assert nomos and all(f["severity"] == "blocker" for f in nomos)
    assert on_report["exit_code"] == 1


def test_help_and_version_lines_are_not_checked_and_keep_json_clean(
    tmp_path: Path, clean_lint, capsys,
) -> None:
    """A --help line used to pass and print usage into the --json output."""
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")
    commands = tmp_path / "commands.txt"
    commands.write_text(
        "okg ingest --help --no-such-flag\n"
        "okg --version\n"
        "okg schema deployment.yaml\n"
        "okg --json status --deployment compatdep\n"
        "okg status 'unterminated\n"
    )

    rc = cli.main([
        "deployment", "compat", str(dep), "--commands", str(commands), "--json",
    ])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    by_code: dict[str, list[str]] = {}
    for finding in payload["findings"]:
        by_code.setdefault(finding["code"], []).append(finding["message"])
    assert len(by_code["compat.cli.not_checked"]) == 2
    assert "compat.cli.command_refused" not in by_code  # schema rewrite, --json global
    assert by_code["compat.cli.line_unreadable"][0].startswith("line 5")
    assert payload["verdict"] == "unknown"
    assert rc == 2


def test_cli_exit_code_follows_the_verdict(tmp_path: Path, capsys) -> None:
    dep = _deployment(
        tmp_path,
        adapter_source=ADAPTER.replace("NotesAdapter", "NotesSource"),
        params="{root: /data}",
    )

    rc = cli.main(["deployment", "compat", str(dep), "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["verdict"] == "incompatible"


def test_contract_is_deterministic_and_names_its_schema() -> None:
    first = render_contract(build_contract())
    second = render_contract(build_contract())

    assert first == second
    contract = json.loads(first)
    assert contract["schema"] == CONTRACT_SCHEMA
    sections = contract["sections"]
    assert "okg deployment compat" in sections["cli"]
    assert sections["cli"]["okg deployment compat --against"]["kind"] == "option"
    assert "okg pact" not in sections["cli"]
    assert "source_registry" in sections["deployment_keys"]
    assert "strict_v1:sources" in sections["source_registry_keys"]
    assert "dsn" in sections["reserved_source_params"]
    assert sections["exit_codes"]["3"]["meaning"].startswith("a publish ran")


@pytest.mark.parametrize(
    ("before", "after", "breaking"),
    [
        ({"required": False}, {"required": True}, True),
        ({"choices": ["a", "b"]}, {"choices": ["a"]}, True),
        ({"choices": ["a"]}, {"choices": ["a", "b"]}, False),
        ({"required": True}, {"required": False}, False),
        ({"types": ["string", "array"]}, {"types": ["string"]}, True),
    ],
)
def test_diff_classifies_tightening_as_breaking(before, after, breaking) -> None:
    from okg.substrate.release_contract import diff_contracts

    def _contract(attrs: dict) -> dict:
        return {
            "schema": CONTRACT_SCHEMA,
            "okg_version": "x",
            "sections": {"cli": {"okg thing --flag": attrs}},
        }

    diff = diff_contracts(_contract(before), _contract(after))

    assert bool(diff["breaking"]) is breaking
    assert bool(diff["changed"]) is not breaking
    row = (diff["breaking"] or diff["changed"])[0]
    assert row["change"] != "attributes changed"


def test_diff_reports_removed_items_as_breaking_and_new_items_as_added() -> None:
    from okg.substrate.release_contract import diff_contracts

    old = {"schema": CONTRACT_SCHEMA, "okg_version": "1",
           "sections": {"mcp_tools": {"search": {}, "retired": {}}}}
    new = {"schema": CONTRACT_SCHEMA, "okg_version": "2",
           "sections": {"mcp_tools": {"search": {}, "fresh": {}}}}

    diff = diff_contracts(old, new)

    assert diff["breaking"] == [
        {"section": "mcp_tools", "item": "retired", "change": "removed"},
    ]
    assert diff["added"] == [{"section": "mcp_tools", "item": "fresh"}]


def test_a_flag_that_stops_taking_a_value_is_breaking() -> None:
    """`--mode {a,b}` becoming a plain on/off flag breaks `--mode a`."""
    from okg.substrate.release_contract import diff_contracts

    def _contract(attrs: dict) -> dict:
        return {"schema": CONTRACT_SCHEMA, "okg_version": "x",
                "sections": {"cli": {"okg thing --mode": attrs}}}

    diff = diff_contracts(
        _contract({"kind": "option", "required": False,
                   "choices": ["a", "b"], "value": "one"}),
        _contract({"kind": "option", "required": False,
                   "choices": None, "value": "none"}),
    )

    assert [row["item"] for row in diff["breaking"]] == ["okg thing --mode"]
    assert "takes none value(s), was one" in diff["breaking"][0]["change"]


def test_an_untyped_key_that_gains_a_type_is_narrowed() -> None:
    from okg.substrate.release_contract import diff_contracts

    def _contract(types: list[str]) -> dict:
        return {"schema": CONTRACT_SCHEMA, "okg_version": "x",
                "sections": {"deployment_keys": {"extractor": {
                    "required": False, "types": types}}}}

    narrowed = diff_contracts(_contract(["any"]), _contract(["object"]))
    widened = diff_contracts(_contract(["object"]), _contract(["any"]))

    assert narrowed["breaking"] and not widened["breaking"]


def test_union_typed_schema_keys_record_their_branch_types() -> None:
    keys = build_contract()["sections"]["deployment_keys"]

    assert all(entry["types"] for entry in keys.values())
    assert any(
        len(entry["types"]) > 1 and "any" not in entry["types"]
        for entry in keys.values()
    )


def test_positionals_are_keyed_by_position() -> None:
    cli_items = build_contract()["sections"]["cli"]

    assert cli_items["okg deployment compat <1>"]["kind"] == "positional"
    assert not any(key.endswith("<deployment>") for key in cli_items)


def _bundle(tmp_path: Path, monkeypatch, contracts: dict[str, dict]) -> None:
    import gzip

    import okg.substrate.release_contract as rc

    root = tmp_path / "bundled"
    root.mkdir()
    for version, contract in contracts.items():
        (root / f"okg-contract-v{version}.json.gz").write_bytes(
            gzip.compress(render_contract(contract).encode()),
        )
    monkeypatch.setattr(rc, "_bundled_root", lambda: root)
    # Independent of the declared version: the running okg is a later one.
    # Patch the metadata lookup okg.__version__ reads, not the attribute:
    # okg has no real __version__ (a module __getattr__ answers it), so
    # monkeypatch's undo would store the installed version as a real
    # attribute and freeze it for every later test on this worker.
    import importlib.metadata

    real_version = importlib.metadata.version
    monkeypatch.setattr(
        importlib.metadata,
        "version",
        lambda name: "0.9.0" if name == "okg" else real_version(name),
    )


def test_bundle_helper_leaves_no_real_okg_version_behind(tmp_path: Path) -> None:
    """KILLS: a version patch that outlives its test.

    okg.__version__ comes from a module __getattr__. Undoing a setattr on it
    stored the installed version as a real attribute, so a later test on the
    same worker that patches the package metadata still read the installed
    version (scholarly User-Agent test, coverage-linux 2026-10-07).
    """
    import okg

    with pytest.MonkeyPatch.context() as patch:
        _bundle(tmp_path, patch, {})
        assert okg.__version__ == "0.9.0"
    assert "__version__" not in vars(okg)


def test_tested_with_diffs_against_the_bundled_contract(
    tmp_path: Path, clean_lint, monkeypatch,
) -> None:
    old = build_contract()
    old["okg_version"] = "0.2.0"
    old["sections"]["deployment_keys"]["legacy_block"] = {"required": False, "types": ["object"]}
    _bundle(tmp_path, monkeypatch, {"0.2.0": old})
    dep = _deployment(
        tmp_path, adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="release:\n  channel: dev\n  tested_with: v0.2.0\nlegacy_block: {}\n",
    )

    report = check_deployment_compat(dep)

    since = report["breaking_since"]
    assert since["from_version"] == "0.2.0"
    assert [row["item"] for row in since["affecting_this_deployment"]] == ["legacy_block"]
    # Decision compatexit A: a breaking change touching what the deployment
    # uses makes it incompatible, each change listed as a blocker.
    touching = [f for f in report["findings"] if f["code"] == "compat.breaking.touches_deployment"]
    assert [f["details"]["item"] for f in touching] == ["legacy_block"]
    assert all(f["severity"] == "blocker" for f in touching)
    assert "okg 0.2.0" in touching[0]["message"]
    assert report["verdict"] == "incompatible"
    assert report["exit_code"] == 1


def test_tested_with_without_a_bundled_contract_says_why(
    tmp_path: Path, clean_lint, monkeypatch,
) -> None:
    _bundle(tmp_path, monkeypatch, {})
    dep = _deployment(
        tmp_path, adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="release:\n  channel: dev\n  tested_with: 0.3.0\n",
    )

    report = check_deployment_compat(dep)

    codes = _codes(report)
    assert "compat.release.tested_with_not_bundled" in codes
    finding = next(
        f for f in report["findings"]
        if f["code"] == "compat.release.tested_with_not_bundled"
    )
    message = finding["message"]
    assert "from v0.2.0 on" in message and "outside the release cut" in message
    assert finding["severity"] == "warning"  # decision rcmissing A
    assert report["verdict"] == "compatible"
    assert report["exit_code"] == 0  # a warning, not a blocker


def test_bundled_versions_sort_numerically(tmp_path: Path, monkeypatch) -> None:
    from okg.substrate.release_contract import bundled_contract_versions

    _bundle(tmp_path, monkeypatch, {v: build_contract() for v in ("0.10.0", "0.2.0", "0.9.0")})

    assert bundled_contract_versions() == ["0.2.0", "0.9.0", "0.10.0"]


def test_tested_with_the_running_version_lists_nothing(
    tmp_path: Path, clean_lint, monkeypatch,
) -> None:
    _bundle(tmp_path, monkeypatch, {})
    dep = _deployment(
        tmp_path, adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="release:\n  channel: dev\n  tested_with: v0.9.0\n",
    )

    report = check_deployment_compat(dep)

    assert "breaking_since" not in report
    assert not any(f["check"] == "tested_with" for f in report["findings"])
    assert report["exit_code"] == 0


def test_an_unreadable_bundled_contract_makes_the_verdict_unknown(
    tmp_path: Path, clean_lint, monkeypatch,
) -> None:
    _bundle(tmp_path, monkeypatch, {})
    (tmp_path / "bundled" / "okg-contract-v0.2.0.json.gz").write_bytes(b"not gzip")
    dep = _deployment(
        tmp_path, adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="release:\n  channel: dev\n  tested_with: 0.2.0\n",
    )

    report = check_deployment_compat(dep)

    assert "compat.tested_with.could_not_run" in _codes(report)
    assert report["exit_code"] == 2


def _old_contract_with(**extra_keys: dict) -> dict:
    old = build_contract()
    old["okg_version"] = "0.2.0"
    old["sections"]["deployment_keys"].update(extra_keys)
    return old


def test_against_a_breaking_change_touching_the_deployment_is_incompatible(
    tmp_path: Path, clean_lint,
) -> None:
    old_path = tmp_path / "old-contract.json"
    old_path.write_text(render_contract(_old_contract_with(
        legacy_block={"required": False, "types": ["object"]},
        other_block={"required": False, "types": ["object"]},
    )))
    dep = _deployment(
        tmp_path, adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="legacy_block: {}\n",
    )

    report = check_deployment_compat(dep, against=old_path)

    touching = [f for f in report["findings"] if f["code"] == "compat.breaking.touches_deployment"]
    assert [f["details"]["item"] for f in touching] == ["legacy_block"]
    assert touching[0]["details"]["section"] == "deployment_keys"
    assert "release notes" in touching[0]["next_action"]
    assert report["verdict"] == "incompatible"
    assert report["exit_code"] == 1


def test_against_a_breaking_change_the_deployment_does_not_use_stays_compatible(
    tmp_path: Path, clean_lint,
) -> None:
    old_path = tmp_path / "old-contract.json"
    old_path.write_text(render_contract(_old_contract_with(
        other_block={"required": False, "types": ["object"]},
    )))
    dep = _deployment(tmp_path, adapter_source=ADAPTER, params="{root: /data}")

    report = check_deployment_compat(dep, against=old_path)

    assert report["breaking_since"]["breaking_count"] >= 1
    assert report["breaking_since"]["affecting_this_deployment"] == []
    assert report["verdict"] == "compatible"
    assert report["exit_code"] == 0


def test_an_unquoted_tested_with_number_is_refused(
    tmp_path: Path, clean_lint, monkeypatch,
) -> None:
    _bundle(tmp_path, monkeypatch, {})
    dep = _deployment(
        tmp_path, adapter_source=ADAPTER, params="{root: /data}",
        manifest_extra="release:\n  channel: dev\n  tested_with: 0.10\n",
    )

    report = check_deployment_compat(dep)

    finding = next(
        f for f in report["findings"]
        if f["code"] == "compat.release.tested_with_not_a_string"
    )
    assert "0.1" in finding["message"] and "quote" in finding["next_action"]
    assert report["verdict"] == "unknown"
    assert report["exit_code"] == 2

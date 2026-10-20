"""Prove authored schemas survive both distribution routes and real installs."""

from __future__ import annotations

import json
import os
from argparse import Namespace
from pathlib import Path
import subprocess
import sys
import tarfile
from urllib.parse import urldefrag, urlparse
import zipfile

import pytest

from okg.substrate import schema_resources
from okg.substrate.deployment_bootstrap import workflow
from okg.substrate.sources import registry

ROOT = Path(__file__).resolve().parents[2]


def test_checkout_schemas_are_authoritative(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas/source_registry.schema.json").write_text("{}")
    assert schema_resources.schemas_dir() == ROOT / "schemas"
    assert workflow.schemas_dir is schema_resources.schemas_dir
    assert registry.schemas_dir is schema_resources.schemas_dir


def test_missing_checkout_resources_do_not_fall_back(monkeypatch, tmp_path):
    module = tmp_path / "src/okg/substrate/schema_resources.py"
    module.parent.mkdir(parents=True)
    module.touch()
    (tmp_path / "pyproject.toml").touch()
    monkeypatch.setattr(schema_resources, "__file__", str(module))
    with pytest.raises(FileNotFoundError, match="schema resources missing"):
        schema_resources.schemas_dir()
    (tmp_path / "schemas").mkdir()
    target = tmp_path / "schemas/source_registry.schema.json"
    target.write_text('{"checkout": true}')
    assert json.loads((schema_resources.schemas_dir() / target.name).read_text()) == {
        "checkout": True,
    }


def test_zip_resources_need_no_extraction(monkeypatch, tmp_path):
    archive = tmp_path / "package.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("okg/_schemas/source_registry.schema.json", '{"zip": true}')
    monkeypatch.setattr(
        schema_resources, "__file__", "/installed/okg/substrate/schema_resources.py"
    )
    with zipfile.ZipFile(archive) as bundle:
        monkeypatch.setattr(
            schema_resources, "files", lambda package: zipfile.Path(bundle, "okg/")
        )
        schema = schema_resources.schemas_dir() / "source_registry.schema.json"
        assert json.loads(schema.read_text()) == {"zip": True}


def test_missing_installed_resources_fail(monkeypatch, tmp_path):
    monkeypatch.setattr(
        schema_resources, "__file__", "/installed/okg/substrate/schema_resources.py"
    )
    monkeypatch.setattr(schema_resources, "files", lambda package: tmp_path)
    with pytest.raises(FileNotFoundError, match="schema resources missing"):
        schema_resources.schemas_dir()


def test_corrupt_schema_does_not_relax_admission(monkeypatch, tmp_path):
    (tmp_path / "source_registry.schema.json").write_text("not JSON")
    monkeypatch.setattr(registry, "schemas_dir", lambda: tmp_path)
    with pytest.raises(json.JSONDecodeError):
        registry.admit_source_registry_document(
            {"admission_contract": "strict_v1", "sources": {}},
            registry_path=tmp_path / "source_registry.yaml",
        )


@pytest.mark.parametrize("contents", ["not JSON", '{"type": 42}'])
def test_schema_show_refuses_malformed_resource(
    monkeypatch, tmp_path, capsys, contents
):
    from okg.substrate.cli.main import _cmd_schema_show

    (tmp_path / "source_registry.schema.json").write_text(contents)
    monkeypatch.setattr(schema_resources, "schemas_dir", lambda: tmp_path)
    result = _cmd_schema_show(Namespace(file="source_registry.yaml"), as_json=True)
    output = capsys.readouterr()
    assert result != 0
    assert "schema file invalid" in output.out + output.err
    assert output.out.strip() != contents


def _refs(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "$ref":
                yield child
            else:
                yield from _refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from _refs(child)


def test_schema_local_references_are_bundled():
    schemas = {
        path.name: json.loads(path.read_text())
        for path in (ROOT / "schemas").glob("*.json")
    }
    for name, schema in schemas.items():
        for ref in _refs(schema):
            target, fragment = urldefrag(ref)
            if urlparse(target).scheme:
                continue
            assert not target or target in schemas, (name, ref)
            referenced = schemas[target] if target else schema
            if fragment:
                assert fragment.startswith("/"), (name, ref)
                for part in fragment[1:].split("/"):
                    referenced = referenced[part.replace("~1", "/").replace("~0", "~")]


def _run(command, *, cwd):
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run(
        command, cwd=cwd, env=env, text=True, capture_output=True, timeout=600
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    return result.stdout


@pytest.mark.slow
def test_wheel_and_sdist_installed_schema_consumers(tmp_path):
    canonical = {
        path.name: path.read_bytes() for path in (ROOT / "schemas").glob("*.json")
    }
    direct = tmp_path / "direct"
    _run(["uv", "build", "--wheel", "--out-dir", str(direct)], cwd=ROOT)
    _run(["uv", "build", "--sdist", "--out-dir", str(direct)], cwd=ROOT)
    sdist = next(direct.glob("*.tar.gz"))
    extracted = tmp_path / "extracted"
    with tarfile.open(sdist) as archive:
        archive.extractall(extracted, filter="data")
    rebuilt = tmp_path / "rebuilt"
    _run(
        ["uv", "build", "--wheel", "--out-dir", str(rebuilt)],
        cwd=next(extracted.iterdir()),
    )
    wheels = [next(directory.glob("*.whl")) for directory in (direct, rebuilt)]
    for wheel in wheels:
        with zipfile.ZipFile(wheel) as archive:
            observed = {
                Path(name).name: archive.read(name)
                for name in archive.namelist()
                if name.startswith("okg/_schemas/") and name.endswith(".json")
            }
            assert observed == canonical
    venv = tmp_path / "installed"
    _run(["uv", "venv", "--python", sys.executable, str(venv)], cwd=tmp_path)
    python = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    _run(
        ["uv", "pip", "install", "--python", str(python), str(wheels[1])], cwd=tmp_path
    )
    # A plausible CWD resource must not override the imported distribution.
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas/source_registry.schema.json").write_text("{}")
    output = _run(
        [
            str(python),
            "-I",
            "-c",
            """
import json
from importlib.metadata import distribution
from pathlib import Path
import okg
from okg.deployment import ConnectorRegistration, SdkVersionRange, registration_registry_entry
from okg.substrate.schema_resources import schemas_dir
from okg.substrate.sources.registry import admit_source_registry_document
registration = ConnectorRegistration(
    connector_type="packaged_records", version="1.0.0", module="external.records",
    class_name="Records", ownership_id="external.records", source_class="reference_catalog",
    trust_label="external", sdk_range=SdkVersionRange("1.0.0", "2.0.0"),
    output_signature={"nodes": [{"subtype": "Document"}]},
    record_identity_kind="domain_key", record_identity_fields=("id",),
    source_revision_kind="content_hash", deletion_semantics="missing_from_completed_scope",
    publication_mode="published_generation",
)
document = {"admission_contract": "strict_v1", "sources": {
    "packaged_records": registration_registry_entry(registration),
}}
admitted = admit_source_registry_document(document, registry_path=Path("source_registry.yaml"))
assert "packaged_records" in admitted.entries
document["sources"]["packaged_records"]["unexpected"] = True
try:
    admit_source_registry_document(document, registry_path=Path("source_registry.yaml"))
except ValueError as exc:
    assert "source_registry_strict_schema" in str(exc)
else:
    raise AssertionError("invalid registry was admitted")
resources = schemas_dir()
assert resources == Path(okg.__file__).parent / "_schemas"
record = {str(path) for path in distribution("okg").files}
assert all("okg/_schemas/" + path.name in record for path in resources.iterdir())
print(json.dumps({path.name: path.read_text() for path in resources.iterdir()}))
""",
        ],
        cwd=tmp_path,
    )
    assert {
        name: value.encode() for name, value in json.loads(output).items()
    } == canonical
    for filename in canonical:
        authored = filename.replace(".schema.json", ".yaml")
        if authored == "aliases.yaml":
            authored = "aliases.json"
        shown = _run(
            [
                str(python),
                "-I",
                "-c",
                "from okg.substrate.cli.main import main; raise SystemExit(main())",
                "schema",
                "show",
                authored,
            ],
            cwd=tmp_path,
        )
        assert json.loads(shown) == json.loads(canonical[filename])

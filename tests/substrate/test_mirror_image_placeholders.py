"""This copy names no private image or repository as a default.

okg's container images and the reference-deployments repository are not
published, so every default that pointed at them is a placeholder the reader
must set, and each one refuses loudly when it is left unset:

* the Kubernetes chart in this source tree has no image registry, and its
  schema refuses to render (the released AKMON chart sets its own images);
* the chat image pin is under the reserved ``.invalid`` domain, so a pull fails;
* ``okg fetch-deployments`` needs ``--repo`` or ``OKG_DEPLOYMENTS_REPO`` to
  fetch; listing an already-fetched copy by ``--source-name`` needs neither.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

from okg.substrate.chat.instance import (
    PINNED_IMAGE_REF,
    TESTED_IMAGES,
    ChatImageNotTested,
    resolve_instance_image,
)
from okg.substrate.cli import fetch
from okg.substrate.cli.main import _build_parser
from okg.substrate.deployments.chat_block import (
    EXAMPLE_PINNED_IMAGE,
    PINNED_IMAGE_REFERENCE,
)
from scripts.verify_kubernetes_chart import _base_sets

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "ops" / "kubernetes" / "okg"
REGISTRY_SETS = {
    "images.okg.repository=registry.example/okg",
    "postgresql.image.repository=registry.example/okg-postgres",
}


def _helm() -> str:
    explicit = os.environ.get("HELM_BIN")
    if explicit and Path(explicit).is_file():
        return explicit
    found = shutil.which("helm")
    if found:
        return found
    return pytest.skip("Helm is not installed; the schema test above still runs")


def _sets_without_registry() -> list[str]:
    sets = _base_sets("kubernetes-smoke", set_storage_defaults=True, internal_postgres=True)
    kept: list[str] = []
    for flag, value in zip(sets[::2], sets[1::2]):
        if value not in REGISTRY_SETS:
            kept += [flag, value]
    return kept


def test_chart_names_no_image_registry() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["images"]["okg"]["repository"] == ""
    assert values["postgresql"]["image"]["repository"] == ""
    schema = json.loads((CHART / "values.schema.json").read_text(encoding="utf-8"))
    paths = {tuple(error.absolute_path) for error in Draft202012Validator(schema).iter_errors(values)}
    assert ("images", "okg", "repository") in paths
    assert ("postgresql", "image", "repository") in paths


def test_helm_refuses_to_render_until_the_registry_is_set() -> None:
    helm = _helm()
    command = [helm, "template", "probe", str(CHART)]
    refused = subprocess.run(
        [*command, *_sets_without_registry()], capture_output=True, text=True, check=False,
    )
    assert refused.returncode != 0
    assert "repository" in refused.stderr
    rendered = subprocess.run(
        [*command, *_base_sets("kubernetes-smoke", set_storage_defaults=True, internal_postgres=True)],
        capture_output=True, text=True, check=False,
    )
    assert rendered.returncode == 0, rendered.stderr
    assert "registry.example/okg:verification" in rendered.stdout
    assert "registry.example/okg-postgres:verification" in rendered.stdout


def test_chat_image_pin_is_an_unpullable_placeholder() -> None:
    assert PINNED_IMAGE_REF.startswith("your-registry.invalid/okg-openwebui-compat@sha256:")
    assert PINNED_IMAGE_REFERENCE.match(PINNED_IMAGE_REF)
    assert set(TESTED_IMAGES) == {PINNED_IMAGE_REF}
    assert EXAMPLE_PINNED_IMAGE == PINNED_IMAGE_REF


def test_untested_image_hint_says_the_default_is_a_placeholder() -> None:
    class _Block:
        image = "docker.io/library/nginx:v1.2.3"
        raw = {"image": image}

    with pytest.raises(ChatImageNotTested) as caught:
        resolve_instance_image("placeholder-probe", _Block())
    hint = caught.value.fix_hint
    assert "the default chat image is a placeholder" in hint
    assert "ops/openwebui/Dockerfile" in hint
    assert "TESTED_IMAGES" in hint


def test_fetch_deployments_has_no_default_repository() -> None:
    assert fetch.DEFAULT_REPO_URL is None


@pytest.mark.parametrize(
    ("list_only", "source_name"), [(False, None), (True, None), (False, "cached")],
)
def test_fetch_deployments_refuses_without_a_repository(
    list_only: bool,
    source_name: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("OKG_DEPLOYMENTS_REPO", raising=False)
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    args = argparse.Namespace(repo=None, branch=None, source_name=source_name, list=list_only)
    assert fetch.cmd_fetch_deployments(args, as_json=True) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "repo_required"
    assert "--repo" in payload["hint"]
    assert "OKG_DEPLOYMENTS_REPO" in payload["hint"]
    assert not (tmp_path / "cache").exists()
    assert not (tmp_path / "xdg").exists()


def test_fetch_deployments_lists_a_fetched_copy_by_name_without_a_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """--list --source-name reads only the local cache, so it needs no remote."""

    monkeypatch.delenv("OKG_DEPLOYMENTS_REPO", raising=False)
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    from okg.substrate.deployments import _cache_dir

    profile = _cache_dir() / "deployments" / "cached" / "demo"
    profile.mkdir(parents=True)
    (profile / "profile.yaml").write_text("name: demo\n", encoding="utf-8")
    args = argparse.Namespace(repo=None, branch=None, source_name="cached", list=True)
    assert fetch.cmd_fetch_deployments(args, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["profiles"] == ["demo"]


def test_fetch_deployments_takes_the_repository_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "no-such-repository"
    monkeypatch.setenv("OKG_DEPLOYMENTS_REPO", f"file://{missing}")
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    args = argparse.Namespace(repo=None, branch=None, source_name=None, list=False)
    assert fetch.cmd_fetch_deployments(args, as_json=True) != 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "fetch_failed"
    assert payload["repo"] == f"file://{missing}"


def test_fetch_deployments_help_names_no_default(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["fetch-deployments", "--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "Required: this copy names no default" in text
    assert "mitdbg" not in text

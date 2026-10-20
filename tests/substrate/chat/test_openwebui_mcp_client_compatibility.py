"""Hermetic contracts for the pinned OpenWebUI MCP client compatibility patch."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
from pathlib import Path
import re
import sys
from typing import Any
from uuid import uuid4

from mcp.types import CallToolResult, TextContent
import pytest


ROOT = Path(__file__).resolve().parents[3]
PATCHER_PATH = ROOT / "ops" / "openwebui" / "apply_mcp_client_patch.py"
DOCKERFILE_PATH = ROOT / "ops" / "openwebui" / "Dockerfile"
NOTICE_DIR = ROOT / "ops" / "openwebui" / "upstream-notices"
FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "openwebui_v0_11_4_call_tool.py"
)
FIXTURE_SHA256 = "7171ef25f55c785bce858e110c59f8cbfc612ef47303ba290c2300f343c6f422"
NOTICE_SHA256 = {
    "LICENSE": "5f1bd74c48bf13ab0f82e177ad9e637313b92533d20ead2593d49347a47fc232",
    "LICENSE_HISTORY": "b1567cc9763241a41569e01f08272a167dac57273f717b95b31ba7b5ec935e17",
}


def _load(path: Path, stem: str) -> Any:
    name = f"{stem}_{uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


patcher = _load(PATCHER_PATH, "openwebui_mcp_patcher")


def test_image_build_pins_provenance_and_runs_both_fail_closed_checks() -> None:
    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    assert (
        "ghcr.io/open-webui/open-webui@sha256:"
        "9591b13f13843c7721c2b8eaf7382846c81b3ffe126526d1888d1fed50c6a33f"
        in dockerfile
    )
    assert dockerfile.count(
        "FROM ghcr.io/open-webui/open-webui@sha256:"
        "9591b13f13843c7721c2b8eaf7382846c81b3ffe126526d1888d1fed50c6a33f"
    ) == 2
    assert " AS verifier" in dockerfile
    patch = "python /tmp/okg-apply-mcp-client-patch.py"
    verify = "python /tmp/okg-run-mcp-compatibility.py"
    assert dockerfile.count(patch) == 1
    assert dockerfile.count(verify) == 1
    assert dockerfile.index(patch) < dockerfile.index(verify)
    assert (
        "COPY ops/openwebui/run_compatibility_verifier.py "
        "/tmp/okg-run-mcp-compatibility.py"
    ) in dockerfile
    assert "trap 'rm -rf \"$verify_root\"' 0" in dockerfile
    for assignment in (
        'OKG_OPENWEBUI_VERIFY_ROOT="$verify_root"',
        'DATA_DIR="$verify_root/data"',
        'DATABASE_URL="sqlite:///$verify_root/data/webui.db"',
        'HOME="$verify_root/home"',
        "PYTHONPATH=/app/backend",
        'STATIC_DIR="$verify_root/static"',
        'XDG_CACHE_HOME="$verify_root/cache/xdg"',
        'HF_HOME="$verify_root/cache/huggingface"',
        'SENTENCE_TRANSFORMERS_HOME="$verify_root/cache/sentence-transformers"',
        'WHISPER_MODEL_DIR="$verify_root/cache/whisper"',
        'TIKTOKEN_CACHE_DIR="$verify_root/cache/tiktoken"',
        'AUDIT_LOGS_FILE_PATH="$verify_root/data/audit.log"',
        "ENABLE_AUDIT_LOGS_FILE=False",
        "ENABLE_DB_MIGRATIONS=False",
        "ENABLE_PLUGINS=False",
        "ENABLE_VERSION_UPDATE_CHECK=False",
        "CUSTOM_NAME=",
        "HF_HUB_OFFLINE=1",
        "OFFLINE_MODE=True",
        "OAUTH_SESSION_TOKEN_ENCRYPTION_KEY="
        "okg-public-verifier-fixture",
        "WEBUI_AUTH=False",
    ):
        assert assignment in dockerfile
    assert patcher.UPSTREAM_CLIENT_SHA256 in dockerfile
    assert patcher.PATCHED_CLIENT_SHA256 in dockerfile
    assert "ARG OPENWEBUI_BASE_IMAGE" not in dockerfile
    for name in NOTICE_SHA256:
        assert f"COPY ops/openwebui/upstream-notices/{name} " in dockerfile
    assert (
        "COPY --from=verifier /app/backend/open_webui/utils/mcp/client.py "
        "/app/backend/open_webui/utils/mcp/client.py"
    ) in dockerfile
    assert (
        "COPY --from=verifier /usr/share/licenses/okg-openwebui-compat/ "
        "/usr/share/licenses/okg-openwebui-compat/"
    ) in dockerfile
    assert "docker push" not in dockerfile

    final_stage = dockerfile.rsplit("\nFROM ", maxsplit=1)[1]
    assert "OAUTH_SESSION_TOKEN_ENCRYPTION_KEY" not in final_stage
    assert "okg-public-verifier-fixture" not in final_stage

    verifier = (ROOT / "ops" / "openwebui" / "verify_compatibility.py").read_text(
        encoding="utf-8"
    )
    assert 'Path("/app/backend/open_webui").resolve()' in verifier
    assert "package_origin.is_relative_to(pinned_package_root)" in verifier
    assert '_require_inside(root, STATIC_DIR, "STATIC_DIR")' in verifier


def test_base_labels_agree_with_the_pinned_base_image() -> None:
    """A base bump must move the FROM digest and every label that names it."""

    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")
    froms = re.findall(
        r"^FROM ghcr\.io/open-webui/open-webui@(sha256:[0-9a-f]{64})",
        dockerfile,
        flags=re.MULTILINE,
    )
    assert len(froms) == 2 and froms[0] == froms[1]

    def label(name: str) -> str:
        found = re.findall(rf'{re.escape(name)}="([^"]+)"', dockerfile)
        assert len(found) == 1, name
        return found[0]

    assert label("org.opencontainers.image.base.digest") == froms[0]
    base_name = label("org.opencontainers.image.base.name")
    assert base_name.startswith("ghcr.io/open-webui/open-webui:v")
    upstream_version = base_name.rsplit(":v", maxsplit=1)[1]
    assert label("org.opencontainers.image.version").startswith(
        f"{upstream_version}-okg-"
    )
    assert (
        label("org.mitdbg.okg.openwebui.client-source.sha256")
        == patcher.UPSTREAM_CLIENT_SHA256
    )
    assert (
        label("org.mitdbg.okg.openwebui.client-patched.sha256")
        == patcher.PATCHED_CLIENT_SHA256
    )


def test_official_upstream_notices_are_retained_byte_for_byte() -> None:
    for name, expected in NOTICE_SHA256.items():
        assert hashlib.sha256((NOTICE_DIR / name).read_bytes()).hexdigest() == expected


class _Session:
    def __init__(self, result: CallToolResult) -> None:
        self.result = result

    async def call_tool(
        self, function_name: str, function_args: dict[str, Any]
    ) -> CallToolResult:
        assert function_name == "inspect"
        assert function_args == {"node_id": "n:1"}
        return self.result


def _call(client_type: type, result: CallToolResult) -> Any:
    client = client_type()
    client.session = _Session(result)
    return asyncio.run(client.call_tool("inspect", {"node_id": "n:1"}))


def _patched_client(tmp_path: Path) -> type:
    target = tmp_path / "client.py"
    source = FIXTURE_PATH.read_bytes()
    target.write_bytes(source)
    patched = source.replace(
        patcher.UPSTREAM_RESULT_SELECTION,
        patcher.COMPATIBLE_RESULT_SELECTION,
    )
    patcher.patch_client(
        target,
        expected_source_sha256=hashlib.sha256(source).hexdigest(),
        expected_patched_sha256=hashlib.sha256(patched).hexdigest(),
    )
    return _load(target, "patched_openwebui_client").MCPClient


def test_frozen_method_is_exact_and_still_exhibits_the_pinned_data_loss() -> None:
    source = FIXTURE_PATH.read_bytes()
    assert hashlib.sha256(source).hexdigest() == FIXTURE_SHA256
    assert source.count(patcher.UPSTREAM_RESULT_SELECTION) == 1

    client_type = _load(FIXTURE_PATH, "pinned_openwebui_client").MCPClient
    success = CallToolResult(
        content=[],
        structuredContent={"items": [{"node_id": "n:1"}]},
    )
    assert _call(client_type, success) == []

    refusal = CallToolResult(
        content=[],
        structuredContent={"error": {"code": "access_denied"}},
        isError=True,
    )
    with pytest.raises(Exception) as caught:
        _call(client_type, refusal)
    assert caught.value.args == ([],)


def test_patcher_refuses_source_digest_drift_before_writing(tmp_path: Path) -> None:
    target = tmp_path / "client.py"
    source = FIXTURE_PATH.read_bytes()
    target.write_bytes(source)

    with pytest.raises(patcher.PatchRefused, match="source drifted"):
        patcher.patch_client(target)

    assert target.read_bytes() == source


@pytest.mark.parametrize("copies", [0, 2])
def test_patcher_requires_exactly_one_result_selection(
    tmp_path: Path, copies: int
) -> None:
    target = tmp_path / "client.py"
    source = b"prefix\n" + patcher.UPSTREAM_RESULT_SELECTION * copies + b"suffix\n"
    target.write_bytes(source)

    with pytest.raises(
        patcher.PatchRefused,
        match=f"must occur exactly once; found {copies}",
    ):
        patcher.patch_client(
            target,
            expected_source_sha256=hashlib.sha256(source).hexdigest(),
            expected_patched_sha256="unused",
        )
    assert target.read_bytes() == source


def test_patcher_refuses_unreviewed_patched_bytes_before_writing(
    tmp_path: Path,
) -> None:
    target = tmp_path / "client.py"
    source = FIXTURE_PATH.read_bytes()
    target.write_bytes(source)

    with pytest.raises(patcher.PatchRefused, match="reviewed result"):
        patcher.patch_client(
            target,
            expected_source_sha256=hashlib.sha256(source).hexdigest(),
            expected_patched_sha256="0" * 64,
        )
    assert target.read_bytes() == source


def test_patched_client_preserves_real_sdk_result_shapes(tmp_path: Path) -> None:
    client_type = _patched_client(tmp_path)

    legacy = CallToolResult(
        content=[TextContent(type="text", text='{"legacy": true}')],
        structuredContent=None,
    )
    legacy_dump = legacy.model_dump(mode="json")
    assert legacy_dump["structuredContent"] is None
    assert _call(client_type, legacy) == legacy_dump["content"]

    assert _call(
        client_type,
        CallToolResult(content=[], structuredContent={}),
    ) == {}

    success = {"items": [{"node_id": "n:1"}], "total_count": 1}
    selected = _call(
        client_type,
        CallToolResult(
            content=[TextContent(type="text", text='{"legacy_duplicate": true}')],
            structuredContent=success,
        ),
    )
    assert selected == success
    assert list(selected) == ["items", "total_count"]

    for detail in (
        {"error": {"code": "access_denied", "message": "not allowed"}},
        {},
    ):
        with pytest.raises(Exception) as caught:
            _call(
                client_type,
                CallToolResult(
                    content=[],
                    structuredContent=detail,
                    isError=True,
                ),
            )
        assert caught.value.args == (detail,)

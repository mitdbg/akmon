"""Apply OKG's source-guarded OpenWebUI v0.11.4 MCP client correction."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import stat
import tempfile


CLIENT_PATH = Path("/app/backend/open_webui/utils/mcp/client.py")
UPSTREAM_CLIENT_SHA256 = (
    "f46c5e7e01cf44bad0e75716f707776437aaf8e2aa7927e511185a014d0e64bb"
)
PATCHED_CLIENT_SHA256 = (
    "12fac6a236a4eb6464ab18b3970c76be3b030355cb0a30e6a6ee2667d94e9813"
)

# This excerpt is copied from OpenWebUI v0.11.4, where it is byte-identical to
# v0.11.0; only the whole-file digests above changed (v0.11.4 paginates
# list_tools). The official source and its LICENSE and LICENSE_HISTORY
# notices are retained under ops/openwebui/.
# https://github.com/open-webui/open-webui/blob/v0.11.4/backend/open_webui/utils/mcp/client.py
UPSTREAM_RESULT_SELECTION = b"""        result_dict = result.model_dump(mode='json')
        result_content = result_dict.get('content', {})

        if result.isError:
            raise Exception(result_content)
        else:
            return result_content
"""

COMPATIBLE_RESULT_SELECTION = b"""        result_dict = result.model_dump(mode='json')
        structured_content = result_dict.get('structuredContent')
        result_content = (
            structured_content
            if structured_content is not None
            else result_dict.get('content', {})
        )

        if result.isError:
            raise Exception(result_content)
        else:
            return result_content
"""


class PatchRefused(RuntimeError):
    """The pinned source does not match the reviewed patch input or output."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def patch_client(
    path: Path,
    *,
    expected_source_sha256: str = UPSTREAM_CLIENT_SHA256,
    expected_patched_sha256: str = PATCHED_CLIENT_SHA256,
) -> str:
    """Patch one exact upstream client file and return its verified digest."""

    source = path.read_bytes()
    source_digest = _sha256(source)
    if source_digest != expected_source_sha256:
        raise PatchRefused(
            "OpenWebUI MCP client source drifted: "
            f"expected sha256:{expected_source_sha256}, "
            f"found sha256:{source_digest}"
        )

    matches = source.count(UPSTREAM_RESULT_SELECTION)
    if matches != 1:
        raise PatchRefused(
            "OpenWebUI MCP result-selection block must occur exactly once; "
            f"found {matches}"
        )

    patched = source.replace(
        UPSTREAM_RESULT_SELECTION,
        COMPATIBLE_RESULT_SELECTION,
        1,
    )
    patched_digest = _sha256(patched)
    if patched_digest != expected_patched_sha256:
        raise PatchRefused(
            "patched OpenWebUI MCP client bytes differ from the reviewed result: "
            f"expected sha256:{expected_patched_sha256}, "
            f"found sha256:{patched_digest}"
        )

    mode = stat.S_IMODE(path.stat().st_mode)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.okg-",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(patched)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)

    readback_digest = _sha256(path.read_bytes())
    if readback_digest != expected_patched_sha256:
        raise PatchRefused(
            "patched OpenWebUI MCP client readback differs: "
            f"expected sha256:{expected_patched_sha256}, "
            f"found sha256:{readback_digest}"
        )
    return readback_digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=CLIENT_PATH)
    args = parser.parse_args()
    digest = patch_client(args.path)
    print(f"patched OpenWebUI MCP client sha256:{digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

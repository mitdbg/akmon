"""Run the pinned OpenWebUI verifier with a bounded failure diagnostic."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys
from types import ModuleType, TracebackType
from typing import NoReturn


VERIFIER_PATH = Path("/tmp/okg-verify-mcp-compatibility.py")
SENTINEL = "OKG_OPENWEBUI_DIAGNOSTIC_V1"

_EXCEPTION_CODES = {
    ModuleNotFoundError: "ModuleNotFoundError",
    ImportError: "ImportError",
    AssertionError: "AssertionError",
    RuntimeError: "RuntimeError",
    TypeError: "TypeError",
    ValueError: "ValueError",
    KeyError: "KeyError",
    OSError: "OSError",
}


def _exception_code(exc: BaseException) -> str:
    for exception_type, code in _EXCEPTION_CODES.items():
        if isinstance(exc, exception_type):
            return code
    return "OtherException"


def _failure_code(exc: BaseException) -> str:
    if isinstance(exc, ModuleNotFoundError):
        return "module_not_found"
    if isinstance(exc, ImportError):
        return "import_error"
    if isinstance(exc, AssertionError):
        return "assertion_failed"
    return "runtime_exception"


def _verifier_line(exc: BaseException, verifier_path: Path) -> int:
    expected = os.path.abspath(os.fspath(verifier_path))
    matching: list[tuple[str, int]] = []
    traceback = exc.__traceback__
    while traceback is not None:
        frame = traceback.tb_frame
        frame_path = os.path.abspath(frame.f_code.co_filename)
        if frame_path == expected:
            matching.append((frame.f_code.co_name, traceback.tb_lineno))
        traceback = traceback.tb_next

    caller_lines = [line for name, line in matching if name != "_require"]
    if caller_lines:
        return caller_lines[-1]
    if matching:
        return matching[-1][1]
    return 0


def _emit_failure(
    *, phase: str, exc: BaseException, verifier_path: Path
) -> NoReturn:
    line = _verifier_line(exc, verifier_path)
    print(
        f"{SENTINEL} phase={phase} failure={_failure_code(exc)} "
        f"exception={_exception_code(exc)} line={line}"
    )
    raise SystemExit(1)


def _load_verifier(verifier_path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "okg_openwebui_compatibility_verifier",
        verifier_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _FailureBoundary:
    def __init__(self, *, phase: str, verifier_path: Path) -> None:
        self._phase = phase
        self._verifier_path = verifier_path

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, traceback
        if exc is None:
            return False
        _emit_failure(
            phase=self._phase,
            exc=exc,
            verifier_path=self._verifier_path,
        )
        return False


def run(verifier_path: Path = VERIFIER_PATH) -> int:
    with _FailureBoundary(phase="verifier_import", verifier_path=verifier_path):
        verifier = _load_verifier(verifier_path)

    with _FailureBoundary(phase="verifier_execute", verifier_path=verifier_path):
        result = verifier.main()
        if result != 0:
            raise RuntimeError
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())

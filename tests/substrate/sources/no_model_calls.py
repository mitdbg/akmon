"""A tripwire for model calls, for tests that promise a path makes none.

`install(log_path)` makes every route to a hosted model fail loudly, and
records the attempt in `log_path` first so a test can read the record even if
a caller swallows the error:

- the LiteLLM, OpenAI and Anthropic call functions and clients;
- OKG's own captioning service, vision client and PDF table transcription
  service (their constructors raise: a path that builds none makes no call);
- every socket connection or name lookup that leaves this machine, which
  catches an OpenRouter, embedding or any other HTTP call that goes round the
  named entry points.

`sitecustomize_text(log_path)` returns the text of a `sitecustomize.py` that
installs the same tripwire in a child Python process, so `okg deployment
add-source --apply`, which imports in a child, is covered too.
"""
from __future__ import annotations

import importlib.abc
import importlib.machinery
import socket
import sys
from pathlib import Path
from typing import Any

_LOG: Path | None = None
_LOOPBACK = {"localhost", "127.0.0.1", "::1", "", "0.0.0.0"}


class ModelCallAttempted(BaseException):
    """Raised by the tripwire. A BaseException, so no `except Exception`
    in the code under test can hide it."""


def _hit(what: str) -> None:
    if _LOG is not None:
        with open(_LOG, "a", encoding="utf-8") as handle:
            handle.write(what + "\n")
    raise ModelCallAttempted(what)


def _raiser(what: str):
    def _fail(*_args: Any, **_kwargs: Any) -> Any:
        _hit(what)

    return _fail


def _patch_litellm(module: Any) -> None:
    for name in (
        "completion", "acompletion", "embedding", "aembedding", "text_completion",
        "atext_completion", "transcription", "atranscription", "image_generation",
        "aimage_generation", "responses", "aresponses", "batch_completion",
    ):
        if hasattr(module, name):
            setattr(module, name, _raiser(f"litellm.{name}"))


def _patch_class_init(module: Any, *class_names: str) -> None:
    for name in class_names:
        cls = getattr(module, name, None)
        if cls is not None:
            cls.__init__ = _raiser(f"{module.__name__}.{name}()")


#: module name -> patch applied the moment the module is imported.
_PATCHES = {
    "litellm": _patch_litellm,
    "openai": lambda m: _patch_class_init(m, "OpenAI", "AsyncOpenAI", "AzureOpenAI"),
    "anthropic": lambda m: _patch_class_init(m, "Anthropic", "AsyncAnthropic"),
    "okg.views.execution.llm": lambda m: _patch_class_init(m, "VLMClient"),
    "okg.substrate.library.captioning.service": (
        lambda m: _patch_class_init(m, "CaptionService")
    ),
    "okg.substrate.library.extraction.pdf_table_vlm": (
        lambda m: _patch_class_init(m, "PdfTableTranscriptionService")
    ),
}


class _PatchAfterImport(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):  # noqa: ANN001
        patch = _PATCHES.get(name)
        if patch is None:
            return None
        spec = importlib.machinery.PathFinder.find_spec(name, path)
        if spec is None or spec.loader is None:
            return spec
        loader = spec.loader
        exec_module = loader.exec_module

        def patched_exec(module: Any) -> None:
            exec_module(module)
            patch(module)

        loader.exec_module = patched_exec  # type: ignore[method-assign]
        return spec


def _is_remote(address: Any) -> bool:
    if isinstance(address, (str, bytes)):
        return False  # a unix socket path
    host = address[0] if address else ""
    return isinstance(host, str) and host not in _LOOPBACK


def install(log_path: str | Path | None = None) -> None:
    """Arm the tripwire in this process. Safe to call once per process."""
    global _LOG
    _LOG = Path(log_path) if log_path else None
    for name, patch in _PATCHES.items():
        if name in sys.modules:
            patch(sys.modules[name])
    if not any(isinstance(f, _PatchAfterImport) for f in sys.meta_path):
        sys.meta_path.insert(0, _PatchAfterImport())

    real_connect, real_connect_ex = socket.socket.connect, socket.socket.connect_ex
    real_getaddrinfo = socket.getaddrinfo

    def connect(self, address):  # noqa: ANN001
        if _is_remote(address):
            _hit(f"socket.connect {address!r}")
        return real_connect(self, address)

    def connect_ex(self, address):  # noqa: ANN001
        if _is_remote(address):
            _hit(f"socket.connect_ex {address!r}")
        return real_connect_ex(self, address)

    def getaddrinfo(host, *args, **kwargs):  # noqa: ANN001
        if isinstance(host, str) and host not in _LOOPBACK:
            _hit(f"socket.getaddrinfo {host!r}")
        return real_getaddrinfo(host, *args, **kwargs)

    socket.socket.connect = connect  # type: ignore[method-assign]
    socket.socket.connect_ex = connect_ex  # type: ignore[method-assign]
    socket.getaddrinfo = getaddrinfo  # type: ignore[assignment]


def sitecustomize_text(log_path: str | Path) -> str:
    """The `sitecustomize.py` that arms this tripwire in a child process."""
    here = str(Path(__file__).resolve())
    return (
        "import importlib.util\n"
        f"_spec = importlib.util.spec_from_file_location('_no_model_calls', {here!r})\n"
        "_mod = importlib.util.module_from_spec(_spec)\n"
        "_spec.loader.exec_module(_mod)\n"
        f"_mod.install({str(log_path)!r})\n"
    )


def attempts(log_path: str | Path) -> list[str]:
    """The calls the tripwire recorded, empty when none was attempted."""
    path = Path(log_path)
    if not path.exists():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]

"""Recorded Open WebUI measurements that ``okg chat`` reads at run time.

These files ship inside the okg package so that an installed okg -- which
has no repository checkout and no ``pact/`` directory -- can run
``okg chat plan`` and ``okg chat sync``.

- ``ui-knob-census.json``: which ``chat.ui`` knobs are drivable at the pinned
  Open WebUI version, and by which mechanism. It is the ADMITTED set the
  ``chat.ui`` schema is bound to (``okg.substrate.deployments.chat_block``).
  Measured by PACT ``openwebui-chat-frontend``; the copy under that change's
  evidence directory is history, and this file is the source of truth.
- ``prompts-api-readback.json`` (absent today): a write-plus-readback proof
  of the Open WebUI prompts API. Until it exists the prompts surface is
  dropped with a named reason (``okg.substrate.chat.sync``).

Re-measuring a knob means replacing the file here, in the same change as
the schema it moves.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

__all__ = ["PACKAGE_RELATIVE_DIR", "chat_data_dir"]

#: How reports name this directory: relative to the installed ``okg``
#: package's parent, never as this machine's absolute install path.
PACKAGE_RELATIVE_DIR = "okg/substrate/chat/data"


def chat_data_dir() -> Path:
    """The installed directory holding these recorded measurements.

    The okg wheel installs as ordinary files, so the package directory is a
    real filesystem path. A zipped install would not be. That case raises the
    same typed refusal a missing census does (``chat_ui_census_unavailable``,
    reason ``missing_or_unreadable``) rather than returning a path that
    silently reads nothing.
    """
    root = resources.files(__name__)
    path = Path(str(root))
    if not path.is_dir():
        from okg.substrate.deployments.chat_block import ChatUiCensusUnavailable

        raise ChatUiCensusUnavailable(
            f"the recorded Open WebUI measurements are not on disk ({root!r}): "
            "okg.substrate.chat.data is not a directory, so the chat.ui knob "
            "census cannot be read and no knob is driven.",
            data={"artifact": str(root), "reason": "missing_or_unreadable"},
            fix_hint="install okg as files (a wheel or editable install), not from a zip archive",
        )
    return path

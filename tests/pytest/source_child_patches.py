"""Install test stubs and spies inside real ``spawn`` source children.

Production runs every ingest source in its own child process, and a
monkeypatch in the test process cannot reach it. A test that must stub or
observe code the source run calls -- a Nomos boundary, the compensation
entry point -- names an *installer* here instead: a module-level function,
given as ``"module:function"``, that each source child runs before its
source. The installer's spies write JSON lines to a record file the test
reads afterwards. The source still runs on the production child path.
"""
from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.ingest import source_jobs

_INSTALLER = "OKG_TEST_SOURCE_CHILD_INSTALLER"
_PARAMS = "OKG_TEST_SOURCE_CHILD_PARAMS"
_RECORDS = "OKG_TEST_SOURCE_CHILD_RECORDS"
_REAL_CHILD_MAIN = source_jobs.child_main


def patched_child_main(result_conn: Any, lifeline: Any, payload: dict) -> None:
    module_name, _, function = os.environ[_INSTALLER].partition(":")
    installer = getattr(importlib.import_module(module_name), function)
    installer(**json.loads(os.environ.get(_PARAMS) or "{}"))
    _REAL_CHILD_MAIN(result_conn, lifeline, payload)


def install_in_source_children(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    installer: str,
    **params: Any,
) -> Path:
    """Run ``installer(**params)`` in every source child; return the record file."""
    records = tmp_path / "source-child-records.jsonl"
    records.write_text("")
    monkeypatch.setenv(_INSTALLER, installer)
    monkeypatch.setenv(_PARAMS, json.dumps(params))
    monkeypatch.setenv(_RECORDS, str(records))
    monkeypatch.setattr(source_jobs, "child_main", patched_child_main)
    return records


def record(kind: str, **fields: Any) -> None:
    """Append one record from inside a source child."""
    line = json.dumps({"kind": kind, **fields}, default=str) + "\n"
    fd = os.open(os.environ[_RECORDS], os.O_WRONLY | os.O_APPEND)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)


def read_records(path: Path, kind: str) -> list[dict[str, Any]]:
    return [
        row
        for row in (json.loads(line) for line in path.read_text().splitlines() if line)
        if row.get("kind") == kind
    ]

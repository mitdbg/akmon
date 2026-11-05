"""The spend ledger: one JSONL file shared by every run, with a per-run and a total cap.

Each model call first writes a ``reserve`` row (the configured reserve) and,
when it ends, a ``settle`` row with its real cost, computed from the reply's
token usage and the pinned price table. Spend is the settled costs plus every
reserve not yet settled, so concurrent calls (and a call that never returns)
count against the cap while in flight. A call is refused when its reserve
would take this run past ``run_cap_usd`` or the ledger past ``total_cap_usd``.

A call that ends without usable usage (a timeout, a cancelled call, a reply
without usage) settles at its reserve, marked ``reserve_unknown_usage``: the
provider may still bill it, and the reserve is the runner's charge for it. A
request refused with an HTTP error status settles at 0 (``http_error_status``):
the API does not bill a refused request. Known limit: a call that costs more
than its reserve can take spend past a cap by that excess, once per call in
flight; the next reserve is then refused. A
reserve left open by a crashed process keeps counting: the ledger fails closed.

Rows are appended under an exclusive ``fcntl`` lock, so two processes on one
host share the file safely. A row that does not parse stops every run that
reads the file (``ledger_corrupt``) instead of being skipped.
"""
from __future__ import annotations

import fcntl
import json
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .errors import CapReached, ReplayProblem, refuse

LEDGER_VERSION = 1
SOURCE_USAGE = "usage"
SOURCE_RESERVE = "reserve_unknown_usage"
#: An HTTP error status (no reply body with usage): the API does not bill a refused request.
SOURCE_HTTP_ERROR = "http_error_status"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Ledger:
    def __init__(self, path: Path, *, run_id: str, run_cap_usd: float, total_cap_usd: float,
                 reserve_usd: float) -> None:
        self.path = Path(path)
        self.run_id = run_id
        self.run_cap = run_cap_usd
        self.total_cap = total_cap_usd
        self.reserve_usd = reserve_usd
        self._offset = 0
        self._settled_total = 0.0
        self._settled_by_run: dict[str, float] = {}
        self._open: dict[str, tuple[str, float]] = {}  # reserve id -> (run, usd)

    # -- reading -----------------------------------------------------------------------------------------------
    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                self._handle = handle
                self._catch_up(handle)
                yield
            finally:
                self._handle = None
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _catch_up(self, handle: Any) -> None:
        handle.seek(self._offset)
        raw = handle.read()
        try:
            data = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise refuse("ledger_corrupt", f"the ledger {self.path.name} is not UTF-8") from exc
        if not data:
            return
        if not data.endswith("\n"):
            raise refuse("ledger_corrupt", f"the ledger {self.path.name} ends in a partial row")
        for number, line in enumerate(data.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                self._apply(row)
            except (ValueError, KeyError, TypeError) as exc:
                raise refuse("ledger_corrupt",
                             f"a row of the ledger {self.path.name} does not parse "
                             f"({type(exc).__name__}, row {number} after byte {self._offset})") from exc
        self._offset += len(raw)

    def _apply(self, row: Mapping[str, Any]) -> None:
        kind, rid, run, usd = row["kind"], row["id"], row["run"], float(row["usd"])
        if usd < 0 or usd != usd:
            raise ValueError("negative or NaN usd")
        if kind == "reserve":
            self._open[rid] = (run, usd)
        elif kind == "settle":
            self._open.pop(rid, None)
            self._settled_total += usd
            self._settled_by_run[run] = self._settled_by_run.get(run, 0.0) + usd
        else:
            raise ValueError(f"unknown row kind {kind!r}")

    def _spent(self) -> tuple[float, float]:
        open_total = sum(usd for _, usd in self._open.values())
        open_run = sum(usd for run, usd in self._open.values() if run == self.run_id)
        return (self._settled_total + open_total,
                self._settled_by_run.get(self.run_id, 0.0) + open_run)

    def totals(self) -> dict[str, float]:
        """Spend so far: the whole ledger and this run, open reserves included."""
        with self._locked():
            total, run = self._spent()
        return {"total_usd": round(total, 8), "run_usd": round(run, 8)}

    # -- the cap -----------------------------------------------------------------------------------------------
    def check_start(self) -> None:
        """Refuse a run that could pass the total cap, or reuse a run id the ledger already has."""
        with self._locked():
            total, run = self._spent()
            used = self.run_id in self._settled_by_run or any(r == self.run_id for r, _ in self._open.values())
        if used:
            raise refuse("run_id_in_ledger",
                         f"the ledger already has spend for run {self.run_id!r}; pick a new run id")
        if total + self.run_cap > self.total_cap:
            raise refuse("total_cap_insufficient",
                         f"ledger total ${total:.4f} plus this run's cap ${self.run_cap:.2f} would pass the total "
                         f"cap ${self.total_cap:.2f}")

    def reserve(self, *, purpose: str, question_id: str, try_number: int | None) -> str:
        """Hold ``reserve_usd`` for one model call, or raise CapReached."""
        with self._locked():
            total, run = self._spent()
            if run + self.reserve_usd > self.run_cap:
                raise CapReached(ReplayProblem(
                    "run_cap_reached",
                    f"run {self.run_id} has spent ${run:.4f}; one more call's reserve ${self.reserve_usd:.4f} "
                    f"would pass the run cap ${self.run_cap:.2f}"))
            if total + self.reserve_usd > self.total_cap:
                raise CapReached(ReplayProblem(
                    "total_cap_reached",
                    f"the ledger has ${total:.4f}; one more call's reserve ${self.reserve_usd:.4f} would pass "
                    f"the total cap ${self.total_cap:.2f}"))
            rid = uuid.uuid4().hex
            self._write({"v": LEDGER_VERSION, "kind": "reserve", "id": rid, "run": self.run_id,
                         "usd": self.reserve_usd, "purpose": purpose, "question": question_id,
                         "try": try_number, "at": _now()})
        return rid

    def settle(self, rid: str, *, usd: float, source: str, model: str, usage: Mapping[str, Any] | None,
               purpose: str, question_id: str, try_number: int | None) -> None:
        if source not in (SOURCE_USAGE, SOURCE_RESERVE, SOURCE_HTTP_ERROR):
            raise ValueError(f"unknown cost source {source!r}")
        row = {"v": LEDGER_VERSION, "kind": "settle", "id": rid, "run": self.run_id, "usd": round(usd, 8),
               "source": source, "model": model, "purpose": purpose, "question": question_id,
               "try": try_number, "at": _now()}
        for key in ("input_tokens", "cached_tokens", "output_tokens", "reasoning_tokens", "long_context"):
            if usage is not None and key in usage:
                row[key] = usage[key]
        with self._locked():
            self._write(row)

    def _write(self, row: Mapping[str, Any]) -> None:
        line = json.dumps(row, sort_keys=True) + "\n"
        self._handle.seek(0, 2)
        self._handle.write(line.encode("utf-8"))
        self._handle.flush()
        self._apply(row)
        self._offset += len(line.encode("utf-8"))

    def run_rows(self) -> list[dict[str, Any]]:
        """This run's settle rows (for the run summary)."""
        rows = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                if row.get("run") == self.run_id and row.get("kind") == "settle":
                    rows.append(row)
        return rows

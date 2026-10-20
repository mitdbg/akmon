"""The `kind: mcp` adapter against the real DBS and Rucio MCP servers.

PACT live-relation-mcp-adapter, req.mcp.real-upstream. HAND-RUN ONLY:
`tests/live/conftest.py` does not collect this directory unless
`OKG_MCP_CMS_LIVE=1`, so no CI lane ever sees it.

It stages the test-only deployment `tests/substrate/e2e/fixtures/live-mcp-cms`
as committed, publishes one node of each system subtype into a disposable
database, builds the MCP server with the fixture's own policy bundle, and
asks four things through `expand(live=...)`:

(a) the wide DBS call that returned 60 MB in the C4 eval
    (`primary_ds_name: *HI*`, `detail: true`) comes back within the budget,
    as a prefix plus digest or as a bounded `response_over_cap` refusal;
(b) a narrow DBS call -- one dataset, by its exact full path, through the
    pattern relation (detail fixed off) -- returns its row whole, and a
    narrow Rucio call returns its RSEs whole;
(c) a Nomos deny never reaches the server: the DBS container logs no new
    `CallToolRequest` across the denied call;
(d) every call wrote its sample row.

RUNNING IT
----------
On the host that runs the two MCP containers, from a checkout of this
branch, with a DISPOSABLE Postgres (the run truncates it):

    export OKG_TEST_DSN=postgresql://...      # your own scratch database
    export OKG_MCP_CMS_GATE_TOKEN="$(cat <token file>)"   # never on argv
    export OKG_MCP_CMS_NARROW_DATASET=/<primary>/<processed>/<TIER>
    OKG_MCP_CMS_LIVE=1 PYTEST_ADDOPTS= .venv/bin/python -m pytest \
        -o addopts= -p no:xdist -p no:cov -q -s tests/live/mcp_cms

`OKG_MCP_CMS_GATE_TOKEN` is the raw bearer token of the gates in front of
the servers. The test turns it into the header value the fixture's
`secret_refs` names (`OKG_MCP_CMS_GATE_AUTH`) inside this process; it is
never printed, logged or passed on a command line. Optional:
`OKG_MCP_CMS_DBS_CONTAINER` (default `lcm-live-dbs`) names the container
whose log (c) counts, and `OKG_MCP_CMS_RSE_EXPRESSION` (default `tier=0`)
the narrow Rucio question. Filter storage type with `rse_type=DISK` or
`rse_type=TAPE`; Rucio has no `type` attribute. The committed ports are 5952 (DBS) and 5951
(Rucio) on 127.0.0.1.

Each call is bounded by its relation's deadline: 120 s for the wide DBS
call, 60 s for the others. A full run is a few minutes at most.

Once the flag is set, a missing variable or an unreachable server FAILS the
test; it never skips. The printed summary (sizes, markers, digests, counts)
is the evidence; it carries no argument values beyond those above, no row
contents and no credential.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone

import pytest

from tests.substrate.external_backed.test_live_relation_mcp_adapter import (
    BUDGET,
    CMS_AUTH_ENV,
    build_cms_server,
    cms_samples,
    deny_every_live_call,
    drive,
    live_block,
    publish_cms_systems,
    stage_cms_fixture,
)

pytestmark = pytest.mark.postgres


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.fail(
            f"OKG_MCP_CMS_LIVE=1 was set but {name} is not. See this "
            f"module's docstring for what each variable holds."
        )
    return value


def since_stamp(now: datetime) -> str:
    """`now` as RFC 3339 in UTC with microseconds, for `podman logs --since`.

    Sub-second, because a stamp rounded down to the second reaches back
    over the allowed call made just before the deny, and that call's own
    log line then sits inside the window being counted.
    """
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def count_call_tool_requests(log_text: str) -> int:
    """How many tool calls an MCP server's log records."""
    return log_text.count("CallToolRequest")


def _call_tool_lines(container: str, since: str) -> int:
    """`CallToolRequest` lines the MCP server in `container` logged since."""
    done = subprocess.run(  # noqa: S603
        ["podman", "logs", "--since", since, container],
        capture_output=True, text=True, check=False, timeout=60,
    )
    assert done.returncode == 0, (
        f"podman logs {container} failed ({done.returncode}); (c) cannot be "
        f"counted:\n{done.stderr[-500:]}"
    )
    return count_call_tool_requests(done.stdout + done.stderr)


def _summary(out: dict) -> dict:
    block = live_block(out)
    error = block.get("error") or {}
    return {
        "served_bytes": out["served"],
        "state": block.get("state"),
        "rows": len(block.get("rows") or []),
        "rows_returned": block.get("rows_returned"),
        "truncated": block.get("truncated"),
        "truncation_reason": block.get("truncation_reason"),
        "budget_bytes": block.get("budget_bytes"),
        "digest": block.get("digest"),
        "row_count": (block.get("summary") or {}).get("row_count"),
        "failure": error.get("failure"),
        "narrow_with": error.get("narrow_with"),
    }


def test_the_adapter_meets_the_real_servers(
    cms_dsn, monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    # `cms_dsn` is a parsed `Dsn`, whose repr and str mask the password: a
    # failing test prints its arguments, and this one's must not carry the
    # database credential into the log or the JUnit XML.
    token = _required("OKG_MCP_CMS_GATE_TOKEN")
    narrow_dataset = _required("OKG_MCP_CMS_NARROW_DATASET")
    container = os.environ.get("OKG_MCP_CMS_DBS_CONTAINER", "lcm-live-dbs")
    rse_expression = os.environ.get("OKG_MCP_CMS_RSE_EXPRESSION", "tier=0")
    monkeypatch.setenv(CMS_AUTH_ENV, f"Bearer {token}")

    dsn = cms_dsn.render()
    nodes = publish_cms_systems(dsn)
    staged = stage_cms_fixture(tmp_path / "deployments")
    server = build_cms_server(dsn, staged.parent, monkeypatch)
    dbs, rucio = nodes["dbs_instance"], nodes["rucio_instance"]
    report: dict[str, object] = {}

    # (a) the C4 call.
    started = time.monotonic()
    wide = drive(server, dbs, relation="datasets",
                 args={"primary_ds_name": "*HI*", "detail": True},
                 max_bytes=BUDGET)
    report["a_wide_dbs"] = {**_summary(wide),
                            "seconds": round(time.monotonic() - started, 1)}
    block = live_block(wide)
    assert wide["served"] <= BUDGET, report
    if block["state"] == "ok":
        assert block["truncated"] is True, report
        assert block["truncation_reason"] == "response_byte_budget", report
        assert block["digest"], report
    else:
        assert block["state"] == "error", report
        assert block["error"]["failure"] == "response_over_cap", report
        assert len(json.dumps(block["error"])) <= 1024, report

    # (b) narrow calls, served whole.
    one = drive(server, dbs, relation="dataset",
                args={"dataset": narrow_dataset}, max_bytes=BUDGET)
    report["b_narrow_dbs"] = _summary(one)
    block = live_block(one)
    assert block["state"] == "ok", report
    assert block.get("truncated") is not True, report
    assert len(block["rows"]) == 1, report

    rses = drive(server, rucio, relation="rses",
                 args={"rse_expression": rse_expression}, max_bytes=BUDGET)
    report["b_narrow_rucio"] = _summary(rses)
    block = live_block(rses)
    assert block["state"] == "ok", report
    assert block.get("truncated") is not True, report
    assert block["rows"], report

    # (c) a deny never reaches the server, counted in the server's own log.
    # Let the allowed calls' log lines land first, then open the window at
    # this instant, to the microsecond: nothing the server logs from here on
    # can come from an earlier call, so `before` should be 0 and `after`
    # must equal it.
    time.sleep(1.5)
    since = since_stamp(datetime.now(timezone.utc))
    before = _call_tool_lines(container, since)
    deny_every_live_call(server, tmp_path / "no-bundle")
    denied = drive(server, dbs, relation="datasets",
                   args={"primary_ds_name": "*HI*", "detail": True},
                   max_bytes=BUDGET)
    time.sleep(3)
    after = _call_tool_lines(container, since)
    report["c_deny"] = {"state": live_block(denied)["state"],
                        "since": since,
                        "call_tool_lines_before": before,
                        "call_tool_lines_after": after}
    assert live_block(denied)["state"] == "denied", report
    assert after == before, report

    # (d) one sample per call, allowed and denied alike.
    samples = cms_samples(dsn)
    report["d_samples"] = [
        {"relation": s["relation"], "decision": s["decision"],
         "status": s["status"], "policy": s["policy"],
         "adapter_ran": (s["attrs"] or {}).get("adapter_ran")}
        for s in samples
    ]
    print("MCP_CMS_LIVE_REPORT " + json.dumps(report, sort_keys=True))
    assert [(s["relation"], s["decision"]) for s in samples] == [
        ("datasets", "allow"), ("dataset", "allow"), ("rses", "allow"),
        ("datasets", "deny"),
    ], report
    assert samples[-1]["attrs"]["adapter_ran"] is False, report

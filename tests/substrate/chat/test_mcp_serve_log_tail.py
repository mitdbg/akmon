"""The live chat drivers put a redacted mcp-serve log tail in their refusal.

Run 36692144664 of chat-live.yml lost mcp-serve.log to the artifact storage
quota, so the step log said only "connection refused" for 240 seconds. These
tests hold the replacement contract without a database, a container or a
server: when the wait for mcp-serve fails, the refusal carries the last lines
of the server's own log, passed through the drivers' shared Scrubber, and no
password, connection string or bearer token survives into the message.

As in the other driver tests, the live drivers are not imported (they need
Docker and a wheel-installed okg); their source text is read for the wiring
check. Every credential below is synthetic.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.live.chat.grounded_api_chain import (
    REFUSAL_LOG_TAIL_CHARS,
    REFUSAL_LOG_TAIL_LINE_CHARS,
    REFUSAL_LOG_TAIL_LINES,
    UNREGISTERED_CREDENTIAL,
    Refusal,
    Scrubber,
    poll_until,
    refusal_with_log_tail,
)

LIVE_CHAT = Path(__file__).resolve().parents[2] / "live" / "chat"

PASSWORD = "Pw-synthetic-3f9a1c7e5b2d"
DSN = f"postgresql://okg_mcp:{PASSWORD}@127.0.0.1:5433/okg"
TOKEN = "tok-synthetic-9d8c7b6a5f4e3d2c1b0a"
CAUSE = "error [mcp_read_dsn_unusable]: okg mcp-serve: OKG_MCP_RO_DSN is set"


class FakeClock:
    """A clock the poll loop advances itself, so the deadline is exercised."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _driver_scrubber() -> Scrubber:
    """Registered values a driver holds: the whole DSN and the bearer token.

    The drivers never hold the okg_mcp password on its own (the workflow
    mints it), so a bare ``password=...`` line is masked by pattern, not by
    registration; test_unregistered_credentials_are_masked_by_shape pins that.
    """
    scrub = Scrubber()
    scrub.hide(DSN, "<redacted:okg_mcp_ro_dsn>")
    scrub.hide(TOKEN, "<redacted:mcp-bearer-token>")
    return scrub


def _time_out_waiting(log_path: Path, scrub: Scrubber, exit_code: int | None) -> str:
    """Drive the real poll loop to its deadline, as the drivers' wait does."""
    url = "http://127.0.0.1:5632/mcp"
    clock = FakeClock()
    with pytest.raises(Refusal) as refused:
        poll_until(
            lambda: "URLError: <urlopen error [Errno 111] Connection refused>",
            lambda failure: failure == "",
            timeout=10,
            on_timeout=lambda failure: refusal_with_log_tail(
                f"timed out after 10s waiting for {url} ({failure})",
                log_path=log_path,
                scrub=scrub,
                process_name="mcp-serve",
                exit_code=exit_code,
            ),
            clock=clock,
            sleep=clock.sleep,
            interval=2.0,
        )
    return str(refused.value)


def _planted_log(tmp_path: Path) -> Path:
    noise = [f"startup chatter line {n}" for n in range(60)]
    planted = [
        f"connecting with {DSN}",
        f"password={PASSWORD}",
        f"Authorization: Bearer {TOKEN}",
        CAUSE + "   ",
    ]
    log = tmp_path / "mcp-serve.log"
    log.write_text("\n".join(noise + planted) + "\n", encoding="utf-8")
    return log


def test_refusal_carries_redacted_tail(tmp_path: Path) -> None:
    message = _time_out_waiting(_planted_log(tmp_path), _driver_scrubber(), 1)

    assert message.startswith("timed out after 10s waiting for")
    assert "mcp-serve exited with code 1" in message
    assert CAUSE in message
    assert "<redacted:okg_mcp_ro_dsn>" in message
    assert f"password={UNREGISTERED_CREDENTIAL}" in message
    assert "<redacted:mcp-bearer-token>" in message
    for secret in (PASSWORD, DSN, TOKEN):
        assert secret not in message
    # Only the tail: the first noise lines are gone, the budget is honoured.
    assert "startup chatter line 0\n" not in message
    assert f"last {REFUSAL_LOG_TAIL_LINES} line(s) of mcp-serve.log" in message
    assert message.count("\n") == REFUSAL_LOG_TAIL_LINES + 1


def test_unregistered_credentials_are_masked_by_shape(tmp_path: Path) -> None:
    """A DSN or bearer the driver never registered still does not leak."""
    log = _planted_log(tmp_path)
    message = _time_out_waiting(log, Scrubber(), None)

    assert "mcp-serve still running" in message
    assert DSN not in message
    assert f"Bearer {TOKEN}" not in message
    assert f"postgresql://okg_mcp:{UNREGISTERED_CREDENTIAL}@127.0.0.1" in message
    # A bare password line is masked by pattern (the drivers never register
    # the okg_mcp password on its own).
    assert PASSWORD not in message
    assert f"password={UNREGISTERED_CREDENTIAL}" in message


@pytest.mark.parametrize(
    ("line", "secret"),
    [
        ("host=h user=okg_mcp password=hunter2secret dbname=okg", "hunter2secret"),
        ("postgresql://okg_mcp@h/okg?password=qs-secret-123", "qs-secret-123"),
        ("postgresql://:secretpw99@h/db", "secretpw99"),
        ("postgresql://u:p@ss-word-7@h/db", "ss-word-7"),
        ("authorization: bearer abcdefghijkl", "abcdefghijkl"),
        ("OKG_MCP_AUTH_TOKEN=abcdefghijklmnop", "abcdefghijklmnop"),
        ("MY_API_KEY = zzzzyyyyxxxx", "zzzzyyyyxxxx"),
    ],
)
def test_other_credential_forms_are_masked_unregistered(
    tmp_path: Path, line: str, secret: str
) -> None:
    """Review of the first head: these survived an empty Scrubber."""
    log = tmp_path / "mcp-serve.log"
    log.write_text(f"starting\n{line}\nerror: gave up\n")
    message = _time_out_waiting(log, Scrubber(), 1)

    assert secret not in message
    assert UNREGISTERED_CREDENTIAL in message


def test_tail_is_capped_by_characters(tmp_path: Path) -> None:
    """One unbroken 5 MB line must not flood the step log."""
    log = tmp_path / "mcp-serve.log"
    log.write_text("x" * 5_000_000 + "\nlast line\n")
    message = _time_out_waiting(log, Scrubber(), 1)

    assert len(message) < REFUSAL_LOG_TAIL_CHARS + 2_000
    assert "more characters cut" in message
    assert "last line" in message


def test_a_cut_never_leaves_a_secret_fragment(tmp_path: Path) -> None:
    """Masking runs before the per-line cut."""
    log = tmp_path / "mcp-serve.log"
    secret = "s" * 40
    log.write_text("y" * (REFUSAL_LOG_TAIL_LINE_CHARS - 10) + f" password={secret}\n")
    message = _time_out_waiting(log, Scrubber(), 1)

    assert "sssss" not in message


def test_missing_log_is_stated(tmp_path: Path) -> None:
    message = _time_out_waiting(tmp_path / "mcp-serve.log", Scrubber(), 1)

    assert (
        "mcp-serve exited with code 1; its log mcp-serve.log does not exist" in message
    )


@pytest.mark.parametrize("content", ["", "\n\n   \n"])
def test_empty_log_is_stated(tmp_path: Path, content: str) -> None:
    log = tmp_path / "mcp-serve.log"
    log.write_text(content, encoding="utf-8")

    message = _time_out_waiting(log, Scrubber(), None)

    assert message.endswith("mcp-serve still running; its log mcp-serve.log is empty")


@pytest.mark.parametrize(
    "driver", ["drive_real_graph_turn.py", "drive_chat_lifecycle.py"]
)
def test_driver_mcp_wait_refuses_with_log_tail(driver: str) -> None:
    """Each driver's wait for mcp-serve re-raises through the tail helper.

    Checked on the syntax tree: an ``except Refusal`` handler that raises
    ``refusal_with_log_tail(...)`` with ``process_name="mcp-serve"``, the
    driver's own scrubber, and the ``mcp-serve.log`` path.
    """
    tree = ast.parse((LIVE_CHAT / driver).read_text("utf-8"))
    wired = []
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler):
            continue
        if not (isinstance(handler.type, ast.Name) and handler.type.id == "Refusal"):
            continue
        for node in ast.walk(handler):
            if (
                isinstance(node, ast.Raise)
                and isinstance(node.exc, ast.Call)
                and isinstance(node.exc.func, ast.Name)
                and node.exc.func.id == "refusal_with_log_tail"
            ):
                keywords = {kw.arg: ast.unparse(kw.value) for kw in node.exc.keywords}
                wired.append(keywords)
    assert wired, f"{driver}: no mcp-serve wait refuses with the log tail"
    for keywords in wired:
        assert keywords["process_name"] == "'mcp-serve'"
        assert keywords["scrub"].endswith("scrub")
        assert "mcp-serve.log" in keywords["log_path"]

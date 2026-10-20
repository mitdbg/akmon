"""Shared helpers for the AKMON reviewer quickstart and claim scripts.

Every script in this directory prints one ``... PASS ...`` line and exits 0
when the behaviour it demonstrates is observed, and prints a ``... FAIL ...``
line naming the step and the observed values and exits 1 otherwise. Nothing
here substitutes a default for a value the product failed to return.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, NoReturn, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
RECORDS_FIXTURE = REPO_ROOT / "tests/fixtures/deployment_bootstrap/records.json"
DEFAULT_DEPLOYMENT_NAME = "akmon-reviewer"
DSN_ENV = "OKG_REVIEWER_DSN"
STATE_FILE_NAME = "reviewer-state.json"

# Variables that could let a run reach a hosted model or service. The
# reproduce driver removes them from every child environment; the claim
# scripts refuse to start when one is present, so a pass can never lean on a
# key that happened to be exported in the reviewer's shell.
_PROVIDER_VARIABLES = frozenset({
    "OPENAI_BASE_URL",
    "OPENAI_API_BASE",
    "OPENAI_ORG_ID",
    "ANTHROPIC_BASE_URL",
    "AZURE_OPENAI_ENDPOINT",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
    "HF_TOKEN",
    "HUGGING_FACE_HUB_TOKEN",
    "COHERE_API_KEY",
    "MISTRAL_API_KEY",
    "GROQ_API_KEY",
    "TOGETHER_API_KEY",
    "VOYAGE_API_KEY",
    "LITELLM_API_KEY",
    "OLLAMA_HOST",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
})
_SECRET_NAME = re.compile(r"(?:_API_KEY|_TOKEN|_SECRET|_SECRET_KEY)$", re.I)


def credential_variables(environ: dict[str, str] | None = None) -> list[str]:
    """Names (never values) of key, token and provider variables set."""
    env = os.environ if environ is None else environ
    return sorted(
        name for name in env
        if name in _PROVIDER_VARIABLES or _SECRET_NAME.search(name)
    )


def keyless_environment(environ: dict[str, str] | None = None) -> tuple[dict[str, str], list[str]]:
    """Return a copy of the environment without credential variables."""
    env = dict(os.environ if environ is None else environ)
    removed = credential_variables(env)
    for name in removed:
        env.pop(name, None)
    return env, removed


def require_keyless(step: str) -> None:
    """Refuse to run while any key, token or provider variable is set."""
    present = credential_variables()
    if present:
        fail(
            step,
            "credential variables are set; run through scripts/akmon/reproduce.py "
            "or unset them",
            variables=",".join(present),
        )


def fail(step: str, message: str, **observed: Any) -> NoReturn:
    detail = " ".join(f"{key}={_fmt(value)}" for key, value in observed.items())
    print(redact(f"FAIL {step}: {message}" + (f" {detail}" if detail else "")), flush=True)
    raise SystemExit(1)


def passed(label: str, **observed: Any) -> None:
    detail = " ".join(f"{key}={_fmt(value)}" for key, value in observed.items())
    print(redact(f"{label} PASS {detail}".rstrip()), flush=True)


def run_main(step: str, main: Callable[[], int]) -> NoReturn:
    """Run a script's main so every failure is one `FAIL <step>` line.

    A product error raised deep in a call (a refused publish, a database
    error) would otherwise end the script with a Python traceback instead of
    the promised line. Set AKMON_DEBUG=1 to also print the traceback.
    """
    try:
        code = main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print(f"FAIL {step}: interrupted", flush=True)
        raise SystemExit(1)
    except BaseException as exc:  # noqa: BLE001 - report every failure the same way
        if os.environ.get("AKMON_DEBUG") == "1":
            import traceback

            traceback.print_exc()
        message = " ".join(str(exc).split())
        if len(message) > 800:
            message = message[:800] + " ..."
        fail(step, f"unexpected {type(exc).__name__}: {message}")
    raise SystemExit(code)


def note(message: str) -> None:
    print(redact(f"  {message}"), flush=True)


def _fmt(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


def redact(text: str) -> str:
    """Hide every database password in ``text`` before it is printed or logged.

    Delegates to okg's own masker, which covers URL passwords (with or without
    a user name, containing ``@``), ``?password=`` query values, keyword
    ``password=`` values and ``PGPASSWORD=`` assignments. ``dsn_flags=()``
    keeps the rest of a ``--dsn`` URL (host, port, database) readable in the
    printed commands; its password is still masked.
    """
    from okg.substrate.db.dsn import redact_text

    return redact_text(str(text), dsn_flags=())


def okg_command(*args: str) -> list[str]:
    return [sys.executable, "-m", "okg.substrate.cli.main", *args]


def run_okg(
    step: str,
    args: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    expect_ok: bool = True,
) -> dict[str, Any]:
    """Run one ``okg`` command with ``--json`` output and parse it."""
    command = okg_command(*args)
    note("$ okg " + " ".join(args))
    result = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    payload = _last_json_object(result.stdout) or _last_json_object(result.stderr)
    if expect_ok and result.returncode != 0:
        fail(
            step,
            f"`okg {' '.join(args[:3])}` exited {result.returncode}",
            output=(payload if payload is not None else _tail(result.stderr or result.stdout)),
        )
    if payload is None:
        fail(step, "command printed no JSON", output=_tail(result.stderr or result.stdout))
    payload["_returncode"] = result.returncode
    return payload


def _tail(text: str, lines: int = 12) -> str:
    return "\n".join((text or "").strip().splitlines()[-lines:])


def _last_json_object(text: str) -> dict[str, Any] | None:
    text = (text or "").strip()
    if not text:
        return None
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        pass
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
    return None


def resolve_dsn(value: str | None) -> str:
    dsn = value or os.environ.get(DSN_ENV)
    if not dsn:
        fail("setup", f"pass --dsn or set {DSN_ENV}")
    return dsn


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        fail(
            "setup",
            "reviewer state file not found; run scripts/akmon/reviewer_quickstart.py first",
            state=path,
        )
    return json.loads(path.read_text(encoding="utf-8"))


def deployment_environment(state: dict[str, Any], dsn: str) -> dict[str, str]:
    """Environment under which the reviewer deployment resolves by name."""
    env = dict(os.environ)
    env["OKG_DEPLOYMENTS_DIR"] = state["deployments_dir"]
    env[state.get("dsn_env") or DSN_ENV] = dsn
    return env


def activate_deployment(state: dict[str, Any], dsn: str) -> None:
    """Make the reviewer deployment resolvable by name in this process."""
    os.environ.update(deployment_environment(state, dsn))

"""``okg chat setup`` writes the ``chat:`` block from answers (issue #2288).

No shipped profile declares a ``chat:`` block, so chat used to need a
hand-edited ``deployment.yaml``. These tests drive the real scaffold against a
manifest shaped like the one ``okg install --profile academic-researcher`` writes and
prove: the written block is accepted by the loader every chat verb uses;
credentials are written as variable NAMES only, even when the variable is set;
a re-run is byte-identical; an existing block is never overwritten; and bad
answers are refused with a typed error while the file stays untouched.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.chat.block_scaffold import (
    ANSWER_FLAGS,
    DEFAULT_TOOLS,
    DOCUMENT_SUBTYPES,
    PROVIDER_PRESETS,
    SCAFFOLD_SEARCH_PROFILE,
    SYSTEM_PROMPT_REF,
    ChatAnswers,
    ChatBlockScaffoldError,
    base_url_masked_parts,
    ensure_chat_block,
    key_env_refusal,
    masked_base_url,
)
from okg.substrate.chat.setup import STEP_CONFIGURE, STEPS, SetupOptions, render_human, run_setup
from okg.substrate.deployments.chat_block import ChatManifestError, parse_chat_block
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.search_profiles import parse_search_config
from tests.substrate._fake_credentials import fake_credential

DEPLOYMENT = "scaffold-fixture"
MCP_PORT = 5431
#: A value that must never reach any file. Not a real credential.
CANARY = "canary-provider-value-2288"
#: A fake key in a real key's shape, pasted where a variable NAME belongs.
#: Obviously not a credential.
PASTED = fake_credential("sk-", "FAKE-SENTINEL-2315f-not-a-real-key-000000")

#: The shape `okg install --profile academic-researcher` writes: no search:, no chat:,
#: the MCP port recorded under profile.init_answers, extraction loaded.
INSTALLED_MANIFEST = f"""\
# written by okg install; this comment must survive the scaffold
name: {DEPLOYMENT}
profile:
  name: academic-researcher
  init_answers:
    mcp_port: {MCP_PORT}
modules:
- extraction
- concepts
postgres:
  dsn: ${{OKG_DSN}}
"""

OPENAI = ChatAnswers(provider="openai", model="gpt-4o-mini", instance_port=3000)


def _deployment(tmp_path: Path, text: str = INSTALLED_MANIFEST) -> Path:
    dep = tmp_path / DEPLOYMENT
    dep.mkdir()
    (dep / "deployment.yaml").write_text(text, encoding="utf-8")
    return dep


def _all_bytes(dep: Path) -> str:
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(dep.rglob("*")) if p.is_file()
    )


def test_writes_a_block_the_chat_loader_accepts(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)

    result = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)

    assert result.status == "written"
    text = (dep / "deployment.yaml").read_text(encoding="utf-8")
    # Appended, not re-dumped: the install's bytes, comment included, survive.
    assert text.startswith(INSTALLED_MANIFEST)
    raw = yaml.safe_load(text)
    chat = parse_chat_block(raw, manifest_path=dep / "deployment.yaml")
    assert chat is not None and chat.enabled
    assert chat.models.builtin == ("gpt-4o-mini",) and chat.models.default == "gpt-4o-mini"
    assert chat.provider.base_url == PROVIDER_PRESETS["openai"].base_url
    assert chat.provider.api_key_env == "OPENAI_API_KEY"
    assert chat.mcp.port == MCP_PORT and chat.mcp.tools == DEFAULT_TOOLS
    assert chat.instance_port == 3000
    assert chat.search_profile == SCAFFOLD_SEARCH_PROFILE
    search = parse_search_config(raw["search"])
    assert search.profiles[SCAFFOLD_SEARCH_PROFILE].subtypes == DOCUMENT_SUBTYPES
    # The prompt file the block references exists and is not empty (sync
    # refuses an empty declared prompt).
    assert (dep / SYSTEM_PROMPT_REF).read_text(encoding="utf-8").strip()
    assert result.decisions["mcp_port"].startswith("profile.init_answers.mcp_port")
    assert sorted(result.written) == sorted(
        [str(dep / "deployment.yaml"), str(dep / SYSTEM_PROMPT_REF)]
    )


def test_a_set_credential_variable_never_reaches_any_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _deployment(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", CANARY)
    monkeypatch.setenv("OKG_FIXTURE_KEY_2288", CANARY)

    ensure_chat_block(
        DEPLOYMENT,
        ChatAnswers(
            provider="openai-compatible", provider_base_url="http://host.docker.internal:8000/v1",
            provider_key_env="OKG_FIXTURE_KEY_2288", model="m", instance_port=3000,
        ),
        deployment_dir=dep,
    )

    assert CANARY not in _all_bytes(dep)
    raw = yaml.safe_load((dep / "deployment.yaml").read_text(encoding="utf-8"))
    assert raw["chat"]["provider"] == {
        "base_url": "http://host.docker.internal:8000/v1",
        "api_key_env": "OKG_FIXTURE_KEY_2288",
    }


def test_a_credential_pasted_as_the_variable_name_is_refused_and_withheld(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(
            DEPLOYMENT, ChatAnswers(provider="openai", provider_key_env=PASTED,
                                    model="m", instance_port=3000),
            deployment_dir=dep,
        )

    assert info.value.data["reason"] == "provider_key_env_not_a_name"
    assert info.value.data["shape"] == "provider_key_prefix"
    # What `okg chat setup` would print for this refusal.
    from okg.substrate.chat.setup import _error_payload

    assert PASTED not in json.dumps(_error_payload(info.value), default=str)
    assert PASTED not in str(info.value)
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == INSTALLED_MANIFEST
    assert not (dep / SYSTEM_PROMPT_REF).exists()


def _setup_json(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str]:
    from okg.substrate.cli.main import main

    code = main(argv)
    return code, capsys.readouterr().out


def test_a_pasted_key_on_an_existing_block_is_refused_and_withheld_everywhere(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Review blocker on #2315: the conflict report used to copy the answer."""
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    before = _all_bytes(dep)
    answers = ChatAnswers(provider="openai", provider_key_env=PASTED,
                          model="gpt-4o-mini", instance_port=3000)

    # 1. The exception, its payload, and its text.
    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, answers, deployment_dir=dep, environ={})
    assert info.value.data["reason"] == "provider_key_env_not_a_name"
    assert PASTED not in str(info.value) + (info.value.fix_hint or "")
    assert PASTED not in json.dumps(info.value.to_json(), default=str)

    # 2. The setup report: its dict (the --json / OKG_AGENT=1 output) and
    # the human rendering.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    report = run_setup(
        DEPLOYMENT, options=SetupOptions(chat_answers=answers, configure_only=True),
        environ={},
    )
    assert report.failed_step == STEP_CONFIGURE
    assert PASTED not in json.dumps(report.to_dict(), default=str)
    assert PASTED not in render_human(report)

    # 3. The CLI, both output modes.
    argv = [
        "chat", "setup", "--deployment", DEPLOYMENT, "--provider", "openai",
        "--provider-key-env", PASTED, "--model", "gpt-4o-mini",
        "--instance-port", "3000", "--configure-only",
    ]
    code, out = _setup_json(capsys, [*argv, "--json"])
    assert code == 1 and PASTED not in out
    assert json.loads(out)["error"]["data"]["reason"] == "provider_key_env_not_a_name"
    code, out = _setup_json(capsys, argv)
    assert code == 1 and PASTED not in out
    assert _all_bytes(dep) == before


def test_a_disagreeing_key_env_name_is_named_by_flag_not_by_value(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(
            DEPLOYMENT,
            ChatAnswers(provider="openai", provider_key_env="OTHER_FAKE_KEY_NAME"),
            deployment_dir=dep, environ={},
        )

    (conflict,) = info.value.data["conflicts"]
    assert conflict == {
        "path": "chat.provider.api_key_env", "declared": "OPENAI_API_KEY",
        "answered": "the value given for --provider-key-env",
        "flag": "--provider-key-env",
    }
    assert "OTHER_FAKE_KEY_NAME" not in json.dumps(info.value.to_json(), default=str)


def test_configure_error_data_is_kept_by_allowlist(tmp_path: Path) -> None:
    """A field a future error grows is withheld until allowlisted on purpose."""

    def configure(_answers: Any) -> Any:
        raise ChatBlockScaffoldError(
            "fixture refusal", data={
                "reason": "fixture", "flag": "--model",
                "future_field": "FAKE-SENTINEL-2315f-future",
                "conflicts": [{"path": "p", "flag": "--model", "declared": 1,
                               "answered": 2, "raw": "FAKE-SENTINEL-2315f-raw"}],
            },
        )

    report = run_setup(
        DEPLOYMENT, options=SetupOptions(chat_answers=OPENAI, configure_only=True),
        environ={}, configure=configure,
    )

    data = report.error["data"]
    assert "FAKE-SENTINEL-2315f" not in json.dumps(report.to_dict(), default=str)
    assert data["withheld_fields"] == ["future_field"]
    assert data["conflicts"] == [{"path": "p", "flag": "--model", "declared": 1, "answered": 2}]
    assert data["reason"] == "fixture" and data["step"] == STEP_CONFIGURE


def test_a_pasted_answer_is_scrubbed_from_any_configure_error_text() -> None:
    """Second layer: even an error that quotes the answer prints it redacted."""

    def configure(_answers: Any) -> Any:
        raise ChatBlockScaffoldError(
            f"fixture refusal quoting {PASTED}",
            data={"reason": "fixture", "observed": PASTED},
            fix_hint=f"not {PASTED}",
        )

    report = run_setup(
        DEPLOYMENT,
        options=SetupOptions(
            chat_answers=dataclasses.replace(OPENAI, provider_key_env=PASTED),
            configure_only=True,
        ),
        environ={}, configure=configure,
    )

    assert report.failed_step == STEP_CONFIGURE
    assert PASTED not in json.dumps(report.to_dict(), default=str)
    assert PASTED not in render_human(report)


def test_a_base_url_with_credentials_is_refused_and_withheld(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    url = "http://fakeuser:FAKE-SENTINEL-2315f@host.docker.internal:8000/v1"

    report_answers = ChatAnswers(provider="openai-compatible", provider_base_url=url,
                                 model="m", instance_port=3000)
    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, report_answers, deployment_dir=dep, environ={})

    assert info.value.data["reason"] == "provider_base_url_has_credentials"
    assert "FAKE-SENTINEL-2315f" not in json.dumps(info.value.to_json(), default=str)
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == INSTALLED_MANIFEST


@pytest.mark.parametrize(
    "name",
    ["MY_KEY", "OPENAI_API_KEY", "openai_key", "HF_TOKEN", "hf_token",
     "GROQ_API_KEY", "OKG_FIXTURE_KEY_2288", "sk_key", "A" * 64,
     "Provider2Key"],
)
def test_realistic_variable_names_are_accepted(name: str) -> None:
    assert key_env_refusal(name) is None


@pytest.mark.parametrize(
    ("value", "shape"),
    [
        ("sk-proj-FAKEsentinel2315fNotAKey", "provider_key_prefix"),
        (fake_credential("sk-ant-", "FAKEsentinel2315fNotAKey"), "provider_key_prefix"),
        ("sk_" + "live_FAKEsentinel2315f", "provider_key_prefix"),
        ("gsk_FAKEsentinel2315fNotAKey00", "provider_key_prefix"),
        ("xai-FAKEsentinel2315fNotAKey", "provider_key_prefix"),
        ("pk-FAKEsentinel2315fNotAKey", "provider_key_prefix"),
        ("AIzaFAKEsentinel2315fNotAKey", "provider_key_prefix"),
        ("hf_FAKEsentinel2315fNotAKey", "provider_key_prefix"),
        ("fake key with spaces", "not_a_var_name"),
        ("0f0f-fake", "not_a_var_name"),
        ("F" + "0a" * 32, "too_long"),
        ("FakeSentinel2315fMistralShape", "token_like"),
        ("f" + "0a1b" * 8, "token_like"),
    ],
    ids=[
        "openai-project", "anthropic", "stripe-style", "groq", "xai",
        "pk-prefix", "google", "huggingface", "spaces", "leading-digit",
        "over-64", "mixed-token", "long-hex-token",
    ],
)
def test_pasted_key_shapes_are_refused(tmp_path: Path, value: str, shape: str) -> None:
    assert key_env_refusal(value) == shape
    dep = _deployment(tmp_path)
    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(
            DEPLOYMENT,
            ChatAnswers(provider="openai-compatible",
                        provider_base_url="http://host.docker.internal:8000/v1",
                        provider_key_env=value, model="m", instance_port=3000),
            deployment_dir=dep, environ={},
        )
    assert info.value.data["shape"] == shape
    assert value not in json.dumps(info.value.to_json(), default=str)
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == INSTALLED_MANIFEST


# Every provider-key prefix the scaffold knows, listed here independently so
# that dropping any one from KEY_PREFIXES fails its row (#2779). The tail has
# no `_`, so it reads as a key after a name-like prefix, and the whole value
# stays under the length at which the generic token check would refuse it
# anyway. Without its entry a name-like value would pass as a variable name
# and a hyphenated one would be refused only as `not_a_var_name`.
_KEY_TAIL = "FAKEsentinel2315f"
_EVERY_KEY_PREFIX = [
    ("openai", "sk-"), ("stripe-secret", "sk_"), ("stripe-secret-live", "sk_live_"),
    ("stripe-secret-test", "sk_test_"), ("public-hyphen", "pk-"), ("stripe-public", "pk_"),
    ("stripe-public-live", "pk_live_"), ("stripe-public-test", "pk_test_"),
    ("stripe-restricted", "rk_"), ("stripe-restricted-live", "rk_live_"),
    ("stripe-restricted-test", "rk_test_"), ("groq", "gsk_"), ("xai", "xai-"),
    ("google", "AIza"), ("huggingface", "hf_"), ("github-classic", "ghp_"),
    ("github-oauth", "gho_"), ("github-user", "ghu_"), ("github-server", "ghs_"),
    ("github-refresh", "ghr_"), ("github-fine-grained", "github_pat_"),
    ("gitlab", "glpat-"), ("slack-bot", "xoxb-"), ("slack-user", "xoxp-"),
    ("aws-long-term", "AKIA"), ("aws-session", "ASIA"), ("replicate", "r8_"),
    ("perplexity", "pplx-"), ("nvidia", "nvapi-"), ("fireworks", "fw_"),
    ("cerebras", "csk-"), ("together", "tgp_"),
]


@pytest.mark.parametrize(
    "prefix",
    [prefix for _, prefix in _EVERY_KEY_PREFIX],
    ids=[name for name, _ in _EVERY_KEY_PREFIX],
)
def test_every_known_key_prefix_is_refused(prefix: str) -> None:
    assert key_env_refusal(prefix + _KEY_TAIL) == "provider_key_prefix"


def test_fine_grained_github_key_with_underscores_is_refused() -> None:
    """GitHub fine-grained keys carry `_` after the prefix; it must not read as a name."""
    assert key_env_refusal("github" + "_pat_" + "FAKE_sentinel_2315f") == "provider_key_prefix"


def test_an_unset_key_variable_is_a_warning_checked_by_name_only(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    unset = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    assert any("$OPENAI_API_KEY is not set" in w for w in unset.warnings)

    present = ensure_chat_block(
        DEPLOYMENT, OPENAI, deployment_dir=dep, environ={"OPENAI_API_KEY": CANARY},
    )
    assert not any("OPENAI_API_KEY" in w for w in present.warnings)
    assert CANARY not in json.dumps(present.as_dict()) + json.dumps(unset.as_dict())


def test_rerunning_with_the_same_answers_changes_nothing(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)
    before = _all_bytes(dep)

    again = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)
    bare = ensure_chat_block(DEPLOYMENT, ChatAnswers(), deployment_dir=dep)

    assert again.status == "unchanged" and bare.status == "unchanged"
    assert again.written == [] and bare.written == []
    assert _all_bytes(dep) == before


def test_a_disagreeing_answer_refuses_and_never_overwrites(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)
    before = _all_bytes(dep)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(
            DEPLOYMENT, ChatAnswers(provider="openai", model="gpt-4o", instance_port=3000),
            deployment_dir=dep,
        )

    assert info.value.data["reason"] == "chat_block_answer_conflicts"
    assert [c["flag"] for c in info.value.data["conflicts"]] == ["--model"]
    assert _all_bytes(dep) == before


def test_a_broken_existing_block_is_reported_not_called_unchanged(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, INSTALLED_MANIFEST + "chat:\n  enabled: true\n")

    with pytest.raises(ChatManifestError):
        ensure_chat_block(DEPLOYMENT, ChatAnswers(), deployment_dir=dep)


@pytest.mark.parametrize(
    ("answers", "reason", "flag"),
    [
        (ChatAnswers(provider="anthropic-direct", model="m", instance_port=3000),
         "provider_unknown", "--provider"),
        (ChatAnswers(model="m", instance_port=3000), "provider_missing", "--provider"),
        (ChatAnswers(provider="openai-compatible", model="m", instance_port=3000),
         "provider_base_url_missing", "--provider-base-url"),
        (ChatAnswers(provider="openai", instance_port=3000), "model_missing", "--model"),
        (ChatAnswers(provider="openai", model="m"), "instance_port_missing", "--instance-port"),
        (ChatAnswers(provider="openai", model="m", instance_port=MCP_PORT),
         "instance_port_equals_mcp_port", "--instance-port"),
        (ChatAnswers(provider="openai", model="m", instance_port=3000, search_profile="x"),
         "search_profile_undeclared", "--scope"),
    ],
)
def test_bad_answers_are_refused_typed_and_write_nothing(
    tmp_path: Path, answers: ChatAnswers, reason: str, flag: str,
) -> None:
    dep = _deployment(tmp_path)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, answers, deployment_dir=dep)

    assert info.value.code == "chat_block_scaffold_refused"
    assert info.value.data["reason"] == reason
    assert info.value.data["flag"] == flag
    assert flag in (info.value.fix_hint or "") or reason == "provider_unknown"
    assert sorted(p.name for p in dep.iterdir()) == ["deployment.yaml"]
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == INSTALLED_MANIFEST


def test_an_unrecorded_mcp_port_is_asked_for_not_guessed(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, INSTALLED_MANIFEST.replace(f"    mcp_port: {MCP_PORT}\n", ""))

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)
    assert info.value.data["reason"] == "mcp_port_missing"

    written = ensure_chat_block(
        DEPLOYMENT,
        ChatAnswers(provider="openai", model="gpt-4o-mini", instance_port=3000, mcp_port=8100),
        deployment_dir=dep,
    )
    assert written.chat_block["mcp"]["port"] == 8100


def test_a_declared_search_profile_is_reused_and_ambiguity_refuses(tmp_path: Path) -> None:
    one = INSTALLED_MANIFEST + "search:\n  profiles:\n    papers:\n      subtypes: [publication]\n"
    dep = _deployment(tmp_path, one)
    result = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)
    assert result.chat_block["search_profile"] == "papers"
    assert yaml.safe_load((dep / "deployment.yaml").read_text())["search"] == {
        "profiles": {"papers": {"subtypes": ["publication"]}},
    }

    two = one + "    notes:\n      subtypes: [document]\n"
    (tmp_path / "b").mkdir()
    dep2 = _deployment(tmp_path / "b", two)
    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep2)
    assert info.value.data["reason"] == "search_profile_ambiguous"


def test_no_extraction_module_means_the_subtypes_are_asked_for(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, INSTALLED_MANIFEST.replace("- extraction\n", ""))
    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep)
    assert info.value.data["reason"] == "search_subtypes_missing"


def test_the_default_subtypes_are_the_extraction_modules_classes() -> None:
    """The default scope must name subtypes the catalog actually has."""
    import okg.substrate.library.ontologies as ontologies

    schema = Path(ontologies.__file__).parent / "extraction" / "schema.yaml"
    classes = yaml.safe_load(schema.read_text(encoding="utf-8"))["classes"]
    snake = {re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower() for name in classes}
    assert set(DOCUMENT_SUBTYPES) <= snake


# ---------------------------------------------------------------------------
# Through `okg chat setup`
# ---------------------------------------------------------------------------

def test_setup_configure_only_writes_the_block_and_starts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    started: list[Any] = []

    report = run_setup(
        DEPLOYMENT,
        options=SetupOptions(chat_answers=OPENAI, configure_only=True),
        environ={},
        stand_up=lambda *a, **k: started.append(a),
    )

    assert report.ok and report.configure_only
    assert STEPS[0] == STEP_CONFIGURE
    assert report.steps[STEP_CONFIGURE].status == "ok"
    assert [report.steps[s].status for s in STEPS[1:]] == ["not_attempted"] * (len(STEPS) - 1)
    assert started == []
    assert "okg chat setup --deployment" in report.next_action
    assert "configure only" in render_human(report)


def test_setup_hands_the_written_block_to_the_instance_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The instance step reads the block the configure step just wrote."""
    _deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    seen: dict[str, Any] = {}

    def stand_up(deployment: str, **kwargs: Any) -> Any:
        seen.update(kwargs)
        raise SubstrateError("fixture stops here", data={"reason": "fixture_stop"})

    def load_manifest() -> tuple[Any, str]:
        manifest = tmp_path / DEPLOYMENT / "deployment.yaml"
        raw = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        return parse_chat_block(raw, manifest_path=manifest), "postgresql://fixture"

    report = run_setup(
        DEPLOYMENT, options=SetupOptions(chat_answers=OPENAI, instance_port=3000),
        environ={}, load_manifest=load_manifest, stand_up=stand_up,
    )

    assert report.steps[STEP_CONFIGURE].status == "ok"
    assert report.failed_step == "instance"
    assert seen["chat_block"].provider.api_key_env == "OPENAI_API_KEY"
    assert seen["instance_port_override"] == 3000


def test_setup_refuses_an_unknown_provider_as_a_typed_configure_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.main import main

    _deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    code = main([
        "chat", "setup", "--deployment", DEPLOYMENT, "--provider", "nope",
        "--model", "m", "--instance-port", "3000", "--configure-only", "--json",
    ])

    envelope = json.loads(capsys.readouterr().out)
    assert code == 1
    assert envelope["failed_step"] == STEP_CONFIGURE
    assert envelope["error"]["code"] == "chat_block_scaffold_refused"
    assert envelope["error"]["data"]["reason"] == "provider_unknown"
    assert (tmp_path / DEPLOYMENT / "deployment.yaml").read_text() == INSTALLED_MANIFEST


def test_cli_configure_only_writes_then_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.cli.main import main

    _deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OPENAI_API_KEY", CANARY)
    argv = [
        "chat", "setup", "--deployment", DEPLOYMENT, "--provider", "openai",
        "--model", "gpt-4o-mini", "--instance-port", "3000", "--configure-only", "--json",
    ]

    assert main(argv) == 0
    first = json.loads(capsys.readouterr().out)
    after_first = _all_bytes(tmp_path / DEPLOYMENT)
    assert main(argv) == 0
    second = json.loads(capsys.readouterr().out)

    assert first["steps"][0]["data"]["status"] == "written"
    assert second["steps"][0]["data"]["status"] == "unchanged"
    assert _all_bytes(tmp_path / DEPLOYMENT) == after_first
    assert CANARY not in after_first and CANARY not in json.dumps(first) + json.dumps(second)


def _shipped_profiles() -> list[Path]:
    import okg.substrate.library as library

    root = Path(library.__file__).parent / "profiles"
    return sorted(
        p for p in root.iterdir()
        if p.is_dir() and not p.name.startswith("_") and (p / "modules.yaml").is_file()
    )


#: Shipped profiles whose install records no graph endpoint port: their
#: profile.yaml asks no `mcp_port` question. `okg chat setup` refuses them with
#: `mcp_port_missing` and asks for --mcp-port.
#: `minimal` (the bare `okg install` starter, issue #3414) asks only for a
#: deployment name and a DSN, so its chat setup needs --mcp-port.
NO_RECORDED_MCP_PORT: set[str] = {"minimal"}

#: Shipped profiles that load no `extraction` module, so chat has no document
#: subtype to default to: `okg chat setup` refuses with
#: `search_subtypes_missing` and asks for --search-subtype. `minimal` gains the
#: module when `okg deployment add-source` adds a folder source.
NO_EXTRACTION_MODULE: set[str] = {"minimal"}


def _recorded_init_answers(profile_dir: Path) -> dict[str, Any]:
    """What an unattended `okg install` records: each optional question's default."""
    from okg.substrate.catalog.profile import load_profile, validate_answer

    profile = load_profile(profile_dir / "profile.yaml")
    return {
        q.id: validate_answer(q, None)
        for q in profile.init_questions
        if not q.required and q.default is not None
    }


@pytest.mark.parametrize("profile_dir", _shipped_profiles(), ids=lambda p: p.name)
def test_every_shipped_profile_gets_a_valid_block_without_a_hand_edit(
    tmp_path: Path, profile_dir: Path,
) -> None:
    """The class guard for issue #2288: no shipped profile declares chat:, so
    each one must reach a loader-accepted block through the guided answers.

    The recorded answers are the profile's own question defaults, so a
    profile that asks no `mcp_port` question records none: that profile is
    refused with a named fix (`--mcp-port`), then succeeds with the flag.
    """
    defaults = yaml.safe_load((profile_dir / "deployment-defaults.yaml").read_text()) or {}
    modules = yaml.safe_load((profile_dir / "modules.yaml").read_text())["modules"]
    init_answers = _recorded_init_answers(profile_dir)
    manifest = {
        "name": DEPLOYMENT,
        "profile": {"name": profile_dir.name, "init_answers": init_answers},
        "modules": modules,
        **defaults,
    }
    assert "chat" not in manifest  # the gap this issue closes
    dep = _deployment(tmp_path, yaml.safe_dump(manifest, sort_keys=False))

    answers = OPENAI
    if profile_dir.name in NO_RECORDED_MCP_PORT:
        assert "mcp_port" not in init_answers
        with pytest.raises(ChatBlockScaffoldError) as info:
            ensure_chat_block(DEPLOYMENT, answers, deployment_dir=dep, environ={})
        assert info.value.data["reason"] == "mcp_port_missing"
        assert info.value.data["flag"] == "--mcp-port"
        answers = dataclasses.replace(OPENAI, mcp_port=MCP_PORT)
    else:
        assert isinstance(init_answers.get("mcp_port"), int)
    if profile_dir.name in NO_EXTRACTION_MODULE:
        assert "extraction" not in modules
        with pytest.raises(ChatBlockScaffoldError) as info:
            ensure_chat_block(DEPLOYMENT, answers, deployment_dir=dep, environ={})
        assert info.value.data["reason"] == "search_subtypes_missing"
        assert info.value.data["flag"] == "--search-subtype"
        answers = dataclasses.replace(answers, search_subtypes=DOCUMENT_SUBTYPES)

    result = ensure_chat_block(DEPLOYMENT, answers, deployment_dir=dep, environ={})

    raw = yaml.safe_load((dep / "deployment.yaml").read_text(encoding="utf-8"))
    chat = parse_chat_block(raw, manifest_path=dep / "deployment.yaml")
    assert result.status == "written" and chat is not None and chat.enabled
    # The composed catalog was compared with the chat scope for every profile.
    assert result.coverage.get("searched"), result.warnings


# ---------------------------------------------------------------------------
# An existing chat: block: every answer is compared (review item 3)
# ---------------------------------------------------------------------------

DIFFERING_ANSWERS: dict[str, ChatAnswers] = {
    "provider": ChatAnswers(provider="ollama"),
    "provider_base_url": ChatAnswers(provider_base_url="http://other.invalid:8000/v1"),
    "provider_key_env": ChatAnswers(provider_key_env="OTHER_FAKE_KEY_NAME"),
    "model": ChatAnswers(model="other-model"),
    "mcp_port": ChatAnswers(mcp_port=9999),
    "instance_port": ChatAnswers(instance_port=3001),
    "search_profile": ChatAnswers(search_profile="other"),
    "search_subtypes": ChatAnswers(search_subtypes=("paper",)),
}


def test_every_answer_field_is_compared_with_an_existing_block() -> None:
    fields = {f.name for f in dataclasses.fields(ChatAnswers)}
    assert set(ANSWER_FLAGS) == fields
    assert set(DIFFERING_ANSWERS) == fields


@pytest.mark.parametrize("field_name", sorted(DIFFERING_ANSWERS))
def test_a_differing_answer_on_an_existing_block_is_never_unchanged(
    tmp_path: Path, field_name: str,
) -> None:
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    before = _all_bytes(dep)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, DIFFERING_ANSWERS[field_name],
                          deployment_dir=dep, environ={})

    assert info.value.data["reason"] == "chat_block_answer_conflicts"
    assert [c["flag"] for c in info.value.data["conflicts"]] == [ANSWER_FLAGS[field_name]]
    assert _all_bytes(dep) == before


def test_matching_search_subtypes_on_an_existing_block_agree(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    again = ensure_chat_block(
        DEPLOYMENT, ChatAnswers(search_subtypes=tuple(reversed(DOCUMENT_SUBTYPES))),
        deployment_dir=dep, environ={},
    )
    assert again.status == "unchanged"


def test_openai_compatible_without_a_url_against_a_preset_block_conflicts(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, ChatAnswers(provider="openai-compatible"),
                          deployment_dir=dep, environ={})
    assert [c["flag"] for c in info.value.data["conflicts"]] == ["--provider"]


def test_openai_compatible_without_a_url_against_a_custom_block_says_so(tmp_path: Path) -> None:
    dep = _deployment(tmp_path)
    custom = "http://host.docker.internal:8000/v1"
    ensure_chat_block(
        DEPLOYMENT,
        ChatAnswers(provider="openai-compatible", provider_base_url=custom,
                    model="m", instance_port=3000),
        deployment_dir=dep, environ={},
    )

    again = ensure_chat_block(DEPLOYMENT, ChatAnswers(provider="openai-compatible"),
                              deployment_dir=dep, environ={})

    assert again.status == "unchanged"
    assert any("--provider-base-url" in n and custom in n for n in again.notes)


# ---------------------------------------------------------------------------
# Search scope (review items 4 and 5)
# ---------------------------------------------------------------------------

def test_a_bare_search_key_counts_as_declared_and_is_not_duplicated(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, INSTALLED_MANIFEST + "search:\n")

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    assert info.value.data["reason"] == "search_block_without_profiles"
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == INSTALLED_MANIFEST + "search:\n"


def _academic_researcher_manifest() -> str:
    profile_dir = next(p for p in _shipped_profiles() if p.name == "academic-researcher")
    modules = yaml.safe_load((profile_dir / "modules.yaml").read_text())["modules"]
    return yaml.safe_dump({
        "name": DEPLOYMENT,
        "profile": {"name": "academic-researcher", "init_answers": {"mcp_port": MCP_PORT}},
        "modules": modules,
    }, sort_keys=False)


def test_setup_names_the_record_types_chat_will_not_search(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, _academic_researcher_manifest())

    result = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    assert "paper" in result.coverage["not_searched_by_module"]["scholarly"]
    assert result.coverage["searched"] == sorted(DOCUMENT_SUBTYPES)
    assert any("chat will not search" in n and "scholarly" in n for n in result.notes)


def test_search_subtype_widens_the_academic_researcher_scope_to_papers(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, _academic_researcher_manifest())

    result = ensure_chat_block(
        DEPLOYMENT,
        dataclasses.replace(OPENAI, search_subtypes=("paper", *DOCUMENT_SUBTYPES)),
        deployment_dir=dep, environ={},
    )

    assert "paper" in result.coverage["searched"]
    assert "paper" not in result.coverage["not_searched_by_module"].get("scholarly", [])
    assert result.coverage["unknown_profile_subtypes"] == []


# ---------------------------------------------------------------------------
# File shape (review nits 7-9)
# ---------------------------------------------------------------------------

def test_crlf_line_endings_are_preserved(tmp_path: Path) -> None:
    crlf = INSTALLED_MANIFEST.replace("\n", "\r\n").encode("utf-8")
    dep = tmp_path / DEPLOYMENT
    dep.mkdir()
    (dep / "deployment.yaml").write_bytes(crlf)

    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    data = (dep / "deployment.yaml").read_bytes()
    assert data.startswith(crlf)
    assert data.count(b"\n") == data.count(b"\r\n")
    assert parse_chat_block(yaml.safe_load(data), manifest_path=dep / "deployment.yaml")


def test_a_final_block_scalar_without_a_newline_is_refused_not_altered(tmp_path: Path) -> None:
    text = INSTALLED_MANIFEST + "notes: |\n  a\n  b"
    dep = _deployment(tmp_path, text)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    assert info.value.data["reason"] == "manifest_final_value_unterminated"
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == text
    assert not (dep / SYSTEM_PROMPT_REF).exists()


def test_a_final_plain_value_without_a_newline_still_appends(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, INSTALLED_MANIFEST + "extra: value")
    result = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    assert result.status == "written"
    assert yaml.safe_load((dep / "deployment.yaml").read_text())["extra"] == "value"


FLOW_ROOT = "{name: x, modules: [extraction], profile: {init_answers: {mcp_port: %d}}}\n" % MCP_PORT


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        (INSTALLED_MANIFEST + "...\n", "manifest_cannot_be_appended"),
        (INSTALLED_MANIFEST + "---\nother: 1\n", "manifest_unparseable"),
        (FLOW_ROOT, "manifest_cannot_be_appended"),
        (INSTALLED_MANIFEST + "bad: [unclosed\n", "manifest_unparseable"),
    ],
    ids=["document-end-marker", "second-document", "flow-root", "parser-error"],
)
def test_unusual_file_shapes_are_typed_refusals(
    tmp_path: Path, text: str, reason: str,
) -> None:
    dep = _deployment(tmp_path, text)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    assert info.value.data["reason"] == reason
    assert (dep / "deployment.yaml").read_text(encoding="utf-8") == text
    assert not (dep / SYSTEM_PROMPT_REF).exists()


# ---------------------------------------------------------------------------
# --provider-base-url may carry a key (review 2315b blocker)
# ---------------------------------------------------------------------------

#: A fake secret. Obviously not a credential.
URL_SENTINEL = "FAKESENTINEL2315gNotARealKey"
#: A fake key in a real key's shape, pasted where the URL belongs.
URL_PASTED_KEY = fake_credential("sk-", "proj-FAKESENTINEL2315gNotARealKey000")
#: Fake tokens with no key prefix (#2735): 64 hex digits, and 42 base62
#: characters that switch case and digit every character.
URL_HEX_TOKEN = hashlib.sha256(b"okg-fake-token-2735").hexdigest()
URL_MIXED_TOKEN = "".join(f"{chr(65 + i)}{i % 10}{chr(97 + i)}" for i in range(14))

BAD_BASE_URLS = {
    "query-api-key": (f"http://host.docker.internal:8000/v1?api_key={URL_SENTINEL}",
                      "credential_parameter"),
    "query-token": (f"http://host.docker.internal:8000/v1?token={URL_SENTINEL}",
                    "credential_parameter"),
    "bare-pasted-key": (URL_PASTED_KEY, "not_http"),
    # #2735: a `;name=value` path parameter, a percent-encoded key prefix
    # (`%73` is `s`), and a bare token with no known prefix.
    "path-param-api-key": (f"http://host.docker.internal:8000/v1;api_key={URL_SENTINEL}",
                           "credential_parameter"),
    "encoded-key-prefix": (f"http://host.docker.internal:8000/%73k-proj-{URL_SENTINEL}/v1",
                           "key_shaped_part"),
    "hex-token-segment": (f"http://host.docker.internal:8000/{URL_HEX_TOKEN}/v1",
                          "token_shaped_part"),
}
AZURE_URL = "https://fake-2315g.openai.azure.invalid/openai/v1?api-version=2024-06-01"


def _secret_of(url: str) -> str:
    if url == URL_PASTED_KEY:
        return URL_PASTED_KEY
    return URL_HEX_TOKEN if URL_HEX_TOKEN in url else URL_SENTINEL


def _setup_argv(url: str) -> list[str]:
    return [
        "chat", "setup", "--deployment", DEPLOYMENT,
        "--provider", "openai-compatible", "--provider-base-url", url,
        "--model", "m", "--instance-port", "3000", "--configure-only",
    ]


def _run_all_modes(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, argv: list[str],
) -> dict[str, tuple[int, str]]:
    """``okg chat setup`` as text, as --json, and under OKG_AGENT=1."""
    from okg.substrate.cli.main import main

    out: dict[str, tuple[int, str]] = {}
    monkeypatch.delenv("OKG_AGENT", raising=False)
    out["text"] = (main(argv), capsys.readouterr().out)
    out["json"] = (main([*argv, "--json"]), capsys.readouterr().out)
    monkeypatch.setenv("OKG_AGENT", "1")
    out["agent"] = (main(argv), capsys.readouterr().out)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    return out


@pytest.mark.parametrize("existing", [False, True], ids=["first-write", "existing-block"])
@pytest.mark.parametrize("case", sorted(BAD_BASE_URLS))
def test_a_base_url_that_may_carry_a_key_is_refused_and_withheld_everywhere(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    case: str, existing: bool,
) -> None:
    url, shape = BAD_BASE_URLS[case]
    secret = _secret_of(url)
    dep = _deployment(tmp_path)
    if existing:
        ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    before = _all_bytes(dep)
    answers = ChatAnswers(provider="openai-compatible", provider_base_url=url,
                          model="m", instance_port=3000)

    # The exception, first on every path: never a conflict report.
    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(DEPLOYMENT, answers, deployment_dir=dep, environ={})
    assert info.value.data["shape"] == shape
    assert info.value.data["observed"] == "<withheld>"
    assert "conflicts" not in info.value.data
    assert secret not in json.dumps(info.value.to_json(), default=str)

    # The CLI in all three output modes.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    results = _run_all_modes(capsys, monkeypatch, _setup_argv(url))
    for mode, (code, out) in results.items():
        assert code == 1, mode
        assert secret not in out, mode
    for mode in ("json", "agent"):
        data = json.loads(results[mode][1])["error"]["data"]
        assert data["flag"] == "--provider-base-url" and data["shape"] == shape, mode
        assert data["path"] == "chat.provider.base_url", mode
    assert _all_bytes(dep) == before


def test_an_azure_api_version_query_is_accepted_and_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    dep = _deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    results = _run_all_modes(capsys, monkeypatch, _setup_argv(AZURE_URL))

    assert [code for code, _ in results.values()] == [0, 0, 0]
    written = yaml.safe_load((dep / "deployment.yaml").read_text(encoding="utf-8"))
    assert written["chat"]["provider"]["base_url"] == AZURE_URL
    assert json.loads(results["agent"][1])["steps"]


def test_a_hand_written_base_url_that_is_not_a_url_is_withheld_by_the_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """An operator pasted a key into chat.provider.base_url by hand."""
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    manifest = dep / "deployment.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            PROVIDER_PRESETS["openai"].base_url, URL_PASTED_KEY),
        encoding="utf-8",
    )

    with pytest.raises(ChatManifestError) as info:
        parse_chat_block(yaml.safe_load(manifest.read_text()), manifest_path=manifest)
    assert info.value.data == {
        "path": "chat.provider.base_url", "observed": "<withheld>", "shape": "not_http",
    }
    assert URL_PASTED_KEY not in json.dumps(info.value.to_json(), default=str)

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    argv = ["chat", "setup", "--deployment", DEPLOYMENT, "--model", "gpt-4o-mini",
            "--instance-port", "3000", "--configure-only"]
    for mode, (code, out) in _run_all_modes(capsys, monkeypatch, argv).items():
        assert code == 1, mode
        assert URL_PASTED_KEY not in out, mode


def test_a_declared_base_url_with_a_key_is_refused_by_the_loader_and_never_shown(
    tmp_path: Path,
) -> None:
    """A hand-written credential URL no longer loads at all (#2344).

    Before #2344 the loader accepted it and the scaffold reported a conflict
    with the declared value withheld; now the loader applies setup's own
    ``base_url_refusal``, so the manifest is refused before any comparison.
    """
    declared = f"http://host.docker.internal:8000/v1?api_key={URL_SENTINEL}"
    dep = _deployment(tmp_path)
    ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})
    manifest = dep / "deployment.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            PROVIDER_PRESETS["openai"].base_url, declared),
        encoding="utf-8",
    )

    with pytest.raises(ChatManifestError) as info:
        ensure_chat_block(
            DEPLOYMENT,
            ChatAnswers(provider="openai-compatible",
                        provider_base_url="http://host.docker.internal:9000/v1"),
            deployment_dir=dep, environ={},
        )

    assert info.value.data["path"] == "chat.provider.base_url"
    assert info.value.data["shape"] == "credential_parameter"
    assert info.value.data["observed"] == "<withheld>"
    assert URL_SENTINEL not in json.dumps(info.value.to_json(), default=str) + str(info.value)


# ---------------------------------------------------------------------------
# Key-shaped names versus realistic names (review 2315b should-fix 2)
# ---------------------------------------------------------------------------

#: Names that start like a key prefix or read as CamelCase words, but are
#: names: the tail after the prefix has an underscore, or the pieces are words.
PREFIX_LIKE_NAMES = [
    "hf_inference_api_key", "hf_access_token_prod", "sk_openai_api_key_prod",
    "fw_api_key_production", "pk_live_name_for_key", "rk_provider_key_name",
    "ASIA_PACIFIC_OPENAI_KEY", "AKIA_STYLE_NAME_LONGER", "LiteLLMMasterKey2024Prod",
    "OpenAIKey2024ProdEnvironment",
]
#: Names the prior review and dispatch listed. All must stay accepted.
COMMON_NAMES = [
    "AZURE_OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY", "GROQ_API_KEY",
    "TOGETHER_API_KEY", "LLM_KEY_2024", "groq_api_key", "MISTRAL_API_KEY",
    "DEEPSEEK_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY", "FIREWORKS_API_KEY",
    "XAI_API_KEY", "PERPLEXITY_API_KEY", "NVIDIA_API_KEY", "HUGGINGFACEHUB_API_TOKEN",
    "HF_API_TOKEN",
]
#: Fake values in real key shapes. None is a credential.
KEY_SHAPES = {
    "openai-project": ("sk-proj-FAKEsentinel2315gNotAKey", "provider_key_prefix"),
    "groq": ("gsk_FAKEsentinel2315gNotAKey00", "provider_key_prefix"),
    "xai": ("xai-FAKEsentinel2315gNotAKey", "provider_key_prefix"),
    "google": ("AIzaFAKEsentinel2315gNotAKey", "provider_key_prefix"),
    "stripe-live": ("sk_" + "live_FAKEsentinel2315g", "provider_key_prefix"),
    "huggingface": ("hf_FAKEsentinel2315gNotAKey", "provider_key_prefix"),
    "aws-access-key": (fake_credential("AKIA", "FAKESENTINEL2315"), "provider_key_prefix"),
    "github-pat": ("github_pat_11FAKE2315g_sentinelNOTAKEY", "provider_key_prefix"),
    "hex-40": ("f" + "0a1b" * 10, "token_like"),
    "hex-40-word-runs": ("deadbeef0123cafebabe4567facefeed89abcdef", "token_like"),
    "base62-32": ("Fq7ZrT0bWk9xLpA2mN5sVc8yHd3JgE1u", "token_like"),
    "base62-32-letters": ("FqZrTbWkxLpAmNsVcyHdJgEuQoRiPtXw", "token_like"),
    "mixed-24": ("Fq7ZrT0bWk9xLpA2mN5sVc8y", "token_like"),
}


@pytest.mark.parametrize("name", PREFIX_LIKE_NAMES + COMMON_NAMES)
def test_names_that_look_like_prefixes_or_words_are_accepted(name: str) -> None:
    assert key_env_refusal(name) is None


@pytest.mark.parametrize("case", sorted(KEY_SHAPES))
def test_real_key_shapes_are_still_refused(case: str) -> None:
    value, shape = KEY_SHAPES[case]
    assert key_env_refusal(value) == shape


# ---------------------------------------------------------------------------
# Search-settings errors and the coverage note (review 2315b 3 and 5)
# ---------------------------------------------------------------------------

def test_a_search_settings_error_keeps_the_names_an_agent_needs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    typo = INSTALLED_MANIFEST + "search:\n  profiles:\n    chat:\n      subtypez: [document]\n"
    _deployment(tmp_path, typo)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    report = run_setup(
        DEPLOYMENT, options=SetupOptions(chat_answers=OPENAI, configure_only=True),
        environ={},
    )

    data = report.error["data"]
    assert data["path"] == "search.profiles.chat"
    assert data["unknown_keys"] == ["subtypez"]
    assert "subtypes" in data["accepted_keys"]
    assert data["step"] == STEP_CONFIGURE


def test_the_coverage_note_suggests_the_most_connected_type_per_module(tmp_path: Path) -> None:
    dep = _deployment(tmp_path, _academic_researcher_manifest())

    result = ensure_chat_block(DEPLOYMENT, OPENAI, deployment_dir=dep, environ={})

    assert result.coverage["suggested_by_module"]["scholarly"] == "paper"
    note = next(n for n in result.notes if "chat will not search" in n)
    assert re.search(r"scholarly \(\d+, e\.g\. paper\)", note)


@pytest.mark.parametrize("case", sorted(BAD_BASE_URLS))
def test_a_refused_base_url_is_scrubbed_from_any_configure_error_text(case: str) -> None:
    """Second layer: an error that quotes a refused URL prints it redacted."""
    url, _shape = BAD_BASE_URLS[case]

    def configure(_answers: Any) -> Any:
        raise ChatBlockScaffoldError(
            f"fixture refusal quoting {url}",
            data={"reason": "fixture", "observed": url}, fix_hint=f"not {url}",
        )

    report = run_setup(
        DEPLOYMENT,
        options=SetupOptions(
            chat_answers=dataclasses.replace(OPENAI, provider_base_url=url),
            configure_only=True,
        ),
        environ={}, configure=configure,
    )

    secret = _secret_of(url)
    assert report.failed_step == STEP_CONFIGURE
    assert secret not in json.dumps(report.to_dict(), default=str)
    assert secret not in render_human(report)


# ---------------------------------------------------------------------------
# masked_base_url: the one rendering every okg output uses (#2344)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(("url", "shown"), [
    ("http://host.docker.internal:11434/v1", "http://host.docker.internal:11434/v1"),
    ("https://api.openai.com/v1/", "https://api.openai.com/v1/"),
    (f"https://okg:{URL_SENTINEL}@api.example.invalid/v1", "https://<withheld>@api.example.invalid/v1"),
    (f"http://h.invalid:8000/v1?api_key={URL_SENTINEL}", "http://h.invalid:8000/v1?<withheld>"),
    (f"http://h.invalid/v1#token={URL_SENTINEL}", "http://h.invalid/v1#<withheld>"),
    (f"https://h.invalid/{URL_PASTED_KEY}/v1", "https://h.invalid/<withheld>/v1"),
    (AZURE_URL, "https://fake-2315g.openai.azure.invalid/openai/v1?<withheld>"),
    (URL_PASTED_KEY, "<withheld>"),
    ("", "<withheld>"),
    (None, "<withheld>"),
])
def test_masked_base_url_drops_userinfo_and_withholds_query_fragment_and_key_parts(
    url: str | None, shown: str,
) -> None:
    assert masked_base_url(url) == shown
    if isinstance(url, str) and url:
        assert URL_SENTINEL not in masked_base_url(url)
        assert URL_PASTED_KEY not in masked_base_url(url)


#: #2735 shapes a URL that never passed the loader (one already registered
#: on the instance) may still carry: (url, what it is shown as, the secret).
_MASKED_2735 = [
    (f"https://h.invalid/v1;api_key={URL_SENTINEL}", "https://h.invalid/<withheld>", URL_SENTINEL),
    (f"https://h.invalid/v1;x={URL_SENTINEL}/chat", "https://h.invalid/<withheld>/chat", URL_SENTINEL),
    (f"https://h.invalid/token={URL_SENTINEL}/v1", "https://h.invalid/<withheld>/v1", URL_SENTINEL),
    (f"https://h.invalid/%73k-proj-{URL_SENTINEL}/v1", "https://h.invalid/<withheld>/v1", URL_SENTINEL),
    (f"https://h.invalid/%2573k-proj-{URL_SENTINEL}/v1", "https://h.invalid/<withheld>/v1", URL_SENTINEL),
    (f"https://h.invalid/{URL_HEX_TOKEN}/v1", "https://h.invalid/<withheld>/v1", URL_HEX_TOKEN),
    (f"https://h.invalid/{URL_MIXED_TOKEN}/v1", "https://h.invalid/<withheld>/v1", URL_MIXED_TOKEN),
    # #2735 review: a key prefix after a boundary inside the part.
    (f"https://h.invalid/v1/x%2F{URL_PASTED_KEY}", "https://h.invalid/v1/<withheld>", URL_SENTINEL),
    (f"https://h.invalid/key-{URL_PASTED_KEY}/v1", "https://h.invalid/<withheld>/v1", URL_SENTINEL),
    (f"https://h.invalid/.{URL_PASTED_KEY}/v1", "https://h.invalid/<withheld>/v1", URL_SENTINEL),
]


@pytest.mark.parametrize(("url", "shown", "secret"), _MASKED_2735,
                         ids=[f"2735-{i}" for i in range(len(_MASKED_2735))])
def test_masked_base_url_withholds_path_params_encoded_keys_and_tokens(
    url: str, shown: str, secret: str,
) -> None:
    assert masked_base_url(url) == shown
    assert secret not in masked_base_url(url)
    # The scrub list names the raw segment and, when it differs, its decoded
    # form, so a vendor error echoing either is scrubbed by identity.
    parts = base_url_masked_parts(url)
    assert url in parts
    assert any(secret in part for part in parts[1:])
    if "%" in url:
        from urllib.parse import unquote
        raw = next(p for p in parts[1:] if "%" in p)
        assert unquote(unquote(raw)) in parts


def test_base_url_masked_parts_names_what_was_hidden_and_nothing_for_a_clean_url() -> None:
    assert base_url_masked_parts("http://host.docker.internal:11434/v1") == []
    parts = base_url_masked_parts(f"https://okg:{URL_SENTINEL}@h.invalid/v1?api_key={URL_SENTINEL}")
    assert f"okg:{URL_SENTINEL}" in parts
    assert f"api_key={URL_SENTINEL}" in parts


def test_a_declared_url_query_is_never_shown_in_configure_output(tmp_path: Path) -> None:
    """A URL the loader accepts still prints through masked_base_url (#2344).

    ``?deployment=`` is not credential-named, so the manifest loads; the
    configure step's conflict, note and --json data must still withhold it.
    """
    declared = f"http://host.docker.internal:8000/v1?deployment={URL_SENTINEL}"
    dep = _deployment(tmp_path)
    first = ensure_chat_block(
        DEPLOYMENT,
        ChatAnswers(provider="openai-compatible", provider_base_url=declared,
                    model="m", instance_port=3000),
        deployment_dir=dep, environ={},
    )

    # Written raw; only the rendering is masked.
    assert declared in (dep / "deployment.yaml").read_text(encoding="utf-8")
    assert URL_SENTINEL not in json.dumps(first.as_dict(), default=str)

    kept = ensure_chat_block(DEPLOYMENT, ChatAnswers(), deployment_dir=dep, environ={})
    assert URL_SENTINEL not in json.dumps(kept.as_dict(), default=str)

    with pytest.raises(ChatBlockScaffoldError) as info:
        ensure_chat_block(
            DEPLOYMENT,
            ChatAnswers(provider="openai-compatible",
                        provider_base_url="http://host.docker.internal:9000/v1"),
            deployment_dir=dep, environ={},
        )
    (conflict,) = info.value.data["conflicts"]
    assert conflict["declared"] == "http://host.docker.internal:8000/v1?<withheld>"
    assert URL_SENTINEL not in json.dumps(info.value.to_json(), default=str) + str(info.value)

"""`okg search --method semantic` is reachable, or says what is missing (#797).

Before this contract the CLI advertised `--method semantic` in its `choices`
but never constructed an embedder, so `search()` raised on its own guard and
`cmd_search`'s generic `except ValueError` reported "check --method, --limit,
--offset, --select, and --attr-filter values" — advice that misdirects an
operator whose arguments were all correct. Semantic search was reachable only
through the MCP server.

ANTI-VACUITY NOTE. Every refusal test here asserts the ERROR CODE and the HINT
CONTENT, never merely a non-zero exit status: the command already exited
non-zero before this change, so an exit-status assertion would pass unchanged
against the broken behaviour and prove nothing.

The DB-free tests carry no `postgres` mark so the `fast` lane picks them up;
the publish-backed ones are marked and registered by path in the `substrate-db`
lane.
"""
from __future__ import annotations

import argparse
from types import SimpleNamespace

import pytest

from okg.substrate.cli._read_side import (
    _EMBEDDER_ENV,
    _resolve_embedder_spec,
)
from okg.substrate.cli.main import _mcp_serve_embedding_plan
from okg.substrate.deployments import (
    DeploymentConfigError,
    DeploymentResolutionError,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.embeddings import EmbeddingConfig


def _args(**kw) -> argparse.Namespace:
    base = {
        "embedder": None,
        "allow_unverified_embedding_provenance": False,
    }
    base.update(kw)
    return argparse.Namespace(**base)


def _set_manifest(
    monkeypatch: pytest.MonkeyPatch,
    *,
    enabled: bool,
    backend: str | None = None,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            embedding_config=EmbeddingConfig(enabled, backend)
        ),
    )


def test_unconfigured_semantic_search_names_what_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal must name the embedder, not blame the arguments."""
    monkeypatch.delenv(_EMBEDDER_ENV, raising=False)
    _set_manifest(monkeypatch, enabled=False)
    with pytest.raises(SubstrateError) as exc:
        _resolve_embedder_spec(_args(), method="semantic", deployment="test")
    assert exc.value.code == "semantic_embedder_not_configured"
    hint = exc.value.fix_hint or ""
    # A resolved deployment names only its canonical authority.
    assert "--embedder" not in hint
    assert "deployment.yaml" in hint
    assert "lexical" in hint
    # And the message says the arguments were fine — the old generic hint
    # said the opposite.
    assert "arguments you passed is wrong" in str(exc.value)
    # The old misdirecting advice must be gone.
    assert "--limit" not in hint


def test_explicit_embedder_is_authority_only_for_standalone_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: (_ for _ in ()).throw(
            DeploymentResolutionError("standalone")
        ),
    )
    spec, source = _resolve_embedder_spec(
        _args(embedder="hash"), method="semantic", deployment="test",
    )
    assert (spec, source) == ("hash", "standalone_argument")


def test_matching_legacy_env_defers_to_manifest_and_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A matching legacy env value cannot become a second authority."""
    monkeypatch.setenv(_EMBEDDER_ENV, "hash")
    _set_manifest(monkeypatch, enabled=True, backend="hash")
    with pytest.warns(UserWarning, match="deprecated"):
        spec, source = _resolve_embedder_spec(
            _args(), method="semantic", deployment="test",
        )
    assert (spec, source) == ("hash", "deployment_manifest")

    # A process-local argument cannot override deployment authority.
    with pytest.raises(DeploymentConfigError, match="conflicts"):
        _resolve_embedder_spec(
            _args(embedder="sentence-transformers:bge-large-en-v1.5"),
            method="semantic", deployment="test",
        )
    with pytest.warns(UserWarning, match="deprecated"):
        spec, source = _resolve_embedder_spec(
            _args(embedder="hash"), method="semantic", deployment="test",
        )
    assert (spec, source) == ("hash", "deployment_manifest")


def test_blank_env_var_is_not_a_configured_embedder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty string is absence, not configuration.

    Treating `OKG_MCP_EMBEDDER=""` as a spec would fail later with a spec
    error, blaming the wrong thing.
    """
    monkeypatch.setenv(_EMBEDDER_ENV, "   ")
    _set_manifest(monkeypatch, enabled=False)
    with pytest.raises(SubstrateError) as exc:
        _resolve_embedder_spec(_args(), method="semantic", deployment="test")
    assert exc.value.code == "semantic_embedder_not_configured"


@pytest.mark.parametrize("method", ["lexical", "alias"])
def test_embedder_with_a_non_semantic_method_raises(method: str) -> None:
    """Silently ignoring it would leave the operator believing it was used."""
    with pytest.raises(SubstrateError) as exc:
        _resolve_embedder_spec(
            _args(embedder="hash"), method=method, deployment="test",
        )
    assert "--method semantic" in str(exc.value)

    with pytest.raises(SubstrateError):
        _resolve_embedder_spec(
            _args(allow_unverified_embedding_provenance=True), method=method,
            deployment="test",
        )

    # ...and with no embedder arguments a non-semantic method resolves to
    # nothing, rather than raising: this is the ordinary path.
    assert _resolve_embedder_spec(
        _args(), method=method, deployment="test",
    ) == (None, None)


def test_an_invalid_spec_surfaces_the_spec_code_not_a_generic_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo'd spec is a spec error, reported as one.

    Validation happens at resolution so the failure names the spec, rather than
    surfacing later from inside the search with less context.
    """
    monkeypatch.delenv(_EMBEDDER_ENV, raising=False)
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: (_ for _ in ()).throw(
            DeploymentResolutionError("standalone")
        ),
    )
    with pytest.raises(SubstrateError) as exc:
        _resolve_embedder_spec(
            _args(embedder="sentence-transformers:"), method="semantic",
            deployment="test",
        )
    assert exc.value.code == "embedder_spec_invalid"


def test_resolution_does_not_construct_an_embedder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse only — construction is deferred past the corpus probes.

    A `sentence-transformers:` spec loads its model from disk in `__init__`, so
    an unembedded deployment must be refused before that cost is paid. Proven
    by OBSERVATION (a counter), not by the absence of an error: "no embedder was
    constructed" is invisible unless counted.
    """
    from okg.substrate.embeddings import identity

    calls: list[str] = []
    monkeypatch.setattr(
        identity,
        "build_embedder",
        lambda spec: calls.append(spec),  # type: ignore[arg-type,return-value]
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: (_ for _ in ()).throw(
            DeploymentResolutionError("standalone")
        ),
    )
    spec, source = _resolve_embedder_spec(
        _args(embedder="sentence-transformers:bge-large-en-v1.5"),
        method="semantic", deployment="test",
    )
    assert spec == "sentence-transformers:bge-large-en-v1.5"
    assert source == "standalone_argument"
    assert calls == [], (
        "resolving a spec must not construct an embedder; construction is "
        "deferred until the coverage and model-identity probes pass"
    )


def test_mcp_serve_uses_manifest_plan_and_refuses_cli_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_manifest(monkeypatch, enabled=True, backend="hash")
    plan = _mcp_serve_embedding_plan("test", asserted_spec=None)
    assert plan.enabled is True
    assert plan.model_id == "hash:2560"
    assert plan.observed_dimension == 2560

    with pytest.warns(UserWarning, match="deprecated"):
        repeated = _mcp_serve_embedding_plan(
            "test", asserted_spec="hash",
        )
    assert repeated.fingerprint == plan.fingerprint

    with pytest.raises(DeploymentConfigError, match="conflicts"):
        _mcp_serve_embedding_plan(
            "test", asserted_spec="sentence-transformers:other",
        )


def test_mcp_serve_manifest_absence_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_manifest(monkeypatch, enabled=False)
    plan = _mcp_serve_embedding_plan("test", asserted_spec=None)
    assert plan.enabled is False
    assert plan.embedder is None

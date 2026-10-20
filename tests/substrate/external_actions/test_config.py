from __future__ import annotations

import pytest

from okg.substrate.external_actions.config import ConfigError, load_action_contracts, load_config
from okg.substrate.external_actions.signing import generate_keypair


def _config() -> dict:
    _, public = generate_keypair()
    return {
        "schema": "okg.external_action_review.config/v1",
        "deployment": "support-prod",
        "authentication": {"audience": "deployment_scoped_review_api", "allowed_algorithms": ["EdDSA"]},
        "signing_keys": [{"kid": "support-prod/epoch/1", "purpose": "execution_epoch_lease", "algorithm": "EdDSA", "public_key_b64": public, "state": "current"}],
    }


def test_config_is_strict_and_has_closed_resource_limits() -> None:
    config = load_config(_config())
    assert config.limits.request_body_bytes_maximum == 262144
    bad = _config()
    bad["unexpected"] = True
    with pytest.raises(ConfigError):
        load_config(bad)


def test_config_rejects_looser_limits_and_static_production_bearer() -> None:
    bad = _config()
    bad["limits"] = {"request_body_bytes_maximum": 262145}
    with pytest.raises(ConfigError):
        load_config(bad)
    bad = _config()
    bad["authentication"] = {"audience": "a", "allowed_algorithms": ["EdDSA"], "static_bearer": True, "development_loopback_only": False}
    with pytest.raises(ConfigError):
        load_config(bad)


def test_config_accepts_only_a_verifier_capability_environment_reference() -> None:
    config = _config()
    config["verifier_capability_env"] = "OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY"
    loaded = load_config(config)
    assert loaded.verifier_capability_env == "OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY"

    raw_secret = "a" * 64
    bad = _config()
    bad["verifier_capability_env"] = raw_secret
    with pytest.raises(ConfigError):
        load_config(bad)

    bad = _config()
    bad["verifier_capability"] = raw_secret
    with pytest.raises(ConfigError):
        load_config(bad)


def test_action_registry_unknown_nested_fields_are_refused() -> None:
    with pytest.raises(ConfigError):
        load_action_contracts({"schema": "okg.external_action_contracts/v1", "actions": [{"action_type": "a", "extra": 1}]})


def test_yaml_duplicate_keys_and_invalid_signing_keys_are_refused() -> None:
    with pytest.raises(ConfigError):
        load_config("schema: okg.external_action_review.config/v1\ndeployment: support-prod\ndeployment: support-prod\nauthentication: {audience: a, allowed_algorithms: [EdDSA]}\nsigning_keys: []")
    bad = _config()
    bad["signing_keys"] = [{"kid": "k", "purpose": "execution_epoch_lease", "algorithm": "RS256", "public_key_b64": "A" * 43, "state": "current"}]
    with pytest.raises(ConfigError):
        load_config(bad)

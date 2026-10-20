from copy import deepcopy

import pytest
import yaml

from okg.substrate.distributions.install_policy import apply_source_policy, read_source_policy
from okg.substrate.distributions.install_execution import _configuration_digest
from okg.substrate.errors import SubstrateError

_PRIVATE_POSTURE = {
    'runtime_enforcement': 'enforce',
    'deployment_posture': {
        'deployment_class': 'personal_okg', 'read_principals': ['human_operator', 'okg_service'],
        'write_principals': ['human_operator', 'source_adapter', 'okg_service'],
        'mcp_write_default': 'deny', 'export_policy': 'no_export', 'secret_handling': 'secret_ref_required',
        'live_call_default': 'deny', 'allowed_source_sensitivities': ['local_private'],
        'allowed_data_classifications': ['private'], 'allowed_exportability': ['no_export'],
        'privacy_obligations': ['audit', 'no_export'], 'readiness_profiles': ['personal'],
    },
}
_REDACTED_SOURCE_POLICY = {
    'sensitivity': 'local_private', 'data_classification': 'private', 'credential_ref_policy': 'none',
    'live_call_allowed': False, 'exportability': 'no_export', 'retention': 'normal',
    'provenance': 'local_filesystem', 'privacy_obligations': ['audit', 'no_export', 'redaction'],
    'store_raw': False,
}


def _inputs():
    """A private deployment with one text source and one source with no PII fields."""
    policy = {'sources': {
        'notes': {'source_policy': {**deepcopy(_REDACTED_SOURCE_POLICY),
                                    'pii_classes': {'text': 'free_text', 'title': 'free_text'}}},
        'repository': {'source_policy': {**deepcopy(_REDACTED_SOURCE_POLICY), 'pii_classes': {}}},
    }}
    registry = {'sources': {name: {'module': 'example', 'class': 'Source', 'params': {}}
                            for name in policy['sources']}}
    files = {
        'deployment.yaml': yaml.safe_dump({'name': 'test', 'nomos': deepcopy(_PRIVATE_POSTURE)}, sort_keys=False).encode(),
        'source_registry.yaml': yaml.safe_dump(registry, sort_keys=False).encode(),
    }
    return files, policy


def test_explicit_policy_is_applied_and_identity_bound():
    files, policy = _inputs()
    result = apply_source_policy(files, policy, require_redaction=True)
    for name, item in yaml.safe_load(result['source_registry.yaml'])['sources'].items():
        assert item['source_policy'] == policy['sources'][name]['source_policy']
    changed = deepcopy(policy)
    changed['sources']['notes']['source_policy']['pii_classes']['new_field'] = 'free_text'
    assert _configuration_digest(result) != _configuration_digest(apply_source_policy(files, changed, require_redaction=True))
    assert 'source-policy.yaml' in result


@pytest.mark.parametrize('mutation', ['missing', 'extra', 'unknown_class', 'nonstring', 'raw', 'structure'])
def test_invalid_policy_refuses(mutation):
    files, policy = _inputs()
    value = policy['sources']['notes']['source_policy']
    if mutation == 'missing':
        del policy['sources']['repository']
    elif mutation == 'extra':
        policy['sources']['unexpected'] = deepcopy(policy['sources']['repository'])
    elif mutation == 'unknown_class':
        value['pii_classes']['text'] = 'unknown'
    elif mutation == 'nonstring':
        value['pii_classes'][1] = 'free_text'
    elif mutation == 'raw':
        value['store_raw'] = True
    else:
        policy['sources']['notes']['extra'] = True
    with pytest.raises(SubstrateError):
        apply_source_policy(files, policy, require_redaction=True)


@pytest.mark.parametrize('body', ['sources: {}\nsources: {}', 'sources: {x: {source_policy: {pii_classes: {text: free_text, text: non_pii}}}}', 'sources: {}\nextra: true'])
def test_duplicate_and_unknown_structure_refuses(tmp_path, body):
    path = tmp_path / 'policy.yaml'
    path.write_text(body)
    with pytest.raises(SubstrateError):
        read_source_policy(path)


def test_equivalent_yaml_canonicalizes(tmp_path):
    files, policy = _inputs()
    path = tmp_path / 'policy.yaml'
    path.write_text(yaml.safe_dump(policy, sort_keys=True))
    assert apply_source_policy(files, read_source_policy(path), require_redaction=True) == apply_source_policy(files, policy, require_redaction=True)

from __future__ import annotations

from pathlib import Path

from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan


def _write_attestor_deployment(root: Path, *, config: str = "label: pilot") -> Path:
    deployment = root / "wired"
    package = deployment / "attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "receipt.py").write_text(
        "class ReceiptAttestor:\n"
        "    attestor_id = 'wired.receipt'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'label': {'type': 'string'}},\n"
        "        'required': ['label'],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root, label): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    (deployment / "deployment.yaml").write_text(
        "name: wired\n"
        "checkpoint_attestors:\n"
        "  - id: wired.receipt\n"
        "    class: attestors.receipt.ReceiptAttestor\n"
        "    config:\n"
        f"      {config}\n",
        encoding="utf-8",
    )
    return deployment


def test_wiring_plan_surfaces_ordered_checkpoint_attestors(tmp_path: Path) -> None:
    deployment = _write_attestor_deployment(tmp_path)

    plan = resolve_deployment_wiring_plan(deployment, check_imports=True)

    assert [item.component_id for item in plan.checkpoint_attestors] == [
        "wired.receipt"
    ]
    attestor = plan.checkpoint_attestors[0]
    assert attestor.kind == "checkpoint_attestor"
    assert attestor.import_ref == "attestors.receipt:ReceiptAttestor"
    assert attestor.attrs == {
        "class": "attestors.receipt.ReceiptAttestor",
        "config_keys": ["label"],
    }


def test_wiring_plan_rejects_raw_environment_substitution(tmp_path: Path) -> None:
    deployment = _write_attestor_deployment(
        tmp_path,
        config="label: ${ATTESTATION_LABEL}",
    )

    plan = resolve_deployment_wiring_plan(deployment, check_imports=False)

    diagnostics = plan.diagnostics_by_code("checkpoint_attestors_invalid")
    assert len(diagnostics) == 1
    assert "environment substitutions" in str(diagnostics[0].observed)
    assert plan.checkpoint_attestors == ()

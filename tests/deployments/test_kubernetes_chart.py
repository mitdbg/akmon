from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import yaml
from jsonschema import Draft202012Validator

from scripts.verify_kubernetes_chart import (
    VerificationError,
    _delete_kind_cluster,
    _helm_test_mcp_proof,
    _mcp_proof_from_helm_test,
    _validate_live_evidence,
    _wait_for_clean_qa_metrics,
    _wait_for_json_ok,
    inspect_render,
    render_chart,
)
import scripts.verify_kubernetes_chart as kubernetes_verifier
from tests.mirror_scope import private_path


REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "ops" / "kubernetes" / "okg"
OPENSTACK_VALUES = (
    REPO / "deployments" / "okg-ci" / "kubernetes" / "values.openstack.yaml"
)
DOCKERFILE = REPO / "Dockerfile"
POSTGRES_DOCKERFILE = REPO / "ops" / "pg" / "Dockerfile"
SMOKE_DEPLOYMENT = REPO / "deployments" / "kubernetes-smoke" / "deployment.yaml"


def _helm() -> str:
    explicit = os.environ.get("HELM_BIN")
    if explicit and Path(explicit).is_file():
        return explicit
    discovered = shutil.which("helm")
    if discovered:
        return discovered
    pinned = REPO / ".scratch" / "helm" / "linux-amd64" / "helm"
    if pinned.is_file():
        return str(pinned)
    candidates = sorted(
        (REPO / ".scratch" / "tools").glob("helm-*/darwin-arm64/helm"),
        reverse=True,
    )
    if candidates:
        return str(candidates[0])
    return pytest.skip(
        "Helm is unavailable; every blocking fast-linux CI partition installs "
        "the checksum-pinned Helm before running this file"
    )


def test_values_schema_rejects_missing_authority_and_ephemeral_storage() -> None:
    schema = json.loads((CHART / "values.schema.json").read_text())
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    errors = sorted(
        Draft202012Validator(schema).iter_errors(values),
        key=lambda row: list(row.absolute_path),
    )
    paths = {tuple(row.absolute_path) for row in errors}
    assert ("okg", "deployment") in paths
    assert ("secrets", "existingSecret") in paths
    assert ("postgresql", "storage", "storageClassName") in paths
    assert ("runtime", "storage", "storageClassName") in paths


def test_runtime_image_definition_is_locked_non_root_and_secret_safe() -> None:
    dockerfile = DOCKERFILE.read_text()
    postgres_dockerfile = POSTGRES_DOCKERFILE.read_text()
    dockerignore = (REPO / ".dockerignore").read_text().splitlines()
    assert dockerfile.startswith("FROM ghcr.io/astral-sh/uv@sha256:")
    # One locked dependency sync; okg itself is installed from a wheel
    # (test_runtime_image_installs_okg_from_a_wheel_built_at_vcs_ref).
    assert dockerfile.count("uv sync --frozen") == 1
    assert "USER 10001:10001" in dockerfile
    assert "STOPSIGNAL SIGTERM" in dockerfile
    assert "OKG_INSTALL_OS_TOOLS" not in dockerfile
    assert "\n        gh \\" in dockerfile
    assert (
        "FROM docker.io/timescale/timescaledb-ha:pg17.10-ts2.29.0-all@sha256:"
        in postgres_dockerfile
    )
    assert "ARG POSTGRES_VERSION=17.10" in postgres_dockerfile
    assert "ARG TIMESCALEDB_VERSION=2.29.0" in postgres_dockerfile
    assert 'io.okg.timescaledb.version="${TIMESCALEDB_VERSION}"' in postgres_dockerfile
    # pg_textsearch 1.5.1 is upstream's release package installed over the
    # base image's bundled 1.3.0: the download is pinned by sha256 per
    # architecture, and the build checks the installed version afterwards.
    assert "ARG PG_TEXTSEARCH_VERSION=1.5.1" in postgres_dockerfile
    assert (
        "ARG PG_TEXTSEARCH_SHA256_AMD64="
        "ed4410e8b6879a8971cd87fd03853f182e47b85b199c9bfcd9b34f159caba9fb"
    ) in postgres_dockerfile
    assert (
        "ARG PG_TEXTSEARCH_SHA256_ARM64="
        "e65f953e99f47c05dde88a031884cf36ea269bcb62377f554b76ddb4d13fdc3e"
    ) in postgres_dockerfile
    assert "https://github.com/timescale/pg_textsearch/releases/download/" in postgres_dockerfile
    assert "sha256sum --check --strict -" in postgres_dockerfile
    assert "--proto '=https' --tlsv1.2" in postgres_dockerfile
    assert "--retry 5 --retry-all-errors" in postgres_dockerfile
    # The library's exact version, every upgrade script from the base's
    # 1.3.0, and no stale 1.3.0 install script.
    assert 'test "$LIB_VERSION" = "$PG_TEXTSEARCH_VERSION"' in postgres_dockerfile
    assert 'ARG PG_TEXTSEARCH_UPGRADE_PATH="1.3.0 1.3.1 1.4.0 1.5.0 1.5.1"' in postgres_dockerfile
    assert "pg_textsearch--1.3.0.sql\"" in postgres_dockerfile
    assert "grep -q -a -F" not in postgres_dockerfile
    assert 'test "$PGTS_VERSION" = "$PG_TEXTSEARCH_VERSION"' in postgres_dockerfile
    assert '"${PG_TEXTSEARCH_VERSION}-1"' in postgres_dockerfile
    assert (
        'io.okg.pg_textsearch.version="${PG_TEXTSEARCH_VERSION}"'
        in postgres_dockerfile
    )
    # The download is checked before anything is installed from it.
    assert postgres_dockerfile.index("sha256sum --check") < postgres_dockerfile.index(
        "dpkg --install"
    )
    for pattern in (".git", ".secrets", "secrets/", "*.env", "*.pem", "*.key"):
        assert pattern in dockerignore


def _dockerfile_instructions() -> list[str]:
    """Non-comment Dockerfile lines, continuation lines joined."""
    joined = DOCKERFILE.read_text().replace("\\\n", " ")
    return [
        " ".join(line.split())
        for line in joined.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def test_runtime_image_installs_okg_from_a_wheel_built_at_vcs_ref() -> None:
    """An editable okg in the image could not publish outside /opt/okg.

    Release authority needs an okg Git checkout for an editable install, and
    the image has no .git. A wheel install is named by its RECORD instead, so
    the image must import okg from a wheel built with the commit VCS_REF
    names, and nothing may put /opt/okg/src back on the import path.
    """

    lines = _dockerfile_instructions()
    stages = [line for line in lines if line.startswith("FROM ")]
    assert [stage.split()[-1] for stage in stages] == ["wheel", "runtime"]
    wheel_stage = lines[: lines.index(stages[1])]
    runtime_stage = lines[lines.index(stages[1]) :]

    build = next(line for line in wheel_stage if "uv build --wheel" in line)
    assert 'OKG_BUILD_SOURCE_REVISION="${VCS_REF}" uv build --wheel' in build
    # CI supplies the tree and clean/dirty state; unsupplied means unknown.
    for arg in (
        "ARG VCS_REF=unknown",
        "ARG OKG_BUILD_SOURCE_TREE=unknown",
        "ARG OKG_BUILD_SOURCE_DIRTY=unknown",
    ):
        assert arg in wheel_stage
    # The build backend is the setuptools uv.lock locks, so a rebuild of one
    # commit reproduces the payload digest a prod pin names.
    lock = (REPO / "uv.lock").read_text()
    locked = lock.split('name = "setuptools"\nversion = "', 1)[1].split('"', 1)[0]
    assert f"printf 'setuptools=={locked}\\n' > /tmp/build-constraints.txt" in build
    assert "--build-constraints /tmp/build-constraints.txt" in build

    syncs = [line for line in runtime_stage if "uv sync" in line]
    assert len(syncs) == 1 and "--no-install-project" in syncs[0]
    assert "COPY --from=wheel /dist/ /tmp/okg-wheel/" in runtime_stage
    install = next(line for line in runtime_stage if "uv pip install" in line)
    assert "--no-deps /tmp/okg-wheel/okg-*.whl" in install
    # uv writes the --python it was given into the okg console script's
    # shebang; the worker identity gate declares .venv/bin/python3.
    from okg.substrate import deployment_apply

    interpreter = str(deployment_apply.CONTAINER_INTERPRETER)
    assert f"uv pip install --python {interpreter} " in install
    assert f'test "$(head -n 1 /opt/okg/.venv/bin/okg)" = "#!{interpreter}"' in install
    assert "-e " not in install and "--editable" not in install
    # The build fails unless okg imports from a wheel whose RECORD verifies.
    assert "installed_framework_distribution" in install
    assert "assert i is not None" in install

    assert not any("PYTHONPATH" in line for line in lines)
    assert any("UV_NO_SYNC=1" in line for line in runtime_stage)
    assert any("OKG_FRAMEWORK_TREE=/opt/okg" in line for line in runtime_stage)
    # The installed wheel stays root-owned; only the rest of /opt/okg goes to
    # the runtime user.
    assert "chown -R okg:okg /opt/okg " not in install
    assert (
        "find /opt/okg -mindepth 1 -maxdepth 1 ! -name .venv "
        "-exec chown -R okg:okg {} +"
    ) in install
    # What external images build on stays put.
    assert "USER 10001:10001" in runtime_stage
    assert runtime_stage[-2:] == [
        'LABEL org.opencontainers.image.revision="${VCS_REF}"',
        'ENV OKG_CODE_REVISION="${VCS_REF}"',
    ]


def test_internal_and_external_postgres_renders_are_safe() -> None:
    helm = _helm()
    internal, rendered = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
    )
    report = inspect_render(
        internal,
        expect_postgres=True,
        expect_mirror=False,
        expect_repository_sync=False,
        expect_worker_secrets=False,
    )
    assert report["runtime_containers"] == ["mcp", "worker"]
    assert report["mcp_service_type"] == "ClusterIP"
    runtime = next(
        row
        for row in internal
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "runtime"
    )
    worker = next(
        row
        for row in runtime["spec"]["template"]["spec"]["containers"]
        if row["name"] == "worker"
    )
    mcp = next(
        row
        for row in runtime["spec"]["template"]["spec"]["containers"]
        if row["name"] == "mcp"
    )
    assert {"localhost:*", "127.0.0.1:*", "[::1]:*"}.issubset(set(mcp["args"]))
    assert {
        key: worker["startupProbe"][key]
        for key in ("periodSeconds", "timeoutSeconds", "failureThreshold")
    } == {
        "periodSeconds": 30,
        "timeoutSeconds": 60,
        "failureThreshold": 30,
    }
    assert {
        key: worker["readinessProbe"][key]
        for key in ("periodSeconds", "timeoutSeconds", "failureThreshold")
    } == {
        "periodSeconds": 60,
        "timeoutSeconds": 60,
        "failureThreshold": 3,
    }
    assert "kind: Secret" not in rendered
    assert "postgresql://postgres:" not in rendered
    postgres = next(
        row
        for row in internal
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "postgres"
    )
    assert postgres["spec"]["template"]["spec"]["securityContext"] == {
        "fsGroup": 1000,
        "fsGroupChangePolicy": "OnRootMismatch",
        "runAsGroup": 1000,
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    # Lock-table sizing is load-bearing, not tuning. Postgres sizes the shared
    # lock table as max_locks_per_transaction * (max_connections +
    # max_prepared_transactions); at the initdb default of 64 that is 6400
    # slots, and a catalog apply takes a lock per relation it touches. The
    # nightly Kubernetes lifecycle proof for okg-workspace failed
    # catalog_version 9 during alias_seeding with `out of shared memory /
    # You might need to increase "max_locks_per_transaction"` (run
    # 33513581450) because this chart never set it. The dogfood compose
    # profile has run 1024 since fix-dogfood-postgres-lock-and-memory-sizing.
    postgres_args = next(
        row
        for row in postgres["spec"]["template"]["spec"]["containers"]
        if row["name"] == "postgres"
    )["args"]
    assert "max_locks_per_transaction=1024" in postgres_args, postgres_args

    external, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=False,
    )
    external_report = inspect_render(
        external,
        expect_postgres=False,
        expect_mirror=False,
        expect_repository_sync=False,
        expect_worker_secrets=False,
    )
    assert external_report["postgres"] is False


def _claim_templates(documents: list[dict]) -> dict[str, list[dict]]:
    return {
        row["metadata"]["name"]: row["spec"]["volumeClaimTemplates"]
        for row in documents
        if row.get("kind") == "StatefulSet"
    }


def test_chart_version_bump_leaves_volume_claim_templates_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Kubernetes refuses any change to a StatefulSet's volumeClaimTemplates,
    # so a chart or app version stamped into them makes `helm upgrade` fail
    # after every version bump (issue 2842). Render the chart as it is and as
    # a copy with both versions bumped; the claim templates must be identical.
    helm = _helm()
    bumped = tmp_path / "okg"
    shutil.copytree(CHART, bumped, symlinks=True)
    chart_yaml = yaml.safe_load((bumped / "Chart.yaml").read_text())
    chart_yaml["version"] = "99.1.0"
    chart_yaml["appVersion"] = "99.1.0-bumped"
    (bumped / "Chart.yaml").write_text(yaml.safe_dump(chart_yaml, sort_keys=False))

    before, _ = render_chart(
        helm=helm, deployment="kubernetes-smoke", internal_postgres=True
    )
    monkeypatch.setattr(kubernetes_verifier, "CHART", bumped)
    after, after_text = render_chart(
        helm=helm, deployment="kubernetes-smoke", internal_postgres=True
    )
    # The bump really reached the render; otherwise equality proves nothing.
    assert "okg-99.1.0" in after_text
    assert "99.1.0-bumped" in after_text

    before_claims = _claim_templates(before)
    after_claims = _claim_templates(after)
    assert sorted(before_claims) == [
        "verification-okg-postgres",
        "verification-okg-runtime",
    ]
    assert after_claims == before_claims
    for claims in before_claims.values():
        for claim in claims:
            labels = claim["metadata"]["labels"]
            assert "helm.sh/chart" not in labels
            assert "app.kubernetes.io/version" not in labels


def test_bootstrap_proceeds_on_the_provision_capacity_warning_only(
    tmp_path: Path,
) -> None:
    """Decision 23a: the bootstrap init container runs `okg provision` and
    treats exit 8 (published with a capacity warning) as success. Every other
    non-zero code still fails the init container."""
    helm = _helm()
    documents, _rendered = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
    )
    runtime = next(
        row
        for row in documents
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "runtime"
    )
    bootstrap = next(
        row
        for row in runtime["spec"]["template"]["spec"]["initContainers"]
        if row["name"] == "bootstrap"
    )
    assert bootstrap["args"][0] == "provision"
    argv_file = tmp_path / "argv"
    for okg_exit, expected in ((0, 0), (8, 0), (1, 1), (2, 2), (3, 3)):
        stub = tmp_path / "okg"
        stub.write_text(
            f'#!/bin/sh\nprintf "%s\\n" "$@" > "{argv_file}"\nexit {okg_exit}\n',
            encoding="utf-8",
        )
        stub.chmod(0o755)
        result = subprocess.run(
            [*bootstrap["command"], *bootstrap["args"]],
            env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == expected, (okg_exit, result.stderr)
        assert argv_file.read_text().splitlines() == [
            str(arg) for arg in bootstrap["args"]
        ]


def test_verifier_requires_the_capacity_warning_bootstrap_wrapper(
    tmp_path: Path,
) -> None:
    """Helm-free: the verifier refuses a bare `okg provision` bootstrap, the
    template carries the exact wrapper, and the wrapper maps only 8 to 0."""
    from scripts.verify_kubernetes_chart import (
        BOOTSTRAP_COMMAND,
        check_bootstrap_container,
    )

    check_bootstrap_container({"command": BOOTSTRAP_COMMAND, "args": ["provision"]})
    for bad in (
        {"command": ["okg"], "args": ["provision"]},
        {"command": ["/bin/sh", "-c", 'okg "$@"', "okg-bootstrap"], "args": ["provision"]},
        {"command": BOOTSTRAP_COMMAND, "args": ["runtime", "bootstrap"]},
    ):
        with pytest.raises(VerificationError):
            check_bootstrap_container(bad)
    template = (CHART / "templates" / "runtime-statefulset.yaml").read_text()
    block = template.split("- name: bootstrap\n", 1)[1].split("args:", 1)[0]
    command = yaml.safe_load(block.split("command:", 1)[1])
    assert command == BOOTSTRAP_COMMAND
    stub = tmp_path / "okg"
    for okg_exit, expected in ((0, 0), (8, 0), (1, 1), (3, 3)):
        stub.write_text(f"#!/bin/sh\nexit {okg_exit}\n", encoding="utf-8")
        stub.chmod(0o755)
        result = subprocess.run(
            [*BOOTSTRAP_COMMAND, "provision"],
            env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
            check=False,
        )
        assert result.returncode == expected, okg_exit


def test_verifier_accepts_only_the_rename_compat_probe_prefix() -> None:
    """Helm-free: worker probes run `okg deployment ready`, bare or behind
    exactly `env OKG_RENAMED_FLAG_WARNINGS=0` (decision 310 compat); any
    other prefix is refused."""
    from scripts.verify_kubernetes_chart import check_worker_probe_command

    ready = ["okg", "deployment", "ready", "dep", "--profile", "general"]
    check_worker_probe_command(ready)
    check_worker_probe_command(["env", "OKG_RENAMED_FLAG_WARNINGS=0", *ready])
    for bad in (
        [],
        ["okg", "doctor"],
        ["env", "OKG_RENAMED_FLAG_WARNINGS=1", *ready],
        ["env", "OKG_DSN=postgresql://x", *ready],
        ["env", "OKG_RENAMED_FLAG_WARNINGS=0", "sh", "-c", " ".join(ready)],
        ["/bin/sh", "-c", " ".join(ready)],
    ):
        with pytest.raises(VerificationError, match="deployment readiness"):
            check_worker_probe_command(bad)


def test_operator_commands_match_multi_container_test_and_metrics_cli() -> None:
    runbook = (
        REPO / "deployments" / "okg-ci" / "kubernetes" / "README.md"
    ).read_text()
    notes = (CHART / "templates" / "NOTES.txt").read_text()
    assert "helm test okg-ci -n okg-ci --logs" not in runbook
    assert "helm test {{ .Release.Name }} -n {{ .Release.Namespace }} --logs" not in notes
    assert "logs pod/okg-ci-okg-readiness-test -c readiness" in runbook
    assert "logs pod/okg-ci-okg-readiness-test -c mcp" in runbook
    assert "okg metrics --qa-snapshot" in runbook
    assert '--generation "${GENERATION_ID}"' in runbook


def test_okg_ci_openstack_overlay_has_expected_topology() -> None:
    helm = _helm()
    documents, rendered = render_chart(
        helm=helm,
        deployment="okg-ci",
        values=OPENSTACK_VALUES,
        internal_postgres=True,
    )
    report = inspect_render(
        documents,
        expect_postgres=True,
        expect_mirror=True,
        expect_repository_sync=True,
        expect_worker_secrets=True,
    )
    assert report["runtime_containers"] == ["mcp", "mirror", "worker"]
    # okg-ci turns the disruption budget on explicitly; the default is off.
    assert report["disruption_budget"] is True
    assert "OKG_CI_DSN" in rendered
    assert "csi-cinder-sc" in rendered
    assert "100Gi" in rendered
    assert "50Gi" in rendered
    assert "github-token" in rendered
    assert "repository-sync" in rendered
    assert "fetch --force --prune" in rendered
    assert "remote set-url origin" in rendered
    assert "clean -ffdx" in rendered
    assert "password=$GIT_PASSWORD" in rendered
    assert "password=gh" not in rendered.lower()
    assert "OKG_DEPLOYMENTS_DIR" in rendered
    assert "/var/lib/okg-ci/repository/deployments" in rendered
    config = next(row for row in documents if row.get("kind") == "ConfigMap")
    assert config["data"]["runtime-environment"] == "kubernetes"
    assert "kind: Secret" not in rendered
    assert "value: gh" not in rendered.lower()
    readiness_test = next(
        row for row in documents if row.get("metadata", {}).get("annotations", {}).get(
            "helm.sh/hook"
        ) == "test"
    )
    test_containers = readiness_test["spec"]["containers"]
    assert [row["name"] for row in test_containers] == ["readiness", "mcp"]
    assert test_containers[0]["resources"] == {
        "requests": {"cpu": "500m", "memory": "1Gi"},
        "limits": {"cpu": "4", "memory": "4Gi"},
    }
    assert test_containers[1]["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "1", "memory": "1Gi"},
    }
    readiness_script = test_containers[0]["args"][0]
    assert 'while [ "$attempt" -lt 30 ]' in readiness_script
    subprocess.run(
        ["/bin/sh", "-n"],
        input=readiness_script,
        text=True,
        check=True,
        capture_output=True,
    )
    mcp_test_script = test_containers[1]["args"][0]
    compile(mcp_test_script, "<mcp-helm-test>", "exec")
    assert '"inspect"' in mcp_test_script
    assert mcp_test_script.count('"filter"') == 2
    assert '"visible_node_sample_count"' in mcp_test_script
    assert '"visible_edge_sample_count"' in mcp_test_script
    assert '"node_sample_pinned_generation_id"' in mcp_test_script
    assert '"edge_sample_pinned_generation_id"' in mcp_test_script
    # `filter` returns `items`; the counting itself is executed against a
    # real server in tests/substrate/mcp/test_kubernetes_readiness_hook_mcp.py.
    assert 'response.get("items")' in mcp_test_script
    assert '"rows"' not in mcp_test_script
    assert readiness_test["spec"]["volumes"] == [{"name": "tmp", "emptyDir": {}}]
    for container in test_containers:
        assert {"name": "tmp", "mountPath": "/tmp"} in container["volumeMounts"]
    runtime = next(
        row
        for row in documents
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "runtime"
    )
    repository_script = runtime["spec"]["template"]["spec"]["initContainers"][0][
        "args"
    ][0]
    subprocess.run(
        ["/bin/sh", "-n"],
        input=repository_script,
        text=True,
        check=True,
        capture_output=True,
    )
    pod_spec = runtime["spec"]["template"]["spec"]
    containers_by_name = {
        row["name"]: row
        for row in [*pod_spec["initContainers"], *pod_spec["containers"]]
    }
    compile(containers_by_name["mcp-role"]["args"][0], "<mcp-role>", "exec")
    for container in [
        *pod_spec["initContainers"],
        *pod_spec["containers"],
    ]:
        if container["name"] == "worker-secrets":
            continue
        env_names = {row["name"] for row in container["env"]}
        assert "OKG_REPO_ROOT" in env_names
        assert "OKG_DEPLOYMENTS_DIR" in env_names
        assert "OKG_RUNTIME_ENVIRONMENT" in env_names
    assert containers_by_name["runtime-bootstrap"]["args"][:2] == [
        "runtime",
        "bootstrap",
    ]
    for name in ("bootstrap", "runtime-bootstrap", "mcp-role", "worker", "mirror"):
        subject = next(
            row
            for row in containers_by_name[name]["env"]
            if row["name"] == "OKG_NOMOS_SUBJECT"
        )
        assert subject["valueFrom"]["configMapKeyRef"]["key"] == (
            "runtime-nomos-subject"
        )
    mcp_subject = next(
        row
        for row in containers_by_name["mcp"]["env"]
        if row["name"] == "OKG_NOMOS_SUBJECT"
    )
    assert mcp_subject["valueFrom"]["configMapKeyRef"]["key"] == (
        "mcp-nomos-subject"
    )
    mcp_args = containers_by_name["mcp"]["args"]
    assert "verification-okg-mcp:*" in mcp_args
    assert "verification-okg-mcp.default.svc:*" in mcp_args
    for name in (
        "repository-sync",
        "bootstrap",
        "runtime-bootstrap",
        "mcp-role",
        "mcp",
    ):
        assert "GH_TOKEN" not in {
            row["name"] for row in containers_by_name[name]["env"]
        }
    for name in ("worker", "mirror"):
        assert "GH_TOKEN" in {
            row["name"] for row in containers_by_name[name]["env"]
        }
    mirror_env = {row["name"] for row in containers_by_name["mirror"]["env"]}
    assert {"OKG_DSN", "OKG_MCP_RO_DSN"} <= mirror_env
    assert "MCP_ROLE_PASSWORD" in {
        row["name"] for row in containers_by_name["mcp-role"]["env"]
    }
    assert "OKG_MCP_RO_DSN" in {
        row["name"] for row in containers_by_name["mcp"]["env"]
    }
    assert containers_by_name["worker"]["resources"] == {
        "requests": {"cpu": "1", "memory": "2Gi"},
        "limits": {"cpu": "4", "memory": "4Gi"},
    }
    assert containers_by_name["mcp"]["resources"]["limits"]["memory"] == "1Gi"
    assert containers_by_name["mirror"]["resources"]["limits"]["memory"] == "768Mi"
    worker_secret_mount = {
        "name": "worker-secrets",
        "mountPath": "/home/ubuntu/.ssh",
        "subPath": "fleet-monitor",
        "readOnly": True,
    }
    assert worker_secret_mount in containers_by_name["worker"]["volumeMounts"]
    for name in ("mcp", "mirror"):
        assert worker_secret_mount not in containers_by_name[name]["volumeMounts"]
    worker_secret_volume = next(
        row for row in pod_spec["volumes"] if row["name"] == "worker-secrets"
    )
    assert worker_secret_volume["emptyDir"] == {
        "medium": "Memory",
        "sizeLimit": "4Mi",
    }
    fleet_source = next(
        row
        for row in pod_spec["volumes"]
        if row["name"] == "worker-secret-source-fleet-monitor"
    )
    assert fleet_source["secret"]["items"] == [
        {"key": "fleet-monitor-ssh-key", "path": "okg_fleet_monitor", "mode": 288},
        {"key": "fleet-monitor-known-hosts", "path": "known_hosts", "mode": 288},
    ]
    worker_secret_init = containers_by_name["worker-secrets"]
    assert "stat -c %u:%g:%a" in worker_secret_init["args"][0]
    assert "10001:10001:600" in worker_secret_init["args"][0]
    assert "10001:10001:644" in worker_secret_init["args"][0]
    subprocess.run(
        ["/bin/sh", "-n"],
        input=worker_secret_init["args"][0],
        text=True,
        check=True,
        capture_output=True,
    )
    claims = runtime["spec"]["volumeClaimTemplates"]
    assert claims[0]["spec"]["storageClassName"] == "csi-cinder-sc"
    assert claims[0]["spec"]["resources"]["requests"]["storage"] == "50Gi"
    postgres = next(
        row
        for row in documents
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "postgres"
    )
    pg_claim = postgres["spec"]["volumeClaimTemplates"][0]
    assert pg_claim["spec"]["storageClassName"] == "csi-cinder-sc"
    assert pg_claim["spec"]["resources"]["requests"]["storage"] == "100Gi"
    assert postgres["spec"]["template"]["spec"]["containers"][0]["resources"][
        "limits"
    ]["memory"] == "3Gi"
    readiness_subject = next(
        row
        for row in test_containers[0]["env"]
        if row["name"] == "OKG_NOMOS_SUBJECT"
    )
    assert readiness_subject["valueFrom"]["configMapKeyRef"]["key"] == (
        "runtime-nomos-subject"
    )
    assert "GH_TOKEN" not in {row["name"] for row in test_containers[0]["env"]}
    services = [row for row in documents if row.get("kind") == "Service"]
    headless = next(row for row in services if row["spec"].get("clusterIP") == "None")
    assert "ports" not in headless["spec"]
    assert "publishNotReadyAddresses" not in headless["spec"]


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("runtime.replicas", "2", "additional properties 'replicas' not allowed"),
        (
            "mcp.service.type",
            "LoadBalancer",
            "additional properties 'service' not allowed",
        ),
    ],
)
def test_chart_fails_closed_on_unsafe_topology_overrides(
    path: str,
    value: str,
    message: str,
) -> None:
    helm = _helm()
    with pytest.raises(RuntimeError, match=message):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=["--set-string", f"{path}={value}"],
        )


def test_chart_fails_closed_on_reserved_secret_env_and_incomplete_repo() -> None:
    helm = _helm()
    with pytest.raises(RuntimeError, match="reserved by the chart"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=[
                "--set-string",
                "secrets.extraEnv[0].name=OKG_DSN",
                "--set-string",
                "secrets.extraEnv[0].key=bad",
                "--set-string",
                "secrets.extraEnv[0].targets[0]=worker",
            ],
        )
    with pytest.raises(RuntimeError, match="repositorySync.url"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=["--set", "repositorySync.enabled=true"],
        )
    for unsafe_url in (
        "https://embedded-token@example.com/repo.git",
        "https://example.com/repo.git?token=embedded",
    ):
        with pytest.raises(RuntimeError, match="repositorySync.url"):
            render_chart(
                helm=helm,
                deployment="kubernetes-smoke",
                internal_postgres=True,
                values=None,
                set_values=[
                    "--set",
                    "repositorySync.enabled=true",
                    "--set-string",
                    f"repositorySync.url={unsafe_url}",
                    "--set-string",
                    "repositorySync.revision=reviewed",
                ],
            )
    with pytest.raises(RuntimeError, match="okg.deploymentDsnEnv"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=[
                "--set-string",
                "okg.deploymentDsnEnv=OKG_REPO_ROOT",
            ],
        )
    with pytest.raises(RuntimeError, match="mcp.manageRole"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=["--set", "mcp.manageRole=false"],
        )



_SMOKE_REPOSITORY_SYNC = (
    "--set", "repositorySync.enabled=true",
    "--set-string", "repositorySync.url=https://example.com/repo.git",
    "--set-string", "repositorySync.revision=reviewed-revision",
    "--set", "bootstrap.requireApprovalRevision=true",
)


def test_chart_fails_closed_when_the_revision_is_not_the_approved_one() -> None:
    """bootstrap.approvedRevision must equal repositorySync.revision (shipped deployment)."""
    helm = _helm()
    # The control: the approved revision matches, so the render succeeds and
    # the refusal below is the mismatch's.
    render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[
            *_SMOKE_REPOSITORY_SYNC,
            "--set-string", "bootstrap.approvedRevision=reviewed-revision",
        ],
    )
    with pytest.raises(RuntimeError, match="bootstrap.approvedRevision"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=[
                *_SMOKE_REPOSITORY_SYNC,
                "--set-string", "bootstrap.approvedRevision=a-different-revision",
            ],
        )


def test_okg_ci_overlay_fails_closed_on_reserved_env_and_unreviewed_revision() -> None:
    """The okg-ci arms, split out of the test above.

    deployments/okg-ci is this repository's own deployment; the public copy
    skips this test (tests/mirror_scope.py).
    """
    helm = _helm()
    values = private_path("deployments/okg-ci/kubernetes/values.openstack.yaml")
    with pytest.raises(RuntimeError, match="reserved by the chart"):
        render_chart(
            helm=helm,
            deployment="okg-ci",
            internal_postgres=True,
            values=values,
            set_values=[
                "--set-string",
                "secrets.extraEnv[1].name=OKG_CI_DSN",
                "--set-string",
                "secrets.extraEnv[1].key=bad",
                "--set-string",
                "secrets.extraEnv[1].targets[0]=worker",
            ],
        )
    with pytest.raises(RuntimeError, match="bootstrap.approvedRevision"):
        render_chart(
            helm=helm,
            deployment="okg-ci",
            internal_postgres=True,
            values=values,
            set_values=[
                "--set-string",
                "repositorySync.revision=unreviewed-revision",
            ],
        )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("worker.secretMounts[0].mountDir", "/tmp/$(id)"),
        ("worker.secretMounts[0].mountDir", "/tmp/../escape"),
        ("worker.secretMounts[0].files[0].fileName", "../key"),
        ("worker.secretMounts[0].files[0].fileName", "key`id`"),
    ],
)
def test_worker_secret_paths_reject_shell_and_traversal_input(
    path: str,
    value: str,
) -> None:
    # A valid secret mount on the shipped kubernetes-smoke deployment; each
    # case overrides one field with an unsafe value (a later --set wins).
    helm = _helm()
    # The control: the mount as written renders, so the refusal below is
    # the unsafe value's, not an incomplete mount's.
    render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=list(_SAFE_SECRET_MOUNT),
    )
    with pytest.raises(RuntimeError, match="values don't meet the specifications"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=[*_SAFE_SECRET_MOUNT, "--set-string", f"{path}={value}"],
        )


_SAFE_SECRET_MOUNT = (
    "--set-string", "worker.secretMounts[0].name=example-ssh",
    "--set-string", "worker.secretMounts[0].mountDir=/home/okg/.ssh",
    "--set-string", "worker.secretMounts[0].files[0].secretKey=example-ssh-key",
    "--set-string", "worker.secretMounts[0].files[0].fileName=id_example",
    "--set-string", "worker.secretMounts[0].files[0].mode=0600",
)


def test_live_proof_never_mutates_operator_kubeconfig(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operator_kubeconfig = "/operator/openstack-kubeconfig"
    monkeypatch.setenv("KUBECONFIG", operator_kubeconfig)
    before = set((REPO / ".scratch").glob("okg-kubernetes-proof-*"))

    def fail_build(*_args: object, **_kwargs: object) -> None:
        raise VerificationError("synthetic build failure")

    monkeypatch.setattr(
        kubernetes_verifier,
        "_docker_build_images",
        fail_build,
    )
    with pytest.raises(VerificationError, match="synthetic build failure"):
        kubernetes_verifier.live_kind_proof(
            helm="helm",
            kind="kind",
            kubectl="kubectl",
            deployment="kubernetes-smoke",
            keep_cluster=False,
        )

    assert os.environ["KUBECONFIG"] == operator_kubeconfig
    assert set((REPO / ".scratch").glob("okg-kubernetes-proof-*")) == before


def test_kind_cleanup_failure_is_not_silently_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_delete(*_args: object, **_kwargs: object) -> None:
        raise VerificationError("synthetic cleanup failure")

    monkeypatch.setattr(kubernetes_verifier, "_run", fail_delete)
    with pytest.raises(VerificationError, match="synthetic cleanup failure"):
        _delete_kind_cluster(kind="kind", cluster="proof", env={})


def test_okg_ci_kubernetes_runtime_omits_legacy_local_vm_probe() -> None:
    path = REPO / "deployments" / "okg-ci" / "workflows" / "resource_monitor.py"
    spec = importlib.util.spec_from_file_location("okg_ci_resource_monitor", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    hosts = [
        type("Host", (), {"target": "local"})(),
        type("Host", (), {"target": "ubuntu@runner"})(),
    ]

    assert module._select_hosts_for_runtime(hosts, "host") == hosts
    assert module._select_hosts_for_runtime(hosts, "kubernetes") == hosts[1:]
    with pytest.raises(ValueError, match="OKG_RUNTIME_ENVIRONMENT"):
        module._select_hosts_for_runtime(hosts, "unknown")


def test_okg_ci_resource_monitor_output_binds_host_names_to_node_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import okg.substrate.deployments as deployments
    from okg.substrate.external_backed import collector
    import psycopg

    path = REPO / "deployments" / "okg-ci" / "workflows" / "resource_monitor.py"
    spec = importlib.util.spec_from_file_location("okg_ci_resource_monitor_output", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    hosts = [type("Host", (), {"name": "okg-ci", "target": "local"})()]
    connection = object()

    class ConnectionContext:
        def __enter__(self):
            return connection

        def __exit__(self, *_args):
            return False

    monkeypatch.setenv("OKG_RUNTIME_ENVIRONMENT", "host")
    inventory = tmp_path / "runner-fleet-inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "schema": "okg.runner_fleet_inventory/v1",
                "schema_version": 1,
                "generated_at": "2030-01-01T00:00:00+00:00",
                "freshness": "fresh",
                "completeness": {"status": "complete", "expected_count": 0},
                "producer": "test-controller",
                "readiness": {"ready": True, "blockers": []},
                "runners": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_CI_RUNNER_FLEET_INVENTORY", str(inventory))
    load_inventory = module._load_runner_inventory
    monkeypatch.setattr(
        module,
        "_load_runner_inventory",
        lambda: load_inventory(
            inventory,
            now=module._parse_timestamp("2030-01-01T00:01:00+00:00"),
        ),
    )
    monkeypatch.setattr(collector, "load_hosts", lambda _path: hosts)
    monkeypatch.setattr(
        collector,
        "run_collection",
        # `**_kwargs` absorbs `write_guard`, which the collector now takes so
        # the deployment writer permit wraps the okg writes without wrapping
        # the ssh probes (issue #1338).
        lambda _conn, *, deployment_name, hosts, **_kwargs: {
            "host_count": 1,
            "hosts": {
                "okg-ci": {
                    "reachable": True,
                    "node_id": "host:okg-ci",
                }
            },
        },
    )
    monkeypatch.setattr(deployments, "resolve_deployment", lambda _name: object())
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: "postgres://unused",
    )
    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: ConnectionContext())

    result = module.run_resource_monitor()
    assert result["ok"] is True
    assert result["host_count"] == 1
    assert result["reachable"] == {"okg-ci": True}
    assert result["node_ids"] == {"okg-ci": "host:okg-ci"}
    assert result["freshness"] == {"producer": "fresh", "sample": "fresh"}
    assert result["completeness"] == {
        "inventory": "complete",
        "sample": "complete",
        "expected_host_count": 1,
        "observed_host_count": 1,
    }


def test_smoke_deployment_declares_fast_health_schedules_and_disk_bounds() -> None:
    manifest = yaml.safe_load(SMOKE_DEPLOYMENT.read_text())
    assert manifest["runtime"]["schedules"] == {
        "okg_publisher_cycle": "*/30 * * * * *",
        "okg_substrate_doctor": "*/30 * * * * *",
    }
    assert manifest["host_storage"] == {
        "warn_available_pct": 15,
        "blocker_available_pct": 6,
    }


def test_wait_for_json_ok_retries_nonzero_health_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            subprocess.CompletedProcess(
                args=["okg", "doctor"],
                returncode=1,
                stdout='{"ok": false, "errors": [{"code": "warming"}]}',
                stderr="",
            ),
            subprocess.CompletedProcess(
                args=["okg", "doctor"],
                returncode=0,
                stdout='{"ok": true}',
                stderr="",
            ),
        ]
    )
    monkeypatch.setattr(kubernetes_verifier, "_run", lambda *_args, **_kwargs: next(responses))
    monkeypatch.setattr(kubernetes_verifier.time, "sleep", lambda _seconds: None)

    assert _wait_for_json_ok(["okg", "doctor"], timeout=5) == {"ok": True}


def test_wait_for_clean_qa_metrics_retries_transient_queue_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = "okg:kubernetes-smoke:branch:default:gen:proof"
    active = {
        "generation_id": generation_id,
        "queue_depth": {
            "ingest": 1,
            "link": 0,
            "enrich": 0,
            "publish": 0,
            "embed": 0,
        },
        "dlq_count": 0,
        "generation_blocked_count": 0,
        "doctor_exit_code": 0,
    }
    clean = {
        **active,
        "queue_depth": {
            key: 0
            for key in active["queue_depth"]
        },
    }
    responses = iter(
        [
            subprocess.CompletedProcess(
                args=["okg", "metrics"],
                returncode=0,
                stdout=json.dumps(active),
                stderr="",
            ),
            subprocess.CompletedProcess(
                args=["okg", "metrics"],
                returncode=0,
                stdout=json.dumps(clean),
                stderr="",
            ),
        ]
    )
    monkeypatch.setattr(
        kubernetes_verifier,
        "_run",
        lambda *_args, **_kwargs: next(responses),
    )
    monkeypatch.setattr(
        kubernetes_verifier.time,
        "sleep",
        lambda _seconds: None,
    )

    assert _wait_for_clean_qa_metrics(
        ["okg", "metrics"],
        generation_id=generation_id,
        timeout=5,
    ) == clean


def test_wait_for_clean_qa_metrics_rejects_terminal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = "okg:kubernetes-smoke:branch:default:gen:proof"
    terminal = {
        "generation_id": generation_id,
        "queue_depth": {
            "ingest": 0,
            "link": 0,
            "enrich": 0,
            "publish": 0,
            "embed": 0,
        },
        "dlq_count": 1,
        "generation_blocked_count": 0,
        "doctor_exit_code": 0,
    }
    monkeypatch.setattr(
        kubernetes_verifier,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=["okg", "metrics"],
            returncode=0,
            stdout=json.dumps(terminal),
            stderr="",
        ),
    )
    monkeypatch.setattr(
        kubernetes_verifier.time,
        "sleep",
        lambda _seconds: pytest.fail("terminal metrics must not be retried"),
    )

    with pytest.raises(VerificationError, match="terminal QA metrics failure"):
        _wait_for_clean_qa_metrics(
            ["okg", "metrics"],
            generation_id=generation_id,
            timeout=5,
        )


class _FakeClock:
    """A monotonic clock that only moves when the wait sleeps."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_wait_for_clean_qa_metrics_timeout_reports_last_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = "okg:kubernetes-smoke:branch:default:gen:proof"
    busy = {
        "generation_id": generation_id,
        "queue_depth": {
            "ingest": 1,
            "link": 0,
            "enrich": 0,
            "publish": 0,
            "embed": 0,
        },
        "dlq_count": 0,
        "generation_blocked_count": 0,
        "doctor_exit_code": 0,
    }
    clock = _FakeClock()
    monkeypatch.setattr(kubernetes_verifier.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(kubernetes_verifier.time, "sleep", clock.sleep)
    monkeypatch.setattr(
        kubernetes_verifier,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=["okg", "metrics"],
            returncode=0,
            stdout=json.dumps(busy),
            stderr="",
        ),
    )

    with pytest.raises(VerificationError) as caught:
        _wait_for_clean_qa_metrics(
            ["okg", "metrics"],
            generation_id=generation_id,
            timeout=12,
            interval=5,
        )

    message = str(caught.value)
    assert "timed out waiting for clean" in message
    assert json.dumps(busy, sort_keys=True) in message


def test_wait_for_clean_qa_metrics_treats_call_timeout_as_one_try(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = "okg:kubernetes-smoke:branch:default:gen:proof"
    clean = {
        "generation_id": generation_id,
        "queue_depth": {"ingest": 0, "link": 0, "enrich": 0, "publish": 0, "embed": 0},
        "dlq_count": 0,
        "generation_blocked_count": 0,
        "doctor_exit_code": 0,
    }
    calls: list[int] = []

    def fake_run(argv, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
        return subprocess.CompletedProcess(
            args=argv, returncode=0, stdout=json.dumps(clean), stderr=""
        )

    clock = _FakeClock()
    monkeypatch.setattr(kubernetes_verifier.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(kubernetes_verifier.time, "sleep", clock.sleep)
    monkeypatch.setattr(kubernetes_verifier, "_run", fake_run)

    assert _wait_for_clean_qa_metrics(
        ["okg", "metrics"], generation_id=generation_id, timeout=60
    ) == clean
    assert len(calls) == 2


def test_wait_for_clean_qa_metrics_timeout_reports_last_call_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    clock = _FakeClock()
    monkeypatch.setattr(kubernetes_verifier.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(kubernetes_verifier.time, "sleep", clock.sleep)
    monkeypatch.setattr(kubernetes_verifier, "_run", fake_run)

    with pytest.raises(VerificationError, match="snapshot call timed out"):
        _wait_for_clean_qa_metrics(
            ["okg", "metrics"],
            generation_id="g",
            timeout=12,
            interval=5,
        )


def test_live_evidence_requires_generation_mcp_and_queue_alignment() -> None:
    generation_id = "okg:kubernetes-smoke:branch:default:gen:proof"
    described = {
        "generation_id": generation_id,
        "deployment_name": "kubernetes-smoke",
        "status": "published",
        "nodes_live_at_gen": 4,
        "edges_live_at_gen": 3,
        "projection_deltas": {
            "nodes_added": 4,
            "edges_added": 3,
            "noop": False,
        },
    }
    doctor = {"ok": True}
    metrics = {
        "generation_id": generation_id,
        "queue_depth": {
            "ingest": 0,
            "link": 0,
            "enrich": 0,
            "publish": 0,
            "embed": 0,
        },
        "dlq_count": 0,
        "generation_blocked_count": 0,
        "doctor_exit_code": 0,
    }
    readiness = {
        "ok": True,
        "generation": {
            "generation_id": generation_id,
            "count_integrity": {"ok": True},
        },
        "mcp": {"ok": True, "pinned_generation_id": generation_id},
        "queues": {
            "active_queue_depth": 0,
            "max_active_queue_depth": 0,
            "dlq_count": 0,
        },
    }
    mcp_http = {
        "pinned_generation_id": generation_id,
        "node_sample_pinned_generation_id": generation_id,
        "edge_sample_pinned_generation_id": generation_id,
        "node_count": 4,
        "edge_count": 3,
        "visible_node_sample_count": 4,
        "visible_edge_sample_count": 3,
    }
    report = _validate_live_evidence(
        generation_id=generation_id,
        described=described,
        doctor=doctor,
        metrics=metrics,
        readiness=readiness,
        mcp_http=mcp_http,
    )
    assert report["mcp_pinned_generation_id"] == generation_id
    assert report["nodes_live_at_gen"] == 4
    assert report["delta_proof_mode"] == "strict"

    predecessor_id = "okg:kubernetes-smoke:branch:default:gen:prior"
    strict_with_unavailable_predecessor = {
        **described,
        "predecessor_generation_id": predecessor_id,
    }
    strict_report = _validate_live_evidence(
        generation_id=generation_id,
        described=strict_with_unavailable_predecessor,
        predecessor_described=None,
        doctor=doctor,
        metrics=metrics,
        readiness=readiness,
        mcp_http=mcp_http,
    )
    assert strict_report["delta_proof_mode"] == "strict"

    predecessor = {
        "generation_id": predecessor_id,
        "deployment_name": "kubernetes-smoke",
        "status": "published",
        "nodes_live_at_gen": 4,
        "edges_live_at_gen": 3,
    }
    incremental = {
        **described,
        "deployment_name": "kubernetes-smoke",
        "predecessor_generation_id": predecessor_id,
        "projection_deltas": {
            "nodes_added": 0,
            "edges_added": 4,
            "noop": False,
        },
    }
    incremental_report = _validate_live_evidence(
        generation_id=generation_id,
        described=incremental,
        predecessor_described=predecessor,
        doctor=doctor,
        metrics=metrics,
        readiness=readiness,
        mcp_http=mcp_http,
    )
    assert incremental_report["delta_proof_mode"] == (
        "partial_with_populated_predecessor"
    )
    assert incremental_report["predecessor_generation_id"] == predecessor_id

    explicit_noop = {
        **incremental,
        "projection_deltas": {
            "nodes_added": 0,
            "edges_added": 0,
            "noop": True,
        },
    }
    noop_report = _validate_live_evidence(
        generation_id=generation_id,
        described=explicit_noop,
        predecessor_described=predecessor,
        doctor=doctor,
        metrics=metrics,
        readiness=readiness,
        mcp_http=mcp_http,
    )
    assert noop_report["delta_proof_mode"] == (
        "explicit_noop_with_populated_predecessor"
    )

    for invalid_described, invalid_predecessor in (
        (
            {
                **explicit_noop,
                "projection_deltas": {
                    "nodes_added": 0,
                    "edges_added": 0,
                    "noop": False,
                },
            },
            predecessor,
        ),
        (incremental, None),
        (
            incremental,
            {**predecessor, "generation_id": "different"},
        ),
        (
            {
                **incremental,
                "projection_deltas": {
                    "nodes_added": 0.5,
                    "edges_added": 4,
                    "noop": False,
                },
            },
            predecessor,
        ),
        (
            {
                **incremental,
                "projection_deltas": {
                    "nodes_added": 0,
                    "edges_added": 4,
                    "noop": True,
                },
            },
            predecessor,
        ),
        (
            {**incremental, "generation_id": "different"},
            predecessor,
        ),
        (
            {**incremental, "deployment_name": ""},
            predecessor,
        ),
        (
            {
                **incremental,
                "projection_deltas": {
                    "nodes_added": True,
                    "edges_added": 0,
                    "noop": False,
                },
            },
            predecessor,
        ),
        (
            {**incremental, "nodes_live_at_gen": True},
            predecessor,
        ),
        (
            {**incremental, "edges_live_at_gen": 1.5},
            predecessor,
        ),
        (
            {
                **incremental,
                "projection_deltas": {
                    "nodes_added": 0,
                    "edges_added": 4,
                    "noop": None,
                },
            },
            predecessor,
        ),
    ):
        with pytest.raises(
            VerificationError,
            match="must be|lacks|does not match",
        ):
            _validate_live_evidence(
                generation_id=generation_id,
                described=invalid_described,
                predecessor_described=invalid_predecessor,
                doctor=doctor,
                metrics=metrics,
                readiness=readiness,
                mcp_http=mcp_http,
            )

    bounded_mcp_http = {
        **mcp_http,
        "node_count": None,
        "edge_count": None,
        "truncated": True,
        "visibility": (
            "enforce; exact counts unavailable for a graph this size"
        ),
    }
    bounded_report = _validate_live_evidence(
        generation_id=generation_id,
        described=described,
        doctor=doctor,
        metrics=metrics,
        readiness=readiness,
        mcp_http=bounded_mcp_http,
    )
    assert bounded_report["mcp_http_visible_node_sample_count"] == 4
    empty_bounded_sample = {
        **bounded_mcp_http,
        "visible_node_sample_count": 0,
    }
    with pytest.raises(
        VerificationError,
        match="bounded graph samples are empty",
    ):
        _validate_live_evidence(
            generation_id=generation_id,
            described=described,
            doctor=doctor,
            metrics=metrics,
            readiness=readiness,
            mcp_http=empty_bounded_sample,
        )

    for invalid_visibility in (None, "unfiltered"):
        invalid_bounded_mcp_http = {
            **bounded_mcp_http,
            "visibility": invalid_visibility,
        }
        with pytest.raises(
            VerificationError,
            match="omitted counts without the canonical Nomos enforce",
        ):
            _validate_live_evidence(
                generation_id=generation_id,
                described=described,
                doctor=doctor,
                metrics=metrics,
                readiness=readiness,
                mcp_http=invalid_bounded_mcp_http,
            )

    readiness["mcp"]["pinned_generation_id"] = "different"
    with pytest.raises(VerificationError, match="generation alignment failed"):
        _validate_live_evidence(
            generation_id=generation_id,
            described=described,
            doctor=doctor,
            metrics=metrics,
            readiness=readiness,
            mcp_http=mcp_http,
        )

    readiness["mcp"]["pinned_generation_id"] = generation_id
    metrics["dlq_count"] = 1
    with pytest.raises(VerificationError, match="QA metrics are not clean"):
        _validate_live_evidence(
            generation_id=generation_id,
            described=described,
            doctor=doctor,
            metrics=metrics,
            readiness=readiness,
            mcp_http=mcp_http,
        )


def test_helm_test_log_parser_requires_one_structured_mcp_proof() -> None:
    payload = _mcp_proof_from_helm_test(
        'readiness ok\nOKG_MCP_PROOF={"node_count": 4, '
        '"pinned_generation_id": "generation"}\n'
    )
    assert payload["pinned_generation_id"] == "generation"
    with pytest.raises(VerificationError, match="found 0"):
        _mcp_proof_from_helm_test("readiness only")


def test_helm_test_collects_each_container_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[1] == "test":
            return subprocess.CompletedProcess(argv, 0, "test passed", "")
        container = argv[-1]
        output = (
            'OKG_MCP_PROOF={"node_count": 4, "edge_count": 3, '
            '"pinned_generation_id": "generation"}'
            if container == "mcp"
            else "readiness ok"
        )
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(kubernetes_verifier, "_run", fake_run)
    proof = _helm_test_mcp_proof(
        helm="helm",
        kubectl="kubectl",
        release="proof",
        namespace="okg-proof",
        env={},
    )
    assert proof["pinned_generation_id"] == "generation"


def test_cli_rejects_values_with_generic_live_kind_proof(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = kubernetes_verifier.main(
        [
            "--live-kind",
            "--values",
            str(OPENSTACK_VALUES),
            "--report",
            str(tmp_path / "report.json"),
        ]
    )
    assert result == 1
    assert "cannot be combined with --values" in capsys.readouterr().err


# External deployments (the archi-crab chart copy) had to patch these five
# things into a vendored chart; the chart now carries them as values.
_EXTERNAL_VALUES = (
    "--set-string", "images.pullSecrets[0]=registry-pull",
    "--set-string", "extraEnv[0].name=ARCHI_DATA_ROOT",
    "--set-string", "extraEnv[0].value=/var/lib/okg",
    "--set-string", "extraEnv[0].targets[0]=bootstrap",
    "--set-string", "extraEnv[0].targets[1]=worker",
    "--set-string", "extraEnv[0].targets[2]=mcp",
    "--set-string", "gitCredentials.hosts[0].host=gitlab.example.org",
    "--set-string", "gitCredentials.hosts[0].username=oauth2",
    "--set-string", "gitCredentials.hosts[0].secretKey=gitlab-token",
    "--set-string", "gitCredentials.hosts[1].host=github.com",
    "--set-string", "gitCredentials.hosts[1].username=x-access-token",
    "--set-string", "gitCredentials.hosts[1].secretKey=github-token",
)


def _runtime_containers(documents: list[dict]) -> dict[str, dict]:
    runtime = next(
        row
        for row in documents
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "runtime"
    )
    spec = runtime["spec"]["template"]["spec"]
    return {
        row["name"]: row
        for row in [*spec["initContainers"], *spec["containers"]]
    }


def _test_pod(documents: list[dict]) -> dict:
    return next(row for row in documents if row.get("kind") == "Pod")


def test_default_render_keeps_helm_test_hook_and_no_external_extras() -> None:
    helm = _helm()
    documents, _ = render_chart(
        helm=helm, deployment="kubernetes-smoke", internal_postgres=True
    )
    pod = _test_pod(documents)
    assert pod["metadata"]["annotations"] == {
        "helm.sh/hook": "test",
        "helm.sh/hook-delete-policy": "before-hook-creation",
    }
    assert "imagePullSecrets" not in pod["spec"]
    # A minAvailable=1 budget on the single runtime replica blocks node
    # drains, so the default renders none.
    assert not [row for row in documents if row.get("kind") == "PodDisruptionBudget"]
    for name, container in _runtime_containers(documents).items():
        names = {row["name"] for row in container.get("env") or []}
        assert not any(row.startswith("GIT_CONFIG") for row in names), name
        assert "ARCHI_DATA_ROOT" not in names, name


def test_every_database_url_carries_where_it_is_kept() -> None:
    """Issue #3582: okg's error for an unreachable database names the
    Secret and key the URL came from, so each container that gets OKG_DSN
    from a Secret also gets OKG_DSN_SOURCE naming them."""
    helm = _helm()
    documents, _ = render_chart(
        helm=helm, deployment="kubernetes-smoke", internal_postgres=True
    )
    containers = []
    for row in documents:
        spec = (row.get("spec") or {}).get("template", {}).get("spec") or row.get("spec") or {}
        containers.extend(
            [*(spec.get("initContainers") or []), *(spec.get("containers") or [])]
        )
    checked = 0
    for container in containers:
        env = {item["name"]: item for item in container.get("env") or []}
        ref = (env.get("OKG_DSN", {}).get("valueFrom") or {}).get("secretKeyRef")
        if not ref:
            continue
        checked += 1
        assert env["OKG_DSN_SOURCE"]["value"] == (
            f"Secret {ref['name']}, key {ref['key']}"
        ), container["name"]
    assert checked >= 3


@pytest.mark.parametrize(
    "setting",
    [
        ["--set-string", "extraEnv[0].name=OKG_DSN_SOURCE",
         "--set-string", "extraEnv[0].value=x",
         "--set-string", "extraEnv[0].targets[0]=bootstrap"],
        ["--set-string", "okg.deploymentDsnEnv=OKG_DSN_SOURCE"],
    ],
)
def test_the_chart_owns_the_database_url_source_name(setting: list[str]) -> None:
    helm = _helm()
    with pytest.raises(RuntimeError, match="OKG_DSN_SOURCE"):
        render_chart(
            helm=helm, deployment="kubernetes-smoke", internal_postgres=True,
            values=None, set_values=setting,
        )


def test_external_values_reach_the_named_containers_and_test_pod() -> None:
    helm = _helm()
    documents, rendered = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[*_EXTERNAL_VALUES, "--set", "mirror.enabled=true"],
    )
    pod = _test_pod(documents)
    assert pod["spec"]["imagePullSecrets"] == [{"name": "registry-pull"}]
    statefulsets = {
        row["metadata"]["labels"]["app.kubernetes.io/component"]: row
        for row in documents
        if row.get("kind") == "StatefulSet"
    }
    assert set(statefulsets) == {"runtime", "postgres"}
    for component, workload in statefulsets.items():
        assert workload["spec"]["template"]["spec"]["imagePullSecrets"] == [
            {"name": "registry-pull"}
        ], component
    containers = _runtime_containers(documents)
    env = {
        name: {row["name"]: row for row in container.get("env") or []}
        for name, container in containers.items()
    }
    for name in ("bootstrap", "worker", "mcp"):
        assert env[name]["ARCHI_DATA_ROOT"] == {
            "name": "ARCHI_DATA_ROOT",
            "value": "/var/lib/okg",
        }
    for name in ("mirror", "runtime-bootstrap", "mcp-role"):
        assert "ARCHI_DATA_ROOT" not in env[name], name
    # Only the containers that clone and fetch get Git credentials.
    for name in ("bootstrap", "worker"):
        assert env[name]["GIT_CONFIG_COUNT"]["value"] == "2"
        assert env[name]["GIT_TERMINAL_PROMPT"]["value"] == "0"
        assert env[name]["GIT_CONFIG_KEY_0"]["value"] == (
            "credential.https://gitlab.example.org.helper"
        )
        assert env[name]["GIT_CRED_TOKEN_1"]["valueFrom"] == {
            "secretKeyRef": {"name": "okg-runtime", "key": "github-token"}
        }
    for name in ("mcp", "mirror", "runtime-bootstrap", "mcp-role"):
        assert not any(key.startswith("GIT_C") for key in env[name]), name
    assert not any(
        row["name"].startswith("GIT_C")
        for container in pod["spec"]["containers"]
        for row in container.get("env") or []
    )
    # The token is only ever a secret reference.
    assert "kind: Secret" not in rendered


def test_git_credential_helper_answers_get_for_its_host_only(
    tmp_path: Path,
) -> None:
    git = shutil.which("git")
    if git is None:
        pytest.skip("git is unavailable")
    helm = _helm()
    documents, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=list(_EXTERNAL_VALUES),
    )
    worker = _runtime_containers(documents)["worker"]
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp_path),
        "GIT_CONFIG_NOSYSTEM": "1",
    }
    for row in worker["env"]:
        if row["name"].startswith("GIT_") and "value" in row:
            env[row["name"]] = row["value"]
    env["GIT_CRED_TOKEN_0"] = "gitlab-secret-value"
    env["GIT_CRED_TOKEN_1"] = "github-secret-value"

    def fill(host: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [git, "credential", "fill"],
            input=f"protocol=https\nhost={host}\n\n",
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

    gitlab = fill("gitlab.example.org")
    assert gitlab.returncode == 0, gitlab.stderr
    assert "username=oauth2" in gitlab.stdout.splitlines()
    assert "password=gitlab-secret-value" in gitlab.stdout.splitlines()
    github = fill("github.com")
    assert "password=github-secret-value" in github.stdout.splitlines()
    # A host with no helper gets no token, and with prompts off git fails
    # instead of waiting on a terminal.
    other = fill("other.example.org")
    assert other.returncode != 0
    assert "secret-value" not in other.stdout


_ARGO_DELETE_POLICY = (
    "tests.annotations.argocd\\.argoproj\\.io/hook-delete-policy=BeforeHookCreation"
)


def test_argo_cd_post_sync_annotations_replace_the_helm_test_hook() -> None:
    helm = _helm()
    documents, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[
            "--set", "tests.annotations.helm\\.sh/hook=null",
            "--set", "tests.annotations.helm\\.sh/hook-delete-policy=null",
            "--set-string",
            "tests.annotations.argocd\\.argoproj\\.io/hook=PostSync",
            "--set-string",
            _ARGO_DELETE_POLICY,
        ],
    )
    assert _test_pod(documents)["metadata"]["annotations"] == {
        "argocd.argoproj.io/hook": "PostSync",
        "argocd.argoproj.io/hook-delete-policy": "BeforeHookCreation",
    }
    # Dropping every hook would make the proof pod an ordinary pod that is
    # created on every install; the chart refuses that.
    with pytest.raises(RuntimeError, match="tests.annotations must keep"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=[
                "--set", "tests.annotations.helm\\.sh/hook=null",
            ],
        )


def _released_chart_copy(tmp_path: Path, released: str) -> Path:
    chart = tmp_path / "okg"
    shutil.copytree(CHART, chart)
    metadata = (chart / "Chart.yaml").read_text()
    marker = 'okg.mitdbg.io/released: "false"'
    assert metadata.count(marker) == 1
    (chart / "Chart.yaml").write_text(
        metadata.replace(marker, f'okg.mitdbg.io/released: "{released}"')
    )
    return chart


def test_source_checkout_chart_still_requires_a_runtime_tag() -> None:
    # No image is published for a source checkout's appVersion, so an empty
    # tag must fail at render time, not leave pods in ImagePullBackOff.
    metadata = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert metadata["annotations"]["okg.mitdbg.io/released"] == "false"
    helm = _helm()
    with pytest.raises(
        RuntimeError,
        match=r"images\.okg\.tag is required when digest is empty: .*"
        r"okg\.mitdbg\.io/released",
    ):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=["--set-string", "images.okg.tag="],
        )


def test_runtime_image_tag_defaults_to_the_app_version_of_a_released_chart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helm = _helm()
    app_version = yaml.safe_load((CHART / "Chart.yaml").read_text())["appVersion"]
    monkeypatch.setattr(
        kubernetes_verifier, "CHART", _released_chart_copy(tmp_path, "true")
    )
    documents, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=["--set-string", "images.okg.tag="],
    )
    images = {
        container["image"] for container in _runtime_containers(documents).values()
    }
    assert images == {f"registry.example/okg:{app_version}"}
    pinned, _ = render_chart(
        helm=helm, deployment="kubernetes-smoke", internal_postgres=True
    )
    assert {
        row["image"] for row in _runtime_containers(pinned).values()
    } == {"registry.example/okg:verification"}
    digest = "sha256:" + "a" * 64
    by_digest, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[
            "--set-string", "images.okg.tag=",
            "--set-string", f"images.okg.digest={digest}",
        ],
    )
    assert {
        row["image"] for row in _runtime_containers(by_digest).values()
    } == {f"registry.example/okg@{digest}"}
    # A tag and a digest together stay refused.
    with pytest.raises(RuntimeError, match="/images/okg"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=["--set-string", f"images.okg.digest={digest}"],
        )


def _postgres_image(documents: list[dict]) -> str:
    postgres = next(
        row
        for row in documents
        if row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component")
        == "postgres"
    )
    (container,) = postgres["spec"]["template"]["spec"]["containers"]
    return container["image"]


def test_postgres_image_tag_follows_the_runtime_image_rule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No workflow publishes a fixed Postgres tag such as "17": the dev
    # workflow pushes sha-<commit> and dev, and a release pushes its version.
    # So the default tag is empty, and only a released chart may fill it in,
    # with its appVersion, exactly as it does for the runtime image.
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["postgresql"]["image"]["tag"] == ""
    assert values["postgresql"]["image"]["digest"] == ""
    helm = _helm()
    empty = ["--set-string", "postgresql.image.tag="]
    with pytest.raises(
        RuntimeError,
        match=r"postgresql\.image\.tag is required when digest is empty: .*"
        r"okg\.mitdbg\.io/released",
    ):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=empty,
        )
    # An external database needs no Postgres image, so an empty tag is fine.
    external, rendered = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=False,
        values=None,
        set_values=["--set-string", "postgresql.image.repository=registry.example/pg", *empty],
    )
    assert not any(
        row.get("kind") == "StatefulSet"
        and row["metadata"]["labels"].get("app.kubernetes.io/component") == "postgres"
        for row in external
    )
    assert "registry.example/pg" not in rendered
    app_version =yaml.safe_load((CHART / "Chart.yaml").read_text())["appVersion"]
    monkeypatch.setattr(
        kubernetes_verifier, "CHART", _released_chart_copy(tmp_path, "true")
    )
    registry = ["--set-string", "postgresql.image.repository=registry.example/pg"]
    released, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[*registry, *empty],
    )
    assert _postgres_image(released) == f"registry.example/pg:{app_version}"
    pinned, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=registry,
    )
    assert _postgres_image(pinned) == "registry.example/pg:verification"
    digest = "sha256:" + "b" * 64
    by_digest, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[
            *registry,
            *empty,
            "--set-string", f"postgresql.image.digest={digest}",
        ],
    )
    assert _postgres_image(by_digest) == f"registry.example/pg@{digest}"
    # A tag and a digest together stay refused.
    with pytest.raises(RuntimeError, match="/postgresql/image"):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=["--set-string", f"postgresql.image.digest={digest}"],
        )


@pytest.mark.parametrize(
    ("set_values", "message"),
    [
        (
            [
                "--set-string", "extraEnv[0].name=OKG_DSN",
                "--set-string", "extraEnv[0].value=x",
                "--set-string", "extraEnv[0].targets[0]=worker",
            ],
            "extraEnv name OKG_DSN is reserved",
        ),
        (
            [
                "--set-string", "extraEnv[0].name=GIT_CONFIG_COUNT",
                "--set-string", "extraEnv[0].value=1",
                "--set-string", "extraEnv[0].targets[0]=worker",
            ],
            "extraEnv name GIT_CONFIG_COUNT is reserved",
        ),
        (
            [
                "--set-string", "secrets.extraEnv[0].name=GIT_CRED_TOKEN_0",
                "--set-string", "secrets.extraEnv[0].key=token",
                "--set-string", "secrets.extraEnv[0].targets[0]=worker",
            ],
            "secrets.extraEnv name GIT_CRED_TOKEN_0 is reserved",
        ),
        (
            [
                "--set-string", "secrets.extraEnv[0].name=GH_TOKEN",
                "--set-string", "secrets.extraEnv[0].key=token",
                "--set-string", "secrets.extraEnv[0].targets[0]=worker",
                "--set-string", "extraEnv[0].name=GH_TOKEN",
                "--set-string", "extraEnv[0].value=plain",
                "--set-string", "extraEnv[0].targets[0]=worker",
            ],
            "extraEnv name GH_TOKEN is already set for worker",
        ),
        (
            [
                "--set-string", "gitCredentials.hosts[0].host=git.example.org/x",
                "--set-string", "gitCredentials.hosts[0].username=u",
                "--set-string", "gitCredentials.hosts[0].secretKey=k",
            ],
            "/gitCredentials/hosts/0/host",
        ),
        (
            [
                "--set-string", "gitCredentials.hosts[0].host=git.example.org",
                "--set-string", "gitCredentials.hosts[0].username=u; id",
                "--set-string", "gitCredentials.hosts[0].secretKey=k",
            ],
            "/gitCredentials/hosts/0/username",
        ),
        (
            [
                "--set-string", "extraEnv[0].name=PLAIN",
                "--set-string", "extraEnv[0].value=x",
                "--set-string", "extraEnv[0].targets[0]=mcp-role",
            ],
            "/extraEnv/0/targets/0",
        ),
        (
            [
                "--set-string", "extraEnv[0].name=GIT_CONFIG_KEY_0",
                "--set-string", "extraEnv[0].value=x",
                "--set-string", "extraEnv[0].targets[0]=worker",
            ],
            "extraEnv name GIT_CONFIG_KEY_0 is reserved",
        ),
        (
            [
                "--set-string", "secrets.extraEnv[0].name=GIT_CONFIG_PARAMETERS",
                "--set-string", "secrets.extraEnv[0].key=token",
                "--set-string", "secrets.extraEnv[0].targets[0]=worker",
            ],
            "secrets.extraEnv name GIT_CONFIG_PARAMETERS is reserved",
        ),
        (
            [
                "--set-string", "gitCredentials.hosts[0].host=git.example.org",
                "--set-string", "gitCredentials.hosts[0].username=u",
                "--set-string", "gitCredentials.hosts[0].secretKey=k",
                "--set-string", "gitCredentials.hosts[1].host=git.example.org",
                "--set-string", "gitCredentials.hosts[1].username=v",
                "--set-string", "gitCredentials.hosts[1].secretKey=k2",
            ],
            "gitCredentials.hosts lists host git.example.org twice",
        ),
        (
            ["--set-string", "tests.annotations.helm\\.sh/hook="],
            'helm.sh/hook is ""',
        ),
        (
            ["--set-string", "tests.annotations.helm\\.sh/hook=test\\,bogus"],
            'helm.sh/hook is "test,bogus"',
        ),
        (
            [
                "--set", "tests.annotations.helm\\.sh/hook=null",
                "--set-string",
                "tests.annotations.argocd\\.argoproj\\.io/hook=PostSynk",
            ],
            'argocd.argoproj.io/hook is "PostSynk"',
        ),
    ],
)
def test_chart_fails_closed_on_unsafe_external_values(
    set_values: list[str],
    message: str,
) -> None:
    helm = _helm()
    with pytest.raises(RuntimeError, match=message):
        render_chart(
            helm=helm,
            deployment="kubernetes-smoke",
            internal_postgres=True,
            values=None,
            set_values=set_values,
        )


def test_hook_annotations_accept_known_types_and_comma_lists() -> None:
    helm = _helm()
    documents, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[
            "--set-string",
            "tests.annotations.helm\\.sh/hook=test\\, test-success",
            "--set-string",
            "tests.annotations.argocd\\.argoproj\\.io/hook=PostSync\\,SyncFail",
        ],
    )
    annotations = _test_pod(documents)["metadata"]["annotations"]
    assert annotations["helm.sh/hook"] == "test, test-success"
    assert annotations["argocd.argoproj.io/hook"] == "PostSync,SyncFail"


def test_extra_env_reaches_the_mirror_and_git_config_global_stays_allowed() -> None:
    helm = _helm()
    documents, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=[
            "--set", "mirror.enabled=true",
            "--set-string", "extraEnv[0].name=MIRROR_ONLY",
            "--set-string", "extraEnv[0].value=on",
            "--set-string", "extraEnv[0].targets[0]=mirror",
            "--set-string", "extraEnv[1].name=GIT_CONFIG_GLOBAL",
            "--set-string", "extraEnv[1].value=/etc/okg/gitconfig",
            "--set-string", "extraEnv[1].targets[0]=worker",
        ],
    )
    env = {
        name: {row["name"]: row for row in container.get("env") or []}
        for name, container in _runtime_containers(documents).items()
    }
    assert env["mirror"]["MIRROR_ONLY"] == {"name": "MIRROR_ONLY", "value": "on"}
    for name in ("bootstrap", "worker", "mcp"):
        assert "MIRROR_ONLY" not in env[name], name
    assert env["worker"]["GIT_CONFIG_GLOBAL"]["value"] == "/etc/okg/gitconfig"


def test_disruption_budget_is_opt_in_and_checked_when_on() -> None:
    helm = _helm()
    documents, _ = render_chart(
        helm=helm,
        deployment="kubernetes-smoke",
        internal_postgres=True,
        values=None,
        set_values=["--set", "podDisruptionBudget.enabled=true"],
    )
    budgets = [row for row in documents if row.get("kind") == "PodDisruptionBudget"]
    assert [row["spec"]["minAvailable"] for row in budgets] == [1]
    report = inspect_render(documents, expect_postgres=True, expect_mirror=False)
    assert report["disruption_budget"] is True
    budgets[0]["spec"]["minAvailable"] = 0
    with pytest.raises(VerificationError, match="one minAvailable=1 budget"):
        inspect_render(documents, expect_postgres=True, expect_mirror=False)


def _smoke_copy_outside_repository(tmp_path: Path) -> Path:
    from okg.substrate.publisher.repository_provenance import (
        deployment_is_outside_repository,
    )

    # The image bakes the deployment at /opt/okg/deployments with no .git;
    # a copy under the test's temporary directory is in the same position.
    copy = tmp_path / "deployments" / "kubernetes-smoke"
    shutil.copytree(SMOKE_DEPLOYMENT.parent, copy)
    if not deployment_is_outside_repository(copy):
        pytest.skip(f"{tmp_path} is inside a Git working tree on this host")
    return copy


def test_smoke_deployment_has_release_authority_without_git(tmp_path: Path) -> None:
    from okg.substrate.deployment_release import (
        STANDALONE_RELEASE_KIND,
        current_release_artifact,
    )
    from okg.substrate.deployments import DeploymentManifest

    copy = _smoke_copy_outside_repository(tmp_path)
    artifact = current_release_artifact(
        DeploymentManifest.from_yaml(copy / "deployment.yaml")
    )
    assert artifact.release_kind == STANDALONE_RELEASE_KIND
    assert artifact.code_revision is None
    assert artifact.schema_checksum


def test_smoke_deployment_without_standalone_has_no_release_authority(
    tmp_path: Path,
) -> None:
    from okg.substrate.deployment_release import current_release_artifact
    from okg.substrate.deployments import DeploymentManifest
    from okg.substrate.errors import SubstrateError

    # The failure the akmon image hit before the declaration: the same
    # deployment, outside Git, with only `release: channel: dev`.
    copy = _smoke_copy_outside_repository(tmp_path)
    manifest = yaml.safe_load((copy / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev"}
    (copy / "deployment.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))
    with pytest.raises(
        SubstrateError, match="deployment release authority is unavailable"
    ):
        current_release_artifact(
            DeploymentManifest.from_yaml(copy / "deployment.yaml")
        )


def test_smoke_deployment_declares_the_chart_default_database() -> None:
    # `okg mcp-serve` refuses a DSN for a database other than the declared one
    # (or its isolation clone), so a default install's MCP sidecar starts only
    # when the two names agree.
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    manifest = yaml.safe_load(SMOKE_DEPLOYMENT.read_text())
    database = values["postgresql"]["database"]
    assert manifest["postgres"]["database"] == database
    # The kind proof installs kubernetes-smoke with the same database in the
    # chart values and in both DSNs.
    assert kubernetes_verifier.KIND_PROOF_DATABASE == database
    assert kubernetes_verifier.kind_proof_database_sets() == [
        "--set-string",
        f"postgresql.database={database}",
    ]
    for user in ("postgres", "okg_mcp"):
        dsn = urlsplit(
            kubernetes_verifier.kind_proof_dsn(user, "disposable", release="proof")
        )
        assert (dsn.username, dsn.hostname, dsn.port, dsn.path) == (
            user,
            "proof-okg-postgres",
            5432,
            f"/{database}",
        )


def test_smoke_deployment_reads_only_what_its_own_directory_ships() -> None:
    # The public copy and the image built from it carry no pact/changes, so
    # the smoke source must read a fixture inside the deployment directory,
    # which both ship.
    registry = yaml.safe_load(
        (SMOKE_DEPLOYMENT.parent / "source_registry.yaml").read_text()
    )
    pact = registry["sources"]["pact"]
    root = pact["params"]["pact_root"]
    assert root == "${deployment_dir}/pactfixture"
    assert pact["sync"]["watch"]["roots"] == [root]
    changes = sorted(
        (SMOKE_DEPLOYMENT.parent / "pactfixture" / "changes").glob("*/pact.yaml")
    )
    assert len(changes) >= 2
    for path in changes:
        record = yaml.safe_load(path.read_text())
        assert record["id"] == path.parent.name
        assert record["requirements"] and record["tasks"]

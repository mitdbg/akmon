"""standardize-deployment-authority: resolved_target identity contract +
the dsn-agreement comparison core (req.deploy.single-authority /
req.deploy.agreement-doctor)."""

from __future__ import annotations

from okg.substrate.deployment_agreement import agreement_report
from okg.substrate.deployment_authority import (
    publish_provenance_verdict,
    resolved_target,
)


class TestResolvedTarget:
    def test_exact_match_is_ok(self):
        t = resolved_target(
            "postgres://u:p@db.example:5433/okg_workspace",
            deployment="okg-workspace",
            expected_database="okg_workspace",
        )
        assert t["identity"] == "ok"
        assert t["database"] == "okg_workspace"
        assert t["host"] == "db.example"
        assert t["port"] == 5433

    def test_isolation_suffix_is_flagged_not_refused(self):
        t = resolved_target(
            "postgres://u:p@h:5433/okg_workspace_51b8fac220",
            deployment="okg-workspace",
            expected_database="okg_workspace",
        )
        assert t["identity"] == "isolated"

    def test_other_database_is_mismatch(self):
        t = resolved_target(
            "postgres://u:p@h:5433/okg_kdflex",
            deployment="okg-workspace",
            expected_database="okg_workspace",
        )
        assert t["identity"] == "mismatch"

    def test_no_declaration_is_undeclared(self):
        t = resolved_target(
            "postgres://u:p@h:5433/anything",
            deployment="okg-workspace",
        )
        assert t["identity"] == "undeclared"

    def test_no_declaration_with_readable_manifest_stays_undeclared(self):
        """A readable manifest that simply omits a database key is a
        legitimate operator state: identity MUST stay `undeclared`, not the
        fail-closed `unresolved`. Guards against over-tightening the fix."""
        t = resolved_target(
            "postgres://u:p@h:5433/anything",
            deployment="okg-workspace",
            expected_database=None,
            manifest_readable=True,
        )
        assert t["identity"] == "undeclared"

    def test_unreadable_manifest_is_unresolved_not_undeclared(self):
        """A corrupt manifest yields no trustworthy declared database. The
        DSN must NOT be accepted as `undeclared` (the fail-open hole); it
        fails closed to `unresolved`, regardless of expected_database."""
        t = resolved_target(
            "postgres://u:p@h:5433/anything",
            deployment="okg-workspace",
            expected_database=None,
            manifest_readable=False,
        )
        assert t["identity"] == "unresolved"
        assert t["database"] == "anything"

    def test_unreadable_manifest_overrides_even_a_pinned_database(self):
        """manifest_readable=False fails closed even if a stale
        expected_database name was somehow threaded through."""
        t = resolved_target(
            "postgres://u:p@h:5433/okg_workspace",
            deployment="okg-workspace",
            expected_database="okg_workspace",
            manifest_readable=False,
        )
        assert t["identity"] == "unresolved"

    def test_same_name_different_host_is_mismatch(self):
        """The 2026-06-10 incident shape: the RIGHT-named database on the
        WRONG host. Name-only would call this `ok`; the (host, port, database)
        triple refuses it as `mismatch`."""
        t = resolved_target(
            "postgres://u:p@fork.example:5433/okg_workspace",
            deployment="okg-workspace",
            expected_database="okg_workspace",
            expected_host="canonical.example",
            expected_port=5433,
        )
        assert t["identity"] == "mismatch"

    def test_same_name_different_port_is_mismatch(self):
        """The fork server :5433 vs canonical :55434 - same name, same host,
        different port is still the wrong server."""
        t = resolved_target(
            "postgres://u:p@db.example:5433/okg_workspace",
            deployment="okg-workspace",
            expected_database="okg_workspace",
            expected_host="db.example",
            expected_port=55434,
        )
        assert t["identity"] == "mismatch"

    def test_localhost_aliases_are_not_a_false_mismatch(self):
        """localhost / 127.0.0.1 / ::1 / unix-socket(None host) all denote the
        same local server: a benign loopback alias must stay `ok`, never a
        false `mismatch`."""
        # IPv6 literals must be bracketed in a URL; urlsplit yields "::1".
        for host in ("localhost", "127.0.0.1", "[::1]"):
            t = resolved_target(
                f"postgres://u:p@{host}:55434/okg_workspace",
                deployment="okg-workspace",
                expected_database="okg_workspace",
                expected_host="127.0.0.1",
                expected_port=55434,
            )
            assert t["identity"] == "ok", host
        # A unix-socket DSN parses to a None host; it must fold to localhost
        # and match a loopback expectation, not mis-compare to `mismatch`.
        socket_target = resolved_target(
            "postgres:///okg_workspace?host=/var/run/postgresql",
            deployment="okg-workspace",
            expected_database="okg_workspace",
            expected_host="localhost",
            expected_port=None,
        )
        assert socket_target["identity"] == "ok"

    def test_triple_match_is_ok(self):
        """Same name, same canonicalized host, same port -> `ok`."""
        t = resolved_target(
            "postgres://u:p@db.example:55434/okg_workspace",
            deployment="okg-workspace",
            expected_database="okg_workspace",
            expected_host="db.example",
            expected_port=55434,
        )
        assert t["identity"] == "ok"

    def test_no_expected_host_port_stays_name_only(self):
        """Unchanged behavior: with no expected_host/port (the interpolated-DSN
        deployments that declare only a name), identity is name-only. A
        different host on a matching name is still `ok`."""
        t = resolved_target(
            "postgres://u:p@some.other.example:5433/okg_workspace",
            deployment="okg-workspace",
            expected_database="okg_workspace",
        )
        assert t["identity"] == "ok"

    def test_no_dsn_is_unresolved(self):
        t = resolved_target(None, deployment="okg-workspace")
        assert t["identity"] == "unresolved"
        assert t["database"] is None

    def test_target_never_carries_credentials(self):
        t = resolved_target(
            "postgres://user:secretpw@h:5433/db",
            deployment="d",
        )
        assert "secretpw" not in str(t)


def _surface(name: str, db: str | None, port: int | None = 5433):
    return {
        "surface": name,
        "database": db,
        "host": "127.0.0.1" if db else None,
        "port": port if db else None,
        "identity": "undeclared" if db else "unresolved",
    }


class TestAgreementReport:
    def test_agreement_when_all_resolved_surfaces_match(self):
        report = agreement_report(
            "okg-workspace",
            [
                _surface("deployment_authority", "okg_workspace"),
                _surface("local_env_file", "okg_workspace"),
                _surface("local_env_missing", None),  # unresolved ignored
            ],
        )
        assert report["ok"] is True
        assert report["distinct_databases"] == 1

    def test_divergence_reproduces_the_incident(self):
        """The 2026-06-10 shape: local.env on a clone, MCP elsewhere."""
        report = agreement_report(
            "okg-workspace",
            [
                _surface("deployment_authority", "okg_workspace_51b8fac220"),
                _surface(
                    "running_mcp_server",
                    "okg_workspace_agent_ready_clean3_16aa",
                    port=55434,
                ),
            ],
        )
        assert report["ok"] is False
        assert report["distinct_databases"] == 2
        assert "okg_workspace_51b8fac220" in report["reason"]
        assert "55434" in report["reason"]
        assert report["repair"]
        assert len(report["divergent"]) == 2

    def test_same_database_name_different_host_is_divergent(self):
        """The doctor tuples on (host, port, database): two surfaces that
        agree on the database NAME but sit on different hosts are still two
        different servers and MUST be reported divergent."""
        report = agreement_report(
            "okg-workspace",
            [
                {
                    "surface": "deployment_authority",
                    "database": "okg_workspace",
                    "host": "canonical.example",
                    "port": 55434,
                    "identity": "ok",
                },
                {
                    "surface": "running_mcp_server",
                    "database": "okg_workspace",
                    "host": "fork.example",
                    "port": 55434,
                    "identity": "ok",
                },
            ],
        )
        assert report["ok"] is False
        assert report["distinct_databases"] == 2
        assert "canonical.example" in report["reason"]
        assert "fork.example" in report["reason"]
        assert len(report["divergent"]) == 2

    def test_nothing_resolved_is_a_failure_not_a_pass(self):
        report = agreement_report("x", [_surface("a", None)])
        assert report["ok"] is False
        assert "no surface resolves" in report["reason"]


def _ok_target() -> dict:
    return {"identity": "ok", "database": "okg_workspace",
            "host": "127.0.0.1", "port": 55434, "deployment": "okg-workspace"}


class TestProvenanceVerdictManifestReadable:
    def test_unreadable_manifest_refuses_canonical_publish(self):
        """A corrupt manifest yields no trustworthy sync_root. The verdict
        must refuse (allowed False, mode manifest_unreadable) and must NOT
        fall through to the permissive sync_root_undeclared allow."""
        verdict = publish_provenance_verdict(
            sync_root=None,
            checkout="/anywhere",
            target=_ok_target(),
            manifest_readable=False,
        )
        assert verdict["allowed"] is False
        assert verdict["mode"] == "manifest_unreadable"
        assert "could not be parsed" in verdict["reason"]

    def test_unreadable_manifest_check_precedes_sync_root_branch(self):
        """Even if a sync_root somehow survived, an unreadable manifest still
        fails closed: the manifest_unreadable check runs first."""
        verdict = publish_provenance_verdict(
            sync_root="/repo/main",
            checkout="/repo/main",
            target=_ok_target(),
            manifest_readable=False,
        )
        assert verdict["allowed"] is False
        assert verdict["mode"] == "manifest_unreadable"

    def test_isolated_clone_still_legal_even_if_manifest_unreadable(self):
        """Isolated clones are always legal - the isolation check precedes
        the manifest-readable gate, so a parse failure does not block them."""
        target = {**_ok_target(), "identity": "isolated"}
        verdict = publish_provenance_verdict(
            sync_root=None,
            checkout="/anywhere",
            target=target,
            manifest_readable=False,
        )
        assert verdict == {"allowed": True, "mode": "isolated_clone"}

    def test_readable_manifest_with_no_sync_root_keeps_warned_allow(self):
        """Unchanged behavior: a genuinely-readable manifest that declares no
        sync_root still allows with a loud warning, NOT manifest_unreadable."""
        verdict = publish_provenance_verdict(
            sync_root=None,
            checkout="/anywhere",
            target=_ok_target(),
            manifest_readable=True,
        )
        assert verdict["allowed"] is True
        assert verdict["mode"] == "sync_root_undeclared"

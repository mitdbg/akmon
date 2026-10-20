from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.cli.main import _build_parser
from okg.substrate.exchange import generate_ed25519_keypair
from okg.substrate.exchange.readiness import public_key_fingerprint


def test_federation_argparse_routes() -> None:
    args = _build_parser().parse_args(
        [
            "federation",
            "route",
            "check",
            "--local-authority",
            "okg:personal:riley",
            "--discovery",
            "/tmp/discovery.json",
            "--key-fingerprint",
            "sha256:abc",
            "--action",
            "federation.graph.pull",
            "--target",
            "okg://mit-shared-okg-fixture/pull/public-context",
            "--subject",
            "okg:personal:riley",
            "--nomos-decision-id",
            "nomos:decision:allow",
        ]
    )

    assert args.command == "federation"
    assert args.federation_subcmd == "route"
    assert args.route_subcmd == "check"
    assert args.action == "federation.graph.pull"


def test_federation_identity_and_pull_surface_argparse_routes() -> None:
    identity = _build_parser().parse_args(
        [
            "federation",
            "identity",
            "show",
            "--authority",
            "okg:personal:riley",
        ]
    )
    assert identity.command == "federation"
    assert identity.federation_subcmd == "identity"
    assert identity.identity_subcmd == "show"

    pull_surface = _build_parser().parse_args(
        [
            "federation",
            "pull-surface",
            "list",
            "--discovery",
            "/tmp/discovery.json",
        ]
    )
    assert pull_surface.command == "federation"
    assert pull_surface.federation_subcmd == "pull-surface"
    assert pull_surface.pull_surface_subcmd == "list"


def test_federation_discover_cli_reports_pending_candidate(
    tmp_path: Path,
    capsys,
) -> None:
    keypair = generate_ed25519_keypair("mit-sign")
    discovery = {
        "authority_id": "okg:mit-shared-okg-fixture",
        "keys": [
            {
                "kid": keypair.kid,
                "public_key": keypair.public_key,
                "fingerprint": public_key_fingerprint(keypair.public_key),
            }
        ],
        "boards": [
            {
                "address": "okg://mit-shared-okg-fixture/boards/proposals",
                "visibility": "public",
                "message_profiles": ["proposal"],
            }
        ],
        "pull_surfaces": [
            {
                "address": "okg://mit-shared-okg-fixture/pull/public-context",
                "visibility": "public",
                "graph_profile": "context",
            }
        ],
        "mailboxes": [
            {
                "address": "okg://mit-shared-okg-fixture/mailboxes/inbox",
                "queue_ref": "https://mailbox.example/mit-shared/queue",
                "max_inline_bytes": 100_000,
                "max_object_bytes": 1_000_000_000,
            }
        ],
        "visibility_tiers": ["public"],
    }
    path = tmp_path / "discovery.json"
    path.write_text(json.dumps(discovery), encoding="utf-8")

    from okg.substrate.cli.federation import cmd_federation

    args = _build_parser().parse_args(["federation", "--json", "discover", str(path)])
    assert cmd_federation(args, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["authority_id"] == "okg:mit-shared-okg-fixture"
    assert payload["trust_granted"] is False
    assert payload["secret_free"] is True
    assert payload["pull_surfaces"][0]["address"].endswith("/pull/public-context")
    assert payload["mailboxes"][0]["address"].endswith("/mailboxes/inbox")


def test_federation_mailbox_list_cli_reports_no_inbound_requirement(
    tmp_path: Path,
    capsys,
) -> None:
    keypair = generate_ed25519_keypair("personal-sign")
    discovery = {
        "authority_id": "okg:personal:riley",
        "keys": [
            {
                "kid": keypair.kid,
                "public_key": keypair.public_key,
                "fingerprint": public_key_fingerprint(keypair.public_key),
            }
        ],
        "mailboxes": [
            {
                "address": "okg://personal:riley/mailboxes/inbox",
                "provider": "generic_queue_object",
                "queue_ref": "https://mailbox.example/personal-riley/queue",
                "object_ref": "s3://okg-mailbox/personal-riley/objects/",
                "ack_mode": "after_durable_receipt",
            }
        ],
        "visibility_tiers": ["public"],
    }
    path = tmp_path / "personal-discovery.json"
    path.write_text(json.dumps(discovery), encoding="utf-8")

    from okg.substrate.cli.federation import cmd_federation

    args = _build_parser().parse_args(
        ["federation", "--json", "mailbox", "list", "--discovery", str(path)]
    )
    assert cmd_federation(args, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["authority_id"] == "okg:personal:riley"
    assert payload["inbound_exposure_required"] is False
    assert payload["mailboxes"][0]["ack_mode"] == "after_durable_receipt"

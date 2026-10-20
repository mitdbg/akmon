"""Stand up, supervise, and tear down a deployment's Open WebUI instance
(PACT ``chat-instance-lifecycle``).

``okg chat-instance up --deployment D`` is the missing PRODUCER of the unit
every other child of ``openwebui-chat-frontend`` consumes. ``okg chat sync``
renders configuration INTO a running instance; nothing created one. This
module creates it: the container at the pinned version, the per-deployment
Postgres app database it points at, the manifest-owned port it binds, the
supervisor that restarts it, and the operator-state record that lets a second
deployment discover the first one's port and app database.

What this module does NOT do
----------------------------
It renders NO chat configuration through the API. Theme, features, models,
preset, prompts, MCP registration and group ACL are ``chat-sync-projection``'s,
and nothing here asserts on a ``chat.ui`` or preset value. The two touch in
two places: the site name, which Open WebUI reads only from ``WEBUI_NAME`` at
start, is passed from ``chat.ui.site_name`` at launch (issue #2875; sync
still reads it back), and this module refuses to emit ``CUSTOM_NAME`` (see F3
below), because that env var would take ``WEBUI_NAME`` out of the manifest's
hands.

Operator state versus deployment identity
-----------------------------------------
design.md:262 splits the two: the deployment repository owns identity (the
``chat:`` block), while ``~/.local/share/okg/chat-instances/<name>/`` owns
runtime wiring (DSN, ports, credentials, instance location). The state path is
deliberately outside ``~/.local/share/okg/deployments/`` because that directory
is a deployment lookup root. Two consequences bind this module:

* the app database DSN and the instance secret key arrive as ENV VAR NAMES on
  the command line and are read out of the environment. A literal never
  appears in an argv, in a manifest, in a state file, in a report, or in a log
  line. ``docker``'s ``-e VAR`` (name only, inherit-from-parent) form is what
  keeps the DSN out of the container-runtime argv as well;
* the ``chat:`` schema is asserted INCAPABLE of carrying the DSN. The test
  reads ``chat_block.schema_field_census()`` rather than trusting this
  sentence.

Measured facts this module is built on
--------------------------------------
Every one re-measured against the running ``open-webui`` container
(``ghcr.io/open-webui/open-webui:v0.11.0``) on 2026-08-10 with read-only
``docker exec … sed``. The image this program now pins is built FROM that
exact upstream build and changes only MCP result selection, so every fact
below still describes the running container.

**F1 — the vendor default store is SQLite at**
``/app/backend/data/webui.db``. ``env.py:222`` resolves
``DATA_DIR = Path(os.getenv('DATA_DIR', BACKEND_DIR / 'data')).resolve()`` and
``env.py:267`` sets ``DATABASE_URL = os.getenv('DATABASE_URL',
f'sqlite:///{DATA_DIR}/webui.db')``. That default is the silent fallback
:data:`VENDOR_DEFAULT_STORE` exists to name: an instance whose ``DATABASE_URL``
we failed to supply comes up FINE, serving a private SQLite file, and every
readback the program does afterwards succeeds against the wrong store. This
module therefore never starts a container it has not first proved a reachable
Postgres app database for, and :func:`vendor_default_store_present` measures
the absence rather than assuming it.

**F2 — ``DATABASE_URL`` is the only supported handle we set.** ``env.py:269``
onward also assembles a DSN from ``DATABASE_TYPE/USER/PASSWORD/HOST/PORT/NAME``
when ALL of them are set. Splitting a credential across six env vars is more
surface for no gain, so this module sets exactly one variable and asserts the
six-part form is absent from the spec.

**F3 — a non-empty ``CUSTOM_NAME`` reaches the network at import and
REASSIGNS ``WEBUI_NAME``.** ``config.py:184`` reads it; ``config.py:186-211``
then GETs ``api.openwebui.com/api/v1/custom/<name>``, overwrites
``static/favicon.png`` and ``static/splash.png``, and sets
``WEBUI_NAME = data['name']``. Site name is ``chat-sync-projection``'s
governed knob, so a spec carrying ``CUSTOM_NAME`` would make it ungoverned by
a vendor round trip. :func:`build_instance_spec` refuses one.

**F4 — ``GET /api/version`` is the liveness signal.** Measured live:
``{"version":"0.11.0","deployment_id":""}``. A container that reports
``created``, a process visible in the table, and a zero exit from
``docker run`` are all satisfied by an instance that never binds. Only the
instance's own version response closes :func:`stand_up`, and
:func:`instance_status` re-derives state from the same response every time
rather than reading the launch record back.

**F5 — ``env.py:892-893`` appends ``' (Open WebUI)'`` to ``WEBUI_NAME``**
whenever it differs from the default. Recorded here only so nobody adds a
site-name assertion to this module; the normalization lives in
``chat.sync.expected_site_name`` and stays there.

**Re-read at v0.11.4** (upstream revision ``8bd8b4fa``, 2026-09-29) for the
move to the v0.11.4-based image. F1-F5 hold unchanged; only the line numbers
moved: ``DATA_DIR`` is ``env.py:228``, the SQLite default ``env.py:271``, the
six-part form starts at ``env.py:273``, ``CUSTOM_NAME`` is
``config.py:193-211``, and the site-name suffix ``env.py:951-953``. The
app database gains migrations ``1ce6ade7d93b``, ``6d09d1bf1f23`` and
``d4c1a8e37b62`` (a nullable ``chat.timer_at`` column and indexes); the
first start of the new image applies them to an existing app database.

Refusals, and why none of them defaults
---------------------------------------
Six inputs can be missing or wrong, and each refuses by name: no chat block,
a disabled chat block, an undeclared instance port, an unset app-database env
var, an app database that resolves to the deployment's graph database or to
another deployment's app database, and an occupied port. A port is never
incremented and never replaced by an ephemeral one; an unreachable DSN never
degrades to the vendor default; an instance labelled for another deployment is
never adopted.

"I could not find out" is never "there is nothing there"
--------------------------------------------------------
Three inputs to this module arrive as an absence that could mean either, and
each one, read as "nothing there", disables a check silently:

* an unresolvable GRAPH DSN. ``deployments/__init__.py:977-981`` returns
  ``None`` for the ``dsn: ${OKG_DSN}`` shape this repo uses whenever the
  variable is unexported, so this is the ORDINARY value, not a rare one.
  Skipping the isolation comparison on it accepts an app DSN that IS the graph
  DSN, and Open WebUI then migrates its own tables into the deployment's graph
  database. :func:`resolve_app_database` refuses;
* an UNREADABLE operator-state file. Read as "this deployment has no
  instance", it takes that deployment's app database out of the collision set
  :func:`peer_instances` builds, and a shared app database is accepted.
  :func:`read_instance` refuses;
* an UNREACHABLE container runtime. ``docker inspect`` exits non-zero both for
  "no such container" and for "the daemon is not running", and read as the
  former, :func:`tear_down` forgets the record of a container that is still
  running. :class:`DockerRuntime` matches the absence markers POSITIVELY and
  refuses on everything else.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import unquote, urlsplit

from okg.substrate.chat.container_network import (
    HOST_NETWORK_REACH,
    LISTEN_HOST,
    LISTEN_HOST_VAR,
    LISTEN_PORT_VAR,
    LOOPBACK_MODES,
    NETWORK_GATEWAY,
    NETWORK_HOST,
    NETWORK_PASTA,
    loopback_ports,
    network_fingerprint_part,
    pasta_network_arg,
    require_forwardable,
    resolve_network_mode,
)
from okg.substrate.errors import SubstrateError

__all__ = [
    "CONTAINER_FIXED_ENV",
    "CONTAINER_HTTP_PORT",
    "DEFAULT_APP_DATABASE_ENV",
    "DEFAULT_SECRET_KEY_ENV",
    "IMAGE_KEY",
    "INSTANCE_PORT_KEY",
    "LABEL_DEPLOYMENT",
    "PINNED_IMAGE_REF",
    "UPSTREAM_IMAGE_REF",
    "PINNED_VERSION",
    "TESTED_IMAGES",
    "STATE_ABSENT",
    "STATE_DOWN",
    "STATE_UP",
    "STATUS_VOCABULARY",
    "VENDOR_DEFAULT_STORE",
    "AppDatabase",
    "ChatImageNotTested",
    "ChatInstanceError",
    "ContainerRuntime",
    "DockerRuntime",
    "ImagePlan",
    "InstanceRecord",
    "InstanceSpec",
    "InstanceStatus",
    "PgTarget",
    "PortPlan",
    "StandUpReport",
    "SupervisionEvent",
    "build_instance_spec",
    "container_name_for",
    "forget_instance",
    "instance_status",
    "manifest_admits_instance_port",
    "normalize_dsn",
    "peer_instances",
    "port_occupancy",
    "probe_version",
    "read_instance",
    "record_instance",
    "resolve_app_database",
    "resolve_instance_image",
    "resolve_instance_port",
    "tested_image_version",
    "stand_up",
    "supervise_once",
    "tear_down",
    "vendor_default_store_present",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ChatInstanceError(SubstrateError):
    """The instance lifecycle refused, or an observation disagreed.

    One code, because the operator action is always the same shape: read
    ``data['reason']`` and the named input out of ``data``, supply or correct
    it, run the command again. A partially provisioned instance is reported
    here as a failure with the teardown recorded, never as a degraded success.
    """

    code = "chat_instance_failed"


class ChatImageNotTested(ChatInstanceError):
    """The named image is not one this program has been measured against.

    Its own code, not a ``reason`` under the general one, because it is the
    single refusal a manifest author and an upgrade both meet, and the
    operator action -- pick one of the tested images, or run the measurements
    and add one -- is unlike every other lifecycle refusal.
    """

    code = "chat_image_not_tested"


# ---------------------------------------------------------------------------
# Pinned vendor constants, each traceable to a measurement (see module docs)
# ---------------------------------------------------------------------------

#: F4. The version this program is proved against.
PINNED_VERSION = "0.11.4"

#: The image the pinned version ships as HERE.
#:
#: Not the upstream build. Open WebUI v0.11.4's native MCP client prefers a
#: result's ``content`` and drops ``structuredContent``. When okg answers were
#: structured-only (#2010) they arrived empty through it, so this repository
#: builds a derived image that changes only that selection
#: (``ops/openwebui/README.md``, #2048). Since issue #3059 every okg result is
#: one text block in ``content`` with no structured copy, which both images
#: select; the patched image stays the one measured here. Adoption means
#: pinning its DIGEST: a tag would let the bytes move under a pin whose whole
#: purpose is that they cannot.
PINNED_IMAGE_REF = (
    "your-registry.invalid/okg-openwebui-compat@"
    "sha256:2561170716571aaf7e41417decd7e23b43c390a00395c957b362a17e80557c11"
)

#: The upstream image the patched one is built FROM, named so a refusal can
#: say what an operator reaching for it should use instead. It is NOT tested:
#: it answers 0.11.4 at ``/api/version`` and would still lose every
#: structured result.
UPSTREAM_IMAGE_REF = "ghcr.io/open-webui/open-webui:v0.11.4"

#: Every image this program has been MEASURED against, mapped to the version
#: that image answers at ``GET /api/version``.
#:
#: One entry today, and that is the honest size of the set: every fact in this
#: module's header was re-read at v0.11.4 and the image run live. The
#: v0.11.0-based image left the set with this change: the knob census and
#: the chat-sessions schema pin now describe v0.11.4, and an instance on
#: the old image would be compared against measurements it does not match.
#: This is an allow-list
#: rather than a version comparison because "newer than the pin" is not a
#: statement anyone measured -- an image is admitted by having been run, not by
#: sorting above something that was. Adding an entry is the deliberate act of
#: recording that the measurements were re-taken.
#:
#: :data:`PINNED_IMAGE_REF` is this mapping's DEFAULT, not a second source of
#: truth: :func:`resolve_instance_image` falls back to it when neither the
#: manifest nor the operator names an image, and the version any run is checked
#: against is looked up here from the image actually used.
TESTED_IMAGES: Mapping[str, str] = MappingProxyType({
    PINNED_IMAGE_REF: PINNED_VERSION,
})

#: The port Open WebUI listens on INSIDE its container. The host-side port is
#: manifest-owned and published onto this one.
CONTAINER_HTTP_PORT = 8080

#: F4. The liveness path. Nothing else counts as "running".
VERSION_PATH = "/api/version"

#: F1. ``env.py:222`` + ``env.py:267``. The store an instance silently uses
#: when ``DATABASE_URL`` is unset — the fallback the app-database requirement
#: exists to exclude, measured rather than assumed.
VENDOR_DEFAULT_STORE = "/app/backend/data/webui.db"

#: F2. The one env var we set to point the instance at its app database.
CONTAINER_APP_DATABASE_VAR = "DATABASE_URL"

#: F2. The six-part alternative. Asserted ABSENT from every spec: one variable
#: is one place a credential can leak from, six is six.
CONTAINER_SPLIT_DATABASE_VARS = (
    "DATABASE_TYPE",
    "DATABASE_USER",
    "DATABASE_PASSWORD",
    "DATABASE_HOST",
    "DATABASE_PORT",
    "DATABASE_NAME",
)

#: F3. ``config.py:184-211``. Refused in every spec.
CONTAINER_FORBIDDEN_VARS = ("CUSTOM_NAME",)

#: The instance's own session-signing key. Operator state; supplied as a
#: variable NAME and read from the environment, never as a literal.
CONTAINER_SECRET_KEY_VAR = "WEBUI_SECRET_KEY"

#: The site name. Open WebUI reads it ONLY from this variable at start
#: (issue #2875), so the container is launched with ``chat.ui.site_name``
#: here; a changed name is a settings change, like a rotated secret.
CONTAINER_SITE_NAME_VAR = "WEBUI_NAME"

#: Vendor features the site switches off with an environment variable,
#: because v0.11.4 reads them ONLY at start (Open WebUI cut list, 2026-09-29):
#:
#: * ``ENABLE_VERSION_UPDATE_CHECK`` -- the admin "new version available"
#:   toast. ``env.py:1202``; served as ``features.enable_version_update_check``
#:   (``main.py:2347``) and consulted by ``src/routes/(app)/+layout.svelte:370``.
#: * ``ENABLE_EASTER_EGGS`` -- at v0.11.4 it adds the "Her" theme and the
#:   Doge language to Settings > General (``env.py:968``;
#:   ``src/lib/components/chat/Settings/General.svelte:136``, ``:241``).
#:
#: Both are part of every spec and therefore of the settings fingerprint: for
#: a container started without them ``up`` refuses
#: ``instance_settings_changed``, and it needs ``chat-instance down`` then
#: ``up``. ``chat sync`` reads both back from ``GET /api/config``.
CONTAINER_FIXED_ENV: Mapping[str, str] = MappingProxyType({
    "ENABLE_VERSION_UPDATE_CHECK": "false",
    "ENABLE_EASTER_EGGS": "false",
})

#: How long ``tear_down`` waits for the instance port to be released after
#: the container is gone (issue #2876 item 3): rootless podman's network
#: helper can hold it for seconds after removal.
DEFAULT_PORT_RELEASE_TIMEOUT = 60.0

#: Container label carrying the owning deployment. What makes
#: "this instance serves a DIFFERENT deployment" an observation rather than a
#: guess from a name collision.
LABEL_DEPLOYMENT = "okg.deployment"
LABEL_MANAGED_BY = "okg.managed-by"
MANAGED_BY = "okg-chat-instance-lifecycle"

#: Container label carrying a fingerprint of the settings the container was
#: started with (every environment variable name and value, hashed). A running
#: container never re-reads them, so without this a changed ``chat.oidc_login``
#: or a rotated secret would be silently ignored by an "already running"
#: answer. Only the hash is recorded, never a value.
LABEL_SETTINGS = "okg.settings-fingerprint"


# ---------------------------------------------------------------------------
# Operator-environment inputs. Named, and refused by name when unset.
# ---------------------------------------------------------------------------

#: DEFAULT NAME of the environment variable holding the app database DSN. The
#: value is never a CLI argument: ``--app-database-url-env`` names the
#: variable, matching ``chat.mcp.auth_token_env`` in the manifest schema.
DEFAULT_APP_DATABASE_ENV = "OKG_CHAT_APP_DATABASE_URL"

#: DEFAULT NAME of the environment variable holding the instance secret key.
DEFAULT_SECRET_KEY_ENV = "OKG_CHAT_WEBUI_SECRET_KEY"


# ---------------------------------------------------------------------------
# Status vocabulary
# ---------------------------------------------------------------------------

#: The three states an instance can be observed in. ``up`` and ``down`` are
#: the words design.md section 8 risk 3 and this PACT's scenarios use;
#: :data:`STATUS_VOCABULARY` maps them onto the deployment status words the
#: rest of the substrate already emits (``environment_services.py:1818`` /
#: ``:1825``, ``deployment_upgrade.py:228``), so one instance does not invent
#: a fourth vocabulary.
STATE_UP = "up"
STATE_DOWN = "down"
STATE_ABSENT = "absent"

STATUS_VOCABULARY: Mapping[str, str] = {
    STATE_UP: "healthy",
    STATE_DOWN: "stopped",
    STATE_ABSENT: "missing",
}


# ---------------------------------------------------------------------------
# The manifest's instance-port key
# ---------------------------------------------------------------------------

#: The key ``chat:`` must admit before a port can be manifest-declared.
#: ``chat_block.py`` admits it as of this slice; :func:`manifest_admits_
#: instance_port` still READS that file's own census rather than asserting the
#: answer, so a schema that ever dropped the key would make this module refuse
#: by name instead of resolving against a key nothing accepts.
INSTANCE_PORT_KEY = "instance_port"

#: The sibling key naming the vendor image. Same treatment as the port: the
#: manifest may declare it, the operator flag may name one, and neither being
#: present is a defined state -- for the image, the tested default.
IMAGE_KEY = "image"

#: Where a port came from. Recorded on every report so "the operator supplied
#: it on the command line" can never read as "the manifest declares it".
PORT_SOURCE_MANIFEST = "manifest"
PORT_SOURCE_OPERATOR_FLAG = "operator_flag"
#: Both inputs present and EQUAL. Recorded distinctly so a reader can tell a
#: run that was handed one input from a run that was handed two agreeing ones;
#: two that disagree refuse rather than picking a winner.
PORT_SOURCE_MANIFEST_AGREED = "manifest_and_flag_agree"


def manifest_admits_instance_port() -> bool:
    """True when ``chat-manifest-block`` admits the instance-port key.

    Read out of the sibling's own :func:`schema_field_census` rather than
    restated: a copy of the key set here would go stale exactly when the
    sibling lands, which is the one moment this answer changes.
    """
    from okg.substrate.deployments.chat_block import schema_field_census

    return INSTANCE_PORT_KEY in schema_field_census().get("chat", frozenset())


# ---------------------------------------------------------------------------
# DSN normalization and app-database isolation
# ---------------------------------------------------------------------------

#: Host spellings that all mean "this machine's loopback". Normalized to one
#: form so a graph DSN on ``localhost`` and an app DSN on ``127.0.0.1`` are
#: recognised as the SAME host rather than two.
_LOOPBACK_FORMS = frozenset({
    "localhost",
    "localhost.localdomain",
    "127.0.0.1",
    "::1",
    "[::1]",
    "",
})
LOOPBACK_CANONICAL = "127.0.0.1"

_POSTGRES_SCHEMES = frozenset({"postgres", "postgresql", "postgresql+psycopg"})
DEFAULT_POSTGRES_PORT = 5432


@dataclass(frozen=True)
class PgTarget:
    """The connection parameters a DSN RESOLVES to.

    Structural identity, per the requirement: a DSN that differs only in
    spelling, credentials, query parameters, or the loopback host form still
    resolves to the same target and compares equal here. String inequality of
    two DSNs proves nothing and is never used.
    """

    host: str
    port: int
    database: str

    def as_dict(self) -> dict[str, Any]:
        return {"host": self.host, "port": self.port, "database": self.database}

    def describe(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"


def normalize_dsn(dsn: str, *, label: str) -> PgTarget:
    """Resolve a Postgres DSN to its connection target.

    Credentials, query parameters, scheme spelling and loopback host form are
    all discarded — they are the four ways two DSNs can differ while pointing
    at one database, and the isolation check must see through every one.
    """
    raw = (dsn or "").strip()
    if not raw:
        raise ChatInstanceError(
            f"the {label} DSN is empty",
            data={"reason": "dsn_empty", "label": label},
            fix_hint=f"set the {label} DSN in the operator environment",
        )
    split = urlsplit(raw)
    scheme = (split.scheme or "").lower()
    if scheme not in _POSTGRES_SCHEMES:
        raise ChatInstanceError(
            f"the {label} DSN is not a Postgres DSN (scheme {scheme or 'none'!r})",
            data={"reason": "dsn_not_postgres", "label": label, "scheme": scheme},
            fix_hint=(
                "Open WebUI's app database must be Postgres, one per "
                "deployment (design.md:235-237). A sqlite:// URL is the "
                "vendor default store this program refuses"
            ),
        )
    host = (split.hostname or "").strip().lower()
    if host in _LOOPBACK_FORMS:
        host = LOOPBACK_CANONICAL
    try:
        port = int(split.port) if split.port else DEFAULT_POSTGRES_PORT
    except (TypeError, ValueError) as exc:  # pragma: no cover - urlsplit raises first
        raise ChatInstanceError(
            f"the {label} DSN carries an unparseable port",
            data={"reason": "dsn_bad_port", "label": label},
        ) from exc
    database = unquote((split.path or "").lstrip("/")).strip()
    if not database:
        raise ChatInstanceError(
            f"the {label} DSN names no database",
            data={"reason": "dsn_no_database", "label": label, "host": host},
            fix_hint="append /<database> to the DSN",
        )
    return PgTarget(host=host, port=port, database=database)


@dataclass(frozen=True)
class AppDatabase:
    """The resolved app database, and the variable it came out of.

    ``dsn`` is the ONLY secret-bearing field in this module. It is excluded
    from :meth:`as_dict` and from :meth:`__repr__`, so a report, a state file,
    a JSON envelope, or a traceback frame renders the variable name and the
    resolved target and never the credential.

    ``graph_target`` is the graph database this one was proved distinct from,
    and it is REQUIRED. Carrying it makes the isolation check's having run a
    positive fact in every report rather than an inference from the absence of
    a refusal: a reader can see the two targets and check the comparison
    themselves. A ``PgTarget`` holds host, port and database name only, so
    naming it here leaks no credential.
    """

    env_var: str
    target: PgTarget
    dsn: str = field(repr=False, compare=False)
    graph_target: PgTarget = field(compare=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "env_var": self.env_var,
            **self.target.as_dict(),
            "proved_distinct_from_graph_database": self.graph_target.as_dict(),
        }

    def __repr__(self) -> str:  # pragma: no cover - defensive, exercised by tests
        return f"AppDatabase(env_var={self.env_var!r}, target={self.target!r})"


def resolve_app_database(
    deployment: str,
    *,
    env_var: str,
    environ: Mapping[str, str],
    graph_dsn: str | None,
    peers: Sequence["InstanceRecord"] = (),
) -> AppDatabase:
    """Resolve, and prove isolated, the deployment's app database.

    Five refusals, in the order a wrong input is cheapest to diagnose:

    1. the variable is unset — named, never defaulted;
    2. the DSN does not parse as Postgres, or names no database;
    3. the deployment's GRAPH DSN is unresolvable, so the comparison in 4
       cannot be made. This refuses; it does NOT skip 4. ``None`` is the
       ORDINARY value here — ``deployments/__init__.py:977-981`` returns it for
       the ``dsn: ${OKG_DSN}`` shape this repo uses whenever the variable is
       unexported — so a skipped comparison would be the common case rather
       than the exotic one, and the app DSN pointing at the graph database
       would be ACCEPTED;
    4. it resolves to the deployment's GRAPH database. Checked on the
       normalized target, so a credentials, query-parameter or
       ``localhost``-versus-``127.0.0.1`` variant is caught;
    5. it resolves to an app database another deployment already recorded.

    Reachability is deliberately NOT checked here: it needs a network round
    trip, and :func:`stand_up` sequences it after the cheap structural
    refusals so an operator with two wrong inputs hears about the structural
    one first.
    """
    raw = (environ.get(env_var) or "").strip()
    if not raw:
        raise ChatInstanceError(
            f"the app database DSN is not set: environment variable "
            f"{env_var} is empty or absent",
            data={
                "reason": "app_database_env_unset",
                "deployment": deployment,
                "env_var": env_var,
            },
            fix_hint=(
                f"export {env_var}=postgresql://…/<a database for this "
                f"deployment only>, or name a different variable with "
                f"--app-database-url-env. The DSN is operator state: it is "
                f"never read out of the manifest"
            ),
        )
    target = normalize_dsn(raw, label="app database")

    if not (graph_dsn or "").strip():
        raise ChatInstanceError(
            f"deployment {deployment!r}'s graph database DSN is unresolvable, "
            f"so the app database cannot be proved distinct from it",
            data={
                "reason": "graph_dsn_unresolvable",
                "deployment": deployment,
                "env_var": env_var,
                "manifest_key": "postgres.dsn",
                "resolved": target.as_dict(),
            },
            fix_hint=(
                "export the variable the manifest's postgres.dsn interpolates "
                "(OKG_DSN for the `dsn: ${OKG_DSN}` shape) and run the command "
                "again. The isolation check is not optional: without the graph "
                "DSN there is nothing to compare the app DSN against, and an "
                "app DSN that IS the graph DSN would be accepted — Open WebUI "
                "then migrates its own tables into the deployment's graph "
                "database"
            ),
        )
    graph_target = normalize_dsn(graph_dsn or "", label="graph database")
    if graph_target == target:
        raise ChatInstanceError(
            f"the app database DSN resolves to deployment {deployment!r}'s "
            f"GRAPH database ({target.describe()})",
            data={
                "reason": "app_database_is_graph_database",
                "deployment": deployment,
                "env_var": env_var,
                "resolved": target.as_dict(),
                "graph_database": graph_target.as_dict(),
            },
            fix_hint=(
                "Open WebUI-side state never lives in a deployment "
                "database (design.md:235-237). Create a separate Postgres "
                f"database for the chat instance and point {env_var} at "
                "it. The two DSNs are compared on resolved host, port and "
                "database name, so respelling one will not satisfy this"
            ),
        )

    for peer in peers:
        if peer.deployment == deployment:
            continue
        if peer.app_database == target:
            raise ChatInstanceError(
                f"the app database {target.describe()} is already the app "
                f"database of deployment {peer.deployment!r}",
                data={
                    "reason": "app_database_shared_with_peer",
                    "deployment": deployment,
                    "env_var": env_var,
                    "resolved": target.as_dict(),
                    "holder": peer.deployment,
                },
                fix_hint=(
                    "one app database per deployment. Create a second "
                    f"database and point {env_var} at it"
                ),
            )

    return AppDatabase(
        env_var=env_var, target=target, dsn=raw, graph_target=graph_target,
    )


# The reachability probe, as a seam. The default opens a real TCP connection
# and then, when psycopg is importable, a real session — a listening socket
# proves the host is up, not that the DATABASE exists, and "database does not
# exist" is exactly the case that would otherwise reach the vendor default.
ReachabilityProbe = Callable[[PgTarget, str], None]


def _default_reachability_probe(target: PgTarget, dsn: str) -> None:
    with socket.create_connection((target.host, target.port), timeout=5.0):
        pass
    try:
        # The factory module imports psycopg at import time, so this still
        # fails exactly when psycopg is unavailable -- the case the guard was
        # written for. It is deliberately narrow: one import, no work.
        from okg.substrate.db.connection import connect as db_connect
    except ImportError:  # pragma: no cover - psycopg is a hard dependency here
        return
    with db_connect(dsn, connect_timeout=5) as conn:
        conn.execute("SELECT 1")


def require_app_database_reachable(
    app_database: AppDatabase,
    *,
    deployment: str,
    probe: ReachabilityProbe | None = None,
) -> None:
    """Refuse to start unless the app database accepts a connection.

    This is the check that stands between an operator typo and F1: without it
    the container starts, ``DATABASE_URL`` points nowhere useful, and Open
    WebUI comes up on ``sqlite:////app/backend/data/webui.db`` reporting
    perfect health. The error message carries the RESOLVED target rather than
    the DSN, because the DSN carries the password.
    """
    probe = probe or _default_reachability_probe
    try:
        probe(app_database.target, app_database.dsn)
    except Exception as exc:  # noqa: BLE001 - every driver failure is one refusal
        raise ChatInstanceError(
            f"the app database {app_database.target.describe()} did not accept "
            f"a connection: {type(exc).__name__}",
            data={
                "reason": "app_database_unreachable",
                "deployment": deployment,
                "env_var": app_database.env_var,
                "resolved": app_database.target.as_dict(),
                # The exception text can echo the DSN, password included.
                "detail": type(exc).__name__,
            },
            fix_hint=(
                "start the Postgres server, create the database, or correct "
                f"{app_database.env_var}. The instance is NOT started on an "
                f"unreachable DSN: Open WebUI would fall back to its built-in "
                f"store at {VENDOR_DEFAULT_STORE} (env.py:267) and report "
                "itself healthy on the wrong database"
            ),
        ) from exc


def vendor_default_store_present(
    runtime: "ContainerRuntime", container: str,
) -> bool:
    """True when the vendor's SQLite store exists on the instance's filesystem.

    The measurement behind the no-silent-fallback arm. ``test -f`` inside the
    container, at :data:`VENDOR_DEFAULT_STORE`, whose location was measured at
    the pinned version (F1) rather than assumed. A container that does not
    exist has no store, which is the answer the refusal arms need.
    """
    if runtime.inspect(container) is None:
        return False
    code, _out, _err = runtime.execute(container, ["test", "-f", VENDOR_DEFAULT_STORE])
    return code == 0


# ---------------------------------------------------------------------------
# Ports: manifest-owned, validated against the deployment's own mcp port
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PortPlan:
    """The instance port, where it came from, and the mcp port it must miss."""

    instance_port: int
    source: str
    mcp_port: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "instance_port": self.instance_port,
            "source": self.source,
            "mcp_port": self.mcp_port,
        }


def _declared_instance_port(chat_block: Any) -> int | None:
    """The manifest's declared instance port, or None.

    Reads the typed attribute first and the block's ``raw`` mapping second, so
    the port is found whichever shape ``chat-manifest-block`` lands.
    """
    declared = getattr(chat_block, INSTANCE_PORT_KEY, None)
    if declared is None:
        raw = getattr(chat_block, "raw", None)
        if isinstance(raw, Mapping):
            declared = raw.get(INSTANCE_PORT_KEY)
    if declared is None:
        return None
    return int(declared)


def resolve_instance_port(
    deployment: str,
    chat_block: Any,
    *,
    override: int | None = None,
) -> PortPlan:
    """Decide the instance's host port, or refuse.

    Manifest first, operator flag second, refusal third. There is no fourth
    branch: no default constant, no ephemeral port, no increment of an
    occupied one. ``--instance-port`` is not a default — it is an explicit
    operator input, recorded as :data:`PORT_SOURCE_OPERATOR_FLAG` on every
    report so it can never be read as a manifest declaration.
    """
    mcp_port = getattr(getattr(chat_block, "mcp", None), "port", None)
    mcp_port = int(mcp_port) if mcp_port else None

    admitted = manifest_admits_instance_port()
    declared = _declared_instance_port(chat_block) if admitted else None

    if declared is not None and override is not None and int(declared) != int(override):
        raise ChatInstanceError(
            f"deployment {deployment!r} declares chat.{INSTANCE_PORT_KEY} "
            f"{int(declared)} and --instance-port names {int(override)}",
            data={
                "reason": "instance_port_disagreement",
                "deployment": deployment,
                "declared": int(declared),
                "override": int(override),
            },
            fix_hint=(
                "pass the declared port, drop the flag, or change the "
                "manifest. Silently preferring one of them would make the "
                "site's address depend on which input the reader looked at"
            ),
        )
    if declared is not None:
        plan = PortPlan(
            instance_port=int(declared),
            source=(
                PORT_SOURCE_MANIFEST_AGREED
                if override is not None
                else PORT_SOURCE_MANIFEST
            ),
            mcp_port=mcp_port,
        )
    elif override is not None:
        plan = PortPlan(
            instance_port=int(override),
            source=PORT_SOURCE_OPERATOR_FLAG,
            mcp_port=mcp_port,
        )
    elif admitted:
        raise ChatInstanceError(
            f"deployment {deployment!r} declares no chat.{INSTANCE_PORT_KEY}",
            data={
                "reason": "instance_port_undeclared",
                "deployment": deployment,
                "manifest_key": f"chat.{INSTANCE_PORT_KEY}",
                "manifest_admits_key": True,
            },
            fix_hint=(
                f"declare chat.{INSTANCE_PORT_KEY} in the deployment manifest "
                "(the manifest owns ports, design.md section 8 risk 3), or "
                "pass --instance-port for a one-off. No port is defaulted"
            ),
        )
    else:
        raise ChatInstanceError(
            f"deployment {deployment!r} cannot declare an instance port: the "
            f"chat schema does not admit chat.{INSTANCE_PORT_KEY} yet",
            data={
                "reason": "instance_port_key_not_admitted",
                "deployment": deployment,
                "manifest_key": f"chat.{INSTANCE_PORT_KEY}",
                "manifest_admits_key": False,
                "blocked_on": "chat-manifest-block",
            },
            fix_hint=(
                f"chat_block.py's unknown-key rejection makes chat."
                f"{INSTANCE_PORT_KEY} a load-time DeploymentConfigError until "
                "the chat-manifest-block PACT admits it. Until then pass "
                "--instance-port explicitly. The port is never defaulted"
            ),
        )

    if plan.mcp_port is not None and plan.instance_port == plan.mcp_port:
        raise ChatInstanceError(
            f"deployment {deployment!r} assigns its chat instance port "
            f"{plan.instance_port}, which is the port its own mcp-serve uses "
            f"(chat.mcp.port)",
            data={
                "reason": "instance_port_equals_mcp_port",
                "deployment": deployment,
                "instance_port": plan.instance_port,
                "mcp_port": plan.mcp_port,
                "source": plan.source,
            },
            fix_hint=(
                "give the instance a different port. One deployment cannot "
                "serve chat and its MCP endpoint on one port"
            ),
        )
    return plan


# ---------------------------------------------------------------------------
# The image: manifest-owned, and admitted only from the tested set
# ---------------------------------------------------------------------------

#: Where an image came from. ``default_pin`` is the honest name for "nobody
#: named one": it is a fact about this program, not a declaration by the
#: deployment, and a report that called it ``manifest`` would credit the
#: operator with a decision they never made.
IMAGE_SOURCE_MANIFEST = "manifest"
IMAGE_SOURCE_OPERATOR_FLAG = "operator_flag"
IMAGE_SOURCE_MANIFEST_AGREED = "manifest_and_flag_agree"
IMAGE_SOURCE_DEFAULT_PIN = "default_pin"


@dataclass(frozen=True)
class ImagePlan:
    """The image to run, where it came from, and the version it answers."""

    image: str
    source: str
    version: str

    def as_dict(self) -> dict[str, Any]:
        return {"image": self.image, "source": self.source, "version": self.version}


def _repository_of(image: str) -> str:
    """The repository part of an image reference, without tag or digest.

    A registry may carry a PORT (``registry.example.com:5000/x/y:v1.2.3``), so
    the first colon is not the tag separator. The tag separator is the last
    colon that has no ``/`` after it; anything else belongs to the host.
    """
    repository = image.split("@", 1)[0]
    head, separator, tail = repository.rpartition(":")
    if separator and "/" not in tail:
        return head
    return repository


def tested_image_version(
    image: str, *, tested_images: Mapping[str, str] | None = None,
) -> str:
    """The version ``image`` answers, or refuse naming the whole tested set.

    ``tested_images`` is an injection point, not a policy knob: the default is
    :data:`TESTED_IMAGES`, and it exists so a caller holding a DIFFERENT
    measured set -- a test proving the two-version arms, a future slice that
    reads a recorded census -- can supply it without this module growing a
    second opinion about which images are tested.
    """
    catalog = TESTED_IMAGES if tested_images is None else tested_images
    if not catalog:
        # An empty catalog admits nothing, and must not admit EVERYTHING by
        # having no arm that fires: a vacuous allow-list is how a gate goes
        # quiet without going away.
        raise ChatImageNotTested(
            "the tested-image set is empty, so no image can be admitted",
            data={"reason": "tested_set_empty", "image": image, "tested": []},
            fix_hint="restore okg.substrate.chat.instance.TESTED_IMAGES",
        )
    version = catalog.get(image)
    if version is None:
        is_upstream = _repository_of(image) == _repository_of(UPSTREAM_IMAGE_REF)
        because = (
            " Upstream Open WebUI v0.11.4 prefers a result's `content` and "
            "drops `structuredContent`; it has not been measured against "
            "this program, and the patched image "
            "your-registry.invalid/okg-openwebui-compat, which changes only that "
            "selection, is the one that has."
            if is_upstream else ""
        )
        raise ChatImageNotTested(
            f"image {image!r} is not one this program has been measured "
            f"against. The tested set is {sorted(catalog)}.{because}",
            data={
                "reason": "image_not_tested",
                "image": image,
                "tested": sorted(catalog),
                "upstream_ref": is_upstream,
            },
            fix_hint=(
                "the default chat image is a placeholder: build "
                "ops/openwebui/Dockerfile, set chat.image, and add its digest "
                "to TESTED_IMAGES. Run the patched image okg-openwebui-compat "
                "pinned by digest -- upstream v0.11.4 drops structured tool "
                "results -- or re-take the measurements in "
                "okg/substrate/chat/instance.py's header against a new image "
                "and add it to TESTED_IMAGES"
            ),
        )
    return version


def _declared_image(chat_block: Any) -> str | None:
    """The manifest's declared image, or None. Typed attribute, then ``raw``."""
    declared = getattr(chat_block, IMAGE_KEY, None)
    if declared is None:
        raw = getattr(chat_block, "raw", None)
        if isinstance(raw, Mapping):
            declared = raw.get(IMAGE_KEY)
    return str(declared) if declared else None


def resolve_instance_image(
    deployment: str,
    chat_block: Any,
    *,
    override: str | None = None,
    tested_images: Mapping[str, str] | None = None,
) -> ImagePlan:
    """Decide which image to run, or refuse.

    Manifest first, operator flag second, the tested default third. Unlike the
    port there IS a third branch, because an unnamed image has a defensible
    answer -- the one version every measured fact in this module was taken at
    -- while an unnamed port has none. Two inputs that disagree refuse either
    way: the site's identity must not depend on which input a reader read.

    Whatever the source, the result is checked against the tested set, so a
    manifest cannot smuggle in an unmeasured image by declaring it and a flag
    cannot by naming it.
    """
    declared = _declared_image(chat_block)
    if declared is not None and override is not None and declared != override:
        raise ChatInstanceError(
            f"deployment {deployment!r} declares chat.{IMAGE_KEY} {declared!r} "
            f"and --image names {override!r}",
            data={
                "reason": "image_disagreement",
                "deployment": deployment,
                "declared": declared,
                "override": override,
            },
            fix_hint=(
                "pass the declared image, drop the flag, or change the manifest"
            ),
        )
    if declared is not None:
        image, source = declared, (
            IMAGE_SOURCE_MANIFEST_AGREED if override is not None
            else IMAGE_SOURCE_MANIFEST
        )
    elif override is not None:
        image, source = override, IMAGE_SOURCE_OPERATOR_FLAG
    else:
        image, source = PINNED_IMAGE_REF, IMAGE_SOURCE_DEFAULT_PIN
    return ImagePlan(
        image=image,
        source=source,
        version=tested_image_version(image, tested_images=tested_images),
    )


#: What a port probe found. ``bound`` and ``answering`` are both occupied;
#: distinguishing them tells an operator whether something is listening or
#: whether the address is merely reserved.
OCCUPANCY_FREE = "free"
OCCUPANCY_BOUND = "bound"
OCCUPANCY_ANSWERING = "answering"


def port_occupancy(port: int, *, host: str = LOOPBACK_CANONICAL) -> str:
    """Probe a real socket. Never simulated, never inferred from a record.

    Two independent signals, because either alone has a blind spot: a
    ``bind`` that fails proves the address is taken, and a ``connect`` that
    succeeds proves something is listening even where the platform would have
    let the bind through on a different local address.

    The binds set ``SO_REUSEADDR``, as container runtimes do when they
    publish a port, so a closed connection's ``TIME_WAIT`` record is not
    read as a holder (issue #2876 item 3); a listener still refuses them.
    ``SO_REUSEADDR`` lets a specific-address bind through beside a wildcard
    one on BSD/macOS, so a second bind on the IPv4 wildcard keeps a
    ``0.0.0.0`` or dual-stack ``::`` holder that is bound but not listening
    reading ``bound`` (measured on macOS 2026-09-28; it refuses there, while
    ``TIME_WAIT`` alone does not).
    """
    for address in dict.fromkeys((host, "0.0.0.0")):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((address, port))
        except OSError:
            return OCCUPANCY_BOUND
        finally:
            probe.close()
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return OCCUPANCY_ANSWERING
    except OSError:
        return OCCUPANCY_FREE


def require_port_free(
    port: int,
    *,
    deployment: str,
    host: str = LOOPBACK_CANONICAL,
    peers: Sequence["InstanceRecord"] = (),
) -> None:
    """Refuse an occupied port, naming the deployment holding it.

    No increment, no ephemeral replacement, no "try the next one". The holder
    is looked up in operator state so the message names a deployment when one
    is recorded, and says so honestly when the socket is held by something
    outside okg.
    """
    occupancy = port_occupancy(port, host=host)
    if occupancy == OCCUPANCY_FREE:
        return
    holder = next(
        (
            peer.deployment
            for peer in peers
            if peer.port == port and peer.deployment != deployment
        ),
        None,
    )
    held_by = (
        f"deployment {holder!r}" if holder else "a process outside okg's records"
    )
    raise ChatInstanceError(
        f"port {port} on {host} is already {occupancy}, held by {held_by}",
        data={
            "reason": "instance_port_conflict",
            "deployment": deployment,
            "port": port,
            "host": host,
            "occupancy": occupancy,
            "holder": holder,
        },
        fix_hint=(
            f"free port {port}, or declare a different chat."
            f"{INSTANCE_PORT_KEY} for {deployment!r}. The port is NOT "
            "incremented and NOT replaced by an ephemeral port: a chat site "
            "that silently moved is a site whose front door, deep links and "
            "proxy configuration all now point at nothing"
        ),
    )


# ---------------------------------------------------------------------------
# Operator state: what this host has stood up
# ---------------------------------------------------------------------------

#: One file per deployment under the chat operator-state tree (design.md:262).
#: Carries the port, the container and the RESOLVED app-database target — the
#: three facts a second deployment's stand-up must consult — and no credential.
INSTANCE_STATE_FILE = "chat-instance.json"

#: The graph URL ``okg chat setup`` registered with this instance, the probe
#: URL it came from and the instance's base URL (issue #2876 item 1). Written
#: by ``chat.setup``; removed with the instance record.
GRAPH_ENDPOINT_STATE_FILE = "graph-endpoint.json"


@dataclass(frozen=True)
class InstanceRecord:
    """What operator state remembers about one deployment's instance.

    Emphatically NOT the status surface. Nothing here is evidence that the
    instance is running: :func:`instance_status` re-observes every time. This
    record answers only "where should I look, and who holds what".
    """

    deployment: str
    container: str
    host: str
    port: int
    image: str
    #: Required, not optional. This is the field a second deployment's
    #: stand-up compares its own app DSN against; a record allowed to carry
    #: ``None`` here would compare unequal to every target and so leave the
    #: collision check with a hole shaped exactly like this deployment.
    app_database: PgTarget
    port_source: str
    recorded_at: str
    #: The install-plan directory the instance was stood up with (its site
    #: name may come from that bundle). Only NAMED in messages -- a later
    #: ``up`` without ``--install-plan`` never reuses it (#2875). ``None``:
    #: no bundle.
    install_plan: str | None = None
    #: How the container reaches the host's loopback (#2876 item 2). A record
    #: from before the field existed reads as ``gateway``, which is what
    #: every earlier instance ran with.
    network_mode: str = NETWORK_GATEWAY
    #: The host-loopback ports forwarded into the container (``pasta``).
    forwarded_ports: tuple[int, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "deployment": self.deployment,
            "container": self.container,
            "host": self.host,
            "port": self.port,
            "image": self.image,
            "app_database": self.app_database.as_dict(),
            "port_source": self.port_source,
            "recorded_at": self.recorded_at,
            "network_mode": self.network_mode,
        }
        if self.forwarded_ports:
            payload["forwarded_ports"] = list(self.forwarded_ports)
        if self.install_plan:
            payload["install_plan"] = self.install_plan
        return payload


def _state_root(state_root: Path | str | None = None) -> Path:
    if state_root is not None:
        return Path(state_root)
    from okg.substrate.deployments import user_state_chat_instances_root

    return user_state_chat_instances_root()


def _state_roots(state_root: Path | str | None = None) -> tuple[Path, ...]:
    """Return current state first, plus the pre-issue-1200 legacy root.

    Explicit roots are test/operator injection points and are never widened.
    Default reads keep seeing records written before the path fix, while all
    new writes land only in the isolated chat-instances root.
    """
    primary = _state_root(state_root)
    if state_root is not None:
        return (primary,)
    from okg.substrate.deployments import user_state_deployments_root

    legacy = user_state_deployments_root()
    return (primary,) if primary == legacy else (primary, legacy)


def _state_paths(
    deployment: str, state_root: Path | str | None = None,
) -> tuple[Path, ...]:
    return tuple(
        root / deployment / INSTANCE_STATE_FILE
        for root in _state_roots(state_root)
    )


def instance_base_url(host: str, port: int) -> str:
    """``http://host:port``, with an IPv6 host bracketed (``[::]``) so the
    URL parses."""
    shown = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"http://{shown}:{port}"


def container_name_for(deployment: str) -> str:
    """The container this module owns for a deployment. Stable and derived."""
    return f"okg-chat-{deployment}"


def record_instance(
    record: InstanceRecord, *, state_root: Path | str | None = None,
) -> Path:
    """Persist the record. Never a credential — :meth:`as_dict` cannot emit one."""
    path = _state_root(state_root) / record.deployment / INSTANCE_STATE_FILE
    if state_root is None:
        legacy = _state_paths(record.deployment)[1:]
        existing_legacy = next((candidate for candidate in legacy if candidate.is_file()), None)
        if existing_legacy is not None and path.is_file():
            raise ChatInstanceError(
                f"deployment {record.deployment!r} has both current and legacy "
                "chat-instance state files",
                data={
                    "reason": "instance_state_ambiguous",
                    "deployment": record.deployment,
                    "paths": [str(path), str(existing_legacy)],
                },
                fix_hint=(
                    f"keep the authoritative state at {path}, remove the legacy "
                    f"file at {existing_legacy}, and run the command again"
                ),
            )
        if existing_legacy is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            existing_legacy.replace(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record.as_dict(), indent=2, sort_keys=True) + "\n")
    return path


def _unreadable_state(
    deployment: str, path: Path, detail: str,
) -> "ChatInstanceError":
    """The one refusal for a state file that exists and cannot be trusted.

    Separated from :func:`read_instance` only so every unreadable shape raises
    the identical error; the discrimination that matters is between ABSENT (no
    file — return ``None``) and UNREADABLE (a file we cannot parse — refuse).
    """
    return ChatInstanceError(
        f"deployment {deployment!r}'s chat-instance state file is unreadable: "
        f"{detail}",
        data={
            "reason": "instance_state_unreadable",
            "deployment": deployment,
            "path": str(path),
            "detail": detail,
        },
        fix_hint=(
            f"repair or delete {path}. It is NOT read as 'this deployment has "
            "no instance': that answer would drop the deployment's app "
            "database out of the collision set a second deployment's stand-up "
            "checks against, and a SHARED app database would then be accepted"
        ),
    )


def _read_instance_file(
    deployment: str, path: Path,
) -> InstanceRecord | None:
    """Read and validate one already-resolved state file."""
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise _unreadable_state(
            deployment, path, f"{type(exc).__name__}: {exc}",
        ) from exc
    if not isinstance(raw, Mapping):
        raise _unreadable_state(
            deployment, path, f"the record is a {type(raw).__name__}, not an object",
        )
    app = raw.get("app_database")
    if not isinstance(app, Mapping):
        raise _unreadable_state(
            deployment, path, "the record names no app_database target",
        )
    try:
        record = InstanceRecord(
            deployment=str(raw.get("deployment") or deployment),
            container=str(raw.get("container") or container_name_for(deployment)),
            host=str(raw.get("host") or LOOPBACK_CANONICAL),
            port=int(raw.get("port") or 0),
            image=str(raw.get("image") or ""),
            app_database=PgTarget(
                host=_required_state_field(deployment, path, app, "host"),
                port=int(app.get("port") or DEFAULT_POSTGRES_PORT),
                database=_required_state_field(deployment, path, app, "database"),
            ),
            port_source=str(raw.get("port_source") or ""),
            recorded_at=str(raw.get("recorded_at") or ""),
            install_plan=(
                str(raw["install_plan"]) if raw.get("install_plan") else None
            ),
            network_mode=str(raw.get("network_mode") or NETWORK_GATEWAY),
            forwarded_ports=tuple(
                int(p) for p in (raw.get("forwarded_ports") or ())
            ),
        )
    except (TypeError, ValueError) as exc:
        raise _unreadable_state(
            deployment, path, f"{type(exc).__name__}: {exc}",
        ) from exc
    return record


def read_instance(
    deployment: str, *, state_root: Path | str | None = None,
) -> InstanceRecord | None:
    """The recorded instance, ``None`` when there is none, else a refusal.

    ``None`` means one thing only: no state file. A file that exists but does
    not parse, or parses to something other than a complete record, REFUSES.
    Default reads check the isolated chat-state root first and then the legacy
    deployment-root location so an existing instance is not forgotten by the
    path migration.
    """
    paths = tuple(path for path in _state_paths(deployment, state_root) if path.is_file())
    if len(paths) > 1:
        raise ChatInstanceError(
            f"deployment {deployment!r} has both current and legacy "
            "chat-instance state files",
            data={
                "reason": "instance_state_ambiguous",
                "deployment": deployment,
                "paths": [str(path) for path in paths],
            },
            fix_hint=(
                f"keep the authoritative state at {paths[0]}, remove the "
                f"legacy file at {paths[1]}, and run the command again"
            ),
        )
    if paths:
        return _read_instance_file(deployment, paths[0])
    return None


def _required_state_field(
    deployment: str, path: Path, app: Mapping[str, Any], key: str,
) -> str:
    value = app.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _unreadable_state(
            deployment, path, f"the app_database target names no {key}",
        )
    return value.strip()


def forget_instance(
    deployment: str, *, state_root: Path | str | None = None,
) -> None:
    for path in _state_paths(deployment, state_root):
        path.unlink(missing_ok=True)
        # The graph URL setup registered belongs to THIS instance; a later
        # instance must not inherit it (#2876 item 1).
        (path.parent / GRAPH_ENDPOINT_STATE_FILE).unlink(missing_ok=True)


def peer_instances(
    *, exclude: str | None = None, state_root: Path | str | None = None,
) -> list[InstanceRecord]:
    """Every OTHER deployment's recorded instance on this host."""
    found: list[InstanceRecord] = []
    candidates: dict[str, list[Path]] = {}
    for root in _state_roots(state_root):
        if not root.is_dir():
            continue
        for child in sorted(root.iterdir()):
            if not child.is_dir() or child.name == exclude:
                continue
            path = child / INSTANCE_STATE_FILE
            if not path.is_file():
                continue
            candidates.setdefault(child.name, []).append(path)
    for deployment, paths in sorted(candidates.items()):
        if len(paths) > 1:
            raise ChatInstanceError(
                f"deployment {deployment!r} has both current and legacy "
                "chat-instance state files",
                data={
                    "reason": "instance_state_ambiguous",
                    "deployment": deployment,
                    "paths": [str(path) for path in paths],
                },
                fix_hint=(
                    f"keep the authoritative state at {paths[0]}, remove the "
                    f"legacy file at {paths[1]}, and run the command again"
                ),
            )
        record = _read_instance_file(deployment, paths[0])
        if record is not None:
            found.append(record)
    return found


# ---------------------------------------------------------------------------
# The container spec
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class InstanceSpec:
    """Everything needed to launch one instance, with the secrets segregated.

    ``env`` is split from the argv on purpose. Container runtimes accept
    ``-e NAME`` (no ``=``) meaning "inherit NAME from my environment", so the
    DSN and the secret key travel through the child process's environment and
    the argv carries only NAMES. A ``ps`` on the host, a shell history, a CI
    log and a crash dump of the argv all see variable names.
    """

    deployment: str
    container: str
    image: str
    host: str
    host_port: int
    container_port: int
    env: Mapping[str, str] = field(default_factory=dict, repr=False)
    labels: Mapping[str, str] = field(default_factory=dict)
    host_gateway_alias: str | None = None
    notes: tuple[str, ...] = ()
    #: How the container reaches the host's loopback (``container_network``).
    network_mode: str = NETWORK_GATEWAY
    #: The host-loopback ports pasta forwards (``pasta`` mode only).
    forward_ports: tuple[int, ...] = ()

    @property
    def base_url(self) -> str:
        return instance_base_url(self.host, self.host_port)

    def env_var_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.env))

    def as_dict(self) -> dict[str, Any]:
        """Reportable form. Emits env var NAMES; values never leave the spec."""
        return {
            "deployment": self.deployment,
            "container": self.container,
            "image": self.image,
            "host": self.host,
            "host_port": self.host_port,
            "container_port": self.container_port,
            "env_var_names": list(self.env_var_names()),
            "labels": dict(self.labels),
            "host_gateway_alias": self.host_gateway_alias,
            "network_mode": self.network_mode,
            "forward_ports": list(self.forward_ports),
            "notes": list(self.notes),
        }

    def run_argv(self, runtime_binary: str) -> list[str]:
        """The launch argv. Contains no secret VALUE — assert it in tests."""
        argv = [
            runtime_binary,
            "run",
            "--detach",
            "--name",
            self.container,
        ]
        if self.network_mode == NETWORK_HOST:
            # No --publish: the container IS on the host network, bound to
            # 127.0.0.1 by HOST/PORT in its environment (never 0.0.0.0).
            argv += ["--network", "host"]
        else:
            if self.network_mode == NETWORK_PASTA:
                argv += ["--network", pasta_network_arg(self.forward_ports)]
            argv += [
                "--publish",
                f"{self.host}:{self.host_port}:{self.container_port}",
            ]
        for key, value in sorted(self.labels.items()):
            argv += ["--label", f"{key}={value}"]
        if self.host_gateway_alias:
            # Linux modes: the alias is the container's own 127.0.0.1, which
            # is the host's loopback for a forwarded port (pasta) or for any
            # port (host network).
            target = (
                LISTEN_HOST if self.network_mode in LOOPBACK_MODES
                else "host-gateway"
            )
            argv += ["--add-host", f"{self.host_gateway_alias}:{target}"]
        for name in self.env_var_names():
            # Name only. The value rides in the child environment.
            argv += ["--env", name]
        argv.append(self.image)
        return argv


def _rewrite_for_container(
    dsn: str, target: PgTarget, alias: str | None,
) -> tuple[str, str | None]:
    """Point a loopback app DSN at the host from inside the container.

    ``127.0.0.1`` inside a container is the CONTAINER. An app database on the
    host's loopback is unreachable from the instance unless the host gateway
    is named, so the container's copy of the DSN gets the alias substituted
    and the substitution is RECORDED as a note. It is not silent: the report,
    the JSON envelope and the operator's terminal all carry it, and the
    isolation checks upstream ran on the operator-side target so the rewrite
    can never launder a collision.
    """
    if target.host != LOOPBACK_CANONICAL or not alias:
        return dsn, None
    split = urlsplit(dsn)
    netloc = split.netloc
    # Replace only the host component, leaving userinfo and port untouched.
    userinfo, _, hostport = netloc.rpartition("@")
    port_suffix = f":{split.port}" if split.port else ""
    rebuilt_host = f"{alias}{port_suffix}"
    rebuilt = f"{userinfo}@{rebuilt_host}" if userinfo else rebuilt_host
    rewritten = dsn.replace(netloc, rebuilt, 1) if hostport else dsn
    note = (
        f"the app database is on the host's loopback, so the container's "
        f"copy of {CONTAINER_APP_DATABASE_VAR} names {alias} instead of "
        f"{LOOPBACK_CANONICAL}; isolation was checked on the host-side target "
        f"{target.describe()}"
    )
    return rewritten, note


def settings_fingerprint(env: Mapping[str, str], *, network: str | None = None) -> str:
    """A stable hash over every launch variable's name and value.

    The values include credentials, so only the digest leaves this function.
    ``network`` is the container-network part (mode and forwarded ports);
    ``None`` -- the gateway mode -- leaves the digest as it always was.
    """
    import hashlib

    digest = hashlib.sha256()
    for name in sorted(env):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(env[name]).encode("utf-8"))
        digest.update(b"\0")
    if network is not None:
        digest.update(b"\0network\0")
        digest.update(network.encode("utf-8"))
    return f"sha256:{digest.hexdigest()}"


def _refuse_changed_settings(
    deployment: str, spec: "InstanceSpec", facts: Mapping[str, Any],
    *, install_plan: Path | str | None = None,
    recorded_install_plan: str | None = None,
    recorded_network_mode: str | None = None,
) -> None:
    """Refuse to call a container "up" when it runs different settings.

    A container reads its environment once, at creation, so answering
    ``already_running`` for one started with other settings would report the
    manifest as applied when it is not. Recreating is the operator's call --
    it ends every signed-in session -- so this refuses and names the fix
    rather than doing it.

    A container from before the fingerprint existed has no label. For it the
    old behavior is kept, EXCEPT when the desired settings carry an OIDC
    login, or a network mode other than ``gateway``: those are exactly the
    changes a pre-existing container cannot have (it predates both, so it
    runs the gateway mode).

    ``recorded_network_mode`` is the mode the instance record says the
    container runs (``None``: no record, which is the gateway mode).
    """
    recorded = (facts.get("labels") or {}).get(LABEL_SETTINGS)
    desired = spec.labels.get(LABEL_SETTINGS)
    if recorded == desired:
        return
    if recorded is None and spec.network_mode == NETWORK_GATEWAY and not any(
        name.startswith(("OAUTH_", "OPENID_")) for name in spec.env
    ):
        return
    running_mode = (
        NETWORK_GATEWAY if recorded is None
        else (recorded_network_mode or NETWORK_GATEWAY)
    )
    keep_mode = (
        f"; or keep the running instance by passing --container-network "
        f"{running_mode} (it runs the {running_mode} network mode, this run "
        f"asked for {spec.network_mode})"
        if running_mode != spec.network_mode
        else ""
    )
    if recorded_install_plan and not install_plan:
        # Named, never reused implicitly: the plan may be gone, and setup's
        # sync would not compose it either (#2875 review).
        fix_hint = (
            f"this container was started with --install-plan "
            f"{recorded_install_plan}; pass it again (okg chat-instance up "
            f"--deployment {deployment} --install-plan {recorded_install_plan}), "
            f"or run okg chat-instance down --deployment {deployment} then up "
            "to drop the bundle's settings. The app database is kept"
        )
    else:
        fix_hint = (
            f"recreate it: okg chat-instance down --deployment {deployment}, "
            f"then okg chat-instance up --deployment {deployment}"
            + (
                f" --install-plan {install_plan}"
                if install_plan
                else " (with --install-plan <dir> if it was installed from a bundle)"
            )
            + ". The app database is kept; signed-in sessions end"
        )
    raise ChatInstanceError(
        f"deployment {deployment!r}'s instance container {spec.container!r} "
        "was started with different settings than the manifest and operator "
        "environment now declare (for example a changed chat.oidc_login, a "
        "changed chat.ui.site_name, a rotated secret, another "
        "--container-network mode or forwarded port, or a container started "
        "before okg set ENABLE_VERSION_UPDATE_CHECK=false and "
        "ENABLE_EASTER_EGGS=false), and a running container never re-reads "
        "them",
        data={
            "reason": "instance_settings_changed",
            "deployment": deployment,
            "container": spec.container,
            "fingerprint_recorded": recorded is not None,
            "recorded_install_plan": recorded_install_plan,
            "recorded_network_mode": running_mode,
            "desired_network_mode": spec.network_mode,
        },
        fix_hint=fix_hint + keep_mode,
    )


def _oidc_login_env(
    deployment: str, oidc_login: Any, environ: Mapping[str, str],
) -> dict[str, str]:
    """The instance's own OIDC login, as Open WebUI reads it (PACT chat-oidc-login).

    Open WebUI signs a person in with the identity provider directly, maps the
    provider's roles onto its user/admin roles, keeps the person's token, and
    forwards it to ``mcp-serve`` on every tool call, where OKG verifies it again
    itself (PACT mcp-oidc-auth). No proxy vouches for anyone.

    These variables are authoritative on every start: ``oauth.*`` settings are
    not persisted to the instance database at the pinned version
    (ENABLE_OAUTH_PERSISTENT_CONFIG defaults off). Like the two variables
    above, they ride the child environment and only their NAMES reach argv.
    """
    secret_env = oidc_login.client_secret_env
    secret = (environ.get(secret_env) or "").strip()
    if not secret:
        raise ChatInstanceError(
            f"the chat OIDC client secret is not set: environment variable "
            f"{secret_env} is empty or absent",
            data={
                "reason": "oidc_client_secret_env_unset",
                "deployment": deployment,
                "env_var": secret_env,
            },
            fix_hint=(
                f"export {secret_env} to the client secret of "
                f"{oidc_login.client_id}. The value is never a command-line "
                "argument"
            ),
        )
    env = {
        "OAUTH_CLIENT_ID": oidc_login.client_id,
        "OAUTH_CLIENT_SECRET": secret,
        "OPENID_PROVIDER_URL": oidc_login.discovery_url,
        "OAUTH_SCOPES": "openid email profile",
        "OAUTH_PROVIDER_NAME": oidc_login.provider_name or "SSO",
        "ENABLE_OAUTH_SIGNUP": "true",
        # Declared, never assumed: the pinned instance merges on the email
        # alone without consulting `email_verified` (see ChatOidcLogin).
        "OAUTH_MERGE_ACCOUNTS_BY_EMAIL": (
            "true" if oidc_login.merge_accounts_by_email else "false"
        ),
        "ENABLE_OAUTH_ROLE_MANAGEMENT": "true",
        "OAUTH_ROLES_CLAIM": oidc_login.roles_claim,
        "OAUTH_ALLOWED_ROLES": ",".join(oidc_login.allowed_roles),
        "OAUTH_ADMIN_ROLES": ",".join(oidc_login.admin_roles),
    }
    if oidc_login.redirect_uri:
        env["OPENID_REDIRECT_URI"] = oidc_login.redirect_uri
    return env


def build_instance_spec(
    deployment: str,
    *,
    app_database: AppDatabase,
    secret_env: str,
    environ: Mapping[str, str],
    port_plan: PortPlan,
    image: str = PINNED_IMAGE_REF,
    host: str = LOOPBACK_CANONICAL,
    host_gateway_alias: str | None = "host.docker.internal",
    oidc_login: Any | None = None,
    site_name: str | None = None,
    network_mode: str = NETWORK_GATEWAY,
    forward_ports: Sequence[int] = (),
) -> InstanceSpec:
    """Assemble the launch spec, refusing every way it could be half-wired.

    ``site_name`` (``chat.ui.site_name``) becomes ``WEBUI_NAME``: the only
    way the pinned vendor takes a site name. It is part of the settings
    fingerprint, so a changed name makes ``up`` refuse ``instance_settings_changed``.

    Three assertions this function exists for, beyond assembling strings:

    * the secret-key variable is present. An instance with no
      ``WEBUI_SECRET_KEY`` generates one per boot and invalidates every
      session on restart — which the supervisor makes routine;
    * ``CUSTOM_NAME`` is absent (F3). Present, it would take ``WEBUI_NAME``
      out of ``chat sync``'s hands via a call to ``api.openwebui.com``;
    * the six-part ``DATABASE_*`` form (F2) is absent, so exactly one variable
      carries the DSN.
    """
    secret = (environ.get(secret_env) or "").strip()
    if not secret:
        raise ChatInstanceError(
            f"the instance admin credential source is not set: environment "
            f"variable {secret_env} is empty or absent",
            data={
                "reason": "secret_key_env_unset",
                "deployment": deployment,
                "env_var": secret_env,
            },
            fix_hint=(
                f"export {secret_env} to a long random string, or name a "
                f"different variable with --admin-secret-env. Open WebUI "
                f"generates a per-boot key when {CONTAINER_SECRET_KEY_VAR} is "
                "unset, so every supervisor restart would sign every operator "
                "out. The value is never a command-line argument"
            ),
        )
    forbidden = [name for name in CONTAINER_FORBIDDEN_VARS if environ.get(name)]
    if forbidden:
        raise ChatInstanceError(
            f"refusing to start deployment {deployment!r}'s instance while "
            f"{', '.join(forbidden)} is set in this environment",
            data={
                "reason": "forbidden_env_var_set",
                "deployment": deployment,
                "variables": forbidden,
            },
            fix_hint=(
                "CUSTOM_NAME makes Open WebUI fetch api.openwebui.com at "
                "import and REASSIGN WEBUI_NAME plus overwrite favicon.png "
                "and splash.png (config.py:184-211). Site name is the "
                "manifest's governed knob, rendered by `okg chat sync`; a "
                "vendor round trip would make it ungoverned. Unset "
                "CUSTOM_NAME"
            ),
        )

    if network_mode in LOOPBACK_MODES:
        # The container dials the host's loopback as 127.0.0.1 itself.
        container_dsn, note = app_database.dsn, None
    else:
        container_dsn, note = _rewrite_for_container(
            app_database.dsn, app_database.target, host_gateway_alias,
        )
    env = {
        CONTAINER_APP_DATABASE_VAR: container_dsn,
        CONTAINER_SECRET_KEY_VAR: secret,
        **CONTAINER_FIXED_ENV,
    }
    if site_name:
        env[CONTAINER_SITE_NAME_VAR] = site_name
    if oidc_login is not None:
        env.update(_oidc_login_env(deployment, oidc_login, environ))
    leaked = sorted(set(env) & set(CONTAINER_SPLIT_DATABASE_VARS))
    if leaked:  # pragma: no cover - structural guard on the dict above
        raise ChatInstanceError(
            f"the spec carries the six-part DATABASE_* form: {leaked}",
            data={"reason": "split_database_vars_present", "variables": leaked},
        )
    notes = [note] if note else []
    container_port = CONTAINER_HTTP_PORT
    ports = tuple(sorted({int(p) for p in forward_ports}))
    if network_mode == NETWORK_HOST:
        if host != LOOPBACK_CANONICAL:
            raise ChatInstanceError(
                f"refusing to start deployment {deployment!r}'s instance on "
                f"the host network bound to {host}: in that mode Open WebUI "
                "listens on the host itself, and only 127.0.0.1 keeps it off "
                "every other interface",
                data={
                    "reason": "host_network_needs_loopback",
                    "deployment": deployment,
                    "host": host,
                },
                fix_hint=(
                    "drop --host, or use --container-network gateway to "
                    "publish the site on another interface"
                ),
            )
        # ALWAYS set: start.sh defaults HOST to 0.0.0.0, which on the host
        # network would expose the site on every interface.
        env[LISTEN_HOST_VAR] = LISTEN_HOST
        env[LISTEN_PORT_VAR] = str(port_plan.instance_port)
        container_port = port_plan.instance_port
        ports = ()
        notes.append(
            f"--container-network host: {HOST_NETWORK_REACH}. Open WebUI "
            f"listens on {LISTEN_HOST}:{port_plan.instance_port} "
            f"({LISTEN_HOST_VAR}, {LISTEN_PORT_VAR}) and dials the host's "
            "loopback as 127.0.0.1"
        )
    elif network_mode == NETWORK_PASTA:
        require_forwardable(deployment, network_mode, ports, container_port)
        notes.append(
            "the container reaches the host's loopback port(s) "
            f"{', '.join(str(p) for p in ports) or '(none)'} as 127.0.0.1 "
            "through pasta forwarding; only those ports, and nothing new "
            "listens on the host"
        )
    else:
        ports = ()
    alias_used = note is not None or network_mode in LOOPBACK_MODES
    return InstanceSpec(
        deployment=deployment,
        container=container_name_for(deployment),
        image=image,
        host=host,
        host_port=port_plan.instance_port,
        container_port=container_port,
        env=env,
        labels={
            LABEL_DEPLOYMENT: deployment,
            LABEL_MANAGED_BY: MANAGED_BY,
            LABEL_SETTINGS: settings_fingerprint(
                env, network=network_fingerprint_part(network_mode, ports),
            ),
        },
        host_gateway_alias=host_gateway_alias if alias_used else None,
        notes=tuple(notes),
        network_mode=network_mode,
        forward_ports=ports,
    )


# ---------------------------------------------------------------------------
# The container runtime seam
# ---------------------------------------------------------------------------

class ContainerRuntime(Protocol):
    """The four operations this lifecycle needs from a container runtime.

    A seam, so the hermetic suite can drive the whole lifecycle against real
    sockets and real HTTP without a container runtime on the machine — and so
    the live arm can drive the SAME code through docker. The pytest arm and
    the live arm therefore differ in one object, not in a code path.

    Two obligations every implementation carries, because the lifecycle reads
    these answers as facts about the world:

    * :meth:`inspect` returns ``None`` for "no such container" and RAISES for
      "I could not ask" — an unreachable daemon collapsed into ``None`` reads
      as an absent container, which is how a teardown reports success while
      the instance is still running;
    * :meth:`stop` and :meth:`remove` raise unless the operation succeeded.
      Their exit codes are the only evidence the container is gone.
    """

    binary: str

    def inspect(self, container: str) -> dict[str, Any] | None:
        """Container facts, or None when the runtime says no such container.

        Raises :class:`ChatInstanceError` when the runtime could not be asked.
        """

    def run(self, spec: InstanceSpec) -> str:
        """Launch, returning the container id."""

    def stop(self, container: str) -> None:
        """Stop it. Raises unless the runtime reported success."""

    def remove(self, container: str) -> None:
        """Remove it. Raises unless the runtime reported success."""

    def start(self, container: str) -> None:
        """Restart an existing, exited container."""

    def execute(
        self, container: str, argv: Sequence[str],
    ) -> tuple[int, str, str]:
        """Run a command inside the container."""


CommandRunner = Callable[[Sequence[str], Mapping[str, str] | None], "subprocess.CompletedProcess[str]"]


def _default_runner(
    argv: Sequence[str], env: Mapping[str, str] | None,
) -> "subprocess.CompletedProcess[str]":
    child_env = dict(os.environ)
    if env:
        child_env.update(env)
    return subprocess.run(  # noqa: S603 - argv is built here, never shell
        list(argv), capture_output=True, text=True, env=child_env, check=False,
    )


#: What a container runtime says when the container genuinely is not there.
#: ``docker inspect`` answers ``Error: No such object: <name>``; newer clients
#: and podman answer ``No such container``. Matched POSITIVELY: absence has to
#: be witnessed, because the alternative reading of a non-zero exit — the
#: daemon is unreachable — is the one that makes a teardown lie.
_RUNTIME_ABSENT_MARKERS = ("no such object", "no such container", "no such image")


def _runtime_unavailable(
    binary: str, container: str, verb: str, detail: str,
) -> "ChatInstanceError":
    return ChatInstanceError(
        f"{binary} could not be asked about container {container!r}: {detail}",
        data={
            "reason": "container_runtime_unavailable",
            "container": container,
            "binary": binary,
            "verb": verb,
            "detail": detail,
        },
        fix_hint=(
            f"start {binary} (or the daemon it talks to) and run the command "
            f"again. A runtime that cannot be asked is NOT reported as 'no "
            f"such container': that reading lets `okg chat-instance down` "
            f"forget the record while the instance is still running"
        ),
    )


class DockerRuntime:
    """``docker`` (or ``podman``) driven through argv, never a shell.

    The launch call is the only one that passes a child environment, and it
    passes it precisely because :meth:`InstanceSpec.run_argv` deliberately
    left the values out of the argv.
    """

    def __init__(
        self, binary: str = "docker", *, runner: CommandRunner | None = None,
    ) -> None:
        self.binary = binary
        self._runner = runner or _default_runner

    def _run(
        self, argv: Sequence[str], *, env: Mapping[str, str] | None = None,
        verb: str = "run", container: str = "",
    ) -> "subprocess.CompletedProcess[str]":
        try:
            return self._runner(argv, env)
        except OSError as exc:
            # The binary is missing, or not executable. Distinct from "the
            # daemon said no", and equally not an absent container.
            raise _runtime_unavailable(
                self.binary, container, verb, f"{type(exc).__name__}: {exc}",
            ) from exc

    def inspect(self, container: str) -> dict[str, Any] | None:
        result = self._run(
            [self.binary, "inspect", container], verb="inspect", container=container,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            lowered = stderr.lower()
            if not any(marker in lowered for marker in _RUNTIME_ABSENT_MARKERS):
                # Anything we cannot read as "not there" is "could not ask".
                # Fail closed: an unreachable daemon, a socket we lack
                # permission on, and a client/server version mismatch all land
                # here rather than being reported as an absent container.
                raise _runtime_unavailable(
                    self.binary, container, "inspect",
                    stderr[:2000] or f"exit code {result.returncode}",
                )
            return None
        try:
            payload = json.loads(result.stdout or "[]")
        except ValueError as exc:
            raise _runtime_unavailable(
                self.binary, container, "inspect",
                f"the inspect output did not parse as JSON: {exc}",
            ) from exc
        if not payload:
            return None
        entry = payload[0]
        config = entry.get("Config") or {}
        state = entry.get("State") or {}
        return {
            "id": entry.get("Id", ""),
            "labels": dict(config.get("Labels") or {}),
            "image": config.get("Image", ""),
            "running": bool(state.get("Running")),
            "exit_code": state.get("ExitCode"),
        }

    def run(self, spec: InstanceSpec) -> str:
        result = self._run(spec.run_argv(self.binary), env=spec.env)
        if result.returncode != 0:
            raise ChatInstanceError(
                f"{self.binary} run failed for deployment {spec.deployment!r}",
                data={
                    "reason": "container_run_failed",
                    "deployment": spec.deployment,
                    "container": spec.container,
                    "exit_code": result.returncode,
                    # The argv is safe to echo: it carries env var NAMES only.
                    "argv": spec.run_argv(self.binary),
                    "stderr": (result.stderr or "").strip()[:2000],
                },
                fix_hint=(
                    f"check that {self.binary} is running and that "
                    f"{spec.image} is pullable"
                    " (the default chat image is a placeholder: build "
                    "ops/openwebui/Dockerfile, set chat.image, and add its digest "
                    "to TESTED_IMAGES)"
                ),
            )
        return (result.stdout or "").strip()

    def _require_success(
        self, result: "subprocess.CompletedProcess[str]", *,
        container: str, verb: str,
    ) -> None:
        """A non-zero exit from stop/remove is the only evidence we get.

        Discarding it is what let :func:`tear_down` forget a record for a
        container that is still running, so it is raised. A container that was
        already gone is not a failure — the runtime says so by name.
        """
        if result.returncode == 0:
            return
        stderr = (result.stderr or "").strip()
        if any(marker in stderr.lower() for marker in _RUNTIME_ABSENT_MARKERS):
            return
        raise ChatInstanceError(
            f"{self.binary} {verb} failed for container {container!r}",
            data={
                "reason": f"container_{verb}_failed",
                "container": container,
                "binary": self.binary,
                "exit_code": result.returncode,
                "stderr": stderr[:2000],
            },
            fix_hint=(
                f"read the runtime's message above and remove the container by "
                f"hand ({self.binary} rm --force {container}) if it is still "
                f"there. The operator-state record is deliberately NOT "
                f"forgotten while a teardown is unproven"
            ),
        )

    def stop(self, container: str) -> None:
        self._require_success(
            self._run(
                [self.binary, "stop", "--time", "10", container],
                verb="stop", container=container,
            ),
            container=container, verb="stop",
        )

    def start(self, container: str) -> None:
        self._run([self.binary, "start", container], verb="start", container=container)

    def remove(self, container: str) -> None:
        self._require_success(
            self._run(
                [self.binary, "rm", "--force", container],
                verb="remove", container=container,
            ),
            container=container, verb="remove",
        )

    def execute(
        self, container: str, argv: Sequence[str],
    ) -> tuple[int, str, str]:
        result = self._run(
            [self.binary, "exec", container, *argv],
            verb="exec", container=container,
        )
        return result.returncode, result.stdout or "", result.stderr or ""

    def query(self, args: Sequence[str]) -> tuple[int, str, str]:
        """``<binary> <args>`` about the runtime itself (``--version``,
        ``info``): how :func:`resolve_network_mode` identifies it."""
        result = self._run([self.binary, *args], verb="query")
        return result.returncode, result.stdout or "", result.stderr or ""


# ---------------------------------------------------------------------------
# The liveness observation
# ---------------------------------------------------------------------------

#: The HTTP seam, so the supervision and status arms can be driven without a
#: container. Returns ``(status, body)``.
HttpGet = Callable[[str, float], "tuple[int, str]"]


def _default_http_get(url: str, timeout: float) -> tuple[int, str]:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return response.status, response.read().decode("utf-8", "replace")


def probe_version(
    base_url: str, *, http_get: HttpGet | None = None, timeout: float = 5.0,
) -> str | None:
    """``GET /api/version`` off the instance. None when it does not answer.

    The ONLY definition of "running" this module uses (F4). Deliberately
    returns None rather than raising for an unreachable instance: the status
    surface and the supervisor both need "did not answer" as a value, and an
    exception there would tempt a caller into treating its own launch record
    as the answer instead.
    """
    getter = http_get or _default_http_get
    try:
        status, body = getter(f"{base_url.rstrip('/')}{VERSION_PATH}", timeout)
    except (
        urllib.error.URLError,
        # Same hole as the MCP probe had: something bound to the port that
        # accepts a connection and does not speak HTTP raises BadStatusLine,
        # which is an HTTPException and not an OSError. Uncaught, the status
        # surface RAISES instead of reporting `down` — and "the port is held
        # by something that is not the instance" is exactly the case the
        # occupancy refusals exist for, so it is a case this probe meets.
        http.client.HTTPException,
        OSError,
        ValueError,
    ):
        return None
    if status != 200:
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    version = payload.get("version") if isinstance(payload, Mapping) else None
    return str(version) if version else None


def _await_version(
    base_url: str,
    *,
    http_get: HttpGet | None,
    timeout: float,
    interval: float,
    sleep: Callable[[float], None],
) -> str | None:
    deadline = time.monotonic() + timeout
    while True:
        version = probe_version(base_url, http_get=http_get)
        if version is not None:
            return version
        if time.monotonic() >= deadline:
            return None
        sleep(interval)


# ---------------------------------------------------------------------------
# The deployment's mcp-serve endpoint: observed and NAMED, never assumed
# ---------------------------------------------------------------------------

#: The endpoint's three observable states. Deliberately NOT folded into
#: :data:`STATE_UP` / :data:`STATE_DOWN`: this PACT defines the instance's
#: state as its own ``GET /api/version`` answer and nothing else, so a second
#: observation reports beside that one rather than mixing into it.
MCP_STATE_UP = "up"
MCP_STATE_DOWN = "down"

#: Nothing was asked, because no endpoint was named. The third state exists so
#: "we did not look" can never be recorded as "it is fine" — the whole failure
#: mode this observation was added for is a status surface that stayed silent
#: about a dead endpoint.
MCP_STATE_UNOBSERVED = "unobserved"

#: The condition an operator greps for. Named in the observation and in the
#: supervision event, because mcp-serve is NOT lifecycle-managed by this
#: module (see :func:`supervise_once`) and a condition nothing acts on must at
#: minimum be one something SAYS.
MCP_ENDPOINT_DEAD = "mcp_endpoint_dead"

#: What this observation is, stated wherever it is reported. A completed HTTP
#: exchange proves a listener is alive on the port; it does not prove
#: ``initialize`` plus ``tools/list`` succeeds, which is
#: ``okg chat sync``'s check (``chat/sync.py:3979-4043``) and needs the
#: credential named by ``chat.mcp.auth_token_env``.
MCP_OBSERVATION_KIND = "transport"


def mcp_endpoint_for(chat_block: Any, *, host: str = LOOPBACK_CANONICAL) -> str | None:
    """The deployment's mcp-serve endpoint, or ``None`` when none is declared.

    The same construction ``okg chat sync`` uses (``chat/sync.py:3124``), so
    the endpoint the status surface asks at is the endpoint the projection
    verb probes. ``None`` when ``chat.mcp.port`` is undeclared, which the
    caller reports as :data:`MCP_STATE_UNOBSERVED` — never as reachable.
    """
    mcp = getattr(chat_block, "mcp", None)
    port = getattr(mcp, "port", None) if mcp is not None else None
    if not port:
        return None
    return f"http://{host}:{int(port)}/mcp"


@dataclass(frozen=True)
class McpObservation:
    """One look at the deployment's mcp-serve endpoint.

    Reported alongside the instance's state on every status and supervision
    answer. The instance and its MCP endpoint are separate processes with
    separate lifetimes — the instance is a container this module runs, and
    mcp-serve today is a process an operator starts — so an instance that
    answers while its tool endpoint does not is an ordinary state, and one the
    person reading the status needs told.
    """

    state: str
    endpoint: str | None
    detail: str
    reason: str | None = None
    kind: str = MCP_OBSERVATION_KIND

    @property
    def dead(self) -> bool:
        return self.state == MCP_STATE_DOWN

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "endpoint": self.endpoint,
            "detail": self.detail,
            "reason": self.reason,
            "kind": self.kind,
        }


def probe_mcp_endpoint(
    endpoint: str | None,
    *,
    http_get: HttpGet | None = None,
    timeout: float = 5.0,
) -> McpObservation:
    """Ask the endpoint whether anything is listening, and say what was asked.

    ANY completed HTTP exchange counts as answering, including a 4xx. That is
    deliberate and it is why :data:`MCP_OBSERVATION_KIND` travels with the
    result: mcp-serve's streamable-http transport refuses a bare ``GET /mcp``
    on method and content negotiation, and an authenticated deployment refuses
    it on the credential — both are a live server declining a request, which is
    exactly the distinction from nothing being there. Whether the tools are
    actually reachable is ``okg chat sync``'s readback, and this observation
    says so rather than standing in for it.
    """
    if not endpoint:
        return McpObservation(
            state=MCP_STATE_UNOBSERVED,
            endpoint=None,
            detail=(
                "no mcp endpoint was named (chat.mcp.port undeclared), so "
                "nothing was asked. This is not a reachability result"
            ),
            reason="mcp_endpoint_undeclared",
        )
    getter = http_get or _default_http_get
    try:
        status, _body = getter(endpoint, timeout)
    except (urllib.error.HTTPError,) as exc:
        # An HTTP status IS an answer: something is listening and it spoke
        # HTTP. urlopen raises for 4xx/5xx, which would otherwise read as the
        # endpoint being dead.
        return McpObservation(
            state=MCP_STATE_UP,
            endpoint=endpoint,
            detail=(
                f"GET {endpoint} answered {exc.code}; a listener is present. "
                f"This is a {MCP_OBSERVATION_KIND} observation, not an "
                f"initialize + tools/list readback (that is `okg chat sync`)"
            ),
        )
    except (
        urllib.error.URLError,
        # NOT an OSError. A listener that accepts a connection and then does
        # not speak HTTP — a TLS socket, a stale non-HTTP process, a half-open
        # proxy — raises BadStatusLine or IncompleteRead, which are
        # HTTPException. Uncaught, the condition this observation exists to
        # NAME arrives instead as a traceback out of `okg chat-instance
        # status`, which is a worse outcome than the silence it replaced.
        http.client.HTTPException,
        OSError,
        ValueError,
    ) as exc:
        return McpObservation(
            state=MCP_STATE_DOWN,
            endpoint=endpoint,
            detail=(
                f"GET {endpoint} did not answer usably ({type(exc).__name__}). "
                "The chat instance's tools resolve through this endpoint, so "
                "the site can be up and every tool call still fail"
            ),
            reason=MCP_ENDPOINT_DEAD,
        )
    return McpObservation(
        state=MCP_STATE_UP,
        endpoint=endpoint,
        detail=(
            f"GET {endpoint} answered {status}; a listener is present. This is "
            f"a {MCP_OBSERVATION_KIND} observation, not an initialize + "
            f"tools/list readback (that is `okg chat sync`)"
        ),
    )


# ---------------------------------------------------------------------------
# Status — derived from the instance, never from the launch record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class InstanceStatus:
    """One observation. ``observed`` is False only when there was nothing to ask.

    ``state`` carries this PACT's words (``up``/``down``/``absent``);
    ``status`` carries the substrate's existing deployment vocabulary. Both
    are computed from ``version``, which came off the instance. There is no
    branch in this dataclass, or anywhere that builds it, that consults what
    the supervisor believes it launched.
    """

    deployment: str
    state: str
    version: str | None
    base_url: str | None
    container: str | None
    detail: str
    #: The deployment's mcp-serve endpoint, observed separately. It does NOT
    #: feed ``state``: the instance's state is its own version answer, per the
    #: PACT. It is reported because the instance answering while its tool
    #: endpoint does not is a real, ordinary condition, and one nobody used to
    #: be told about.
    mcp: McpObservation = field(
        default_factory=lambda: McpObservation(
            state=MCP_STATE_UNOBSERVED,
            endpoint=None,
            detail="no mcp endpoint was named, so nothing was asked",
            reason="mcp_endpoint_undeclared",
        ),
    )

    @property
    def status(self) -> str:
        return STATUS_VOCABULARY[self.state]

    @property
    def observed(self) -> bool:
        return self.base_url is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "state": self.state,
            "status": self.status,
            "version": self.version,
            "base_url": self.base_url,
            "container": self.container,
            "observed": self.observed,
            "detail": self.detail,
            "mcp": self.mcp.as_dict(),
        }


def instance_status(
    deployment: str,
    *,
    state_root: Path | str | None = None,
    http_get: HttpGet | None = None,
    record: InstanceRecord | None = None,
    mcp_endpoint: str | None = None,
    mcp_http_get: HttpGet | None = None,
) -> InstanceStatus:
    """Observe the instance and report. The record supplies only the address.

    A status surface that reports healthy while the instance answers nothing
    is the failure this requirement names, so the ONLY input to ``state`` is
    :func:`probe_version`. The operator-state record contributes the URL to
    ask at and nothing else.

    ``mcp_endpoint`` adds a SECOND, separately reported observation — see
    :func:`probe_mcp_endpoint`. It is deliberately not folded into ``state``:
    the instance is what this module's lifecycle owns, mcp-serve is a process
    it does not run, and collapsing the two would make ``up`` mean something
    different depending on which subsystem failed. Unnamed, it reports
    :data:`MCP_STATE_UNOBSERVED`, never reachable.
    """
    mcp = probe_mcp_endpoint(mcp_endpoint, http_get=mcp_http_get or http_get)
    record = record or read_instance(deployment, state_root=state_root)
    if record is None or not record.port:
        return InstanceStatus(
            deployment=deployment,
            state=STATE_ABSENT,
            version=None,
            base_url=None,
            container=None,
            detail=(
                "no instance is recorded for this deployment on this host; "
                "run `okg chat-instance up`"
            ),
            mcp=mcp,
        )
    base_url = instance_base_url(record.host, record.port)
    version = probe_version(base_url, http_get=http_get)
    if version is None:
        return InstanceStatus(
            deployment=deployment,
            state=STATE_DOWN,
            version=None,
            base_url=base_url,
            container=record.container,
            detail=f"GET {base_url}{VERSION_PATH} did not answer",
            mcp=mcp,
        )
    detail = f"GET {base_url}{VERSION_PATH} returned version {version}"
    if mcp.dead:
        # Said on the UP answer specifically. A dead tool endpoint behind a
        # site that loads is the shape this condition hides in: the person
        # reads `healthy`, opens the chat, and every tool call fails with
        # nothing anywhere having mentioned it.
        detail += (
            f"; the deployment's mcp endpoint {mcp.endpoint} did NOT answer "
            f"({MCP_ENDPOINT_DEAD}), so tool calls from this site will fail"
        )
    return InstanceStatus(
        deployment=deployment,
        state=STATE_UP,
        version=version,
        base_url=base_url,
        container=record.container,
        detail=detail,
        mcp=mcp,
    )


# ---------------------------------------------------------------------------
# Stand-up
# ---------------------------------------------------------------------------

#: Outcomes of a stand-up. ``already_running`` is what makes the verb
#: idempotent, and it is reached only after the instance ANSWERED.
OUTCOME_STARTED = "started"
OUTCOME_ALREADY_RUNNING = "already_running"


@dataclass(frozen=True)
class StandUpReport:
    """What one stand-up did, in terms a live-proof transcript can quote."""

    deployment: str
    outcome: str
    version: str
    base_url: str
    container: str
    port_plan: PortPlan
    app_database: AppDatabase
    image: str
    created_container: bool
    created_app_database_record: bool
    notes: tuple[str, ...] = ()
    #: Where the image came from, on the same principle the port records it:
    #: a report that showed only the ref could not tell a declared image from
    #: the default this program falls back to.
    image_source: str = IMAGE_SOURCE_DEFAULT_PIN
    network_mode: str = NETWORK_GATEWAY
    forwarded_ports: tuple[int, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "network_mode": self.network_mode,
            "forwarded_ports": list(self.forwarded_ports),
            "deployment": self.deployment,
            "outcome": self.outcome,
            "version": self.version,
            "base_url": self.base_url,
            "container": self.container,
            "image": self.image,
            "image_source": self.image_source,
            "port": self.port_plan.as_dict(),
            "app_database": self.app_database.as_dict(),
            "created_container": self.created_container,
            "created_app_database_record": self.created_app_database_record,
            "notes": list(self.notes),
        }


def _require_enabled_chat_block(deployment: str, chat_block: Any) -> None:
    if chat_block is None:
        raise ChatInstanceError(
            f"deployment {deployment!r} declares no chat: block",
            data={"reason": "no_chat_block", "deployment": deployment},
            fix_hint=(
                "write one with `okg chat setup --deployment "
                f"{deployment} --provider <openai|ollama|openai-compatible> "
                "--model <id> --instance-port <port>`. An instance is not "
                "stood up for a deployment that does not declare one"
            ),
        )
    if not getattr(chat_block, "enabled", False):
        raise ChatInstanceError(
            f"deployment {deployment!r} declares chat.enabled: false",
            data={"reason": "chat_block_disabled", "deployment": deployment},
            fix_hint=(
                "set chat.enabled: true in the deployment manifest. A "
                "disabled chat block is an explicit decision, not a gap to "
                "fill in"
            ),
        )


def _tear_down_after_failed_launch(
    runtime: "ContainerRuntime", container: str, created: bool,
) -> dict[str, Any]:
    """Remove a container we created, on the way to refusing about it.

    Returns what to put in the refusal's ``data`` rather than raising, because
    the operator needs the LAUNCH failure first: a removal that itself failed
    is a fact about the same run, not a replacement for it. Now that
    :meth:`ContainerRuntime.remove` raises, saying nothing here would have
    swapped "your instance never answered" for "docker rm failed".
    """
    if not created:
        return {"torn_down": False}
    try:
        runtime.remove(container)
    except ChatInstanceError as exc:
        return {"torn_down": False, "teardown_failed": exc.message}
    return {"torn_down": True}


def _refuse_foreign_container(
    deployment: str, container: str, facts: Mapping[str, Any],
) -> None:
    """Never adopt a container that belongs to someone else.

    Adoption is the failure mode that would put two deployments' chat state in
    one instance and one app database. The label is the observation; a name
    match is not.
    """
    labels = facts.get("labels") or {}
    owner = labels.get(LABEL_DEPLOYMENT)
    if owner == deployment:
        return
    raise ChatInstanceError(
        f"container {container!r} exists but is not this deployment's: its "
        f"{LABEL_DEPLOYMENT} label is {owner!r}, not {deployment!r}",
        data={
            "reason": "foreign_instance_not_adopted",
            "deployment": deployment,
            "container": container,
            "owner": owner,
        },
        fix_hint=(
            "remove or rename the existing container, or stand this "
            "deployment's instance up under a different name. An instance "
            "serving another deployment is never adopted: it holds that "
            "deployment's app database and its chat history"
        ),
    )


def _require_existing_image(
    deployment: str,
    container: str,
    facts: Mapping[str, Any],
    image_plan: ImagePlan,
) -> None:
    """Compare the observed image reference, independently of HTTP version."""
    observed = facts.get("image")
    measured = isinstance(observed, str) and bool(observed.strip())
    if measured and observed == image_plan.image:
        return
    raise ChatInstanceError(
        f"container {container!r}'s image reference "
        f"{'differs from' if measured else 'does not establish'} the selected "
        f"tested image {image_plan.image!r}",
        data={
            "reason": (
                "existing_instance_image_mismatch"
                if measured
                else "existing_instance_image_unmeasured"
            ),
            "deployment": deployment,
            "container": container,
            "expected_image": image_plan.image,
            "observed_image": observed if isinstance(observed, str) else None,
        },
        fix_hint=(
            "inspect the existing container and explicitly recreate it with "
            f"{image_plan.image} before retrying. Matching HTTP versions do "
            "not establish matching image references"
        ),
    )


def compose_instance_chat_block(
    chat_block: Any, install_plan: Path | str | None,
) -> Any:
    """The chat block with the installed bundle's contribution composed in.

    ``WEBUI_NAME`` must be the site name the plan and readback expect, and
    those read the COMPOSED block (``chat.sync.load_inputs``): a deployment
    whose chat block names no site takes the bundle's branding name. The
    composition is ``chat.bundle_composition.compose_bundle_chat`` itself,
    so the two cannot disagree. No install plan: the block is unchanged.
    """
    if install_plan is None or chat_block is None:
        return chat_block
    from okg.substrate.chat.bundle_composition import compose_bundle_chat
    from okg.substrate.chat.sync import load_bundle_chat

    selection = load_bundle_chat(install_plan)
    if selection is None:
        return chat_block
    composed, _record = compose_bundle_chat(chat_block, selection)
    return composed


def stand_up(
    deployment: str,
    *,
    chat_block: Any,
    graph_dsn: str | None,
    runtime: ContainerRuntime,
    environ: Mapping[str, str] | None = None,
    app_database_env: str = DEFAULT_APP_DATABASE_ENV,
    secret_env: str = DEFAULT_SECRET_KEY_ENV,
    instance_port_override: int | None = None,
    image: str | None = None,
    tested_images: Mapping[str, str] | None = None,
    host: str = LOOPBACK_CANONICAL,
    host_gateway_alias: str | None = "host.docker.internal",
    state_root: Path | str | None = None,
    reachability_probe: ReachabilityProbe | None = None,
    http_get: HttpGet | None = None,
    ready_timeout: float = 120.0,
    ready_interval: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    install_plan: Path | str | None = None,
    container_network: str = NETWORK_GATEWAY,
    extra_forward_ports: Sequence[int] = (),
    platform: str | None = None,
) -> StandUpReport:
    """Stand a deployment's instance up, or refuse naming the missing input.

    The ORDER of the checks is the requirement's post-state guarantee: every
    refusal that can be decided from inputs alone is decided before anything
    is created, so a refused run leaves no container, no app-database record
    and no listening socket. The two checks that can only fail after a launch
    — the instance never answering, and answering with the wrong version —
    tear the container down before raising, so the same post-state holds.

    ``install_plan`` composes the installed bundle into the block
    (:func:`compose_instance_chat_block`) and is recorded. It is never reused
    implicitly: without it the manifest's block is what the container is
    compared against, and the recorded plan is only NAMED in the
    ``instance_settings_changed`` refusal (#2875).

    ``container_network`` is ``auto``, ``gateway``, ``pasta`` or ``host``
    (:mod:`okg.substrate.chat.container_network`); ``auto`` picks by
    ``platform`` (default ``sys.platform``) and the runtime binary. The
    library default is ``gateway``; the CLI defaults to ``auto``.
    ``extra_forward_ports`` adds host-loopback ports the caller knows the
    container dials (setup's ``--mcp-probe-url``).
    """
    environ = os.environ if environ is None else environ
    network_mode, network_note = resolve_network_mode(
        container_network, runtime_binary=getattr(runtime, "binary", ""),
        platform=platform,
        # Runtimes without `query` (the hermetic ProcessRuntime) are
        # identified as unknown: `auto` then says it used the gateway mode.
        query=getattr(runtime, "query", None),
    )

    # 1-2. Inputs that are decidable from the manifest alone.
    _require_enabled_chat_block(deployment, chat_block)
    chat_block = compose_instance_chat_block(chat_block, install_plan)
    port_plan = resolve_instance_port(
        deployment, chat_block, override=instance_port_override,
    )
    # `image` is None-by-default rather than defaulted in the signature so
    # "the operator named one" stays distinguishable from "nobody did"; the
    # tested default is applied HERE, where it is recorded as a default.
    image_plan = resolve_instance_image(
        deployment, chat_block, override=image, tested_images=tested_images,
    )
    image = image_plan.image

    # 3-4. The app database, structurally isolated from the graph and peers.
    peers = peer_instances(exclude=deployment, state_root=state_root)
    app_database = resolve_app_database(
        deployment,
        env_var=app_database_env,
        environ=environ,
        graph_dsn=graph_dsn,
        peers=peers,
    )

    # 5. The launch spec, which refuses an unset secret and a set CUSTOM_NAME.
    spec = build_instance_spec(
        deployment,
        app_database=app_database,
        secret_env=secret_env,
        environ=environ,
        port_plan=port_plan,
        image=image,
        host=host,
        host_gateway_alias=host_gateway_alias,
        oidc_login=getattr(chat_block, "oidc_login", None),
        site_name=getattr(getattr(chat_block, "ui", None), "site_name", None),
        network_mode=network_mode,
        forward_ports=loopback_ports(
            chat_block,
            app_database_host=app_database.target.host,
            app_database_port=app_database.target.port,
            alias=host_gateway_alias,
            extra=extra_forward_ports,
        ),
    )
    if network_note:
        spec = replace(spec, notes=(network_note, *spec.notes))

    # 6. Idempotence, decided by an OBSERVATION and not by the state file.
    existing = runtime.inspect(spec.container)
    if existing is not None:
        _refuse_foreign_container(deployment, spec.container, existing)
        previous = read_instance(deployment, state_root=state_root)
        _refuse_changed_settings(
            deployment, spec, existing, install_plan=install_plan,
            recorded_install_plan=previous.install_plan if previous else None,
            recorded_network_mode=previous.network_mode if previous else None,
        )
        _require_existing_image(deployment, spec.container, existing, image_plan)
        running_version = probe_version(spec.base_url, http_get=http_get)
        if running_version is not None:
            _require_pinned_version(
                deployment, running_version, image_plan,
            )
            return StandUpReport(
                deployment=deployment,
                outcome=OUTCOME_ALREADY_RUNNING,
                version=running_version,
                base_url=spec.base_url,
                container=spec.container,
                port_plan=port_plan,
                app_database=app_database,
                image=image,
                image_source=image_plan.source,
                created_container=False,
                created_app_database_record=False,
                notes=spec.notes,
                network_mode=spec.network_mode,
                forwarded_ports=spec.forward_ports,
            )

    # 7. The network round trip, last of the refusals: an operator with a
    #    structural mistake AND an unreachable database hears the cheap one.
    require_app_database_reachable(
        app_database, deployment=deployment, probe=reachability_probe,
    )

    # 8. The port, probed on a real socket immediately before the bind. Both
    #    branches need it: a stopped container is not holding its port, so
    #    something else may have taken it while it was down, and a restart
    #    into an occupied port must refuse by name rather than fail obscurely
    #    inside the runtime.
    require_port_free(
        port_plan.instance_port, deployment=deployment, host=host, peers=peers,
    )
    created = False
    if existing is None:
        runtime.run(spec)
        created = True
    else:
        # Ours, present, but not answering: restart rather than duplicate.
        runtime.start(spec.container)

    version = _await_version(
        spec.base_url,
        http_get=http_get,
        timeout=ready_timeout,
        interval=ready_interval,
        sleep=sleep,
    )
    if version is None:
        raise ChatInstanceError(
            f"deployment {deployment!r}'s instance never answered GET "
            f"{spec.base_url}{VERSION_PATH} within {ready_timeout:.0f}s",
            data={
                "reason": "instance_never_answered",
                "deployment": deployment,
                "container": spec.container,
                "base_url": spec.base_url,
                **_tear_down_after_failed_launch(runtime, spec.container, created),
            },
            fix_hint=(
                f"read the container log ({spec.container}). A container that "
                "reports created is not a stood-up instance; only the "
                "version response is"
            ),
        )
    try:
        _require_pinned_version(deployment, version, image_plan)
    except ChatInstanceError as exc:
        exc.data.update(
            _tear_down_after_failed_launch(runtime, spec.container, created)
        )
        raise

    record = InstanceRecord(
        deployment=deployment,
        container=spec.container,
        host=host,
        port=port_plan.instance_port,
        image=image,
        app_database=app_database.target,
        port_source=port_plan.source,
        recorded_at=datetime.now(timezone.utc).isoformat(),
        install_plan=str(Path(install_plan).resolve()) if install_plan else None,
        network_mode=spec.network_mode,
        forwarded_ports=spec.forward_ports,
    )
    had_record = read_instance(deployment, state_root=state_root) is not None
    record_instance(record, state_root=state_root)
    return StandUpReport(
        deployment=deployment,
        outcome=OUTCOME_STARTED,
        version=version,
        base_url=spec.base_url,
        container=spec.container,
        port_plan=port_plan,
        app_database=app_database,
        image=image,
        image_source=image_plan.source,
        created_container=created,
        created_app_database_record=not had_record,
        notes=spec.notes,
        network_mode=spec.network_mode,
        forwarded_ports=spec.forward_ports,
    )


def _require_pinned_version(
    deployment: str, version: str, image_plan: ImagePlan,
) -> None:
    """The instance must answer the version its OWN image is tested at.

    Not :data:`PINNED_VERSION` unconditionally: once the tested set holds more
    than one entry, comparing every instance against the default pin would
    refuse a correctly-running second tested image. The expected version is
    looked up from the image actually launched, which is also the only way the
    check can catch the case it exists for -- a container answering something
    other than what the image it was started from is known to answer.
    """
    expected = image_plan.version
    if version == expected:
        return
    raise ChatInstanceError(
        f"deployment {deployment!r}'s instance answers version {version!r}, "
        f"not the {expected!r} its image is tested at",
        data={
            "reason": "version_not_pinned",
            "deployment": deployment,
            "observed": version,
            "expected": expected,
            "image": image_plan.image,
        },
        fix_hint=(
            f"every measured fact recorded for {image_plan.image} was taken "
            f"at {expected}. Pull that image, or re-record the evidence at "
            "the version you intend to run"
        ),
    )


def tear_down(
    deployment: str,
    *,
    runtime: ContainerRuntime,
    state_root: Path | str | None = None,
    port_release_timeout: float = DEFAULT_PORT_RELEASE_TIMEOUT,
    port_release_interval: float = 0.25,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Stop and remove the instance, and forget it — once that is PROVEN.

    When a container was removed, this also waits (up to
    ``port_release_timeout``) until its recorded port is free by the same
    probe ``stand_up`` refuses on, so an immediate ``up`` does not meet
    ``instance_port_conflict`` (issue #2876 item 3). A port still held at the
    deadline refuses by name and keeps the record, which names the holder.

    The app DATABASE is deliberately untouched: it holds the operator's chat
    history, and a lifecycle verb that drops it on teardown would make an
    ordinary restart destructive.

    The record is forgotten only after the container is observed gone. Three
    things had to change for that sentence to be true: :meth:`inspect` now
    refuses rather than answering "no such container" when it could not ask,
    :meth:`stop` and :meth:`remove` now raise on a non-zero exit, and the
    removal is CONFIRMED by a second inspect. Without them a stopped Docker
    daemon produced a clean ``removed: false``, the record was dropped, and
    the still-running instance became invisible to the next stand-up — which
    would then refuse on its port with no record explaining who held it.
    """
    record = read_instance(deployment, state_root=state_root)
    container = record.container if record else container_name_for(deployment)
    existed = runtime.inspect(container) is not None
    if existed:
        runtime.stop(container)
        runtime.remove(container)
        if runtime.inspect(container) is not None:
            raise ChatInstanceError(
                f"container {container!r} is still present after {deployment!r}'s "
                f"teardown reported success",
                data={
                    "reason": "container_still_present_after_teardown",
                    "deployment": deployment,
                    "container": container,
                },
                fix_hint=(
                    "remove the container by hand and run the command again. "
                    "The operator-state record is kept: forgetting it would "
                    "leave a running instance nothing knows about"
                ),
            )
    port_released: bool | None = None
    if existed and record is not None:
        deadline = now() + port_release_timeout
        while True:
            occupancy = port_occupancy(record.port, host=record.host)
            if occupancy == OCCUPANCY_FREE:
                port_released = True
                break
            if now() >= deadline:
                raise ChatInstanceError(
                    f"container {container!r} was removed, but port "
                    f"{record.port} on {record.host} is still {occupancy} after "
                    f"{port_release_timeout:.0f}s",
                    data={
                        "reason": "instance_port_not_released",
                        "deployment": deployment,
                        "container": container,
                        "port": record.port,
                        "host": record.host,
                        "occupancy": occupancy,
                        "timeout_seconds": port_release_timeout,
                    },
                    fix_hint=(
                        f"wait, or find what still holds port {record.port} "
                        "(a container runtime's network helper can linger "
                        "after removal); run okg chat-instance up only once "
                        "the port is free. The operator-state record is kept "
                        "until then; re-running down forgets it"
                    ),
                )
            sleep(port_release_interval)
    forget_instance(deployment, state_root=state_root)
    return {
        "deployment": deployment,
        "container": container,
        "removed": existed,
        "app_database_dropped": False,
        "port": record.port if record else None,
        "port_released": port_released,
    }


# ---------------------------------------------------------------------------
# Supervision
# ---------------------------------------------------------------------------

#: What one supervision pass did.
SUPERVISION_HEALTHY = "healthy"
SUPERVISION_RESTARTED = "restarted"
SUPERVISION_RESTART_FAILED = "restart_failed"
SUPERVISION_ABSENT = "absent"


@dataclass(frozen=True)
class SupervisionEvent:
    """One pass: what was observed before, what was done, what answered after."""

    deployment: str
    action: str
    before: InstanceStatus
    after: InstanceStatus

    @property
    def conditions(self) -> tuple[str, ...]:
        """Named conditions this pass observed and did NOT act on.

        Today that is exactly one — :data:`MCP_ENDPOINT_DEAD` — and it is a
        list rather than a flag because the useful property is the shape: a
        supervisor that reports ``healthy`` while something it does not manage
        is down should hand the reader the name of that thing, not an empty
        summary they have to disbelieve.
        """
        return (MCP_ENDPOINT_DEAD,) if self.after.mcp.dead else ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "action": self.action,
            "before": self.before.as_dict(),
            "after": self.after.as_dict(),
            "conditions": list(self.conditions),
            "unmanaged": ["mcp-serve"],
        }


def supervise_once(
    deployment: str,
    *,
    runtime: ContainerRuntime,
    state_root: Path | str | None = None,
    http_get: HttpGet | None = None,
    mcp_endpoint: str | None = None,
    mcp_http_get: HttpGet | None = None,
    restart_timeout: float = 120.0,
    restart_interval: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> SupervisionEvent:
    """Observe; restart if the instance does not answer; observe again.

    Both observations go through :func:`instance_status`, so the ``before``
    and ``after`` in the event are the same kind of evidence the status
    surface serves — a restart is never reported as recovered on the strength
    of the runtime having accepted a ``start``.

    What this supervisor does NOT do, stated because the event says ``healthy``
    and an operator will read that as "everything is fine": it does not manage
    mcp-serve. That process is not this module's to start, and on the
    deployment this PACT was proved against it is a session process an
    operator launched by hand. So a dead MCP endpoint is REPORTED — the event
    carries :data:`MCP_ENDPOINT_DEAD` and the ``after`` status carries the
    observation — and is never restarted, and never silently omitted either.
    """
    observe = dict(
        state_root=state_root,
        http_get=http_get,
        mcp_endpoint=mcp_endpoint,
        mcp_http_get=mcp_http_get,
    )
    before = instance_status(deployment, **observe)
    if before.state == STATE_ABSENT:
        return SupervisionEvent(
            deployment=deployment, action=SUPERVISION_ABSENT,
            before=before, after=before,
        )
    if before.state == STATE_UP:
        return SupervisionEvent(
            deployment=deployment, action=SUPERVISION_HEALTHY,
            before=before, after=before,
        )

    record = read_instance(deployment, state_root=state_root)
    container = record.container if record else container_name_for(deployment)
    runtime.start(container)
    version = _await_version(
        str(before.base_url),
        http_get=http_get,
        timeout=restart_timeout,
        interval=restart_interval,
        sleep=sleep,
    )
    after = instance_status(deployment, **observe)
    action = (
        SUPERVISION_RESTARTED
        if version is not None and after.state == STATE_UP
        else SUPERVISION_RESTART_FAILED
    )
    return SupervisionEvent(
        deployment=deployment, action=action, before=before, after=after,
    )

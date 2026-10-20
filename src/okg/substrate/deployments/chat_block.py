"""The typed ``chat:`` deployment-manifest block (PACT chat-manifest-block).

A deployment declares its governed multi-user chat site here: what the site
is called, what it looks like, which models it offers, what its system
prompt and starter questions are, which search profile scopes its answers,
which MCP tools it exposes, and which groups may see it. Per design.md §4
(``pact/changes/openwebui-chat-frontend/design.md``) the block is
*deployment* identity, not instance wiring: every install of a profile gets
the same site by construction.

Most of the block is still validation-only; its consumer is the sibling PACT
``chat-sync-projection``. The exception is ``chat.sso``, which
``okg.substrate.chat.sso`` reads at site bring-up (PACT
``chat-sso-integration``) to decide the deployment's identity model.

Three rules govern every line below.

**Reject, never guess.** An unknown key at any depth, a ``search_profile``
naming an undeclared profile, an ``mcp.tools`` entry outside the admitted
surface, or a ``models.default`` outside ``models.builtin`` raises at load.
No rule degrades to a default, to the first candidate, or to an unscoped
search.

**An absent declaration set is not permission.** Every membership test here
fails closed on its own absence case: a manifest with no ``search:`` block
refuses a ``search_profile`` rather than passing on an empty declared set;
the tool guard handed an explicitly empty admitted set refuses every tool
rather than admitting all of them; an empty ``models.builtin`` refuses every
default. An empty reference set fails every membership test and must never
read as data.

**No secrets.** Provider credentials, the instance admin key, and the admin
API endpoint are operator environment. The schema declares no
credential-bearing field, so a secret has no place to be written.
``mcp.auth_token_env`` carries an environment VARIABLE NAME for the
``mcp-http-auth`` sibling, and ``sso.client_secret_env`` does the same for
the identity provider's client secret — never a literal credential. Both are
exempt from the credential-shape refusal by their ``_env`` suffix, which IS
the indirection the rule exists to force, and both are refused when the
declared value is not a valid environment-variable name (the value is
withheld from the error, because the likeliest way to get one is pasting the
credential).

**Byte delivery is not a knob.** ``chat.css_bundle`` (PACT
``chat-css-bundle-contract``) is the one sub-block here whose contract is
delivery of FILE BYTES rather than application of a value. The census
measured that this vendor version exposes no way to be asked what its
styling is, so the declaration promises exactly one thing: the bytes named by
``ref`` are the bytes the instance serves. It is deliberately NOT a member of
``chat.ui`` — that field set is bounded by the census-admitted knob set, and
``chat.ui.theme`` stays refused by presence
(:func:`resolve_theme_accent`, :func:`_parse_ui`) precisely so nobody can
mistake the byte contract for the value knob the census refused.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.search_profiles import UnknownSearchProfile

from . import DeploymentConfigError


__all__ = [
    "CANDIDATE_UI_FEATURE_KEYS",
    "CANDIDATE_UI_KEYS",
    "CSS_BUNDLE_SUFFIX",
    "EXAMPLE_PINNED_IMAGE",
    "PINNED_IMAGE_REFERENCE",
    "ChatBlock",
    "ChatCssBundle",
    "ChatLiveTools",
    "ChatManifestError",
    "ChatMcpAudience",
    "ChatOidcLogin",
    "ChatSso",
    "ChatSsoError",
    "ChatUiCensusUnavailable",
    "ChatUnknownSearchProfile",
    "SHIPPED_UI_FEATURE_KEYS",
    "SHIPPED_UI_KEYS",
    "SSO_IDP_ELEMENTS",
    "SSO_MODES",
    "SSO_MODE_MULTI_USER",
    "SSO_MODE_SINGLE_OPERATOR",
    "UI_KNOB_CENSUS_FILE",
    "UI_KNOB_CENSUS_PINNED_VERSION",
    "VIZ_CHAT_ARCHIVE_COMMIT",
    "VIZ_CHAT_ARCHIVE_PATH",
    "VIZ_THEME_DECLARED",
    "VIZ_THEME_POLARITIES",
    "VIZ_THEME_POLARITY_DARK",
    "VIZ_THEME_POLARITY_LIGHT",
    "VIZ_THEME_SURFACE_FAMILY",
    "VIZ_THEME_TOKENS",
    "VIZ_THEME_UNDECLARED",
    "VIZ_THEME_UNRESOLVED",
    "VizChatRetired",
    "VizThemeIncoherent",
    "VizThemeInvalid",
    "VizThemeState",
    "load_ui_feature_census",
    "load_ui_knob_census",
    "parse_chat_block",
    "parse_viz_theme",
    "reject_retired_viz_chat",
    "resolve_theme_accent",
    "shipped_ui_field_set",
    "theme_color_luminance",
    "ui_default_model",
    "ui_knob_census_path",
    "theme_polarity",
    "validate_requested_tools",
    "viz_theme_undeclared",
    "viz_theme_unresolved",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ChatManifestError(DeploymentConfigError, SubstrateError):
    """A ``chat:`` block violates the manifest contract.

    Deliberately both a :class:`DeploymentConfigError` (so every existing
    manifest-load caller — notably ``deployment_lint._check_manifest_model``
    — catches it as the load-contract rejection it is) and a
    :class:`SubstrateError` (so it carries a stable wire ``code`` the CLI
    and tests can dispatch on).
    """

    code = "chat_manifest_invalid"


class ChatUnknownSearchProfile(ChatManifestError, UnknownSearchProfile):
    """``chat.search_profile`` names a profile the deployment does not declare.

    Same contract as :class:`~okg.substrate.mcp.search_profiles.UnknownSearchProfile`
    — including its ``unknown_search_profile`` code — because it is the same
    failure at a different moment: a typo'd profile must never degrade to the
    unscoped search or to ``search.default_profile``.
    """

    code = "unknown_search_profile"


class ChatSsoError(ChatManifestError):
    """The ``chat.sso`` block does not declare a usable identity model.

    A distinct code from ``chat_manifest_invalid`` because the operator
    action differs: an ordinary manifest violation is a typo, whereas every
    refusal raised under this code says the deployment has NOT decided
    whether it is single-operator or multi-user, or has decided multi-user
    and left the identity provider half-declared. Both would otherwise end
    with a site serving somebody.
    """

    code = "chat_sso_invalid"


class VizChatRetired(ChatManifestError):
    """The manifest declares ``viz.chat``, a sub-block that no longer exists.

    A distinct code from ``chat_manifest_invalid`` because the operator action
    differs: an ordinary manifest violation is a typo, whereas this is a
    deployment carrying configuration for a surface that was deleted. Left
    unchecked it would be the quietest possible failure — ``viz:`` is a known
    top-level key and nothing validates its sub-keys, so the block would load,
    validate, and configure nothing at all.
    """

    code = "viz_chat_retired"


class VizThemeInvalid(ChatManifestError):
    """A ``viz.theme`` token is not a value the console could render.

    A distinct code from ``chat_manifest_invalid`` because the operator action
    and the blast radius both differ. These tokens are served to the
    operator console and applied there as CSS custom properties, so a
    malformed one does not merely fail to configure something — it lands in a
    stylesheet. Normalizing it would be the quiet failure this module refuses
    everywhere else: the operator would see their color in the file and a
    different color, or none, on the screen.

    The refusal is deliberately scoped to the ``viz.theme`` token set. The
    rest of the untyped ``viz`` dict — ``port``, ``presentation``, the layout
    tuning — keeps its existing pass-through semantics, so typing the theme
    does not retroactively type its neighbors.
    """

    code = "viz_theme_invalid"


class VizThemeIncoherent(ChatManifestError):
    """Every ``viz.theme`` token is individually valid; the SET contradicts itself.

    A distinct code from :class:`VizThemeInvalid` because nothing here is
    malformed. ``background: "#121417"`` is a perfectly good color; it is a
    good color that, declared alone, paints a dark page ground behind cards
    and rails the manifest left at the console's light built-in values. The
    operator's edit is different too: :class:`VizThemeInvalid` says "that
    string is not a color", this says "you named the ground and not the
    surfaces standing on it".

    Introduced by the operator ruling of 2026-08-18 recorded in PACT
    ``operator-ui-deployment-theme``. The measured defect it closes: applying
    ``okg-workspace``'s dark declaration over the light default firing took
    the accessibility audit from 3 violations to 1102, 1099 of them
    ``color-contrast``, because the surface family was outside the token set.
    Widening the set makes a coherent declaration expressible; this refusal
    makes the incoherent one fail closed at load instead of rendering
    unreadable.
    """

    code = "viz_theme_incoherent"


class ChatUiCensusUnavailable(ChatManifestError):
    """A census-BOUND ``chat.ui`` knob was declared with no admitted set to check it against.

    The ``ui`` knob enumeration is bounded by the umbrella's recorded census
    artifact (``req.openwebui-chat-frontend.knob-census``): the design.md §4
    field list is the CANDIDATE set, the census is the ADMITTED set, and the
    shipped schema equals the census. Until that artifact exists there is no
    admitted set, so a census-bound knob is REFUSED rather than accepted and
    applied best-effort. Absence of the reference set is a distinct
    fail-closed state, never a membership test that passes vacuously.
    """

    code = "chat_ui_census_unavailable"


# ---------------------------------------------------------------------------
# The census-bounded ui knob set
# ---------------------------------------------------------------------------

#: The umbrella's recorded knob census: a file in the packaged
#: ``okg.substrate.chat.data`` directory (see :func:`ui_knob_census_path`), so
#: an installed okg with no repository checkout can read it. Produced by
#: ``task.openwebui-chat-frontend.read-handshake``; consumed here as the
#: ADMITTED set for :func:`shipped_ui_field_set`.
UI_KNOB_CENSUS_FILE = "ui-knob-census.json"

#: The pinned Open WebUI version the census must record. A census taken at any
#: other version proves nothing about the knobs this schema would drive. This
#: is design.md's program-wide pin (section 1); it was briefly a different
#: value here, which rejected the real census and refused every census-bound
#: knob for the wrong reason.
UI_KNOB_CENSUS_PINNED_VERSION = "0.11.4"

#: design.md §4's CANDIDATE knob list. Never the shipped schema — recorded so
#: the refusal message can say what a census would have to admit.
CANDIDATE_UI_KEYS: frozenset[str] = frozenset({
    "site_name",
    "logo_ref",
    "theme",
    "features",
    "banner",
})

#: The ``ui`` field set this schema PARSES, reconciled against the landed
#: census. It must equal the census-admitted set, and
#: ``test_ui_schema_field_set_equals_census_admitted_set`` enforces that.
#:
#: The correspondence is checked in CI, not at manifest-load time: the
#: census is a measurement record that ``okg chat plan``/``sync`` consult at
#: run time, while manifest validation must not depend on reading it. So the
#: artifact JUSTIFIES this constant and a test binds them; the parser reads
#: the constant.
#:
#: ``theme`` is deliberately ABSENT despite appearing in design.md's candidate
#: list. The census measured it NOT drivable at the pinned version: Open WebUI
#: v0.11.0 exposes no accent config key, env var, or admin API, and
#: ``theme.base`` is browser-local. Accepting a manifest field that provably
#: cannot be applied is precisely the silent no-op the census mechanism exists
#: to prevent, so the field is refused rather than accepted and ignored.
SHIPPED_UI_KEYS: frozenset[str] = frozenset({
    "site_name",
    "logo_ref",
    "banner",
    "features",
})

#: design.md §4's CANDIDATE feature list. The census NARROWS this; it never
#: widens it. ``memory`` is deliberately absent: the census measured it
#: drivable, but design.md records per-user memory as OFF — a governance
#: decision that ``record`` is the write-back path and a second ungoverned
#: memory store is not wanted. Drivable is a statement about the vendor;
#: admitted is a statement about the vendor AND this program.
CANDIDATE_UI_FEATURE_KEYS: frozenset[str] = frozenset({
    "context_compaction",
    "web_search",
    "image_generation",
    "code_interpreter",
    "arena",
    "channels",
    "byok",
})

#: The feature toggles ``chat.ui.features`` accepts: the candidate list
#: INTERSECTED with the census-measured drivable set, enforced against the
#: artifact by ``test_ui_feature_set_equals_candidate_intersect_census``.
#:
#: This is the same rule the top-level key set obeys — shipped = candidate ∩
#: admitted — where it happens to reduce to the census set because every
#: measured-drivable ``chat.ui`` knob is also a candidate.
SHIPPED_UI_FEATURE_KEYS: frozenset[str] = frozenset({
    "context_compaction",
    "web_search",
    "image_generation",
    "code_interpreter",
    "arena",
    "channels",
    "byok",
})


def ui_knob_census_path(data_dir: Path | None = None) -> Path:
    """Where the knob census is read from.

    ``data_dir`` defaults to the packaged ``okg.substrate.chat.data``
    directory; a test passes its own directory to exercise a different
    census.
    """
    if data_dir is None:
        from okg.substrate.chat.data import chat_data_dir

        data_dir = chat_data_dir()
    return Path(data_dir) / UI_KNOB_CENSUS_FILE


def _census_knob_entries(
    *, data_dir: Path | None = None,
) -> list[dict]:
    """Read the census artifact and return its validated ``knobs`` entries.

    Shared by the two derivations below so the artifact's read, version pin,
    and shape checks have exactly one implementation — two copies could drift
    into disagreeing about whether the same census is readable.
    """
    artifact = ui_knob_census_path(data_dir)
    if not artifact.is_file():
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census artifact does not exist: {artifact}. "
            "The ui schema is bounded by the recorded census (the ADMITTED "
            "set), not by design.md's candidate list "
            f"({sorted(CANDIDATE_UI_KEYS)}); with no census there is no "
            "admitted set, so census-bound knobs are refused.",
            data={"artifact": str(artifact), "reason": "missing"},
            fix_hint=(
                "record the census via "
                "task.openwebui-chat-frontend.read-handshake, then re-run"
            ),
        )
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census artifact is unreadable: {artifact} "
            f"({exc})",
            data={"artifact": str(artifact), "reason": "unreadable"},
            fix_hint="fix or re-record the census artifact",
        ) from exc
    if not isinstance(payload, dict):
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census artifact is not a JSON object: "
            f"{artifact}",
            data={"artifact": str(artifact), "reason": "not_a_mapping"},
            fix_hint="re-record the census artifact",
        )
    version = payload.get("openwebui_version")
    if version != UI_KNOB_CENSUS_PINNED_VERSION:
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census records openwebui_version {version!r}, "
            f"not the pin {UI_KNOB_CENSUS_PINNED_VERSION!r}: {artifact}. A "
            "census taken at another version proves nothing about the knobs "
            "this schema would drive.",
            data={
                "artifact": str(artifact),
                "reason": "version_mismatch",
                "observed": version,
                "expected": UI_KNOB_CENSUS_PINNED_VERSION,
            },
            fix_hint="re-record the census at the pinned version",
        )
    knobs = payload.get("knobs")
    if not isinstance(knobs, list) or not knobs:
        raise ChatUiCensusUnavailable(
            f"the chat.ui knob census has no 'knobs' list: {artifact}",
            data={"artifact": str(artifact), "reason": "no_knobs"},
            fix_hint="re-record the census artifact with a 'knobs' list",
        )
    for entry in knobs:
        if not isinstance(entry, dict):
            raise ChatUiCensusUnavailable(
                f"the chat.ui knob census has a non-object knob entry: "
                f"{artifact}",
                data={"artifact": str(artifact), "reason": "bad_knob_entry"},
                fix_hint="re-record the census artifact",
            )
    return knobs


def load_ui_knob_census(
    *, data_dir: Path | None = None,
) -> frozenset[str]:
    """Return the census-ADMITTED ``chat.ui`` knob set.

    Raises :class:`ChatUiCensusUnavailable` when the artifact is missing,
    unreadable, malformed, or records an ``openwebui_version`` other than the
    pin. Every one of those is the same fact — there is no admitted set — and
    each must refuse rather than fall back to :data:`CANDIDATE_UI_KEYS`.
    """
    # The artifact is a MEASUREMENT record — one entry per knob, carrying the
    # mechanism, the persistent-config finding, the readback, and a drivable
    # verdict. The admitted set is DERIVED from it here rather than written
    # into it, so the evidence stays a record of what was observed and this
    # module owns the one policy question: a knob is admitted only when the
    # census says it is drivable AND names the `chat.ui` field it drives.
    # Anything else — drivable but mapping elsewhere, or measured and found
    # not drivable — is simply absent, which is the refusal.
    admitted: set[str] = set()
    for entry in _census_knob_entries(data_dir=data_dir):
        if entry.get("drivable") is not True:
            continue
        field = entry.get("manifest_field")
        if not isinstance(field, str):
            continue
        head, _, rest = field.partition(".")
        section, _, leaf = rest.partition(".")
        if head != "chat" or section != "ui" or not leaf:
            continue
        # `features.web_search` is admitted as `features`: the schema's field
        # set is top-level `ui` keys, and a per-feature verdict admits the
        # container only when at least one feature under it is drivable.
        admitted.add(leaf.partition(".")[0])
    if not admitted:
        raise ChatUiCensusUnavailable(
            "the chat.ui knob census admits no chat.ui knob at all. Every "
            "measured knob was either not drivable or drives something other "
            "than chat.ui.",
            data={"reason": "empty_admitted_set"},
            fix_hint="re-record the census, or narrow the ui schema to nothing",
        )
    return frozenset(admitted)


def load_ui_feature_census(
    *, data_dir: Path | None = None,
) -> frozenset[str]:
    """Return the census-admitted ``chat.ui.features.*`` leaf names.

    :func:`load_ui_knob_census` collapses ``features.web_search`` to the
    container key ``features``; this reads the same artifact for the LEAF
    names, so the feature toggle set is bounded by measurement too. Without
    it, ``features`` would be an admitted container holding an unaudited
    vocabulary — the census would bound the door and not the room.
    """
    entries = _census_knob_entries(data_dir=data_dir)
    features: set[str] = set()
    for entry in entries:
        if entry.get("drivable") is not True:
            continue
        field = entry.get("manifest_field")
        if not isinstance(field, str):
            continue
        prefix, _, leaf = field.partition("chat.ui.features.")
        if prefix or not leaf:
            continue
        features.add(leaf.partition(".")[0])
    if not features:
        raise ChatUiCensusUnavailable(
            "the chat.ui knob census admits no chat.ui.features toggle at "
            "all: every measured feature was not drivable.",
            data={"reason": "empty_admitted_features"},
            fix_hint="re-record the census, or drop features from the schema",
        )
    return frozenset(features)


def shipped_ui_field_set() -> frozenset[str]:
    """The ``ui`` field set this schema accepts.

    Stated as a fact about the code so a test can bind it to the
    census-ADMITTED set read from the artifact. See :data:`SHIPPED_UI_KEYS`
    for why the binding is a test rather than a runtime read.
    """
    return SHIPPED_UI_KEYS


# ---------------------------------------------------------------------------
# Typed schema
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ChatUi:
    """Instance-level UI identity — the census-admitted knobs, and only those.

    There is no ``theme`` field. design.md §4 proposed one, but the census
    measured both ``theme.accent`` and ``theme.base`` as NOT drivable at the
    pinned version, so a theme declared here could never reach the running
    site. The field is refused rather than accepted-and-ignored; see
    :data:`SHIPPED_UI_KEYS`.
    """

    site_name: str | None = None
    logo_ref: str | None = None
    banner: str | None = None
    features: Mapping[str, bool] | None = None


#: The only extension the css bundle reference may carry. The bytes are
#: delivered to a path the vendor links as a STYLESHEET, so a declared ``.png``
#: or ``.js`` would be shipped and served under a content type it is not. The
#: suffix check is the one cheap way to catch a reference that was pointed at
#: the wrong file in the deployment repo.
CSS_BUNDLE_SUFFIX = ".css"


@dataclass(frozen=True)
class ChatCssBundle:
    """A BYTE-DELIVERY declaration. Not a knob, and not a styling value.

    ``ref`` names a deployment-owned file; ``approved_for_version`` names the
    Open WebUI version an operator last checked those bytes against. The whole
    contract this sub-block carries is: *the bytes named by ``ref`` are the
    bytes the instance serves at ``/static/custom.css``*. Nothing here is ever
    read back as a value — the recorded census measured that the instance
    exposes no way to be asked what it is styled like, which is exactly why
    this is a separate sub-block under its own name rather than a member of
    :class:`ChatUi`.

    ``approved_for_version`` is not decoration. The bytes override vendor
    build-generated class names that carry no cross-version compatibility
    guarantee, so a version bump invalidates the approval; editing this field
    after a visual re-check against the new pin IS the re-approval act, and it
    is versioned in the deployment repo like every other identity change.
    """

    ref: str
    approved_for_version: str


@dataclass(frozen=True)
class ChatModels:
    """The admin-curated model list for this deployment.

    ``default`` and ``ui_default`` answer two different questions, and
    #1183 was the cost of having only the first one.

    ``default`` is the PROVIDER model the deployment's preset runs on: it
    is passed as the preset row's ``base_model_id``, so it has to name a
    raw model the provider serves. It is not, and cannot be, the thing the
    browser opens on -- a raw provider model has no MCP tools bound and no
    system prompt, so a reader who opens the site and asks a domain
    question gets general knowledge with nothing saying the graph was
    never consulted.

    ``ui_default`` is the model the BROWSER pre-selects. Left unset -- the
    case for every deployment that has not thought about it -- the site
    opens on the preset ``okg chat sync`` just built, tools and grounding
    instructions included. It is declared only to opt OUT of that: to open
    a shared instance on a raw model on purpose. So the hazardous choice is
    the explicit one, and no deployment has to edit its manifest to be
    safe.

    Resolution lives in :func:`ui_default_model`, which both ``sync`` and
    ``chat plan`` call, so the plan and the applied state cannot hold two
    different opinions of the same field.
    """

    builtin: tuple[str, ...] = ()
    default: str | None = None
    ui_default: str | None = None


def ui_default_model(models: "ChatModels | None", *, preset_id: str) -> str:
    """The model id the browser pre-selects, resolved from the manifest.

    ``preset_id`` is the deployment's preset row -- the only configuration
    on the instance that carries the graph tools and the system prompt --
    and it is the answer whenever the manifest does not name another.

    Returns an id, never an empty string: the preset row is written on
    every run, so there is always something accessible to point at.
    """
    declared = models.ui_default if models is not None else None
    return declared or preset_id


@dataclass(frozen=True)
class ChatPreset:
    """The deployment's chat preset: prompt, description, starter questions."""

    display_name: str | None = None
    description: str | None = None
    system_prompt_ref: str | None = None
    suggestions: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChatMcpAudience:
    """One named audience and the tool set the deployment wants it to have.

    Declarable, and validated at load exactly like ``chat.mcp.tools`` — but
    NOT independently projectable. Open WebUI's per-tool filter
    (``function_name_filter_list``) is a property of the CONNECTION, not of a
    preset or a group, so one MCP registration serves exactly one tool set.
    Two audiences asking for DIFFERENT sets over one connection is refused at
    sync (PACT chat-tool-surface,
    scn.chat-tool-surface.two-audiences-need-two-connections); the remedy is
    two connections or two server processes. The key exists so that intent is
    stated and refused with its real reason, rather than dying as an unknown
    manifest key.
    """

    name: str
    tools: tuple[str, ...] = ()
    #: The live-tool names this audience is GRANTED, out of the closed set
    #: `chat.mcp.live_tools.names` allows. Absent means granted none, and
    #: that is the default: a live tool reaches an operational system, so
    #: an audience receives it only where the manifest says so by name.
    live_tools: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChatLiveTools:
    """The deployment's live-tool slot: a SECOND MCP connection, alone.

    A live tool reads a CURRENT external system rather than the published
    graph, so it carries a different authority from a graph operator and
    must never ride the graph connection. Open WebUI's per-tool filter is a
    property of the connection, so the only way one client can hold both
    authorities separably is two registrations: this block is the second
    one, with its own endpoint, its own bearer variable and its own filter.

    ``names`` is a CLOSED list the deployment allows. It is a ceiling, not a
    grant -- `chat.mcp.audiences.<n>.live_tools` does the granting, and with
    no audience naming one the connection is not registered at all.

    ``url`` is the endpoint the INSTANCE dials. It is instance topology, so
    it is a manifest value and never a bundle one: a package that could name
    a URL could point a reviewed chat site at a system nobody reviewed.
    """

    names: tuple[str, ...]
    url: str
    auth_token_env: str | None = None


@dataclass(frozen=True)
class ChatMcp:
    """The MCP registration the chat site presents.

    ``auth_token_env`` names an ENVIRONMENT VARIABLE for the ``mcp-http-auth``
    sibling. It is not, and structurally cannot be, a credential: a literal
    token would have to be written under some other key, and every other key
    is rejected.
    """

    port: int | None = None
    tools: tuple[str, ...] = ()
    auth_token_env: str | None = None
    audiences: tuple[ChatMcpAudience, ...] = ()
    live_tools: ChatLiveTools | None = None


@dataclass(frozen=True)
class ChatAccess:
    """Open WebUI group names that may see this deployment's site."""

    groups: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChatProvider:
    """The OpenAI-compatible model provider ``okg chat setup`` registers.

    ``base_url`` is the provider's API base as the INSTANCE reaches it (a
    container reaches a host-local server through ``host.docker.internal``,
    not ``127.0.0.1``). ``api_key_env`` names an environment VARIABLE holding
    the provider credential -- never the credential -- and is optional
    because a keyless local endpoint is a real configuration, not a gap.
    A declared-but-unset variable refuses at setup time; nothing here
    degrades to an empty key.

    ``tools`` says who runs the graph tools. ``instance`` (the default): the
    chat site runs them, and ``okg chat setup`` requires the site's own
    tool-result sources. ``provider``: the provider brings its own tools
    (``okg chat codex-shim`` runs Codex against the deployment's MCP server),
    the site's tool loop never runs, and setup instead reads the provider's
    own tool-call record -- reported as provider-reported evidence, never as
    the site's.
    """

    base_url: str
    api_key_env: str | None = None
    tools: str = "instance"


@dataclass(frozen=True)
class ChatDeepLinks:
    """Public base URLs for the two graphical surfaces. Identity, not secrets."""

    operator_ui_base: str | None = None
    chat_site_base: str | None = None


#: The deployment serves one operator through the instance's own local auth.
#: This is today's behavior and what design.md decision 9 calls "the honest
#: interim".
SSO_MODE_SINGLE_OPERATOR = "single_operator"

#: The deployment serves many people, each identified by the fronting SSO and
#: carried per-request to ``mcp-serve`` (PACT ``mcp-principal-mapping``).
SSO_MODE_MULTI_USER = "multi_user"

SSO_MODES: frozenset[str] = frozenset({
    SSO_MODE_SINGLE_OPERATOR,
    SSO_MODE_MULTI_USER,
})

#: Every element multi-user mode needs, in the order the refusal lists them.
#: This tuple IS the completeness contract: multi-user with any one of these
#: absent is refused by name, and single-operator with any one of them PRESENT
#: is refused too. There is no subset that means anything.
SSO_IDP_ELEMENTS: tuple[str, ...] = (
    "provider_id",
    "issuer",
    "audience",
    "discovery_endpoint",
    "client_id",
    "client_secret_env",
    "principal_header",
    "groups_claim",
    "admin_group",
    "user_group",
)


@dataclass(frozen=True)
class ChatSso:
    """The deployment's declared identity model for BOTH graphical surfaces.

    ``mode`` is the whole point of this dataclass. It is required whenever
    the block is present and it is never derived: a half-declared identity
    provider does not select single-operator mode, it refuses. See
    :func:`_parse_sso` for the three refusals that make that true.

    Nothing here is a credential. ``client_secret_env`` names an environment
    VARIABLE; the value is resolved at startup by
    :func:`okg.substrate.chat.sso.resolve_client_secret`, which fails closed
    and never echoes what it read.
    """

    mode: str
    provider_id: str | None = None
    issuer: str | None = None
    audience: str | None = None
    discovery_endpoint: str | None = None
    client_id: str | None = None
    client_secret_env: str | None = None
    principal_header: str | None = None
    groups_claim: str | None = None
    admin_group: str | None = None
    user_group: str | None = None

    @property
    def multi_user(self) -> bool:
        return self.mode == SSO_MODE_MULTI_USER

    @property
    def projected_groups(self) -> tuple[str, ...]:
        """The deployment's two groups, in the ONE vocabulary they have.

        These are the IdP group-claim values verbatim. They are also the
        Open WebUI group names and the tail of the Nomos subject
        (``subject:<group>``) — one projection, two consumers, no second
        vocabulary (design.md section 5 mechanism 4).
        """
        return tuple(
            name for name in (self.admin_group, self.user_group) if name
        )

    def to_dict(self) -> dict[str, Any]:
        """Serializable form. Carries no secret: only the variable NAME."""
        return {
            "mode": self.mode,
            **{key: getattr(self, key) for key in SSO_IDP_ELEMENTS},
        }


#: PACT chat-oidc-login. Every key ``chat.oidc_login`` admits, and the ones
#: it requires. Optional keys have a meaning when absent; required ones do not.
_OIDC_LOGIN_KEYS: frozenset[str] = frozenset({
    "issuer",
    "client_id",
    "client_secret_env",
    "roles_claim",
    "allowed_roles",
    "admin_roles",
    "provider_name",
    "redirect_uri",
    "merge_accounts_by_email",
})
_OIDC_LOGIN_REQUIRED: tuple[str, ...] = (
    "issuer",
    "client_id",
    "client_secret_env",
    "roles_claim",
    "allowed_roles",
    "admin_roles",
)


@dataclass(frozen=True)
class ChatOidcLogin:
    """The chat instance's own OIDC login (PACT chat-oidc-login).

    The instance signs a person in with the identity provider directly, maps
    the provider's roles onto its own user/admin roles, and forwards the
    person's token to ``mcp-serve``, which verifies it again itself (PACT
    mcp-oidc-auth). No fronting proxy vouches for anyone, which is why this
    and a multi-user ``chat.sso`` are refused together.

    Nothing here is a credential: ``client_secret_env`` names an environment
    VARIABLE, resolved when the instance is started.
    """

    issuer: str
    client_id: str
    client_secret_env: str
    roles_claim: str
    allowed_roles: tuple[str, ...]
    admin_roles: tuple[str, ...]
    provider_name: str | None = None
    redirect_uri: str | None = None
    #: Whether a first provider sign-in may attach to an existing local
    #: account with the same email. OFF unless declared: the pinned instance
    #: matches on the lowercased email alone and never consults
    #: `email_verified`, so with a provider that lets people set an
    #: unverified address this is an account takeover. Declare it only for a
    #: provider that verifies every email it issues.
    merge_accounts_by_email: bool = False

    @property
    def discovery_url(self) -> str:
        return f"{self.issuer.rstrip('/')}/.well-known/openid-configuration"

    def to_dict(self) -> dict[str, Any]:
        return {
            "issuer": self.issuer,
            "client_id": self.client_id,
            "client_secret_env": self.client_secret_env,
            "roles_claim": self.roles_claim,
            "allowed_roles": list(self.allowed_roles),
            "admin_roles": list(self.admin_roles),
            "provider_name": self.provider_name,
            "redirect_uri": self.redirect_uri,
            "merge_accounts_by_email": self.merge_accounts_by_email,
        }


@dataclass(frozen=True)
class ChatBlock:
    """The parsed ``chat:`` block."""

    enabled: bool = False
    ui: ChatUi | None = None
    #: The byte-delivery declaration, or None when the deployment declares
    #: none. ``None`` means NEVER DECLARED — a distinct state from a
    #: declaration that was removed, which only the sync side can tell apart
    #: (it holds the recorded ship state). Nothing here infers one from the
    #: other.
    css_bundle: ChatCssBundle | None = None
    models: ChatModels | None = None
    preset: ChatPreset | None = None
    search_profile: str | None = None
    mcp: ChatMcp | None = None
    access: ChatAccess | None = None
    #: The model provider ``okg chat setup`` registers on the instance, or
    #: None when the deployment relies on connections configured elsewhere.
    provider: ChatProvider | None = None
    deep_links: ChatDeepLinks | None = None
    sso: ChatSso | None = None
    oidc_login: ChatOidcLogin | None = None
    #: The vendor image the instance runs, PINNED (``:vX.Y.Z`` or
    #: ``@sha256:``). ``None`` means the manifest names none, which the
    #: instance layer reads as "use the version this program is proved
    #: against" -- a default it records as such, never as a declaration.
    #: Whether a declared image is one of the TESTED ones is not decided
    #: here: this file owns the shape, and the tested set lives beside the
    #: measurements in :mod:`okg.substrate.chat.instance`.
    image: str | None = None
    #: The host port the instance publishes on. ``None`` means undeclared,
    #: which stand-up refuses rather than defaulting.
    instance_port: int | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def accent(self) -> str | None:
        """Always None at the pinned version: chat carries no accent.

        Kept, and kept honest, rather than deleted: the shared-identity-token
        requirement is scoped-not-cancelled, and a caller asking a chat block
        for its accent deserves the measured answer — there is none — instead
        of an AttributeError that reads like a coding mistake. If a future
        census admits a drivable theme knob, this is where it reappears.
        """
        return None


# Key sets, per level. Strict at EVERY level rather than only at the top: the
# top-level census governs only the top-level key SET, so `display_nam:`
# nested under `preset:` would otherwise sail through and leave the site
# silently default-configured.
_CHAT_KEYS: frozenset[str] = frozenset({
    "enabled",
    "ui",
    "css_bundle",
    "models",
    "preset",
    "search_profile",
    "mcp",
    "access",
    "deep_links",
    "sso",
    "oidc_login",
    "image",
    "instance_port",
    "provider",
})
_SSO_KEYS: frozenset[str] = frozenset({"mode", *SSO_IDP_ELEMENTS})
#: Both keys are REQUIRED whenever the sub-block is present. A ``ref`` with no
#: approval version is unapproved bytes with nothing to invalidate them at the
#: next pin bump; an approval version with no ``ref`` states nothing at all.
_CSS_BUNDLE_KEYS: frozenset[str] = frozenset({"ref", "approved_for_version"})
_MODELS_KEYS: frozenset[str] = frozenset({"builtin", "default", "ui_default"})
_PRESET_KEYS: frozenset[str] = frozenset({
    "display_name",
    "description",
    "system_prompt_ref",
    "suggestions",
})
_MCP_KEYS: frozenset[str] = frozenset({
    "port", "tools", "auth_token_env", "audiences", "live_tools",
})
_MCP_AUDIENCE_KEYS: frozenset[str] = frozenset({"tools", "live_tools"})
_MCP_LIVE_TOOLS_KEYS: frozenset[str] = frozenset({
    "names", "url", "auth_token_env",
})
_ACCESS_KEYS: frozenset[str] = frozenset({"groups"})
_PROVIDER_KEYS: frozenset[str] = frozenset({"base_url", "api_key_env", "tools"})
#: Who runs the graph tools (``ChatProvider.tools``).
PROVIDER_TOOLS_INSTANCE = "instance"
PROVIDER_TOOLS_PROVIDER = "provider"
PROVIDER_TOOLS_VALUES: frozenset[str] = frozenset(
    {PROVIDER_TOOLS_INSTANCE, PROVIDER_TOOLS_PROVIDER}
)
_DEEP_LINKS_KEYS: frozenset[str] = frozenset({
    "operator_ui_base",
    "chat_site_base",
})

#: Sub-blocks an ENABLED chat must declare. The only minimal legal form is
#: ``enabled: false`` with no other keys; a hollow ``chat: {}`` or an
#: enabled block missing any of these raises rather than degrading into a
#: default-configured chat.
_REQUIRED_WHEN_ENABLED: tuple[str, ...] = (
    "ui",
    "models",
    "preset",
    "search_profile",
    "mcp",
)

#: Field names that would carry a credential. Asserted absent from every key
#: set above by the schema-census test, so "no secrets" is mechanical rather
#: than a promise.
_CREDENTIAL_SHAPED = re.compile(
    r"(api_key|apikey|secret|password|passwd|credential|admin_key|"
    r"access_key|private_key|(^|_)token($|_))",
    re.IGNORECASE,
)


def _is_credential_shaped(name: str) -> bool:
    """True when a field name would invite a literal secret.

    ``auth_token_env`` is exempt by construction: the ``_env`` suffix names an
    environment VARIABLE, which is the indirection the "no secrets" rule
    exists to force.
    """
    if name.endswith("_env"):
        return False
    return bool(_CREDENTIAL_SHAPED.search(name))


def schema_field_census() -> dict[str, frozenset[str]]:
    """Every key this schema accepts, by path. Used by the no-credential test."""
    return {
        "chat": _CHAT_KEYS,
        "chat.ui": SHIPPED_UI_KEYS,
        "chat.ui.features": SHIPPED_UI_FEATURE_KEYS,
        "chat.css_bundle": _CSS_BUNDLE_KEYS,
        "chat.models": _MODELS_KEYS,
        "chat.preset": _PRESET_KEYS,
        "chat.mcp": _MCP_KEYS,
        "chat.mcp.live_tools": _MCP_LIVE_TOOLS_KEYS,
        "chat.access": _ACCESS_KEYS,
        "chat.provider": _PROVIDER_KEYS,
        "chat.deep_links": _DEEP_LINKS_KEYS,
        "chat.sso": _SSO_KEYS,
    }


# ---------------------------------------------------------------------------
# Parse + validate
# ---------------------------------------------------------------------------

def _where(manifest_path: Path | None) -> str:
    return f" (Manifest: {manifest_path})" if manifest_path else ""


def _reject_unknown_keys(
    block: Mapping[str, Any],
    known: Iterable[str],
    *,
    path: str,
    manifest_path: Path | None,
) -> None:
    known_set = frozenset(known)
    unknown = sorted(set(block) - known_set)
    if not unknown:
        return
    keys_phrase = ", ".join(repr(k) for k in unknown)
    raise ChatManifestError(
        f"deployment.yaml has unknown key(s) {keys_phrase} at {path}. "
        f"Known keys at {path}: {sorted(known_set)}. A typo'd key is "
        f"silently ignored otherwise, leaving a default-configured chat "
        f"identity — fix the key.{_where(manifest_path)}",
        data={"path": path, "unknown": unknown, "known": sorted(known_set)},
        fix_hint=f"remove or correct {keys_phrase} under {path}",
    )


def _require_mapping(
    value: Any, *, path: str, manifest_path: Path | None,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ChatManifestError(
            f"deployment.yaml {path} must be a mapping; observed "
            f"{type(value).__name__}.{_where(manifest_path)}",
            data={"path": path, "observed": type(value).__name__},
            fix_hint=f"declare {path} as a mapping",
        )
    return value


def _string_list(
    value: Any, *, path: str, manifest_path: Path | None,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or isinstance(value, (str, bytes)):
        raise ChatManifestError(
            f"deployment.yaml {path} must be a list of strings; observed "
            f"{type(value).__name__}.{_where(manifest_path)}",
            data={"path": path, "observed": type(value).__name__},
            fix_hint=f"declare {path} as a list of strings",
        )
    out: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise ChatManifestError(
                f"deployment.yaml {path}[{index}] must be a non-empty "
                f"string; observed {item!r}.{_where(manifest_path)}",
                data={"path": f"{path}[{index}]", "observed": repr(item)},
                fix_hint=f"fix {path}[{index}]",
            )
        out.append(item)
    return tuple(out)


def _optional_string(
    block: Mapping[str, Any],
    key: str,
    *,
    path: str,
    manifest_path: Path | None,
) -> str | None:
    if key not in block:
        return None
    value = block[key]
    if not isinstance(value, str) or not value.strip():
        # Name the type, never the value: a credential pasted into the wrong
        # key (or YAML-parsed into a number or a mapping) would otherwise be
        # echoed into every error, --json and agent output.
        observed = "a blank string" if isinstance(value, str) else (
            f"a value of type {type(value).__name__}"
        )
        raise ChatManifestError(
            f"deployment.yaml {path}.{key} must be a non-empty string; "
            f"observed {observed}.{_where(manifest_path)}",
            data={"path": f"{path}.{key}", "observed": observed},
            fix_hint=f"fix {path}.{key}",
        )
    return value


def validate_requested_tools(
    requested: Iterable[str],
    *,
    admitted: Iterable[str],
    manifest_path: Path | None = None,
    path: str = "chat.mcp.tools",
) -> tuple[str, ...]:
    """Refuse any requested tool outside ``admitted``.

    ``admitted`` is a PARAMETER, not a module lookup, for two reasons. It
    makes the empty-admitted-set arm — where every request must be refused —
    exercisable without monkeypatching module constants (which are non-empty
    by construction, so the arm would otherwise be untestable). And it keeps
    the caller honest about WHICH surface it is checking against: this
    function checks the KNOWN tool universe resolvable at manifest load; the
    LIVE admitted surface is a launch-time property of the serving process,
    checked at sync time by ``tools/list`` readback.

    An unrecognized tool is never silently dropped from the registered set —
    a chat site that quietly lost its ``search`` tool answers from nothing.
    """
    admitted_set = frozenset(admitted)
    requested_tuple = tuple(requested)
    if not admitted_set:
        # An EMPTY admitted set is not "everything is allowed". It is the
        # absence of a reference set, and an absent reference set fails every
        # membership test. Refuse whatever was asked for, and say so.
        raise ChatManifestError(
            f"deployment.yaml {path} requests "
            f"{sorted(requested_tuple)} but the admitted tool surface is "
            f"EMPTY, so no tool can be admitted. An empty admitted set "
            f"refuses every tool rather than admitting all of "
            f"them.{_where(manifest_path)}",
            data={
                "path": path,
                "requested": sorted(requested_tuple),
                "admitted": [],
            },
            fix_hint="supply a non-empty admitted tool surface",
        )
    unknown = sorted(set(requested_tuple) - admitted_set)
    if unknown:
        raise ChatManifestError(
            f"deployment.yaml {path} names tool(s) "
            f"{unknown} outside the known tool universe "
            f"{sorted(admitted_set)}. An unrecognized tool is refused, never "
            f"dropped from the registered set.{_where(manifest_path)}",
            data={
                "path": path,
                "unknown": unknown,
                "admitted": sorted(admitted_set),
            },
            fix_hint=(
                f"remove {unknown} or use one of {sorted(admitted_set)}"
            ),
        )
    return requested_tuple


def _default_admitted_tools() -> tuple[str, ...]:
    """The KNOWN tool universe resolvable at manifest load.

    The AKMON read operators plus the query escape hatch — design.md §4's
    declared tool list exactly. Write and source-admin tools belong to the
    admin preset, whose surface is a launch-time property owned by the
    sync/source-admin siblings.
    """
    from okg.substrate.mcp.operators import (
        AKMON_READ_TOOLS,
        QUERY_ESCAPE_TOOLS,
    )

    return tuple(AKMON_READ_TOOLS) + tuple(QUERY_ESCAPE_TOOLS)


def _declared_search_profiles(raw_manifest: Mapping[str, Any]) -> list[str]:
    """Profile names declared under the manifest's ``search:`` block.

    A manifest with no ``search:`` block yields an EMPTY list — which then
    fails every membership test below. That is the point: absence of a
    declared set is not permission.
    """
    search = raw_manifest.get("search")
    if not isinstance(search, Mapping):
        return []
    profiles = search.get("profiles")
    if not isinstance(profiles, Mapping):
        return []
    return sorted(str(name) for name in profiles)


def _validate_search_profile(
    requested: str,
    *,
    raw_manifest: Mapping[str, Any],
    manifest_path: Path | None,
) -> None:
    declared = _declared_search_profiles(raw_manifest)
    if requested in declared:
        return
    detail = (
        f"declared profiles: {declared}"
        if declared
        else "the manifest declares no search: block at all, so the declared "
        "profile set is EMPTY and matches nothing"
    )
    raise ChatUnknownSearchProfile(
        f"deployment.yaml chat.search_profile names {requested!r}, which the "
        f"deployment does not declare — {detail}. The chat surface is never "
        f"configured with an unscoped search or with "
        f"search.default_profile.{_where(manifest_path)}",
        data={
            "path": "chat.search_profile",
            "requested": requested,
            "declared": declared,
        },
        fix_hint=(
            f"declare {requested!r} under search.profiles, or name one of "
            f"{declared}"
        ),
    )


def _parse_features(
    value: Any, *, manifest_path: Path | None,
) -> dict[str, bool]:
    """Parse ``chat.ui.features`` — an EXPLICIT toggle map, never inferred.

    Every key must be a census-measured drivable feature and every value must
    be a literal boolean. design.md calls this an "explicit ENABLE_*
    projection": a feature the manifest does not mention is left at the
    instance default deliberately, so a truthy string or a missing key must
    never be coerced into a decision the operator did not write.
    """
    block = _require_mapping(
        value, path="chat.ui.features", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block,
        SHIPPED_UI_FEATURE_KEYS,
        path="chat.ui.features",
        manifest_path=manifest_path,
    )
    out: dict[str, bool] = {}
    for name, flag in block.items():
        if not isinstance(flag, bool):
            raise ChatManifestError(
                f"deployment.yaml chat.ui.features.{name} must be a boolean; "
                f"observed {flag!r}. A feature toggle is an explicit "
                f"projection — a non-boolean would have to be guessed at."
                f"{_where(manifest_path)}",
                data={
                    "path": f"chat.ui.features.{name}",
                    "observed": repr(flag),
                },
                fix_hint=f"set chat.ui.features.{name} to true or false",
            )
        out[str(name)] = flag
    return out


def _parse_ui(
    value: Any,
    *,
    manifest_path: Path | None,
) -> ChatUi:
    """Parse ``chat.ui`` against the census-reconciled field set.

    The field set is :data:`SHIPPED_UI_KEYS`, a constant bound to the census
    artifact by test rather than read from it here — see that constant for
    why. Nothing about this function is census-path-dependent, so it takes no
    ``repo_root``: a parameter that was accepted and then ignored would be the
    very accept-then-ignore pattern this module refuses in manifests.
    """
    block = _require_mapping(
        value, path="chat.ui", manifest_path=manifest_path,
    )
    inadmissible = sorted(set(block) & (CANDIDATE_UI_KEYS - SHIPPED_UI_KEYS))
    if inadmissible:
        # A field design.md proposed that the census measured as NOT drivable.
        # It gets its own message because "unknown key" would read as a typo,
        # and this is the opposite: the operator wrote a real, documented
        # field that this version of Open WebUI cannot apply.
        raise ChatManifestError(
            f"deployment.yaml chat.ui declares {inadmissible}, which the "
            f"recorded knob census measured as NOT drivable at Open WebUI "
            f"{UI_KNOB_CENSUS_PINNED_VERSION}: there is no config key, env "
            f"var, or admin API for it, so a value declared here could never "
            f"reach the running site. Refused rather than accepted and "
            f"silently ignored.{_where(manifest_path)}",
            data={
                "path": "chat.ui",
                "inadmissible": inadmissible,
                "admitted": sorted(SHIPPED_UI_KEYS),
                "openwebui_version": UI_KNOB_CENSUS_PINNED_VERSION,
            },
            fix_hint=(
                f"remove {inadmissible} from chat.ui; the admitted knobs are "
                f"{sorted(SHIPPED_UI_KEYS)}"
            ),
        )
    _reject_unknown_keys(
        block, SHIPPED_UI_KEYS, path="chat.ui", manifest_path=manifest_path,
    )
    # `in block`, not truthiness: `features: {}` is a declared-but-empty toggle
    # map, which parses to {} and is distinguishable from an absent block. The
    # projection treats absent as "leave instance defaults alone"; conflating
    # the two would erase that distinction before anyone could act on it.
    features = (
        _parse_features(block["features"], manifest_path=manifest_path)
        if "features" in block
        else None
    )
    return ChatUi(
        site_name=_optional_string(
            block, "site_name", path="chat.ui", manifest_path=manifest_path,
        ),
        logo_ref=_optional_string(
            block, "logo_ref", path="chat.ui", manifest_path=manifest_path,
        ),
        banner=_optional_string(
            block, "banner", path="chat.ui", manifest_path=manifest_path,
        ),
        features=features,
    )


def _parse_css_bundle(
    value: Any, *, manifest_path: Path | None,
) -> ChatCssBundle:
    """Parse ``chat.css_bundle`` — a byte-delivery declaration, strictly.

    Four refusals, and each one closes a way the declaration could ship bytes
    nobody decided to ship.

    1. **An unknown key.** Same rule as every other level of this block: a
       typo'd ``approved_for_versoin`` would otherwise leave the pin gate
       unarmed while the reference still shipped bytes.
    2. **A missing key.** Both are required together; see
       :data:`_CSS_BUNDLE_KEYS`.
    3. **A reference that escapes the deployment directory.** ``ref`` is
       resolved against the deployment directory by the sync side, so an
       absolute path or a ``..`` segment would let a manifest name any file on
       the operator's machine and have its bytes served publicly by the chat
       site. Refused here, where the string is first read.
    4. **A reference that is not a stylesheet.** The bytes land at a path the
       vendor links as a stylesheet; a ``.png`` pointed at by mistake would be
       shipped and served all the same.
    """
    block = _require_mapping(
        value, path="chat.css_bundle", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block,
        _CSS_BUNDLE_KEYS,
        path="chat.css_bundle",
        manifest_path=manifest_path,
    )
    missing = sorted(_CSS_BUNDLE_KEYS - set(block))
    if missing:
        raise ChatManifestError(
            f"deployment.yaml chat.css_bundle declares no {missing}. The "
            f"reference and the approval version are declared together: a "
            f"reference with no approval version is bytes nothing can "
            f"invalidate at the next vendor pin bump, and an approval version "
            f"with no reference states nothing.{_where(manifest_path)}",
            data={
                "path": "chat.css_bundle",
                "missing": missing,
                "required": sorted(_CSS_BUNDLE_KEYS),
            },
            fix_hint=f"declare {missing} under chat.css_bundle",
        )
    ref = _optional_string(
        block, "ref", path="chat.css_bundle", manifest_path=manifest_path,
    )
    approved = _optional_string(
        block,
        "approved_for_version",
        path="chat.css_bundle",
        manifest_path=manifest_path,
    )
    # `_optional_string` has already refused a non-string and an empty one; the
    # asserts below are for the type checker, not a second validation.
    assert ref is not None and approved is not None
    candidate = PurePosixPath(ref)
    if candidate.is_absolute() or ".." in candidate.parts or ref.startswith("~"):
        raise ChatManifestError(
            f"deployment.yaml chat.css_bundle.ref is {ref!r}, which does not "
            f"stay inside the deployment directory. The reference is resolved "
            f"against that directory and its bytes are then served publicly by "
            f"the chat site, so an absolute path or a '..' segment would "
            f"publish a file the deployment does not "
            f"own.{_where(manifest_path)}",
            data={"path": "chat.css_bundle.ref", "observed": ref},
            fix_hint=(
                "name a path relative to the deployment directory, e.g. "
                "chat/custom.css"
            ),
        )
    if candidate.suffix.lower() != CSS_BUNDLE_SUFFIX:
        raise ChatManifestError(
            f"deployment.yaml chat.css_bundle.ref is {ref!r}, which does not "
            f"end in {CSS_BUNDLE_SUFFIX!r}. The bytes are delivered to a path "
            f"the vendor links as a stylesheet, so a reference pointed at "
            f"another kind of file would be shipped and served all the "
            f"same.{_where(manifest_path)}",
            data={
                "path": "chat.css_bundle.ref",
                "observed": ref,
                "required_suffix": CSS_BUNDLE_SUFFIX,
            },
            fix_hint=f"point chat.css_bundle.ref at a {CSS_BUNDLE_SUFFIX} file",
        )
    return ChatCssBundle(ref=ref, approved_for_version=approved)


def _parse_models(
    value: Any, *, manifest_path: Path | None,
) -> ChatModels:
    block = _require_mapping(
        value, path="chat.models", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block, _MODELS_KEYS, path="chat.models", manifest_path=manifest_path,
    )
    has_builtin = "builtin" in block
    builtin = (
        _string_list(
            block["builtin"],
            path="chat.models.builtin",
            manifest_path=manifest_path,
        )
        if has_builtin
        else ()
    )
    default = _optional_string(
        block, "default", path="chat.models", manifest_path=manifest_path,
    )
    ui_default = _optional_string(
        block, "ui_default", path="chat.models", manifest_path=manifest_path,
    )
    if ui_default is not None and ui_default not in builtin:
        # The field exists to name a RAW model deliberately, so the only
        # values it may carry are the declared built-ins. The preset is
        # reached by leaving the key out, never by typing an id: the preset
        # id is derived from the deployment name, and a manifest that spelt
        # it by hand would silently stop selecting anything the day that
        # derivation changed.
        raise ChatManifestError(
            f"deployment.yaml chat.models.ui_default is {ui_default!r}, "
            f"which chat.models.builtin ({list(builtin)}) does not declare. "
            "This field names the model the browser pre-selects instead of "
            "the deployment's preset, so it may only name a built-in; an "
            "unknown id would open the site on nothing."
            f"{_where(manifest_path)}",
            data={
                "path": "chat.models.ui_default",
                "requested": ui_default,
                "builtin": list(builtin),
            },
            fix_hint=(
                "remove chat.models.ui_default to open on the deployment's "
                f"preset, or name one of {list(builtin)}"
            ),
        )
    if default is not None and default not in builtin:
        # Both absence arms land here on purpose, each with its own reason:
        # an ABSENT builtin key and an EMPTY builtin list are different
        # mistakes, and neither may fall back to the first built-in or to the
        # instance default.
        if not has_builtin:
            reason = (
                "chat.models declares no builtin key at all, so the "
                "built-in set is EMPTY and contains no default"
            )
        elif not builtin:
            reason = (
                "chat.models.builtin is an empty list, so it contains no "
                "default — an empty declared set refuses every default"
            )
        else:
            reason = f"chat.models.builtin declares {list(builtin)}"
        raise ChatManifestError(
            f"deployment.yaml chat.models.default is {default!r} but "
            f"{reason}. The default is never resolved to the first built-in "
            f"or to the instance default.{_where(manifest_path)}",
            data={
                "path": "chat.models.default",
                "requested": default,
                "builtin": list(builtin),
            },
            fix_hint=(
                f"add {default!r} to chat.models.builtin, or name one of "
                f"{list(builtin)}"
            ),
        )
    return ChatModels(builtin=builtin, default=default, ui_default=ui_default)


def _parse_preset(
    value: Any, *, manifest_path: Path | None,
) -> ChatPreset:
    block = _require_mapping(
        value, path="chat.preset", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block, _PRESET_KEYS, path="chat.preset", manifest_path=manifest_path,
    )
    suggestions = (
        _string_list(
            block["suggestions"],
            path="chat.preset.suggestions",
            manifest_path=manifest_path,
        )
        if "suggestions" in block
        else ()
    )
    return ChatPreset(
        display_name=_optional_string(
            block,
            "display_name",
            path="chat.preset",
            manifest_path=manifest_path,
        ),
        description=_optional_string(
            block,
            "description",
            path="chat.preset",
            manifest_path=manifest_path,
        ),
        system_prompt_ref=_optional_string(
            block,
            "system_prompt_ref",
            path="chat.preset",
            manifest_path=manifest_path,
        ),
        suggestions=suggestions,
    )


def _surface_declares_chat_tools(raw_manifest: Mapping[str, Any]) -> bool:
    """True when ``agent_surface.projections.chat.tools`` is declared.

    Read off the RAW manifest rather than the parsed surface: this module is
    imported by the surface parser's own package, and the chat block is
    parsed on the same pass. Shape only — the surface parser owns validation
    and has already refused a malformed block by the time a legal one reaches
    a consumer.
    """
    surface = raw_manifest.get("agent_surface")
    if not isinstance(surface, Mapping):
        return False
    projections = surface.get("projections")
    if not isinstance(projections, Mapping):
        return False
    chat = projections.get("chat")
    return isinstance(chat, Mapping) and chat.get("tools") is not None


def _surface_declares_chat_prompt(raw_manifest: Mapping[str, Any]) -> bool:
    surface = raw_manifest.get("agent_surface")
    if not isinstance(surface, Mapping):
        return False
    projections = surface.get("projections")
    if not isinstance(projections, Mapping):
        return False
    chat = projections.get("chat")
    return (
        isinstance(chat, Mapping)
        and chat.get("system_prompt_ref") is not None
    )


def _parse_mcp(
    value: Any,
    *,
    manifest_path: Path | None,
    admitted_tools: Iterable[str] | None,
    surface_declares_tools: bool = False,
) -> ChatMcp:
    """Parse ``chat.mcp``.

    ``surface_declares_tools`` says an ``agent_surface.projections.chat``
    block already states this client's tool set (PACT
    ``effective-surface-declaration``). The key then moves from REQUIRED to
    REFUSED rather than merely becoming optional: a manifest that declared
    both would make the projected set depend on which loader asked, which is
    the drift the block exists to remove.
    """
    block = _require_mapping(
        value, path="chat.mcp", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block, _MCP_KEYS, path="chat.mcp", manifest_path=manifest_path,
    )
    port = block.get("port")
    if port is not None and (
        isinstance(port, bool)
        or not isinstance(port, int)
        or not (1 <= port <= 65535)
    ):
        raise ChatManifestError(
            f"deployment.yaml chat.mcp.port must be an integer in 1..65535; "
            f"observed {port!r}.{_where(manifest_path)}",
            data={"path": "chat.mcp.port", "observed": repr(port)},
            fix_hint="set chat.mcp.port to a valid port number",
        )
    if surface_declares_tools:
        if "tools" in block:
            raise ChatManifestError(
                f"deployment.yaml declares chat.mcp.tools AND "
                f"agent_surface.projections.chat.tools. Refusing rather than "
                f"merging or preferring one: the agent_surface block exists "
                f"so that ONE place states what a client is given, and a "
                f"manifest holding both would make the projected set depend "
                f"on which loader asked.{_where(manifest_path)}",
                data={
                    "path": "chat.mcp.tools",
                    "replacement": "agent_surface.projections.chat.tools",
                },
                fix_hint=(
                    "delete chat.mcp.tools; "
                    "agent_surface.projections.chat.tools is the declaration"
                ),
            )
        tools: tuple[str, ...] = ()
    else:
        if "tools" not in block:
            raise ChatManifestError(
                f"deployment.yaml chat.mcp declares no tools. A chat MCP "
                f"registration with no declared tool set is hollow — it would "
                f"answer from nothing.{_where(manifest_path)}",
                data={"path": "chat.mcp.tools", "observed": "absent"},
                fix_hint=(
                    "declare chat.mcp.tools with at least one tool, or "
                    "declare agent_surface.projections.chat.tools"
                ),
            )
        tools = _string_list(
            block["tools"], path="chat.mcp.tools", manifest_path=manifest_path,
        )
        if not tools:
            raise ChatManifestError(
                f"deployment.yaml chat.mcp.tools is empty. A chat MCP "
                f"registration with no tools is hollow — it would answer from "
                f"nothing.{_where(manifest_path)}",
                data={"path": "chat.mcp.tools", "observed": []},
                fix_hint="declare at least one tool under chat.mcp.tools",
            )
        validate_requested_tools(
            tools,
            admitted=(
                admitted_tools
                if admitted_tools is not None
                else _default_admitted_tools()
            ),
            manifest_path=manifest_path,
        )
    return ChatMcp(
        port=port,
        tools=tools,
        auth_token_env=_optional_string(
            block,
            "auth_token_env",
            path="chat.mcp",
            manifest_path=manifest_path,
        ),
        audiences=_parse_mcp_audiences(
            block.get("audiences"),
            manifest_path=manifest_path,
            admitted_tools=admitted_tools,
        ),
        live_tools=_parse_live_tools(
            block.get("live_tools"), manifest_path=manifest_path,
        ),
    )


def _parse_live_tools(
    value: Any, *, manifest_path: Path | None,
) -> ChatLiveTools | None:
    """Parse ``chat.mcp.live_tools``. SHAPE only.

    Absent yields ``None`` -- the deployment declares no live-tool slot, and
    that is the default state, not a degraded one. Whether the names are
    allowed, whether they collide with a graph operator and whether any
    audience may have them are decided in
    :mod:`okg.substrate.chat.live_tool_slot`, which is the one site all
    three verbs read; deciding half of it here would give plan and sync two
    opinions of one slot.
    """
    if value is None:
        return None
    block = _require_mapping(
        value, path="chat.mcp.live_tools", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block, _MCP_LIVE_TOOLS_KEYS, path="chat.mcp.live_tools",
        manifest_path=manifest_path,
    )
    names = _string_list(
        block.get("names"), path="chat.mcp.live_tools.names",
        manifest_path=manifest_path,
    )
    if not names:
        raise ChatManifestError(
            f"deployment.yaml chat.mcp.live_tools declares no names. An empty "
            f"live-tool slot is an unresolved intent; omit the key to declare "
            f"no live tools.{_where(manifest_path)}",
            data={"path": "chat.mcp.live_tools.names", "observed": list(names)},
            fix_hint="remove chat.mcp.live_tools, or name at least one live tool",
        )
    url = _optional_string(
        block, "url", path="chat.mcp.live_tools", manifest_path=manifest_path,
    )
    if not url or not str(url).startswith(("http://", "https://")):
        raise ChatManifestError(
            f"deployment.yaml chat.mcp.live_tools.url must be an http(s) URL "
            f"the INSTANCE can dial; observed {url!r}. The slot binds names to "
            f"an endpoint, and a slot with no endpoint registers a connection "
            f"that reaches nothing.{_where(manifest_path)}",
            data={"path": "chat.mcp.live_tools.url", "observed": repr(url)},
            fix_hint="set chat.mcp.live_tools.url to the live tool server's URL",
        )
    return ChatLiveTools(
        names=names,
        url=str(url),
        auth_token_env=_optional_string(
            block, "auth_token_env", path="chat.mcp.live_tools",
            manifest_path=manifest_path,
        ),
    )


def _parse_mcp_audiences(
    value: Any,
    *,
    manifest_path: Path | None,
    admitted_tools: Iterable[str] | None,
) -> tuple[ChatMcpAudience, ...]:
    """Parse ``chat.mcp.audiences``, validating each tool set like the main one.

    Absent yields ``()``. Present-but-empty REFUSES: an empty audiences map is
    an unresolved intent, not a statement that there are no audiences — the
    way to say that is to omit the key.

    Whether the declared audiences can be SERVED is not decided here. That is
    a property of the connection, and it is decided at sync.
    """
    if value is None:
        return ()
    block = _require_mapping(
        value, path="chat.mcp.audiences", manifest_path=manifest_path,
    )
    if not block:
        raise ChatManifestError(
            f"deployment.yaml chat.mcp.audiences is empty. An empty audience "
            f"map is an unresolved intent; omit the key to declare no "
            f"audiences.{_where(manifest_path)}",
            data={"path": "chat.mcp.audiences", "observed": {}},
            fix_hint="remove chat.mcp.audiences, or declare at least one audience",
        )
    audiences: list[ChatMcpAudience] = []
    for name in sorted(block):
        path = f"chat.mcp.audiences.{name}"
        body = _require_mapping(
            block[name], path=path, manifest_path=manifest_path,
        )
        _reject_unknown_keys(
            body, _MCP_AUDIENCE_KEYS, path=path, manifest_path=manifest_path,
        )
        if "tools" not in body:
            raise ChatManifestError(
                f"deployment.yaml {path} declares no tools. An audience that "
                f"names no tool set states nothing.{_where(manifest_path)}",
                data={"path": f"{path}.tools", "observed": "absent"},
                fix_hint=f"declare {path}.tools",
            )
        tools = _string_list(
            body["tools"], path=f"{path}.tools", manifest_path=manifest_path,
        )
        if not tools:
            raise ChatManifestError(
                f"deployment.yaml {path}.tools is empty. An empty declared "
                f"set is never treated as permission, and an audience that "
                f"gets nothing is declared by omitting the "
                f"audience.{_where(manifest_path)}",
                data={"path": f"{path}.tools", "observed": []},
                fix_hint=f"declare at least one tool under {path}.tools",
            )
        validate_requested_tools(
            tools,
            admitted=(
                admitted_tools
                if admitted_tools is not None
                else _default_admitted_tools()
            ),
            manifest_path=manifest_path,
            path=f"{path}.tools",
        )
        granted: tuple[str, ...] = ()
        if "live_tools" in body:
            granted = _string_list(
                body["live_tools"], path=f"{path}.live_tools",
                manifest_path=manifest_path,
            )
            if not granted:
                raise ChatManifestError(
                    f"deployment.yaml {path}.live_tools is empty. An empty "
                    f"grant is never permission; an audience that receives no "
                    f"live tool is declared by omitting the "
                    f"key.{_where(manifest_path)}",
                    data={"path": f"{path}.live_tools", "observed": []},
                    fix_hint=f"omit {path}.live_tools, or name a live tool",
                )
        audiences.append(
            ChatMcpAudience(name=str(name), tools=tools, live_tools=granted)
        )
    return tuple(audiences)


def _parse_access(
    value: Any, *, manifest_path: Path | None,
) -> ChatAccess:
    block = _require_mapping(
        value, path="chat.access", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block, _ACCESS_KEYS, path="chat.access", manifest_path=manifest_path,
    )
    groups = (
        _string_list(
            block["groups"],
            path="chat.access.groups",
            manifest_path=manifest_path,
        )
        if "groups" in block
        else ()
    )
    return ChatAccess(groups=groups)


def _parse_sso(value: Any, *, manifest_path: Path | None) -> ChatSso:
    """Parse ``chat.sso``: the deployment's identity-model declaration.

    Four refusals, and each one closes a way a deployment could end up
    serving somebody it never decided to serve.

    1. **No ``mode``.** The mode is DECLARED, never derived. Inferring it
       from which IdP elements happen to be present is the exact defect the
       requirement names: a manifest that lost its discovery endpoint in a
       bad merge would quietly become single-operator and keep serving,
       with every user collapsed onto one principal. The message therefore
       lists which elements ARE declared, so the operator can see what the
       inference would have been and why it was refused instead.
    2. **``multi_user`` with an incomplete provider.** Refused by NAME, all
       missing elements at once — an operator fixing them one refusal at a
       time learns the shape of the block one restart at a time.
    3. **``single_operator`` with IdP elements declared.** The mirror of
       refusal 2, and the one that actually stops the silent downgrade: a
       manifest carrying a client id, an issuer and a discovery endpoint
       under ``single_operator`` is either a mode that was never flipped or
       an SSO that was never removed. Reading it as "single-operator, and
       that configuration is dead" is a decision nobody made.
    4. **A ``client_secret_env`` that is not a variable name.** The likeliest
       way to land there is pasting the secret itself, so the value is
       withheld from the message and the payload.
    """
    block = _require_mapping(value, path="chat.sso", manifest_path=manifest_path)
    _reject_unknown_keys(
        block, _SSO_KEYS, path="chat.sso", manifest_path=manifest_path,
    )

    declared = sorted(key for key in SSO_IDP_ELEMENTS if key in block)
    if "mode" not in block:
        raise ChatSsoError(
            f"deployment.yaml chat.sso declares no 'mode'. The identity "
            f"model is an explicit declaration, never inferred from which "
            f"identity-provider elements happen to be present: this block "
            f"declares {declared or 'none of them'}, and neither that set "
            f"nor its complement selects a mode. Declare mode: "
            f"{sorted(SSO_MODES)}.{_where(manifest_path)}",
            data={
                "path": "chat.sso.mode",
                "observed": "absent",
                "declared_idp_elements": declared,
                "modes": sorted(SSO_MODES),
            },
            fix_hint=(
                f"declare chat.sso.mode: {SSO_MODE_MULTI_USER} (and every "
                f"element of {list(SSO_IDP_ELEMENTS)}), or chat.sso.mode: "
                f"{SSO_MODE_SINGLE_OPERATOR} with no identity-provider keys"
            ),
        )
    mode = block["mode"]
    if not isinstance(mode, str) or mode not in SSO_MODES:
        raise ChatSsoError(
            f"deployment.yaml chat.sso.mode must be one of "
            f"{sorted(SSO_MODES)}; observed {mode!r}. An unrecognized mode "
            f"is refused, never resolved to the safer-looking "
            f"one.{_where(manifest_path)}",
            data={
                "path": "chat.sso.mode",
                "observed": repr(mode),
                "modes": sorted(SSO_MODES),
            },
            fix_hint=f"set chat.sso.mode to one of {sorted(SSO_MODES)}",
        )

    values = {
        key: _optional_string(
            block, key, path="chat.sso", manifest_path=manifest_path,
        )
        for key in SSO_IDP_ELEMENTS
    }

    if mode == SSO_MODE_MULTI_USER:
        missing = [key for key in SSO_IDP_ELEMENTS if not values[key]]
        if missing:
            raise ChatSsoError(
                f"deployment.yaml chat.sso declares mode: "
                f"{SSO_MODE_MULTI_USER} but is missing {missing}. A "
                f"multi-user site whose identity provider is half-declared "
                f"cannot authenticate anyone, and the partial declaration "
                f"never degrades into {SSO_MODE_SINGLE_OPERATOR} "
                f"mode.{_where(manifest_path)}",
                data={
                    "path": "chat.sso",
                    "mode": mode,
                    "missing": missing,
                    "required": list(SSO_IDP_ELEMENTS),
                },
                fix_hint=f"declare {missing} under chat.sso",
            )
    else:
        extra = sorted(key for key in SSO_IDP_ELEMENTS if values[key])
        if extra:
            raise ChatSsoError(
                f"deployment.yaml chat.sso declares mode: "
                f"{SSO_MODE_SINGLE_OPERATOR} alongside identity-provider "
                f"element(s) {extra}. That is either a mode that was never "
                f"flipped to {SSO_MODE_MULTI_USER} or an identity provider "
                f"that was never removed; reading it as single-operator "
                f"would silently serve every caller as one "
                f"principal.{_where(manifest_path)}",
                data={
                    "path": "chat.sso",
                    "mode": mode,
                    "declared_idp_elements": extra,
                },
                fix_hint=(
                    f"set chat.sso.mode: {SSO_MODE_MULTI_USER}, or remove "
                    f"{extra}"
                ),
            )
        return ChatSso(mode=mode)

    _validate_sso_header_name(values["principal_header"], manifest_path)
    _validate_secret_env_name(values["client_secret_env"], manifest_path)
    _validate_discovery_endpoint(values["discovery_endpoint"], manifest_path)
    for key in SSO_IDP_ELEMENTS:
        _validate_config_safe_scalar(key, values[key], manifest_path)
    if values["admin_group"] == values["user_group"]:
        raise ChatSsoError(
            f"deployment.yaml chat.sso.admin_group and chat.sso.user_group "
            f"name the same group {values['admin_group']!r}. One group "
            f"cannot distinguish two capability tiers: every user would "
            f"receive the admin preset.{_where(manifest_path)}",
            data={
                "path": "chat.sso.admin_group",
                "observed": values["admin_group"],
            },
            fix_hint="name two distinct identity-provider groups",
        )
    return ChatSso(mode=mode, **values)


#: Characters that would let a manifest value break out of the line it is
#: rendered on. The rendered fronts (nginx, oauth2-proxy) are line-oriented
#: config formats with quoted scalars, so a newline or a quote turns a
#: ``client_id`` into an extra DIRECTIVE -- an ``upstreams`` entry pointing
#: somewhere else, an ``email_domains = ["*"]``, a ``skip_auth_regex``. The
#: manifest is governance-controlled, which is why this is a guard rather
#: than the only defence, but a value that cannot be written cannot be
#: injected.
_CONFIG_UNSAFE = re.compile(r'[\r\n"\'\\\x00-\x1f\x7f]')


def _validate_config_safe_scalar(
    key: str, value: str | None, manifest_path: Path | None,
) -> None:
    """Refuse a value that could not be rendered on one config line."""
    if value is None or not _CONFIG_UNSAFE.search(value):
        return
    raise ChatSsoError(
        f"deployment.yaml chat.sso.{key} contains a character that cannot be "
        f"rendered into the fronting proxy's configuration (a newline, a "
        f"quote, a backslash or a control character). Such a value would not "
        f"be escaped into one directive -- it would become several, and the "
        f"extra ones are whatever the value says.{_where(manifest_path)}",
        data={"path": f"chat.sso.{key}", "reason": "config_unsafe_scalar"},
        fix_hint=f"use a plain single-line value for chat.sso.{key}",
    )


def _validate_sso_header_name(value: str | None, manifest_path: Path | None) -> None:
    """Refuse a principal header the MCP mapping could never accept.

    The regex is IMPORTED from ``mcp/principal_mapping.py`` rather than
    restated, because the two must agree by construction: a header this
    block admits and that block rejects would be a deployment that loads
    and then refuses every request as ``header_absent``, which reads like a
    client bug.
    """
    from okg.substrate.mcp.principal_mapping import _HEADER_NAME_RE

    if value and _HEADER_NAME_RE.match(value):
        return
    raise ChatSsoError(
        f"deployment.yaml chat.sso.principal_header is not a valid HTTP "
        f"header field name; observed {value!r}. The fronting proxy stamps "
        f"this header and mcp-serve reads it, so a name neither can send is "
        f"a site that refuses every request.{_where(manifest_path)}",
        data={"path": "chat.sso.principal_header", "observed": repr(value)},
        fix_hint="use a token such as X-OKG-Principal",
    )


def _validate_secret_env_name(
    value: str | None, manifest_path: Path | None,
) -> None:
    """Refuse anything that is not an environment-variable NAME.

    The rejected value is withheld from the message and from ``data``: the
    likeliest way to land here is pasting the client secret into the
    manifest, and a message that echoed it would copy it into logs, CI
    output and evidence artifacts. Same rule, same reason, as
    ``mcp/http_auth.py``'s ``--auth-token-env``.
    """
    from okg.substrate.mcp.http_auth import _ENV_VAR_NAME_RE

    if value and _ENV_VAR_NAME_RE.match(value):
        return
    raise ChatSsoError(
        "deployment.yaml chat.sso.client_secret_env takes the NAME of an "
        "environment variable holding the identity provider's client "
        "secret, and what is declared is not a valid environment-variable "
        "name (letters, digits and underscore, not starting with a digit). "
        "The value is withheld from this message because it may itself be "
        f"the secret.{_where(manifest_path)}",
        data={
            "path": "chat.sso.client_secret_env",
            "observed": "<withheld>",
            "reason": "not_a_var_name",
        },
        fix_hint=(
            "declare the variable NAME, e.g. client_secret_env: "
            "OKG_CHAT_OIDC_CLIENT_SECRET, and export the secret in the "
            "operator environment. The manifest describes what, never "
            "credentials to where (design.md section 4)"
        ),
    )


def _validate_discovery_endpoint(
    value: str | None, manifest_path: Path | None,
) -> None:
    """Refuse a discovery endpoint that is not an absolute, safe URL.

    Cleartext is refused on any routable host: the OIDC discovery document
    decides which keys verify the deployment's tokens, so fetching it over
    http from a routable host lets whoever holds the path choose the
    verifier. Loopback is exempt so a local test IdP remains usable.
    """
    from urllib.parse import urlsplit

    parts = urlsplit(value or "")
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ChatSsoError(
            f"deployment.yaml chat.sso.discovery_endpoint must be an "
            f"absolute http(s) URL; observed {value!r}.{_where(manifest_path)}",
            data={
                "path": "chat.sso.discovery_endpoint",
                "observed": repr(value),
            },
            fix_hint=(
                "point at the provider's OpenID configuration, e.g. "
                "https://idp.example.edu/.well-known/openid-configuration"
            ),
        )
    if parts.scheme == "https":
        return
    host = (parts.hostname or "").lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        return
    raise ChatSsoError(
        f"deployment.yaml chat.sso.discovery_endpoint uses cleartext http "
        f"to the routable host {host!r}. The discovery document names the "
        f"keys that verify this deployment's tokens, so an unauthenticated "
        f"cleartext fetch lets whoever holds the path choose the "
        f"verifier.{_where(manifest_path)}",
        data={"path": "chat.sso.discovery_endpoint", "host": host},
        fix_hint="use https, or a loopback endpoint for a local test IdP",
    )


def _parse_provider(
    value: Any, *, manifest_path: Path | None,
) -> ChatProvider:
    block = _require_mapping(
        value, path="chat.provider", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block, _PROVIDER_KEYS, path="chat.provider", manifest_path=manifest_path,
    )
    base_url = _optional_string(
        block, "base_url", path="chat.provider", manifest_path=manifest_path,
    )
    # The same check `okg chat setup --provider-base-url` applies, so a
    # hand-written manifest cannot carry what setup refuses (#2344).
    from okg.substrate.chat.block_scaffold import (
        _BASE_URL_CREDENTIAL_SHAPES,
        _BASE_URL_SHAPE_TEXT,
        base_url_refusal,
    )

    shape = "missing" if base_url is None else base_url_refusal(base_url)
    if shape is not None:
        # The value is withheld: a key pasted where the URL belongs, or a URL
        # carrying one, would otherwise be echoed into every error, --json
        # and agent output.
        if base_url is None:
            observed, said = "missing", "it is missing"
        else:
            observed = "<withheld>"
            said = (
                f"the declared value {_BASE_URL_SHAPE_TEXT[shape]}, and is "
                "withheld because it may be or carry a credential"
            )
        credential = shape in _BASE_URL_CREDENTIAL_SHAPES
        raise ChatManifestError(
            f"deployment.yaml chat.provider.base_url must be an http(s) URL "
            f"the chat instance can reach, with no credential in it; "
            f"{said}.{_where(manifest_path)}",
            data={
                "path": "chat.provider.base_url",
                "observed": observed,
                "shape": shape,
            },
            fix_hint=(
                f"edit {manifest_path or 'deployment.yaml'} by hand: remove the "
                "credential from chat.provider.base_url and name the environment "
                "variable that holds it in chat.provider.api_key_env. `okg chat "
                "setup --provider-base-url` cannot repair it, because it loads "
                "this manifest first"
                if credential
                else (
                    "set chat.provider.base_url to the provider's OpenAI-compatible "
                    "API base, e.g. http://host.docker.internal:11434/v1"
                )
            ),
        )
    api_key_env = _optional_string(
        block, "api_key_env", path="chat.provider", manifest_path=manifest_path,
    )
    if api_key_env is not None:
        from okg.substrate.mcp.http_auth import _ENV_VAR_NAME_RE

        if not _ENV_VAR_NAME_RE.match(api_key_env):
            # The value is withheld: the likeliest way to land here is
            # pasting the provider key itself into the manifest.
            raise ChatManifestError(
                "deployment.yaml chat.provider.api_key_env takes the NAME of "
                "an environment variable holding the provider credential, and "
                "what is declared is not a valid environment-variable name. "
                f"The value is withheld because it may be the credential."
                f"{_where(manifest_path)}",
                data={
                    "path": "chat.provider.api_key_env",
                    "observed": "<withheld>",
                    "reason": "not_a_var_name",
                },
                fix_hint="name a variable such as OKG_CHAT_PROVIDER_API_KEY",
            )
    tools = _optional_string(
        block, "tools", path="chat.provider", manifest_path=manifest_path,
    )
    if tools is not None and tools not in PROVIDER_TOOLS_VALUES:
        raise ChatManifestError(
            f"deployment.yaml chat.provider.tools must be one of "
            f"{sorted(PROVIDER_TOOLS_VALUES)}; observed {tools!r}."
            f"{_where(manifest_path)}",
            data={"path": "chat.provider.tools", "observed": repr(tools)},
            fix_hint=(
                "omit it (the chat site runs the tools) or set 'provider' for a "
                "provider that brings its own tools, such as okg chat codex-shim"
            ),
        )
    return ChatProvider(
        base_url=base_url,
        api_key_env=api_key_env,
        tools=tools or PROVIDER_TOOLS_INSTANCE,
    )


def _parse_deep_links(
    value: Any, *, manifest_path: Path | None,
) -> ChatDeepLinks:
    block = _require_mapping(
        value, path="chat.deep_links", manifest_path=manifest_path,
    )
    _reject_unknown_keys(
        block,
        _DEEP_LINKS_KEYS,
        path="chat.deep_links",
        manifest_path=manifest_path,
    )
    return ChatDeepLinks(
        operator_ui_base=_optional_string(
            block,
            "operator_ui_base",
            path="chat.deep_links",
            manifest_path=manifest_path,
        ),
        chat_site_base=_optional_string(
            block,
            "chat_site_base",
            path="chat.deep_links",
            manifest_path=manifest_path,
        ),
    )


#: Where the retired surface's code can still be read. PACT
#: layout-history-openspec deleted ``archive/`` from the tree, so the code
#: lives only in git: :data:`VIZ_CHAT_ARCHIVE_PATH` inside this commit, the
#: last one that changed it. Named in the refusal so an operator who wants to
#: see what their block used to configure can run ``git show`` instead of
#: guessing from a deleted path.
VIZ_CHAT_ARCHIVE_COMMIT = "aa9a7eed801536e7e75d1e28d6819f1fb1bd5821"
#: The archive's path inside :data:`VIZ_CHAT_ARCHIVE_COMMIT`. It is not a
#: path in the working tree.
VIZ_CHAT_ARCHIVE_PATH = "archive/chat/viz-chat-2026-08-04"


#: A PINNED image reference: a repository plus either a ``vX.Y.Z`` tag or a
#: content digest. Deliberately narrow. A floating tag (``:latest``, ``:main``,
#: or no tag at all) names DIFFERENT bytes on different days, so a manifest
#: carrying one describes no particular instance and every claim a receipt
#: makes about "the image" would be a claim about whatever had been pulled
#: last. The digest form is admitted because it is the strictest pin there is.
PINNED_IMAGE_REFERENCE = re.compile(
    r"^[a-z0-9][a-z0-9._/-]*(?::v\d+\.\d+\.\d+|@sha256:[0-9a-f]{64})$"
)

#: The reference the hints above should send an operator to. It is a LITERAL
#: and not an import of ``chat.instance.PINNED_IMAGE_REF``: manifest parsing
#: sits below the chat instance layer and must not import it. A test asserts
#: the two agree, so the duplication cannot drift silently.
#:
#: Naming upstream here instead would be worse than vague. Upstream open-webui
#: v0.11.4 satisfies the pinned-reference rule, so an operator following the
#: hint would write a manifest that parses and only then refuses with
#: ``chat_image_not_tested``, one layer further in.
EXAMPLE_PINNED_IMAGE = (
    "your-registry.invalid/okg-openwebui-compat@"
    "sha256:2561170716571aaf7e41417decd7e23b43c390a00395c957b362a17e80557c11"
)


def _parse_image(value: Any, *, manifest_path: Path | None) -> str:
    """``chat.image``: a pinned vendor image reference, or a refusal."""
    if not isinstance(value, str) or not value.strip():
        raise ChatManifestError(
            f"deployment.yaml chat.image must be a non-empty string; observed "
            f"{value!r}.{_where(manifest_path)}",
            data={"path": "chat.image", "observed": repr(value)},
            fix_hint=f"name a pinned image, e.g. {EXAMPLE_PINNED_IMAGE}",
        )
    image = value.strip()
    if not PINNED_IMAGE_REFERENCE.match(image):
        raise ChatManifestError(
            f"deployment.yaml chat.image {image!r} is not a pinned reference. "
            f"It must end in a vX.Y.Z tag or an @sha256: digest; a floating "
            f"tag names different bytes on different days, so nothing this "
            f"deployment records about its image would stay "
            f"true.{_where(manifest_path)}",
            data={"path": "chat.image", "observed": image},
            fix_hint=(
                f"pin the digest the instance layer's tested set names, "
                f"e.g. {EXAMPLE_PINNED_IMAGE}"
            ),
        )
    return image


def _parse_instance_port(value: Any, *, manifest_path: Path | None) -> int:
    """``chat.instance_port``: the host port the site publishes on."""
    # `bool` first: it is an `int` subclass, and `instance_port: true` would
    # otherwise resolve to port 1 rather than being called the mistake it is.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ChatManifestError(
            f"deployment.yaml chat.instance_port must be an integer; observed "
            f"{value!r}.{_where(manifest_path)}",
            data={"path": "chat.instance_port", "observed": repr(value)},
            fix_hint="declare an unprivileged TCP port, e.g. 8080",
        )
    if not 1 <= value <= 65535:
        raise ChatManifestError(
            f"deployment.yaml chat.instance_port {value} is not a TCP port "
            f"(1-65535).{_where(manifest_path)}",
            data={"path": "chat.instance_port", "observed": value},
            fix_hint="declare a port in 1-65535",
        )
    return int(value)


def reject_retired_viz_chat(
    raw_manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None = None,
) -> None:
    """Refuse a manifest that declares ``viz.chat``. PACT ``retire-viz-chat``.

    ``viz.chat`` configured the operator-local chat backend: a Claude Code
    subprocess streamed to the browser, opted into by a sibling file beside
    ``deployment.yaml``. Operator decision 6 retired it in favor of the
    governed ``chat:`` block. Its code was archived under
    :data:`VIZ_CHAT_ARCHIVE_PATH`, whose ``INDEX.md`` maps every retired
    piece to its replacement; that tree now lives only in git, at
    :data:`VIZ_CHAT_ARCHIVE_COMMIT`.

    The refusal exists because the alternative is the quietest failure this
    manifest has. ``viz`` is a known top-level key and nothing rejects an
    unknown key underneath it, so after the removal a ``viz.chat`` block
    would parse, load, pass every check, and configure nothing — the
    operator's system hints would sit in the file and never reach any model.
    A retired key must point at its replacement, not evaporate.

    This runs BEFORE the ``chat:`` presence test in :func:`parse_chat_block`,
    so it also covers the case that used to have its own guard: declaring
    ``chat:`` and ``viz.chat`` together. There is now one refusal instead of
    two, because after the removal there is only one thing wrong with
    ``viz.chat`` — it names a surface that does not exist.

    **Three callers, on purpose.** The typed loader
    (``DeploymentManifest._build_from_raw`` via :func:`parse_chat_block`) is
    the main one, and it covers the CLI, lint, publish and sync. The other
    two read manifests raw and never reach the parser: ``okg.viz.boot``,
    which starts the very process that used to host the retired surface, and
    ``okg.viz.registry``, which enumerates OTHER deployments for the
    switcher. Same rule the sibling ``chat-manifest-block`` PACT set for the
    accent refusal — a rule that holds on the strict path only is a rule an
    operator meets late, or not at all.
    """
    viz = raw_manifest.get("viz")
    if not isinstance(viz, Mapping) or "chat" not in viz:
        return
    raise VizChatRetired(
        f"deployment.yaml declares viz.chat, which is RETIRED. It configured "
        f"the operator-local chat backend — a Claude Code subprocess streamed "
        f"to the browser — and PACT retire-viz-chat removed that surface. "
        f"Its code is kept in git at commit {VIZ_CHAT_ARCHIVE_COMMIT}, under "
        f"{VIZ_CHAT_ARCHIVE_PATH}/. The replacement is the "
        f"top-level chat: block, which declares the deployment's governed "
        f"chat site. The block is refused rather than ignored: nothing reads "
        f"viz.chat any more, so a manifest that kept it would load clean and "
        f"configure nothing.{_where(manifest_path)}",
        data={
            "path": "viz.chat",
            "replacement_path": "chat",
            "pact": "retire-viz-chat",
            "archive_commit": VIZ_CHAT_ARCHIVE_COMMIT,
            "archive_path": VIZ_CHAT_ARCHIVE_PATH,
        },
        fix_hint=(
            "remove viz.chat and declare the top-level chat: block instead "
            f"(git show {VIZ_CHAT_ARCHIVE_COMMIT}:{VIZ_CHAT_ARCHIVE_PATH}/"
            "INDEX.md shows what each retired piece maps to)"
        ),
    )


def _parse_role_list(
    block: Mapping[str, Any], key: str, *, manifest_path: Path | None,
) -> tuple[str, ...]:
    """A non-empty list of role names, each safe to join with a comma."""
    value = block.get(key)
    path = f"chat.oidc_login.{key}"
    if not isinstance(value, list) or not value:
        raise ChatManifestError(
            f"deployment.yaml {path} must be a non-empty list of role names; "
            f"observed {value!r}.{_where(manifest_path)}",
            data={"path": path, "observed": repr(value)},
            fix_hint=f"declare {key}: [role-name, ...]",
        )
    roles: list[str] = []
    for entry in value:
        text = str(entry).strip() if isinstance(entry, str) else ""
        if not text or "," in text:
            raise ChatManifestError(
                f"deployment.yaml {path} contains {entry!r}, which is not a "
                f"single role name (empty, not a string, or containing the "
                f"comma the instance splits roles on).{_where(manifest_path)}",
                data={"path": path, "observed": repr(entry)},
                fix_hint="list each role as its own string",
            )
        _validate_config_safe_scalar(key, text, manifest_path)
        if text not in roles:
            roles.append(text)
    return tuple(roles)


def _parse_oidc_login(value: Any, *, manifest_path: Path | None) -> ChatOidcLogin:
    """Parse ``chat.oidc_login``: the instance's own OIDC login.

    Strict like every other chat sub-block. The two role lists are REQUIRED
    and non-empty because the pinned Open WebUI reads the roles claim only
    when a claim, the allowed roles AND the admin roles are all set; with any
    one missing it silently skips role mapping and admits every person the
    provider authenticates.
    """
    block = _require_mapping(value, path="chat.oidc_login", manifest_path=manifest_path)
    _reject_unknown_keys(
        block, _OIDC_LOGIN_KEYS, path="chat.oidc_login", manifest_path=manifest_path,
    )
    missing = [key for key in _OIDC_LOGIN_REQUIRED if block.get(key) in (None, "", [])]
    if missing:
        raise ChatManifestError(
            f"deployment.yaml chat.oidc_login is missing {missing}. A login "
            f"that cannot name its provider, its client, or who may enter is "
            f"refused, never defaulted.{_where(manifest_path)}",
            data={"path": "chat.oidc_login", "missing": missing},
            fix_hint=f"declare {missing} under chat.oidc_login",
        )
    strings = {
        key: _optional_string(
            block, key, path="chat.oidc_login", manifest_path=manifest_path,
        )
        for key in (
            "issuer",
            "client_id",
            "client_secret_env",
            "roles_claim",
            "provider_name",
            "redirect_uri",
        )
    }
    for key, text in strings.items():
        if text is not None:
            _validate_config_safe_scalar(key, text, manifest_path)
    from urllib.parse import urlsplit

    issuer = urlsplit(str(strings["issuer"]))
    if issuer.scheme != "https" or not issuer.netloc:
        raise ChatManifestError(
            f"deployment.yaml chat.oidc_login.issuer must be an absolute https "
            f"URL.{_where(manifest_path)}",
            data={"path": "chat.oidc_login.issuer"},
            fix_hint="declare the issuer exactly as its tokens' `iss` claim",
        )
    if strings["redirect_uri"] is not None:
        redirect = urlsplit(strings["redirect_uri"])
        if redirect.scheme not in {"http", "https"} or not redirect.netloc:
            raise ChatManifestError(
                f"deployment.yaml chat.oidc_login.redirect_uri must be an "
                f"absolute http(s) URL.{_where(manifest_path)}",
                data={"path": "chat.oidc_login.redirect_uri"},
                fix_hint="declare the URL registered with the identity provider",
            )
    _validate_secret_env_name(strings["client_secret_env"], manifest_path)
    merge = block.get("merge_accounts_by_email", False)
    if not isinstance(merge, bool):
        raise ChatManifestError(
            f"deployment.yaml chat.oidc_login.merge_accounts_by_email must be a "
            f"boolean; observed {merge!r}.{_where(manifest_path)}",
            data={"path": "chat.oidc_login.merge_accounts_by_email", "observed": repr(merge)},
            fix_hint="declare true only for a provider that verifies every email",
        )
    return ChatOidcLogin(
        issuer=str(strings["issuer"]),
        client_id=str(strings["client_id"]),
        client_secret_env=str(strings["client_secret_env"]),
        roles_claim=str(strings["roles_claim"]),
        allowed_roles=_parse_role_list(block, "allowed_roles", manifest_path=manifest_path),
        admin_roles=_parse_role_list(block, "admin_roles", manifest_path=manifest_path),
        provider_name=strings["provider_name"],
        redirect_uri=strings["redirect_uri"],
        merge_accounts_by_email=merge,
    )


def parse_chat_block(
    raw_manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None = None,
    admitted_tools: Iterable[str] | None = None,
) -> ChatBlock | None:
    """Parse and validate ``raw_manifest['chat']``.

    Returns None when the manifest declares no ``chat:`` block — a manifest
    without one loads unchanged. Raises :class:`ChatManifestError` (a
    :class:`DeploymentConfigError`) on any violation.

    Also refuses a retired ``viz.chat`` sub-block, whether or not a ``chat:``
    block is present; see :func:`reject_retired_viz_chat`.
    """
    # Checked BEFORE the no-chat-block early return: a manifest carrying
    # viz.chat and nothing else must refuse too, and that is the case the
    # early return would otherwise wave through.
    reject_retired_viz_chat(raw_manifest, manifest_path=manifest_path)

    if "chat" not in raw_manifest:
        return None
    if raw_manifest.get("chat") is None:
        raise ChatManifestError(
            f"deployment.yaml declares an empty chat: block. An empty "
            f"declaration is not a configured chat; the only minimal legal "
            f"form is 'chat: {{enabled: false}}'.{_where(manifest_path)}",
            data={"path": "chat", "observed": "null"},
            fix_hint="remove the chat: key, or declare enabled: false",
        )

    block = _require_mapping(
        raw_manifest["chat"], path="chat", manifest_path=manifest_path,
    )

    _reject_unknown_keys(
        block, _CHAT_KEYS, path="chat", manifest_path=manifest_path,
    )

    if "enabled" not in block:
        raise ChatManifestError(
            f"deployment.yaml chat: declares no 'enabled' key. A chat block "
            f"must state whether the site exists; an empty or hollow block "
            f"never degrades into a default-configured chat. An enabled chat "
            f"must additionally declare "
            f"{list(_REQUIRED_WHEN_ENABLED)}.{_where(manifest_path)}",
            data={
                "path": "chat.enabled",
                "observed": "absent",
                "required": list(_REQUIRED_WHEN_ENABLED),
            },
            fix_hint="declare chat.enabled: true or false",
        )
    enabled = block["enabled"]
    if not isinstance(enabled, bool):
        raise ChatManifestError(
            f"deployment.yaml chat.enabled must be a boolean; observed "
            f"{enabled!r}.{_where(manifest_path)}",
            data={"path": "chat.enabled", "observed": repr(enabled)},
            fix_hint="set chat.enabled to true or false",
        )

    if enabled:
        missing = [key for key in _REQUIRED_WHEN_ENABLED if key not in block]
        if missing:
            raise ChatManifestError(
                f"deployment.yaml chat.enabled is true but the block "
                f"declares no {missing}. An enabled chat must declare "
                f"{list(_REQUIRED_WHEN_ENABLED)}; a hollow declaration never "
                f"degrades into a default-configured "
                f"chat.{_where(manifest_path)}",
                data={
                    "path": "chat",
                    "missing": missing,
                    "required": list(_REQUIRED_WHEN_ENABLED),
                },
                fix_hint=f"declare {missing} under chat:",
            )
    else:
        # `enabled: false` is a statement that there is no site. Carrying
        # site content alongside it is an unresolved intent, not a
        # configuration — refuse rather than pick a meaning.
        extra = sorted(set(block) - {"enabled"})
        if extra:
            raise ChatManifestError(
                f"deployment.yaml chat.enabled is false but the block also "
                f"declares {extra}. The only minimal legal form is "
                f"'chat: {{enabled: false}}' with no other "
                f"keys.{_where(manifest_path)}",
                data={"path": "chat", "extra": extra},
                fix_hint=(
                    "set chat.enabled: true, or remove the other chat keys"
                ),
            )
        return ChatBlock(enabled=False, raw=dict(block))

    ui = _parse_ui(
        block["ui"], manifest_path=manifest_path,
    )
    # `in block`, not truthiness, and no default: an ABSENT css_bundle is the
    # never-declared state, which the sync side must be able to tell apart from
    # a declaration that was removed. Manufacturing an empty ChatCssBundle here
    # would erase that distinction before anyone could act on it.
    css_bundle = (
        _parse_css_bundle(block["css_bundle"], manifest_path=manifest_path)
        if "css_bundle" in block
        else None
    )
    models = _parse_models(block["models"], manifest_path=manifest_path)
    if not models.builtin:
        raise ChatManifestError(
            f"deployment.yaml chat.models.builtin is empty or absent while "
            f"chat.enabled is true. A chat that offers no model is hollow; "
            f"an empty declared set is never treated as "
            f"permission.{_where(manifest_path)}",
            data={"path": "chat.models.builtin", "observed": []},
            fix_hint="declare at least one model under chat.models.builtin",
        )
    if (
        _surface_declares_chat_prompt(raw_manifest)
        and isinstance(block.get("preset"), Mapping)
        and block["preset"].get("system_prompt_ref") is not None
    ):
        raise ChatManifestError(
            f"deployment.yaml declares chat.preset.system_prompt_ref AND "
            f"agent_surface.projections.chat.system_prompt_ref. Refusing "
            f"rather than preferring one: the projection owns both halves of "
            f"what the chat client is told — the tool set and the prompt that "
            f"describes it — and the deployment check scans that prompt "
            f"against that tool set.{_where(manifest_path)}",
            data={
                "path": "chat.preset.system_prompt_ref",
                "replacement":
                    "agent_surface.projections.chat.system_prompt_ref",
            },
            fix_hint=(
                "delete chat.preset.system_prompt_ref; the projection is the "
                "declaration"
            ),
        )
    preset = _parse_preset(block["preset"], manifest_path=manifest_path)
    search_profile = block["search_profile"]
    if not isinstance(search_profile, str) or not search_profile.strip():
        raise ChatManifestError(
            f"deployment.yaml chat.search_profile must be a non-empty "
            f"string; observed {search_profile!r}.{_where(manifest_path)}",
            data={
                "path": "chat.search_profile",
                "observed": repr(search_profile),
            },
            fix_hint="name a profile declared under search.profiles",
        )
    _validate_search_profile(
        search_profile,
        raw_manifest=raw_manifest,
        manifest_path=manifest_path,
    )
    mcp = _parse_mcp(
        block["mcp"],
        manifest_path=manifest_path,
        admitted_tools=admitted_tools,
        surface_declares_tools=_surface_declares_chat_tools(raw_manifest),
    )
    # Both OPTIONAL, and both `in block` rather than `.get`: an absent key is
    # the never-declared state, which the instance layer must be able to tell
    # apart from a declared value. Neither joins `_REQUIRED_WHEN_ENABLED` --
    # every deployment that loads today declares neither, and an image has a
    # tested default while a port is refused rather than defaulted, so the
    # two absences already have well-defined and different meanings.
    image = (
        _parse_image(block["image"], manifest_path=manifest_path)
        if "image" in block
        else None
    )
    instance_port = (
        _parse_instance_port(block["instance_port"], manifest_path=manifest_path)
        if "instance_port" in block
        else None
    )
    access = (
        _parse_access(block["access"], manifest_path=manifest_path)
        if "access" in block
        else None
    )
    deep_links = (
        _parse_deep_links(block["deep_links"], manifest_path=manifest_path)
        if "deep_links" in block
        else None
    )
    provider = (
        _parse_provider(block["provider"], manifest_path=manifest_path)
        if "provider" in block
        else None
    )
    sso = (
        _parse_sso(block["sso"], manifest_path=manifest_path)
        if "sso" in block
        else None
    )
    if sso is not None and sso.multi_user:
        _validate_access_groups_against_sso(
            access, sso, manifest_path=manifest_path,
        )
    oidc_login = (
        _parse_oidc_login(block["oidc_login"], manifest_path=manifest_path)
        if "oidc_login" in block
        else None
    )
    if oidc_login is not None and sso is not None and sso.multi_user:
        raise ChatManifestError(
            f"deployment.yaml chat declares both oidc_login and a multi_user "
            f"chat.sso. The instance's own login and a fronting proxy that "
            f"vouches for people are two identity models for one site; one of "
            f"them must go.{_where(manifest_path)}",
            data={"path": "chat.oidc_login"},
            fix_hint="remove chat.sso (the proxy model) or chat.oidc_login",
        )
    return ChatBlock(
        enabled=True,
        ui=ui,
        css_bundle=css_bundle,
        models=models,
        preset=preset,
        search_profile=search_profile,
        mcp=mcp,
        access=access,
        provider=provider,
        deep_links=deep_links,
        sso=sso,
        oidc_login=oidc_login,
        image=image,
        instance_port=instance_port,
        raw=dict(block),
    )


def _validate_access_groups_against_sso(
    access: ChatAccess | None,
    sso: ChatSso,
    *,
    manifest_path: Path | None,
) -> None:
    """One group vocabulary, not two.

    ``chat.access.groups`` predates this block and names Open WebUI groups
    directly. In multi-user mode those groups are no longer authored by
    hand: they are PROJECTED from the identity provider's group claims
    (design.md section 5 mechanism 4). A name here outside the projected
    pair would be a group the projection never creates and no IdP claim
    ever fills — a second vocabulary whose members are always empty, which
    reads as a working ACL and grants nothing.
    """
    declared = tuple(access.groups) if access is not None else ()
    projected = set(sso.projected_groups)
    stray = sorted(set(declared) - projected)
    if not stray:
        return
    raise ChatSsoError(
        f"deployment.yaml chat.access.groups names {stray}, which "
        f"chat.sso does not project. In "
        f"{SSO_MODE_MULTI_USER} mode the deployment's groups come from the "
        f"identity provider's group claims and are projected verbatim as "
        f"{sorted(projected)} — one projection, two consumers, no second "
        f"group vocabulary. A group outside that set is never filled by any "
        f"claim, so it would read as an access grant while granting "
        f"nothing.{_where(manifest_path)}",
        data={
            "path": "chat.access.groups",
            "declared": list(declared),
            "projected": sorted(projected),
            "stray": stray,
        },
        fix_hint=(
            f"declare chat.access.groups from {sorted(projected)}, or "
            f"rename chat.sso.admin_group / chat.sso.user_group"
        ),
    )


# ---------------------------------------------------------------------------
# Shared visual-identity tokens
# ---------------------------------------------------------------------------

#: The console's per-deployment theme token set. The first five are named as
#: the archived viz app's own ``default_config.yaml`` named them; ``surface``
#: and ``rail`` are the SURFACE FAMILY added by the operator ruling of
#: 2026-08-18 (PACT ``operator-ui-deployment-theme``), because a token set
#: that could name the page ground but not the cards and rail standing on it
#: could only express declarations the console renders unreadable.
#:
#: This is the WHOLE typed surface: a key under ``viz.theme`` outside this set
#: is refused, because the console reads these seven and nothing else, so an
#: eighth would load, validate, and configure nothing — a typo'd ``acccent``
#: must not read as agreement.
#:
#: Order matters only in that it is asserted identically on the browser side
#: (``VIZ_THEME_TOKENS`` in ``operator-ui/styles/deploymentTheme.ts``); the two
#: new tokens are APPENDED so the original five keep their positions.
VIZ_THEME_TOKENS: tuple[str, ...] = (
    "background",
    "accent",
    "fg",
    "fg_faint",
    "border",
    "surface",
    "rail",
)

#: The surface family: the tokens that paint what STANDS ON the page ground.
#:
#: Named as a set of its own because :func:`_reject_incoherent_polarity` is
#: about the relationship between ``background`` and ``surface``, not about
#: any one token's value. ``rail`` is in the family but not in the coherence
#: rule: every one of the console's four built-in firings paints the rail dark
#: — light and dark firings alike — so the rail does not flip with the
#: operator's palette choice and a declared background cannot contradict it
#: the way it contradicts a card.
VIZ_THEME_SURFACE_FAMILY: tuple[str, ...] = ("surface", "rail")

#: The states a deployment's theme can be in as the SERVER knows them.
#:
#: - ``declared``   — a manifest was read and it declares ``viz.theme``.
#: - ``undeclared`` — a manifest was read and it declares no ``viz.theme``.
#:                    A fact about a file that was opened.
#: - ``unresolved`` — no manifest was read, or the one that was read is
#:                    refused. Saying "declares no theme" here would be a
#:                    claim about a file nobody successfully opened.
#:
#: The browser adds a fourth state, ``pending``, for the read that has not
#: answered yet. This three-way split is the server half of the four-state
#: pattern in ``operator-ui/shared/hooks/chatSiteContext.ts``, and it exists
#: for that file's reason: the absences are different, and collapsing them
#: makes the console assert things it does not know.
VIZ_THEME_DECLARED = "declared"
VIZ_THEME_UNDECLARED = "undeclared"
VIZ_THEME_UNRESOLVED = "unresolved"

#: A declared theme's POLARITY: is the ground it declares light or dark?
#:
#: Added by the operator ruling of 2026-08-18 (the second one, "base firing +
#: overrides"). The token set names seven properties and the console's firings
#: define roughly forty, so every declared token has an undeclared sibling that
#: keeps the chosen firing's value — measured at 694 residual contrast
#: violations under the seven-token widening, 98% of them ink drawn by
#: ``--okg-color-strong``, ``--okg-color-link``, ``--okg-color-surface-muted``
#: and ``--okg-color-subtle``. Widening the token set further would chase that
#: tail forever. The polarity is what lets the console instead pick a COHERENT
#: BASE — a built-in firing of the declared polarity, whose whole palette is
#: already internally consistent — and layer the seven declared tokens on top
#: of it. The siblings then come from a palette that agrees with the
#: declaration instead of contradicting it.
#:
#: This is a SERVED FACT, not a browser inference: the luminance machinery
#: (:func:`theme_color_luminance`, :data:`_POLARITY_MIDPOINT_LUMINANCE`) already
#: lives here for the coherence refusal, and a second implementation in
#: TypeScript would be a copy that drifts from the one that does the refusing.
VIZ_THEME_POLARITY_DARK = "dark"
VIZ_THEME_POLARITY_LIGHT = "light"
VIZ_THEME_POLARITIES: tuple[str, ...] = (
    VIZ_THEME_POLARITY_LIGHT,
    VIZ_THEME_POLARITY_DARK,
)

# A theme token's admissible shapes. Anchored at both ends, so no value can
# carry a `;`, a `}`, a newline, or a `url(` past the check — these strings
# become CSS custom property values in the browser, and a declaration that can
# be escaped is a declaration that can rewrite the page.
_HEX_COLOR_RE = re.compile(r"#[0-9a-fA-F]+\Z")
_HEX_COLOR_DIGITS = frozenset({3, 4, 6, 8})
_FUNCTIONAL_COLOR_RE = re.compile(r"(?:rgb|rgba|hsl|hsla)\([0-9a-zA-Z.,%+\-/ ]*\)\Z")
_IDENTIFIER_COLOR_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9-]*\Z")


def _is_admissible_color(value: str) -> bool:
    """Whether ``value`` has the shape of a CSS color the console can apply.

    Three arms: hex (``#rgb`` / ``#rgba`` / ``#rrggbb`` / ``#rrggbbaa``),
    functional notation (``rgb()`` / ``rgba()`` / ``hsl()`` / ``hsla()``), and
    a bare CSS identifier, which covers the named colors plus ``transparent``
    and ``currentColor``.

    **What this proves, exactly.** Every admitted value is a syntactically
    well-formed CSS component value that cannot escape the declaration it is
    written into. It is NOT a membership test against the 148 CSS named
    colors: the identifier arm admits any identifier, so ``notacolor`` passes
    here and simply paints nothing. Enumerating the named-color list would
    make this function a copy of a browser table that drifts; the refusals
    that matter — a number, a mapping, an empty string, a truncated hex like
    ``#12345``, anything carrying CSS punctuation — are all caught.
    """
    if _HEX_COLOR_RE.match(value):
        return len(value) - 1 in _HEX_COLOR_DIGITS
    return bool(
        _FUNCTIONAL_COLOR_RE.match(value)
        or _IDENTIFIER_COLOR_RE.match(value)
    )


# ---------------------------------------------------------------------------
# Polarity — is a declared color light or dark?
# ---------------------------------------------------------------------------

#: The relative luminance at which a surface is exactly as readable under
#: black ink as under white ink, and therefore the only non-arbitrary place to
#: put the light/dark boundary.
#:
#: DERIVED, not chosen. WCAG 2.x contrast is ``(L_lighter + 0.05) /
#: (L_darker + 0.05)``, so a surface of luminance ``L`` has contrast
#: ``1.05 / (L + 0.05)`` against white and ``(L + 0.05) / 0.05`` against black.
#: Setting those equal gives ``(L + 0.05)**2 == 0.0525``, hence
#: ``L == sqrt(0.0525) - 0.05 ≈ 0.1791``. Below it a surface wants light ink,
#: above it dark ink — which is precisely the question "does this background
#: flip polarity" asks. A midpoint of 0.5 would have been eyeballed and wrong:
#: sRGB is non-linear, and mid-grey ``#808080`` has a relative luminance near
#: 0.216, already on the light side of this boundary.
_POLARITY_MIDPOINT_LUMINANCE = math.sqrt(0.0525) - 0.05

_FUNCTIONAL_COLOR_PARTS_RE = re.compile(r"(rgb|rgba|hsl|hsla)\((.*)\)\Z", re.DOTALL)

#: Degrees per unit, for the angle units CSS admits as an ``<hue>``.
_ANGLE_UNIT_DEGREES = {"deg": 1.0, "grad": 0.9, "rad": 180.0 / math.pi, "turn": 360.0}


def _parse_number(text: str) -> tuple[float, bool] | None:
    """A CSS ``<number>`` or ``<percentage>`` as ``(value, was_percentage)``.

    ``None`` for anything that is not one, including the non-finite floats
    Python's ``float()`` would otherwise accept from ``"nan"`` and ``"inf"``.
    """
    text = text.strip()
    is_percentage = text.endswith("%")
    if is_percentage:
        text = text[:-1].strip()
    if not text or text[-1] not in "0123456789.":
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value, is_percentage


def _srgb_component(channel: float) -> float:
    """Linearize one sRGB channel given as 0..1. WCAG 2.x, verbatim."""
    if channel <= 0.03928:
        return channel / 12.92
    return ((channel + 0.055) / 1.055) ** 2.4


def _relative_luminance(red: float, green: float, blue: float) -> float:
    """WCAG relative luminance of an opaque sRGB color, channels given 0..1."""
    return (
        0.2126 * _srgb_component(red)
        + 0.7152 * _srgb_component(green)
        + 0.0722 * _srgb_component(blue)
    )


def _rgb_from_hex(value: str) -> tuple[float, float, float] | None:
    """``#rgb`` / ``#rgba`` / ``#rrggbb`` / ``#rrggbbaa`` as 0..1 channels.

    A TRANSLUCENT color returns ``None``: what it paints depends on the
    backdrop behind it, and this parser cannot see the backdrop. Guessing one
    would be the silent default this module refuses everywhere else.
    """
    digits = value[1:]
    if len(digits) in (3, 4):
        channels = [int(digit * 2, 16) / 255.0 for digit in digits]
    elif len(digits) in (6, 8):
        channels = [
            int(digits[index:index + 2], 16) / 255.0
            for index in range(0, len(digits), 2)
        ]
    else:
        return None
    if len(channels) == 4 and channels[3] < 1.0:
        return None
    return channels[0], channels[1], channels[2]


def _rgb_channel(text: str) -> float | None:
    """One ``rgb()`` channel as 0..1. Clamped, because CSS clamps it too."""
    parsed = _parse_number(text)
    if parsed is None:
        return None
    value, is_percentage = parsed
    channel = value / 100.0 if is_percentage else value / 255.0
    return min(max(channel, 0.0), 1.0)


def _hsl_fraction(text: str) -> float | None:
    """An ``hsl()`` saturation or lightness as 0..1.

    A bare number is admitted as a 0..100 percentage, which is what CSS Color
    4 made it mean; refusing it would fail closed on values browsers render.
    """
    parsed = _parse_number(text)
    if parsed is None:
        return None
    return min(max(parsed[0] / 100.0, 0.0), 1.0)


def _hue_degrees(text: str) -> float | None:
    """A CSS ``<hue>`` in degrees, or ``None`` if it is not one."""
    text = text.strip().lower()
    # "grad" before "rad": the former ends with the latter.
    for unit in ("grad", "turn", "rad", "deg"):
        if text.endswith(unit):
            parsed = _parse_number(text[: -len(unit)])
            if parsed is None or parsed[1]:
                return None
            return parsed[0] * _ANGLE_UNIT_DEGREES[unit]
    parsed = _parse_number(text)
    if parsed is None or parsed[1]:
        return None
    return parsed[0]


def _rgb_from_hsl(
    hue: float, saturation: float, lightness: float,
) -> tuple[float, float, float]:
    """CSS Color 4's HSL-to-RGB conversion, channels out as 0..1."""
    hue = hue % 360.0
    chroma = (1.0 - abs(2.0 * lightness - 1.0)) * saturation
    secondary = chroma * (1.0 - abs((hue / 60.0) % 2.0 - 1.0))
    offset = lightness - chroma / 2.0
    sector = int(hue // 60.0) % 6
    red, green, blue = (
        (chroma, secondary, 0.0),
        (secondary, chroma, 0.0),
        (0.0, chroma, secondary),
        (0.0, secondary, chroma),
        (secondary, 0.0, chroma),
        (chroma, 0.0, secondary),
    )[sector]
    return red + offset, green + offset, blue + offset


def _rgb_from_functional(value: str) -> tuple[float, float, float] | None:
    """``rgb()`` / ``rgba()`` / ``hsl()`` / ``hsla()`` as 0..1 channels.

    Handles the legacy comma form and the modern space form, with alpha given
    either as a fourth argument or after a ``/``. A translucent color returns
    ``None`` for :func:`_rgb_from_hex`'s reason.
    """
    match = _FUNCTIONAL_COLOR_PARTS_RE.match(value)
    if match is None:
        return None
    name, body = match.group(1).lower(), match.group(2)
    if body.count("/") > 1:
        return None
    head, slash, tail = body.partition("/")
    alpha_text: str | None = tail if slash else None
    parts = [part for part in re.split(r"[,\s]+", head.strip()) if part]
    if alpha_text is None and len(parts) == 4:
        parts, alpha_text = parts[:3], parts[3]
    if len(parts) != 3:
        return None
    if alpha_text is not None:
        alpha = _parse_number(alpha_text)
        if alpha is None:
            return None
        if (alpha[0] / 100.0 if alpha[1] else alpha[0]) < 1.0:
            return None
    if name in ("rgb", "rgba"):
        channels = [_rgb_channel(part) for part in parts]
        if any(channel is None for channel in channels):
            return None
        return channels[0], channels[1], channels[2]  # type: ignore[return-value]
    hue = _hue_degrees(parts[0])
    saturation = _hsl_fraction(parts[1])
    lightness = _hsl_fraction(parts[2])
    if hue is None or saturation is None or lightness is None:
        return None
    return _rgb_from_hsl(hue, saturation, lightness)


def theme_color_luminance(value: str) -> float | None:
    """The WCAG relative luminance of a theme token, or ``None`` if unknowable.

    ``None`` is a real answer, not a failure to try: it means this parser
    cannot compute a luminance for the form the operator wrote, and every
    caller must fail closed on it rather than assume a polarity.

    Computable: hex and the functional notations, when opaque. NOT computable:
    a bare CSS identifier (``rebeccapurple``, ``transparent``,
    ``currentColor``), because resolving one needs the browser's 148-entry
    named-color table — the same table :func:`_is_admissible_color` refuses to
    copy, since a copy drifts from the browsers it is meant to describe. Also
    not computable: any translucent color, whose painted luminance depends on
    a backdrop the manifest does not contain.
    """
    if value.startswith("#"):
        rgb = _rgb_from_hex(value)
    else:
        rgb = _rgb_from_functional(value)
    return None if rgb is None else _relative_luminance(*rgb)


def theme_polarity(tokens: Mapping[str, str]) -> str | None:
    """Whether a declared token set describes a light or a dark console.

    ``None`` is a real answer and the common one: it means the declaration says
    nothing about the polarity of the page — an ``accent``-only theme is the
    ordinary case — so the console must NOT pick a base firing for it and must
    leave the operator's chosen palette exactly where it is. Guessing "light"
    here would hand every accent-only deployment a palette switch nobody asked
    for.

    Two sources, in order, and the order is the point:

    - ``background`` — the page ground, and the token the operator ruling names.
      It is what :func:`_reject_incoherent_polarity` already reasons about, so
      taking the polarity from anywhere else first would let a manifest load
      under one polarity and render under another.
    - ``surface`` — the cards. Consulted only when ``background`` is absent or
      has no computable luminance (a bare identifier, ``transparent``, anything
      translucent — see :func:`theme_color_luminance`). This arm is
      load-bearing rather than decorative: a manifest declaring ``surface`` and
      ``fg`` without a ``background`` is accepted today, and the surface is the
      plane the stranded ink is drawn ON, so a dark surface with no polarity
      would reproduce the exact defect the ruling closes.

    A theme declaring the two at OPPOSITE polarities resolves to the
    ``background``'s. That combination is accepted at load (the operator named
    both planes deliberately), and the ground is the larger surface; the base
    firing it selects is a starting palette, not a claim that every declared
    token agrees with it.
    """
    for name in ("background", "surface"):
        value = tokens.get(name)
        if value is None:
            continue
        luminance = theme_color_luminance(value)
        if luminance is None:
            continue
        return (
            VIZ_THEME_POLARITY_LIGHT
            if luminance >= _POLARITY_MIDPOINT_LUMINANCE
            else VIZ_THEME_POLARITY_DARK
        )
    return None


@dataclass(frozen=True)
class VizThemeState:
    """A deployment's theme tokens together with HOW they came to be absent.

    ``tokens`` is non-empty only when ``status`` is ``declared``. In both
    absent states it is empty, and it is empty rather than pre-filled: the
    console's built-in palettes are the fallback, and a fallback that arrives
    labelled as a deployment declaration is the defect this type exists to
    prevent.

    ``polarity`` is the declared ground's light/dark side
    (:func:`theme_polarity`), served so the console can pick a coherent base
    firing before layering the tokens. It is ``None`` in both absent states,
    and also for a declaration that names no ground — three different reasons
    to leave the operator's chosen palette alone, all of which the browser
    treats identically because the resulting behaviour is identical.
    """

    status: str
    tokens: dict[str, str] = field(default_factory=dict)
    polarity: str | None = None

    def as_payload(self) -> dict[str, Any]:
        """The JSON shape served on ``/viz/api/deployments``."""
        return {
            "status": self.status,
            "tokens": dict(self.tokens),
            "polarity": self.polarity,
        }


def viz_theme_undeclared() -> VizThemeState:
    """The state for a manifest that was READ and declares no ``viz.theme``."""
    return VizThemeState(status=VIZ_THEME_UNDECLARED)


def viz_theme_unresolved() -> VizThemeState:
    """The state for a manifest that was never read, or was read and refused."""
    return VizThemeState(status=VIZ_THEME_UNRESOLVED)


def _reject_viz_theme_token(
    name: str,
    value: Any,
    *,
    manifest_path: Path | None = None,
) -> str:
    """Return the declared token, or raise :class:`VizThemeInvalid`.

    The returned string is the declared value with surrounding whitespace
    removed, and that is also the string that was validated — the served token
    is always exactly the one this function admitted, never a repaired or
    substituted one.
    """
    if not isinstance(value, str):
        raise VizThemeInvalid(
            f"deployment.yaml declares viz.theme.{name}={value!r}, which is "
            f"{type(value).__name__}, not a color string. The console applies "
            f"these tokens as CSS custom properties; a non-string has no "
            f"rendering, so it is refused rather than coerced into "
            f"one.{_where(manifest_path)}",
            data={
                "path": f"viz.theme.{name}",
                "observed_type": type(value).__name__,
            },
            fix_hint=(
                f"declare viz.theme.{name} as a quoted color string, e.g. "
                f'"#10b981"'
            ),
        )
    token = value.strip()
    if not token:
        # PRESENCE, not truthiness — matching the accent rule above. An
        # operator who wrote `accent: ""` wrote a declaration, and it is just
        # as unrenderable as a malformed one. Reading it as "undeclared" is
        # precisely the absence-dressed-as-agreement this change removes.
        raise VizThemeInvalid(
            f"deployment.yaml declares an empty viz.theme.{name}. An empty "
            f"value is a declaration the operator wrote and the console "
            f"cannot render; it is refused rather than silently read as no "
            f"declaration at all.{_where(manifest_path)}",
            data={"path": f"viz.theme.{name}", "observed": "empty"},
            fix_hint=(
                f"give viz.theme.{name} a color value, or remove the key"
            ),
        )
    if not _is_admissible_color(token):
        raise VizThemeInvalid(
            f"deployment.yaml declares viz.theme.{name}={token!r}, which is "
            f"not a CSS color value. Accepted forms are hex (#rgb, #rgba, "
            f"#rrggbb, #rrggbbaa), functional notation (rgb/rgba/hsl/hsla), "
            f"and a bare CSS identifier. The value is refused rather than "
            f"normalized: it is written into a stylesheet, so a value that "
            f"cannot be parsed there must not reach "
            f"there.{_where(manifest_path)}",
            data={"path": f"viz.theme.{name}", "observed": token},
            fix_hint=(
                f'declare viz.theme.{name} as a color, e.g. "#10b981" or '
                f'"rgb(16 185 129)"'
            ),
        )
    return token


def _reject_incoherent_polarity(
    tokens: Mapping[str, str],
    *,
    manifest_path: Path | None = None,
) -> None:
    """Refuse a declared ground that contradicts the surfaces standing on it.

    THE RULE. The console's default firing (``cyclades``, see
    ``operator-ui/styles/theme.ts``) is light, and every built-in firing pairs
    a page ground with cards of the SAME polarity. A manifest declaring
    ``background`` alone therefore replaces the ground and leaves
    ``--okg-color-surface`` at the firing's value: declare a dark ground and
    the console paints light cards on it, with the declared light ink landing
    on those near-white cards. That is not a hypothetical — it is the measured
    defect the 2026-08-18 operator ruling responds to. So a declared
    ``background`` on the dark side of :data:`_POLARITY_MIDPOINT_LUMINANCE`
    must be accompanied by a declared ``surface``, and is refused otherwise.

    WHY THE CHECK FAILS CLOSED ON AN UNCOMPUTABLE VALUE. A bare identifier has
    no luminance this parser can compute (see :func:`theme_color_luminance`),
    so a ``background: rebeccapurple`` declared without a ``surface`` cannot be
    shown coherent. Loading it would be a check that passes because it could
    not run — absence dressed as agreement. It is refused, and the message
    asks for either a computable form or the ``surface`` that makes the
    question moot.

    WHAT THIS DOES NOT PROVE. It is one implication, not full coherence, and
    reading it as "the render is now accessible" would be exactly the
    over-claim this module exists to refuse. Three known gaps, all of them
    consequences of a deliberately minimal widening:

    - The console's semantic ink — ``--okg-color-link``, ``--okg-color-strong``,
      ``--okg-color-subtle``, the ok/warn/bad/info families and
      ``--okg-color-surface-muted`` — is OUTSIDE the token set, so it keeps the
      chosen firing's values. This check does nothing about that; what does is
      the SECOND operator ruling of 2026-08-18, which has the console select a
      base firing of the declared polarity (:func:`theme_polarity`) before
      layering the tokens, so those siblings come from a palette that agrees
      with the declaration. This function's job stays what it was: refusing a
      manifest that contradicts ITSELF.
    - A manifest declaring ``background`` and ``surface`` at OPPOSITE
      polarities is accepted, because the operator named both surfaces.

    The honest summary: this refuses the ground/surface contradiction that a
    manifest can express, and it does not certify the whole page.
    """
    background = tokens.get("background")
    if background is None or "surface" in tokens:
        return

    luminance = theme_color_luminance(background)
    if luminance is None:
        raise VizThemeIncoherent(
            f"deployment.yaml declares viz.theme.background={background!r} "
            f"without viz.theme.surface, and the polarity of that value "
            f"cannot be computed from the manifest. The console has to know "
            f"whether a declared ground is light or dark to know whether it "
            f"contradicts the cards drawn on it, and bare CSS identifiers, "
            f"'transparent' and translucent colors resolve only in a "
            f"browser. Write background as an opaque hex or rgb()/hsl() "
            f"value, or declare viz.theme.surface alongside it — either one "
            f"answers the question.{_where(manifest_path)}",
            data={
                "path": "viz.theme.background",
                "observed": background,
                "requires": "surface",
                "reason": "luminance_not_computable",
            },
            fix_hint=(
                'write viz.theme.background as an opaque color, e.g. '
                '"#121417", or declare viz.theme.surface alongside it'
            ),
        )

    if luminance >= _POLARITY_MIDPOINT_LUMINANCE:
        return

    raise VizThemeIncoherent(
        f"deployment.yaml declares a dark viz.theme.background="
        f"{background!r} (relative luminance {luminance:.4f}, below the "
        f"{_POLARITY_MIDPOINT_LUMINANCE:.4f} light/dark midpoint) without a "
        f"viz.theme.surface. The console's default palette is light, so this "
        f"declaration paints a dark page ground and leaves the cards, panels "
        f"and menus at their near-white built-in values — the declared ink "
        f"then lands on those cards unreadably. Declare viz.theme.surface "
        f"(and usually viz.theme.rail) at the same polarity as the "
        f"background, or leave background undeclared and let the operator's "
        f"chosen palette supply a coherent pair.{_where(manifest_path)}",
        data={
            "path": "viz.theme.background",
            "observed": background,
            "luminance": luminance,
            "midpoint": _POLARITY_MIDPOINT_LUMINANCE,
            "requires": "surface",
            "reason": "polarity_flip_without_surface",
        },
        fix_hint=(
            "declare viz.theme.surface (a slightly lighter shade of the "
            'background, e.g. "#1a1d21" for background "#121417"), or remove '
            "viz.theme.background"
        ),
    )


def parse_viz_theme(
    raw_manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None = None,
) -> VizThemeState:
    """Parse ``viz.theme`` into a typed token set with an explicit state.

    Returns :data:`VIZ_THEME_DECLARED` with the declared tokens verbatim, or
    :data:`VIZ_THEME_UNDECLARED` with none. Raises :class:`VizThemeInvalid`
    for anything malformed WITHIN ``viz.theme``, and
    :class:`VizThemeIncoherent` when the tokens are each valid but contradict
    one another — see :func:`_reject_incoherent_polarity`.

    **Scope.** This function reads ``viz.theme`` and nothing else. ``viz`` as
    a whole remains the untyped pass-through dict it has always been, so
    ``viz.port``, ``viz.presentation`` and the layout tuning keep their
    current semantics; a non-mapping ``viz`` is left to the loader's existing
    handling rather than refused here.

    **Why an empty theme is refused.** ``theme: {}`` and ``theme:`` (null)
    both raise, on the same reasoning :func:`parse_chat_block` applies to an
    empty ``chat:`` block — an empty declaration is not a configured theme.
    Reading it as "undeclared" would let a block the operator wrote load,
    validate, and paint nothing, which is the exact failure this typing
    exists to end.

    **Why unknown keys are refused.** The console consumes
    :data:`VIZ_THEME_TOKENS` and nothing else, so an eighth key could never
    reach a screen. ``viz`` has no unknown-key check, which is what let this
    sub-block accumulate a shape nobody read; the check stops at
    ``viz.theme``'s own boundary.
    """
    viz = raw_manifest.get("viz")
    if not isinstance(viz, Mapping) or "theme" not in viz:
        return viz_theme_undeclared()

    theme = viz["theme"]
    if theme is None or (isinstance(theme, Mapping) and not theme):
        raise VizThemeInvalid(
            f"deployment.yaml declares an empty viz.theme block. An empty "
            f"declaration is not a configured theme: it would load, validate, "
            f"and leave the console on its built-in palette while the "
            f"manifest suggests otherwise. Declare at least one of "
            f"{list(VIZ_THEME_TOKENS)}, or remove the "
            f"block.{_where(manifest_path)}",
            data={
                "path": "viz.theme",
                "observed": "null" if theme is None else "empty",
                "tokens": list(VIZ_THEME_TOKENS),
            },
            fix_hint=(
                f"declare at least one of {list(VIZ_THEME_TOKENS)} under "
                f"viz.theme, or remove viz.theme"
            ),
        )
    if not isinstance(theme, Mapping):
        raise VizThemeInvalid(
            f"deployment.yaml declares viz.theme as "
            f"{type(theme).__name__}, but it is a mapping of token names to "
            f"color values. Declare it as "
            f"{list(VIZ_THEME_TOKENS)}.{_where(manifest_path)}",
            data={"path": "viz.theme", "observed_type": type(theme).__name__},
            fix_hint=(
                f"declare viz.theme as a mapping over "
                f"{list(VIZ_THEME_TOKENS)}"
            ),
        )

    unknown = sorted(str(key) for key in theme if key not in VIZ_THEME_TOKENS)
    if unknown:
        raise VizThemeInvalid(
            f"deployment.yaml declares unknown viz.theme keys {unknown}. The "
            f"console reads {list(VIZ_THEME_TOKENS)} and nothing else, so a "
            f"key outside that set would load, validate, and never reach a "
            f"screen.{_where(manifest_path)}",
            data={
                "path": "viz.theme",
                "unknown": unknown,
                "tokens": list(VIZ_THEME_TOKENS),
            },
            fix_hint=(
                f"remove {unknown} from viz.theme; the token set is "
                f"{list(VIZ_THEME_TOKENS)}"
            ),
        )

    tokens = {
        name: _reject_viz_theme_token(
            name, theme[name], manifest_path=manifest_path,
        )
        for name in VIZ_THEME_TOKENS
        if name in theme
    }
    # Every token is now individually valid. The remaining question is whether
    # they are valid TOGETHER — see :func:`_reject_incoherent_polarity`.
    _reject_incoherent_polarity(tokens, manifest_path=manifest_path)
    return VizThemeState(
        status=VIZ_THEME_DECLARED,
        tokens=tokens,
        polarity=theme_polarity(tokens),
    )


def resolve_theme_accent(
    raw_manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None = None,
) -> str | None:
    """Resolve the deployment's accent for the switcher card's flat field.

    **Scope, as measured.** The accent is an operator-console token and only
    that: ``viz.theme.accent``, flattened by ``okg.viz.registry`` to
    ``theme_accent`` for the deployment-switcher card. It does NOT reach the
    chat surface.

    **This is no longer the console's only manifest read.** It was, and the
    superseded claim is recorded here so the change is legible: until PACT
    ``operator-ui-deployment-theme`` the accent was the single manifest value
    the console consumed, and this resolver was its single consumer. The
    console now also reads the TYPED seven-token set —
    :data:`VIZ_THEME_TOKENS`, parsed by :func:`parse_viz_theme` and served
    with an explicit declared / undeclared / unresolved state on the same
    ``/viz/api/deployments`` entries. This function survives alongside it and
    is deliberately NOT reimplemented on top of it: it keeps the tolerant
    behavior the flat field has always had, whereas :func:`parse_viz_theme`
    refuses what this one waves through. A caller wanting the strict read
    wants the parser.

    The recorded knob census measured Open WebUI v0.11.0 as
    exposing no accent config key, no accent env var, and no
    admin API — the only lever is a static ``custom.css`` whose contract is
    byte delivery, not a readable value — so ``theme.accent`` is not an
    admissible ``chat.ui`` field and no accent declared for a deployment can
    render on its chat site.

    **What this means for the shared-identity-token requirement.** It is
    scoped, not delivered and not cancelled. The token set was supposed to
    stop two graphical surfaces drifting on one value; at this version only
    one surface can consume the value, so there is nothing to drift. The
    three-state precedence therefore collapses to its first state — a
    ``viz.theme`` accent resolves exactly as it did before any of this work —
    and the other two states describe a coupling that does not exist yet.
    Whether losing per-deployment chat accent is acceptable is an OPEN
    operator decision on the program's census floor (the census records the
    floor as not met); this function does not presume the answer.

    **Why the chat branch still exists.** A ``chat.ui.theme.accent`` in a
    manifest is refused, loudly, in both paths that can see it: the parser
    rejects the field, and this resolver refuses to hand any consumer a value
    from it. Silently ignoring a documented field the operator wrote is
    exactly the no-op the census mechanism exists to catch — the operator
    would see their color in the file and never on the screen. If a later
    census admits a drivable theme knob, the three-state precedence returns
    here intact.
    """
    viz = raw_manifest.get("viz")
    viz_theme = viz.get("theme") if isinstance(viz, Mapping) else None
    viz_accent = (
        viz_theme.get("accent") if isinstance(viz_theme, Mapping) else None
    )
    viz_accent = viz_accent or None

    chat = raw_manifest.get("chat")
    chat_ui = chat.get("ui") if isinstance(chat, Mapping) else None
    chat_theme = chat_ui.get("theme") if isinstance(chat_ui, Mapping) else None
    # PRESENCE, not truthiness: `accent: ""` is a declaration the operator
    # wrote, and it is just as unrenderable as a real color. Coercing falsy
    # values to "undeclared" would let exactly those slip through unrefused.
    chat_declares_accent = (
        isinstance(chat_theme, Mapping) and "accent" in chat_theme
    )
    chat_accent = (
        chat_theme.get("accent") if chat_declares_accent else None
    )

    if chat_declares_accent:
        # Note this subsumes the old "disagreeing accents raise" rule: an
        # AGREEING chat-side accent is refused too, because agreement with the
        # console says nothing about whether the chat site can render it — and
        # it cannot.
        raise ChatManifestError(
            f"deployment.yaml declares chat.ui.theme.accent="
            f"{chat_accent!r}, but the recorded knob census measured the chat "
            f"accent as NOT drivable at Open WebUI "
            f"{UI_KNOB_CENSUS_PINNED_VERSION}: no config key, no env var, no "
            f"admin API, and no readable accent value. The declaration could "
            f"never render, so it is refused instead of silently ignored. The "
            f"accent remains an operator-console token "
            f"(viz.theme.accent).{_where(manifest_path)}",
            data={
                "path": "chat.ui.theme.accent",
                "chat_accent": chat_accent,
                "viz_accent": viz_accent,
                "openwebui_version": UI_KNOB_CENSUS_PINNED_VERSION,
            },
            fix_hint=(
                "remove chat.ui.theme; declare the console accent under "
                "viz.theme.accent"
            ),
        )
    return viz_accent

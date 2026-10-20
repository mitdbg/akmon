"""Deployment registry for the viz switcher.

Derived from the repo's ``deployments/*/deployment.yaml`` tree — the
one source of truth we already have. Each manifest may declare an
optional advisory ``viz.port`` field consumed only here (the running
server's own port still comes from ``OKG_VIZ_PORT``).

The registry is a launchpad, not a live-presence indicator: the
browser probes entries lazily when the user opens the switcher. The
server only enumerates what it finds on disk plus whatever metadata
each manifest declares.
"""

from __future__ import annotations
from okg.substrate import fastyaml

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_VIZ_PORT = 8765


def _coerce_port(value: Any, *, source: str) -> int | None:
    """Return a valid port int from ``value`` or ``None``.

    Invalid values (non-int, out of range) are logged and dropped —
    the registry should not break a page load over a malformed advisory
    field.
    """
    if value is None:
        return None
    try:
        port = int(value)
    except (TypeError, ValueError):
        logger.warning("viz.port in %s is not an integer: %r", source, value)
        return None
    if not (1 <= port <= 65535):
        logger.warning("viz.port in %s is out of range: %d", source, port)
        return None
    return port


def _read_manifest_viz_port(manifest_path: Path) -> int | None:
    """Parse ``viz.port`` from a deployment.yaml without loading the
    full manifest. The field is advisory metadata; we tolerate
    malformed siblings so one bad manifest does not hide the others.
    """

    try:
        with manifest_path.open() as f:
            raw = fastyaml.safe_load(f) or {}
    except Exception as exc:
        logger.warning("could not read %s: %s", manifest_path, exc)
        return None
    viz = raw.get("viz")
    if not isinstance(viz, dict):
        return None
    return _coerce_port(viz.get("port"), source=str(manifest_path))


def _read_chat_site_base(manifest: dict[str, Any]) -> str | None:
    """Read ``chat.deep_links.chat_site_base`` out of a deployment manifest.

    The console's reverse navigation link has no other data source: this
    value is how a browser learns where the deployment's chat site lives
    (PACT ``chat-deep-links``, design.md section 4). Absent, non-string, or
    blank means the deployment declares no chat site — the console then
    renders no link and says so. There is deliberately no default host: a
    guessed chat URL is worse than an absent one.

    The field's schema and validation belong to ``chat-manifest-block``;
    this is a read, not a second configuration source.
    """
    chat = manifest.get("chat")
    if not isinstance(chat, dict):
        return None
    deep_links = chat.get("deep_links")
    if not isinstance(deep_links, dict):
        return None
    value = deep_links.get("chat_site_base")
    if not isinstance(value, str):
        return None
    return value.strip() or None


def _read_operator_ui_base(manifest: dict[str, Any]) -> str | None:
    """Read the deployment's declared operator-console base URL.

    The read API and the static operator app are intentionally served by
    different processes, so ``viz.port`` cannot be converted into a console
    URL.  ``chat.deep_links.operator_ui_base`` is the deployment-owned source
    of truth already validated by the manifest loader.
    """
    chat = manifest.get("chat")
    if not isinstance(chat, dict):
        return None
    deep_links = chat.get("deep_links")
    if not isinstance(deep_links, dict):
        return None
    value = deep_links.get("operator_ui_base")
    if not isinstance(value, str):
        return None
    return value.strip() or None


def _resolved_accent(raw: dict[str, Any], *, source: str) -> str | None:
    """Resolve the accent, which is an operator-console token and only that.

    ``viz.theme.accent`` was the one manifest value this console read. It no
    longer is: PACT ``operator-ui-deployment-theme`` added the typed
    seven-token read below (:func:`_resolved_viz_theme`), and this flat field
    is now the switcher card's arm of it. Both are served on the same entry
    and both are kept, because they answer different questions — this one is
    tolerant and returns a bare string or nothing, the typed one distinguishes
    "declares no theme" from "no manifest was read".

    The recorded knob census measured Open WebUI v0.11.0 as exposing no accent
    knob at all, so a chat site cannot be given one; ``chat.ui.theme`` is
    therefore not an admissible manifest field and a manifest declaring it
    fails to load.

    Here — enumerating OTHER deployments' manifests for the switcher, where a
    malformed sibling must not hide the rest — that refusal is reported and
    the accent is left UNRESOLVED. Rendering an accent read from a manifest
    the loader rejects would let an invalid file drive the console.

    The retired-``viz.chat`` refusal is checked on the same footing and for
    the same reason (PACT ``retire-viz-chat``). This function is the ONE
    place the switcher observes another deployment's raw manifest, and it
    never calls the strict parser; without the check here, a deployment the
    loader rejects would still render a normal, accented card.
    """
    from okg.substrate.deployments.chat_block import (
        ChatManifestError,
        reject_retired_viz_chat,
        resolve_theme_accent,
    )

    try:
        reject_retired_viz_chat(raw)
        return resolve_theme_accent(raw)
    except ChatManifestError as exc:
        # Not necessarily a CONFLICT: the common case is a manifest declaring
        # a chat accent at all, which is inadmissible whether or not the
        # console's own accent agrees with it.
        logger.warning(
            "unusable accent declaration in %s, rendering none: %s",
            source, exc,
        )
        return None


def _resolved_viz_theme(raw: dict[str, Any], *, source: str) -> dict[str, Any]:
    """Resolve the deployment's typed theme tokens into a served state.

    Returns the ``{"status": ..., "tokens": {...}}`` payload the console
    reads. Three states, never two: ``declared`` carries the tokens verbatim,
    ``undeclared`` says this manifest was READ and declares no theme, and
    ``unresolved`` says the manifest could not be read or does not load. The
    console's built-in palettes are the fallback for both absences, but only
    one of them is a statement about the operator's file.

    Same tolerance rule as :func:`_resolved_accent`, and for the same reason:
    here the switcher is enumerating OTHER deployments' manifests, where a
    malformed sibling must not hide the rest. The refusal is reported and the
    theme is left UNRESOLVED — never downgraded to ``undeclared``, which would
    tell the console a rejected file declares nothing.
    """
    from okg.substrate.deployments.chat_block import (
        ChatManifestError,
        parse_viz_theme,
        reject_retired_viz_chat,
        viz_theme_unresolved,
    )

    try:
        reject_retired_viz_chat(raw)
        return parse_viz_theme(raw).as_payload()
    except ChatManifestError as exc:
        logger.warning(
            "unusable viz.theme declaration in %s, serving unresolved: %s",
            source, exc,
        )
        return viz_theme_unresolved().as_payload()


def _unresolved_viz_theme() -> dict[str, Any]:
    """The served state for an entry built from no manifest at all.

    The registry synthesizes entries in two cases — a current deployment whose
    manifest lives outside the enumerated root, and the placeholder for an
    empty registry. Neither opened a file, so neither may claim the deployment
    declares no theme.
    """
    from okg.substrate.deployments.chat_block import viz_theme_unresolved

    return viz_theme_unresolved().as_payload()


def _read_manifest_meta(manifest_path: Path) -> dict[str, Any]:
    """Pull just the fields the switcher needs: name, description,
    the resolved theme accent, the typed ``viz.theme`` token state, and the
    deployment's declared browser bases. Returns
    a best-effort dict; missing fields fall back to directory-name / empty
    string.
    """

    try:
        with manifest_path.open() as f:
            raw = fastyaml.safe_load(f) or {}
    except Exception as exc:
        logger.warning("could not read %s: %s", manifest_path, exc)
        raw = {}
    viz = raw.get("viz") if isinstance(raw.get("viz"), dict) else {}
    return {
        "name": raw.get("name") or manifest_path.parent.name,
        "description": raw.get("description") or "",
        "port": _coerce_port(viz.get("port"), source=str(manifest_path)),
        "theme_accent": _resolved_accent(raw, source=str(manifest_path)),
        "viz_theme": _resolved_viz_theme(raw, source=str(manifest_path)),
        "chat_site_base": _read_chat_site_base(raw),
        "operator_ui_base": _read_operator_ui_base(raw),
    }


def current_deployment(
    *,
    manifest: dict[str, Any] | None,
    bound_port: int | None,
    host: str = "127.0.0.1",
) -> dict[str, Any]:
    """Build the current-deployment record.

    ``manifest`` is the deployment.yaml dict (as parsed by
    `okg.viz.boot._load_manifest`), or `None` for a degenerate
    fallback. The bound port is whatever the process actually
    listens on, not an advisory manifest value.
    """
    if manifest is None:
        return {
            "name": "viz",
            "description": "",
            "port": bound_port,
            "url": f"http://{host}:{bound_port}/viz/" if bound_port else None,
            "theme_accent": None,
            # No manifest was read, so the theme is UNRESOLVED, not
            # undeclared. This branch is the degenerate fallback; claiming the
            # deployment declares no theme would describe a file that was
            # never opened.
            "viz_theme": _unresolved_viz_theme(),
            "chat_site_base": None,
            "operator_ui_base": None,
        }
    from okg.substrate.deployments.chat_block import (
        parse_viz_theme,
        resolve_theme_accent,
    )

    # The CURRENT deployment's manifest is authoritative: a conflicting pair
    # of accent declarations propagates rather than being downgraded to
    # "no accent", because this deployment's own manifest already fails to
    # load for the same reason. The typed theme read is strict here on the
    # same footing — this process is SERVING this manifest, so a malformed
    # token of its own is an error to raise, not a sibling's problem to
    # tolerate.
    return {
        "name": manifest.get("name") or "viz",
        "description": manifest.get("description", "") or "",
        "port": bound_port,
        "url": f"http://{host}:{bound_port}/viz/" if bound_port else None,
        "theme_accent": resolve_theme_accent(manifest),
        "viz_theme": parse_viz_theme(manifest).as_payload(),
        "chat_site_base": _read_chat_site_base(manifest),
        "operator_ui_base": _read_operator_ui_base(manifest),
    }


def _served_links(manifest: dict[str, Any]) -> tuple[str | None, str | None]:
    """The browser links of the manifest this process serves.

    The same two reads :func:`current_deployment` makes, so the current
    deployment's registry entry and ``/viz/api/deployment`` cannot disagree.
    """
    return _read_chat_site_base(manifest), _read_operator_ui_base(manifest)


def _canonical_launch_command(name: str, port: int) -> str:
    """Return the recommended launch command for a sibling deployment.

    Mirrors the shape used in CLAUDE workflows: env-var-driven,
    deployment selected via ``OKG_DEPLOYMENT``. The command is a
    hint for the operator, not an executable contract.
    """
    return (
        f"OKG_DEPLOYMENT={name} OKG_VIZ_PORT={port} "
        f"python -m okg.viz --deployment {name}"
    )


def load_registry(
    *,
    deployments_root: Path | None,
    current_name: str | None,
    current_port: int | None,
    host: str = "127.0.0.1",
    current_manifest: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Enumerate sibling deployments under ``deployments_root``.

    Each entry is a bookmark with everything the switcher needs to
    render: name, description, expected URL, advisory port,
    ``theme_accent``, the typed ``viz_theme`` token state, the deployment's
    declared chat and operator browser bases, a canonical launch
    command, and an ``is_current`` flag.

    ``viz_theme`` is present on EVERY entry, including the synthesized ones —
    an absent key and a null value would be two different absences for the
    browser to tell apart, and its status field already says which absence
    this is.

    ``current_manifest`` is the manifest this process SERVES (the one
    ``/viz/api/deployment`` reads). When given, the current deployment's entry
    takes its browser links from it — whether or not its manifest lives under
    ``deployments_root`` — so the list and the singular read never disagree
    about where this deployment's chat site is. A deployment served with
    ``OKG_DEPLOYMENTS_DIR`` outside the repo otherwise had no manifest here and
    read as declaring no chat site. Without it (no context yet), the current
    entry has no manifest to read and its links stay ``None``.

    When ``deployments_root`` is None or empty, the function returns
    a single-entry list reflecting the current deployment so the
    switcher still has something to render.
    """
    entries: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    if deployments_root and deployments_root.is_dir():
        for manifest_path in sorted(deployments_root.glob("*/deployment.yaml")):
            meta = _read_manifest_meta(manifest_path)
            name = meta["name"]
            port = meta["port"]
            # Prefer the actually-bound port for the current deployment
            # so drift between manifest and launch env is visible.
            if current_name and name == current_name and current_port:
                port = current_port
            if port is None:
                port = DEFAULT_VIZ_PORT
            chat_site_base = meta["chat_site_base"]
            operator_ui_base = meta["operator_ui_base"]
            if current_manifest is not None and current_name and name == current_name:
                chat_site_base, operator_ui_base = _served_links(current_manifest)
            entries.append({
                "name": name,
                "description": meta["description"],
                "port": port,
                "url": f"http://{host}:{port}/viz/",
                "theme_accent": meta["theme_accent"],
                "viz_theme": meta["viz_theme"],
                "chat_site_base": chat_site_base,
                "operator_ui_base": operator_ui_base,
                "launch_command": _canonical_launch_command(name, port),
                "is_current": bool(current_name and name == current_name),
            })
            seen_names.add(name)

    # Guarantee the current deployment appears even if its manifest
    # lives outside ``deployments_root`` (external deployments).
    if current_name and current_name not in seen_names:
        port = current_port or DEFAULT_VIZ_PORT
        chat_site_base, operator_ui_base = (
            _served_links(current_manifest)
            if current_manifest is not None
            else (None, None)
        )
        entries.append({
            "name": current_name,
            "description": "",
            "port": port,
            "url": f"http://{host}:{port}/viz/",
            "theme_accent": None,
            "viz_theme": _unresolved_viz_theme(),
            "chat_site_base": chat_site_base,
            "operator_ui_base": operator_ui_base,
            "launch_command": _canonical_launch_command(current_name, port),
            "is_current": True,
        })
    elif not entries:
        # No root, no current: a blank registry is still legal but
        # renders poorly. Return one placeholder so the UI does not
        # show a mysterious empty dropdown.
        port = current_port or DEFAULT_VIZ_PORT
        entries.append({
            "name": "viz",
            "description": "",
            "port": port,
            "url": f"http://{host}:{port}/viz/",
            "theme_accent": None,
            "viz_theme": _unresolved_viz_theme(),
            "chat_site_base": None,
            "operator_ui_base": None,
            "launch_command": _canonical_launch_command("viz", port),
            "is_current": True,
        })

    return entries

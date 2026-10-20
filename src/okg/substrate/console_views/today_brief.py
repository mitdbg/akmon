"""Pydantic model for the Today brief read payload.

The retired TypeScript schema and parity test are archived with the legacy UI.
This active module owns the backend payload shape until the future single
operator app adds a new frontend contract.

Per the rebuild-console-views-visual-gated PACT, the Today body
renders a TodayBrief verbatim. The substrate composer (at
`compose_today` below, currently a fixture stub for the
static-build state) MUST return this exact shape; once the
visual gate signs off, the composer reads from
`okg.v_today_brief_<deployment>`.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class TodayStat(BaseModel):
    """A single stat in the five-stat strip under the headline."""

    id: str
    """Stable id, e.g. 'inbox'. React key."""

    label: str
    """Caps mono label, e.g. 'INBOX', 'AWAITING YOU'."""

    value: str
    """Serif display value, e.g. '12', '18m'."""

    subtitle: str
    """Italic serif subtitle under the value, e.g. 'captures'."""


AttentionTone = Literal["accent", "info", "warn", "bad", "good"]


class TodayAction(BaseModel):
    """Action button on an attention card."""

    label: str
    """Button label as rendered, e.g. 'Open thread'."""

    verb: str
    """Stable verb id, e.g. 'open_thread'. Resolved by the client dispatcher."""

    ref: str | None = None
    """Optional ref the verb operates on (node id, order id)."""

    style: Literal["primary", "quiet"]
    """Visual style: terracotta-filled primary or quiet text link."""


class AttentionItem(BaseModel):
    """A single card in the 'worth your attention' section."""

    id: str
    """Stable id used as React key + for selection state."""

    rank: int
    """Position in the list, 1-indexed. Big serif numeral."""

    tone: AttentionTone
    """Left-rail tint."""

    glyph: str | None = None
    """Optional left-margin glyph name (primitives Glyph names)."""

    title: str
    """Card title, serif semibold."""

    meta: str
    """Mono metadata line under the title — source-kind only.
    The substrate never emits relative-time strings; the renderer
    composes them from `observed_at`."""

    observed_at: str | None = None
    """Absolute ISO-8601 UTC timestamp of the underlying observation.
    Optional because aggregate attention items may have no single
    underlying timestamp."""

    sub_italic: str | None = None
    """Optional italic-serif 'why this matters' line under meta."""

    primary_action: TodayAction
    """Primary action: terracotta filled button."""

    secondary_action: TodayAction | None = None
    """Secondary action: quiet text link. May be omitted."""


class LandedSubtotal(BaseModel):
    """Mini-stat in the landed-overnight section."""

    label: str
    """Caps mono label."""

    value: int
    """Numeric value."""


class AttentionSection(BaseModel):
    """The 'worth your attention' section."""

    title: str
    """Section eyebrow, e.g. 'WORTH YOUR ATTENTION'."""

    subtitle: str
    """Right-aligned italic-serif subtitle."""

    items: list[AttentionItem]
    """Up to ~5 cards."""


class LandedOvernightSection(BaseModel):
    """The 'what landed overnight' section."""

    title: str
    """Section eyebrow."""

    subtitle: str
    """Right-aligned italic-serif subtitle."""

    subtotals: list[LandedSubtotal]
    """Mini-stats grid (AWAITING TRIAGE 4, AUTO-ROUTED BY RULE 8)."""


# --- PI/Scientist profile sections ------------------------------------


class DeskCard(BaseModel):
    """The single 'where you left off' session card."""

    id: str
    title: str
    nickname: str
    branch: str
    cwd: str
    cwd_short: str
    denial_count: int
    last_modified_at: str | None = None
    primary_action: TodayAction


class DeskSection(BaseModel):
    """The DESK section: zero or one session card."""

    session: DeskCard | None


class QueueSection(BaseModel):
    """REVIEWS / DRAFTS: a ranked queue of AttentionItem cards."""

    title: str
    subtitle: str
    items: list[AttentionItem]


class OvernightItem(BaseModel):
    """One row in the OVERNIGHT section — a finished agent session."""

    id: str
    rank: int
    tone: AttentionTone
    glyph: str | None = None
    title: str
    nickname: str
    branch: str
    commit_short: str
    denial_count: int
    ended_at: str | None = None
    started_at: str | None = None


class OvernightSection(BaseModel):
    """The OVERNIGHT section: recent finished sessions, top-N."""

    title: str
    subtitle: str
    items: list[OvernightItem]


class ReadingItem(BaseModel):
    """One row in the READING section — a document marked as reading material."""

    id: str
    rank: int
    tone: AttentionTone
    glyph: str | None = None
    title: str
    kind: str
    path: str
    parsed_at: str | None = None
    byte_count: int


class ReadingSection(BaseModel):
    """The READING section: top-N reading-material documents."""

    title: str
    subtitle: str
    items: list[ReadingItem]


class TodoItem(BaseModel):
    """One row in the TODOS section — a personal plan parsed
    from `plans.md` (or substrate `plan` subtype when promoted)."""

    id: str
    rank: int
    tone: AttentionTone
    glyph: str | None = None
    title: str
    section: str
    projects: list[str]
    due: str | None = None
    people: list[str]
    line_number: int
    source_path: str


class TodosSection(BaseModel):
    """The TODOS section: parsed from `plans.md`."""

    title: str
    subtitle: str
    items: list[TodoItem]


class TodayGreeting(BaseModel):
    """The Today greeting block."""

    text: str
    """Static text, e.g. 'Good morning'."""

    addressee: str
    """Display-name to address, rendered inline + accent-ink."""

    punctuation: str
    """Period/exclamation after the addressee, e.g. '.', '!'."""


class TodayEyebrow(BaseModel):
    """Caps mono eyebrow row above the headline."""

    day: str
    """Day of week, e.g. 'TUESDAY'."""

    date: str
    """Date, e.g. '21 MAY'."""

    generation_label: str
    """Generation label, e.g. 'GEN 4012'."""


class TodayBrief(BaseModel):
    """The Today view payload. One per (deployment, generation)."""

    view: Literal["today"] = "today"
    """Discriminator. Always 'today'."""

    generation_id: int
    """Generation id the payload pins to."""

    generated_at: str
    """ISO timestamp when the composer produced this payload."""

    eyebrow: TodayEyebrow
    """Eyebrow row above the headline."""

    greeting: TodayGreeting
    """Greeting block (the serif display headline)."""

    stats: list[TodayStat] | None = Field(default=None)
    """Five-stat strip under the headline. Optional — the
    pi_scientist profile drops the strip entirely (None);
    the operator profile fills it with five cells."""

    attention: AttentionSection | None = None
    """Operator-profile section. Absent on the pi_scientist payload."""

    landed_overnight: LandedOvernightSection | None = None
    """Operator-profile section. Absent on the pi_scientist payload."""

    desk: DeskSection | None = None
    """PI/Scientist: most recent session card. Absent on operator view."""

    reviews: QueueSection | None = None
    """PI/Scientist: PACTs awaiting your verdict."""

    drafts: QueueSection | None = None
    """PI/Scientist: PACTs you're actively writing."""

    overnight: OvernightSection | None = None
    """PI/Scientist: agent sessions ended in the last 24h."""

    reading: ReadingSection | None = None
    """PI/Scientist: top-N reading-material documents."""

    todos: TodosSection | None = None
    """PI/Scientist: personal plans parsed from `plans.md`."""


# --- Fixture composer (static-build state) ----------------------------
#
# Deterministic fixture composer for structural tests.


def compose_today_brief_from_view_manifest(
    view_manifest: dict[str, object],
    *,
    generation_id: int,
    generated_at: str,
    operator_name: str = "",
) -> TodayBrief:
    """Build a TodayBrief from the publisher's materialized view manifest.

    The native view stage emits `view_manifest` as part of the
    JSON output slot. It carries the list of fragments produced
    by today_brief's planner. Each fragment's `content` is a
    JSON document for one anchor (eyebrow, greeting, stats, an
    attention item, or landed_overnight).

    This composer maps that fragment list onto the typed
    `TodayBrief` payload the renderer consumes. It is the
    inverse of `build_corpus` on the substrate side.

    Args:
      view_manifest: the `view_manifest` field from
        `read_view_slot(slot_name="json")`'s payload.
      generation_id: the manifest's `target_generation_id`.
      generated_at: ISO-8601 timestamp the endpoint serves
        the response at (typically the generation's
        `published_at`).
      operator_name: who's running the console. The substrate
        doesn't know this — the endpoint resolves it from runtime
        config (env var → git user.name → $USER) and passes it
        here so the greeting renders with the real operator's
        name. Empty string is the no-personalization default;
        the renderer drops the comma + addressee when empty.

    Raises:
      ValueError: if a required aggregate anchor is missing.
    """
    import json

    fragments = view_manifest.get("fragments") or []

    eyebrow_payload: dict | None = None
    greeting_payload: dict | None = None
    stats_payload: dict | None = None
    landed_payload: dict | None = None
    attention_items: list[dict] = []

    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        # The native view stage stores the anchor id as
        # `fragment["id"]` (for QuoteSpec-backed fragments
        # it equals the anchor id because the planner sets
        # both `id` and `anchor_id` to the same value).
        anchor_id = str(fragment.get("id") or "")
        if not anchor_id:
            sources = fragment.get("sources") or []
            anchor_id = sources[0] if sources else ""
        try:
            payload = json.loads(fragment.get("content") or "null")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue
        if anchor_id == "today:eyebrow":
            eyebrow_payload = payload
        elif anchor_id == "today:greeting":
            greeting_payload = payload
        elif anchor_id == "today:stats":
            stats_payload = payload
        elif anchor_id == "today:landed_overnight":
            landed_payload = payload
        elif anchor_id.startswith("today:attention:"):
            attention_items.append(payload)

    if eyebrow_payload is None:
        raise ValueError("today_brief view_manifest missing today:eyebrow fragment")
    if greeting_payload is None:
        raise ValueError("today_brief view_manifest missing today:greeting fragment")
    if stats_payload is None:
        raise ValueError("today_brief view_manifest missing today:stats fragment")
    if landed_payload is None:
        raise ValueError(
            "today_brief view_manifest missing today:landed_overnight fragment",
        )

    # Aggregate attention items by `rank` to render a stable order.
    attention_items.sort(key=lambda x: int(x.get("rank") or 0))

    # Legacy-fragment normalization. Pre-v3 substrate fragments
    # emitted tone/style values outside the schema's Literal
    # (caution/review/standard/ghost). The wire-boundary composer
    # maps them to the canonical vocabulary so the endpoint can
    # serve old manifests without re-materialization. New
    # substrate code emits v3-compliant values directly.
    _LEGACY_TONE = {
        "caution": "bad",
        "review": "accent",
        "standard": "info",
    }
    _LEGACY_STYLE = {"ghost": "quiet"}

    def _normalize_action(action: dict | None) -> dict | None:
        if not isinstance(action, dict):
            return action
        style = action.get("style")
        if isinstance(style, str) and style in _LEGACY_STYLE:
            action = {**action, "style": _LEGACY_STYLE[style]}
        return action

    normalized_items: list[dict] = []
    for item in attention_items:
        tone = item.get("tone")
        if isinstance(tone, str) and tone in _LEGACY_TONE:
            item = {**item, "tone": _LEGACY_TONE[tone]}
        item = {
            **item,
            "primary_action": _normalize_action(item.get("primary_action")),
            "secondary_action": _normalize_action(item.get("secondary_action")),
        }
        normalized_items.append(item)
    attention_items = normalized_items

    # Override addressee from runtime config. The substrate
    # emits empty (or, on pre-v4 cached manifests, a legacy
    # "Iris" lifted from the mockup). The composer is the
    # wire boundary that knows the operator's name.
    greeting_with_operator = {
        **greeting_payload,
        "addressee": operator_name or "",
    }

    # Count-based attention subtitle (replaces "N things, in order").
    # Reads tones to surface the actionable shape of the queue.
    by_tone: dict[str, int] = {}
    for it in attention_items:
        t = str(it.get("tone") or "info")
        by_tone[t] = by_tone.get(t, 0) + 1
    parts: list[str] = []
    if by_tone.get("bad", 0):
        parts.append(f"{by_tone['bad']} blocked")
    if by_tone.get("accent", 0):
        parts.append(f"{by_tone['accent']} to review")
    if by_tone.get("warn", 0):
        parts.append(f"{by_tone['warn']} need evidence")
    if by_tone.get("info", 0):
        parts.append(f"{by_tone['info']} in flight")
    attention_subtitle = " · ".join(parts) if parts else "all clear"

    return TodayBrief(
        view="today",
        generation_id=generation_id,
        generated_at=generated_at,
        eyebrow=TodayEyebrow(**eyebrow_payload),
        greeting=TodayGreeting(**greeting_with_operator),
        stats=[TodayStat(**c) for c in stats_payload.get("stats", [])],
        attention=AttentionSection(
            title="WORTH YOUR ATTENTION",
            subtitle=attention_subtitle,
            items=[AttentionItem(**i) for i in attention_items],
        ),
        landed_overnight=LandedOvernightSection(
            title=landed_payload.get("title", "LANDED OVERNIGHT"),
            subtitle=landed_payload.get("subtitle", ""),
            subtotals=[
                # The substrate-side LandedSubtotal carries value as a
                # string; the schema's Pydantic model coerces to int.
                LandedSubtotal(label=s["label"], value=int(s["value"]))
                for s in landed_payload.get("subtotals", [])
            ],
        ),
    )


def compose_today_pi_scientist_from_view_manifest(
    view_manifest: dict[str, object],
    *,
    generation_id: int,
    generated_at: str,
    operator_name: str = "",
) -> TodayBrief:
    """Compose the PI/Scientist Today payload from the publisher
    manifest of `view_def_id=okg_workspace.today_brief.pi_scientist`.

    Anchor namespace:
      today:eyebrow / today:greeting / today:stats / today:desk
      today:reviews:NN:<node_id>
      today:drafts:NN:<node_id>
      today:overnight:NN:<node_id>

    Args:
      view_manifest: `view_manifest` from `read_view_slot("json")`.
      generation_id: manifest's target_generation_id.
      generated_at: ISO-8601 string the endpoint serves (usually
        the generation's `published_at`).
      operator_name: resolved at the wire boundary; the substrate
        emits empty addressee.

    Raises:
      ValueError on missing-required-anchor.
    """
    import json as _json

    fragments = view_manifest.get("fragments") or []

    eyebrow_payload: dict | None = None
    greeting_payload: dict | None = None
    stats_payload: dict | None = None
    desk_payload: dict | None = None
    reviews_items: list[dict] = []
    drafts_items: list[dict] = []
    overnight_items: list[dict] = []
    reading_items: list[dict] = []
    todos_items: list[dict] = []

    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        anchor_id = str(fragment.get("id") or "")
        if not anchor_id:
            sources = fragment.get("sources") or []
            anchor_id = sources[0] if sources else ""
        try:
            payload = _json.loads(fragment.get("content") or "null")
        except (_json.JSONDecodeError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue
        if anchor_id == "today:eyebrow":
            eyebrow_payload = payload
        elif anchor_id == "today:greeting":
            greeting_payload = payload
        elif anchor_id == "today:stats":
            stats_payload = payload
        elif anchor_id == "today:desk":
            desk_payload = payload if payload else None
        elif anchor_id.startswith("today:reviews:"):
            reviews_items.append(payload)
        elif anchor_id.startswith("today:drafts:"):
            drafts_items.append(payload)
        elif anchor_id.startswith("today:overnight:"):
            overnight_items.append(payload)
        elif anchor_id.startswith("today:reading:"):
            reading_items.append(payload)
        elif anchor_id.startswith("today:todos:"):
            todos_items.append(payload)

    if eyebrow_payload is None:
        raise ValueError("pi_scientist view_manifest missing today:eyebrow")
    if greeting_payload is None:
        raise ValueError("pi_scientist view_manifest missing today:greeting")
    # `today:stats` is optional on v2 (the strip is dropped).

    # Operator name override at the wire boundary.
    greeting_with_operator = {
        **greeting_payload,
        "addressee": operator_name or "",
    }

    # Normalize legacy tone/style on attention-style items
    # (defence in depth; today_brief_pi_scientist already emits
    # the canonical vocabulary).
    _LEGACY_TONE = {"caution": "bad", "review": "accent", "standard": "info"}
    _LEGACY_STYLE = {"ghost": "quiet"}

    def _norm_action(action):
        if not isinstance(action, dict):
            return action
        style = action.get("style")
        if isinstance(style, str) and style in _LEGACY_STYLE:
            return {**action, "style": _LEGACY_STYLE[style]}
        return action

    def _norm_item(item: dict) -> dict:
        if not isinstance(item, dict):
            return item
        tone = item.get("tone")
        if isinstance(tone, str) and tone in _LEGACY_TONE:
            item = {**item, "tone": _LEGACY_TONE[tone]}
        return {
            **item,
            "primary_action": _norm_action(item.get("primary_action")),
            "secondary_action": _norm_action(item.get("secondary_action")),
        }

    reviews_items = [_norm_item(i) for i in
                     sorted(reviews_items, key=lambda x: x.get("rank", 0))]
    drafts_items = [_norm_item(i) for i in
                    sorted(drafts_items, key=lambda x: x.get("rank", 0))]
    overnight_items = sorted(overnight_items, key=lambda x: x.get("rank", 0))
    reading_items = sorted(reading_items, key=lambda x: x.get("rank", 0))
    todos_items = sorted(todos_items, key=lambda x: x.get("rank", 0))

    desk_section: DeskSection
    if desk_payload and desk_payload.get("id"):
        desk_card = DeskCard(
            id=str(desk_payload.get("id") or ""),
            title=str(desk_payload.get("title") or ""),
            nickname=str(desk_payload.get("nickname") or ""),
            branch=str(desk_payload.get("branch") or ""),
            cwd=str(desk_payload.get("cwd") or ""),
            cwd_short=str(desk_payload.get("cwd_short") or ""),
            denial_count=int(desk_payload.get("denial_count") or 0),
            last_modified_at=desk_payload.get("last_modified_at") or None,
            primary_action=TodayAction(
                **(desk_payload.get("primary_action") or {})
            ),
        )
        desk_section = DeskSection(session=desk_card)
    else:
        desk_section = DeskSection(session=None)

    reviews_section = QueueSection(
        title="REVIEWS",
        subtitle=_queue_subtitle(reviews_items, kind="reviews"),
        items=[AttentionItem(**i) for i in reviews_items],
    )
    drafts_section = QueueSection(
        title="DRAFTS",
        subtitle=_queue_subtitle(drafts_items, kind="drafts"),
        items=[AttentionItem(**i) for i in drafts_items],
    )
    overnight_section = OvernightSection(
        title="OVERNIGHT",
        subtitle=_overnight_subtitle(overnight_items),
        items=[OvernightItem(**i) for i in overnight_items],
    )
    reading_section = ReadingSection(
        title="READING",
        subtitle=_reading_subtitle(reading_items),
        items=[ReadingItem(**i) for i in reading_items],
    )
    todos_section = TodosSection(
        title="TODOS",
        subtitle=_todos_subtitle(todos_items),
        items=[TodoItem(**i) for i in todos_items],
    )

    # Stats are optional on the v2 pi_scientist payload; empty
    # list when the strip is dropped.
    stats_list = (
        [TodayStat(**c) for c in (stats_payload or {}).get("stats", [])]
        if stats_payload else []
    )

    return TodayBrief(
        view="today",
        generation_id=generation_id,
        generated_at=generated_at,
        eyebrow=TodayEyebrow(**eyebrow_payload),
        greeting=TodayGreeting(**greeting_with_operator),
        stats=stats_list,
        desk=desk_section,
        reviews=reviews_section,
        drafts=drafts_section,
        overnight=overnight_section,
        reading=reading_section,
        todos=todos_section,
        # Operator sections explicitly null on this profile.
        attention=None,
        landed_overnight=None,
    )


def _queue_subtitle(items: list[dict], *, kind: str) -> str:
    """Count-based subtitle for REVIEWS / DRAFTS."""
    if not items:
        return "all clear"
    bad = sum(1 for it in items if it.get("tone") == "bad")
    warn = sum(1 for it in items if it.get("tone") == "warn")
    parts: list[str] = []
    if kind == "reviews":
        if bad:
            parts.append(f"{bad} blocked")
        rest = len(items) - bad
        if rest:
            parts.append(f"{rest} to review")
    else:  # drafts
        if warn:
            parts.append(f"{warn} need evidence")
        rest = len(items) - warn
        if rest:
            parts.append(f"{rest} in flight")
    return " · ".join(parts) if parts else f"{len(items)} item"


def _todos_subtitle(items: list[dict]) -> str:
    """Subtitle for TODOS: count + per-section breakdown."""
    if not items:
        return "all clear"
    n = len(items)
    by_section: dict[str, int] = {}
    for it in items:
        s = str(it.get("section") or "")
        by_section[s] = by_section.get(s, 0) + 1
    if len(by_section) <= 1:
        return f"{n} open"
    parts = [f"{cnt} {section.lower()}" for section, cnt in by_section.items()]
    return " · ".join(parts)


def _reading_subtitle(items: list[dict]) -> str:
    """Subtitle for READING: count, with a kind breakdown if mixed."""
    if not items:
        return "(empty)"
    n = len(items)
    by_kind: dict[str, int] = {}
    for it in items:
        k = str(it.get("kind") or "")
        if k:
            by_kind[k] = by_kind.get(k, 0) + 1
    if len(by_kind) <= 1:
        return f"{n} item" + ("s" if n != 1 else "")
    parts = [f"{cnt} {kind}" for kind, cnt in sorted(by_kind.items())]
    return " · ".join(parts)


def _overnight_subtitle(items: list[dict]) -> str:
    """Subtitle for OVERNIGHT: total + denials, distinct nicknames."""
    if not items:
        return "quiet"
    n = len(items)
    by_nick = {str(it.get("nickname") or "agent") for it in items}
    denials = sum(int(it.get("denial_count") or 0) for it in items)
    parts = [f"{n} session" + ("s" if n != 1 else "")]
    if len(by_nick) > 1:
        parts.append(f"{len(by_nick)} agents")
    if denials > 0:
        parts.append(f"{denials} refusals")
    return " · ".join(parts)


def compose_today_fixture() -> TodayBrief:
    """Return the canonical Today fixture matching the mockup at
    `pact/changes/rebuild-console-views-visual-gated/references/today.png`."""
    return TodayBrief(
        view="today",
        generation_id=4012,
        generated_at="2026-05-21T07:14:00Z",
        eyebrow=TodayEyebrow(
            day="TUESDAY",
            date="21 MAY",
            generation_label="GEN 4012",
        ),
        greeting=TodayGreeting(
            text="Good morning",
            addressee="Iris",
            punctuation=".",
        ),
        stats=[
            TodayStat(id="inbox",        label="INBOX",        value="12",  subtitle="captures"),
            TodayStat(id="awaiting_you", label="AWAITING YOU", value="4",   subtitle="things"),
            TodayStat(id="in_motion",    label="IN MOTION",    value="7",   subtitle="tasks · 4 yours"),
            TodayStat(id="last_walked",  label="LAST WALKED",  value="18m", subtitle="ago"),
            TodayStat(id="agents",       label="AGENTS",       value="3",   subtitle="ran since dinner"),
        ],
        attention=AttentionSection(
            title="WORTH YOUR ATTENTION",
            subtitle="three things, in order",
            items=[
                AttentionItem(
                    id="aniyah-iv-question",
                    rank=1,
                    tone="accent",
                    glyph="chat",
                    title="Aniyah is waiting on your IV question",
                    meta="Slack · paper: do-calc · 4d · 3 unread",
                    sub_italic="blocks your §3.2 draft",
                    primary_action=TodayAction(
                        label="Open thread",
                        verb="open_thread",
                        ref="slack:thread:aniyah/iv-question",
                        style="primary",
                    ),
                    secondary_action=TodayAction(
                        label="Defer 1 day",
                        verb="defer",
                        ref="slack:thread:aniyah/iv-question",
                        style="quiet",
                    ),
                ),
                AttentionItem(
                    id="scout-bareinboim-summary",
                    rank=2,
                    tone="accent",
                    glyph="book",
                    title="Scout drafted a summary of Bareinboim 2014",
                    meta="200 words · 3 highlights · 4h",
                    sub_italic="auto-accepts at 5pm",
                    primary_action=TodayAction(
                        label="Review draft",
                        verb="review_draft",
                        ref="draft:scout/bareinboim-2014",
                        style="primary",
                    ),
                    secondary_action=TodayAction(
                        label="Accept as-is",
                        verb="accept_draft",
                        ref="draft:scout/bareinboim-2014",
                        style="quiet",
                    ),
                ),
                AttentionItem(
                    id="pearl-7-4-resume",
                    rank=3,
                    tone="info",
                    glyph="read",
                    title="Pearl §7.4 — you promised yourself to come back",
                    meta="paper: do-calc · 14h since",
                    sub_italic="resolves your open SCM question",
                    primary_action=TodayAction(
                        label="Resume reading",
                        verb="resume_reading",
                        ref="paper:pearl-2009#7.4",
                        style="primary",
                    ),
                    secondary_action=TodayAction(
                        label="Drop promise",
                        verb="drop_promise",
                        ref="promise:pearl-7.4",
                        style="quiet",
                    ),
                ),
            ],
        ),
        landed_overnight=LandedOvernightSection(
            title="WHAT LANDED OVERNIGHT",
            subtitle="12 captures · 8 auto-routed · 4 want your eye",
            subtotals=[
                LandedSubtotal(label="AWAITING TRIAGE", value=4),
                LandedSubtotal(label="AUTO-ROUTED BY RULE", value=8),
            ],
        ),
    )


__all__ = [
    "TodayBrief",
    "TodayEyebrow",
    "TodayGreeting",
    "TodayStat",
    "TodayAction",
    "AttentionItem",
    "AttentionTone",
    "AttentionSection",
    "LandedOvernightSection",
    "LandedSubtotal",
    "DeskCard",
    "DeskSection",
    "QueueSection",
    "OvernightItem",
    "OvernightSection",
    "ReadingItem",
    "ReadingSection",
    "TodoItem",
    "TodosSection",
    "compose_today_brief_from_view_manifest",
    "compose_today_pi_scientist_from_view_manifest",
    "compose_today_fixture",
]

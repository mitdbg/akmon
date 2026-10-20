"""Attribute ordering for the operator record reads.

`format_attrs` decides the order a record's stored values are returned in
and `body_sections_for` picks out the long-form ones as titled sections.
Both shape a read's JSON; nothing here renders anything, and no page ships
that consumes it yet (PACT operator-ui-search-view).

Neither function decides visibility. `records.py` applies the Nomos policy
first and hands these only values the reader may see, so an ordering rule
can never reveal a value the policy hid.
"""

from __future__ import annotations

from typing import Any


# Large or noisy keys. They are returned in full, but ordered last so a
# reader scanning the response meets the identifying values first.
LOW_PRIORITY_ATTR_KEYS = {
    "text", "rationale", "transcript_path", "state_digest",
    "intent_digest", "evidence_gaps", "next_task_ids",
}

PACT_BODY_ATTR_KEYS = {
    "what_changes_text",
    "why_text",
    "impact_text",
}

GENERIC_BODY_ATTRS = (
    ("summary", "Summary"),
    ("description", "Description"),
    ("body", "Content"),
    ("content", "Content"),
    ("text", "Content"),
    ("message_body", "Message"),
    ("markdown", "Content"),
)
GENERIC_BODY_ATTR_KEYS = {key for key, _label in GENERIC_BODY_ATTRS}

# Identifying keys, returned first in this order.
HIGH_PRIORITY_ATTR_KEYS = [
    "title", "change_id", "status", "label", "message_subject",
    "name", "path", "repo", "git_branch", "agent_nickname",
    "lifecycle", "decision_number", "authored_at", "ended_at",
    "last_modified_at", "email", "display_name",
]


#: The keys a record's own name is read from, first match wins. `display_name`
#: is what a person record carries (Archiv3 `person:*` stores `display_name`,
#: `person_id` and `text` and nothing else), so without it every person was
#: titled by its hash (PACT console-archiv3-read-layer-fixes).
LABEL_KEYS = (
    "title", "change_id", "label", "message_subject", "name", "display_name",
    "path",
)


def label_for(node_id: str, subtype: str, attrs: dict[str, Any]) -> str:
    for key in LABEL_KEYS:
        v = attrs.get(key)
        if v:
            s = str(v).split("\n", 1)[0].strip()
            return s[:80]

    if subtype == "agent_message":
        for key in ("content_excerpt", "message_body", "content", "body", "text"):
            value = attrs.get(key)
            if value:
                excerpt = " ".join(str(value).split())
                if excerpt:
                    return excerpt[:77] + ("…" if len(excerpt) > 77 else "")

    if subtype == "tool_use_event":
        for key in ("tool_name", "operation", "action", "command"):
            value = attrs.get(key)
            if value:
                action = " ".join(str(value).split())
                return f"Tool · {action}"[:80]

    if subtype == "agent_session":
        harness = str(attrs.get("harness") or attrs.get("agent_nickname") or "Agent")
        context = attrs.get("git_branch") or attrs.get("repository") or attrs.get("cwd")
        if context:
            return f"{harness.title()} session · {' '.join(str(context).split())}"[:80]
        return f"{harness.title()} session"

    kind = subtype.replace("_", " ").strip().title() or "Node"
    suffix = node_id.rsplit(":", 1)[-1].strip()
    return f"{kind} · {suffix}"[:80] if suffix else kind[:80]


def format_attrs(
    attrs: dict[str, Any],
    *,
    omit_keys: set[str] | frozenset[str] = frozenset(),
) -> list[dict[str, str]]:
    """Return complete ordered ``[{key, value}]`` pairs for inspection.

    Identifying keys come first, then the remaining keys alphabetically,
    then the noisy technical ones. ``omit_keys`` drops the long-form values
    a caller is taking as body sections instead. Every value that is
    returned is returned whole: this function never truncates or summarises,
    so a caller cannot silently shorten provenance or a structured payload.
    """
    if not attrs:
        return []
    keys = [k for k in attrs.keys() if k not in omit_keys]
    high = [k for k in HIGH_PRIORITY_ATTR_KEYS if k in attrs]
    low = [k for k in sorted(keys) if k in LOW_PRIORITY_ATTR_KEYS]
    mid = [
        k for k in sorted(keys)
        if k not in high and k not in low
    ]
    ordered = high + mid + low
    out: list[dict[str, str]] = []
    for k in ordered:
        v = attrs.get(k)
        if v is None or v == "":
            continue
        s = str(v)
        out.append({"key": k, "value": " ".join(s.split())})
    return out


def body_sections_for(subtype: str, attrs: dict[str, Any]) -> list[dict[str, str]]:
    """Return long-form body sections without flattening or truncation.

    A record's prose -- a summary, a description, a PACT change's proposal
    text projected from pact.yaml -- is returned as titled sections rather
    than as ordinary attribute values, so a caller can present it as content
    without re-deriving which keys hold prose. Values are returned whole.
    """
    labels = (
        [
            ("what_changes_text", "What changes"),
            ("why_text", "Why"),
            ("impact_text", "Impact"),
        ]
        if subtype == "pact_change"
        else list(GENERIC_BODY_ATTRS)
    )
    sections: list[dict[str, str]] = []
    identity_values = {
        str(attrs.get(key, "")).strip()
        for key in ("title", "change_id", "label", "message_subject", "name", "path")
        if attrs.get(key)
    }
    for key, title in labels:
        value = attrs.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        # Several source adapters stamp a one-line `text` search surrogate that
        # merely repeats the title. It is useful for search, not long-form content.
        if key == "text" and text in identity_values:
            continue
        sections.append({"key": key, "title": title, "value": text})
    return sections

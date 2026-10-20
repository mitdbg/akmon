# Behavior contract — <change-id>

States, events, focus, keyboard — the behaviors the TS prop interface in
`ui/spec.md` can't express. The interface is the canonical contract; this file
covers the dynamics.

## `<Component>`

**State machine.** Enumerate states + transitions:
```
closed → opening → idle → <action> → closing → closed
```

**Event handlers.**

| Event | Handler | Side effects |
|---|---|---|
| <click X> | `onX()` | <transition + side effect> |

**Focus management.** On open / while open (trap) / on close (return to
trigger). 

**Keyboard map.** Tab / Escape / Enter on focused buttons (accessibility
table-stakes). Per pact/ui-change-guide.md: do NOT add chord shortcuts
(`A` approve, `M` merge) without a strong user-need argument + its own PACT.

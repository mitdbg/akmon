# Visual contract — <change-id>

Token references, dimensions, layout, density. Reference token **names** from
`tokens/global.tokens.json` (W3C-DTCG) — never inline hex / px / weight
literals (stylelint enforces). This file is the exhaustive token list; the
`design.md` § Visual describes the *concept* behind the choices.

## `<Component>` tokens

| Role | Token | Notes |
|---|---|---|
| Background | `color.surface` | |
| Text | `color.text.primary` | |
| Spacing | `space.4` / `space.5` | header → body → footer rhythm |
| Radius | `radius.md` | |

## Layout / density

Breakpoints, density modes (compact / cozy / comfortable), reflow rules.
Delete this section if the component renders at a single density.

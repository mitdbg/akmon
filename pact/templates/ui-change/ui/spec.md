# UI contract — <change-id>

The **typed contract** for each component this change adds/modifies. This is
the Stage-2 (contract review) artifact. Author each component as a contract
*slice* — TS interface + token bindings + a11y + 1-3 journey scenarios +
anti-patterns — NOT a prose `SHALL` list. The PACT's `requirements` (in
`pact.yaml`) point at this file; this file holds the detail a YAML statement
can't.

> Why slices, not SHALLs: the **TypeScript prop interface** is the behavior
> contract; the **token names** are the visual contract; the **a11y checklist**
> is the a11y contract. Prose is for *why* and *guidance*, not requirement
> enumeration. (See pact/ui-change-guide.md § "What NOT to do".)

## `<Component>` (lifecycle: <stage>)

One sentence: what the component is for. Note its lifecycle stage (see
`ui/lifecycle.md`).

**Behavior contract** (canonical: `<deployment>/src/components/<Component>.tsx`):

```ts
interface <Component>Props {
  /** <doc each prop; the JSDoc is part of the contract> */
  // ...
}
```

**Visual contract** — token bindings (see `ui/visual.md` for the full list):
- <role>: `<token.name>`
- Padding / radius / type: `<space.*>` / `<radius.*>` / `<type.*>`

**Accessibility contract**:
- Role: `<e.g. alertdialog>`
- Focus: `<trap / return-on-close rules>`
- Keyboard: `<Escape / Tab / Enter behavior>`
- ARIA: `<aria-labelledby / aria-describedby / aria-busy>`

**User-journey scenarios** (1-3 max, journey-level — not field-level):

#### Scenario: <the happy path>
- **GIVEN** <state>
- **WHEN** <user action>
- **THEN** <observable outcome>

**Anti-patterns** (when NOT to use this component):
- <case> → use `<other primitive>` instead.

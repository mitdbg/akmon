# Operator UI — local servers and static checks

What remains here after PACT `operator-ui-remove-four-views`: the two server
scripts an operator or agent uses to bring the UI up against a live deployment,
the layout rule, the first-load bundle budget, and the fixture seeder the
audit database tests still read.

The browser audit lane — accessibility, visual regression, Lighthouse, the
Gemini critique, the layout-metric budgets and the graph-canvas pixel parity —
went with the views it covered. Rendered proof now travels with the change that
makes it: each UI PACT records screenshots and a console-error check against a
real published deployment, and the operator signs them off
(`docs/runbooks/agents/ui.md`).

| Script | What it does |
| --- | --- |
| `npm run operator-ui:servers` | Start the read API and the frontend against a live deployment (default `okg-workspace`), then print the URL |
| `npm run operator-ui:servers:stop` | Stop both |
| `npm run operator-ui:layout-check` | Refuse an unknown top-level directory, console code outside `views/` `model/` `api/` `components/` `viewers/` `styles/` (test data may sit in `__fixtures__/`, test files at the console top level), and a route that imports another route; the unit suite runs it (`app/layoutCheck.test.ts`) |
| `npm run operator-ui:build && npm run operator-ui:bundle-budget` | Hold the first-load bundle to the ceilings in `bundle-budgets.json` |
| `seed_fixture.py` | Seed the deterministic fixture deployment the audit database tests read |

`start-servers.sh` sources the deployment DSN from `.okg/local.env` (override
with `OKG_AUDIT_DSN_ENV`, or export `OKG_DSN`), is idempotent, and takes
`OKG_AUDIT_DEPLOYMENT=<name>`.

## Bundle budget

`bundle-budget.mjs` gzips the entry JavaScript and CSS of a production build and
fails when either passes its declared ceiling. The ceilings live in
`bundle-budgets.json` with the date and the change that measured them. This is
what keeps a library loaded by one route — Ant Design, for example — out of the
bundle every view pays for.

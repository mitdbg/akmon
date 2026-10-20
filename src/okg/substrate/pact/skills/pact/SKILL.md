---
name: pact
description: Drive a PACT change end to end — author it, prove it, get it approved, merge it. Use when starting any non-trivial change, or when the user says "author a pact", "create a change", "open a change", "propose X", "verify the task", "attach evidence", "run the check", "close the pact", "is this done yet".
---

# The PACT workflow (v5)

PACT v5 is git-native: the manifest, its receipts, and its approval are files
and git metadata, and one check decides. `pact.yaml` is canonical: edit it
directly. `pact.md` is generated (so is `pact.yaml.lock`); both are local
files and should not be committed. Run the CLI as `okg pact ...` (or `pact ...`). Report progress to
people plainly: lead with the result, translate terms.

If the repository has `pact/AGENTS.md`, read it first: it holds that
repository's own PACT rules. Otherwise this skill is the whole contract.

## Author → prove → check → approve

**1. Author.** Scaffold, then edit `pact/changes/<id>/pact.yaml`:
```bash
okg pact new <id> --title "<title>"      # add --tier N to declare a tier (default 1)
```
Eight fields, unknown keys refused: `id`, `title`, `tier`, `why`,
`affected_code`, `requirements[] {id, statement}`, `tasks[] {id,
requirement, verification}`, `evidence[]`, `decisions[]`. The tier is
computed from the diff (docs/CI/lockfiles 0; tests-only or under 300 source
lines 1; other code, schema, migration, formal 2; a program 3); declare a
higher one only when a reviewer could disagree with a design choice. A task's
`verification` names a REAL check: a test, a row-count delta, a published
generation — never "compiles".

**2. Prove.** Run the named check, then bind its receipt to `HEAD`:
```bash
okg pact evidence attach <id> --kind executed_test --requirement <req> \
  --artifact pact/changes/<id>/evidence/junit.xml --command "<pytest ...>"
```
Kinds: `executed_test`, `formal_check`, `published_generation`,
`design_review`, `manual` (v4's `evidence ingest --formal-report report.yaml`
is `--kind formal_check --artifact report.yaml`; the report's `property_id:`
and `refinement_tests:` rows travel inside that artifact). The row pins the artifact's
sha256 and the commit; a changed artifact or an unreachable commit stops
counting as proof. Write the artifact under `pact/changes/<id>/evidence/`:
attach refuses anything else (`evidence_artifact_not_generated`), because
pinning a living file's digest freezes that file -- the next edit to it
refuses this change forever. To prove something about a living file, write a
note there that cites it by path and heading, quotes the lines the claim
rests on and names the commit you read it at, and attach the note. `check`
refuses a row you add or edit and only warns about one that already merged --
except with no diff base, where it cannot tell the two apart and refuses both.
An artifact, or any file you add under `evidence/`, that holds an unmasked
password, bearer token, access token or private key is refused
(`evidence_artifact_contains_secret:<path>:<line>`): mask the value as `******`.

**3. Check.** The one gate; there is no `deferred`:
```bash
okg pact check <id> --strict --json
okg pact view <id>
okg pact view <id> --format markdown   # the review page, pact/changes/<id>/pact.md
```
`check` judges the branch's diff against its merge base with `origin/dev` or
`dev`, else the default branch (`origin/HEAD`, `main`, `master`), or the
`--base` you pass. Work on a branch: on the base branch itself there is no
diff, and `--strict` refuses `diff_base_unavailable`.
Lenient `check` refuses an invalid manifest, a broken receipt, or a declared
tier below the computed one, and warns on unproven requirements and a missing
approval. `--strict` (what CI, preflight and the merge run) charges the
effective tier — the higher of declared and computed: tier 0 needs neither
proof nor approval, tier 1 needs an approval (unproven requirements only
warn), tier 2 and 3 need an approval and bound passing evidence for every
requirement (CI workflow files are tier 1, not 0). CI that runs the check can additionally require
both on any diff outside `pact/`, refuses a pull request that changes code
and ships no manifest (`pact_required`), and refuses code no changed
manifest claims in `affected_code` (`affected_code_uncovered`).
Read `verdict` and
`blocked_by`, never `ok` alone; exit 0 is recorded, 1 refused, 2 a usage
error, and there is no exit 3 (nothing defers). Fix what `blocked_by` names;
do not rerun the same command.

**4. Approve.** The operator says "approve" in chat; commit the manifest (the
command refuses a dirty index), then run one command:
```bash
okg pact approve <id>
```
That writes an empty commit trailing `Pact-Approved: <id> <intent-digest>`,
and `check --strict` then passes. Say plainly what it is: an operator record
(`authority: operator_record`), not third-party review — CI proof covers the
code. An APPROVED pull-request review carrying the same line (`okg pact
digest <id>` prints it) is optional extra authority and outranks the trailer;
a `decisions[]` row never binds. Either binds only while the reviewed intent
is unchanged (editing a requirement, a task verification, or the tier makes
it stale) and only until code is pushed after it; attaching evidence is fine.

**5. Merge.** Commit with a `Pact-Id: <id>` trailer and target the
repository's integration branch. Where CI archives merged changes, the
manifest moves to `pact/archive/<id>/` unchanged.

## What is gone

`gate`, `decide`, `hooks`, `sync`, `migrate-version`, closure intents, the
decision ledger, and the graph projection as a gate. Calling a removed verb
returns `verb_removed:<verb>` and names the replacement. A v4 manifest
(`pact_version: 4`) still uses its v4 verbs until it is migrated:
```bash
okg pact migrate <id>            # dry run; add --apply to write
```

## Measure it

Every invocation appends one line to `.okg/pact-usage.jsonl` (a local file;
do not commit it); `okg pact usage` recomputes the usage measures from it.

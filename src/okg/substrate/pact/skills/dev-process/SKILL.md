---
name: dev-process
description: Take an issue or named change through PACT, independent design review, implementation, independent code review, verification, and a pull request against dev. Use when the operator says /dev-process, do the next one, work on an issue, or run the standing development process.
---

# Development process

Own one issue or PACT through a review-ready pull request. Do not make the
operator repeat the standing process.

## 1. Resolve the target

Use the issue, current branch, active PACT, review inbox, and session state to
resolve terse instructions such as “continue” or “do the next one.” State the
target in one plain sentence. Ask only when two plausible targets would lead to
materially different work.

The issue must carry a milestone and a priority label before work starts; set
them by the repository's own issue rules if they are missing, and keep them as
you found them otherwise.

## 2. Create or resume the PACT

Use the `pact` and `agent-workflow` skills. Start with:

```bash
uv run okg pact view <id> --format=bootstrap --json
```

If no PACT exists, create the smallest useful one. Start with the root cause,
observable behavior, and verification of real output.

## 3. Review the design

Give an independent agent the PACT, relevant code, and one question: does this
design meet the request safely and completely? Require findings with file and
line references. Refine the PACT before implementation when a finding changes
scope, safety, or proof.

## 4. Implement and prove

Claim the mutation owner slot for the pull request before your first push, and
release it at merge or handoff:

```bash
uv run okg pact coord owner --pull-request <n> --owner <principal> --json
```

One pull request has one mutation owner; a second claim is refused. If you are
coordinating other agents, you are not the owner — dispatch a worker. Report
`idle` and `blocked` with a reason in your message to the operator rather than
letting them ask; `uv run okg pact coord report blocked --reason "<why>"`
formats one and enforces the reason. The `agent-workflow` skill has the
coordinator contract.

Work one PACT task at a time. Run the named check, record its result, and pass
the task-done check before moving on. Do not use hidden chat hooks or silent
fallbacks as proof.

For graph changes, publish and verify the expected count, metric, queue, or
health change. For UI changes, render and inspect the result.

## 5. Review the code

Give an independent agent the final diff and every PACT requirement. Findings
come first, ordered by severity, with file and line references. Fix or disclose
every finding. A bare “pass” is not a review. Before writing the pull request,
sort every retained item into **Open review issues** or
**Resolved during review**. Record the result as `Review performed: Yes.`; if
review could not run, write `Review performed: No — <reason>.` and keep it open.

## 6. Open the pull request

Write the body in the order the repository's pull-request template and
reporting rules require, if it has them. Keep **Reader summary** short and plain;
put commands, changed paths, and review history in **Technical record**. Every
open review issue also appears in **What remains** as a plain-language
consequence. Run:

```bash
git diff --check
```

then the repository's own pull-request body and preflight checks, as its
root agent instructions name them.

Commit with a `Pact-Id: <id>` trailer. Push and open the PR with `--base dev`.
Check hosted results for the exact commit. Do not call missing, cancelled,
timed-out, red, or pending checks green.

Do not rerun the Tests workflow on the same commit to seek a different result.
Push a fix unless the logs prove a named runner-boundary fault. For that sole
exception, a maintainer applies `runner-fault` before the rerun and removes it
after the authorized attempt.

## 7. Report

Lead with the outcome and current state. Give the behavior before and after,
the proof, any remaining risk, and the next action. Translate internal terms
before they reach the operator.

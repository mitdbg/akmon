---
name: agent-workflow
description: Start, coordinate, verify, and report nontrivial PACT work in this repository. Use for task kickoff or resume, delegated work, status or blocker answers, review readiness, merge readiness, and recovery from an interruption.
---

# Agent workflow

This is the short entry point. Use the linked runbooks only for the situation
at hand; do not load every workflow document into every task.

## Start or resume

1. Run:

   ```bash
   uv run okg pact view <pact-id> --format=bootstrap --json
   ```

2. Read the current task, blockers, proof gaps, Git state, and suggested next
   command. A nonzero PACT exit is a structured refusal, not a reason to
   repeat the same command. Read `blocked_by`. There is no `deferred`
   verdict in v5: a verb either recorded or refused.
3. Check graph readiness only when graph history could change the work. If it
   is healthy, use the `okg-query` skill for bounded context. If it is stale or
   unavailable, say so and continue with Git, files, tests, and live publish
   checks.
4. Work on one PACT task at a time.
5. When you open a GitHub issue, give it a milestone, one type label, and one
   priority label, following the repository's own issue rules if it has them.
   When you pick one up, keep the milestone and priority you found. Never add a
   `sprint:*` label.

For graph context, use the `okg-query` skill; `docs/use/search/index.md`
describes the read surfaces.

## Complete one task

1. Inspect current code and the task’s verification clause.
2. Make the smallest change that meets the requirement.
3. Run the named check against real behavior.
4. Attach the receipt:

   ```bash
   uv run okg pact evidence attach <pact-id> --kind executed_test \
     --requirement <req-id> --artifact <junit.xml> --command "<what you ran>"
   ```

5. Run `uv run okg pact check <pact-id> --strict --json` and fix what it
   names. There is no task-completion verb: a requirement is proven when a
   receipt bound to a commit in range says so.

Use the `pact` skill (and the repository's own PACT instructions, if it keeps
any) for PACT commands and proof rules. Do not copy that manual into chat or
another instruction file.

## Delegate only bounded work

Read the repository's own coordination rules, if it has them, when work is
actually split.

- Give each child one question, clear ownership, and the expected answer.
- Avoid overlapping edits and duplicate investigation.
- Continue independent local work while children run.
- Wait with the longest useful bounded wait. Do not poll unchanged state in a
  tight loop or narrate empty waits.
- Require the child to return its answer, evidence, uncertainty, and changed
  files.
- Check important claims, then integrate the result into one human report.

Child output is input, not final proof.

## Coordinator contract

If you dispatch work, you coordinate it. You do not also edit, commit, or push.

- A coordinator never holds a mutation lease. `chief` and `sprint` assignments
  are refused a `claim` or `renew`. A coordinator that starts editing has lost
  its lane: retire it and dispatch a worker.
- Exactly one mutation owner per pull request. Claim before the first push and
  release at merge or handoff. A second owner is refused.

  ```bash
  uv run okg pact coord owner --pull-request <n> --owner <principal> --json
  ```

- Report `idle` and `blocked` yourself, with a reason and a bounded next
  action, in the message the operator actually reads. Never leave them to
  discover either by asking. The command below only formats and enforces the
  reason; it records nothing.

  ```bash
  uv run okg pact coord report blocked --reason "<why>" --json
  ```

- Cap top-level sessions. Read-only work goes to subagents; it costs no lease.
- Standing approval authority comes from the dispatch prompt. If you hold it,
  record the verdict yourself instead of stalling on a prompt.
- Retire an agent on a missed heartbeat and re-dispatch from a verified
  worktree: absolute path, branch, and `HEAD` against `origin/dev` before the
  first edit. Verify a handoff by worktree state.

## Report to the operator

Follow the repository's own reporting rules, if it has them.

Lead with the result and current state. Add before-and-after behavior, proof,
blockers, risks, or a next action only when they help the reader. Translate
internal terms. Do not dump a machine packet, graph identifiers, hashes, child
statuses, or raw logs into ordinary chat.

For a true interruption or ownership transfer, preserve the detailed recovery
record those rules describe, then give the operator a short summary.

## Ask for a PACT decision

- Use `uv run okg pact view <id> --json` for the brief: state, intent digest,
  proven requirements, approval, next step. Show raw YAML only when asked.
- Use `uv run okg pact check <id> --strict --json` to say what is actually
  missing. Answer from `blocked_by`, not from a headline.
- Record an operator’s “approve” with `uv run okg pact approve <id>`: it writes
  the `Pact-Approved` commit trailer that `pact check --strict` binds. Say what
  it is — the operator’s recorded decision, not third-party review. Echo the
  recorded result; never claim a decision applied when the verb refused.
- A non-author `APPROVED` pull-request review carrying the same line (get it
  with `uv run okg pact digest <id>`) is a second principal, so it outranks the
  trailer where both exist. Never approve your own change.
- Either source binds only while the digest matches the manifest’s current
  intent and the reviewed commit is still an ancestor of the head with no code
  changed since.
- Explain why an item ranks first before recommending it.

There is no decision ledger and no queue verb. v5 removed `decide`, `gate`,
`hooks` and every review-queue view; calling one returns `verb_removed`.
The `pact` skill has the detailed rules.

## Ready for review or merge

Trailer the commit that records the work with `Pact-Id: <id>`. It names the
manifest this pull request is FOR: that one, and any manifest the pull request
adds, get the full tier, proof and approval check. A manifest that already
existed and is only touched in passing is *carried* -- it must still load as
v5, but its tier, proof and approval belong to the branch that authored it,
and its `affected_code` cannot cover this pull request's code. An id naming no
changed manifest is refused (`pact_id_unmatched:<id>`), so fix a stale or
mistyped trailer rather than re-running. With no trailer, every changed
manifest is checked.

Before push:

```bash
git diff --check
```

then the repository's own pull-request body and preflight checks, as its
root agent instructions name them.

Also run an adversarial agent review against every PACT requirement. Resolve or
disclose every issue. In the PR body, put the plain outcome in **Reader summary**
and exact proof in **Technical record**. Sort retained review notes by
their present state: unresolved items go under **Open review issues** and in
**What remains** as a plain-language consequence; fixed notes go under
**Resolved during review** with their disposition. Start **Review status** with
`Review performed: Yes.` or `Review performed: No — <reason>.` Pull requests
target `dev`.

After push, the mutation owner holds the exact commit through hosted CI:

1. Read the pull request's current `headRefOid`, select the hosted run for that
   commit, and record the pull request, head, run id, and run attempt.
2. In the owning session, run
   `gh run watch <run-id> --exit-status --interval 30`. Do not chain it with
   `&&`: a failed run must wake the owner too.
3. Immediately after it returns, record `observed_at`, re-read the run and the
   pull request head, and reject head drift or a nonterminal result. Notify the
   coordinator at once with the exact head, run, terminal verdict, timestamps,
   and `wake_reason: run_completion`.
4. Record the event in an `okg.ci_completion_reaction_receipts.v1` receipt and
   validate it with the repository's receipt validator, if it has one. Missing
   logs, cancellations, and timeouts remain unresolved.

If the owning session cannot hold a blocking watch, declare one owner, a
deadline, and bounded backoff capped at 60 seconds. Each observation still
re-reads the run and pull request head. Use `wake_reason: bounded_backoff` on a
terminal observation. A deadline or transport-retry exhaustion ends with an
explicit failure notification; it never falls back to waiting for a goal tick.

Do not rerun a failed Tests workflow on an unchanged commit. Diagnose the
failure and push a fix. The only exception is a failure proven from the logs to
be at the runner boundary: name that fault in the pull request, have a
maintainer apply the exact `runner-fault` label, rerun once, and have the
maintainer remove the label after the authorized attempt.

Do not rebase merely because `dev` advanced: rebase only for a conflict or when
the branch is more than 20 commits behind. An exact-head hosted-green,
mergeable pull request can merge while `dev` has advanced. A new commit or
rebase invalidates the old result. Use the reporting runbook's recovery record
when ownership must transfer.

## UI and graph changes

- UI work follows the repository's own UI rules, if it has them, and requires
  rendered visual proof.
- Graph behavior needs a real publish plus the expected count, metric, queue,
  or health result. Use the `okg-query` skill to read it.

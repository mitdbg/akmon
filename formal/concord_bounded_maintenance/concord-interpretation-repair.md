---
audience: developer
type: explanation
canonical_id: concord-interpretation-repair-design
---
# How Concord builds and updates consistency checks

Concord turns source material into statements, finds statements worth comparing,
and records what those comparisons establish. When sources change, it updates
the affected checks. An optional repair step proposes which statements to accept
when a declared rule forbids accepting them together.

This is a proposed design. Existing Concord proofs cover narrower models; they
do not prove the complete process described here.

## 1. Build the checks

**Input:** a fixed source version, fixed interpretation, matching and checking rules, and a fixed
collection of stored model answers.

1. **Read the source units.** Identify the documents, code, receipts or records
   being checked. Keep their revisions, exact supporting locations and context.
   A shared glossary or entity map is a dependency too.
2. **Extract statements.** Record their subject, scope, time and units, together
   with the evidence supporting them. Preserve alternative readings when meaning
   is ambiguous. Distinguish “read and found nothing” from “not read” or “failed.”
3. **Find comparisons.** Enumerate a declared finite set of possible comparisons,
   then decide which qualify. For example, compare launch dates for the same
   project. Keep comparisons whose project identity remains unresolved. An
   unfinished source may hide more statements, so checking every known pair
   does not establish that the whole corpus is conflict-free.
4. **Run the checks.** Record exactly what each check establishes: a relationship,
   a model-reported disagreement, observed test behavior, a checked theorem, or
   a violation of a hard rule. One kind of evidence does not become another.
5. **Save the findings.** Attach the source/rule/answer versions, supporting
   evidence, conditions and dependencies. Report unresolved work as pending or
   failed; retained older findings remain explicitly out of date. Known
   violations are valid findings and can be saved.

**Output:** findings and a record of what was checked, what remains unchecked,
and which inputs could change each result.

A definitive conclusion must hold for every possible reading allowed by its
stated conditions. No compatible reading means the interpretation is inconsistent,
not that all checks passed. Enumerating every possibility requires configured
finite limits; otherwise the checker must conservatively leave work unresolved.
“No hard-rule violations” additionally requires complete relevant coverage and no hard
violations. Missing model answers and failures never count as success.

Model calls happen outside this evaluation. Collect unanswered or retryable
requests, spend the available budget, then start an evaluation with a new fixed
answer collection. Reuse an answer only for the same complete request, including
context, prompt and model settings. Preserve the first stored successful answer;
a retry cannot change answers underneath a running check.

## 2. Update the checks

**Input:** previous findings, their dependencies, and new fixed source/rule/answer
versions.

1. **Find the changes.** Include deleted statements, changed context, changed
   rules and changed answers—not just new records.
2. **Replace affected statements.** Remove old contributions and add new ones.
   Keep independent support from other sources: deleting one citation must not
   remove a statement still supported elsewhere.
3. **Find every affected comparison.** Follow both old and new dependencies.
   Moving a statement between projects affects both projects. Discover newly
   possible pairs as well as revisiting existing ones.
4. **Recheck and carry changes forward.** Each stage reads the updated output of
   earlier stages. Pass additions and removals onward. Recheck whole affected
   groups when a total or winning value could change.
5. **Save one versioned result.** Refresh findings, dependencies and answer
   references together. Keep partial work distinct from the last complete
   checkpoint. A complete evaluation may contain violations; saving it does
   not assert that the graph is clean.

**Required guarantee:** updating produces the same findings, coverage and pending
or failed states as checking the new inputs from scratch, using the same answers.
Scheduling history and the older checkpoint are separate from that comparison.

This requires complete dependency tracking. If completeness cannot be established,
recheck the broader scope or the entire input. Treating a missing dependency as
proof that nothing changed is unsafe. Unknown extraction or grouping must also
carry uncertainty into affected downstream checks.

## 3. Propose acceptance repairs, optionally

**Input:** a finite, fully known set of interpreted statements, hard forbidden
combinations, protected statements, and a declared repair preference.

A repair chooses which statements to accept. It does not choose what a document
meant, rewrite source evidence, or establish which statement is true.

1. **Validate the rules.** This first design supports deletion-only acceptance
   choices under rules that forbid positive combinations of facts. Removing
   facts cannot create another such violation. Missing-evidence obligations and
   unresolved model judgments need separate handling.
2. **Enumerate conflicts.** Find every forbidden combination. Unknown statements
   or missing conflict records prevent a certified consistent repair. A protected
   statement absent from the input is an invalid policy reference; a forbidden
   combination made entirely of protected statements makes repair impossible.
3. **Find the permitted repairs.** Enumerate rejection choices that retain all
   protected statements and avoid every conflict. Return either all repairs
   with no unnecessary rejection, or all repairs with minimum declared rejection
   cost. Those are different preferences. Costs are nonnegative; zero costs can
   create extra tied choices. Report impossibility or computation failure
   explicitly.
4. **Reuse unchanged independent groups.** Update groups connected by old or new
   conflicts, including groups that merge or split and isolated statements.
   Costs and protection must be local; global quotas require full computation.
   Handle global contradictions before grouping. Fall back to full repair when
   conflict tracking or reuse cannot be justified.

**Output:** proposed acceptance choices, their rejected statements and reasons.
An answer is guaranteed across repairs only if it holds in every repair selected
by the preference—not just one solver result. Alternative interpretations add a
second requirement: it must hold across every relevant reading too. Unavailable
or impossible cases cannot be silently excluded.

## Example and limits

Two reports disagree about a project's launch date. Concord keeps both source
statements and compares their project/time scopes. When one report changes,
Concord removes its old contribution, discovers new comparisons and updates the
findings. Optional repair might accept either conflicting date; it cannot tell
which report is correct.

Work can remain expensive. Comparing every pair can require quadratic work;
a larger comparison can require more. Repair choices can grow exponentially,
even when separate groups are easy to solve. Broad context changes and full
fallbacks have broad cost. There is no promise that every small edit is cheap.

The Concord design, its proof work and the checked restricted models in
`formal/concord_bounded_maintenance/program/README.md`
provide the starting point. Complete extraction/comparison coverage, dependency
tracking, repair selection and agreement with a full check remain proof
obligations for this extension.

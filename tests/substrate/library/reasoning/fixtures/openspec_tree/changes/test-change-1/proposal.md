---
hypotheses_tested:
  - test-hyp-1
project: okg-substrate
---
# Change: Test change with hypotheses_tested frontmatter

## Why

To verify that the reasoning source adapter extension emits
`Hypothesis supported_by AcceptanceCriterion` edges when a
change's frontmatter declares `hypotheses_tested:`.

## What Changes

- Tests the cross-link rule.

## Impact

- One Hypothesis, three AcceptanceCriteria; three new supported_by edges.

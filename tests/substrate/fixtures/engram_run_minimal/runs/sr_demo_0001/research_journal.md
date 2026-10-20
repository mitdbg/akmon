---
run_id: sr_demo_0001
problem_name: cost-tuned-router-design
agent_count: 1
---

# Research journal — sr_demo_0001

## Iteration 1

Started from a baseline router that round-robins across backends.
Hypothesized that a cost-tuned weighted-random policy would
outperform round-robin on the cost objective. Implemented and ran
two candidate snapshots (Python and C++) and measured both via
the cost simulator.

## Findings so far

The Python candidate landed at score 0.42; the C++ candidate at
0.38. C++ wins by a margin worth investigating, but the
candidate's behavior on the tail latency p99 hasn't been
characterized — see the agent_0 summary's failed-approach note.

This file is also referenced from `[@vale1986storage]`
when discussing the load-balancing literature.

# consistency-fixture

The differential-consistency fixture deployment: a deliberately tiny, source-less
deployment that wires the claim plane end to end for the first time. It loads the
`extraction` + `consistency` ontology modules, registers the `claim_effective` and
`claim_projection` maintained derivations (the latter opted into repair-selection
materialization, so a full recompute records the selected repair `r*` into
`okg.repair_selections`), and declares one claim-state IR check over
`okg.claim_projection` as the read-side consumer that proves detection reads the
resolved claim state. It ingests no sources — the wire test seeds
`consistency_claim` / `predicate_policy` / `claim_precedent` nodes directly and
publishes. PACTs: `consistency-claim-graph-binding` (task.binding.wire-deployment),
`consistency-repair-objective` (task.repair.materialize).

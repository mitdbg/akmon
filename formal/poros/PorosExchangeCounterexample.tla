---- MODULE PorosExchangeCounterexample ----
EXTENDS PorosExchange

\* Negative-check module for the Poros proof gate. A signed receipt is
\* intentionally not publication proof; a receiver may accept a message
\* for admission before any Nomos decision, admission effect, publish
\* effect, receiver generation, or live rows exist. This broken
\* invariant asserts the opposite, so TLC must produce a counterexample.
BrokenReceiptImpliesPublication ==
  \A m \in Messages:
    receipts[m].status = "accepted_for_admission" =>
      m \in publishedMessages

====

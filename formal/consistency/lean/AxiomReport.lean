/-
  Axiom report for the Concord currency/T1 development. Run with
  `lake env lean AxiomReport.lean`; the recorded output is the
  formal_check evidence's axiom report. Target base: `propext` and
  `Quot.sound` only (matching Separability); `Classical.choice` appearing
  here requires a recorded reason.
-/
import Currency

#print axioms Concord.Currency.t1_skip_sound
#print axioms Concord.Currency.currency
#print axioms Concord.Currency.t1_cross_pass
#print axioms Concord.Currency.stamp_le_of_live
#print axioms Concord.Currency.agrees_of_changes_disjoint
#print axioms Concord.Currency.Finset.inter_eq_empty_iff

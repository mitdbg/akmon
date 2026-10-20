/-
  Certificate currency and T1 (skip soundness) — Concord invariant engine.

  PACT `concord-formal-currency-t1` (slice 4 of `concord-invariant-engine`).
  Statements per `pact/changes/concord-formal-currency-t1/designs/
  formal-statements.md` v2 — the killRule parenthesization and the basedAt0
  induction base are review findings (EWD-57..61) and are kept exactly.
  Design canon: invariant-engine-design.md v8 §4 (certificates, membership
  versions, the currency invariant) and §8 (T1).

  Like the paper repository's Separability.lean: Lean 4 CORE ONLY, no
  Mathlib, so the axiom base can stay at `propext` + `Quot.sound`. The
  design's `Finset` is carried by a LIST-BACKED finite-set structure defined
  here (`Concord.Currency.Finset`): membership is list membership,
  intersection is `filter`, and the design's disjointness formula
  `R.facts ∩ Δ.facts = ∅` is stated literally over it; the bridge lemma
  `inter_eq_empty_iff` proves that formula equivalent to propositional
  disjointness, so nothing rests on the representation. This carrier choice
  (a review-visible implementation decision, not a statement change) is
  disclosed in the slice's review memo.

  What the types enforce (design rule → type): certificates may name
  membership versions only through `CollId` — join-defined enumerations have
  no `CollId`, so no certificate can name their membership (the join
  demotion). What is NOT proved here, per the mapping table (§4 of the
  slice design): `ext` for real queries, version faithfulness, `Δ`
  over-approximation, and recorded-correctness — engine premises, listed
  and mutation-tested (E1), never silently assumed proved.
-/

namespace Concord.Currency

/-- A list-backed finite set: the core-only carrier for the design's
`Finset`. Only `∈`, `∩`, and `= ∅` are ever used. -/
structure Finset (α : Type) where
  elems : List α

namespace Finset

instance {α : Type} : Membership α (Finset α) :=
  ⟨fun s a => a ∈ s.elems⟩

instance {α : Type} : EmptyCollection (Finset α) := ⟨⟨[]⟩⟩

/-- Intersection by filtering (needs decidable equality, supplied as an
instance hypothesis — never `Classical.choice`). -/
def inter {α : Type} [DecidableEq α] (s t : Finset α) : Finset α :=
  ⟨s.elems.filter (fun a => decide (a ∈ t.elems))⟩

infixl:70 " ∩ " => Finset.inter

theorem mem_def {α : Type} (s : Finset α) (a : α) :
    a ∈ s ↔ a ∈ s.elems := Iff.rfl

private theorem filter_not_mem_eq_nil {α : Type} [DecidableEq α]
    (t : Finset α) :
    ∀ l : List α, (∀ a, a ∈ l → a ∉ t) →
      l.filter (fun a => decide (a ∈ t.elems)) = []
  | [], _ => rfl
  | head :: tail, h => by
    have hhead : head ∉ t.elems := h head List.mem_cons_self
    have htail : tail.filter (fun a => decide (a ∈ t.elems)) = [] :=
      filter_not_mem_eq_nil t tail
        (fun a ha => h a (List.mem_cons_of_mem _ ha))
    simp [List.filter, decide_eq_false hhead, htail]

/-- The bridge: the design's `s ∩ t = ∅` is exactly propositional
disjointness. -/
theorem inter_eq_empty_iff {α : Type} [DecidableEq α] (s t : Finset α) :
    s ∩ t = (∅ : Finset α) ↔ ∀ a, a ∈ s → a ∉ t := by
  constructor
  · intro h a ha hat
    have hmem : a ∈ (s ∩ t).elems :=
      List.mem_filter.mpr ⟨ha, by simpa [Finset.mem_def] using hat⟩
    rw [h] at hmem
    cases hmem
  · intro h
    have hnil : s.elems.filter (fun a => decide (a ∈ t.elems)) = [] :=
      filter_not_mem_eq_nil t s.elems h
    show Finset.mk _ = Finset.mk _
    rw [hnil]

end Finset

/-! ## The formal model (slice design §2) -/

variable {Fact Value CollId Version : Type}

/-- Check-visible state: a value for every fact and a membership version for
every selection-defined collection. Join-defined enumerations have no
`CollId` — the type IS the join demotion. -/
structure State (Fact Value CollId Version : Type) where
  val : Fact → Value
  mem : CollId → Version

/-- The read-set object a certificate stores: instance facts plus selection
membership versions (design v8 §4). -/
structure ReadSet (Fact CollId : Type) where
  facts : Finset Fact
  colls : Finset CollId

def agreesOn (S S' : State Fact Value CollId Version)
    (R : ReadSet Fact CollId) : Prop :=
  (∀ f, f ∈ R.facts → S.val f = S'.val f) ∧
  (∀ c, c ∈ R.colls → S.mem c = S'.mem c)

/-- One evaluation: its read set, its result function, and T1's premise (a)
— extensionality over the read set. For formal invariants `ext` is
discharged from query semantics (given version faithfulness); for
everything else it is the assumed, mutation-tested premise (design §9). -/
structure Evaluation (Fact Value CollId Version Result : Type) where
  read : ReadSet Fact CollId
  run  : State Fact Value CollId Version → Result
  ext  : ∀ S S', agreesOn S S' read → run S = run S'

/-- A recorded certificate: the evaluation, its recorded result, and the
committed pass of recording. -/
structure Certificate (Fact Value CollId Version Result : Type) where
  eval   : Evaluation Fact Value CollId Version Result
  result : Result
  stamp  : Nat

structure ChangeSet (Fact CollId : Type) where
  facts : Finset Fact
  colls : Finset CollId

/-- The design's disjointness formula, stated literally. -/
def disjoint [DecidableEq Fact] [DecidableEq CollId]
    (R : ReadSet Fact CollId) (Δ : ChangeSet Fact CollId) : Prop :=
  R.facts ∩ Δ.facts = (∅ : Finset Fact) ∧
  R.colls ∩ Δ.colls = (∅ : Finset CollId)

/-- T1's premise (b): `Δ` over-approximates every actual change. The
engine's obligation to compute such a `Δ` is an assumption in the mapping
table, not a theorem here. -/
def changesWithin (S S' : State Fact Value CollId Version)
    (Δ : ChangeSet Fact CollId) : Prop :=
  (∀ f, f ∉ Δ.facts → S.val f = S'.val f) ∧
  (∀ c, c ∉ Δ.colls → S.mem c = S'.mem c)

/-! ## Shared kernel: untouched reads agree -/

theorem agrees_of_changes_disjoint [DecidableEq Fact] [DecidableEq CollId]
    {S S' : State Fact Value CollId Version}
    {R : ReadSet Fact CollId} {Δ : ChangeSet Fact CollId}
    (hΔ : changesWithin S S' Δ) (hdisj : disjoint R Δ) :
    agreesOn S S' R := by
  refine ⟨fun f hf => ?_, fun c hc => ?_⟩
  · exact hΔ.1 f ((Finset.inter_eq_empty_iff _ _).mp hdisj.1 f hf)
  · exact hΔ.2 c ((Finset.inter_eq_empty_iff _ _).mp hdisj.2 c hc)

theorem agreesOn_refl (S : State Fact Value CollId Version)
    (R : ReadSet Fact CollId) : agreesOn S S R :=
  ⟨fun _ _ => rfl, fun _ _ => rfl⟩

theorem agreesOn_trans {S₁ S₂ S₃ : State Fact Value CollId Version}
    {R : ReadSet Fact CollId}
    (h₁₂ : agreesOn S₁ S₂ R) (h₂₃ : agreesOn S₂ S₃ R) :
    agreesOn S₁ S₃ R :=
  ⟨fun f hf => (h₁₂.1 f hf).trans (h₂₃.1 f hf),
   fun c hc => (h₁₂.2 c hc).trans (h₂₃.2 c hc)⟩

/-! ## Result 1 — T1, skip soundness for one step -/

/-- **T1 (skip soundness).** If the change set over-approximates every
actual change and the evaluation's read set is disjoint from it, the
evaluation's result is unchanged: skipped work is unaffected work
(design v8 §8, T1). -/
theorem t1_skip_sound [DecidableEq Fact] [DecidableEq CollId] {Result : Type}
    (e : Evaluation Fact Value CollId Version Result)
    (S S' : State Fact Value CollId Version) (Δ : ChangeSet Fact CollId)
    (hΔ : changesWithin S S' Δ) (hdisj : disjoint e.read Δ) :
    e.run S' = e.run S :=
  (e.ext S S' (agrees_of_changes_disjoint hΔ hdisj)).symm

/-! ## Result 2 — certificate currency across passes -/

section Currency

variable [DecidableEq Fact] [DecidableEq CollId] {Result : Type}

/-- Live-certificate sets per committed pass, as predicates (the design's
`c ∈ L n`). -/
abbrev CertSet (Fact Value CollId Version Result : Type) :=
  Certificate Fact Value CollId Version Result → Prop

variable (σ : Nat → State Fact Value CollId Version)
variable (Δ : Nat → ChangeSet Fact CollId)
variable (L : Nat → CertSet Fact Value CollId Version Result)

/-- The kill rule. Parenthesization matters (review finding): without the
explicit parentheses, `∧` binds tighter than `∨` and the
surviving-⇒-untouched clause would constrain only new certificates — a
never-killing system would satisfy the rule and currency would be
unprovable. The conjunction is the rule. -/
def killRule : Prop :=
  ∀ n, ∀ c, L (n+1) c →
    (L n c ∨ c.stamp = n+1)                       -- live certs persist or are new
    ∧ (L n c → disjoint c.eval.read (Δ n))        -- surviving ⇒ untouched this pass

/-- The induction base (review finding): without it a certificate with
`stamp > n` may sit in `L n` and the claim demands agreement between a
future state and the current one from nothing. -/
def basedAt0 : Prop := ∀ c, ¬ L 0 c

/-- Derived invariant the measure needs: every live certificate's stamp is
in the past. -/
theorem stamp_le_of_live (hkill : killRule Δ L) (hbase : basedAt0 L) :
    ∀ n, ∀ c, L n c → c.stamp ≤ n := by
  intro n
  induction n with
  | zero => intro c hc; exact absurd hc (hbase c)
  | succ n ih =>
    intro c hc
    rcases (hkill n c hc).1 with hlive | hnew
    · exact Nat.le_succ_of_le (ih c hlive)
    · exact Nat.le_of_eq hnew

/-- **Certificate currency** (design v8 §4): at every commit, every live
certificate's named facts and collection versions are unchanged since its
recording — the inductive invariant that lets the one-pass theorems compose
across passes. -/
theorem currency
    (hchange : ∀ n, changesWithin (σ n) (σ (n+1)) (Δ n))
    (hkill : killRule Δ L) (hbase : basedAt0 L) :
    ∀ n, ∀ c, L n c → agreesOn (σ c.stamp) (σ n) c.eval.read := by
  intro n
  induction n with
  | zero => intro c hc; exact absurd hc (hbase c)
  | succ n ih =>
    intro c hc
    have hk := hkill n c hc
    rcases hk.1 with hlive | hnew
    · -- surviving: current up to pass n, and untouched by pass n's change.
      have hstep : agreesOn (σ n) (σ (n+1)) c.eval.read :=
        agrees_of_changes_disjoint (hchange n) (hk.2 hlive)
      exact agreesOn_trans (ih c hlive) hstep
    · -- newly recorded this pass: current by reflexivity.
      rw [hnew]
      exact agreesOn_refl _ _

/-- **Cross-pass reuse (T1 with currency).** A certificate recorded
correctly at its stamp keeps its recorded result in the CURRENT committed
state — the exact statement design v8 §8 says T1 provides "with certificate
currency … in any earlier pass". Recorded-correctness is a substantive
engine hypothesis (the pass's fixpoint discipline); a re-anchored
certificate's recorded-correctness at its new stamp is this theorem applied
to its predecessor, not an extra assumption. -/
theorem t1_cross_pass
    (hchange : ∀ n, changesWithin (σ n) (σ (n+1)) (Δ n))
    (hkill : killRule Δ L) (hbase : basedAt0 L)
    {n : Nat} {c : Certificate Fact Value CollId Version Result}
    (hlive : L n c)
    (hrec : c.result = c.eval.run (σ c.stamp)) :
    c.eval.run (σ n) = c.result := by
  have hagree := currency σ Δ L hchange hkill hbase n c hlive
  have := c.eval.ext (σ c.stamp) (σ n) hagree
  rw [hrec, this]

end Currency

end Concord.Currency

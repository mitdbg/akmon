import Std.Tactic
namespace ConcordExtract
-- Algorithm 1 (EXTRACT) of concord-design.tex. A source item is opaque content
-- plus context; the producer is fixed, so a request is (source id, item).
abbrev SourceId := Nat
abbrev Item := Nat
abbrev Sources := SourceId → Option Item
abbrev Request := SourceId × Item

inductive Answer where
  | success (rows : List Nat)
  | failure
  deriving DecidableEq, Repr

-- `none` is Missing: no stored outcome for the request yet.
abbrev Answers := Request → Option Answer
-- A claim id contains its request and its local row id.
abbrev Claim := Request × Nat

structure Record where
  request : Request
  answer : Option Answer
  claims : List Claim
  deriving DecidableEq, Repr

abbrev Records := SourceId → Option Record

-- decode: success rows give claims; failure (Failed issue) and Missing
-- (Pending issue) give no claims.
def decode (request : Request) : Option Answer → List Claim
  | some (.success rows) => rows.map fun row => (request, row)
  | _ => []

def fresh (answers : Answers) (source : SourceId) (item : Item) : Record :=
  { request := (source, item), answer := answers (source, item),
    claims := decode (source, item) (answers (source, item)) }

-- The from-scratch reference: one fresh record per present source.
def rebuild (sources : Sources) (answers : Answers) : Records := fun source =>
  (sources source).map (fresh answers source)

def claimsOf : Option Record → List Claim
  | none => []
  | some record => record.claims

-- Pending and Failed are both incomplete coverage (line 32).
def incomplete : Option Record → Bool
  | some { answer := some (.success _), .. } => false
  | some _ => true
  | none => false

-- The record waits on `request` (its answer is Missing).
def pendingOn (record : Option Record) (request : Request) : Bool :=
  match record with
  | some r => decide (r.request = request) && r.answer.isNone
  | none => false

structure Output where
  records : Records
  index : Request → List SourceId
  unfinished : SourceId → Bool
  removed : List Claim
  added : List Claim

def selectedOf (index : Request → List SourceId) (changed : List SourceId)
    (arrived : List Request) : List SourceId :=
  changed ++ arrived.flatMap index

-- Lines 03-37. A selected source's record is replaced by its fresh record or
-- removed (lines 16-17, 33-34); its index entry and unfinished flag are
-- replaced with it. removed/added are the selected old and new claims minus
-- each other (lines 36-37).
def extract (old : Records) (index : Request → List SourceId) (unfinished : SourceId → Bool)
    (sources : Sources) (answers : Answers) (changed : List SourceId)
    (arrived : List Request) : Output :=
  let selected := selectedOf index changed arrived
  let records : Records := fun s => if s ∈ selected then rebuild sources answers s else old s
  let oldClaims := selected.flatMap fun s => claimsOf (old s)
  let newClaims := selected.flatMap fun s => claimsOf (records s)
  { records := records,
    index := fun q =>
      (index q).filter (fun s => !(decide (s ∈ selected) && pendingOn (old s) q)) ++
        selected.filter (fun s => pendingOn (records s) q),
    unfinished := fun s => if s ∈ selected then incomplete (records s) else unfinished s,
    removed := oldClaims.filter fun c => c ∉ newClaims,
    added := newClaims.filter fun c => c ∉ oldClaims }

def allClaims (ids : List SourceId) (records : Records) : List Claim :=
  ids.flatMap fun s => claimsOf (records s)

-- Hypotheses of the algorithm, as the design states them.
structure Preconditions (S0 S1 : Sources) (A0 A1 : Answers) (index : Request → List SourceId)
    (changed : List SourceId) (arrived : List Request) : Prop where
  changedCovers : ∀ s, S0 s ≠ S1 s → s ∈ changed
  firstAnswerWins : ∀ q v, A0 q = some v → A1 q = some v
  arrivedCovers : ∀ q, A0 q = none → A1 q ≠ none → q ∈ arrived
  pendingComplete : ∀ s i, S0 s = some i → A0 (s, i) = none → s ∈ index (s, i)

theorem mem_selected_of_changed (index : Request → List SourceId) (changed : List SourceId)
    (arrived : List Request) (s : SourceId) (h : s ∈ changed) :
    s ∈ selectedOf index changed arrived := by
  simp [selectedOf, h]

theorem mem_selected_of_arrived (index : Request → List SourceId) (changed : List SourceId)
    (arrived : List Request) (s : SourceId) (q : Request) (hq : q ∈ arrived) (hs : s ∈ index q) :
    s ∈ selectedOf index changed arrived := by
  simp only [selectedOf, List.mem_append, List.mem_flatMap]
  exact Or.inr ⟨q, hq, hs⟩

-- The core step: an unselected source's fresh record cannot change.
theorem unselected_same (S0 S1 : Sources) (A0 A1 : Answers) (index : Request → List SourceId)
    (changed : List SourceId) (arrived : List Request)
    (pre : Preconditions S0 S1 A0 A1 index changed arrived) (s : SourceId)
    (out : s ∉ selectedOf index changed arrived) :
    rebuild S0 A0 s = rebuild S1 A1 s := by
  have sameItem : S0 s = S1 s := by
    cases Decidable.em (S0 s = S1 s) with
    | inl h => exact h
    | inr h => exact absurd (mem_selected_of_changed index changed arrived s (pre.changedCovers s h)) out
  unfold rebuild
  rw [← sameItem]
  cases hs : S0 s with
  | none => rfl
  | some i =>
    have sameAnswer : A0 (s, i) = A1 (s, i) := by
      cases h0 : A0 (s, i) with
      | some v => exact (pre.firstAnswerWins (s, i) v h0).symm
      | none =>
        cases h1 : A1 (s, i) with
        | none => rfl
        | some v =>
          have arrivedQ := pre.arrivedCovers (s, i) h0 (by simp [h1])
          have waiting := pre.pendingComplete s i hs h0
          exact absurd (mem_selected_of_arrived index changed arrived s (s, i) arrivedQ waiting) out
    simp [fresh, sameAnswer]

-- Theorem 1: incremental extraction equals a rebuild from scratch.
theorem extract_exact (S0 S1 : Sources) (A0 A1 : Answers) (index : Request → List SourceId)
    (unfinished : SourceId → Bool) (changed : List SourceId) (arrived : List Request)
    (pre : Preconditions S0 S1 A0 A1 index changed arrived) :
    (extract (rebuild S0 A0) index unfinished S1 A1 changed arrived).records = rebuild S1 A1 := by
  funext s
  simp only [extract]
  split <;> rename_i h
  · rfl
  · exact unselected_same S0 S1 A0 A1 index changed arrived pre s h

-- Theorem 4: unselected sources keep their stored record, index entries and flag.
theorem unselected_untouched (old : Records) (index : Request → List SourceId)
    (unfinished : SourceId → Bool) (S : Sources) (A : Answers) (changed : List SourceId)
    (arrived : List Request) (s : SourceId) (out : s ∉ selectedOf index changed arrived) :
    (extract old index unfinished S A changed arrived).records s = old s ∧
    (extract old index unfinished S A changed arrived).unfinished s = unfinished s ∧
    (∀ q, s ∈ index q → s ∈ (extract old index unfinished S A changed arrived).index q) := by
  refine ⟨by simp [extract, out], by simp [extract, out], ?_⟩
  intro q member
  simp [extract, out, member]

-- Every claim of a fresh record carries the record's source id.
def Keyed (records : Records) : Prop := ∀ s c, c ∈ claimsOf (records s) → c.1.1 = s

theorem rebuild_keyed (S : Sources) (A : Answers) : Keyed (rebuild S A) := by
  intro s c member
  unfold rebuild at member
  cases hs : S s with
  | none => simp [hs, claimsOf] at member
  | some i =>
    simp only [hs, Option.map, claimsOf, fresh] at member
    unfold decode at member
    split at member
    · simp only [List.mem_map] at member
      obtain ⟨row, _, equal⟩ := member
      rw [← equal]
    · simp at member

-- Theorem 2: claims of records of different sources never coincide.
theorem claims_disjoint (S : Sources) (A : Answers) (s t : SourceId) (c : Claim)
    (different : s ≠ t) (inS : c ∈ claimsOf (rebuild S A s)) : c ∉ claimsOf (rebuild S A t) := by
  intro inT
  exact different ((rebuild_keyed S A s c inS).symm.trans (rebuild_keyed S A t c inT))

theorem mem_all_keyed (ids : List SourceId) (records : Records) (c : Claim)
    (keyed : Keyed records) (covered : c.1.1 ∈ ids ∨ claimsOf (records c.1.1) = []) :
    c ∈ allClaims ids records ↔ c ∈ claimsOf (records c.1.1) := by
  simp only [allClaims, List.mem_flatMap]
  constructor
  · rintro ⟨s, _, member⟩
    rw [keyed s c member]
    exact member
  · intro member
    cases covered with
    | inl inU => exact ⟨c.1.1, inU, member⟩
    | inr empty => simp [empty] at member

theorem mem_selected_keyed (selected : List SourceId) (records : Records) (c : Claim)
    (keyed : Keyed records) :
    c ∈ selected.flatMap (fun s => claimsOf (records s)) ↔
      c.1.1 ∈ selected ∧ c ∈ claimsOf (records c.1.1) := by
  simp only [List.mem_flatMap]
  constructor
  · rintro ⟨s, inSel, member⟩
    rw [keyed s c member]
    exact ⟨inSel, member⟩
  · rintro ⟨inSel, member⟩
    exact ⟨c.1.1, inSel, member⟩

theorem covered_rebuild (ids : List SourceId) (S : Sources) (A : Answers)
    (cover : ∀ s, (S s).isSome → s ∈ ids) (s : SourceId) :
    s ∈ ids ∨ claimsOf (rebuild S A s) = [] := by
  cases hs : S s with
  | none => right; simp [rebuild, hs, claimsOf]
  | some i => left; exact cover s (by simp [hs])

-- Theorem 3: the returned deltas are exactly the differences of all claims.
theorem delta_exact (S0 S1 : Sources) (A0 A1 : Answers) (index : Request → List SourceId)
    (unfinished : SourceId → Bool) (changed : List SourceId) (arrived : List Request)
    (ids : List SourceId) (pre : Preconditions S0 S1 A0 A1 index changed arrived)
    (cover0 : ∀ s, (S0 s).isSome → s ∈ ids) (cover1 : ∀ s, (S1 s).isSome → s ∈ ids)
    (c : Claim) :
    (c ∈ (extract (rebuild S0 A0) index unfinished S1 A1 changed arrived).added ↔
      c ∈ allClaims ids (rebuild S1 A1) ∧ c ∉ allClaims ids (rebuild S0 A0)) ∧
    (c ∈ (extract (rebuild S0 A0) index unfinished S1 A1 changed arrived).removed ↔
      c ∈ allClaims ids (rebuild S0 A0) ∧ c ∉ allClaims ids (rebuild S1 A1)) := by
  have exact := extract_exact S0 S1 A0 A1 index unfinished changed arrived pre
  have newAll := mem_all_keyed ids (rebuild S1 A1) c (rebuild_keyed S1 A1)
    (covered_rebuild ids S1 A1 cover1 c.1.1)
  have oldAll := mem_all_keyed ids (rebuild S0 A0) c (rebuild_keyed S0 A0)
    (covered_rebuild ids S0 A0 cover0 c.1.1)
  have records_def : (extract (rebuild S0 A0) index unfinished S1 A1 changed arrived).records =
      fun s => if s ∈ selectedOf index changed arrived then rebuild S1 A1 s else rebuild S0 A0 s := rfl
  have newRecords : (fun s => if s ∈ selectedOf index changed arrived then rebuild S1 A1 s
      else rebuild S0 A0 s) = rebuild S1 A1 := by rw [← records_def, exact]
  have newSel := mem_selected_keyed (selectedOf index changed arrived) (rebuild S1 A1) c
    (rebuild_keyed S1 A1)
  have oldSel := mem_selected_keyed (selectedOf index changed arrived) (rebuild S0 A0) c
    (rebuild_keyed S0 A0)
  have newFun : (fun s => claimsOf (if s ∈ selectedOf index changed arrived then rebuild S1 A1 s
      else rebuild S0 A0 s)) = fun s => claimsOf (rebuild S1 A1 s) := by
    funext s
    rw [show (if s ∈ selectedOf index changed arrived then rebuild S1 A1 s else rebuild S0 A0 s) =
      rebuild S1 A1 s from congrFun newRecords s]
  simp only [extract, List.mem_filter, decide_eq_true_eq]
  rw [newFun, newSel, oldSel, newAll, oldAll]
  cases Decidable.em (c.1.1 ∈ selectedOf index changed arrived) with
  | inl inSel => simp [inSel]
  | inr outSel =>
    have same := unselected_same S0 S1 A0 A1 index changed arrived pre c.1.1 outSel
    simp [outSel, same]

-- The pending index and unfinished set stay exact, so the hypotheses of the
-- next run hold (the step can be chained).
def IndexExact (records : Records) (index : Request → List SourceId) : Prop :=
  ∀ s q, s ∈ index q ↔ pendingOn (records s) q = true

def UnfinishedExact (records : Records) (unfinished : SourceId → Bool) : Prop :=
  ∀ s, unfinished s = incomplete (records s)

theorem index_exact_preserved (old : Records) (index : Request → List SourceId)
    (unfinished : SourceId → Bool) (S : Sources) (A : Answers) (changed : List SourceId)
    (arrived : List Request) (exactOld : IndexExact old index) :
    IndexExact (extract old index unfinished S A changed arrived).records
      (extract old index unfinished S A changed arrived).index := by
  intro s q
  simp only [extract, List.mem_append, List.mem_filter, Bool.not_and, Bool.or_eq_true,
    Bool.not_eq_true', decide_eq_false_iff_not]
  cases Decidable.em (s ∈ selectedOf index changed arrived) with
  | inl inSel =>
    have oldPending := exactOld s q
    simp only [inSel, ite_true, true_and, not_true_eq_false, false_or]
    constructor
    · rintro (⟨member, notPending⟩ | pending)
      · rw [oldPending.mp member] at notPending; cases notPending
      · exact pending
    · intro pending; exact Or.inr pending
  | inr outSel =>
    simp only [outSel, ite_false, not_false_eq_true, true_or, and_true, false_and, or_false]
    exact exactOld s q

theorem unfinished_exact_preserved (old : Records) (index : Request → List SourceId)
    (unfinished : SourceId → Bool) (S : Sources) (A : Answers) (changed : List SourceId)
    (arrived : List Request) (exactOld : UnfinishedExact old unfinished) :
    UnfinishedExact (extract old index unfinished S A changed arrived).records
      (extract old index unfinished S A changed arrived).unfinished := by
  intro s
  simp only [extract]
  split
  · rfl
  · exact exactOld s

-- An exact index of a rebuild satisfies the pending-completeness precondition.
theorem exact_index_complete (S : Sources) (A : Answers) (index : Request → List SourceId)
    (exactIndex : IndexExact (rebuild S A) index) (s : SourceId) (i : Item)
    (present : S s = some i) (missing : A (s, i) = none) : s ∈ index (s, i) := by
  apply (exactIndex s (s, i)).mpr
  simp [rebuild, present, pendingOn, fresh, missing]

structure Stored (S : Sources) (A : Answers) (records : Records)
    (index : Request → List SourceId) (unfinished : SourceId → Bool) : Prop where
  rebuilt : records = rebuild S A
  indexExact : IndexExact records index
  unfinishedExact : UnfinishedExact records unfinished

-- Chained form: from an exact stored state, one run yields the exact stored
-- state for the new sources and answers.
theorem extract_step (S0 S1 : Sources) (A0 A1 : Answers) (records : Records)
    (index : Request → List SourceId) (unfinished : SourceId → Bool) (changed : List SourceId)
    (arrived : List Request) (stored : Stored S0 A0 records index unfinished)
    (changedCovers : ∀ s, S0 s ≠ S1 s → s ∈ changed)
    (firstAnswerWins : ∀ q v, A0 q = some v → A1 q = some v)
    (arrivedCovers : ∀ q, A0 q = none → A1 q ≠ none → q ∈ arrived) :
    Stored S1 A1 (extract records index unfinished S1 A1 changed arrived).records
      (extract records index unfinished S1 A1 changed arrived).index
      (extract records index unfinished S1 A1 changed arrived).unfinished := by
  have complete : ∀ s i, S0 s = some i → A0 (s, i) = none → s ∈ index (s, i) := by
    intro s i present missing
    rw [stored.rebuilt] at stored
    exact exact_index_complete S0 A0 index stored.indexExact s i present missing
  have pre : Preconditions S0 S1 A0 A1 index changed arrived :=
    ⟨changedCovers, firstAnswerWins, arrivedCovers, complete⟩
  refine ⟨?_, ?_, ?_⟩
  · have := extract_exact S0 S1 A0 A1 index unfinished changed arrived pre
    rw [stored.rebuilt]; exact this
  · exact index_exact_preserved records index unfinished S1 A1 changed arrived stored.indexExact
  · exact unfinished_exact_preserved records index unfinished S1 A1 changed arrived
      stored.unfinishedExact

-- Theorem 5 (negative): ignoring the pending index leaves a stale Pending
-- record after its answer arrives. Source 0 holds item 0 throughout; the
-- answer to (0, 0) arrives as success [5].
def exampleSources : Sources := fun s => if s = 0 then some 0 else none
def exampleBefore : Answers := fun _ => none
def exampleAfter : Answers := fun q => if q = (0, 0) then some (.success [5]) else none

theorem ignoring_index_is_stale :
    (extract (rebuild exampleSources exampleBefore) (fun _ => []) (fun _ => false)
        exampleSources exampleAfter [] [(0, 0)]).records 0 ≠
      rebuild exampleSources exampleAfter 0 := by
  decide

theorem example_preconditions_need_index :
    ¬ Preconditions exampleSources exampleSources exampleBefore exampleAfter (fun _ => []) [] [(0, 0)] := by
  intro pre
  have := pre.pendingComplete 0 0 rfl rfl
  simp at this

#print axioms extract_exact
#print axioms delta_exact
#print axioms claims_disjoint
#print axioms extract_step
#print axioms ignoring_index_is_stale
end ConcordExtract

import Std.Tactic
namespace ConcordRepair
-- Algorithm 3 (REPAIR) of concord-design.tex, Minimal and Weighted modes.
-- Claims are Nat ids; a violating pair is an edge. Sets are lists read up to
-- membership: `Sub` is set inclusion, so order and repeats do not matter.
abbrev Edge := Nat × Nat

def Safe (edges : List Edge) (rejected : List Nat) : Prop :=
  ∀ e ∈ edges, e.1 ∈ rejected ∨ e.2 ∈ rejected

def safeB (edges : List Edge) (rejected : List Nat) : Bool :=
  edges.all fun e => decide (e.1 ∈ rejected) || decide (e.2 ∈ rejected)

theorem safeB_iff (edges : List Edge) (rejected : List Nat) :
    safeB edges rejected = true ↔ Safe edges rejected := by
  simp [safeB, Safe]

def Respects (protectedIds rejected : List Nat) : Prop := ∀ x ∈ rejected, x ∉ protectedIds

def Sub (a b : List Nat) : Prop := ∀ x ∈ a, x ∈ b

def subB (a b : List Nat) : Bool := a.all fun x => decide (x ∈ b)

theorem subB_iff (a b : List Nat) : subB a b = true ↔ Sub a b := by
  simp [subB, Sub]

-- Inclusion-minimal: safe, respects protection, and every safe respecting
-- subset (as a set) is the whole set.
def Minimal (edges : List Edge) (protectedIds rejected : List Nat) : Prop :=
  Safe edges rejected ∧ Respects protectedIds rejected ∧
    ∀ other, Sub other rejected → Safe edges other → Respects protectedIds other →
      Sub rejected other

-- Removing `x` from the rejected set uncovers edge `e`.
def PrivateEdge (rejected : List Nat) (x : Nat) (e : Edge) : Prop :=
  (e.1 = x ∨ e.2 = x) ∧ (e.1 = x ∨ e.1 ∉ rejected) ∧ (e.2 = x ∨ e.2 ∉ rejected)

-- Theorem 1: a safe rejection leaves no violating pair among accepted claims.
theorem safe_accepted_conflict_free (edges : List Edge) (claims rejected : List Nat)
    (safe : Safe edges rejected) :
    ∀ e ∈ edges, ¬ (e.1 ∈ claims.filter (fun x => decide (x ∉ rejected)) ∧
      e.2 ∈ claims.filter (fun x => decide (x ∉ rejected))) := by
  intro e member both
  simp only [List.mem_filter, decide_eq_true_eq] at both
  cases safe e member with
  | inl h => exact both.1.2 h
  | inr h => exact both.2.2 h

-- Proved by induction (core `List.all_eq_false` uses Classical.choice).
theorem exists_uncovered (edges : List Edge) (rejected : List Nat)
    (notSafe : ¬ Safe edges rejected) : ∃ e ∈ edges, e.1 ∉ rejected ∧ e.2 ∉ rejected := by
  induction edges with
  | nil => exact absurd (fun e member => by simp at member) notSafe
  | cons head rest ih =>
    cases Decidable.em (head.1 ∈ rejected ∨ head.2 ∈ rejected) with
    | inr uncovered =>
      exact ⟨head, by simp, fun h => uncovered (Or.inl h), fun h => uncovered (Or.inr h)⟩
    | inl covered =>
      have restUnsafe : ¬ Safe rest rejected := by
        intro safeRest
        apply notSafe
        intro e member
        simp only [List.mem_cons] at member
        cases member with
        | inl h => rw [h]; exact covered
        | inr h => exact safeRest e h
      obtain ⟨e, member, out⟩ := ih restUnsafe
      exact ⟨e, by simp [member], out⟩

-- Minimality is equivalent to every rejected claim having a private edge.
theorem minimal_iff_private_edge (edges : List Edge) (protectedIds rejected : List Nat) :
    Minimal edges protectedIds rejected ↔
      Safe edges rejected ∧ Respects protectedIds rejected ∧
        ∀ x ∈ rejected, ∃ e ∈ edges, PrivateEdge rejected x e := by
  constructor
  · rintro ⟨safe, respects, least⟩
    refine ⟨safe, respects, ?_⟩
    intro x member
    let smaller := rejected.filter fun y => decide (y ≠ x)
    have smallerSub : Sub smaller rejected := by
      intro y h; simp only [smaller, List.mem_filter] at h; exact h.1
    have smallerResp : Respects protectedIds smaller := fun y h => respects y (smallerSub y h)
    have notSafe : ¬ Safe edges smaller := by
      intro safeSmaller
      have back := least smaller smallerSub safeSmaller smallerResp x member
      simp [smaller] at back
    obtain ⟨e, inEdges, out1, out2⟩ := exists_uncovered edges smaller notSafe
    have endpoint : ∀ y, y ∉ smaller → y = x ∨ y ∉ rejected := by
      intro y notIn
      cases Decidable.em (y = x) with
      | inl h => exact Or.inl h
      | inr h =>
        right; intro inR; apply notIn
        simp only [smaller, List.mem_filter, decide_eq_true_eq]; exact ⟨inR, h⟩
    refine ⟨e, inEdges, ?_, endpoint e.1 out1, endpoint e.2 out2⟩
    cases safe e inEdges with
    | inl h =>
      cases endpoint e.1 out1 with
      | inl eq => exact Or.inl eq
      | inr notIn => exact absurd h notIn
    | inr h =>
      cases endpoint e.2 out2 with
      | inl eq => exact Or.inr eq
      | inr notIn => exact absurd h notIn
  · rintro ⟨safe, respects, priv⟩
    refine ⟨safe, respects, ?_⟩
    intro other sub safeOther _ x member
    obtain ⟨e, inEdges, _, first, second⟩ := priv x member
    cases safeOther e inEdges with
    | inl h =>
      cases first with
      | inl eq => rw [← eq]; exact h
      | inr notIn => exact absurd (sub e.1 h) notIn
    | inr h =>
      cases second with
      | inl eq => rw [← eq]; exact h
      | inr notIn => exact absurd (sub e.2 h) notIn

-- Theorem 3: a minimal safe set rejects only claims that occur in some edge.
theorem outside_edges_never_rejected (edges : List Edge) (protectedIds rejected : List Nat)
    (minimal : Minimal edges protectedIds rejected) (x : Nat) (member : x ∈ rejected) :
    ∃ e ∈ edges, e.1 = x ∨ e.2 = x := by
  obtain ⟨_, _, priv⟩ := (minimal_iff_private_edge edges protectedIds rejected).mp minimal
  obtain ⟨e, inEdges, touches, _⟩ := priv x member
  exact ⟨e, inEdges, touches⟩

-- Theorem 2: components with disjoint claim sets decompose exactly.
def vertices (edges : List Edge) : List Nat := edges.flatMap fun e => [e.1, e.2]

def restrict (rejected vs : List Nat) : List Nat := rejected.filter fun x => decide (x ∈ vs)

theorem endpoints_mem (edges : List Edge) (e : Edge) (member : e ∈ edges) :
    e.1 ∈ vertices edges ∧ e.2 ∈ vertices edges := by
  simp only [vertices, List.mem_flatMap, List.mem_cons, List.not_mem_nil, or_false]
  exact ⟨⟨e, member, Or.inl rfl⟩, ⟨e, member, Or.inr rfl⟩⟩

theorem safe_restrict (edges : List Edge) (rejected : List Nat) :
    Safe edges (restrict rejected (vertices edges)) ↔ Safe edges rejected := by
  constructor
  · intro safe e member
    cases safe e member with
    | inl h => simp only [restrict, List.mem_filter] at h; exact Or.inl h.1
    | inr h => simp only [restrict, List.mem_filter] at h; exact Or.inr h.1
  · intro safe e member
    have ends := endpoints_mem edges e member
    cases safe e member with
    | inl h => left; simp [restrict, h, ends.1]
    | inr h => right; simp [restrict, h, ends.2]

theorem safe_append (e1 e2 : List Edge) (rejected : List Nat) :
    Safe (e1 ++ e2) rejected ↔ Safe e1 rejected ∧ Safe e2 rejected := by
  simp only [Safe, List.mem_append]
  constructor
  · intro h; exact ⟨fun e m => h e (Or.inl m), fun e m => h e (Or.inr m)⟩
  · rintro ⟨h1, h2⟩ e (m | m)
    · exact h1 e m
    · exact h2 e m

theorem component_safe (e1 e2 : List Edge) (rejected : List Nat) :
    Safe (e1 ++ e2) rejected ↔
      Safe e1 (restrict rejected (vertices e1)) ∧ Safe e2 (restrict rejected (vertices e2)) := by
  rw [safe_append, safe_restrict, safe_restrict]

theorem private_restrict (edges : List Edge) (rejected : List Nat) (x : Nat) (e : Edge)
    (member : e ∈ edges) :
    PrivateEdge (restrict rejected (vertices edges)) x e ↔ PrivateEdge rejected x e := by
  have ends := endpoints_mem edges e member
  simp [PrivateEdge, restrict, ends.1, ends.2]

theorem component_decomposition (e1 e2 : List Edge) (protectedIds rejected : List Nat)
    (disjoint : ∀ v, v ∈ vertices e1 → v ∉ vertices e2)
    (within : Sub rejected (vertices e1 ++ vertices e2)) :
    Minimal (e1 ++ e2) protectedIds rejected ↔
      Minimal e1 protectedIds (restrict rejected (vertices e1)) ∧
        Minimal e2 protectedIds (restrict rejected (vertices e2)) := by
  simp only [minimal_iff_private_edge]
  have touches : ∀ (edges : List Edge) (e : Edge) (x : Nat), e ∈ edges →
      PrivateEdge rejected x e → x ∈ vertices edges := by
    intro edges e x member priv
    have ends := endpoints_mem edges e member
    cases priv.1 with
    | inl h => rw [← h]; exact ends.1
    | inr h => rw [← h]; exact ends.2
  have restrictMem : ∀ x vs, x ∈ restrict rejected vs ↔ x ∈ rejected ∧ x ∈ vs := by
    intro x vs; simp [restrict]
  constructor
  · rintro ⟨safe, respects, priv⟩
    have safeParts := (component_safe e1 e2 rejected).mp safe
    refine ⟨⟨safeParts.1, fun x h => respects x ((restrictMem x _).mp h).1, ?_⟩,
      ⟨safeParts.2, fun x h => respects x ((restrictMem x _).mp h).1, ?_⟩⟩
    · intro x h
      obtain ⟨inR, inV⟩ := (restrictMem x _).mp h
      obtain ⟨e, inEdges, pe⟩ := priv x inR
      simp only [List.mem_append] at inEdges
      cases inEdges with
      | inl m => exact ⟨e, m, (private_restrict e1 rejected x e m).mpr pe⟩
      | inr m => exact absurd (touches e2 e x m pe) (disjoint x inV)
    · intro x h
      obtain ⟨inR, inV⟩ := (restrictMem x _).mp h
      obtain ⟨e, inEdges, pe⟩ := priv x inR
      simp only [List.mem_append] at inEdges
      cases inEdges with
      | inl m => exact absurd inV (disjoint x (touches e1 e x m pe))
      | inr m => exact ⟨e, m, (private_restrict e2 rejected x e m).mpr pe⟩
  · rintro ⟨⟨safe1, resp1, priv1⟩, ⟨safe2, resp2, priv2⟩⟩
    refine ⟨(component_safe e1 e2 rejected).mpr ⟨safe1, safe2⟩, ?_, ?_⟩
    · intro x inR
      have inV := within x inR
      simp only [List.mem_append] at inV
      cases inV with
      | inl v => exact resp1 x ((restrictMem x _).mpr ⟨inR, v⟩)
      | inr v => exact resp2 x ((restrictMem x _).mpr ⟨inR, v⟩)
    · intro x inR
      have inV := within x inR
      simp only [List.mem_append] at inV
      cases inV with
      | inl v =>
        obtain ⟨e, m, pe⟩ := priv1 x ((restrictMem x _).mpr ⟨inR, v⟩)
        exact ⟨e, List.mem_append_left _ m, (private_restrict e1 rejected x e m).mp pe⟩
      | inr v =>
        obtain ⟨e, m, pe⟩ := priv2 x ((restrictMem x _).mpr ⟨inR, v⟩)
        exact ⟨e, List.mem_append_right _ m, (private_restrict e2 rejected x e m).mp pe⟩

-- Theorem 4: with positive weights a minimum-weight safe set is minimal.
def cost (weight : Nat → Nat) (rejected : List Nat) : Nat := (rejected.map weight).sum

def MinimumWeight (edges : List Edge) (weight : Nat → Nat) (protectedIds rejected : List Nat) : Prop :=
  Safe edges rejected ∧ Respects protectedIds rejected ∧ rejected.Nodup ∧
    ∀ other, Safe edges other → Respects protectedIds other → other.Nodup →
      cost weight rejected ≤ cost weight other

theorem cost_split (weight : Nat → Nat) (p : Nat → Bool) (xs : List Nat) :
    cost weight xs = cost weight (xs.filter p) + cost weight (xs.filter fun x => !p x) := by
  induction xs with
  | nil => simp [cost]
  | cons x rest ih =>
    simp only [cost] at ih ⊢
    cases h : p x <;> simp [h, ih] <;> omega

theorem cost_pos (weight : Nat → Nat) (xs : List Nat) (x : Nat) (member : x ∈ xs)
    (positive : 0 < weight x) : 0 < cost weight xs := by
  induction xs with
  | nil => simp at member
  | cons y rest ih =>
    simp only [cost, List.map_cons, List.sum_cons] at ih ⊢
    simp only [List.mem_cons] at member
    cases member with
    | inl h => rw [← h]; omega
    | inr h => have := ih h; omega

theorem min_weight_is_minimal (edges : List Edge) (weight : Nat → Nat) (protectedIds rejected : List Nat)
    (positive : ∀ x, 0 < weight x) (minimum : MinimumWeight edges weight protectedIds rejected) :
    Minimal edges protectedIds rejected := by
  obtain ⟨safe, respects, nodup, least⟩ := minimum
  refine ⟨safe, respects, ?_⟩
  intro other sub safeOther _ x member
  cases Decidable.em (x ∈ other) with
  | inl h => exact h
  | inr notIn =>
    let kept := rejected.filter fun y => decide (y ∈ other)
    have sameSet : ∀ y, y ∈ kept ↔ y ∈ other := by
      intro y
      simp only [kept, List.mem_filter, decide_eq_true_eq]
      exact ⟨fun h => h.2, fun h => ⟨sub y h, h⟩⟩
    have keptSafe : Safe edges kept := by
      intro e m
      cases safeOther e m with
      | inl h => exact Or.inl ((sameSet e.1).mpr h)
      | inr h => exact Or.inr ((sameSet e.2).mpr h)
    have keptResp : Respects protectedIds kept := fun y h => respects y ((List.mem_filter.mp h).1)
    have keptNodup : kept.Nodup := List.filter_sublist.nodup nodup
    have bound := least kept keptSafe keptResp keptNodup
    have split := cost_split weight (fun y => decide (y ∈ other)) rejected
    have dropped : x ∈ rejected.filter (fun y => !decide (y ∈ other)) := by
      simp [member, notIn]
    have pos := cost_pos weight _ x dropped (positive x)
    simp only [kept] at bound
    omega

-- With a zero weight the conclusion fails: rejecting {1, 2} costs 0, the
-- minimum, but {1} alone is already safe.
def zeroWeight : Nat → Nat := fun x => if x = 0 then 1 else 0

theorem zero_weight_counterexample :
    MinimumWeight [(0, 1)] zeroWeight [] [1, 2] ∧ ¬ Minimal [(0, 1)] [] [1, 2] := by
  refine ⟨⟨(safeB_iff _ _).mp (by decide), by intro x _ h; simp at h, by decide, ?_⟩, ?_⟩
  · intro other _ _ _; simp [cost, zeroWeight]
  · rintro ⟨_, _, least⟩
    have := least [1] (by intro x h; simp at h; simp [h]) ((safeB_iff _ _).mp (by decide)) (by intro x _ h; simp at h)
    have := this 2 (by simp)
    simp at this

-- Theorem 5: the Minimal-mode enumerator of lines 19-27.
def subsets : List Nat → List (List Nat)
  | [] => [[]]
  | x :: rest => subsets rest ++ (subsets rest).map (x :: ·)

theorem mem_subsets (xs ys : List Nat) : ys ∈ subsets xs ↔ ys.Sublist xs := by
  induction xs generalizing ys with
  | nil => simp [subsets]
  | cons x rest ih =>
    simp only [subsets, List.mem_append, List.mem_map, ih, List.sublist_cons_iff]
    constructor
    · rintro (h | ⟨zs, h, rfl⟩)
      · exact Or.inl h
      · exact Or.inr ⟨zs, rfl, h⟩
    · rintro (h | ⟨zs, rfl, h⟩)
      · exact Or.inl h
      · exact Or.inr ⟨zs, h, rfl⟩

def candidates (component protectedIds : List Nat) : List Nat :=
  component.filter fun x => decide (x ∉ protectedIds)

def safeSets (edges : List Edge) (component protectedIds : List Nat) : List (List Nat) :=
  (subsets (candidates component protectedIds)).filter (safeB edges)

def properSubB (a b : List Nat) : Bool := subB a b && !subB b a

def repair (edges : List Edge) (component protectedIds : List Nat) : List (List Nat) :=
  let safe := safeSets edges component protectedIds
  safe.filter fun rejected => !(safe.any fun other => properSubB other rejected)

theorem mem_safeSets (edges : List Edge) (component protectedIds rejected : List Nat) :
    rejected ∈ safeSets edges component protectedIds ↔
      rejected.Sublist (candidates component protectedIds) ∧ Safe edges rejected := by
  simp [safeSets, mem_subsets, safeB_iff]

theorem sublist_candidates_respects (component protectedIds rejected : List Nat)
    (h : rejected.Sublist (candidates component protectedIds)) :
    Respects protectedIds rejected ∧ Sub rejected component := by
  constructor
  · intro x member
    have := h.subset member
    simp only [candidates, List.mem_filter, decide_eq_true_eq] at this
    exact this.2
  · intro x member
    exact (List.mem_filter.mp (h.subset member)).1

theorem repair_sound (edges : List Edge) (component protectedIds rejected : List Nat)
    (member : rejected ∈ repair edges component protectedIds) :
    Safe edges rejected ∧ Respects protectedIds rejected ∧ Sub rejected component ∧
      Minimal edges protectedIds rejected := by
  simp only [repair, List.mem_filter, Bool.not_eq_true', List.any_eq_false] at member
  obtain ⟨inSafe, notDominated⟩ := member
  obtain ⟨sublist, safe⟩ := (mem_safeSets edges component protectedIds rejected).mp inSafe
  obtain ⟨respects, within⟩ := sublist_candidates_respects component protectedIds rejected sublist
  refine ⟨safe, respects, within, safe, respects, ?_⟩
  intro other sub safeOther _
  let kept := rejected.filter fun y => decide (y ∈ other)
  have sameSet : ∀ y, y ∈ kept ↔ y ∈ other := by
    intro y
    simp only [kept, List.mem_filter, decide_eq_true_eq]
    exact ⟨fun h => h.2, fun h => ⟨sub y h, h⟩⟩
  have keptSafe : Safe edges kept := by
    intro e m
    cases safeOther e m with
    | inl h => exact Or.inl ((sameSet e.1).mpr h)
    | inr h => exact Or.inr ((sameSet e.2).mpr h)
  have keptIn : kept ∈ safeSets edges component protectedIds :=
    (mem_safeSets edges component protectedIds kept).mpr
      ⟨(List.filter_sublist).trans sublist, keptSafe⟩
  have notProper := notDominated kept keptIn
  have keptSub : subB kept rejected = true :=
    (subB_iff _ _).mpr fun y h => (List.mem_filter.mp h).1
  have back : subB rejected kept = true := by
    cases h : subB rejected kept with
    | true => rfl
    | false => simp [properSubB, keptSub, h] at notProper
  intro x inR
  exact (sameSet x).mp ((subB_iff _ _).mp back x inR)

-- Every minimal safe respecting set appears (as a set) when the component
-- contains every endpoint of its edges.
theorem repair_complete (edges : List Edge) (component protectedIds rejected : List Nat)
    (closed : ∀ e ∈ edges, e.1 ∈ component ∧ e.2 ∈ component)
    (minimal : Minimal edges protectedIds rejected) :
    ∃ chosen ∈ repair edges component protectedIds, Sub chosen rejected ∧ Sub rejected chosen := by
  obtain ⟨safe, respects, least⟩ := minimal
  let chosen := (candidates component protectedIds).filter fun y => decide (y ∈ rejected)
  have inComponent : ∀ x ∈ rejected, x ∈ component := by
    intro x member
    obtain ⟨e, m, touches⟩ := outside_edges_never_rejected edges protectedIds rejected
      ⟨safe, respects, least⟩ x member
    cases touches with
    | inl h => rw [← h]; exact (closed e m).1
    | inr h => rw [← h]; exact (closed e m).2
  have sameSet : ∀ y, y ∈ chosen ↔ y ∈ rejected := by
    intro y
    simp only [chosen, candidates, List.mem_filter, decide_eq_true_eq]
    exact ⟨fun h => h.2, fun h => ⟨⟨inComponent y h, respects y h⟩, h⟩⟩
  have chosenSafe : Safe edges chosen := by
    intro e m
    cases safe e m with
    | inl h => exact Or.inl ((sameSet e.1).mpr h)
    | inr h => exact Or.inr ((sameSet e.2).mpr h)
  have chosenIn : chosen ∈ safeSets edges component protectedIds :=
    (mem_safeSets edges component protectedIds chosen).mpr ⟨List.filter_sublist, chosenSafe⟩
  refine ⟨chosen, ?_, fun y h => (sameSet y).mp h, fun y h => (sameSet y).mpr h⟩
  simp only [repair, List.mem_filter, Bool.not_eq_true', List.any_eq_false]
  refine ⟨chosenIn, ?_⟩
  intro other otherIn
  obtain ⟨sublist, safeOther⟩ := (mem_safeSets edges component protectedIds other).mp otherIn
  have respOther := (sublist_candidates_respects component protectedIds other sublist).1
  cases h : subB other chosen with
  | false => simp [properSubB, h]
  | true =>
    have sub := (subB_iff _ _).mp h
    have back := least other (fun y m => (sameSet y).mp (sub y m)) safeOther respOther
    have backB : subB chosen other = true :=
      (subB_iff _ _).mpr fun y m => back y ((sameSet y).mp m)
    simp [properSubB, h, backB]

-- Impossible exactly when no safe set avoids the protected claims.
theorem impossible_exact (edges : List Edge) (component protectedIds : List Nat)
    (closed : ∀ e ∈ edges, e.1 ∈ component ∧ e.2 ∈ component) :
    safeSets edges component protectedIds = [] ↔
      ∀ rejected, Safe edges rejected → ¬ Respects protectedIds rejected := by
  constructor
  · intro empty rejected safe respects
    let chosen := (candidates component protectedIds).filter fun y => decide (y ∈ rejected)
    have chosenSafe : Safe edges chosen := by
      intro e m
      have ends := closed e m
      cases safe e m with
      | inl h =>
        left
        simp only [chosen, candidates, List.mem_filter, decide_eq_true_eq]
        exact ⟨⟨ends.1, respects e.1 h⟩, h⟩
      | inr h =>
        right
        simp only [chosen, candidates, List.mem_filter, decide_eq_true_eq]
        exact ⟨⟨ends.2, respects e.2 h⟩, h⟩
    have chosenIn : chosen ∈ safeSets edges component protectedIds :=
      (mem_safeSets edges component protectedIds chosen).mpr ⟨List.filter_sublist, chosenSafe⟩
    rw [empty] at chosenIn
    simp at chosenIn
  · intro none
    cases h : safeSets edges component protectedIds with
    | nil => rfl
    | cons rejected rest =>
      have inSafe : rejected ∈ safeSets edges component protectedIds := by rw [h]; simp
      obtain ⟨sublist, safe⟩ := (mem_safeSets edges component protectedIds rejected).mp inSafe
      exact absurd (sublist_candidates_respects component protectedIds rejected sublist).1
        (none rejected safe)

-- Weighted mode (lines 29-32): keep the safe candidates of least cost.
def repairWeighted (edges : List Edge) (weight : Nat → Nat) (component protectedIds : List Nat) :
    List (List Nat) :=
  let safe := safeSets edges component protectedIds
  safe.filter fun rejected => safe.all fun other => decide (cost weight rejected ≤ cost weight other)

theorem cost_erase (weight : Nat → Nat) (xs : List Nat) (x : Nat) (member : x ∈ xs) :
    cost weight xs = weight x + cost weight (xs.erase x) := by
  induction xs with
  | nil => simp at member
  | cons y rest ih =>
    cases Decidable.em (y = x) with
    | inl h => subst h; simp [cost]
    | inr h =>
      have inRest : x ∈ rest := by
        simp only [List.mem_cons] at member
        cases member with
        | inl e => exact absurd e.symm h
        | inr m => exact m
      have := ih inRest
      simp only [cost] at this ⊢
      rw [List.erase_cons_tail (by simpa using h)]
      simp only [List.map_cons, List.sum_cons]
      omega

-- Proved by induction (core `List.mem_erase_of_ne` uses Classical.choice).
theorem mem_erase_other (xs : List Nat) (x y : Nat) (ne : y ≠ x) (member : y ∈ xs) :
    y ∈ xs.erase x := by
  induction xs with
  | nil => simp at member
  | cons z rest ih =>
    cases Decidable.em (z = x) with
    | inl h =>
      subst h
      simp only [List.erase_cons_head]
      simp only [List.mem_cons] at member
      cases member with
      | inl e => exact absurd e ne
      | inr m => exact m
    | inr h =>
      rw [List.erase_cons_tail (by simpa using h)]
      simp only [List.mem_cons] at member ⊢
      cases member with
      | inl e => exact Or.inl e
      | inr m => exact Or.inr (ih m)

theorem cost_le_of_sub (weight : Nat → Nat) (a b : List Nat) (nodupA : a.Nodup) (nodupB : b.Nodup)
    (sub : Sub a b) : cost weight a ≤ cost weight b := by
  induction a generalizing b with
  | nil => simp [cost]
  | cons x rest ih =>
    have xNotIn : x ∉ rest := (List.nodup_cons.mp nodupA).1
    have restNodup : rest.Nodup := (List.nodup_cons.mp nodupA).2
    have xIn : x ∈ b := sub x (by simp)
    have restSub : Sub rest (b.erase x) := by
      intro y member
      have ne : y ≠ x := fun e => xNotIn (e ▸ member)
      exact mem_erase_other b x y ne (sub y (by simp [member]))
    have bound := ih (b.erase x) restNodup (nodupB.erase x) restSub
    have split := cost_erase weight b x xIn
    simp only [cost, List.map_cons, List.sum_cons] at bound split ⊢
    omega

theorem sublist_candidates_nodup (component protectedIds rejected : List Nat) (nodup : component.Nodup)
    (h : rejected.Sublist (candidates component protectedIds)) : rejected.Nodup :=
  h.nodup (List.filter_sublist.nodup nodup)

-- Restricting any safe respecting set to the candidates keeps it safe.
theorem restrict_candidates (edges : List Edge) (component protectedIds rejected : List Nat)
    (closed : ∀ e ∈ edges, e.1 ∈ component ∧ e.2 ∈ component)
    (safe : Safe edges rejected) (respects : Respects protectedIds rejected) :
    (candidates component protectedIds).filter (fun y => decide (y ∈ rejected)) ∈
      safeSets edges component protectedIds := by
  apply (mem_safeSets edges component protectedIds _).mpr
  refine ⟨List.filter_sublist, ?_⟩
  intro e m
  have ends := closed e m
  cases safe e m with
  | inl h =>
    left
    simp only [candidates, List.mem_filter, decide_eq_true_eq]
    exact ⟨⟨ends.1, respects e.1 h⟩, h⟩
  | inr h =>
    right
    simp only [candidates, List.mem_filter, decide_eq_true_eq]
    exact ⟨⟨ends.2, respects e.2 h⟩, h⟩

theorem repair_weighted_sound (edges : List Edge) (weight : Nat → Nat)
    (component protectedIds rejected : List Nat) (nodup : component.Nodup)
    (closed : ∀ e ∈ edges, e.1 ∈ component ∧ e.2 ∈ component)
    (member : rejected ∈ repairWeighted edges weight component protectedIds) :
    MinimumWeight edges weight protectedIds rejected ∧ Sub rejected component := by
  simp only [repairWeighted, List.mem_filter, List.all_eq_true, decide_eq_true_eq] at member
  obtain ⟨inSafe, least⟩ := member
  obtain ⟨sublist, safe⟩ := (mem_safeSets edges component protectedIds rejected).mp inSafe
  obtain ⟨respects, within⟩ := sublist_candidates_respects component protectedIds rejected sublist
  refine ⟨⟨safe, respects, sublist_candidates_nodup component protectedIds rejected nodup sublist, ?_⟩,
    within⟩
  intro other safeOther respOther nodupOther
  have keptIn := restrict_candidates edges component protectedIds other closed safeOther respOther
  have le1 := least _ keptIn
  have le2 := cost_le_of_sub weight
    ((candidates component protectedIds).filter fun y => decide (y ∈ other)) other
    (sublist_candidates_nodup component protectedIds _ nodup List.filter_sublist) nodupOther
    (fun y h => by simp only [List.mem_filter, decide_eq_true_eq] at h; exact h.2)
  omega

theorem repair_weighted_complete (edges : List Edge) (weight : Nat → Nat)
    (component protectedIds rejected : List Nat) (nodup : component.Nodup)
    (closed : ∀ e ∈ edges, e.1 ∈ component ∧ e.2 ∈ component)
    (minimum : MinimumWeight edges weight protectedIds rejected) (within : Sub rejected component) :
    ∃ chosen ∈ repairWeighted edges weight component protectedIds,
      Sub chosen rejected ∧ Sub rejected chosen := by
  obtain ⟨safe, respects, nodupR, least⟩ := minimum
  let chosen := (candidates component protectedIds).filter fun y => decide (y ∈ rejected)
  have chosenIn := restrict_candidates edges component protectedIds rejected closed safe respects
  have sameSet : ∀ y, y ∈ chosen ↔ y ∈ rejected := by
    intro y
    simp only [chosen, candidates, List.mem_filter, decide_eq_true_eq]
    exact ⟨fun h => h.2, fun h => ⟨⟨within y h, respects y h⟩, h⟩⟩
  have chosenNodup : chosen.Nodup :=
    sublist_candidates_nodup component protectedIds _ nodup List.filter_sublist
  have chosenLe := cost_le_of_sub weight chosen rejected chosenNodup nodupR (fun y h => (sameSet y).mp h)
  refine ⟨chosen, ?_, fun y h => (sameSet y).mp h, fun y h => (sameSet y).mpr h⟩
  simp only [repairWeighted, List.mem_filter, List.all_eq_true, decide_eq_true_eq]
  refine ⟨chosenIn, ?_⟩
  intro other otherIn
  obtain ⟨sublist, safeOther⟩ := (mem_safeSets edges component protectedIds other).mp otherIn
  have bound := least other safeOther (sublist_candidates_respects component protectedIds other sublist).1
    (sublist_candidates_nodup component protectedIds other nodup sublist)
  omega

-- Lines 10-13: an unfinished check (Pending or Failed outcome) in a hard
-- rule's bucket makes repair Unavailable with no proposals.
inductive Status where
  | unavailable
  | impossible
  | available
  deriving DecidableEq, Repr

def repairResult (unfinishedCheck : Bool) (edges : List Edge) (component protectedIds : List Nat) :
    Status × List (List Nat) :=
  if unfinishedCheck then (.unavailable, [])
  else if safeSets edges component protectedIds = [] then (.impossible, [])
  else (.available, repair edges component protectedIds)

theorem unfinished_unavailable (edges : List Edge) (component protectedIds : List Nat) :
    repairResult true edges component protectedIds = (.unavailable, []) := rfl

theorem available_sound (edges : List Edge) (component protectedIds : List Nat) (rejected : List Nat)
    (member : rejected ∈ (repairResult false edges component protectedIds).2) :
    Minimal edges protectedIds rejected ∧ Sub rejected component := by
  simp only [repairResult, Bool.false_eq_true, ite_false] at member
  split at member
  · simp at member
  · have := repair_sound edges component protectedIds rejected member
    exact ⟨this.2.2.2, this.2.2.1⟩

#print axioms minimal_iff_private_edge
#print axioms component_decomposition
#print axioms min_weight_is_minimal
#print axioms zero_weight_counterexample
#print axioms repair_sound
#print axioms repair_complete
#print axioms impossible_exact
#print axioms repair_weighted_sound
#print axioms repair_weighted_complete
end ConcordRepair

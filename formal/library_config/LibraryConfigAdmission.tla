---- MODULE LibraryConfigAdmission ----
EXTENDS FiniteSets

CONSTANTS Entries, BuggyPartialStart

VARIABLES phase, schemaChecked, semanticsChecked, bindingChecked, failedEntry,
          barrier, constructed, frozen, mutated, executed

vars == <<phase, schemaChecked, semanticsChecked, bindingChecked, failedEntry,
          barrier, constructed, frozen, mutated, executed>>

Init ==
  /\ phase = "validating"
  /\ schemaChecked = {}
  /\ semanticsChecked = {}
  /\ bindingChecked = {}
  /\ failedEntry = "none"
  /\ barrier = FALSE
  /\ constructed = {}
  /\ frozen = {}
  /\ mutated = {}
  /\ executed = {}

SchemaPass(e) ==
  /\ phase = "validating"
  /\ e \in Entries \ schemaChecked
  /\ schemaChecked' = schemaChecked \cup {e}
  /\ UNCHANGED <<phase, semanticsChecked, bindingChecked, failedEntry,
                  barrier, constructed, frozen, mutated, executed>>

SchemaFail(e) ==
  /\ phase = "validating"
  /\ e \in Entries \ schemaChecked
  /\ phase' = "refused"
  /\ failedEntry' = e
  /\ UNCHANGED <<schemaChecked, semanticsChecked, bindingChecked, barrier,
                  constructed, frozen, mutated, executed>>

SemanticsPass(e) ==
  /\ phase = "validating"
  /\ e \in schemaChecked \ semanticsChecked
  /\ semanticsChecked' = semanticsChecked \cup {e}
  /\ UNCHANGED <<phase, schemaChecked, bindingChecked, failedEntry,
                  barrier, constructed, frozen, mutated, executed>>

SemanticsFail(e) ==
  /\ phase = "validating"
  /\ e \in schemaChecked \ semanticsChecked
  /\ phase' = "refused"
  /\ failedEntry' = e
  /\ UNCHANGED <<schemaChecked, semanticsChecked, bindingChecked, barrier,
                  constructed, frozen, mutated, executed>>

BindingPass(e) ==
  /\ phase = "validating"
  /\ e \in semanticsChecked \ bindingChecked
  /\ bindingChecked' = bindingChecked \cup {e}
  /\ UNCHANGED <<phase, schemaChecked, semanticsChecked, failedEntry,
                  barrier, constructed, frozen, mutated, executed>>

BindingFail(e) ==
  /\ phase = "validating"
  /\ e \in semanticsChecked \ bindingChecked
  /\ phase' = "refused"
  /\ failedEntry' = e
  /\ UNCHANGED <<schemaChecked, semanticsChecked, bindingChecked, barrier,
                  constructed, frozen, mutated, executed>>

ReachBarrier ==
  /\ phase = "validating"
  /\ bindingChecked = Entries
  /\ phase' = "admitted"
  /\ barrier' = TRUE
  /\ UNCHANGED <<schemaChecked, semanticsChecked, bindingChecked, failedEntry,
                  constructed, frozen, mutated, executed>>

Construct(e) ==
  /\ phase \in {"validating", "admitted", "running"}
  /\ e \in bindingChecked \ constructed
  /\ IF BuggyPartialStart THEN TRUE ELSE barrier
  /\ constructed' = constructed \cup {e}
  /\ frozen' = frozen \cup {e}
  /\ UNCHANGED <<phase, schemaChecked, semanticsChecked, bindingChecked,
                  failedEntry, barrier, mutated, executed>>

Mutate(e) ==
  /\ phase \in {"validating", "admitted", "running"}
  /\ e \in constructed \ mutated
  /\ IF BuggyPartialStart THEN TRUE ELSE barrier
  /\ phase' = "running"
  /\ mutated' = mutated \cup {e}
  /\ UNCHANGED <<schemaChecked, semanticsChecked, bindingChecked, failedEntry,
                  barrier, constructed, frozen, executed>>

Execute(e) ==
  /\ phase = "running"
  /\ e \in mutated \ executed
  /\ IF BuggyPartialStart THEN TRUE ELSE barrier
  /\ executed' = executed \cup {e}
  /\ UNCHANGED <<phase, schemaChecked, semanticsChecked, bindingChecked,
                  failedEntry, barrier, constructed, frozen, mutated>>

Stutter == UNCHANGED vars

Next ==
  (\E e \in Entries: SchemaPass(e) \/ SchemaFail(e) \/
                    SemanticsPass(e) \/ SemanticsFail(e) \/
                    BindingPass(e) \/ BindingFail(e) \/ Construct(e) \/
                    Mutate(e) \/ Execute(e)) \/ ReachBarrier \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in {"validating", "admitted", "running", "refused"}
  /\ schemaChecked \subseteq Entries
  /\ semanticsChecked \subseteq Entries
  /\ bindingChecked \subseteq Entries
  /\ failedEntry \in Entries \cup {"none"}
  /\ barrier \in BOOLEAN
  /\ constructed \subseteq Entries
  /\ frozen \subseteq Entries
  /\ mutated \subseteq Entries
  /\ executed \subseteq Entries

AllSideEffectsRequireDocumentBarrier ==
  (constructed # {} \/ frozen # {} \/ mutated # {} \/ executed # {}) =>
    (barrier /\ schemaChecked = Entries /\ semanticsChecked = Entries /\
     bindingChecked = Entries)

RefusedDocumentHasNoSideEffects ==
  phase = "refused" =>
    (constructed = {} /\ frozen = {} /\ mutated = {} /\ executed = {})

ExecutedEntriesUseFrozenStrictBindings == executed \subseteq frozen

LibraryConfigAdmissionSafety ==
  TypeOK /\ AllSideEffectsRequireDocumentBarrier /\
  RefusedDocumentHasNoSideEffects /\ ExecutedEntriesUseFrozenStrictBindings

====

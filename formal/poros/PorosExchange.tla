---- MODULE PorosExchange ----
EXTENDS FiniteSets

CONSTANTS
  Messages,
  Rows,
  SenderGeneration,
  ReceiverGenerations,
  NoGeneration,
  NoMessage,
  TrustedMessages,
  SignatureValidMessages,
  RecipientValidMessages,
  BodyHashValidMessages,
  FreshMessages,
  DetachedMessages,
  ObjectHashValidMessages,
  ReceiverAuthorizedMessages,
  PromotionAuthorizedMessages,
  ReviewMessages,
  QuarantineMessages,
  SenderHintMessages,
  RestrictedMessages,
  PrivateMessages,
  RedactionEvidenceMessages,
  DeclassificationEvidenceMessages,
  BuggySenderHintGrantsAllow

ReceiptStatuses == {
  "none",
  "accepted_for_admission",
  "rejected_preflight",
  "quarantined",
  "duplicate"
}

Effects == {"none", "allow", "deny", "review", "quarantine"}

VARIABLES
  outbox,
  inbox,
  verified,
  replayCache,
  receipts,
  nomosDecisions,
  promotionDecisions,
  admissions,
  blockedState,
  publishedMessages,
  promotedMessages,
  admissionEffects,
  publicationEffects,
  promotionEffects,
  liveRows,
  rowMessage,
  rowSenderGeneration,
  rowReceiverGeneration,
  messageReceiverGeneration,
  publicProofMessages

Vars == <<
  outbox,
  inbox,
  verified,
  replayCache,
  receipts,
  nomosDecisions,
  promotionDecisions,
  admissions,
  blockedState,
  publishedMessages,
  promotedMessages,
  admissionEffects,
  publicationEffects,
  promotionEffects,
  liveRows,
  rowMessage,
  rowSenderGeneration,
  rowReceiverGeneration,
  messageReceiverGeneration,
  publicProofMessages
>>

DefaultReceipt == [
  recorded |-> FALSE,
  status |-> "none",
  bodyHashBound |-> FALSE
]

ReceiptRecord(m, status) == [
  recorded |-> TRUE,
  status |-> status,
  bodyHashBound |-> m \in BodyHashValidMessages
]

DefaultDecision == [
  recorded |-> FALSE,
  effect |-> "none",
  inputHashBound |-> FALSE,
  usesReceiverPolicy |-> FALSE,
  hasRedaction |-> FALSE,
  hasDeclassification |-> FALSE
]

\* BuggySenderHintGrantsAllow models the refinement-boundary regression this
\* wave closes: a sender's authorization HINT self-grants a receiver "allow"
\* even though the receiver's policy did not authorize the message. It is
\* FALSE on the positive config (the receiver's decision is authoritative), so
\* NoSenderPolicyAuthority holds; the sender-authority witness config sets it
\* TRUE and a sender-hinted, receiver-unauthorized message is allowed and
\* admitted, refuting NoSenderPolicyAuthority by name.
DecisionEffect(m) ==
  IF m \in ReceiverAuthorizedMessages THEN "allow"
  ELSE IF BuggySenderHintGrantsAllow /\ m \in SenderHintMessages THEN "allow"
  ELSE IF m \in ReviewMessages THEN "review"
  ELSE IF m \in QuarantineMessages THEN "quarantine"
  ELSE "deny"

PromotionEffect(m) ==
  IF /\ m \in PromotionAuthorizedMessages
     /\ nomosDecisions[m].recorded
     /\ nomosDecisions[m].effect \in {"review", "quarantine"}
  THEN "allow"
  ELSE "deny"

DecisionRecord(m) == [
  recorded |-> TRUE,
  effect |-> DecisionEffect(m),
  inputHashBound |-> m \in BodyHashValidMessages,
  usesReceiverPolicy |-> TRUE,
  hasRedaction |-> m \in RedactionEvidenceMessages,
  hasDeclassification |-> m \in DeclassificationEvidenceMessages
]

PromotionDecisionRecord(m) == [
  recorded |-> TRUE,
  effect |-> PromotionEffect(m),
  inputHashBound |-> m \in BodyHashValidMessages,
  usesReceiverPolicy |-> TRUE,
  hasRedaction |-> FALSE,
  hasDeclassification |-> FALSE
]

OriginalAllow(m) ==
  /\ nomosDecisions[m].recorded
  /\ nomosDecisions[m].effect = "allow"
  /\ nomosDecisions[m].usesReceiverPolicy

PromotionAllow(m) ==
  /\ promotionDecisions[m].recorded
  /\ promotionDecisions[m].effect = "allow"
  /\ promotionDecisions[m].usesReceiverPolicy
  /\ nomosDecisions[m].recorded
  /\ nomosDecisions[m].effect \in {"review", "quarantine"}

HasReceiverAllow(m) ==
  \/ OriginalAllow(m)
  \/ PromotionAllow(m)

IsDetachedObjectValid(m) ==
  m \notin DetachedMessages \/ m \in ObjectHashValidMessages

IsPreflightValid(m) ==
  /\ m \in TrustedMessages
  /\ m \in SignatureValidMessages
  /\ m \in RecipientValidMessages
  /\ m \in BodyHashValidMessages
  /\ m \in FreshMessages
  /\ IsDetachedObjectValid(m)

CanShowPublicProof(m) ==
  \/ m \notin (PrivateMessages \cup RestrictedMessages)
  \/ /\ nomosDecisions[m].recorded
     /\ \/ nomosDecisions[m].hasRedaction
        \/ nomosDecisions[m].hasDeclassification

Init ==
  /\ outbox = {}
  /\ inbox = {}
  /\ verified = {}
  /\ replayCache = {}
  /\ receipts = [m \in Messages |-> DefaultReceipt]
  /\ nomosDecisions = [m \in Messages |-> DefaultDecision]
  /\ promotionDecisions = [m \in Messages |-> DefaultDecision]
  /\ admissions = {}
  /\ blockedState = {}
  /\ publishedMessages = {}
  /\ promotedMessages = {}
  /\ admissionEffects = [m \in Messages |-> 0]
  /\ publicationEffects = [m \in Messages |-> 0]
  /\ promotionEffects = [m \in Messages |-> 0]
  /\ liveRows = {}
  /\ rowMessage = [r \in Rows |-> NoMessage]
  /\ rowSenderGeneration = [r \in Rows |-> NoGeneration]
  /\ rowReceiverGeneration = [r \in Rows |-> NoGeneration]
  /\ messageReceiverGeneration = [m \in Messages |-> NoGeneration]
  /\ publicProofMessages = {}

Export(m) ==
  /\ m \in Messages
  /\ m \notin outbox
  /\ outbox' = outbox \cup {m}
  /\ UNCHANGED <<
       inbox, verified, replayCache, receipts, nomosDecisions,
       promotionDecisions, admissions, blockedState, publishedMessages,
       promotedMessages, admissionEffects, publicationEffects,
       promotionEffects, liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

SubmitFresh(m) ==
  /\ m \in outbox
  /\ m \notin replayCache
  /\ inbox' = inbox \cup {m}
  /\ replayCache' = replayCache \cup {m}
  /\ receipts' = [
       receipts EXCEPT ![m] =
         IF IsPreflightValid(m)
         THEN ReceiptRecord(m, "accepted_for_admission")
         ELSE ReceiptRecord(m, "rejected_preflight")
     ]
  /\ IF IsPreflightValid(m)
     THEN verified' = verified \cup {m}
     ELSE verified' = verified
  /\ UNCHANGED <<
       outbox, nomosDecisions, promotionDecisions, admissions, blockedState,
       publishedMessages, promotedMessages, admissionEffects,
       publicationEffects, promotionEffects, liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

Replay(m) ==
  /\ m \in replayCache
  /\ receipts' = [receipts EXCEPT ![m] = ReceiptRecord(m, "duplicate")]
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, nomosDecisions,
       promotionDecisions, admissions, blockedState, publishedMessages,
       promotedMessages, admissionEffects, publicationEffects,
       promotionEffects, liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

EvaluateNomos(m) ==
  /\ m \in verified
  /\ ~nomosDecisions[m].recorded
  /\ nomosDecisions' = [
       nomosDecisions EXCEPT ![m] = DecisionRecord(m)
     ]
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, receipts, promotionDecisions,
       admissions, blockedState, publishedMessages, promotedMessages,
       admissionEffects, publicationEffects, promotionEffects,
       liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

RouteBlocked(m) ==
  /\ m \in verified
  /\ nomosDecisions[m].recorded
  /\ nomosDecisions[m].effect \in {"deny", "review", "quarantine"}
  /\ ~promotionDecisions[m].recorded
  /\ m \notin admissions
  /\ m \notin publishedMessages
  /\ m \notin blockedState
  /\ blockedState' = blockedState \cup {m}
  /\ receipts' = [
       receipts EXCEPT ![m] =
         IF nomosDecisions[m].effect = "quarantine"
            \/ nomosDecisions[m].effect = "review"
         THEN ReceiptRecord(m, "quarantined")
         ELSE @
     ]
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, nomosDecisions,
       promotionDecisions, admissions, publishedMessages, promotedMessages,
       admissionEffects, publicationEffects, promotionEffects,
       liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

EvaluatePromotion(m) ==
  /\ m \in blockedState
  /\ nomosDecisions[m].recorded
  /\ nomosDecisions[m].effect \in {"review", "quarantine"}
  /\ ~promotionDecisions[m].recorded
  /\ promotionDecisions' = [
       promotionDecisions EXCEPT ![m] = PromotionDecisionRecord(m)
     ]
  /\ blockedState' =
       IF PromotionEffect(m) = "allow"
       THEN blockedState \ {m}
       ELSE blockedState
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, receipts, nomosDecisions,
       admissions, publishedMessages, promotedMessages, admissionEffects,
       publicationEffects, promotionEffects, liveRows, rowMessage,
       rowSenderGeneration, rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

Admit(m) ==
  /\ m \in verified
  /\ nomosDecisions[m].recorded
  /\ HasReceiverAllow(m)
  /\ m \notin admissions
  /\ admissionEffects[m] = 0
  /\ admissions' = admissions \cup {m}
  /\ admissionEffects' = [admissionEffects EXCEPT ![m] = 1]
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, receipts, nomosDecisions,
       promotionDecisions, blockedState, publishedMessages,
       promotedMessages, publicationEffects, promotionEffects,
       liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration,
       publicProofMessages
     >>

Publish(m, g, r) ==
  /\ m \in admissions
  /\ m \notin publishedMessages
  /\ g \in ReceiverGenerations
  /\ r \in Rows
  /\ r \notin liveRows
  /\ publicationEffects[m] = 0
  /\ publishedMessages' = publishedMessages \cup {m}
  /\ publicationEffects' = [publicationEffects EXCEPT ![m] = 1]
  /\ promotedMessages' =
       IF PromotionAllow(m)
       THEN promotedMessages \cup {m}
       ELSE promotedMessages
  /\ promotionEffects' =
       IF PromotionAllow(m)
       THEN [promotionEffects EXCEPT ![m] = 1]
       ELSE promotionEffects
  /\ liveRows' = liveRows \cup {r}
  /\ rowMessage' = [rowMessage EXCEPT ![r] = m]
  /\ rowSenderGeneration' = [rowSenderGeneration EXCEPT ![r] = SenderGeneration]
  /\ rowReceiverGeneration' = [rowReceiverGeneration EXCEPT ![r] = g]
  /\ messageReceiverGeneration' = [
       messageReceiverGeneration EXCEPT ![m] = g
     ]
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, receipts, nomosDecisions,
       promotionDecisions, admissions, blockedState, admissionEffects,
       publicProofMessages
     >>

ShowPublicProof(m) ==
  /\ m \in Messages
  /\ receipts[m].recorded
  /\ CanShowPublicProof(m)
  /\ m \notin publicProofMessages
  /\ publicProofMessages' = publicProofMessages \cup {m}
  /\ UNCHANGED <<
       outbox, inbox, verified, replayCache, receipts, nomosDecisions,
       promotionDecisions, admissions, blockedState, publishedMessages,
       promotedMessages, admissionEffects, publicationEffects, promotionEffects,
       liveRows, rowMessage, rowSenderGeneration,
       rowReceiverGeneration, messageReceiverGeneration
     >>

Next ==
  \/ \E m \in Messages: Export(m)
  \/ \E m \in Messages: SubmitFresh(m)
  \/ \E m \in Messages: Replay(m)
  \/ \E m \in Messages: EvaluateNomos(m)
  \/ \E m \in Messages: RouteBlocked(m)
  \/ \E m \in Messages: EvaluatePromotion(m)
  \/ \E m \in Messages: Admit(m)
  \/ \E m \in Messages, g \in ReceiverGenerations, r \in Rows:
       Publish(m, g, r)
  \/ \E m \in Messages: ShowPublicProof(m)

Spec == Init /\ [][Next]_Vars

ConstantSetsOK ==
  /\ NoMessage \notin Messages
  /\ NoGeneration \notin ReceiverGenerations
  /\ NoGeneration # SenderGeneration
  /\ TrustedMessages \subseteq Messages
  /\ SignatureValidMessages \subseteq Messages
  /\ RecipientValidMessages \subseteq Messages
  /\ BodyHashValidMessages \subseteq Messages
  /\ FreshMessages \subseteq Messages
  /\ DetachedMessages \subseteq Messages
  /\ ObjectHashValidMessages \subseteq Messages
  /\ ReceiverAuthorizedMessages \subseteq Messages
  /\ PromotionAuthorizedMessages \subseteq Messages
  /\ ReviewMessages \subseteq Messages
  /\ QuarantineMessages \subseteq Messages
  /\ SenderHintMessages \subseteq Messages
  /\ RestrictedMessages \subseteq Messages
  /\ PrivateMessages \subseteq Messages
  /\ RedactionEvidenceMessages \subseteq Messages
  /\ DeclassificationEvidenceMessages \subseteq Messages
  /\ BuggySenderHintGrantsAllow \in BOOLEAN

TypeOK ==
  /\ outbox \subseteq Messages
  /\ inbox \subseteq Messages
  /\ verified \subseteq Messages
  /\ replayCache \subseteq Messages
  /\ receipts \in [
       Messages -> [
         recorded: BOOLEAN,
         status: ReceiptStatuses,
         bodyHashBound: BOOLEAN
       ]
     ]
  /\ nomosDecisions \in [
       Messages -> [
         recorded: BOOLEAN,
         effect: Effects,
         inputHashBound: BOOLEAN,
         usesReceiverPolicy: BOOLEAN,
         hasRedaction: BOOLEAN,
         hasDeclassification: BOOLEAN
       ]
     ]
  /\ promotionDecisions \in [
       Messages -> [
         recorded: BOOLEAN,
         effect: Effects,
         inputHashBound: BOOLEAN,
         usesReceiverPolicy: BOOLEAN,
         hasRedaction: BOOLEAN,
         hasDeclassification: BOOLEAN
       ]
     ]
  /\ admissions \subseteq Messages
  /\ blockedState \subseteq Messages
  /\ publishedMessages \subseteq Messages
  /\ promotedMessages \subseteq Messages
  /\ admissionEffects \in [Messages -> {0, 1}]
  /\ publicationEffects \in [Messages -> {0, 1}]
  /\ promotionEffects \in [Messages -> {0, 1}]
  /\ liveRows \subseteq Rows
  /\ rowMessage \in [Rows -> Messages \cup {NoMessage}]
  /\ rowSenderGeneration \in [Rows -> {SenderGeneration, NoGeneration}]
  /\ rowReceiverGeneration \in [Rows -> ReceiverGenerations \cup {NoGeneration}]
  /\ messageReceiverGeneration \in [
       Messages -> ReceiverGenerations \cup {NoGeneration}
     ]
  /\ publicProofMessages \subseteq Messages

NoUnverifiedAdmission ==
  \A m \in admissions:
    /\ m \in verified
    /\ IsPreflightValid(m)

NoReplayDoubleEffect ==
  /\ \A m \in Messages:
       /\ admissionEffects[m] = 1 <=> m \in admissions
       /\ publicationEffects[m] = 1 <=> m \in publishedMessages
       /\ promotionEffects[m] = 1 <=> m \in promotedMessages
  /\ \A m \in Messages:
       /\ admissionEffects[m] \in {0, 1}
       /\ publicationEffects[m] \in {0, 1}
       /\ promotionEffects[m] \in {0, 1}

ReceiptIsNotPublication ==
  \A m \in publishedMessages:
    /\ receipts[m].recorded
    /\ m \in admissions
    /\ admissionEffects[m] = 1

NomosDecisionRequired ==
  \A m \in admissions \cup publishedMessages:
    /\ nomosDecisions[m].recorded
    /\ HasReceiverAllow(m)

QuarantineNotLive ==
  /\ blockedState \cap admissions = {}
  /\ \A r \in liveRows:
       rowMessage[r] \notin blockedState

GenerationPinPreserved ==
  \A r \in liveRows:
    /\ rowMessage[r] \in Messages
    /\ rowSenderGeneration[r] = SenderGeneration
    /\ rowReceiverGeneration[r] \in ReceiverGenerations
    /\ messageReceiverGeneration[rowMessage[r]] = rowReceiverGeneration[r]

LargeObjectHashBound ==
  \A m \in admissions:
    m \in DetachedMessages => m \in ObjectHashValidMessages

PromotionRequiresReceiverPolicy ==
  \A m \in promotedMessages:
    /\ m \in publishedMessages
    /\ m \in PromotionAuthorizedMessages
    /\ promotionDecisions[m].recorded
    /\ promotionDecisions[m].effect = "allow"
    /\ promotionDecisions[m].usesReceiverPolicy
    /\ promotionDecisions[m].inputHashBound
    /\ nomosDecisions[m].recorded
    /\ nomosDecisions[m].effect \in {"review", "quarantine"}

NoUnauthorizedPromotion ==
  \A m \in Messages:
    (/\ m \notin PromotionAuthorizedMessages
     /\ nomosDecisions[m].recorded
     /\ nomosDecisions[m].effect \in {"review", "quarantine"})
    => /\ ~(promotionDecisions[m].recorded
             /\ promotionDecisions[m].effect = "allow")
       /\ m \notin admissions
       /\ m \notin publishedMessages

PrivatePayloadNotPublic ==
  \A m \in publicProofMessages:
    m \in (PrivateMessages \cup RestrictedMessages) =>
      \/ nomosDecisions[m].hasRedaction
      \/ nomosDecisions[m].hasDeclassification

ProofCompleteness ==
  \A r \in liveRows:
    LET m == rowMessage[r] IN
      /\ m \in Messages
      /\ m \in outbox
      /\ m \in inbox
      /\ m \in verified
      /\ receipts[m].recorded
      /\ receipts[m].bodyHashBound
      /\ nomosDecisions[m].recorded
      /\ nomosDecisions[m].inputHashBound
      /\ HasReceiverAllow(m)
      /\ m \in admissions
      /\ m \in publishedMessages
      /\ rowSenderGeneration[r] = SenderGeneration
      /\ rowReceiverGeneration[r] # NoGeneration

NoSenderPolicyAuthority ==
  \A m \in SenderHintMessages:
    (/\ m \notin ReceiverAuthorizedMessages
     /\ m \notin PromotionAuthorizedMessages)
    =>
      /\ ~(nomosDecisions[m].recorded /\ nomosDecisions[m].effect = "allow")
      /\ ~(promotionDecisions[m].recorded
            /\ promotionDecisions[m].effect = "allow")
      /\ m \notin admissions
      /\ m \notin publishedMessages

====

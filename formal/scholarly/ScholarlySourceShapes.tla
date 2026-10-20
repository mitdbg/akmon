---------------------- MODULE ScholarlySourceShapes ----------------------
EXTENDS FiniteSets

CONSTANTS E1, E2, E3,
          ReviewPaper, ConferencePaper,
          ArticleType, ConferenceType,
          FullVenue, AbbreviatedVenue, ConferenceVenue, AnnualVenue,
          KeyOne, KeyTwo, KeyThree,
          AliasOne, AliasTwo, AliasNorm,
          AliasCanonicalOne, AliasCanonicalTwo

Entries == {E1, E2, E3}
Papers == {ReviewPaper, ConferencePaper}
ConferenceTypes == {ConferenceType}
Aliases == {AliasOne, AliasTwo}

PaperOf(entry) ==
    CASE entry = E1 -> ReviewPaper
      [] entry = E2 -> ReviewPaper
      [] entry = E3 -> ConferencePaper

KeyOf(entry) ==
    CASE entry = E1 -> KeyOne
      [] entry = E2 -> KeyTwo
      [] entry = E3 -> KeyThree

EntryTypeOf(entry) ==
    CASE entry = E1 -> ArticleType
      [] entry = E2 -> ArticleType
      [] entry = E3 -> ConferenceType

VenueOf(entry) ==
    CASE entry = E1 -> FullVenue
      [] entry = E2 -> AbbreviatedVenue
      [] entry = E3 -> ConferenceVenue

HasYear(entry) == entry \in Entries

CanonicalVenueOf(venue) ==
    CASE venue = FullVenue -> AnnualVenue
      [] venue = AbbreviatedVenue -> AnnualVenue
      [] venue = ConferenceVenue -> ConferenceVenue

AliasNormalized(alias) == AliasNorm

AliasCanonical(alias) ==
    CASE alias = AliasOne -> AliasCanonicalOne
      [] alias = AliasTwo -> AliasCanonicalTwo

VARIABLES processed, bibKeys, venues, conferences

vars == <<processed, bibKeys, venues, conferences>>

Init ==
    /\ processed = {}
    /\ bibKeys = [paper \in Papers |-> {}]
    /\ venues = {}
    /\ conferences = {}

Ingest(entry) ==
    /\ processed' = processed \cup {entry}
    /\ bibKeys' = [bibKeys EXCEPT
        ![PaperOf(entry)] = @ \cup {KeyOf(entry)}]
    /\ venues' = venues \cup {CanonicalVenueOf(VenueOf(entry))}
    /\ conferences' =
        IF /\ EntryTypeOf(entry) \in ConferenceTypes
           /\ HasYear(entry)
        THEN conferences \cup {CanonicalVenueOf(VenueOf(entry))}
        ELSE conferences

Next == \E entry \in Entries : Ingest(entry)

TypeOK ==
    /\ processed \subseteq Entries
    /\ bibKeys \in [Papers -> SUBSET {KeyOf(e) : e \in Entries}]
    /\ venues \subseteq {CanonicalVenueOf(VenueOf(e)) : e \in Entries}
    /\ conferences \subseteq {CanonicalVenueOf(VenueOf(e)) : e \in Entries}

NoLostOrInventedKeys ==
    \A paper \in Papers :
        bibKeys[paper] = {
            KeyOf(e) : e \in {candidate \in processed : PaperOf(candidate) = paper}
        }

OnlyDatedConferenceEntriesMintConference ==
    conferences = {
        CanonicalVenueOf(VenueOf(e)) :
            e \in {candidate \in processed :
                /\ EntryTypeOf(candidate) \in ConferenceTypes
                /\ HasYear(candidate)}
    }

OnlyCanonicalVenuesMinted ==
    venues = {CanonicalVenueOf(VenueOf(e)) : e \in processed}

AliasConfigurationSafe ==
    \/ processed = {}
    \/ \A left, right \in Aliases :
        AliasNormalized(left) = AliasNormalized(right)
        => AliasCanonical(left) = AliasCanonical(right)

ReplayIdempotent ==
    \A entry \in processed :
        /\ processed \cup {entry} = processed
        /\ bibKeys[PaperOf(entry)] \cup {KeyOf(entry)}
           = bibKeys[PaperOf(entry)]

ScholarlySourceShapeSafety ==
    /\ TypeOK
    /\ NoLostOrInventedKeys
    /\ OnlyDatedConferenceEntriesMintConference
    /\ OnlyCanonicalVenuesMinted
    /\ AliasConfigurationSafe
    /\ ReplayIdempotent

Spec == Init /\ [][Next]_vars

=============================================================================

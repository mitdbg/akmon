"""Generic reader for a source manifest (`okg.source/v1`): files and JSON APIs.

`okg deployment add-source <d> <name> --manifest <file>` saves a copy of the
manifest under `<deployment>/sources/<name>.source.yaml` and registers this
reader with `params: {manifest: <copy>, source: <name>}`. Each run re-reads
every file the manifest's glob matches and maps it with
`okg.substrate.sources.manifest`, the same code the add-source preview runs.

The reader holds the rules every hand-written connector repeats:

- identity: node ids are `<source>:<type>:<key>`; each record, nested ones
  included, is its own record-set entry keyed `<type>:<key>`;
- change: a record's revision hashes its mapped content, the manifest digest
  and the chunker identity; an unchanged record writes no fact;
- completeness: a read that cannot read a file, is cancelled, or rejects a
  record that existed before is incomplete and removes nothing; two records
  with the same key in one file stop the run before anything is applied;
- deletion: `missing_from_full_read` declares the current record set, so a
  record missing from a complete read is retracted; `never` declares the
  earlier record set plus the current one, so no record is ever retracted
  while a re-read record still loses links and passages it no longer has;
- mass deletion (phase 2): a read that would remove more than
  `max_deleted_fraction` of the earlier records (default 0.5 when any read
  is HTTP) applies its records and removes nothing;
- unknown footprint (phase 2): when the runner says the earlier record set
  was discarded (`--reset-cursor`) while the source still has rows, a read
  that must keep earlier records refuses instead of dropping them;
- conditional GET (phase 2): when every HTTP page of the last run carried
  an `ETag` or `Last-Modified` and the server answers 304 for each, the
  change probe reports the exact token of the last run, so the run is
  skipped.

Design: pact/changes/no-code-source-manifest/design.md and
pact/changes/no-code-source-manifest-http/design.md.
"""
from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path
from typing import Any, Mapping, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT,
    RecordEmission,
    RecordSet,
    SourceHealth,
    SourceRun,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.sources import manifest as manifest_module
from okg.substrate.sources import manifest_http as manifest_http_module
from okg.substrate.sources.manifest import (
    MAX_KEYLESS_FRACTION,
    keyless_over_share,
    keyless_share_message,
    ManifestError,
    ManifestProblem,
    load_manifest,
    map_records,
    match_files,
    parse_manifest,
    record_emission,
    record_facts,
)
from okg.substrate.sources.manifest_http import (
    page_digest_key,
    page_item_bytes,
    read_inputs,
    unchanged_since,
)

CURSOR_SCHEMA = "okg.source_manifest.cursor/v1"


class _ManifestProbe(ContentHashProbe):
    """The content-hash probe, told the earlier cursor so HTTP reads can be
    checked with conditional GETs (`ManifestRecordSource._content_items`)."""

    def __init__(self, owner: "ManifestRecordSource", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._owner = owner

    def probe(self, *, cursor, sync_scope):  # type: ignore[override]
        self._owner._probe_cursor = cursor if isinstance(cursor, Mapping) else None
        try:
            return super().probe(cursor=cursor, sync_scope=sync_scope)
        finally:
            self._owner._probe_cursor = None


class ManifestRecordSource:
    """Reads the records a saved source manifest describes."""

    name = "source_manifest"
    profile = "discovery_crawl"
    # `okg deployment add-source` refuses a reader without a change probe.
    # The probe hashes the saved manifest and every file its glob matches,
    # re-globbed on each call, plus this module and the mapper, so an edit,
    # an added file, a removed file, a manifest change or a code change each
    # counts as a change and anything else lets `okg ingest` skip the source.
    change_probe_kind = "content_hash"

    def __init__(
        self, manifest: str, source: str, *, env: Optional[Mapping[str, str]] = None,
    ) -> None:
        if not isinstance(source, str) or not source:
            raise ValueError("ManifestRecordSource needs a non-empty source name")
        self.manifest_path = Path(manifest).expanduser()
        self.source = source
        self.last_run_metrics: dict[str, float] = {}
        self._replay_prior: Optional[RecordSet] = None
        self._prior_footprint_known = True
        self._probe_cursor: Optional[Mapping[str, Any]] = None
        self._env = env
        # Each run returns an exact applied token computed from the bytes it
        # parsed (the saved manifest and every matched file), keyed as the
        # probe keys them, and the output depends on nothing else: no record
        # links to another source. So an unchanged probe may skip the read.
        self.probe_short_circuit_safe = True
        self.change_probe = _ManifestProbe(
            self,
            content_items=self._content_items,
            config={"manifest": str(manifest), "source": source},
            emit_targets=(sys.modules[__name__], manifest_module, manifest_http_module),
        )

    def _content_items(self) -> list[tuple[str, Any]]:
        items: list[tuple[str, Any]] = [("manifest", self.manifest_path)]
        try:
            manifest = load_manifest(self.manifest_path)
        except ManifestError:
            # The saved copy itself is hashed above; the run reports why it
            # does not load.
            return items
        items += [(f"file:{path}", Path(path)) for path in match_files(manifest)]
        cursor = self._probe_cursor or {}
        stored = cursor.get("http_pages") if cursor.get("schema") == CURSOR_SCHEMA else None
        for _type_name, read in manifest.all_reads():
            if read.kind != "http":
                continue
            pages = (stored or {}).get(read.where) if isinstance(stored, Mapping) else None
            if pages and unchanged_since(read, pages, env=self._environ()):
                items += [
                    (page_digest_key(read, index), page_item_bytes(str(page["sha256"])))
                    for index, page in enumerate(pages)
                ]
            else:
                # No validators, or the server says it changed: the read
                # must run. A fresh value makes the token differ.
                items.append((f"http:{read.where}:changed", os.urandom(16)))
        return items

    def _environ(self) -> Mapping[str, str]:
        return os.environ if self._env is None else self._env

    def prepare_authoritative_replay(
        self, *, record_set: RecordSet, prior_footprint_known: bool = True,
    ) -> None:
        """Keep the prior record set: a replay resets this reader's cursor,
        and `deletion: never` must still carry every earlier record.

        `prior_footprint_known=False` is the runner saying the earlier record
        set was discarded (`--reset-cursor`) while the source still has rows:
        a read that must keep earlier records then refuses."""
        self._replay_prior = record_set
        self._prior_footprint_known = bool(prior_footprint_known)

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[Mapping[str, Any]] = None,
        sync_scope: Optional[Mapping[str, Any]] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        # What prepare_authoritative_replay said holds for this run only.
        replay_prior, prior_footprint_known = self._replay_prior, self._prior_footprint_known
        self._replay_prior, self._prior_footprint_known = None, True
        try:
            try:
                manifest_bytes = self.manifest_path.read_bytes()
                manifest_text = manifest_bytes.decode("utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                raise ManifestError([ManifestProblem(
                    "manifest_unreadable",
                    f"cannot read the saved manifest {self.manifest_path}: {exc}",
                    "re-run `okg deployment add-source --manifest` to restore it",
                )]) from exc
            manifest = parse_manifest(
                manifest_text, base_dir=self.manifest_path.resolve().parent,
                origin=str(self.manifest_path),
            )
            inputs = read_inputs(manifest, env=self._environ())
        except ManifestError as exc:
            raise SubstrateError(
                f"source {self.source!r}: {exc}",
                data={"source": self.source, **exc.as_dict()},
                fix_hint=exc.fix,
            ) from exc
        prior = cursor if isinstance(cursor, Mapping) else {}
        if prior.get("schema") != CURSOR_SCHEMA:
            prior = {}
        previous: dict[str, str] = dict(prior.get("revisions") or {})
        prior_emissions: dict[str, Any] = dict(prior.get("emissions") or {})
        if not prior_emissions and replay_prior is not None:
            prior_emissions = {
                key: emission.to_cursor() for key, emission in replay_prior.records.items()
            }
        known_before = set(previous) | set(prior_emissions)

        mapped = map_records(
            manifest, source=self.source, inputs=inputs.records, input_types=inputs.record_types,
        )
        if mapped.fatal:
            first = mapped.duplicate_keys_in_file[0]
            raise SubstrateError(
                f"source {self.source!r}: {first.message} ({first.file}); "
                "nothing was applied or removed",
                data={
                    "source": self.source,
                    "reason_code": "duplicate_key_in_file",
                    "duplicates": [p.as_dict() for p in mapped.duplicate_keys_in_file],
                },
                fix_hint=first.fix,
            )

        # A record with no key value cannot be imported or matched to an
        # earlier one, so it is skipped and counted. When too many of one
        # type's records have none, the key path is probably wrong, and
        # deleting everything the read missed would be a mass deletion:
        # refuse the read.
        # Under `deletion: never` nothing is removed, so skipping is enough.
        keyless_total = sum(mapped.keyless_by_type.values())
        over = keyless_over_share(mapped) if manifest.deletion != "never" else {}
        if over:
            raise SubstrateError(
                f"source {self.source!r}: {keyless_share_message(over)}; "
                "nothing was applied or removed",
                data={
                    "source": self.source,
                    "reason_code": "keyless_share_exceeded",
                    "keyless": {name: {"keyless": c, "read": t} for name, (c, t) in over.items()},
                    "max_keyless_fraction": MAX_KEYLESS_FRACTION,
                },
                fix_hint=(
                    "check the type's key path against the data (`okg source draft` shows "
                    "the fields); records with no value at the key path are skipped"
                ),
            )

        # What this read could not account for. Under
        # `missing_from_full_read` any of it withholds ALL deletion for this
        # read: valid records still apply, and every earlier record is
        # carried forward. Under `never` nothing is ever removed, so these
        # are warnings only.
        bad: list[str] = []
        for problem in inputs.failed:
            if problem.code == "http_read_failed":
                bad.append(f"incomplete read {problem.path}: {problem.message}")
            else:
                bad.append(f"unreadable file {problem.file}: {problem.message}")
        for rejected in mapped.rejected:
            if rejected.record_key is not None and rejected.record_key in mapped.records:
                continue  # another parent supplied a valid copy
            if rejected.missing_key:
                continue  # skipped and counted above
            label = rejected.record_key or "a record with no usable key"
            bad.append(f"{rejected.file} {rejected.record} ({label}): {rejected.reason}")
        keep_prior = manifest.deletion == "never" or bool(bad)
        # Mass-deletion guard: a read that would remove more than
        # max_deleted_fraction of the earlier records removes nothing. An
        # empty read is left to the runner's full-retraction guard, which
        # `okg ingest --allow-full-retraction <source>` overrides.
        guard: Optional[str] = None
        fraction = manifest.max_deleted_fraction
        removing = 0
        if not keep_prior and fraction is not None and prior_emissions and mapped.records:
            removing = sum(1 for key in prior_emissions if key not in mapped.records)
            if removing / len(prior_emissions) > fraction:
                keep_prior = True
                guard = (
                    f"this read would remove {removing} of {len(prior_emissions)} earlier records "
                    f"({removing / len(prior_emissions):.0%}), more than max_deleted_fraction "
                    f"{fraction:g}; the records read were applied and NOTHING was removed. If the "
                    "removal is intended, raise max_deleted_fraction in the manifest for one run "
                    "(re-run `okg deployment add-source --manifest` with it, then lower it again); "
                    "if every record is going, run `okg ingest --allow-full-retraction "
                    f"{self.source}`"
                )
        footprint_lost = not prior_footprint_known and not prior_emissions
        if footprint_lost and not keep_prior and fraction is not None:
            # The mass-deletion guard compares against the earlier record
            # set, which --reset-cursor discarded: it cannot be checked, and
            # the runner would retract whatever this read misses.
            raise SubstrateError(
                f"source {self.source!r}: the source's state was reset (--reset-cursor), so "
                f"max_deleted_fraction {fraction:g} cannot be checked against its earlier "
                "records and this read could remove any number of them; nothing was applied "
                "or removed",
                data={"source": self.source, "reason_code": "prior_footprint_unknown",
                      "max_deleted_fraction": fraction},
                fix_hint=(
                    "run `okg ingest` without --reset-cursor; to rebuild the source from "
                    "scratch, remove it with `okg deployment remove-source` and add it again"
                ),
            )
        if keep_prior and footprint_lost:
            raise SubstrateError(
                f"source {self.source!r}: this read must keep the earlier records "
                f"({'deletion: never' if manifest.deletion == 'never' else '; '.join(bad[:5]) or guard}), "
                "but the source's state was reset (--reset-cursor) and its earlier record set is "
                "unknown, so they would be dropped; nothing was applied or removed",
                data={"source": self.source, "reason_code": "prior_footprint_unknown",
                      "problems": bad[:20]},
                fix_hint=(
                    "run `okg ingest` without --reset-cursor; to rebuild the source from "
                    "scratch, remove it with `okg deployment remove-source` and add it again"
                ),
            )
        if keep_prior and known_before and not prior_emissions:
            # The earlier record set is not available to carry forward, so
            # this read cannot withhold deletion; refuse it whole.
            raise SubstrateError(
                f"source {self.source!r}: this read must keep the earlier records "
                f"({'deletion: never' if manifest.deletion == 'never' else '; '.join(bad[:5])}), "
                "but the earlier record set is not available; nothing was applied or removed",
                data={"source": self.source, "reason_code": "prior_record_set_unavailable",
                      "problems": bad[:20]},
                fix_hint=(
                    "fix the rows named and re-run `okg ingest`; if the source's "
                    "state was reset, remove and add the source again"
                ),
            )
        if keep_prior and known_before:
            # Links to earlier records stay valid: those records are kept.
            mapped = map_records(
                manifest, source=self.source, inputs=inputs.records,
                known_keys=frozenset(known_before), input_types=inputs.record_types,
            )

        facts: list[Any] = []
        revisions: dict[str, str] = {}
        records: dict[str, RecordEmission] = {}
        changed = 0
        for key, record in mapped.records.items():
            revisions[key] = record.revision
            if previous.get(key) == record.revision and key in prior_emissions:
                # Unchanged (by content, or by its changes_when field): keep
                # the footprint that was written, since no fact is written now.
                records[key] = RecordEmission.from_cursor(prior_emissions[key])
                continue
            records[key] = record_emission(record, source=self.source)
            if previous.get(key) != record.revision:
                changed += 1
                facts.extend(record_facts(record, source=self.source))
        kept = 0
        if keep_prior:
            for key, raw in prior_emissions.items():
                if key not in records:
                    kept += 1
                    records[key] = RecordEmission.from_cursor(raw)
                    if key in previous:
                        revisions[key] = previous[key]

        metrics = {
            "files.matched": float(len(inputs.files)),
            "files.failed": float(len(inputs.failed)),
            "records.observed": float(len(mapped.records)),
            "records.changed": float(changed),
            "records.rejected": float(len(mapped.rejected) - keyless_total),
            "records.skipped_keyless": float(keyless_total),
            "records.kept_from_earlier_reads": float(kept),
            "links.dropped": float(mapped.dropped_links),
            "nested.conflicts": float(mapped.conflicts),
            "reads.http": float(sum(1 for r in inputs.reads if r["kind"] == "http")),
            "reads.failed": float(sum(1 for r in inputs.reads if not r["complete"])),
            "http.pages": float(sum(int(r.get("pages") or 0) for r in inputs.reads)),
            "http.retries": float(sum(int(r.get("retries") or 0) for r in inputs.reads)),
            "deletion.guard_tripped": float(guard is not None),
            "deletion.guard_withheld": float(removing if guard is not None else 0),
            "changes_when.missing": float(mapped.markers_missing),
        }
        self.last_run_metrics = metrics
        summary = ", ".join(f"{k}={int(v)}" for k, v in metrics.items())
        reason = summary
        if guard is not None:
            reason = f"{guard} ({summary})"
        if bad:
            withheld = (
                "nothing is removed by this read (deletion: never)"
                if manifest.deletion == "never" else
                "valid records were applied and ALL deletion is withheld until these are fixed"
            )
            reason = (
                f"{len(bad)} problem(s): " + "; ".join(bad[:5])
                + (f"; and {len(bad) - 5} more" if len(bad) > 5 else "")
                + f"; {withheld} ({summary})"
            )
        narrowed = bool((sync_scope or {}).get("scope_keys"))
        next_cursor: dict[str, Any] = {
            "schema": CURSOR_SCHEMA,
            "manifest_digest": manifest.digest,
            "revisions": revisions,
            "emissions": {key: e.to_cursor() for key, e in records.items()},
            "last_run": metrics,
        }
        if bad:
            next_cursor["problems"] = bad[:20]
        if guard is not None:
            next_cursor["deletion_guard"] = guard
        if inputs.http_pages:
            next_cursor["http_pages"] = inputs.http_pages
        return SourceRun(
            facts=facts,
            completed_scope=True,
            next_cursor=next_cursor,
            run_mode="scope_complete",
            record_set=RecordSet(records),
            applied_raw_token=self.change_probe.token_from_item_hashes({
                "manifest": hashlib.sha256(manifest_bytes).hexdigest(),
                **inputs.digests,
            }),
            applied_token_scope="global",
            applied_token_authority="exact",
            # An HTTP read cannot be re-read to confirm the token: the token
            # is computed from exactly the response bytes this run parsed,
            # so it is a sealed snapshot. A later change is seen next poll.
            **(
                {"applied_token_binding": APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT}
                if manifest.has_http else {}
            ),
            # The reader always reads its whole configured scope.
            effective_scope_complete=narrowed,
            health=SourceHealth(
                status="ok", mode="live", record_count=len(records), reason=reason,
            ),
        )


__all__ = ["CURSOR_SCHEMA", "ManifestRecordSource"]

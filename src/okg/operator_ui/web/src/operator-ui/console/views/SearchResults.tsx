// Explore's results and landing blocks: the type chips, the results with
// their rail and pages, and one type's listing.
import { CopyableId } from "@operator-ui/console/components/CopyControls";
import { Marked } from "@operator-ui/console/components/TextMarks";
import type { ChipStrip as ChipStripModel, ListingModel } from "@operator-ui/console/model/explore/landing";
import { type Paging, type ResultRow, type ResultsModel, resultRowId } from "@operator-ui/console/model/explore/search";
import type { TitledGroup } from "@operator-ui/console/model/explore/titles";
import { exact } from "@operator-ui/console/model/shared/format";
import { tidyExcerpt } from "@operator-ui/console/api/searchData";
import { Chip } from "@operator-ui/console/components/Chip";
import { Fragment, type ReactElement } from "react";

// --- the chips ------------------------------------------------------------------

export function ChipStrip({ strip, onMore }: { strip: ChipStripModel; onMore: (() => void) | null }): ReactElement | null {
  if (!strip.chips.length) return null;
  return (
    <div className="okg-console__chips" data-testid="explore-chips">
      <p className="okg-console__count-line okg-console__chipgroup" data-group="type">
        {strip.chips.map((chip) => (
          <Chip
            key={chip.type}
            className="okg-console__chipfilter okg-console__typechip"
            checked={chip.selected}
            onToggle={() => { window.location.hash = chip.href; }}
          >
            <code className="okg-console__count-label okg-console__typechip-name">{chip.type}</code>
            {chip.count !== null ? (
              <span className="okg-console__count-value">{exact(chip.count)}</span>
            ) : null}
          </Chip>
        ))}
        {strip.moreTypes ? (
          <button type="button" className="okg-console__link-button okg-console__chipmore" onClick={onMore ?? undefined}>
            and {exact(strip.moreTypes)} more {strip.moreTypes === 1 ? "type" : "types"}
            {" · "}{exact(strip.moreRecords)} records
          </button>
        ) : null}
      </p>
      {strip.countNote ? <p className="okg-console__meta">{strip.countNote}</p> : null}
    </div>
  );
}

// --- the results ----------------------------------------------------------------

/** One result: the row's own title as the link, one meta line, a two-line
 *  snippet with the match marked, then the actions that exist.
 *
 *  Inside a document's group the title is the chunk's position -- "chunk 3" --
 *  because the document is the heading above it and printing the file name on
 *  every chunk of it is the same words four times. */
function ResultItem({
  row, query, focused, inGroup,
}: { row: ResultRow; query: string; focused: boolean; inGroup: boolean }): ReactElement {
  const hit = row.hit;
  return (
    <li
      className="okg-console__result okg-console__copyrow"
      // The id the search box names in `aria-activedescendant` while the
      // arrow keys have this row, so a screen reader announces the row the
      // reader moved to while focus stays in the box.
      id={resultRowId(row.index)}
      data-focused={focused ? "true" : "false"}
      data-index={row.index}
      data-grouped={inGroup ? "true" : "false"}
      data-testid="explore-result"
    >
      <a className="okg-console__result-title" href={row.href}><Marked text={row.title} query={query} /></a>
      <p className="okg-console__result-meta">
        {row.meta.map((part, index) => (
          <Fragment key={index}>
            {index ? " · " : ""}
            {index === 0 ? <code className="okg-console__id">{part}</code> : part}
          </Fragment>
        ))}
        {/* The record id is NOT here. It is a 12-hex hash on every row of the
            page, it is on the record page under the title, and it is in the
            copy text; on a result row it pushed the two facts that tell one
            result from another off to the left. */}
      </p>
      {hit.excerpt ? (
        <p className="okg-console__result-snippet">
          <Marked text={tidyExcerpt(hit.excerpt).replace(/\[nomos:masked\]/g, "hidden")} query={query} />
        </p>
      ) : null}
      {/* Nothing else on the row. The Related count and the Open-in-source
          link are cut because no published generation carries related_count
          or citation_url on a search row; "Copy id" is cut because it was a
          control under EVERY result for an identifier the row no longer
          shows, and the record page one click away copies it. */}
    </li>
  );
}

/** A run of results under one heading.
 *
 *  A document whose chunks matched is ONE entry on the page with its chunks
 *  under it, rather than four rows carrying the same file name. A record that
 *  is not a chunk is a group of one and prints no heading: its own title is
 *  already the row. */
function ResultGroup({
  group, query, focus,
}: { group: TitledGroup<ResultRow>; query: string; focus: number | null }): ReactElement {
  if (!group.chunks) {
    return (
      <>
        {group.rows.map((row) => (
          <ResultItem key={row.hit.nodeId} row={row} query={query} focused={focus === row.index} inGroup={false} />
        ))}
      </>
    );
  }
  return (
    <li className="okg-console__resultgroup" data-testid="explore-result-group">
      <p className="okg-console__resultgroup-head">
        {group.heading}
        {group.rows.length > 1 ? (
          <span className="okg-console__resultgroup-count">
            {" · "}{exact(group.rows.length)} matching chunks
          </span>
        ) : null}
      </p>
      <ul className="okg-console__resultgroup-rows">
        {group.rows.map((row) => (
          <ResultItem key={row.hit.nodeId} row={row} query={query} focused={focus === row.index} inGroup />
        ))}
      </ul>
    </li>
  );
}

function Rail({ model }: { model: ResultsModel }): ReactElement | null {
  // Nothing to filter by: every section was dropped because it offered one
  // name. The aside is not rendered at all, so the page does not keep a
  // 200 px empty column and push the results into the middle of the window.
  if (!model.rail.some((section) => section.options.length)) return null;
  return (
    <aside className="okg-console__rail" data-testid="explore-rail" aria-label="Filters">
      {model.rail.filter((section) => section.options.length).map((section) => (
        <section className="okg-console__rail-section" key={section.key} data-section={section.key}>
          <h2 className="okg-console__rail-title">{section.title}</h2>
          <ul className="okg-console__rail-list" role="group">
            {section.options.map((option) => (
              <li key={option.value}>
                {/* A real button, not an anchor wearing `role="checkbox"`.
                    That anchor announced itself to a screen reader as a
                    checkbox and then ignored the key a checkbox takes: Space
                    scrolled the page, because an anchor's keyboard is Enter
                    alone. A button takes both, and `aria-pressed` is the
                    state a toggle actually has. */}
                {option.href ? (
                  <button
                    type="button"
                    className="okg-console__rail-option"
                    aria-pressed={option.selected}
                    data-selected={option.selected ? "true" : "false"}
                    onClick={() => { window.location.hash = option.href as string; }}
                  >
                    <span className="okg-console__rail-mark" aria-hidden="true">
                      {option.selected ? "☑" : "☐"}
                    </span>
                    <span className="okg-console__rail-label">{option.label}</span>
                    {option.count !== null ? <span className="okg-console__rail-count">{exact(option.count)}</span> : null}
                  </button>
                ) : (
                  /* No href: this option cannot be selected or cleared, so it
                     carries no box. The "☐" drew an empty checkbox on a thing
                     with no control behind it -- the one mark on the rail that
                     promised something and did nothing. */
                  <span className="okg-console__rail-option okg-console__rail-option--inert" data-selected={option.selected ? "true" : "false"}>
                    <span className="okg-console__rail-label">{option.label}</span>
                    {option.count !== null ? <span className="okg-console__rail-count">{exact(option.count)}</span> : null}
                  </span>
                )}
              </li>
            ))}
          </ul>
          {section.more ? <p className="okg-console__rail-more">and {exact(section.more)} more</p> : null}
          {section.note ? <p className="okg-console__rail-note">{section.note}</p> : null}
        </section>
      ))}
    </aside>
  );
}

function Pages({ paging }: { paging: Paging }): ReactElement | null {
  if (paging.kind === "none") return null;
  if (paging.kind === "more") {
    return (
      <nav className="okg-console__pages" data-testid="explore-pages" aria-label="More results">
        {paging.moreHref ? <a className="okg-console__page-link" href={paging.moreHref}>Load 20 more</a> : null}
        <span className="okg-console__pages-note">{paging.note}</span>
      </nav>
    );
  }
  return (
    <nav className="okg-console__pages" data-testid="explore-pages" aria-label="Pages">
      {paging.prevHref ? <a className="okg-console__page-link" href={paging.prevHref}>Previous</a> : null}
      {paging.pages.map((page, index) => (page.number === 0
        ? <span key={`gap-${index}`} className="okg-console__page-gap">…</span>
        : page.current
          ? <span key={page.number} className="okg-console__page-current" aria-current="page">{exact(page.number)}</span>
          : <a key={page.number} className="okg-console__page-link" href={page.href ?? "#"}>{exact(page.number)}</a>))}
      {paging.nextHref ? <a className="okg-console__page-link" href={paging.nextHref}>Next</a> : null}
      <span className="okg-console__pages-note">{paging.note}</span>
    </nav>
  );
}

export function Results({
  model, query, focus, chips = null, onMoreChips = null,
}: {
  model: ResultsModel;
  query: string;
  focus: number | null;
  /** The type chips, on a page that matched nothing. */
  chips?: ChipStripModel | null;
  onMoreChips?: (() => void) | null;
}): ReactElement {
  const marked = model.filters.parsed.text;
  // "Try browsing the types" is the chips, in words. With the chips on the
  // page it is a link to what is already under it.
  const broaden = chips
    ? model.broaden.filter((action) => action.label !== "browsing the types")
    : model.broaden;
  return (
    <div
      className="okg-console__results-page"
      data-testid="explore-results-page"
      data-rail={model.rail.some((section) => section.options.length) ? "true" : "false"}
    >
      <Rail model={model} />
      <div className="okg-console__results" data-testid="explore-results">
        {/* "0 results" over "No record matches ... in version ..." is the
            same fact twice, and the count is the less useful of the two. */}
        {model.noMatchSentence ? null : (
          <p className="okg-console__about" data-testid="explore-about">{model.aboutLine}</p>
        )}
        {model.unappliedNote ? <p className="okg-console__status-note" data-testid="explore-unapplied">{model.unappliedNote}</p> : null}
        {model.skippedTypes ? (
          <details className="okg-console__skipped-types" data-testid="explore-skipped-types">
            <summary title={model.skippedTypes.names.join(", ")}>{model.skippedTypes.line}</summary>
            <p>Not in this version: {model.skippedTypes.names.join(", ")}.</p>
          </details>
        ) : null}
        {model.noMatchSentence ? (
          <div data-testid="explore-empty-results">
            <p className="okg-console__fact">{model.noMatchSentence}</p>
            {chips ? <ChipStrip strip={chips} onMore={onMoreChips} /> : null}
            {broaden.length ? (
              <p className="okg-console__fact okg-console__broaden">
                Try{" "}
                {broaden.map((action, index) => (
                  <Fragment key={action.label}>
                    {index ? " or " : ""}
                    <a href={action.href}>{action.label}</a>
                  </Fragment>
                ))}
                .
              </p>
            ) : null}
          </div>
        ) : (
          <>
            {model.idGroups.length ? (
              <>
                <p className="okg-console__results-head">ID match</p>
                <ul className="okg-console__rows okg-console__result-list">
                  {model.idGroups.map((group) => (
                    <ResultGroup key={group.rows[0].hit.nodeId} group={group} query={marked || query} focus={focus} />
                  ))}
                </ul>
              </>
            ) : null}
            {model.labelGroups.length ? (
              <>
                <p className="okg-console__results-head">Label match</p>
                <ul className="okg-console__rows okg-console__result-list">
                  {model.labelGroups.map((group) => (
                    <ResultGroup key={group.rows[0].hit.nodeId} group={group} query={marked || query} focus={focus} />
                  ))}
                </ul>
              </>
            ) : null}
            {model.textGroups.length ? (
              <>
                {/* Named only under an exact run: without it the first text
                    match reads as one more exact match. A page of text
                    matches alone needs no heading. */}
                {model.idGroups.length || model.labelGroups.length
                  ? <p className="okg-console__results-head">Text match</p>
                  : null}
                <ul className="okg-console__rows okg-console__result-list">
                  {model.textGroups.map((group) => (
                    <ResultGroup key={group.rows[0].hit.nodeId} group={group} query={marked || query} focus={focus} />
                  ))}
                </ul>
              </>
            ) : null}
            <Pages paging={model.paging} />
          </>
        )}
      </div>
    </div>
  );
}

// --- the listing ----------------------------------------------------------------

export function Listing({ model }: { model: ListingModel }): ReactElement {
  return (
    <div className="okg-console__listing" data-testid="explore-list">
      <p className="okg-console__tablecount">{model.countLine}</p>
      {model.emptySentence ? <p className="okg-console__fact">{model.emptySentence}</p> : (
        <table className="okg-console__ltable" data-testid="explore-list-table">
          <thead>
            <tr>
              <th scope="col" data-col="label">Label</th>
              <th scope="col" data-col="type">Type</th>
              <th scope="col" data-col="source">Source</th>
              <th scope="col" data-col="id">Record id</th>
            </tr>
          </thead>
          <tbody>
            {model.rows.map(({ summary, href }) => (
              <tr
                key={summary.nodeId}
                className="okg-console__prow okg-console__copyrow"
                onClick={(event) => {
                  if ((event.target as HTMLElement).closest("a, button")) return;
                  window.location.hash = href;
                }}
              >
                <td data-col="label" title={summary.label}>
                  <a className="okg-console__prow-name" href={href}>{summary.label}</a>
                </td>
                <td data-col="type"><code className="okg-console__id">{summary.type}</code></td>
                <td data-col="source">{summary.source ?? "no source recorded"}</td>
                <td data-col="id" title={summary.nodeId}>
                  <CopyableId text={summary.nodeId} what="record id" reveal="row" />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {model.shortPageNote ? <p className="okg-console__meta">{model.shortPageNote}</p> : null}
      {model.firstHref || model.nextHref ? (
        <p className="okg-console__more">
          {model.firstHref ? <a href={model.firstHref}>First page</a> : null}
          {model.firstHref && model.nextHref ? " · " : null}
          {model.nextHref ? <a href={model.nextHref}>Next page</a> : null}
        </p>
      ) : null}
    </div>
  );
}

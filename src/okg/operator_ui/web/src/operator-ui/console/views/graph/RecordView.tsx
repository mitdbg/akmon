// One record's neighbourhood: the two-sided tree with its side panel, or,
// where the side panel would stack under the stage (820 px and less), the same
// groups as two lists. Ported from the approved prototype (v2.html,
// drawRecord, railRecord and drawRecordTable).
//
// The URL is the state (see model/graph/params.ts): `group=` is the open
// group and `sel=` the selected neighbour, so a shared link or Back opens the
// same view. Opening, closing and selecting REPLACE the history entry through
// the page's `setParams`; centring on another record PUSHES one (`onOpen`).
// A `group=` or `sel=` this record does not have is named on the page's one
// ignored-parameter line (Graph.tsx, `recordChecks`).
//
// The side panel says what the canvas cannot: at rest the record's identity,
// with a group open that group's listed records and ids, with a record
// selected where it sits and where its link came from.
import { type KeyboardEvent as ReactKeyboardEvent, type ReactElement, type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import { GRAPH_RECORD_ROUTE } from "@operator-ui/console/api/graph";
import { CopyableId } from "@operator-ui/console/components/CopyControls";
import { ReadingLine, type RecoveringRead } from "@operator-ui/console/components/readRecovery";
import { type ProducerNamer, cat, fmt, plural } from "@operator-ui/console/model/graph/derive";
import type { NeighbourGroup, RecordNeighbourhoodRead } from "@operator-ui/console/model/graph/reads";
import {
  type GroupRef, type RecordSelection, type SetParams, exploreRecordHref, exploreTypeHref, graphRecordHref, groupParam, groupPatch,
  recordSelection, relatedHref, sameGroup, selPatch,
} from "@operator-ui/console/model/graph/params";
import {
  ABSENT_TYPE, type Leaf, SIDE_CAP, SIDE_TITLE, type TreeGroup, basisText, capClause, capClauses, countText, exploreLinkText, foldNames, foldOf,
  hasNoRelationships, itemName, keyOf, kindList, limitNotes, prose, relationSentence, rtSide, sideTotal, toTreeGroup, toTreeGroups, trunc,
} from "@operator-ui/console/model/graph/recordTree";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { GraphFailure } from "@operator-ui/console/views/graph/GraphFailure";
import { type TreeHandle, type TreeSelection, createRecordTree } from "@operator-ui/console/views/graph/recordTreeDraw";
import { ZoomButtons, useRefitOnResize } from "@operator-ui/console/views/graph/TypeMapView";

/** At and below this window width the side panel would stack under the
 *  stage, so the neighbourhood is drawn as lists instead. */
export const LISTS_MAX_WIDTH = 820;

/** The rail's width; the stage is the window less it above 820 px. */
const RAIL_W = 340;

export interface RecordPageProps {
  /** The id on the URL: "" when it names none. */
  recordId: string;
  record: RecoveringRead<RecordNeighbourhoodRead>;
  /** The read for THIS id, or null while it is pending (a read left over
   *  from the last id is never shown for another URL). */
  read: RecordNeighbourhoodRead | null;
  /** The version the URL asks for (null: the latest), for the failure line. */
  askedVersion: string | null;
  params: Record<string, string>;
  link: LinkFn;
  setParams: SetParams;
  /** Centre the page on another record: a new history entry. */
  onOpen: (id: string) => void;
  producers: ProducerNamer;
  /** The window's width. */
  width: number;
}

/** The record page's body: no id, the read's failure, the wait, or the
 *  neighbourhood. */
export function RecordPage({
  recordId, record, read, askedVersion, params, link, setParams, onOpen, producers, width,
}: RecordPageProps): ReactElement {
  const single = (children: ReactNode) => <div className="work single"><div className="tablewrap">{children}</div></div>;
  if (!recordId) {
    return single(<p className="tnote">This address names no record: add <code>?id=</code> and a record id, or pick one with the search box.</p>);
  }
  if (record.error !== null) {
    // A record the version does not hold, nothing published and a refusal
    // are one plain line with no Read again; every other failure is loud.
    return single(
      <GraphFailure
        error={record.error} route={GRAPH_RECORD_ROUTE} what="This record's neighbourhood" readAgain={record.readAgain}
        retrying={record.retrying} askedVersion={askedVersion} recordId={recordId}
      />,
    );
  }
  if (!read) return single(<ReadingLine what="this record's neighbourhood" />);
  return <RecordView read={read} params={params} link={link} setParams={setParams} onOpen={onOpen} producers={producers} width={width} />;
}

export interface RecordViewProps {
  read: RecordNeighbourhoodRead;
  params: Record<string, string>;
  link: LinkFn;
  setParams: SetParams;
  onOpen: (id: string) => void;
  producers: ProducerNamer;
  width: number;
}

/** What the view does to the URL. */
interface Nav {
  setGroup: (g: GroupRef | null) => void;
  setSel: (g: GroupRef, id: string | null) => void;
  /** Centre the graph on another record: a new history entry. */
  open: (id: string) => void;
}

function useNav(read: RecordNeighbourhoodRead, setParams: SetParams, onOpen: (id: string) => void, group: GroupRef | null): Nav {
  return {
    setGroup: (g) => setParams(groupPatch(g)),
    setSel: (g, id) => setParams(selPatch(read, group, g, id)),
    open: onOpen,
  };
}

export function RecordView(props: RecordViewProps): ReactElement {
  return props.width <= LISTS_MAX_WIDTH ? <RecordLists {...props} /> : <RecordTree {...props} />;
}

/** Escape clears the selection, then closes the open group. */
function useEscape(clear: () => boolean): void {
  const ref = useRef(clear);
  ref.current = clear;
  useEffect(() => {
    const on = (ev: KeyboardEvent) => {
      if (ev.key !== "Escape" || ev.defaultPrevented) return;
      const t = ev.target;
      if (t instanceof Element && t.closest("input, textarea, select, [contenteditable='true']")) return;
      if (ref.current()) ev.preventDefault();
    };
    window.addEventListener("keydown", on);
    return () => window.removeEventListener("keydown", on);
  }, []);
}

function RecordTree({ read, params, link, setParams, onOpen, producers, width }: RecordViewProps): ReactElement {
  const svgRef = useRef<SVGSVGElement>(null);
  const stageRef = useRef<HTMLDivElement>(null);
  const handle = useRef<TreeHandle | null>(null);
  const focusRef = useRef<string | null>(null);
  const refocus = useRef(false);
  const selection = recordSelection(read, params);
  const openKey = selection.group ? keyOf(toTreeGroup(selection.group)) : null;
  const nav = useNav(read, setParams, onOpen, selection.group);
  const [kinds, setKinds] = useState<"in" | "out" | null>(null);
  const [status, setStatus] = useState("");
  useEffect(() => { setKinds(null); }, [read, params.group, params.sel]);
  const treeSel: TreeSelection = kinds
    ? { kinds }
    : selection.sel
      ? { key: keyOf(toTreeGroup(selection.sel.group)), id: selection.sel.id }
      : openKey ? { key: openKey, id: null } : null;
  const selRef = useRef(treeSel);
  selRef.current = treeSel;
  const navRef = useRef(nav);
  navRef.current = nav;
  const stageWidth = width - RAIL_W;
  const empty = read.groups.length === 0;
  const byKey = useCallback((key: string) => read.groups.find((g) => keyOf(toTreeGroup(g)) === key) ?? null, [read]);
  useEffect(() => {
    if (empty || !svgRef.current || !stageRef.current) return undefined;
    handle.current = createRecordTree({
      svg: svgRef.current, stage: stageRef.current, stageWidth: stageRef.current.clientWidth || stageWidth,
      read, expanded: openKey, selected: selRef.current,
      focusId: refocus.current ? focusRef.current : null,
      exploreHref: (g) => relatedHref(link, read.record.id, { direction: g.d, relationship: g.e, type: g.type }),
      onToggle: (key) => {
        const g = byKey(key);
        navRef.current.setGroup(g && key !== openKey ? g : null);
      },
      onSelectLeaf: (key, id) => { const g = byKey(key); if (g) navRef.current.setSel(g, id); },
      onKinds: (dir) => setKinds((k) => (k === dir ? null : dir)),
      onBackground: () => { clearRef.current(); },
      onHome: () => { setKinds(null); navRef.current.setGroup(null); },
      onOpen: (id) => navRef.current.open(id),
      onNavigate: (href) => { window.location.hash = href; },
      onStatus: setStatus,
      onFocusNode: (id) => { focusRef.current = id; },
    });
    const stage = stageRef.current;
    return () => {
      refocus.current = Boolean(stage && document.activeElement && stage.contains(document.activeElement));
      handle.current?.destroy();
      handle.current = null;
    };
    // The selection is applied below without a redraw.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [read, openKey, stageWidth, empty, link, byKey]);
  const selKey = JSON.stringify(treeSel);
  useEffect(() => { handle.current?.setSelection(selRef.current); }, [selKey]);
  useRefitOnResize(handle);
  function clearOne(): boolean {
    if (kinds) { setKinds(null); return true; }
    if (selection.sel) { nav.setSel(selection.sel.group, null); return true; }
    if (selection.group) { nav.setGroup(null); return true; }
    return false;
  }
  const clearRef = useRef(clearOne);
  clearRef.current = clearOne;
  useEscape(() => clearRef.current());
  const onStageKey = (ev: ReactKeyboardEvent<HTMLDivElement>) => {
    if (ev.target instanceof HTMLButtonElement) return;
    if (ev.key === "+" || ev.key === "=") { ev.preventDefault(); handle.current?.zoomBy(1.3); }
    else if (ev.key === "-" || ev.key === "−") { ev.preventDefault(); handle.current?.zoomBy(1 / 1.3); }
    else if (ev.key === "0") { ev.preventDefault(); handle.current?.fit(true); }
  };
  return (
    <div className="work">
      <div className="stagecol">
        <div className="stage" ref={stageRef} data-testid="graph-stage" onKeyDown={onStageKey}>
          {empty ? (
            <EmptyNeighbourhood read={read} className="rt-empty" />
          ) : (
            <>
              <svg ref={svgRef} role="group" aria-label={`The records linked to ${read.record.label || read.record.id}`} />
              <ZoomButtons handle={handle} />
            </>
          )}
        </div>
        {empty ? null : <div className="status" data-testid="graph-status">{status}</div>}
      </div>
      <aside className="rail" aria-live="polite" data-testid="graph-rail">
        <RecordRail read={read} selection={selection} kinds={kinds} link={link} producers={producers} nav={nav} onKinds={setKinds} />
      </aside>
    </div>
  );
}

/** An identifier that may wrap after a path or name separator, never inside
 *  a word: "src/okg/…/node_detail.py" breaks after a slash. */
function Breakable({ text }: { text: string }): ReactElement {
  if (prose(text)) return <>{text}</>;
  const parts = text.split(/(?<=[/.:_])/);
  return <>{parts.map((p, i) => <span key={i}>{p}{i < parts.length - 1 ? <wbr /> : null}</span>)}</>;
}

/** A type, linked to Explore's list of that type; the absent group plain. */
function TypeLink({ type, link }: { type: string | null; link: LinkFn }): ReactElement {
  return type ? <a className="rt-type" href={exploreTypeHref(link, type)}><code>{type}</code></a> : <span className="sub">{ABSENT_TYPE}</span>;
}

/** A producer by name, linked to its Sources page when the producers read names it. */
function Producer({ id, producers }: { id: string; producers: ProducerNamer }): ReactElement {
  const label = producers(id);
  return label.href ? <a href={label.href}>{label.name}</a> : <code>{id}</code>;
}

function Sentence({ g, link }: { g: NeighbourGroup; link: LinkFn }): ReactElement {
  return g.direction === "in"
    ? <><TypeLink type={g.type} link={link} /> <span className="rt-rel">{g.relationship}</span> this record</>
    : <>this record <span className="rt-rel">{g.relationship}</span> <TypeLink type={g.type} link={link} /></>;
}

function floorNote(read: RecordNeighbourhoodRead, g: NeighbourGroup): string {
  return g.countIsFloor ? `, at least: ${capClause(read, g.floorReason)}` : "";
}

/** The stage or list body when no group is drawn: "no relationships" only
 *  when that is true, otherwise what the read could not count. */
function EmptyNeighbourhood({ read, className }: { read: RecordNeighbourhoodRead; className: string }): ReactElement {
  return (
    <p className={className} data-testid="graph-record-empty">
      {hasNoRelationships(read) ? "No relationships in this version." : `${limitNotes(read).join(". ")}.`}
    </p>
  );
}

function RecordRail({
  read, selection, kinds, link, producers, nav, onKinds,
}: {
  read: RecordNeighbourhoodRead;
  selection: RecordSelection;
  kinds: "in" | "out" | null;
  link: LinkFn;
  producers: ProducerNamer;
  nav: Nav;
  onKinds: (dir: "in" | "out" | null) => void;
}): ReactElement {
  const label = read.record.label || read.record.id;
  const colours = kindList(read);
  const dot = (t: string) => <span className="dot rt-dot" style={{ background: cat((colours.indexOf(t) % 9) + 1) }} />;
  if (kinds) {
    const groups = toTreeGroups(read.groups);
    const hidden = rtSide(groups, kinds, SIDE_CAP, null).hidden;
    return (
      <>
        <p className="rt-back"><button type="button" className="linkbtn" onClick={() => onKinds(null)}>‹ {trunc(label, 40)}</button></p>
        <h2 className="plain">{plural(hidden.length, "more kind")}</h2>
        <p className="sub">{SIDE_TITLE[kinds]}, too small to draw beside the first {SIDE_CAP - 1}</p>
        <table className="rt-groups"><tbody>
          {hidden.map((g) => {
            const ng = read.groups.find((x) => keyOf(toTreeGroup(x)) === keyOf(g)) as NeighbourGroup;
            return (
              <tr key={keyOf(g)}>
                <td>
                  <button type="button" className="rt-row" onClick={() => nav.setGroup(ng)}>
                    {dot(g.t)}<code>{g.t}</code><span className="rt-rel">{g.e}</span>
                  </button>
                </td>
                <td className="n">{countText(g)}</td>
              </tr>
            );
          })}
        </tbody></table>
      </>
    );
  }
  if (selection.sel) {
    const g = selection.sel.group;
    const fold = foldOf(g.items, selection.sel.id) as Leaf;
    const item = g.items.find((i) => i.id === selection.sel?.id) ?? fold.it;
    const name = itemName(item);
    const present = g.type !== null;
    const tg = toTreeGroup(g);
    return (
      <>
        <p className="rt-back">
          <button type="button" className="linkbtn" onClick={() => nav.setSel(g, null)}>
            ‹ {selection.group ? `${g.type ?? ABSENT_TYPE} ${g.relationship}` : trunc(label, 40)}
          </button>
        </p>
        <h2 className={`rt-h2${prose(name) ? " prose" : ""}`} data-testid="graph-rail-title"><Breakable text={name} /></h2>
        <p className="sub">{dot(tg.t)}<TypeLink type={g.type} link={link} />{fold.k > 1 ? ` · ${fold.k} listed records share this name` : ""}</p>
        <div className="rt-ids">
          {fold.ids.map((id) => (
            <div key={id} className="rt-idrow" data-current={id === item.id ? "true" : undefined}>
              <CopyableId text={id} what="record id" />
            </div>
          ))}
        </div>
        <h3>{SIDE_TITLE[g.direction]}</h3>
        <p><Sentence g={g} link={link} /></p>
        <table className="rt-kv"><tbody>
          <tr><td>Made by</td><td className="n"><Producer id={item.producer} producers={producers} /></td></tr>
        </tbody></table>
        {present ? (
          <p className="rt-actions">
            <a href={graphRecordHref(link, item.id)} data-testid="graph-rail-open-in-graph">Open in graph</a>
            <a href={exploreRecordHref(link, item.id)}>Open in Explore</a>
          </p>
        ) : <p className="sub">This record is not in this version, so it has no page of its own.</p>}
      </>
    );
  }
  if (selection.group) {
    const g = selection.group;
    const tg = toTreeGroup(g);
    const folds = foldNames(g.items);
    const makers = [...new Set(g.items.map((i) => i.producer))];
    return (
      <>
        <p className="rt-back"><button type="button" className="linkbtn" onClick={() => nav.setGroup(null)}>‹ {trunc(label, 40)}</button></p>
        <h2 className="rt-h2">{dot(tg.t)}{g.type ?? ABSENT_TYPE}</h2>
        <p className="sub"><Sentence g={g} link={link} /></p>
        <div className="big" style={{ marginTop: 12 }}>{countText(tg)}</div>
        <div className="sub">{g.count === 1 && !g.countIsFloor ? "record" : "records"}{floorNote(read, g)}</div>
        <h3>{g.count > g.items.length || g.countIsFloor ? `First ${fmt(g.items.length)} of ${countText(tg)}, by id` : `All ${fmt(g.count)}, by id`}</h3>
        <ul className="rt-items" data-testid="graph-rail-items">
          {folds.map((f) => (
            <li key={f.it.id}>
              <button type="button" className="linkbtn" onClick={() => nav.setSel(g, f.it.id)}>{itemName(f.it)}</button>
              {f.k > 1 ? <span className="rt-x"> ×{fmt(f.k)}</span> : null}
              {f.ids.map((id) => <div key={id} className="rt-idrow"><CopyableId text={id} what="record id" reveal="row" /></div>)}
            </li>
          ))}
        </ul>
        <p className="sub">Made by {makers.map((p, i) => <span key={p}>{i ? ", " : ""}<Producer id={p} producers={producers} /></span>)}</p>
        {g.count > g.items.length || g.countIsFloor ? (
          <p className="rt-actions"><a href={relatedHref(link, read.record.id, g)} data-testid="graph-rail-explore-group">{exploreLinkText(read, tg)}</a></p>
        ) : null}
      </>
    );
  }
  return <RecordIdentity read={read} link={link} producers={producers} />;
}

function RecordIdentity({ read, link, producers }: {
  read: RecordNeighbourhoodRead; link: LinkFn; producers: ProducerNamer;
}): ReactElement {
  const label = read.record.label || read.record.id;
  return (
    <div data-testid="graph-record-identity">
      <h2 className={`rt-h2${prose(label) ? " prose" : ""}`}><Breakable text={label} /></h2>
      <p className="sub">
        <TypeLink type={read.record.type} link={link} />
        {read.record.source ? <>{" · from "}<Producer id={read.record.source} producers={producers} /></> : null}
      </p>
      <div className="rt-ids"><CopyableId text={read.record.id} what="record id" /></div>
      <p className="rt-actions"><a href={exploreRecordHref(link, read.record.id)} data-testid="graph-open-in-explore">Open in Explore</a></p>
    </div>
  );
}

/** At 820 px and less: the same groups as the tree, as two lists. A row
 *  opens to the records listed for that group. */
function RecordLists({ read, params, link, setParams, onOpen, producers }: RecordViewProps): ReactElement {
  const selection = recordSelection(read, params);
  const nav = useNav(read, setParams, onOpen, selection.group);
  const escapeRef = useRef<() => boolean>(() => false);
  escapeRef.current = () => { if (selection.group) { nav.setGroup(null); return true; } return false; };
  useEscape(() => escapeRef.current());
  const colours = kindList(read);
  const tcol = (t: string) => cat((colours.indexOf(t) % 9) + 1);
  const rows = (g: NeighbourGroup) => {
    const tg = toTreeGroup(g);
    const k = groupParam(g);
    const isOpen = selection.group !== null && sameGroup(selection.group, g);
    const go = () => nav.setGroup(isOpen ? null : g);
    const folds = foldNames(g.items);
    return [
      <tr
        key={k} className={`row rt-trow${isOpen ? " open" : ""}`} tabIndex={0} aria-expanded={isOpen}
        onClick={(event) => { if (!(event.target as HTMLElement).closest("a, button")) go(); }}
        onKeyDown={(event) => {
          if (event.target !== event.currentTarget) return;
          if (event.key === "Enter" || event.key === " ") { event.preventDefault(); go(); }
          if (event.key === "ArrowDown" || event.key === "ArrowUp") {
            event.preventDefault();
            const all = [...(event.currentTarget.closest(".rt-narrow")?.querySelectorAll<HTMLElement>("tr.rt-trow") ?? [])];
            all[Math.max(0, Math.min(all.length - 1, all.indexOf(event.currentTarget) + (event.key === "ArrowDown" ? 1 : -1)))]?.focus();
          }
        }}
      >
        <td className="l"><span className="dot rt-dot" style={{ background: tcol(tg.t) }} /><code>{tg.t}</code><div className="sub">{g.relationship}</div></td>
        <td>{countText(tg)}</td>
      </tr>,
      isOpen ? (
        <tr key={`${k}|detail`} className="detail"><td colSpan={2}>
          <ul className="rt-list">
            {folds.map((f) => (
              <li key={f.it.id} data-current={selection.sel && f.ids.includes(selection.sel.id) ? "true" : undefined}>
                {g.type ? <a href={graphRecordHref(link, f.it.id)}>{itemName(f.it)}</a> : <span>{itemName(f.it)}</span>}
                {f.k > 1 ? <span className="rt-x"> ×{fmt(f.k)}</span> : null}
                {f.ids.map((id) => <div key={id} className="rt-idrow"><CopyableId text={id} what="record id" /></div>)}
              </li>
            ))}
          </ul>
          <p className="sub">
            {g.count > g.items.length || g.countIsFloor ? `First ${fmt(g.items.length)} of ${countText(tg)}, by id` : `All ${fmt(g.count)}, by id`}
            {floorNote(read, g)}
            {" · made by "}{[...new Set(g.items.map((i) => i.producer))].map((p, i) => <span key={p}>{i ? ", " : ""}<Producer id={p} producers={producers} /></span>)}
          </p>
          {g.count > g.items.length || g.countIsFloor ? <p><a href={relatedHref(link, read.record.id, g)}>{exploreLinkText(read, tg)}</a></p> : null}
        </td></tr>
      ) : null,
    ];
  };
  const side = (dir: "in" | "out") => {
    const gs = read.groups.filter((g) => g.direction === dir).sort((a, b) => b.count - a.count);
    if (!gs.length) return null;
    return (
      <>
        <h3 className="rt-th">{SIDE_TITLE[dir]} <span>{countText(sideTotal(read, dir))}</span></h3>
        <table className="tt rt-tt"><tbody>{gs.flatMap(rows)}</tbody></table>
      </>
    );
  };
  return (
    <div className="work single">
      <div className="tablewrap rt-narrow" data-testid="graph-record-lists">
        <RecordIdentity read={read} link={link} producers={producers} />
        {read.groups.length ? (
          <>
            <p className="tnote">{basisText(read)}.</p>
            {capClauses(read).length ? <p className="tnote" data-testid="graph-record-floor">“+” marks a count that is a floor: {capClauses(read).join("; ")}.</p> : null}
            {limitNotes(read).map((n) => <p key={n} className="tnote" data-testid="graph-record-limit">{n}.</p>)}
            {side("in")}
            {side("out")}
          </>
        ) : <EmptyNeighbourhood read={read} className="tnote" />}
      </div>
    </div>
  );
}

// The type map: every record type as a circle, coloured by the producer that
// makes most of its records, with a side panel that answers for the
// deployment at rest and for one type when it is picked. Ported from the
// approved prototype (v2.html, drawTypes and railTypes).
//
// The URL is the state (model/graph/typeMapModel.ts, through the page's
// params module): the selected type, the producers switched off and, above
// 40 types, the producers opened from one circle into their types. Every
// change here goes through `setParams` and REPLACES the history entry;
// leaving for a record or another view pushes one, so Back returns to the
// same selection. Producer names are the model's (the producers read).
//
// Keyboard: the drawing is one tab stop; arrows move between nodes, Enter
// selects (or opens a producer), + and − zoom, 0 fits, typing a type's name
// jumps to it, and Escape anywhere on the page clears the selection. The side
// panel's type list works the same way (arrows, typing, Enter).
import { type KeyboardEvent as ReactKeyboardEvent, type ReactElement, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { GRAPH_ONTOLOGY_ROUTE } from "@operator-ui/console/api/graph";
import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import { CopyableId } from "@operator-ui/console/components/CopyControls";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";
import { settledFailure, settledSentence } from "@operator-ui/console/model/graph/failures";
import { ReadingLine } from "@operator-ui/console/components/readRecovery";
import { countBasisSentence, fmt, plural, short, type GraphModel, type Pattern } from "@operator-ui/console/model/graph/derive";
import { type SetParams, exploreTypeHref } from "@operator-ui/console/model/graph/params";
import type { OntologyRead } from "@operator-ui/console/model/graph/reads";
import { moduleLabel } from "@operator-ui/console/model/graph/table";
import { type LabelText, shortProducerName } from "@operator-ui/console/model/graph/typeMapLabels";
import type { TypeMapLayout } from "@operator-ui/console/model/graph/typeMapLayout";
import {
  type LegendEntry, type MapState, legendEntries, mapOwners, mapPatch, otherProducers, producerPhrase, readMapState, typeAhead,
  typeRelations,
} from "@operator-ui/console/model/graph/typeMapModel";
import { type ProducerCoverage, coverageSentence } from "@operator-ui/console/model/graph/table";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { type TypeMapHandle, createTypeMap } from "@operator-ui/console/views/graph/typeMapDraw";

export function ZoomButtons({ handle, innerRef }: {
  handle: { current: { zoomBy: (f: number) => void; fit: (a: boolean) => void } | null };
  innerRef?: { current: HTMLDivElement | null };
}): ReactElement {
  return (
    <div className="zoom" ref={innerRef}>
      <button type="button" aria-label="Zoom in" title="Zoom in (+)" onClick={() => handle.current?.zoomBy(1.3)}>+</button>
      <button type="button" aria-label="Zoom out" title="Zoom out (−)" onClick={() => handle.current?.zoomBy(1 / 1.3)}>−</button>
      <button type="button" aria-label="Fit to screen" title="Fit to screen (0)" onClick={() => handle.current?.fit(true)}>⤢</button>
    </div>
  );
}

/** Refit the drawing when the window changes size, as the prototype did. */
export function useRefitOnResize(handle: { current: { fit: (a: boolean) => void } | null }): void {
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const onResize = () => { clearTimeout(timer); timer = setTimeout(() => handle.current?.fit(false), 150); };
    window.addEventListener("resize", onResize);
    return () => { clearTimeout(timer); window.removeEventListener("resize", onResize); };
  }, [handle]);
}

function useNarrow(): boolean {
  const query = "(max-width: 820px)";
  const [narrow, setNarrow] = useState(() => typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(query).matches);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return undefined;
    const m = window.matchMedia(query);
    const on = () => setNarrow(m.matches);
    m.addEventListener?.("change", on);
    return () => m.removeEventListener?.("change", on);
  }, []);
  return narrow;
}

const TYPE_AHEAD_MS = 800;

export function TypeMapView({
  model, ontology, ontologyFailure, params, link, setParams, producersPending = false, producersFailure = null, coverage = null,
}: {
  model: GraphModel;
  ontology: OntologyRead | null;
  ontologyFailure: unknown;
  params: Record<string, string>;
  link: LinkFn;
  /** The page's URL writer (a selection or toggle replaces the entry). */
  setParams: SetParams;
  /** The producers read is pending: names are ownership ids until then. */
  producersPending?: boolean;
  producersFailure?: unknown;
  /** The producers read's totals against this version's producers. */
  coverage?: ProducerCoverage | null;
}): ReactElement {
  const svgRef = useRef<SVGSVGElement>(null);
  const stageRef = useRef<HTMLDivElement>(null);
  const legendRef = useRef<HTMLDivElement>(null);
  const zoomRef = useRef<HTMLDivElement>(null);
  const railRef = useRef<HTMLElement>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const handle = useRef<TypeMapHandle | null>(null);
  const narrow = useNarrow();
  const [legendOpen, setLegendOpen] = useState(false);
  const [layout, setLayout] = useState<TypeMapLayout | null>(null);
  const [placing, setPlacing] = useState(false);
  const drawnRead = useRef<GraphModel["recordsBySource"] | null>(null);
  const [typed, setTyped] = useState<{ text: string; miss: boolean } | null>(null);
  const typedTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  // Where focus goes after the next draw or render.
  const focusAfter = useRef<{ map?: string; heading?: boolean; fromMap?: boolean } | null>(null);

  const state: MapState = useMemo(() => readMapState(params, model), [params, model]);
  const go = useCallback((next: Parameters<typeof mapPatch>[0]) => setParams(mapPatch(next)), [setParams]);
  // Until the producers read names them, a producer is its ownership id:
  // printed as an id (monospace, as stored), never as an upper-case caption.
  const idsOnly = producersPending && !producersFailure;
  const text: LabelText = useMemo(() => ({
    producer: model.name, shortProducer: (s: string) => shortProducerName(model.name(s), model.deployment), ids: idsOnly,
  }), [model, idsOnly]);
  // The drawing is laid out once per read: new producer names (the model is
  // rebuilt when they arrive) only relabel it.
  const readKey = model.recordsBySource;
  const expandKey = state.expand ? [...state.expand].sort().join(",") : null;
  const hideKey = [...state.hide].sort().join(",");
  const owners = useMemo(() => mapOwners(model), [model]);
  const allHidden = owners.length > 0 && owners.every((s) => state.hide.has(s));

  // Latest values the drawing's callbacks read, so it is not rebuilt for them.
  const live = useRef({ state, go, text, model });
  live.current = { state, go, text, model };

  useEffect(() => {
    if (!svgRef.current || !stageRef.current) return undefined;
    // A new set of opened producers keeps the view and replaces the drawing
    // once the new one is laid out; a new read starts afresh.
    const previous = handle.current;
    const sameRead = drawnRead.current === readKey;
    if (!sameRead) previous?.destroy();
    drawnRead.current = readKey;
    const { state: s, text: t, model } = live.current;
    setPlacing(true);
    handle.current = createTypeMap({
      svg: svgRef.current, stage: stageRef.current, legend: legendRef.current, zoomButtons: zoomRef.current,
      model, expand: s.expand, hidden: s.hide, selected: s.type, text: t, replacing: sameRead ? previous : null,
      onSelect: (type) => {
        focusAfter.current = { fromMap: true };
        live.current.go({ type });
      },
      onExpand: (source, keyboard) => {
        const cur = live.current.state;
        focusAfter.current = keyboard ? { map: model.types.find((x) => model.typeSource(x) === source) } : null;
        live.current.go({ expand: [...(cur.expand ?? []), source] });
      },
      onLayout: (l) => { setLayout(l); setPlacing(false); },
    });
    const f = focusAfter.current;
    if (f?.map) { handle.current.focusNode(f.map); focusAfter.current = null; }
    return undefined;
  }, [readKey, expandKey]);
  useEffect(() => () => { handle.current?.destroy(); handle.current = null; }, []);
  useEffect(() => {
    handle.current?.setSelection(state.type);
    const f = focusAfter.current;
    if (f?.map) { handle.current?.focusNode(f.map); focusAfter.current = null; } else if (f?.heading) {
      headingRef.current?.focus(); focusAfter.current = null;
    } else if (f?.fromMap) {
      // On a phone the panel is under the map: bring the answer into view.
      if (narrow && state.type) railRef.current?.scrollIntoView({ block: "start", behavior: "smooth" });
      focusAfter.current = null;
    }
  }, [state.type, narrow]);
  useEffect(() => { handle.current?.setHidden(state.hide); }, [hideKey]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => { handle.current?.setText(text); }, [text]);
  useEffect(() => { handle.current?.setText(live.current.text); }, [legendOpen, narrow]);
  useEffect(() => () => clearTimeout(typedTimer.current), []);
  useRefitOnResize(handle);

  const selectType = (type: string | null, focus: "heading" | "map" | null = "heading") => {
    focusAfter.current = focus === "heading" ? { heading: true } : focus === "map" && type ? { map: type } : null;
    // A type inside a folded producer opens it, on the URL, so clearing the
    // selection later does not fold the map back under the reader.
    if (type && state.expand) go({ type, expand: new Set([...state.expand, model.typeSource(type)]) });
    else go({ type });
  };
  const toggle = (entry: LegendEntry) => {
    const next = new Set(state.hide);
    const off = entry.members.every((s) => next.has(s));
    for (const s of entry.members) { if (off) next.delete(s); else next.add(s); }
    go({ hide: next });
  };
  const onStageKey = (ev: ReactKeyboardEvent<HTMLDivElement>) => {
    if (ev.ctrlKey || ev.metaKey || ev.altKey) return;
    if (!(ev.target instanceof Element) || !svgRef.current?.contains(ev.target)) return;
    if (ev.key === "+" || ev.key === "=") { ev.preventDefault(); handle.current?.zoomBy(1.3); return; }
    if (ev.key === "-") { ev.preventDefault(); handle.current?.zoomBy(1 / 1.3); return; }
    if (ev.key === "0") { ev.preventDefault(); handle.current?.fit(true); return; }
    if (ev.key.length !== 1 || ev.key === " ") return;
    ev.preventDefault();
    const buffer = `${typed && !typed.miss ? typed.text : ""}${ev.key}`;
    const shownTypes = model.types.filter((t) => !state.hide.has(model.typeSource(t)));
    const hit = typeAhead(shownTypes, buffer);
    setTyped({ text: buffer, miss: !hit });
    clearTimeout(typedTimer.current);
    typedTimer.current = setTimeout(() => setTyped(null), TYPE_AHEAD_MS * 2);
    if (hit) selectType(hit, "map");
  };
  const onKeyDown = (ev: ReactKeyboardEvent<HTMLDivElement>) => {
    if (ev.key !== "Escape" || !state.type) return;
    ev.preventDefault();
    const was = state.type;
    const inRail = railRef.current?.contains(document.activeElement) ?? false;
    selectType(null, null);
    if (inRail) focusAfter.current = { map: was };
  };

  const hiddenCount = state.hide.size;
  const collapsedMode = state.expand !== null;
  let status = "";
  if (layout) {
    const kinds = plural(model.patterns.length, "relationship kind");
    status = collapsedMode
      ? `${plural(model.types.length, "type")} from ${plural(owners.length, "producer")}, one circle per producer`
        + ` (${state.expand?.size ? `${state.expand.size} opened` : "click one to show its types"}) · ${kinds}`
      : `${plural(model.types.length, "type")} · ${kinds}`;
    if (layout.isolated.length) status += ` · ${plural(layout.isolated.length, "circle")} with no relationships, in the strip below the map`;
    if (hiddenCount) status += ` · ${plural(hiddenCount, "producer")} hidden`;
  }
  if (placing) status = `Placing the circles for ${plural(model.types.length, "type")}…`;
  if (typed) status = typed.miss ? `No shown type's name contains “${typed.text}”.` : `Typed “${typed.text}”.`;
  const entries = legendEntries(model);
  const legendBodyId = "graph-legend-body";
  return (
    <div className="work" onKeyDown={onKeyDown}>
      <div className="stagecol">
        <div className="stage" ref={stageRef} data-testid="graph-stage" onKeyDown={onStageKey}>
          <p id="graph-map-help" className="okg-console__sr">
            Arrow keys move between circles; Enter selects a type or opens a producer; plus and minus zoom; 0 fits; typing a name jumps to that type; Escape clears the selection.
          </p>
          <svg ref={svgRef} role="group" aria-label="Map of record types" aria-describedby="graph-map-help" />
          {allHidden ? (
            <p className="map-empty" data-testid="graph-map-all-hidden">
              Every producer is switched off in the legend.{" "}
              <button type="button" className="linkbtn" onClick={() => go({ hide: [] })}>Show all</button>
            </p>
          ) : null}
          <div className={`legend${legendOpen ? " open" : ""}`} ref={legendRef}>
            {narrow ? (
              <button type="button" className="lhead" aria-expanded={legendOpen} aria-controls={legendBodyId} onClick={() => setLegendOpen((o) => !o)}>
                Coloured by producer
              </button>
            ) : <h4>Coloured by producer</h4>}
            <div id={legendBodyId} hidden={narrow && !legendOpen}>
              {entries.map((e) => {
                const off = e.members.every((s) => state.hide.has(s));
                const label = e.key === "smaller" ? `${e.members.length} smaller producers` : model.name(e.key);
                return (
                  <button
                    type="button" key={e.key} className={`lentry${off ? " off" : ""}`} aria-pressed={!off}
                    title={e.key === "smaller" ? e.members.map((s) => model.name(s)).join(", ") : `${off ? "Show" : "Hide"} the types ${label} colours`}
                    onClick={() => toggle(e)}
                  >
                    <span className="dot" style={{ background: e.color }} />
                    <span className={`lname${idsOnly && e.key !== "smaller" ? " lid" : ""}`}>{label}</span>
                    <span className="lcount">{short(e.records)}</span>
                  </button>
                );
              })}
              <div className="lnote">
                A type takes the colour of the producer that made most of its records. Circle size grows with records; line width with the relationships between two types.
                {collapsedMode ? " A circle with a ring inside is a producer: click it to show its types." : ""}
              </div>
            </div>
          </div>
          <ZoomButtons handle={handle} innerRef={zoomRef} />
        </div>
        <div className="status" data-testid="graph-status" aria-live="polite">{status}</div>
      </div>
      <aside className="rail" ref={railRef} data-testid="graph-rail">
        <p className="okg-console__sr" aria-live="polite">{state.type ? `${state.type} selected` : "No type selected"}</p>
        {state.type ? (
          <TypeSelected
            model={model} ontology={ontology} type={state.type} link={link}
            hidden={state.hide.has(model.typeSource(state.type))} headingRef={headingRef}
            onType={(t) => selectType(t)}
            onShow={() => go({ hide: [...state.hide].filter((s) => s !== model.typeSource(state.type as string)) })}
            onFold={state.expand ? () => {
              const src = model.typeSource(state.type as string);
              focusAfter.current = null;
              go({ type: null, expand: [...(state.expand ?? [])].filter((s) => s !== src) });
            } : null}
          />
        ) : (
          <AtRest
            model={model} ontology={ontology} ontologyFailure={ontologyFailure} producersPending={producersPending}
            producersFailure={producersFailure} coverage={coverage} link={link} state={state}
            onType={(t) => selectType(t)} onFoldAll={() => go({ expand: [] })}
          />
        )}
      </aside>
    </div>
  );
}

/** A side-panel read that failed: a failure reading again cannot change
 *  (nothing published, a refusal) is the page's one plain line, never the
 *  generic 404 step ("check the id"), which the map does not take. */
function RailFailure({ route, error, what, deployment }: { route: string; error: unknown; what: string; deployment: string }): ReactElement {
  const settled = settledFailure(error, null);
  if (settled) {
    return <p className={`sub graph-settled graph-settled--${settled.kind}`} role="status">{settledSentence(settled, deployment, what)}</p>;
  }
  return <ReadFailure route={route} error={error} what={what} />;
}

function AtRest({
  model, ontology, ontologyFailure, producersPending, producersFailure, coverage, link, state, onType, onFoldAll,
}: {
  model: GraphModel;
  ontology: OntologyRead | null;
  ontologyFailure: unknown;
  producersPending: boolean;
  producersFailure: unknown;
  coverage: ProducerCoverage | null;
  link: LinkFn;
  state: MapState;
  onType: (t: string) => void;
  onFoldAll: () => void;
}): ReactElement {
  const typeBtn = (t: string) => <button type="button" className="linkbtn mono" onClick={() => onType(t)}>{t}</button>;
  const shownRels = model.patterns.slice(0, 7);
  return (
    <>
      <h2 className="tr-h plain">{model.deployment}</h2>
      <table className="tr-kv"><tbody>
        <tr><td>Records</td><td className="n">{fmt(model.totals.records)}</td></tr>
        <tr><td>Relationships</td><td className="n">{fmt(model.totals.relationships)}</td></tr>
      </tbody></table>
      <p className="sub">{countBasisSentence(model.countBasis)}</p>
      <table className="tr-kv"><tbody>
        <tr>
          <td>Record types holding records</td>
          <td className="n" data-testid="graph-declared">
            {ontology ? `${fmt(model.types.length)} of ${fmt(ontology.types.length)} declared`
              : ontologyFailure ? `${fmt(model.types.length)} · declared not read` : `${fmt(model.types.length)} · reading the declared types`}
          </td>
        </tr>
      </tbody></table>
      <p className="sub" data-testid="graph-coverage">
        {coverageSentence(model, coverage)} <a href={link("sources")}>Sources</a> lists every producer.
      </p>
      {producersPending && !producersFailure ? <ReadingLine what="the producers' names" className="sub" /> : null}
      {ontologyFailure ? <RailFailure route={GRAPH_ONTOLOGY_ROUTE} error={ontologyFailure} what="The declared record types" deployment={model.deployment} /> : null}
      {producersFailure ? <RailFailure route={PRODUCERS_ROUTE} error={producersFailure} what="The producers' names" deployment={model.deployment} /> : null}
      {state.expand && state.expand.size ? (
        <p className="sub">
          {plural(state.expand.size, "producer")} opened into {state.expand.size === 1 ? "its" : "their"} types.{" "}
          <button type="button" className="linkbtn" onClick={onFoldAll}>Fold all</button>
        </p>
      ) : null}
      <h3>Biggest relationships <span className="cnt">{shownRels.length} of {fmt(model.patterns.length)}</span></h3>
      <table className="tr-rel"><tbody>
        {shownRels.map((p) => (
          <tr key={`${p.s}|${p.e}|${p.d}`}>
            <td>{typeBtn(p.s)} <span className="sub">{p.e}</span> {p.s === p.d ? <span className="sub">itself</span> : typeBtn(p.d)}</td>
            <td className="n">{short(p.n)}</td>
          </tr>
        ))}
      </tbody></table>
      <TypeList model={model} hidden={state.hide} onType={onType} />
    </>
  );
}

/** Every type holding records, largest first: one tab stop, arrows and
 *  typing move within it, Enter selects. */
function TypeList({ model, hidden, onType }: { model: GraphModel; hidden: ReadonlySet<string>; onType: (t: string) => void }): ReactElement {
  const [active, setActive] = useState(0);
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const typed = useRef<{ text: string; at: number }>({ text: "", at: 0 });
  const move = (i: number) => {
    const next = Math.max(0, Math.min(model.types.length - 1, i));
    setActive(next);
    refs.current[next]?.focus();
  };
  const onKey = (ev: ReactKeyboardEvent<HTMLUListElement>) => {
    if (ev.key === "ArrowDown") { ev.preventDefault(); move(active + 1); } else if (ev.key === "ArrowUp") { ev.preventDefault(); move(active - 1); } else if (ev.key === "Home") { ev.preventDefault(); move(0); } else if (ev.key === "End") { ev.preventDefault(); move(model.types.length - 1); } else if (ev.key.length === 1 && ev.key !== " " && !ev.ctrlKey && !ev.metaKey && !ev.altKey) {
      const now = Date.now();
      const text = (now - typed.current.at < TYPE_AHEAD_MS ? typed.current.text : "") + ev.key;
      typed.current = { text, at: now };
      const hit = typeAhead(model.types, text);
      if (hit) move(model.types.indexOf(hit));
    }
  };
  return (
    <>
      <h3 id="graph-type-list">Record types <span className="cnt">{fmt(model.types.length)}</span></h3>
      <ul className="tr-types" aria-labelledby="graph-type-list" onKeyDown={onKey}>
        {model.types.map((t, i) => (
          <li key={t}>
            <button
              type="button" ref={(el) => { refs.current[i] = el; }} tabIndex={i === active ? 0 : -1}
              onFocus={() => setActive(i)} onClick={() => onType(t)}
            >
              <span className="dot" style={{ background: model.typeColor(t) }} />
              <span className="mono tname">{t}</span>
              {hidden.has(model.typeSource(t)) ? <span className="sub">hidden</span> : null}
              <span className="n">{short(model.count[t])}</span>
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}

function TypeSelected({
  model, ontology, type, link, hidden, headingRef, onType, onShow, onFold,
}: {
  model: GraphModel;
  ontology: OntologyRead | null;
  type: string;
  link: LinkFn;
  hidden: boolean;
  headingRef: { current: HTMLHeadingElement | null };
  onType: (t: string) => void;
  onShow: () => void;
  onFold: (() => void) | null;
}): ReactElement {
  const declared = ontology?.types.find((t) => t.type === type) ?? null;
  const typeBtn = (t: string) => <button type="button" className="linkbtn mono" onClick={() => onType(t)}>{t}</button>;
  const by = (p: Pattern) => {
    const other = otherProducers(model, p);
    return other.length ? `by ${other.map((x) => producerPhrase(model.label(x))).join(", ")}` : "";
  };
  const rel = typeRelations(model, type);
  const owner = model.typeSource(type);
  const n = model.count[type] ?? 0;
  const sec = (title: string, ps: Pattern[], other: ((p: Pattern) => string) | null) => (ps.length ? (
    <>
      <h3>{title} <span className="cnt">{ps.length}</span></h3>
      <table className="tr-rel"><tbody>
        {ps.map((p) => (
          <tr key={`${p.s}|${p.e}|${p.d}`}>
            <td><span className="rel">{p.e}</span> {other ? typeBtn(other(p)) : null}{by(p) ? <div className="sub">{by(p)}</div> : null}</td>
            <td className="n">{fmt(p.n)}</td>
          </tr>
        ))}
      </tbody></table>
    </>
  ) : null);
  const producerCell = (s: string) => {
    const { name, href } = model.label(s);
    return href ? <a href={href}>{name}</a> : <span className="mono">{name}</span>;
  };
  return (
    <>
      <h2 className="tr-h" tabIndex={-1} ref={headingRef}>
        <span className="dot" style={{ background: model.typeColor(type) }} />
        <CopyableId text={type} what="record type" />
      </h2>
      <div className="tr-big">
        <span className="big">{fmt(n)}</span>
        <span className="sub">records{declared?.module ? ` · ${moduleLabel(declared.module)} ontology` : ""}</span>
      </div>
      {hidden ? (
        <p className="map-hidden" data-testid="graph-selected-hidden">
          Hidden on the map: {model.name(owner)} is switched off in the legend.{" "}
          <button type="button" className="linkbtn" onClick={onShow}>Show it</button>
        </p>
      ) : null}
      <p className="tr-open">
        <a href={exploreTypeHref(link, type)} data-testid="graph-open-in-explore">Open the {fmt(n)} {type} records in Explore</a>
      </p>
      <h3>Made by</h3>
      <table className="tr-kv"><tbody>
        {(model.byType[type] ?? []).map(([s, count]) => (
          <tr key={s}>
            <td><span className="dot" style={{ background: model.srcColor[s], marginRight: 7 }} />{producerCell(s)}</td>
            <td className="n">{fmt(count)}</td>
          </tr>
        ))}
      </tbody></table>
      {sec("Links to", rel.out, (p) => p.d)}
      {sec("Links to itself", rel.self, null)}
      {sec("Linked from", rel.inn, (p) => p.s)}
      {!rel.out.length && !rel.self.length && !rel.inn.length ? <p className="sub">No relationship starts or ends at a {type} record in this version.</p> : null}
      {onFold ? (
        <p className="sub tr-fold">
          <button type="button" className="linkbtn" onClick={onFold}>
            Fold {model.name(owner)}&apos;s {model.types.filter((t) => model.typeSource(t) === owner).length} types into one circle
          </button>
        </p>
      ) : null}
    </>
  );
}

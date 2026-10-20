// The Grid: which types link to which, and which sources link to which. A
// cell opens a small panel beside it saying what it counts. Ported from the
// approved prototype (v2.html, gridTable and drawGrid).
//
// The prototype's middle grid, "which sources make which types", is cut: it
// was the third copy of one fact. The Table's "Made by" column says which
// source makes each type, with the count, and its Sources table says which
// types each source makes; a 25 x 9 matrix of mostly empty cells added
// nothing a reader could not read there in a line.
//
// The second grid has its own read (console/graph/source-links), computed on
// request; when it fails -- a 503 `source_links_unavailable` after its
// statement timeout -- the failure is said IN PLACE of that grid, and the
// grid above it still draws.
//
// Each grid is ONE scroll box, at most the height of the stage, with its
// header row, first column and Total column pinned: it used to be a 600 px
// box inside a scrolling page, so the wheel was caught 69 px short of the
// end and the Total column sat off-screen at 1440.
//
// URL: `cell=<grid>:<row>|<col>` is the open cell (replacing the history
// entry). Keyboard: each grid is one tab stop; the arrow keys, Home and End
// move between cells, Enter or Space opens one, Escape closes it.
import { Fragment, type KeyboardEvent, type ReactElement, type ReactNode, useEffect, useLayoutEffect, useRef, useState } from "react";
import { GRAPH_SOURCE_LINKS_ROUTE } from "@operator-ui/console/api/graph";
import { FixtureReadMissing } from "@operator-ui/console/api/readFailures";
import { LoudFailure } from "@operator-ui/console/components/LoudFailure";
import { ReadingLine } from "@operator-ui/console/components/readRecovery";
import {
  type GraphModel, type SourceLinkModel, NO_SOURCE, fmt, producerText, short, versionCountsSentence,
} from "@operator-ui/console/model/graph/derive";
import {
  SOURCE_LINKS_FAILED_NEXT, SOURCE_LINKS_TIMEOUT_NEXT, settledFailure, settledSentence, sourceLinksBasisClause,
  sourceLinksTimeout, sourceLinksTimeoutSentence,
} from "@operator-ui/console/model/graph/failures";
import { type Matrix, heatBackground, sourceSourceMatrix, typeTypeMatrix } from "@operator-ui/console/model/graph/grid";
import { type GridCell, type SetParams, cellParam, parseCell } from "@operator-ui/console/model/graph/params";
import type { SourceLinksRead } from "@operator-ui/console/model/graph/reads";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import { ReadFailure } from "@operator-ui/console/components/ReadFailure";

interface GridOpts {
  id: GridCell["grid"];
  aria: string;
  corner: [string, string];
  colHead: (c: string) => ReactNode;
  rowHead: (r: string) => ReactNode;
  /** The header's full text, for its title. */
  headText: (x: string) => string;
  title: (r: string, c: string, n: number) => string;
  info: (r: string, c: string) => ReactNode;
}

/** The panel beside an open cell, placed in the window: below the cell, or
 *  above it where there is no room below, and kept inside the window. */
function CellPanel({ anchor, onClose, children, testId }: {
  anchor: HTMLElement; onClose: () => void; children: ReactNode; testId: string;
}): ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<{ left: number; top: number } | null>(null);
  useLayoutEffect(() => {
    let frame = 0;
    const place = () => {
      const panel = ref.current;
      if (!panel) return;
      const a = anchor.getBoundingClientRect();
      const w = panel.offsetWidth;
      const h = panel.offsetHeight;
      const below = a.bottom + 6;
      const top = below + h > window.innerHeight - 8 && a.top - h - 6 > 8 ? a.top - h - 6 : below;
      const left = Math.max(8, Math.min(a.left, window.innerWidth - w - 8));
      setPos({ left, top });
    };
    const schedule = () => { cancelAnimationFrame(frame); frame = requestAnimationFrame(place); };
    place();
    window.addEventListener("scroll", schedule, true);
    window.addEventListener("resize", schedule);
    return () => {
      cancelAnimationFrame(frame);
      window.removeEventListener("scroll", schedule, true);
      window.removeEventListener("resize", schedule);
    };
  }, [anchor]);
  useEffect(() => {
    const onDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (ref.current?.contains(target) || anchor.contains(target)) return;
      onClose();
    };
    document.addEventListener("pointerdown", onDown);
    return () => document.removeEventListener("pointerdown", onDown);
  }, [anchor, onClose]);
  return (
    <div
      ref={ref} className="cellinfo" role="dialog" aria-label="What this cell counts" data-testid={testId}
      style={pos ? { left: pos.left, top: pos.top } : { visibility: "hidden" }}
      onKeyDown={(event) => { if (event.key === "Escape") { event.preventDefault(); onClose(); } }}
    >
      {children}
    </div>
  );
}

function GridTable({ m, opts, open, onOpen }: {
  m: Matrix; opts: GridOpts; open: GridCell | null; onOpen: (cell: GridCell | null) => void;
}): ReactElement {
  const tableRef = useRef<HTMLTableElement>(null);
  const [focus, setFocus] = useState<[number, number]>(() => {
    if (open) {
      const r = m.rows.indexOf(open.row);
      const c = m.cols.indexOf(open.col);
      if (r >= 0 && c >= 0) return [r, c];
    }
    for (let i = 0; i < m.rows.length; i += 1) {
      const j = m.cells[i].findIndex((n) => n > 0);
      if (j >= 0) return [i, j];
    }
    return [0, 0];
  });
  const moveTo = useRef<[number, number] | null>(null);
  useEffect(() => {
    if (!moveTo.current) return;
    const [i, j] = moveTo.current;
    moveTo.current = null;
    tableRef.current?.querySelector<HTMLElement>(`td[data-i="${i}"][data-j="${j}"]`)?.focus();
  });
  const isOpen = (r: string, c: string) => Boolean(open && open.grid === opts.id && open.row === r && open.col === c);
  const onKey = (event: KeyboardEvent<HTMLTableCellElement>, i: number, j: number) => {
    const go = (a: number, b: number) => {
      event.preventDefault();
      const next: [number, number] = [Math.max(0, Math.min(m.rows.length - 1, a)), Math.max(0, Math.min(m.cols.length - 1, b))];
      setFocus(next);
      moveTo.current = next;
    };
    const n = m.cells[i][j];
    if ((event.key === "Enter" || event.key === " ") && n) {
      event.preventDefault();
      onOpen(isOpen(m.rows[i], m.cols[j]) ? null : { grid: opts.id, row: m.rows[i], col: m.cols[j] });
    } else if (event.key === "Escape" && open) { event.preventDefault(); onOpen(null); }
    else if (event.key === "ArrowRight") go(i, j + 1);
    else if (event.key === "ArrowLeft") go(i, j - 1);
    else if (event.key === "ArrowDown") go(i + 1, j);
    else if (event.key === "ArrowUp") go(i - 1, j);
    else if (event.key === "Home") go(event.ctrlKey ? 0 : i, 0);
    else if (event.key === "End") go(event.ctrlKey ? m.rows.length - 1 : i, m.cols.length - 1);
  };
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const openHere = open && open.grid === opts.id && m.rows.includes(open.row) && m.cols.includes(open.col) ? open : null;
  useLayoutEffect(() => {
    if (!openHere) { setAnchor(null); return; }
    const i = m.rows.indexOf(openHere.row);
    const j = m.cols.indexOf(openHere.col);
    const el = tableRef.current?.querySelector<HTMLElement>(`td[data-i="${i}"][data-j="${j}"]`) ?? null;
    if (el && !anchor) el.scrollIntoView?.({ block: "nearest", inline: "nearest" });
    setAnchor(el);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openHere?.row, openHere?.col, m]);
  const close = () => {
    const el = anchor;
    onOpen(null);
    el?.focus();
  };
  return (
    <>
      <div className="gscroll" data-testid={`graph-grid-${opts.id}`}>
        <table className="grid" data-grid={opts.id} aria-label={opts.aria} ref={tableRef}>
          <thead>
            <tr>
              <th className="corner" scope="col"><span>{opts.corner[1]}</span><span>{opts.corner[0]}</span></th>
              {m.cols.map((c) => <th key={c} className="colh" scope="col" title={opts.headText(c)}><div>{opts.colHead(c)}</div></th>)}
              <th className="colh tot" scope="col"><div>Total</div></th>
            </tr>
          </thead>
          <tbody>
            {m.rows.map((r, i) => (
              <tr key={r}>
                <th scope="row" className="rowh" title={opts.headText(r)}><span>{opts.rowHead(r)}</span></th>
                {m.cols.map((c, j) => {
                  const n = m.cells[i][j];
                  const tab = focus[0] === i && focus[1] === j ? 0 : -1;
                  const on = isOpen(r, c);
                  return (
                    <td
                      key={c} className={n ? undefined : "z"} style={n ? { background: heatBackground(n, m.max) } : undefined}
                      title={n ? opts.title(r, c, n) : undefined}
                      data-r={n ? r : undefined} data-c={n ? c : undefined} data-i={i} data-j={j}
                      tabIndex={tab} aria-label={n ? undefined : `${opts.headText(r)} to ${opts.headText(c)}: none`}
                      aria-haspopup={n ? "dialog" : undefined} aria-expanded={n ? on : undefined}
                      onClick={n ? () => { setFocus([i, j]); onOpen(on ? null : { grid: opts.id, row: r, col: c }); } : () => setFocus([i, j])}
                      onKeyDown={(event) => onKey(event, i, j)}
                    >
                      {n ? short(n) : ""}
                    </td>
                  );
                })}
                <td className="tot" title={fmt(m.rowTot[i])}>{short(m.rowTot[i])}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr>
              <th scope="row" className="rowh tot">Total</th>
              {m.colTot.map((n, j) => <td key={m.cols[j]} className="tot" title={n ? fmt(n) : undefined}>{n ? short(n) : ""}</td>)}
              <td className="tot all" title={fmt(m.grand)}>{short(m.grand)}</td>
            </tr>
          </tfoot>
        </table>
      </div>
      {openHere && anchor ? (
        <CellPanel anchor={anchor} onClose={close} testId={`graph-cellinfo-${opts.id}`}>
          {opts.info(openHere.row, openHere.col)}
        </CellPanel>
      ) : null}
    </>
  );
}

function Ty({ t }: { t: string }): ReactElement {
  return <code>{t}</code>;
}

export function GridView({
  model, params, link, setParams, links, linksRead, linksFailure,
}: {
  model: GraphModel;
  params: Record<string, string>;
  link: LinkFn;
  setParams: SetParams;
  links: SourceLinkModel | null;
  linksRead: SourceLinksRead | null;
  linksFailure: { error: unknown; readAgain: () => void; retrying: boolean } | null;
}): ReactElement {
  const tt = typeTypeMatrix(model);
  const cell = parseCell(params.cell);
  const onOpen = (next: GridCell | null) => setParams({ cell: next ? cellParam(next) : null });
  const label = (p: string) => (p === NO_SOURCE ? { name: NO_SOURCE, kind: null, href: null } : model.label(p));
  const text = (p: string) => producerText(label(p));
  const sc = (s: string) => <><span className="dot" style={{ background: model.srcColor[s] ?? "var(--okg-graph-cat-9)" }} />{label(s).name}</>;
  const producerLink = (p: string) => {
    const l = label(p);
    return l.href ? <a href={l.href}>{producerText(l)}</a> : <>{producerText(l)}</>;
  };
  const timeout = linksFailure ? sourceLinksTimeout(linksFailure.error) : null;
  const settled = linksFailure ? settledFailure(linksFailure.error, null) : null;
  const basis = linksRead ? sourceLinksBasisClause(linksRead.computedMs, linksRead.cached) : null;
  return (
    <div className="work single">
      <div className="tablewrap" data-testid="graph-grid">
        <section className="gsec">
          <h2 className="gh">Which types link to which</h2>
          <p className="tnote">
            Relationships from the row type to the column type; the diagonal is relationships within one type.
            {tt.unlinked.length ? (
              <> {tt.unlinked.map((t, i) => (
                <Fragment key={t}>{i ? (i === tt.unlinked.length - 1 ? " and " : ", ") : ""}<code>{t}</code></Fragment>
              ))} {tt.unlinked.length === 1 ? "has" : "have"} no relationships.</>
            ) : null}
          </p>
          <GridTable
            m={tt.matrix} open={cell} onOpen={onOpen}
            opts={{
              id: "tt", aria: "Relationships between record types", corner: ["from ↓", "to →"],
              colHead: (c) => <Ty t={c} />, rowHead: (r) => <Ty t={r} />, headText: (x) => x,
              title: (s, d) => (tt.cell[`${s}|${d}`] ?? []).map((p) => `${s} ${p.e} ${d}: ${fmt(p.n)} (by ${p.prod.map((x) => text(x[0])).join(", ")})`).join("\n"),
              info: (s, d) => (
                <>
                  <b><Ty t={s} /> → <Ty t={d} /></b>
                  {(tt.cell[`${s}|${d}`] ?? []).map((p) => (
                    <span className="ci" key={p.e}>
                      <code>{p.e}</code> {fmt(p.n)}{" "}
                      <span className="sub">by {p.prod.map((x, i) => <Fragment key={x[0]}>{i ? ", " : ""}{producerLink(x[0])}</Fragment>)}</span>
                    </span>
                  ))}
                  <span className="ci links">
                    <a href={link("explore", { type: s })}>{s} in Explore</a>
                    {d !== s ? <a href={link("explore", { type: d })}>{d} in Explore</a> : null}
                  </span>
                </>
              ),
            }}
          />
        </section>
        <section className="gsec" data-testid="graph-source-links">
          <h2 className="gh gh2">Which sources link to which</h2>
          <p className="tnote">
            Relationships by the source that made each end record; counts include relationships the enrichers added.
            {basis ? <> {basis}</> : null}
          </p>
          {links ? (
            <GridTable
              m={sourceSourceMatrix(links)} open={cell} onOpen={onOpen}
              opts={{
                id: "ss", aria: "Relationships between sources", corner: ["from ↓", "to →"],
                colHead: sc, rowHead: sc, headText: (x) => label(x).name,
                title: (a, b, n) => `${fmt(n)} relationships from ${label(a).name} records to ${label(b).name} records\nmade by ${(links.cells[`${a}|${b}`]?.prod ?? []).map(([p, x]) => `${text(p)} ${fmt(x)}`).join(", ")}`,
                info: (a, b) => (
                  <>
                    <b>{sc(a)} → {sc(b)}</b>
                    <span className="ci">
                      {fmt(links.cells[`${a}|${b}`]?.n ?? 0)} relationships{" "}
                      <span className="sub">by {(links.cells[`${a}|${b}`]?.prod ?? []).map(([p, x], i) => <Fragment key={p}>{i ? ", " : ""}{producerLink(p)} {fmt(x)}</Fragment>)}</span>
                    </span>
                  </>
                ),
              }}
            />
          ) : linksFailure ? (
            settled ? (
              <p className="okg-console__fact" data-testid={`graph-settled-${settled.kind}`}>{settledSentence(settled, null, "Which sources link to which")}</p>
            ) : (
              <>
                {timeout ? <p className="tnote" data-testid="graph-source-links-timeout">{sourceLinksTimeoutSentence(timeout)}</p> : null}
                {linksFailure.error instanceof FixtureReadMissing ? (
                  <LoudFailure error={linksFailure.error} route={GRAPH_SOURCE_LINKS_ROUTE} what="Which sources link to which" onReadAgain={linksFailure.readAgain} retrying={linksFailure.retrying} />
                ) : (
                  <ReadFailure
                    route={GRAPH_SOURCE_LINKS_ROUTE} error={linksFailure.error} what="Which sources link to which"
                    onReadAgain={linksFailure.readAgain} retrying={linksFailure.retrying}
                    nextCheck={timeout ? SOURCE_LINKS_TIMEOUT_NEXT : SOURCE_LINKS_FAILED_NEXT}
                  />
                )}
              </>
            )
          ) : (
            <ReadingLine what="the source links, which the server computes on request" testId="graph-source-links-reading" />
          )}
        </section>
        <p className="tnote tfoot" data-testid="graph-grid-footer">
          {versionCountsSentence(model)} Shade is the count on a log scale; hover a total for its exact value.
        </p>
      </div>
    </div>
  );
}

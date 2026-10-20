// The Table: record types (a row opens to the type's declared fields with
// their sampled fill, and the relationships the ontology allows), then the
// sources and the enrichers. Ported from the approved prototype (v2.html,
// drawTable and detail).
//
// URL: `open=<type>` is the open row and `empty=1` lists the declared types
// that hold no records; both replace the history entry. Keyboard: the rows
// are one tab stop; the arrow keys, Home and End move between them, Enter or
// Space opens one, Escape closes it.
import { Fragment, type KeyboardEvent, type ReactElement, type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { CopyableId } from "@operator-ui/console/components/CopyControls";
import { ReadingLine } from "@operator-ui/console/components/readRecovery";
import { type GraphModel, type ProducerLabel, fmt, versionCountsSentence } from "@operator-ui/console/model/graph/derive";
import type { OntologyRead } from "@operator-ui/console/model/graph/reads";
import {
  type ProducerCoverage, UNUSED_RULES_SHOWN, coverageSentence, emptyTypes, enricherPatterns, fillSampleSentence, linksIn,
  linksOut, linksWithin, moduleLabel, typeDetail, typesOf,
} from "@operator-ui/console/model/graph/table";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { SetParams } from "@operator-ui/console/model/graph/params";

const Dot = ({ color, ring = false }: { color?: string; ring?: boolean }) => (
  <span className={`dot${ring ? " ring" : ""}`} style={color ? { background: color } : undefined} />
);

/** A count as the Table prints it: "none" for zero, in every column and in
 *  the copy text (it used to be "none", "0" or blank by column). */
export const countCell = (n: number): string => (n ? fmt(n) : "none");

/** An identifier that may break after each underscore. */
function Breakable({ text }: { text: string }): ReactElement {
  const parts = text.split("_");
  return <code>{parts.map((p, i) => <Fragment key={i}>{i ? <>_<wbr /></> : null}{p}</Fragment>)}</code>;
}

/** A producer's name, linked to its Sources page when the producers read
 *  lists it, with its kind after it where it is not a source. */
export function ProducerName({ label }: { label: ProducerLabel }): ReactElement {
  const kind = label.kind === "enricher" || label.kind === "view"
    ? <> <span className="sub">{label.kind}</span></>
    : null;
  return label.href
    ? <><a className="pn" href={label.href}>{label.name}</a>{kind}</>
    : <><code className="pn">{label.name}</code>{kind}</>;
}

function Detail({
  type, ontology, model, link,
}: { type: string; ontology: OntologyRead; model: GraphModel; link: LinkFn }): ReactElement {
  const declared = useMemo(() => Object.fromEntries(ontology.types.map((t) => [t.type, t])), [ontology]);
  const d = typeDetail(type, ontology, declared);
  const [allUnused, setAllUnused] = useState(false);
  const pct = (v: number) => Math.round(v * 100);
  const bar = (v: number) => (
    <>
      <td className="fl"><span className="fill"><i style={{ width: `${pct(v)}%` }} /></span></td>
      <td className="pc">{pct(v)}%</td>
    </>
  );
  const side = (x: string) => <code className={x === type ? "self" : undefined}>{x}</code>;
  const rule = (r: OntologyRead["rules"][number]) => (
    <tr key={`${r.from}|${r.relationship}|${r.to}`}>
      <td>{side(r.from)} <span className="ed">{r.relationship.replace(/_/g, " ")}</span> {side(r.to)}</td>
      <td className="pc">{r.count ? fmt(r.count) : ""}</td>
    </tr>
  );
  const unused = allUnused ? d.unused : d.unused.slice(0, UNUSED_RULES_SHOWN);
  const count = model.count[type] ?? 0;
  return (
    <div className="dt">
      <p className="dt-actions">
        <CopyableId text={type} what="record type" />
        {count ? (
          <a href={link("explore", { type })}>Open {fmt(count)} {type} {count === 1 ? "record" : "records"} in Explore</a>
        ) : null}
        {count ? <a href={link("graph", { view: "map", type })}>Show on the map</a> : null}
      </p>
      <section>
        <h4>
          Fields{" "}
          {d.sample
            ? <span className="sub">{fmt(d.sample.sampled)} of {fmt(d.sample.population)} records sampled</span>
            : d.declared?.attributes.length ? <span className="sub">no records to sample</span> : null}
        </h4>
        {d.fields.length ? (
          <table className="dtf"><tbody>
            {d.fields.map((f) => (
              <tr key={f.name} data-origin={f.origin}>
                <td><Breakable text={f.name} /></td>
                <td className="rq">
                  {f.origin === "declared"
                    ? (f.required ? "required" : "")
                    : f.origin === "system"
                      ? <span className="sys" title="Written by the platform on records of every type; no schema declares it">system</span>
                      : <span className="tag" title="Seen on records of this type, and declared by no schema">not declared</span>}
                </td>
                <td className="rg">{f.range ?? ""}</td>
                {!d.sample ? <td colSpan={2} /> : f.notSampled ? (
                  <td colSpan={2}><span className="sub" title="The access policy decides on this field, so the sample leaves it out">not sampled</span></td>
                ) : f.fill !== null ? bar(f.fill) : (
                  <td colSpan={2}>
                    {/* Amber only where it matters: a REQUIRED field no sampled
                        record carries. An optional one is only a fact. */}
                    {f.required ? <span className="tag">never seen</span> : <span className="sub">never seen</span>}
                  </td>
                )}
              </tr>
            ))}
          </tbody></table>
        ) : <p className="sub">None declared.</p>}
      </section>
      <section>
        <h4>Relationships the ontology allows <span className="sub">{d.used.length} of {d.rules} used</span></h4>
        {d.rules ? (
          <table className="dtr"><tbody>
            {d.used.map(rule)}
            {d.unused.length ? <tr className="uh"><td colSpan={2}>Allowed, never used</td></tr> : null}
            {unused.map(rule)}
            {d.unused.length > UNUSED_RULES_SHOWN ? (
              <tr className="more"><td colSpan={2}>
                <button type="button" className="okg-console__link-button" onClick={() => setAllUnused((was) => !was)}>
                  {allUnused ? "Show the first 8" : `and ${d.unused.length - UNUSED_RULES_SHOWN} more`}
                </button>
              </td></tr>
            ) : null}
          </tbody></table>
        ) : <p className="sub">The ontology declares no relationship for this type.</p>}
      </section>
    </div>
  );
}

export function TableView({
  model, params, link, setParams, ontology, ontologyFailure, coverage, producersFailure,
}: {
  model: GraphModel;
  params: Record<string, string>;
  link: LinkFn;
  setParams: SetParams;
  ontology: OntologyRead | null;
  /** The ontology read's failure, drawn; null while it answers or reads. */
  ontologyFailure: ReactNode;
  /** How many of the deployment's producers this version holds anything
   *  from; null until the producers read answers. */
  coverage: ProducerCoverage | null;
  producersFailure: ReactNode;
}): ReactElement {
  const open = params.open ?? null;
  const showEmpty = params.empty === "1";
  const declared = useMemo(() => (ontology ? Object.fromEntries(ontology.types.map((t) => [t.type, t])) : {}), [ontology]);
  const empties = ontology ? emptyTypes(model, ontology) : [];
  const rows = [...model.types, ...(showEmpty && ontology ? empties : [])];
  // One tab stop for the rows: the open row, else the one last focused.
  const [active, setActive] = useState<string | null>(open);
  const focusTo = useRef<string | null>(null);
  const bodyRef = useRef<HTMLTableSectionElement>(null);
  const current = active && rows.includes(active) ? active : (open && rows.includes(open) ? open : rows[0]);
  useEffect(() => {
    if (!focusTo.current) return;
    const el = bodyRef.current?.querySelector<HTMLElement>(`tr.row[data-t="${CSS.escape(focusTo.current)}"]`);
    focusTo.current = null;
    el?.focus();
  });
  const toggle = (t: string) => setParams({ open: open === t ? null : t });
  const onKey = (event: KeyboardEvent<HTMLTableRowElement>, t: string) => {
    const at = rows.indexOf(t);
    const move = (to: number) => {
      event.preventDefault();
      const next = rows[Math.max(0, Math.min(rows.length - 1, to))];
      setActive(next);
      focusTo.current = next;
    };
    if (event.key === "Enter" || event.key === " ") { event.preventDefault(); toggle(t); }
    else if (event.key === "Escape" && open) { event.preventDefault(); setParams({ open: null }); }
    else if (event.key === "ArrowDown") move(at + 1);
    else if (event.key === "ArrowUp") move(at - 1);
    else if (event.key === "Home") move(0);
    else if (event.key === "End") move(rows.length - 1);
  };
  const cols = ontology ? 7 : 6;
  const row = (t: string) => {
    const c = model.count[t] ?? 0;
    const isOpen = open === t;
    const made = model.byType[t] ?? [];
    const lin = linksIn(model.patterns, t);
    const lout = linksOut(model.patterns, t);
    const within = linksWithin(model.patterns, t);
    return (
      <Fragment key={t}>
        <tr
          className={`row${c ? "" : " empty"}${isOpen ? " open" : ""}`} tabIndex={t === current ? 0 : -1} aria-expanded={isOpen} data-t={t}
          onClick={() => { setActive(t); toggle(t); }} onKeyDown={(event) => onKey(event, t)} onFocus={() => setActive(t)}
        >
          <td className="l ty"><span className="chev" aria-hidden="true" /><code>{t}</code></td>
          <td className="rec">{countCell(c)}</td>
          <td className={lin ? undefined : "none"} data-l="in">{countCell(lin)}</td>
          <td className={lout ? undefined : "none"} data-l="out">{countCell(lout)}</td>
          <td className={within ? undefined : "none"} data-l="within">{countCell(within)}</td>
          <td className="l mbc">
            {made.map(([s, n]) => (
              <span className="mb" key={s}><Dot color={model.srcColor[s]} />{model.name(s)}{made.length > 1 ? <> <span className="sub">{fmt(n)}</span></> : null}</span>
            ))}
          </td>
          {ontology ? <td className="l sub">{moduleLabel(declared[t]?.module)}</td> : null}
        </tr>
        {isOpen ? (
          <tr className="detail"><td colSpan={cols}>
            {ontology ? <Detail type={t} ontology={ontology} model={model} link={link} /> : ontologyFailure ? (
              <p className="sub">Fields unavailable: the declared-types read failed (above).</p>
            ) : <ReadingLine what="the declared fields" className="sub" />}
          </td></tr>
        ) : null}
      </Fragment>
    );
  };
  const enrichers = model.enrichers.filter(([, n]) => n > 0);
  return (
    <div className="work single">
      <div className="tablewrap" data-testid="graph-table">
        <h2 className="gh">Record types</h2>
        <p className="tnote" data-testid="graph-table-coverage">
          {ontology
            ? `${model.types.length} of ${ontology.types.length} declared types hold records.`
            : `${model.types.length} types hold records.`}
          {ontology ? (
            <label>
              <input type="checkbox" checked={showEmpty} onChange={(event) => setParams({ empty: event.target.checked ? "1" : null })} />
              {" "}List the {empties.length} with none
            </label>
          ) : null}
        </p>
        {!ontology && !ontologyFailure ? (
          <ReadingLine what="the declared types (fields, allowed relationships, the declaring module)" className="tnote" testId="graph-ontology-reading" />
        ) : null}
        {ontologyFailure && !ontology ? ontologyFailure : null}
        <div className="tscroll">
          <table className="tt types">
            <thead>
              <tr>
                <th className="l">Type</th><th>Records</th><th title="Relationships from other types to this one">Links in</th>
                <th title="Relationships from this type to other types">Links out</th>
                <th title="Relationships between two records of this type">Within type</th>
                <th className="l mbc">Made by</th>{ontology ? <th className="l">Declared in</th> : null}
              </tr>
            </thead>
            <tbody ref={bodyRef}>
              {model.types.map(row)}
              {showEmpty && ontology ? (
                <>
                  <tr className="sec"><td colSpan={cols}>Declared, no records</td></tr>
                  {empties.map(row)}
                </>
              ) : null}
            </tbody>
          </table>
        </div>
        <h2 className="gh gh2">Sources</h2>
        <p className="tnote" data-testid="graph-sources-coverage">
          {coverageSentence(model, coverage)}{" "}
          <a href={link("sources")}>All producers in Sources</a>
          {coverage && coverage.unlisted.length ? (
            <> {coverage.unlisted.length === 1
              ? "One producer here is not in the producers list and is named by its id."
              : `${coverage.unlisted.length} producers here are not in the producers list and are named by their ids.`}</>
          ) : null}
        </p>
        {producersFailure}
        <div className="tscroll">
          <table className="tt srcs">
            <thead><tr><th className="l">Source</th><th>Records</th><th>Relationships</th><th className="l mbc">Types it makes</th></tr></thead>
            <tbody>
              {model.sources.map((s) => (
                <tr key={s}>
                  <td className="l pnc"><Dot color={model.srcColor[s]} /><ProducerName label={model.label(s)} /></td>
                  <td className="rec">{countCell(model.srcTotal[s])}</td>
                  <td className={model.edgeBy[s] ? undefined : "none"} data-l="relationships">{countCell(model.edgeBy[s] ?? 0)}</td>
                  <td className="l mbc">{typesOf(model, s).map((x) => <Fragment key={x.type}><code className="tm">{x.type}</code>{" "}</Fragment>)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {enrichers.length ? (
          <>
            <h2 className="gh gh2">Enrichers</h2>
            <p className="tnote">Enrichers add relationships between records that already exist; they make no records.</p>
            <div className="tscroll">
              <table className="tt srcs enrs">
                <thead><tr><th className="l">Enricher</th><th>Relationships</th><th className="l mbc">Relationships it adds</th></tr></thead>
                <tbody>
                  {enrichers.map(([p, n]) => (
                    <tr className="enr" key={p}>
                      <td className="l pnc"><Dot ring /><ProducerName label={{ ...model.label(p), kind: null }} /></td>
                      <td className="rec" data-l="relationships">{countCell(n)}</td>
                      <td className="l mbc">
                        {enricherPatterns(model, p).map((x) => (
                          <span className="tm" key={`${x.s}|${x.e}|${x.d}`}><code>{x.s}</code> <span className="ed">{x.e.replace(/_/g, " ")}</span> <code>{x.d}</code></span>
                        ))}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}
        <p className="tnote tfoot" data-testid="graph-table-footer">
          {versionCountsSentence(model)}
          {fillSampleSentence(ontology) ? ` ${fillSampleSentence(ontology)}` : null}
          {ontology?.attributesBasis === "deployment_schema_files_at_read_time" ? " Declared fields are read from the deployment's schema files as they are now, not as they were at this version." : null}
        </p>
      </div>
    </div>
  );
}

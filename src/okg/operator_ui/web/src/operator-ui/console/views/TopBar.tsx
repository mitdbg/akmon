// The redesign's top bar: one band on every view, and the version position
// printed rather than implied.
//
// Reference: pact/changes/operator-ui-redesign-mocks/ui/references/top-bar.md.
// GitHub's repo home is the proportion reference (mark, nav, search, then
// account controls on one row at 1440; two rows at 390 with the search box
// full width); lakeFS is the reference for naming the ref being read rather
// than leaving the reader to infer it.
//
// Read-only. Every control either navigates or changes what is READ: no run,
// retry, publish or edit. The one control that is not a product control is the
// design-fixture switcher at the far right, which exists only in the harness
// and is styled to look like what it is.
import { useContext, useEffect, useRef, useState, type ReactElement } from "react";
import { ConsoleReadsContext, READ_RETRY_DELAYS_MS } from "@operator-ui/console/components/readRecovery";
import { ExportOutlined, SearchOutlined } from "@operator-ui/console/components/icons";
import { type RecordPickProps, useRecordPicker } from "@operator-ui/console/components/RecordPicker";

import { chatSiteStateForDeployments } from "@operator-ui/app/deployments";
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { type ChatSiteState, useChatSite } from "@operator-ui/shared/hooks/chatSiteContext";
import { AkmonMark } from "@operator-ui/shared/ui/AkmonMark";
import { applyTheme, THEME_LABELS, THEMES, type Theme, useTheme } from "@operator-ui/styles/theme";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import { searchHash } from "@operator-ui/console/model/shared/links";
import { namedReadFailure, readFailureLine } from "@operator-ui/console/model/shared/readFailure";
import { utcStamp } from "@operator-ui/console/model/shared/time";

/** The two reads the bar makes for itself. Named here so the bar can say
 *  which one failed. */
export const DEPLOYMENTS_ROUTE = "/viz/api/deployments";
export const BAR_VERSIONS_ROUTE = "/viz/api/console/versions";

/** What stands where the deployment's name goes. "not read yet" says the read
 *  is still coming, which after a rejection it is not. */
export function deploymentName(failure: unknown): string {
  return failure === null ? "not read yet" : "not read";
}

/** The version picker's placeholder: what it is waiting for, what the read
 *  found, or that the read failed. Each picker answers for its OWN read: one
 *  emptied because the other refused is a bar inventing a fact. */
export function versionsPlaceholder(versions: unknown, failure: unknown): string {
  if (failure !== null) return "not read";
  return versions ? "none published" : "not read yet";
}

/**
 * What the bar prints for each of the four chat-site states.
 *
 * The states are NOT collapsed to "have a link or do not". Three of them are
 * different kinds of absence, and `shared/hooks/chatSiteContext.ts` sets out
 * why conflating them makes the console assert things it never read:
 * `undeclared` is a manifest that WAS read and declares no chat site,
 * `unresolved` is no manifest read at all, and `pending` is the same claim one
 * moment before the read answers.
 *
 * No value is printed in any of them. The declared state's href is the URL the
 * manifest declared; the words the reader sees are these, and never a host --
 * the summary pages print no raw codes and no hostnames.
 */
export const CHAT_LABEL_DECLARED = "Open chat";
export const CHAT_LABEL_UNDECLARED = "No chat site declared";
export const CHAT_LABEL_UNRESOLVED = "Chat site unknown";

/**
 * The bar's chat control, in the RIGHT group after the theme picker.
 *
 * The approved reference (`pact/changes/operator-ui-redesign-mocks/ui/
 * references/top-bar.md`) lists the right side as the deployment picker, the
 * version picker, the theme picker and Open chat; it sat in the left nav group
 * instead, reading as a fifth view.
 *
 * It also rendered NOTHING when no chat site was declared, which is the one
 * thing the four-state contract exists to prevent: silence says the same about
 * a deployment that declares no chat site, a deployment nobody identified, and
 * a read that has not answered. Now each says which it is, in words, and only
 * `declared` is a link.
 */
function ConsoleChatControl({ state }: { state: ChatSiteState }): ReactElement | null {
  const subject = state.deploymentName ? `${state.deploymentName} ` : "";

  // Not yet known is not known-absent. Nothing is printed until the
  // deployments read answers, rather than flashing a claim the bar cannot
  // support for as long as the read takes.
  if (state.status === "pending") return null;

  if (state.status === "declared" && state.href) {
    return (
      <a
        className="okg-console-bar__chat"
        data-testid="console-bar-chat"
        data-chat-state="declared"
        href={state.href}
        target="_blank"
        rel="noopener noreferrer"
        title={`Open the ${subject}chat site`}
        aria-label={`Open the ${subject}chat site (opens in a new tab)`}
      >
        {CHAT_LABEL_DECLARED}
        <ExportOutlined className="okg-console-bar__external" aria-hidden="true" />
      </a>
    );
  }

  // Both remaining states are TEXT, not a disabled button: there is no action
  // to offer, and a greyed control invites a reader to look for the way to
  // enable it. The title carries the manifest key, which the words do not.
  if (state.status === "unresolved") {
    return (
      <span
        className="okg-console-bar__chat okg-console-bar__chat--absent"
        data-testid="console-bar-chat"
        data-chat-state="unresolved"
        title={
          "The current deployment could not be identified from "
          + "/viz/api/deployments, so the console cannot tell whether a chat "
          + "site is declared."
        }
      >
        {CHAT_LABEL_UNRESOLVED}
      </span>
    );
  }

  return (
    <span
      className="okg-console-bar__chat okg-console-bar__chat--absent"
      data-testid="console-bar-chat"
      data-chat-state="undeclared"
      title={
        `${subject || "This deployment "}declares no chat site `
        + "(chat.deep_links.chat_site_base), so the console has no chat link "
        + "to offer."
      }
    >
      {CHAT_LABEL_UNDECLARED}
    </span>
  );
}

export const CONSOLE_VIEWS = ["Home", "Updates", "Sources", "Explore", "Graph"] as const;
export type ConsoleView = (typeof CONSOLE_VIEWS)[number];

interface DeploymentsRead {
  entries: {
    name: string;
    chat_site_base: string | null;
    is_current: boolean;
  }[];
}

interface VersionsRead {
  payload: {
    versions: { generation_id: string; published_at: string }[];
    latest: string | null;
  };
}

export interface ConsoleTopBarProps extends RecordPickProps {
  api: ApiClient;
  view: ConsoleView;
  /** Builds an in-app href, carrying whatever the harness needs to keep the
   *  fixture selected. */
  link: (route: string, params?: Record<string, string>) => string;
  /** The design-only fixture control, rendered at the far right. */
  designControl?: ReactElement | null;
  /** Which views this build actually serves. Omitted, all four are links,
   *  which is what the harness serves. The live console of slice 1 serves
   *  Home alone and passes `["Home"]`: the other three keep their place in
   *  the bar -- removing them would change the composition the operator
   *  approved -- but render as plain text saying they are not live yet,
   *  rather than as links to a page nobody has checked against real data. */
  liveViews?: readonly ConsoleView[] | readonly string[];
  /** Accepted and unused. The bar printed relative ages ("3 d ago") against
   *  this clock; it prints each version's UTC publish minute now, which needs
   *  no clock. Kept so the harness, which passes it, does not change. */
  nowMs?: number | null;
  /** The version the READER CHOSE, off the URL. The picker holds it, so a
   *  choice is not undone by a view whose reads cannot take it; what the
   *  page actually read is said in words by the note under the bar and by
   *  the page's footer. Null means nothing was chosen. */
  version?: string | null;
  /** The version this view's reads answered for. The picker falls back to it
   *  when nothing was chosen, and when the chosen one is not among the ids
   *  this deployment published -- a value the picker cannot select would
   *  leave the control showing some other version's option as if it were the
   *  answer. */
  readVersion?: string | null;
  /** Set when the picker is a real control. Absent leaves it read-only, which
   *  is what the shipping console still is. */
  onVersion?: ((generationId: string) => void) | null;
  /** True on a view whose reads take `?version=` (Explore). False on the
   *  views that always read the latest version -- Home, Updates, Sources --
   *  where the picker shows "latest" whatever was chosen, because that is
   *  what the page under it shows; the page says so in a note under its
   *  title. Omitted, the picker behaves as it does on a version-reading
   *  view, which is what the design harness mounts. */
  versionReading?: boolean;
  /** Changes whenever the page's own reads answer. A bar whose reads failed
   *  reads again when it changes: the page under it has just shown that the
   *  read API is answering, and a bar still saying "not read" above a page
   *  that read fine is the header contradicting the page. */
  pageReadKey?: unknown;
  /** Read the open view again. A click on the current view's own link calls
   *  it; absent, that click does what the browser does, which is nothing. */
  onReloadView?: (() => void) | null;
  /** On Explore the box IS the page's search input: controlled by the page,
   *  results moving as the reader types, Enter and the arrows handled there.
   *  Absent, the box is its own: Enter opens Explore with the text. */
  search?: { value: string; onChange: (value: string) => void; autoFocus?: boolean } | null;
  /** True where the VIEW shows the search box -- Explore's landing has a
   *  640 px one. Two boxes carrying the same draft is the thing rule 9 is
   *  against, so the bar drops its own and "/" finds the page's. */
  hideSearch?: boolean;
}

/** When a published version was published, or the empty string when this
 *  read does not list it. Never a substituted stamp: the caller prints
 *  nothing rather than a time that is not the version's. */
function publishedAtOf(rows: readonly { generation_id: string; published_at: string }[], id: string): string {
  return rows.find((row) => row.generation_id === id)?.published_at ?? "";
}

/** What the version picker prints for one published version: the short id
 *  and the UTC minute it was published.
 *
 *  Not a relative age. Archiv3 published its three versions within ninety
 *  seconds of each other, three days before the review, and every option
 *  read "· 3 d ago" -- three options the reader could tell apart only by a
 *  hex id (review 2026-09-22). */
export function versionOptionLabel(generationId: string, publishedAt: string): string {
  return `${shortVersion(generationId)} · ${utcStamp(publishedAt)}`;
}

/** The picker's entry on a view that always reads the latest version. */
export const LATEST_OPTION_LABEL = "latest";
/** Its value. Not a generation id, so choosing it drops `?version=`. */
export const LATEST_OPTION_VALUE = "";

/** How long the bar waits before each retry of a read that failed. Three
 *  tries, then it stops: a view change or a page read that answers starts it
 *  again. A bar that polled a dead read API forever would be load on it. */
export const BAR_RETRY_DELAYS_MS: readonly number[] = READ_RETRY_DELAYS_MS;

/**
 * What the version picker's CLOSED control shows.
 *
 * The short id and the day and clock it was published, without the year: the
 * year is the same on every row in the list, so it is the first thing to lose
 * when the bar is short of room. The open list prints the full stamp.
 */
export function versionControlLabel(generationId: string, publishedAt: string): string {
  return `${shortVersion(generationId)} · ${utcStamp(publishedAt).slice(5, 16)}`;
}

/** The width below which the bar stacks. One number, shared by the component
 *  and styles/shell.css, so the layout and the labels change together. */
export const NARROW_MAX_PX = 760;

/** True at or below 1360 px, where the version picker lists short ids alone:
 *  with each option's publish minute the picker was 306 px and the bar ran
 *  135 px past a 1280 window (audit 2026-09-27); each option's title keeps the
 *  minute. A label is a value, not a style, so the component has to ask. */
function useNarrow(): boolean {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) return undefined;
    const query = window.matchMedia("(max-width: 1360px)");
    const apply = () => setNarrow(query.matches);
    apply();
    query.addEventListener("change", apply);
    return () => query.removeEventListener("change", apply);
  }, []);
  return narrow;
}

export function ConsoleTopBar({
  api, view, link, designControl = null, version = null, readVersion = null, onVersion = null, search = null,
  hideSearch = false,
  liveViews,
  versionReading = true,
  pageReadKey = null,
  onReloadView = null, ...pick
}: ConsoleTopBarProps): ReactElement {
  const theme = useTheme();
  const narrow = useNarrow();
  const [pickersOpen, setPickersOpen] = useState(false);
  const [deployments, setDeployments] = useState<DeploymentsRead | null>(null);
  const [deploymentsFailure, setDeploymentsFailure] = useState<unknown>(null);
  const [versions, setVersions] = useState<VersionsRead | null>(null);
  const [versionsFailure, setVersionsFailure] = useState<unknown>(null);
  const [ownQuery, setOwnQuery] = useState("");
  const query = search ? search.value : ownQuery;
  const setQuery = search ? search.onChange : setOwnQuery;
  const inputRef = useRef<HTMLInputElement>(null);

  // "/" focuses the box from anywhere on the page that is not already a
  // field (rule 9: one search box, everywhere, with a keyboard shortcut).
  useEffect(() => {
    if (typeof document === "undefined") return undefined;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target && (target.closest("input, textarea, select, [contenteditable]"))) return;
      event.preventDefault();
      // Whichever box is on screen. The bar hides its own on a view that
      // shows one of its own, and "/" has to reach that one.
      const own = inputRef.current;
      const ownShown = own !== null && own.offsetParent !== null
        && own.closest('[data-hidden="true"]') === null;
      if (own && ownShown) { own.focus(); return; }
      const inPage = document.querySelector<HTMLInputElement>('[data-testid="explore-search-input"]');
      inPage?.focus();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (search?.autoFocus) inputRef.current?.focus();
  }, [search?.autoFocus]);

  // What the SHELL already read. HomeApp reads `/viz/api/deployments` for
  // the chat site and the deployment's theme before the console's chunk has
  // even loaded, and the bar used to read it a second time on every load
  // (review 2026-09-22). Where the shell's read answered and found the
  // current deployment, the bar takes the name and the chat site from it and
  // makes no read of its own. Where it did not -- still pending, refused, or
  // no entry came back current -- the bar reads for itself, so a refusal is
  // named in its own line and can recover. A read still in flight is shared
  // by the client's own coalescing rather than sent twice.
  const shellChat = useChatSite();
  const shellKnows = shellChat !== null
    && (shellChat.status === "declared" || shellChat.status === "undeclared");

  // Each read on its own, each failure caught HERE, and each read made
  // again until it answers.
  //
  // The two used to be one `Promise.all` with no `catch`: a refused read was
  // an unhandled rejection and left "not read yet" in the bar. Then they were
  // made ONCE per client: after a 502 the bar kept "not read", an empty
  // picker, "Chat site unknown" and two failure lines long after the read API
  // answered and the page under it had reloaded. A read that has answered is
  // not made again; one that has not is made again when the view changes,
  // when the page's own reads answer (`pageReadKey`), and on a short bounded
  // retry. The failure line stays up until the answer replaces it.
  //
  // The page's reads and the bar's recover TOGETHER. The bar re-reads when
  // any read on the page answers after failing (`recoveryKey`), and a bar
  // read that answers after failing tells the page (`reportRecovered`), so
  // the page's failed reads go again at once. The bar said "answering" over
  // a body still saying "not answering" before (audit 2026-09-23).
  const { recoveryKey, reloadKey, reportRecovered } = useContext(ConsoleReadsContext);
  const [retry, setRetry] = useState(0);
  const deploymentsAnswered = useRef(false);
  const versionsAnswered = useRef(false);
  const barFailed = useRef(false);
  useEffect(() => {
    let cancelled = false;
    const answered = () => {
      if (!barFailed.current) return;
      barFailed.current = false;
      reportRecovered();
    };
    if (!shellKnows && !deploymentsAnswered.current) {
      void api.get<DeploymentsRead>(DEPLOYMENTS_ROUTE).then(
        (value) => {
          if (cancelled) return;
          deploymentsAnswered.current = true;
          setDeployments(value);
          setDeploymentsFailure(null);
          answered();
        },
        (reason: unknown) => {
          if (cancelled) return;
          barFailed.current = true;
          setDeploymentsFailure(reason);
        },
      );
    }
    if (!versionsAnswered.current) {
      void api.get<VersionsRead>(BAR_VERSIONS_ROUTE).then(
        (value) => {
          if (cancelled) return;
          versionsAnswered.current = true;
          setVersions(value);
          setVersionsFailure(null);
          answered();
        },
        (reason: unknown) => {
          if (cancelled) return;
          barFailed.current = true;
          setVersionsFailure(reason);
        },
      );
    }
    return () => { cancelled = true; };
    // `reportRecovered` is stable per console; it is read, not tracked.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, view, pageReadKey, retry, shellKnows, recoveryKey, reloadKey]);

  // The bounded retry. Counted per bar, not per failure, so it cannot run on
  // for as long as the API stays down; a view change or a page read still
  // tries again after it has stopped.
  const failing = (!shellKnows && deploymentsFailure !== null) || versionsFailure !== null;
  useEffect(() => {
    if (!failing || retry >= BAR_RETRY_DELAYS_MS.length) return undefined;
    const timer = setTimeout(() => setRetry((count) => count + 1), BAR_RETRY_DELAYS_MS[retry]);
    return () => clearTimeout(timer);
  }, [failing, retry, deploymentsFailure, versionsFailure]);

  // One line for each read that failed: what happened, and the route once.
  const failures = [
    shellKnows || deploymentsFailure === null
      ? null
      : `This deployment's name could not be read. `
        + `${readFailureLine(namedReadFailure(DEPLOYMENTS_ROUTE, deploymentsFailure))}`,
    versionsFailure === null
      ? null
      : `The published versions could not be read. `
        + `${readFailureLine(namedReadFailure(BAR_VERSIONS_ROUTE, versionsFailure))}`,
  ].filter((line): line is string => line !== null);

  const entries = deployments?.entries ?? [];
  // The name the bar prints. The shell's, where the shell's read found the
  // current deployment; otherwise the bar's own read, falling back to its
  // first entry so there is some name to print, which is cosmetic.
  const ownCurrent = entries.find((entry) => entry.is_current) ?? entries[0] ?? null;
  const currentName = shellKnows ? shellChat.deploymentName : ownCurrent?.name ?? null;
  // The chat control does NOT use that fallback. A chat_site_base becomes an
  // anchor's href, and taking it from an entry the backend did not stamp
  // `is_current` would hand one deployment's chat site to another.
  // `chatSiteStateForDeployments` selects on `is_current` alone, and is the
  // same function the shell's own state comes from.
  //
  // A REFUSED read counts as answered, exactly as the shell counts it
  // (`app/HomeApp.tsx`): no entry came back current, so the state is
  // `unresolved` -- the console cannot say whether a chat site is declared --
  // and the failure itself is named in the bar's own read-failure line below.
  const chatSite = shellKnows
    ? shellChat
    : chatSiteStateForDeployments(entries, deployments !== null || deploymentsFailure !== null);
  const published = versions?.payload.versions ?? [];
  // What the bar PRINTS is what the reader CHOSE, when this deployment
  // published it; otherwise the version this view's reads answered for, and
  // failing that the latest. Rule 10 is that the version position is
  // printed, not that it is always the newest.
  //
  // The publication test is not decoration. The control is a `<select>`, and
  // a value matching none of its options selects nothing: the browser then
  // shows the FIRST option, which is a different version presented as the
  // answer. A chosen id the deployment never published falls back here
  // instead, and the page's own note says a version was asked for and what
  // became of it.
  const chosen = version && published.some((row) => row.generation_id === version)
    ? version
    : null;
  // On a view whose reads take no version the picker names what the page
  // under it shows -- the latest -- and never the choice it cannot honour.
  // The review (2026-09-22) chose fdf6da58cfda on Home: the picker said
  // fdf6, while Home, Sources and Updates went on showing efc517cac053 under
  // a two-clause note. The choice still rides on every link, so Explore
  // opens on it.
  const latestId = versions?.payload.latest ?? null;
  const pageIsLatest = readVersion === null || latestId === null || readVersion === latestId;
  const showLatest = !versionReading && pageIsLatest;
  const latest = versionReading
    ? chosen ?? readVersion ?? latestId
    : readVersion ?? latestId;

  // Submitting the bar's box opens Explore's results for whatever is in it --
  // on every view, including Explore itself.
  //
  // It used to RETURN here whenever the shell owned the draft, on the grounds
  // that "on Explore itself Enter opens the focused result; the page handles
  // it". The page's box was bailing out in the mirror image of this, and once
  // the shell owned one draft for the whole console both conditions were
  // false everywhere: Enter did nothing at all (reported 2026-09-22). Explore
  // still takes Enter first when the reader has arrowed to a result row.
  const openExplore = () => { window.location.hash = searchHash(query, link); };
  const picker = useRecordPicker(api, pick, version ?? latestId, query, () => setQuery(""));

  return (
    <header className="okg-console-bar" data-testid="console-top-bar">
      <div className="okg-console-bar__row">
        <div className="okg-console-bar__left">
        <a className="okg-console-bar__brand" href={link("home")} aria-label="AKMON operator console">
          <span className="okg-console-bar__brand-mark" aria-hidden="true"><AkmonMark /></span>
          {/* One line. The two-line lockup put "OPERATOR" at 9 px, which is
              smaller than anything else in the bar and needed a named
              exemption from rule 11. The shipping Search view's shell is
              untouched. */}
          <span className="okg-console-bar__brand-label">AKMON</span>
        </a>
        <nav className="okg-console-bar__nav" aria-label="Console views">
          {CONSOLE_VIEWS.map((candidate) => (
            liveViews && !liveViews.includes(candidate) ? (
              // The name stays in its place, dimmed and not a link; the words
              // that explain it are printed ONCE after the group rather than
              // on each entry. Three copies of "not live yet" is the same
              // fact three times, and at 390 it pushed the bar off the
              // viewport.
              <span
                key={candidate}
                className="okg-console-bar__link okg-console-bar__link--absent"
                data-testid={`console-nav-not-live-${candidate.toLowerCase()}`}
                data-current="false"
                // The state is carried by the entry itself -- dimmed, not a
                // link, and described for a screen reader -- so it costs the
                // bar NO width. A word after each name, or after the group,
                // pushed the left group into the search box's fixed column
                // and clipped whatever sat at the seam. The page says which
                // views are not live in a line of its own.
                title={`${candidate} is not live yet`}
                aria-label={`${candidate}, not live yet`}
              >
                {candidate}
              </span>
            ) : (
              <a
                key={candidate}
                className="okg-console-bar__link"
                href={link(candidate.toLowerCase())}
                aria-current={candidate === view ? "page" : undefined}
                data-current={candidate === view ? "true" : "false"}
                // The link to the view already open changes no hash, so the
                // browser does nothing. It reads the view again instead: the
                // one gesture a reader tries on a page that looks stuck.
                onClick={candidate === view && onReloadView ? (event) => {
                  const href = link(candidate.toLowerCase());
                  if (typeof window !== "undefined" && window.location.hash === href) {
                    event.preventDefault();
                    onReloadView();
                  }
                } : undefined}
              >
                {candidate}
              </a>
            )
          ))}
          {/* Chat is NOT here. It used to sit at the end of this group,
              reading as a fifth view; the approved reference puts it in the
              right-hand control group after the theme picker, which is where
              `ConsoleChatControl` renders it. */}
        </nav>
        </div>

        {/* A real form, so the browser's own Enter-in-a-single-field
            submission and a screen reader's submit both reach `onSubmit`. */}
        <form
          className="okg-console-bar__search"
          data-testid="console-search"
          role="search"
          // Not `hidden`: removing the form removed a grid cell, and the
          // controls fell into the 400 px middle column and spilled left over
          // the nav (audit B1). The cell stays; shell.css hides its contents
          // and `inert` takes it out of the tab order and the reading order.
          data-hidden={hideSearch ? "true" : undefined}
          inert={hideSearch}
          onSubmit={(event) => { event.preventDefault(); if (!picker) openExplore(); }}
        >
          <span className="okg-console-bar__search-field">
            <SearchOutlined className="okg-console-bar__search-icon" aria-hidden="true" />
            <input
              ref={inputRef}
              type="search"
              className="okg-console-bar__search-input"
              value={query}
              placeholder="Search records"
              aria-label={search ? "Search records" : "Search records; opens Explore"}
              {...picker?.inputProps}
              onChange={(event) => { setQuery(event.target.value); picker?.onFocus(); }}
              onKeyDown={(event) => { if (picker?.onKeyDown(event)) return;
                if (event.key === "Enter") { event.preventDefault(); openExplore(); }
                // Escape empties the box. Explore's page handler takes
                // Escape first when a result row is focused, and clears the
                // focus instead; only when none is does it reach here.
                if (event.key === "Escape") { event.preventDefault(); setQuery(""); }
              }}
            />
            <kbd className="okg-console-bar__shortcut">/</kbd>{picker?.list}
          </span>
        </form>

        {/* At 390 the pickers collapse behind one control, and what stays
            printed beside it is the DEPLOYMENT NAME. It used to be the
            version id: a 12-hex string and a "⋯", with nothing on the phone
            bar saying which deployment any of it was about. The id is one tap
            away, in the panel this button opens, beside the picker that
            changes it; the name is the thing a reader cannot derive from
            anything else on the page. Truncated by CSS with the full name in
            the title, rather than dropped. */}
        <div className="okg-console-bar__summary">
          <span
            className="okg-console-bar__summary-deployment"
            data-testid="console-bar-summary-deployment"
            title={currentName ?? undefined}
          >
            {currentName ?? deploymentName(shellKnows ? null : deploymentsFailure)}
          </span>
          <button
            type="button"
            className="okg-console-bar__more"
            aria-expanded={pickersOpen}
            aria-label="Version and theme"
            onClick={() => setPickersOpen((open) => !open)}
          >
            ⋯
          </button>
        </div>

        <div
          className="okg-console-bar__controls"
          data-open={pickersOpen ? "true" : "false"}
        >
          {/* NOT a picker. A read API serves exactly one deployment --
              `ctx.session` is bound at start-up -- so the control offered a
              list of one and its chevron promised a choice that does not
              exist. The slot stays, and the picker comes back the day a read
              answers for more than one deployment; `ui/cuts.md` records
              that. */}
          <span
            className="okg-console-bar__deployment"
            data-testid="console-bar-deployment"
            title={currentName ?? undefined}
          >
            {currentName ?? deploymentName(shellKnows ? null : deploymentsFailure)}
          </span>

          {/* No "Version" label floating between two controls: the value is a
              12-hex id in monospace, which says what it is. The browser's own
              <select>: Ant Design's carried the whole library into the
              console chunk for a list of published ids, and this one opens
              the platform's list, which is also the one a phone gives. A
              version the console cannot switch to is printed rather than
              offered. */}
          {onVersion ? (
            <label className="okg-console-bar__control">
              <span className="okg-console-bar__sr">Version</span>
              <select
                className="okg-console-bar__select okg-console-bar__select--version"
                data-testid="console-bar-version"
                value={showLatest ? LATEST_OPTION_VALUE : latest ?? ""}
                title={latest ? versionOptionLabel(latest, publishedAtOf(published, latest)) : undefined}
                onChange={(event) => onVersion(event.target.value)}
              >
                {showLatest ? (
                  <option value={LATEST_OPTION_VALUE}>{LATEST_OPTION_LABEL}</option>
                ) : latest ? null : (
                  <option value="" disabled>
                    {versionsPlaceholder(versions, versionsFailure)}
                  </option>
                )}
                {published.map((row) => (
                  <option key={row.generation_id} value={row.generation_id} title={narrow ? versionOptionLabel(row.generation_id, row.published_at) : undefined}>
                    {narrow
                      ? shortVersion(row.generation_id)
                      : versionOptionLabel(row.generation_id, row.published_at)}
                  </option>
                ))}
              </select>
            </label>
          ) : (
            <span
              className="okg-console-bar__select okg-console-bar__select--version"
              data-testid="console-bar-version"
              title={latest ? versionOptionLabel(latest, publishedAtOf(published, latest)) : undefined}
            >
              {showLatest
                ? LATEST_OPTION_LABEL
                : latest
                  ? (narrow
                    ? shortVersion(latest)
                    : versionOptionLabel(latest, publishedAtOf(published, latest)))
                  : versionsPlaceholder(versions, versionsFailure)}
            </span>
          )}

          {/* "Theme", not "Firing". The palettes keep their names -- Cyclades,
              Attica -- because those are what the operator chose them to be
              called; the word in front of them is what the control DOES, and
              a reader who has not read the palette file cannot tell that
              "Firing: Cyclades" is about colour at all. A swatch and its
              name, one control wide, with the full sentence in the title:
              antd's Tooltip and Select are gone from this chunk. */}
          <label
            className="okg-console-bar__control okg-console-bar__firing"
            title={`Theme: ${THEME_LABELS[theme]}`}
          >
            <span className="okg-console-bar__swatch" aria-hidden="true" />
            <select
              aria-label={`Theme: ${THEME_LABELS[theme]}`}
              className="okg-console-bar__select okg-console-bar__select--firing"
              value={theme}
              onChange={(event) => applyTheme(event.target.value as Theme)}
            >
              {THEMES.map((name) => (
                <option key={name} value={name}>{THEME_LABELS[name]}</option>
              ))}
            </select>
          </label>

          {/* After the theme picker and before the harness's fixture switcher:
              the reference's right-hand order is deployment, version, theme,
              Open chat, and the switcher is not a product control at all. */}
          <ConsoleChatControl state={chatSite} />

          {designControl}
        </div>
      </div>
      {failures.length ? (
        <div
          className="okg-console-bar__read-failed"
          role="alert"
          data-testid="console-bar-read-failed"
        >
          {failures.map((line) => <p key={line}>{line}</p>)}
        </div>
      ) : null}
      <div className="okg-console-bar__meander" aria-hidden="true" />
    </header>
  );
}

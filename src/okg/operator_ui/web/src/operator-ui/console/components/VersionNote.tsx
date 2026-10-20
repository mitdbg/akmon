// What a page says when the top bar's version picker is showing a version its
// own reads cannot honour.
//
// The picker is a real control: it changes the URL and every link the console
// builds carries the choice. But most console reads take no `version`
// parameter -- `console/health`, `console/generations`, `console/versions`,
// the diagnostics reads and `console/ontology-rollup` all answer for the
// generation the read API's own session is pinned to. A page whose figures
// came from those reads must not look as though it honoured a choice it never
// saw, so it says which version it IS showing and which one was asked for.
//
// It lives here, not in the route that owns the picker, because Home, the
// Updates list and the attempt page all print it and a view importing the
// route that renders it would close an import cycle.
import type { ReactElement } from "react";

import { shortVersion } from "@operator-ui/console/model/shared/ids";

/**
 * The sentence a view whose reads take no version prints under its title
 * when `?version=` asked for one.
 *
 * `reading` is the version the page's reads answered for, from the health
 * read; `published` is the versions read's ids, newest first. The asked-for
 * value is accepted and never printed: on 2026-09-22 a PostgreSQL DSN,
 * password and all, arrived on `?version=`, and a sentence that echoed it
 * put it in the console's own prose.
 *
 * The subject is "This page", not the page's name: Home, the Updates list,
 * the attempt page and the producer page print the same sentence.
 */
export function versionNotReadHere(
  reading: string | null,
  _asked: string,
  published: readonly string[] = [],
): string {
  // ONE clause, and it says what the page shows. The sentence it replaces
  // named both versions and where the asked one went -- "This page shows
  // version A; it cannot read version B, which is carried on the links to
  // the views that can" -- which the review (2026-09-22) found convoluted,
  // above the title on Updates. The picker now says "latest" on these views,
  // so the note only has to say the page always is; the asked-for value is
  // no longer printed at all, recognised or not.
  const shown = shortVersion(reading);
  if (!shown) return "This deployment has published no version for this page to show.";
  // Compared by the short id: Home hands in the version it already shortened.
  const latest = published.length === 0 || shortVersion(published[0]) === shown;
  return latest
    ? `This page always shows the latest version, ${shown}.`
    : `This page always shows version ${shown}, the one this server reads.`;
}

/**
 * The sentence in place, or nothing when no version was asked for.
 *
 * A page draws this ONCE. The Updates list renders the newest blocked
 * attempt's detail inline, and the attempt detail is the same component the
 * attempt page uses, so the note is passed in by the page rather than drawn by
 * every block that could carry it; two copies would read as two facts about
 * two different reads.
 */
export function VersionNote({ reading, asked, published = [] }: {
  /** The version the page's reads answered for. */
  reading: string | null;
  /** What `?version=` carries, or null when it carries nothing. */
  asked: string | null;
  /** The generation ids the versions read returned, so a real one is named
   *  rather than withheld for failing a shape test. */
  published?: readonly string[];
}): ReactElement | null {
  if (!asked) return null;
  return (
    <p className="okg-console__status-note" data-testid="console-version-note">
      {versionNotReadHere(reading, asked, published)}
    </p>
  );
}

/**
 * The ONE line a page prints about the parameters on its URL.
 *
 * A page can be asked two different things by `?version=`: a version the top
 * bar's picker chose, which the note answers, and a leftover from when the
 * Updates table's own filter shared that name, which the ignored-parameter
 * line answers. They are never printed together, and the rule lives here so
 * that the list and the attempt page cannot answer it differently.
 */
export function VersionNotice({
  reading, asked, ignoredParam = null, published = [],
}: {
  reading: string | null;
  asked: string | null;
  /** The sentence from `ignoredVersionParam`, when there is one. */
  ignoredParam?: string | null;
  /** The generation ids the versions read returned. */
  published?: readonly string[];
}): ReactElement | null {
  if (ignoredParam) {
    return (
      <p className="okg-console__status-note" data-testid="console-ignored-param">
        {ignoredParam}
      </p>
    );
  }
  return <VersionNote reading={reading} asked={asked} published={published} />;
}

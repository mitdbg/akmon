// What every viewer tells the pane around it. "pending" until the artifact
// has been fetched and drawn, "ready" once it is on screen, "failed" with the
// reason when it is not -- the pane prints the reason in place (rule 13) and
// the capture script refuses to screenshot a pending or failed viewer.
export type ViewerState =
  | { kind: "pending" }
  | { kind: "ready"; note?: string }
  | { kind: "failed"; reason: string };

export type OnViewerState = (state: ViewerState) => void;

/** Fetch a text artifact. A non-2xx answer is a failure with the status in
 *  it, never an empty document. */
export async function fetchText(url: string): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url} answered ${response.status}`);
  return response.text();
}

export function reasonOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

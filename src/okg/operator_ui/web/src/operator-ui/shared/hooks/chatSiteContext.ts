import { createContext, useContext } from "react";

/**
 * Where this deployment's chat site lives, as declared by its manifest
 * (`chat.deep_links.chat_site_base`) and carried to the browser over
 * `/viz/api/deployments`.
 *
 * FOUR states, because three of them are different kinds of absence and
 * conflating them makes the console assert things it does not know:
 *
 * - `declared`   — the manifest declares a chat site; `href` is it.
 * - `undeclared` — the current deployment was identified and its manifest
 *                  declares no chat site. A fact about a manifest that was
 *                  READ.
 * - `unresolved` — no entry came back stamped current, so no manifest was
 *                  read at all. `/viz/api/deployments` derives `is_current`
 *                  from a context lookup whose failure is caught and dropped
 *                  (`src/okg/viz/app.py`), so this is reachable in normal
 *                  operation. Saying "declares no chat site" here would be a
 *                  claim about a file nobody opened.
 * - `pending`    — the deployments read has not answered yet. The same lie as
 *                  `unresolved`, one moment earlier.
 *
 * A `null` context value is the fifth case and means no provider is mounted
 * at all (the foundation demo, isolated component tests) — the shell then
 * renders no chat affordance whatsoever.
 *
 * In every non-`declared` state `href` is null. There is no default host, no
 * same-origin guess, and no previously seen base.
 */
export type ChatSiteStatus = "declared" | "undeclared" | "unresolved" | "pending";

export interface ChatSiteState {
  status: ChatSiteStatus;
  href: string | null;
  deploymentName: string | null;
}

export const ChatSiteContext = createContext<ChatSiteState | null>(null);

export function useChatSite(): ChatSiteState | null {
  return useContext(ChatSiteContext);
}

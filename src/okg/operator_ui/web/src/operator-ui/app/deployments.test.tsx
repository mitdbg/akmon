import { describe, expect, it, vi } from "vitest";
import { createApiClient } from "@operator-ui/shared/api/apiClient";
import {
  chatSiteStateForDeployments,
  chatSiteUrlForDeployment,
  currentDeploymentEntry,
  loadDeployments,
  type DeploymentEntry,
} from "@operator-ui/app/deployments";

describe("deployments read", () => {
  it("loads deployment entries through the GET-only client", async () => {
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ entries: [{ name: "cms", is_current: true }] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const api = createApiClient({ fetchImpl });

    await expect(loadDeployments(api)).resolves.toEqual([
      { name: "cms", is_current: true },
    ]);
    expect(fetchImpl).toHaveBeenCalledWith("/viz/api/deployments", {
      method: "GET",
      headers: { Accept: "application/json" },
    });
  });
});

// Moved from shared/ui/chatSiteLink.test.tsx when PACT
// console-default-remove-old-views deleted the old sidebar's chat link that
// file also tested. These state functions still feed the console's top bar.
describe("chat site state from the deployments read", () => {
  const DECLARED = "https://chat.okg.example.org";
  const entries: DeploymentEntry[] = [
    { name: "okg-workspace" },
    { name: "cms", is_current: true, chat_site_base: DECLARED },
  ];

  it("reads the current deployment's declared chat site", () => {
    expect(chatSiteUrlForDeployment(currentDeploymentEntry(entries))).toBe(DECLARED);
    expect(chatSiteStateForDeployments(entries, true)).toEqual({
      status: "declared",
      href: DECLARED,
      deploymentName: "cms",
    });
  });

  it("returns null rather than a default when the field is absent or blank", () => {
    expect(chatSiteUrlForDeployment({ name: "cms" })).toBeNull();
    expect(chatSiteUrlForDeployment({ name: "cms", chat_site_base: null })).toBeNull();
    expect(chatSiteUrlForDeployment({ name: "cms", chat_site_base: "   " })).toBeNull();
    expect(chatSiteUrlForDeployment(null)).toBeNull();
  });

  it("trims a declared value rather than emitting a padded href", () => {
    expect(
      chatSiteUrlForDeployment({ name: "cms", chat_site_base: `  ${DECLARED}  ` }),
    ).toBe(DECLARED);
  });

  it("selects the current entry by is_current alone, never by a name", () => {
    // A deployment merely NAMED like a display fallback ("This OKG"), not
    // stamped current, must not hand its chat_site_base to a different
    // deployment's bar as a live link.
    const trap: DeploymentEntry[] = [
      { name: "This OKG", chat_site_base: "https://wrong-deployment.example.org" },
      { name: "cms" },
    ];

    expect(currentDeploymentEntry(trap)).toBeNull();
    expect(chatSiteStateForDeployments(trap, true)).toEqual({
      status: "unresolved",
      href: null,
      deploymentName: null,
    });
  });

  it("distinguishes unresolved, undeclared, and pending", () => {
    const noCurrent: DeploymentEntry[] = [{ name: "a" }, { name: "b" }];
    const currentNoChat: DeploymentEntry[] = [{ name: "a", is_current: true }];

    expect(chatSiteStateForDeployments(noCurrent, true).status).toBe("unresolved");
    expect(chatSiteStateForDeployments(currentNoChat, true).status).toBe("undeclared");
    expect(chatSiteStateForDeployments([], true).status).toBe("unresolved");
    // Not loaded yet: the same claim would be a lie one moment earlier.
    expect(chatSiteStateForDeployments([], false)).toEqual({
      status: "pending", href: null, deploymentName: null,
    });
    expect(chatSiteStateForDeployments(entries, false).status).toBe("pending");
  });
});

// The error body a /viz/api read returns, as the console's search read parses
// it. The old Home and Health pages' failure summaries were built on top of
// this and went with them (PACT console-default-remove-old-views).
interface FailurePayload {
  error?: string;
  detail?: string | Record<string, unknown>;
  reason?: string;
  next_check?: string;
}

export function parseFailurePayload(responseText: string | undefined): FailurePayload {
  if (!responseText) return {};
  try {
    const parsed = JSON.parse(responseText) as FailurePayload;
    const detail = parsed.detail;
    if (detail && typeof detail === "object") {
      return {
        ...parsed,
        error: typeof detail.error === "string" ? detail.error : parsed.error,
        reason: typeof detail.reason === "string" ? detail.reason : parsed.reason,
        next_check: typeof detail.next_check === "string"
          ? detail.next_check
          : parsed.next_check,
      };
    }
    return parsed;
  } catch {
    return {};
  }
}

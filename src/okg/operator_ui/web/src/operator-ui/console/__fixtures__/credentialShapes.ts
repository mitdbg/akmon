// The credential shapes the console's absence guards are written against.
//
// These used to live in `model/configYaml.ts`, beside a composer that drew a
// producer's registry entry as YAML with credentials redacted in place. No
// read returns the recorded entry, so that section is cut (ui/cuts.md) and
// the composer went with it. What the tests still need is the shapes
// themselves, so a guard can assert that no console surface prints a
// credential-shaped key or value at all -- which is the stronger claim, and
// the one to hold until a redacting read exists.
//
// Test support only: nothing the browser runs imports this file.

// A key that names a credential, whatever its value. Matched by SEGMENT, so
// `author` (contains "auth") and `repo_path` are names, not secrets, while
// `api_key`, `accessToken` and `db-password` are. camelCase is split first.
const CREDENTIAL_KEY_SEGMENT = /(^|[^a-z0-9])(credentials?|tokens?|secrets?|passwords?|passwd|pwd|auth|api[_-]?keys?|private[_-]?keys?|access[_-]?keys?|secret[_-]?keys?|client[_-]?secrets?|dsn)([^a-z0-9]|$)/i;

/** Whether a key names a credential. Its value must never be printed. */
export function isCredentialKey(key: string): boolean {
  return CREDENTIAL_KEY_SEGMENT.test(key.replace(/([a-z0-9])([A-Z])/g, "$1_$2"));
}

/** A value that looks like a bare credential: a known prefix (sk-, ghp_,
 *  xoxb-, AKIA...) or 32+ opaque characters. A path is not one: it starts
 *  with `/`, `.` or `~`, or carries `/` without being base64. */
export const CREDENTIAL_VALUE = /^(sk|pk|ghp|gho|xox[abp]|AKIA)[-_A-Za-z0-9]{8,}$|^[A-Za-z0-9+=_-]{32,}$|^(?![./~])[A-Za-z0-9+/]{32,}={0,2}$/;

/** A `key: value` line naming a credential, anywhere in a copied page. The
 *  keys the console's seeded fixtures actually carry are named first. */
export const CREDENTIAL_LINE = /(dsn|credentials?|auth|private[_-]?key|token|secret|password|api[_-]?key)\s*:/i;

/** What a redaction used to leave behind. Nothing writes it now, and the
 *  guards hold that: a page that shows `<redacted>` is showing an entry. */
export const REDACTED = "<redacted>";

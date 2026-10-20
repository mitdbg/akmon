"""Provider registry for OAuth flow handler."""
from okg.substrate.library.oauth.flow import OAuthProvider


GMAIL = OAuthProvider(
    name="gmail",
    auth_url="https://accounts.google.com/o/oauth2/v2/auth",
    token_url="https://oauth2.googleapis.com/token",
    refresh_url="https://oauth2.googleapis.com/token",
    scopes=[
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/gmail.metadata",
    ],
    client_id_env="OKG_GMAIL_CLIENT_ID",
    client_secret_env="OKG_GMAIL_CLIENT_SECRET",
    api_hosts=[".googleapis.com"],
)

GOOGLE_CALENDAR = OAuthProvider(
    name="google_calendar",
    auth_url="https://accounts.google.com/o/oauth2/v2/auth",
    token_url="https://oauth2.googleapis.com/token",
    refresh_url="https://oauth2.googleapis.com/token",
    scopes=["https://www.googleapis.com/auth/calendar.readonly"],
    client_id_env="OKG_GCAL_CLIENT_ID",
    client_secret_env="OKG_GCAL_CLIENT_SECRET",
    api_hosts=[".googleapis.com"],
)

SLACK = OAuthProvider(
    name="slack",
    auth_url="https://slack.com/oauth/v2/authorize",
    token_url="https://slack.com/api/oauth.v2.access",
    refresh_url=None,
    scopes=["channels:history", "users:read", "team:read"],
    client_id_env="OKG_SLACK_CLIENT_ID",
    client_secret_env="OKG_SLACK_CLIENT_SECRET",
    api_hosts=["slack.com", ".slack.com"],
)


MSGRAPH = OAuthProvider(
    name="msgraph",
    # `organizations` endpoint: any Entra work/school tenant. The
    # academic deployment registers ONE tenant-internal Entra app
    # carrying both delegated scopes; outlook_mail_api and
    # outlook_calendar_api share it (per `add-msgraph-sources`).
    auth_url=(
        "https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize"
    ),
    token_url=(
        "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"
    ),
    refresh_url=(
        "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"
    ),
    scopes=[
        "https://graph.microsoft.com/Mail.Read",
        "https://graph.microsoft.com/Calendars.Read",
        "offline_access",
    ],
    client_id_env="OKG_MSGRAPH_CLIENT_ID",
    client_secret_env="OKG_MSGRAPH_CLIENT_SECRET",
    api_hosts=["graph.microsoft.com", ".graph.microsoft.com"],
)


REGISTRY = {
    "gmail": GMAIL,
    "google_calendar": GOOGLE_CALENDAR,
    "slack": SLACK,
    "msgraph": MSGRAPH,
}


def get_provider(name: str) -> OAuthProvider:
    if name not in REGISTRY:
        raise KeyError(
            f"unknown OAuth provider {name!r}; "
            f"known: {sorted(REGISTRY)}"
        )
    return REGISTRY[name]

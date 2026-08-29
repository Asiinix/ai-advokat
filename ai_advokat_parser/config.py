from __future__ import annotations

from dataclasses import dataclass

BASE_URL = "https://prg.kz"
API_BASE_URL = f"{BASE_URL}/mapi"

DEFAULT_LIST_URL = (
    "https://prg.kz/lawyer/documents/"
    "?text="
    "&currentPage=1"
    "&excludeChangingDocuments=false"
    "&onlyFreeDocuments=true"
    "&excludeArchivedDocuments=false"
    "&excludeGovAnswers=false"
    "&onlyKazakshtanLegislation=false"
    "&mode=1"
    "&wordsProximity=0"
    "&wordEnding=1"
    "&documentStatus=0"
    "&documentStatus=2"
    "&documentStatus=1"
)

# The whole legal catalog, free and paid alike. Used by the catalog scan; the
# free-only DEFAULT_LIST_URL above stays the default for every other command.
DEFAULT_ALL_DOCUMENTS_LIST_URL = (
    "https://prg.kz/lawyer/documents/"
    "?text="
    "&currentPage=1"
    "&excludeChangingDocuments=false"
    "&onlyFreeDocuments=false"
    "&excludeArchivedDocuments=false"
    "&excludeGovAnswers=false"
    "&onlyKazakshtanLegislation=false"
    "&mode=1"
    "&wordsProximity=0"
    "&wordEnding=1"
    "&documentStatus=0"
    "&documentStatus=2"
    "&documentStatus=1"
)
ALL_DOCUMENTS_LIST_URL = DEFAULT_ALL_DOCUMENTS_LIST_URL

AUTH_HOST = "auth.zakon.kz"
AUTH_LOGIN_URL = f"https://{AUTH_HOST}/account/login"
AUTH_RETURN_URL = f"{BASE_URL}/"
AUTH_RETURN_APP = "prgWeb"
AUTH_USERNAME_ENV = "AI_ADVOCAT_PRG_USERNAME"
AUTH_PASSWORD_ENV = "AI_ADVOCAT_PRG_PASSWORD"

# PRG.SOT (judicial decisions) is a different application behind the same
# zakon.kz single sign-on: another returnApp, another origin and a separate
# subscription, therefore separate credentials.
SOT_BASE_URL = "https://sb.prg.kz"
SOT_AUTH_RETURN_URL = f"{SOT_BASE_URL}/"
SOT_AUTH_RETURN_APP = "SUDBASEV2"
SOT_USERNAME_ENV = "AI_ADVOCAT_SOT_USERNAME"
SOT_PASSWORD_ENV = "AI_ADVOCAT_SOT_PASSWORD"


@dataclass(frozen=True)
class AuthProfile:
    """One PRG application behind the shared zakon.kz login.

    Everything that differs between PRG.ZANGER and PRG.SOT lives here so that a
    single :class:`~ai_advokat_parser.http_client.SourceClient` implementation
    can serve both without a global switch: two clients in one process keep two
    cookie jars, two sets of credentials and two return origins.
    """

    name: str
    login_url: str
    return_url: str
    return_app: str
    username_env: str
    password_env: str
    auth_host: str = AUTH_HOST


def sot_auth_profile(
    login_url: str | None = None,
    return_url: str | None = None,
) -> AuthProfile:
    """The PRG.SOT profile; the URLs are overridable for tests and staging."""
    return AuthProfile(
        name="prg_sot",
        login_url=login_url or AUTH_LOGIN_URL,
        return_url=return_url or SOT_AUTH_RETURN_URL,
        return_app=SOT_AUTH_RETURN_APP,
        username_env=SOT_USERNAME_ENV,
        password_env=SOT_PASSWORD_ENV,
        auth_host=AUTH_HOST,
    )


# Every credential variable the process knows about. Used by the log redaction
# and by the stub sanitiser, so adding a profile above is enough to protect it.
CREDENTIAL_ENV_NAMES = (
    AUTH_USERNAME_ENV,
    AUTH_PASSWORD_ENV,
    SOT_USERNAME_ENV,
    SOT_PASSWORD_ENV,
    "AI_ADVOCAT_DATABASE_URL",
    "DATABASE_URL",
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

DEFAULT_FORMATS = ("html", "txt")
SUPPORTED_FORMATS = ("html", "txt", "json", "pdf")

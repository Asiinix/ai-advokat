from __future__ import annotations

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

AUTH_HOST = "auth.zakon.kz"
AUTH_LOGIN_URL = f"https://{AUTH_HOST}/account/login"
AUTH_RETURN_URL = f"{BASE_URL}/"
AUTH_RETURN_APP = "prgWeb"
AUTH_USERNAME_ENV = "AI_ADVOCAT_PRG_USERNAME"
AUTH_PASSWORD_ENV = "AI_ADVOCAT_PRG_PASSWORD"

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

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

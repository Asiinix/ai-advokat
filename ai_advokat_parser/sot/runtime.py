"""Wiring for the PRG.SOT commands: storage choice, client, status and probe.

Kept apart from the CLI so the behaviour is testable without argparse, and kept
apart from the ZANGER crawler so neither pipeline can reach the other's state.
"""

from __future__ import annotations

import html
import os
import re
import urllib.parse
from pathlib import Path
from typing import Any, Mapping

from ..config import SOT_PASSWORD_ENV, SOT_USERNAME_ENV
from ..http_client import SourceAuthError, SourceRateLimitError
from .adapter import MAX_WORKERS, SotSource, build_sot_client
from .model import SotDiscoveryError
from .source_config import SotConfigError, SotSourceConfig
from .postgres_store import SotPostgresStore
from .store import DECISION_FORMATS, SotStore

SCRIPT_SRC_RE = re.compile(r"<script\b[^>]*\bsrc\s*=\s*['\"](?P<src>[^'\"]+)['\"]", re.I)


def frontend_asset_paths(client, base_url: str) -> list[str]:
    """Same-origin script paths from the already returned login landing page."""
    landing = client.authenticated_landing
    if landing is None:
        return []
    allowed = urllib.parse.urlparse(base_url)
    found: list[str] = []
    for match in SCRIPT_SRC_RE.finditer(landing.text):
        absolute = urllib.parse.urljoin(landing.url, html.unescape(match.group("src")))
        parsed = urllib.parse.urlparse(absolute)
        if (parsed.scheme.lower(), parsed.netloc.lower()) != (
            allowed.scheme.lower(),
            allowed.netloc.lower(),
        ):
            continue
        path = urllib.parse.urlunparse(("", "", parsed.path, parsed.params, parsed.query, ""))
        if path and path not in found:
            found.append(path)
    return found


def open_store(out_dir: str | Path, env: Mapping[str, str] | None = None):
    """Postgres when the deploy provides a database, SQLite otherwise."""
    source = os.environ if env is None else env
    database_url = source.get("AI_ADVOCAT_DATABASE_URL") or source.get("DATABASE_URL")
    if database_url and source.get("AI_ADVOCAT_DISABLE_POSTGRES") not in {"1", "true", "yes"}:
        return SotPostgresStore(database_url)
    return SotStore(out_dir)


def load_config(overrides: Mapping[str, Any] | None = None, env: Mapping[str, str] | None = None) -> SotSourceConfig:
    return SotSourceConfig.from_env(env=env, overrides=overrides)


def build_source(
    config: SotSourceConfig,
    timeout: float = 30.0,
    retries: int = 3,
    login_url: str | None = None,
) -> SotSource:
    """Validate the contract first, then build the authenticated adapter.

    Validation happens before anything is opened or written, so a deploy with a
    half-captured contract cannot create a scan row or take a lease.
    """
    config.validate()
    client = build_sot_client(config, timeout=timeout, retries=retries, login_url=login_url)
    return SotSource(client, config)


def credentials_state(env: Mapping[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    has_user = bool((source.get(SOT_USERNAME_ENV) or "").strip())
    has_password = bool(source.get(SOT_PASSWORD_ENV) or "")
    if has_user and has_password:
        return "configured"
    if has_user or has_password:
        return f"incomplete: set both {SOT_USERNAME_ENV} and {SOT_PASSWORD_ENV}"
    return "missing"


def print_status(out_dir: str, scan_id: str | None = None, env: Mapping[str, str] | None = None) -> None:
    """Read-only report. Never contacts the source, never needs credentials."""
    config = load_config(env=env)
    missing = config.missing_requirements()
    print("PRG.SOT (судебные акты)")
    print(f"  origin: {config.base_url}")
    print(f"  учетные данные: {credentials_state(env)}")
    print(f"  контракт источника: {'настроен' if not missing else 'не настроен'}")
    if missing:
        print("    не заданы: " + ", ".join(missing))
        print("    сначала снимите живые запросы в подписанной сессии, см. README > PRG.SOT")
    else:
        try:
            config.validate()
            print(f"    search: {config.search_method} {config.search_url_template}")
            print(f"    decision: {config.decision_method} {config.decision_url_template}")
            print(f"    page_size: {config.page_size}, поля: {', '.join(sorted(config.field_map)) or '-'}")
        except SotConfigError as exc:
            print(f"    контракт отклонен: {exc}")

    store = open_store(out_dir, env=env)
    try:
        print(f"  хранилище: {store.storage_label}")
        stats = store.decision_stats()
        print("  решения:")
        if stats:
            for status, count in sorted(stats.items()):
                print(f"    {status}: {count}")
        else:
            print("    пока пусто")
        if scan_id is None:
            return
        state = store.get_scan(scan_id)
        if state is None:
            print(f"  скан {scan_id}: не найден")
            return
        print(f"  скан: {state.scan_id}")
        print(f"    провенанс: {state.source_system}/{state.corpus_type}")
        print(f"    фаза: {state.phase}")
        print(f"    всего решений: {state.total_decisions if state.total_decisions is not None else '-'}")
        print(f"    страниц: {state.pages_done}/{state.total_pages if state.total_pages is not None else '-'}")
        print(f"    следующая страница: {state.next_page}, курсор: {state.next_cursor or '-'}")
        print(f"    учтено: {state.decisions_seen}, поставлено в очередь: {state.decisions_enqueued}")
        print(f"    начат: {state.started_at}, обновлен: {state.updated_at}")
        if state.completed_at:
            print(f"    завершен: {state.completed_at}")
        if state.error:
            print(f"    примечание: {state.error}")
        if state.rate_limit_note:
            print(f"    лимит: {state.rate_limit_note}")
        print("    итоги решений:")
        outcomes = store.scan_stats(scan_id)
        if outcomes:
            for outcome, count in sorted(outcomes.items()):
                print(f"      {outcome}: {count}")
        else:
            print("      пока пусто")
    finally:
        store.close()


def probe_auth(
    timeout: float = 30.0,
    retries: int = 3,
    page: int | None = None,
    env: Mapping[str, str] | None = None,
    login_url: str | None = None,
    inspect_first_decision: bool = False,
) -> int:
    """Verify the PRG.SOT login and, if asked, read exactly one search page.

    This is the live-validation gate: it is the only SOT command that talks to
    the source without writing anything, so a captured contract can be confirmed
    before a scan is allowed to create state.
    """
    config = load_config(env=env)
    print(f"[sot] origin: {config.base_url}")
    print(f"[sot] учетные данные: {credentials_state(env)}")
    missing = config.missing_requirements()
    if missing:
        print("[sot] контракт источника не настроен: " + ", ".join(missing))

    client = build_sot_client(config, timeout=timeout, retries=retries, login_url=login_url)
    if not client.uses_authentication:
        print(f"[sot] вход невозможен: задайте {SOT_USERNAME_ENV} и {SOT_PASSWORD_ENV}")
        return 2
    try:
        client.authenticate()
    except SourceAuthError as exc:
        print(f"[sot] вход отклонен: {exc}")
        return 2
    print(f"[sot] вход выполнен, returnApp={client.auth.return_app}, returnUrl={client.auth.return_url}")
    assets = frontend_asset_paths(client, config.base_url)
    if assets:
        print("[sot] frontend assets (same-origin):")
        for asset in assets[:20]:
            print(f"[sot]   {asset}")
    else:
        print("[sot] frontend assets: на стартовой странице не найдены")

    if page is None:
        print("[sot] проверка страницы поиска пропущена (передайте --page, когда контракт снят)")
        # Authentication is the only promised check in this mode. A missing
        # search contract is reported above, but it must not turn a successful
        # SSO/subscription check into a failed Railway deployment.
        return 0
    if missing:
        print("[sot] страница поиска не запрошена: контракт источника не настроен")
        return 1

    source = SotSource(client, config)
    try:
        result = source.fetch_search_page(page)
    except SourceRateLimitError as exc:
        print(f"[sot] лимит источника: {exc.rate_limit.describe()}")
        return 3
    except (SotConfigError, SotDiscoveryError) as exc:
        print(f"[sot] ответ поиска не совпал с контрактом: {exc}")
        return 1
    print(
        f"[sot] страница {page}: элементов {result.raw_count}, распознано {len(result.refs)}, "
        f"total {result.total if result.total is not None else '-'}, "
        f"cursor {'есть' if result.next_cursor else 'нет'}"
    )
    print(f"[sot] поля ответа: {', '.join(result.payload_keys) or '-'}")
    print(f"[sot] поля карточки: {', '.join(result.item_keys) or '-'}")
    print(f"[sot] квота: {client.last_rate_limit.describe()}")
    for ref in result.refs[:3]:
        fields = ", ".join(f"{key}={value}" for key, value in sorted(ref.metadata.items()) if key != "parties")
        print(f"[sot]   {ref.decision_key} {fields}")
    if inspect_first_decision:
        if not result.refs:
            print("[sot] схема первой карточки пропущена: страница пуста")
            return 0
        try:
            schema = source.inspect_decision_schema(result.refs[0])
        except SourceRateLimitError as exc:
            print(f"[sot] лимит при проверке карточки: {exc.rate_limit.describe()}")
            return 3
        print(f"[sot] поля решения: {', '.join(schema.root_keys) or '-'}")
        for path, keys in schema.nested_keys:
            print(f"[sot] вложенная схема {path}: {', '.join(keys) or '-'}")
        if schema.text_candidates:
            for path, length in schema.text_candidates:
                print(f"[sot] кандидат текста {path}: {length} символов")
        else:
            print("[sot] кандидаты длинного текста: не найдены")
    return 0


def resolve_workers(requested: int) -> int:
    return max(1, min(int(requested), MAX_WORKERS))


__all__ = [
    "DECISION_FORMATS",
    "build_source",
    "credentials_state",
    "frontend_asset_paths",
    "load_config",
    "open_store",
    "print_status",
    "probe_auth",
    "resolve_workers",
]

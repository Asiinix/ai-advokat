from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

from .config import (
    DEFAULT_ALL_DOCUMENTS_LIST_URL,
    DEFAULT_FORMATS,
    DEFAULT_LIST_URL,
    SUPPORTED_FORMATS,
)
from .crawler import Crawler
from .listing import DocumentRef, fetch_listing_page, load_document_refs_from_file
from .http_client import SourceAuthError, SourceClient, SourceRateLimitError
from .sot import runtime as sot_runtime
from .sot.scan import SotScanner
from .sot.source_config import SotConfigError
from .utils import parse_formats


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Value must be >= 1")
    return number


def non_negative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Value must be >= 0")
    return number


def non_negative_float(value: str) -> float:
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Value must be >= 0")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ai-advokat",
        description="Парсер AI Advokat для юридических документов.",
    )
    parser.add_argument("--out", default="data", help="Папка для результатов и state.sqlite3.")
    parser.add_argument(
        "--formats",
        default=",".join(DEFAULT_FORMATS),
        help=f"Форматы через запятую: {', '.join(SUPPORTED_FORMATS)}.",
    )
    parser.add_argument("--delay", type=float, default=0.8, help="Пауза между запросами, секунд.")
    parser.add_argument("--workers", type=positive_int, default=1, help="Параллельные загрузчики документов.")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout, секунд.")
    parser.add_argument("--retries", type=positive_int, default=3, help="Количество повторов запроса.")
    parser.add_argument("--product", default="lawyer", help="Product API источника, по умолчанию lawyer.")
    parser.add_argument("--include-paid", action="store_true", help="Не пропускать документы без флага free.")
    parser.add_argument("--force", action="store_true", help="Перезаписать уже готовые документы.")
    parser.add_argument(
        "--enqueue-only",
        action="store_true",
        help="Только поставить документы в очередь, не скачивать их сразу.",
    )
    parser.add_argument(
        "--follow-links-depth",
        type=non_negative_int,
        default=0,
        help="Докачивать документы из ссылок: 0 выключено, 1 ссылки выбранных, 2 ссылки из ссылок.",
    )
    parser.add_argument(
        "--max-linked-docs",
        type=non_negative_int,
        help="Ограничить число документов, добавленных из ссылок.",
    )

    subparsers = parser.add_subparsers(dest="command")

    range_parser = subparsers.add_parser("range", help="Скачать документы из диапазона страниц списка.")
    range_parser.add_argument("--from-page", type=positive_int, required=True)
    range_parser.add_argument("--to-page", type=positive_int, required=True)
    range_parser.add_argument("--list-url", default=DEFAULT_LIST_URL, help="URL списка с нужными фильтрами.")
    range_parser.add_argument("--max-docs", type=positive_int, help="Ограничить число документов.")

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="В одном процессе складывать страницы в очередь и параллельно скачивать документы.",
    )
    pipeline_parser.add_argument("--from-page", type=positive_int, required=True)
    pipeline_parser.add_argument("--to-page", type=positive_int, required=True)
    pipeline_parser.add_argument("--list-url", default=DEFAULT_LIST_URL, help="URL списка с нужными фильтрами.")
    pipeline_parser.add_argument("--max-docs", type=positive_int, help="Ограничить число документов со страниц.")
    pipeline_parser.add_argument(
        "--idle-seconds",
        type=non_negative_float,
        default=60,
        help="Сколько ждать пустую очередь после завершения обхода страниц.",
    )
    pipeline_parser.add_argument(
        "--lease-seconds",
        type=positive_int,
        default=1800,
        help="Через сколько секунд возвращать зависшие processing обратно в queued.",
    )
    pipeline_parser.add_argument(
        "--poll-interval",
        type=non_negative_float,
        default=5.0,
        help="Пауза между проверками пустой очереди.",
    )

    file_parser = subparsers.add_parser("file", help="Скачать документы из файла с doc_id или URL.")
    file_parser.add_argument("--input", required=True, help="Файл: один doc_id или URL на строку.")

    doc_parser = subparsers.add_parser("doc", help="Скачать один или несколько doc_id.")
    doc_parser.add_argument("doc_ids", nargs="+")

    list_parser = subparsers.add_parser("list", help="Показать doc_id на странице списка без скачивания документов.")
    list_parser.add_argument("--page", type=positive_int, required=True)
    list_parser.add_argument("--list-url", default=DEFAULT_LIST_URL)

    subparsers.add_parser("status", help="Показать статистику state.sqlite3.")
    subparsers.add_parser("retry", help="Повторить документы со статусом failed.")
    work_parser = subparsers.add_parser("work", help="Обрабатывать очередь документов из базы.")
    work_parser.add_argument("--limit", type=non_negative_int, default=0, help="Сколько документов обработать; 0 = без лимита.")
    work_parser.add_argument(
        "--idle-seconds",
        type=non_negative_float,
        default=0,
        help="Сколько ждать новые задачи после опустошения очереди; 0 = выйти сразу.",
    )
    work_parser.add_argument(
        "--lease-seconds",
        type=positive_int,
        default=1800,
        help="Через сколько секунд возвращать зависшие processing обратно в queued.",
    )
    work_parser.add_argument(
        "--poll-interval",
        type=non_negative_float,
        default=5.0,
        help="Пауза между проверками пустой очереди.",
    )
    enrich_parser = subparsers.add_parser(
        "enrich-failed-titles",
        help="Добрать title у failed-документов без скачивания полного текста.",
    )
    enrich_parser.add_argument(
        "--limit",
        type=non_negative_int,
        default=0,
        help="Сколько failed-документов проверить; 0 = без лимита.",
    )
    enrich_parser.add_argument(
        "--lease-seconds",
        type=positive_int,
        default=86400,
        help="Через сколько секунд повторять failed-title задачу, если прошлый запуск не добрал title.",
    )

    catalog_parser = subparsers.add_parser(
        "catalog-scan",
        help="Полный обход каталога источника с возобновлением по --scan-id.",
    )
    catalog_parser.add_argument("--scan-id", required=True, help="Идентификатор скана; повтор команды продолжает его.")
    catalog_parser.add_argument(
        "--list-url",
        default=DEFAULT_ALL_DOCUMENTS_LIST_URL,
        help="URL списка; по умолчанию весь каталог, включая платные документы.",
    )
    catalog_parser.add_argument("--max-pages", type=positive_int, help="Ограничить число страниц за запуск (smoke-run).")
    catalog_parser.add_argument("--max-docs", type=positive_int, help="Ограничить число документов за запуск (smoke-run).")
    catalog_parser.add_argument(
        "--lease-seconds",
        type=positive_int,
        default=1800,
        help="Через сколько секунд возвращать зависшие processing обратно в queued.",
    )
    catalog_parser.add_argument(
        "--poll-interval",
        type=non_negative_float,
        default=5.0,
        help="Пауза между проверками пустой очереди.",
    )

    catalog_status_parser = subparsers.add_parser("catalog-status", help="Показать состояние скана каталога.")
    catalog_status_parser.add_argument("--scan-id", required=True)

    catalog_stubs_parser = subparsers.add_parser(
        "catalog-stubs",
        help="Выгрузить JSON-заглушки недоступных документов скана.",
    )
    catalog_stubs_parser.add_argument("--scan-id", required=True)
    catalog_stubs_parser.add_argument(
        "--output",
        default="-",
        help="Путь к файлу или - для stdout.",
    )

    sot_status_parser = subparsers.add_parser(
        "sot-status",
        help="Показать состояние корпуса судебных актов PRG.SOT (без обращений к источнику).",
    )
    sot_status_parser.add_argument("--scan-id", help="Показать и состояние конкретного скана.")

    sot_probe_parser = subparsers.add_parser(
        "sot-probe-auth",
        help="Проверить вход в PRG.SOT и, при --page, один запрос поиска. Ничего не записывает.",
    )
    sot_probe_parser.add_argument(
        "--page",
        type=positive_int,
        help="Прочитать одну страницу поиска для проверки снятого контракта.",
    )
    sot_probe_parser.add_argument(
        "--inspect-first-decision",
        action="store_true",
        help="После --page прочитать первую карточку и вывести только схему полей, без значений.",
    )

    sot_scan_parser = subparsers.add_parser(
        "sot-scan",
        help="Обход корпуса PRG.SOT с возобновлением по --scan-id.",
    )
    sot_scan_parser.add_argument("--scan-id", required=True, help="Идентификатор скана; повтор команды продолжает его.")
    sot_scan_parser.add_argument("--max-pages", type=positive_int, help="Ограничить число страниц за запуск.")
    sot_scan_parser.add_argument("--max-decisions", type=positive_int, help="Ограничить число решений за запуск.")
    sot_scan_parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Явно вернуть в очередь failed/inaccessible/not_found этого скана.",
    )
    sot_scan_parser.add_argument(
        "--lease-seconds",
        type=positive_int,
        default=1800,
        help="Через сколько секунд возвращать зависшие processing обратно в queued.",
    )
    sot_scan_parser.add_argument(
        "--poll-interval",
        type=non_negative_float,
        default=5.0,
        help="Пауза между проверками пустой очереди.",
    )
    sot_scan_parser.add_argument(
        "--max-pause-seconds",
        type=non_negative_float,
        default=300.0,
        help="Максимальная пауза по Retry-After; более долгий лимит останавливает запуск.",
    )
    sot_scan_parser.add_argument(
        "--search-url-template",
        help="Шаблон поиска, если он не задан переменной окружения.",
    )
    sot_scan_parser.add_argument("--search-method", help="GET или POST для запроса поиска.")
    sot_scan_parser.add_argument("--search-body-template", help="JSON-шаблон тела запроса поиска.")
    sot_scan_parser.add_argument("--decision-url-template", help="Шаблон запроса одного решения.")
    sot_scan_parser.add_argument("--decision-method", help="GET или POST для запроса решения.")
    sot_scan_parser.add_argument("--decision-body-template", help="JSON-шаблон тела запроса решения.")
    sot_scan_parser.add_argument("--results-path", help="Путь к списку результатов в ответе поиска.")
    sot_scan_parser.add_argument("--total-path", help="Путь к общему числу решений в ответе поиска.")
    sot_scan_parser.add_argument("--next-cursor-path", help="Путь к курсору следующей страницы.")
    sot_scan_parser.add_argument("--id-path", help="Путь к идентификатору решения внутри элемента списка.")
    sot_scan_parser.add_argument("--text-path", help="Путь к тексту решения в ответе документа.")
    sot_scan_parser.add_argument("--field-map", help="JSON: поле -> путь для метаданных суда.")
    sot_scan_parser.add_argument("--page-size", type=positive_int, help="Размер страницы поиска.")
    sot_scan_parser.add_argument("--base-url", help="Origin PRG.SOT; шаблоны обязаны указывать на него.")

    sot_stubs_parser = subparsers.add_parser(
        "sot-stubs",
        help="Выгрузить JSON-заглушки недоступных решений скана PRG.SOT.",
    )
    sot_stubs_parser.add_argument("--scan-id", required=True)
    sot_stubs_parser.add_argument("--output", default="-", help="Путь к файлу или - для stdout.")

    subparsers.add_parser("menu", help="Открыть интерактивную cmd-панель.")
    return parser


def make_crawler(args: argparse.Namespace) -> Crawler:
    # The catalog scan is about the whole catalog, so paid documents are always
    # attempted; inaccessible ones end up as stubs instead of being skipped.
    include_paid = args.include_paid or args.command == "catalog-scan"
    return Crawler(
        out_dir=args.out,
        formats=parse_formats(args.formats),
        product=args.product,
        only_free=not include_paid,
        delay=args.delay,
        timeout=args.timeout,
        retries=args.retries,
        workers=args.workers,
        force=args.force,
        follow_links_depth=args.follow_links_depth,
        max_linked_docs=args.max_linked_docs,
    )


def print_status(out_dir: str) -> None:
    crawler = Crawler(out_dir=out_dir)
    try:
        doc_stats = crawler.store.stats()
        page_stats = crawler.store.listing_stats()
        print("Документы:")
        if doc_stats:
            for status, count in sorted(doc_stats.items()):
                print(f"  {status}: {count}")
        else:
            print("  пока пусто")
        print("Страницы списка:")
        if page_stats:
            for status, count in sorted(page_stats.items()):
                print(f"  {status}: {count}")
        else:
            print("  пока пусто")
        if getattr(crawler.store, "storage_label", "SQLite") == "Postgres":
            print("Storage: Postgres")
        else:
            print(f"State: {Path(out_dir) / 'state.sqlite3'}")
    finally:
        crawler.close()


def print_catalog_status(out_dir: str, scan_id: str) -> None:
    crawler = Crawler(out_dir=out_dir)
    try:
        state = crawler.store.get_catalog_scan(scan_id)
        if state is None:
            print(f"Скан {scan_id}: не найден")
            return
        print(f"Скан: {state.scan_id}")
        print(f"  фаза: {state.phase}")
        print(f"  список: {state.list_url}")
        print(f"  форматы: {', '.join(state.formats) or '-'}")
        print(f"  всего документов: {state.total_documents if state.total_documents is not None else '-'}")
        print(f"  страниц: {state.pages_done}/{state.total_pages if state.total_pages is not None else '-'}")
        print(f"  следующая страница: {state.next_page}")
        print(f"  учтено документов: {state.docs_seen}, поставлено в очередь: {state.docs_enqueued}")
        print(f"  начат: {state.started_at}, обновлен: {state.updated_at}")
        if state.completed_at:
            print(f"  завершен: {state.completed_at}")
        if state.error:
            print(f"  примечание: {state.error}")
        stats = crawler.store.catalog_scan_stats(scan_id)
        print("  итоги документов:")
        if stats:
            for outcome, count in sorted(stats.items()):
                print(f"    {outcome}: {count}")
        else:
            print("    пока пусто")
    finally:
        crawler.close()


def dump_catalog_stubs(out_dir: str, scan_id: str, output: str) -> None:
    # stdout has to stay pure JSON so that `catalog-stubs > file.json` works.
    with contextlib.redirect_stdout(sys.stderr):
        crawler = Crawler(out_dir=out_dir)
    try:
        state = crawler.store.get_catalog_scan(scan_id)
        payload = {
            "scan_id": scan_id,
            "phase": state.phase if state else None,
            "stats": crawler.store.catalog_scan_stats(scan_id),
            "documents": crawler.store.catalog_scan_stubs(scan_id),
        }
    finally:
        crawler.close()
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if output == "-":
        print(rendered)
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{rendered}\n", encoding="utf-8")
    print(f"Заглушки скана {scan_id}: {len(payload['documents'])} -> {path}", file=sys.stderr)


SOT_COMMANDS = ("sot-status", "sot-probe-auth", "sot-scan", "sot-stubs")

SOT_CONFIG_ARGS = (
    "search_url_template",
    "search_method",
    "search_body_template",
    "decision_url_template",
    "decision_method",
    "decision_body_template",
    "results_path",
    "total_path",
    "next_cursor_path",
    "id_path",
    "text_path",
    "field_map",
    "page_size",
    "base_url",
)


def sot_config_overrides(args: argparse.Namespace) -> dict[str, object]:
    """CLI flags win over the environment for the source contract."""
    return {name: getattr(args, name, None) for name in SOT_CONFIG_ARGS}


def run_sot_scan(args: argparse.Namespace, *, wait_when_exhausted: bool = False) -> None:
    """Validate the contract, then run one resumable pass over the corpus."""
    config = sot_runtime.load_config(sot_config_overrides(args))
    # Everything that could be wrong with the contract is rejected here, before
    # a store is opened and before a single row is written.
    source = sot_runtime.build_source(
        config,
        timeout=args.timeout,
        retries=args.retries,
        wait_when_exhausted=wait_when_exhausted,
    )
    if not source.client.authenticate():
        raise SotConfigError(
            "PRG.SOT scan needs a subscribed session: set "
            f"{sot_runtime.SOT_USERNAME_ENV} and {sot_runtime.SOT_PASSWORD_ENV}."
        )
    print("[auth] PRG.SOT: вход выполнен")
    store = sot_runtime.open_store(args.out)
    try:
        scanner = SotScanner(
            store,
            source,
            delay=args.delay,
            workers=sot_runtime.resolve_workers(args.workers),
            max_pause_seconds=args.max_pause_seconds,
        )
        scanner.run(
            scan_id=args.scan_id,
            max_pages=args.max_pages,
            max_decisions=args.max_decisions,
            retry_failed=args.retry_failed,
            lease_seconds=args.lease_seconds,
            poll_interval=args.poll_interval,
        )
    finally:
        store.close()


def dump_sot_stubs(out_dir: str, scan_id: str, output: str) -> None:
    # stdout has to stay pure JSON so that `sot-stubs > file.json` works.
    with contextlib.redirect_stdout(sys.stderr):
        store = sot_runtime.open_store(out_dir)
    try:
        state = store.get_scan(scan_id)
        payload = {
            "scan_id": scan_id,
            "source_system": state.source_system if state else "prg_sot",
            "corpus_type": state.corpus_type if state else "judicial_decision",
            "phase": state.phase if state else None,
            "stats": store.scan_stats(scan_id),
            "decisions": store.scan_stubs(scan_id),
        }
    finally:
        store.close()
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if output == "-":
        print(rendered)
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{rendered}\n", encoding="utf-8")
    print(f"Заглушки скана {scan_id}: {len(payload['decisions'])} -> {path}", file=sys.stderr)


def run_sot_command(args: argparse.Namespace, *, wait_when_exhausted: bool = False) -> None:
    if args.command == "sot-status":
        sot_runtime.print_status(args.out, getattr(args, "scan_id", None))
        return
    if args.command == "sot-probe-auth":
        code = sot_runtime.probe_auth(
            timeout=args.timeout,
            retries=args.retries,
            page=args.page,
            inspect_first_decision=args.inspect_first_decision,
        )
        if code:
            raise SystemExit(code)
        return
    if args.command == "sot-stubs":
        dump_sot_stubs(args.out, args.scan_id, args.output)
        return
    run_sot_scan(args, wait_when_exhausted=wait_when_exhausted)


def run_args(args: argparse.Namespace, *, wait_when_exhausted: bool = False) -> None:
    if args.command in {None, "menu"}:
        run_menu(default_out=args.out)
        return

    if args.command == "status":
        print_status(args.out)
        return

    if args.command == "catalog-status":
        print_catalog_status(args.out, args.scan_id)
        return

    if args.command == "catalog-stubs":
        dump_catalog_stubs(args.out, args.scan_id, args.output)
        return

    if args.command in SOT_COMMANDS:
        run_sot_command(args, wait_when_exhausted=wait_when_exhausted)
        return

    if args.command == "catalog-scan" and args.follow_links_depth > 0:
        raise ValueError(
            "catalog-scan does not follow document links: run it with --follow-links-depth 0."
        )

    if args.command == "list":
        listing = fetch_listing_page(SourceClient(timeout=args.timeout, retries=args.retries), args.page, args.list_url)
        print(f"URL: {listing.url}")
        print(f"Документов на странице: {len(listing.documents)}")
        if listing.total:
            print(f"Всего: {listing.total}, страниц: {listing.total_pages}")
        for item in listing.documents:
            title = f" - {item.title}" if item.title else ""
            print(f"{item.doc_id}{title}")
        return

    crawler = make_crawler(args)
    try:
        local_enqueue_only = args.command in {"file", "doc"} and args.enqueue_only
        if not local_enqueue_only and crawler.client.authenticate():
            print("[auth] PRG: вход выполнен")
        if args.command == "range":
            crawler.crawl_range(
                from_page=args.from_page,
                to_page=args.to_page,
                list_url=args.list_url,
                max_docs=args.max_docs,
                enqueue_only=args.enqueue_only,
            )
        elif args.command == "pipeline":
            crawler.crawl_range_pipeline(
                from_page=args.from_page,
                to_page=args.to_page,
                list_url=args.list_url,
                max_docs=args.max_docs,
                idle_seconds=args.idle_seconds,
                lease_seconds=args.lease_seconds,
                poll_interval=args.poll_interval,
            )
        elif args.command == "file":
            if args.enqueue_only:
                file_refs = load_document_refs_from_file(args.input)
                count = crawler.enqueue_refs(file_refs)
                print(f"[queue] из файла поставлено в очередь: {count}/{len(file_refs)}")
            else:
                crawler.crawl_file(args.input)
        elif args.command == "doc":
            if args.enqueue_only:
                doc_refs = [DocumentRef(doc_id=str(doc_id)) for doc_id in args.doc_ids]
                count = crawler.enqueue_refs(doc_refs)
                print(f"[queue] doc_id поставлено в очередь: {count}/{len(args.doc_ids)}")
            else:
                crawler.crawl_doc_ids(args.doc_ids)
        elif args.command == "retry":
            crawler.retry_failed()
        elif args.command == "work":
            crawler.process_queue(
                limit=args.limit or None,
                idle_seconds=args.idle_seconds,
                lease_seconds=args.lease_seconds,
                poll_interval=args.poll_interval,
            )
        elif args.command == "catalog-scan":
            crawler.run_catalog_scan(
                scan_id=args.scan_id,
                list_url=args.list_url,
                max_pages=args.max_pages,
                max_docs=args.max_docs,
                lease_seconds=args.lease_seconds,
                poll_interval=args.poll_interval,
            )
        elif args.command == "enrich-failed-titles":
            crawler.enrich_failed_titles(
                limit=args.limit or None,
                lease_seconds=args.lease_seconds,
            )
        else:
            raise SystemExit(f"Unknown command: {args.command}")
    finally:
        crawler.close()


def ask(prompt: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default is not None else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value if value else (default or "")


def ask_formats(default: str = ",".join(DEFAULT_FORMATS)) -> tuple[str, ...]:
    while True:
        raw = ask(f"Форматы ({', '.join(SUPPORTED_FORMATS)})", default)
        try:
            return parse_formats(raw)
        except ValueError as exc:
            print(exc)


def ask_int(prompt: str, default: int) -> int:
    while True:
        raw = ask(prompt, str(default))
        try:
            value = int(raw)
            if value < 1:
                raise ValueError
            return value
        except ValueError:
            print("Введите число >= 1.")


def ask_float(prompt: str, default: float) -> float:
    while True:
        raw = ask(prompt, str(default))
        try:
            value = float(raw)
            if value < 0:
                raise ValueError
            return value
        except ValueError:
            print("Введите число >= 0.")


def make_menu_crawler(
    out_dir: str,
    formats: tuple[str, ...],
    delay: float,
    workers: int,
    force: bool,
    follow_links_depth: int,
    max_linked_docs: int | None,
) -> Crawler:
    crawler = Crawler(
        out_dir=out_dir,
        formats=formats,
        delay=delay,
        workers=workers,
        force=force,
        only_free=True,
        follow_links_depth=follow_links_depth,
        max_linked_docs=max_linked_docs,
    )
    try:
        if crawler.client.authenticate():
            print("[auth] PRG: вход выполнен")
        return crawler
    except Exception:
        crawler.close()
        raise


def run_menu(default_out: str = "data") -> None:
    out_dir = default_out
    formats = DEFAULT_FORMATS
    delay = 0.8
    workers = 1
    force = False
    follow_links_depth = 0
    max_linked_docs: int | None = None
    list_url = DEFAULT_LIST_URL

    while True:
        print("\nAI Advokat Parser")
        print("================")
        print(f"Папка: {out_dir}")
        print(f"Форматы: {', '.join(formats)}")
        print(
            f"Пауза: {delay}s | Workers: {workers} | "
            f"Force: {'да' if force else 'нет'} | Links depth: {follow_links_depth} | "
            f"Max linked: {max_linked_docs if max_linked_docs is not None else 'нет'}"
        )
        print("1. Скачать диапазон страниц списка")
        print("2. Скачать документы из файла doc_id/URL")
        print("3. Скачать один doc_id")
        print("4. Показать doc_id на странице списка")
        print("5. Статус")
        print("6. Повторить failed")
        print("7. Настройки")
        print("0. Выход")

        choice = ask("Выбор", "1")
        try:
            if choice == "1":
                from_page = ask_int("С какой страницы", 1)
                to_page = ask_int("По какую страницу", from_page)
                max_docs_raw = ask("Ограничить число документов? пусто = без лимита", "")
                max_docs = int(max_docs_raw) if max_docs_raw else None
                crawler = make_menu_crawler(
                    out_dir, formats, delay, workers, force, follow_links_depth, max_linked_docs
                )
                try:
                    crawler.crawl_range(from_page, to_page, list_url=list_url, max_docs=max_docs)
                finally:
                    crawler.close()
            elif choice == "2":
                path = ask("Путь к файлу")
                crawler = make_menu_crawler(
                    out_dir, formats, delay, workers, force, follow_links_depth, max_linked_docs
                )
                try:
                    crawler.crawl_file(path)
                finally:
                    crawler.close()
            elif choice == "3":
                doc_id = ask("doc_id")
                crawler = make_menu_crawler(
                    out_dir, formats, delay, workers, force, follow_links_depth, max_linked_docs
                )
                try:
                    crawler.crawl_doc_ids([doc_id])
                finally:
                    crawler.close()
            elif choice == "4":
                page = ask_int("Страница списка", 1)
                listing = fetch_listing_page(SourceClient(), page=page, list_url=list_url)
                print(f"Найдено: {len(listing.documents)}")
                if listing.total:
                    print(f"Всего: {listing.total}, страниц: {listing.total_pages}")
                for ref in listing.documents:
                    print(f"{ref.doc_id} - {ref.title}")
            elif choice == "5":
                print_status(out_dir)
            elif choice == "6":
                crawler = make_menu_crawler(
                    out_dir,
                    formats,
                    delay,
                    workers,
                    force=True,
                    follow_links_depth=follow_links_depth,
                    max_linked_docs=max_linked_docs,
                )
                try:
                    crawler.retry_failed()
                finally:
                    crawler.close()
            elif choice == "7":
                out_dir = ask("Папка для результатов", out_dir)
                formats = ask_formats(",".join(formats))
                delay = ask_float("Пауза между запросами", delay)
                workers = ask_int("Workers", workers)
                follow_links_depth = int(
                    ask("Глубина докачки ссылок: 0 нет, 1 ссылки документа, 2 ссылки из ссылок", str(follow_links_depth))
                )
                max_linked_raw = ask("Максимум документов из ссылок? пусто = без лимита", "")
                max_linked_docs = int(max_linked_raw) if max_linked_raw else None
                force = ask("Перезаписывать готовые? yes/no", "no").lower() in {"y", "yes", "да", "д"}
                custom = ask("URL списка с фильтрами? пусто = текущий", "")
                if custom:
                    list_url = custom
            elif choice == "0":
                return
            else:
                print("Неизвестный пункт.")
        except KeyboardInterrupt:
            print("\nОстановлено пользователем.")
        except Exception as exc:
            print(f"Ошибка: {exc}")


def main(
    argv: list[str] | None = None,
    *,
    propagate_source_errors: bool = False,
    wait_when_exhausted: bool = False,
) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        run_args(args, wait_when_exhausted=wait_when_exhausted)
    except (SourceAuthError, SourceRateLimitError) as exc:
        # The interactive CLI keeps its concise argparse-style error. The
        # Railway supervisor, however, needs the concrete exception type to
        # distinguish temporary quota/egress conditions from bad credentials.
        if propagate_source_errors:
            raise
        parser.error(str(exc))
    except (SotConfigError, ValueError) as exc:
        parser.error(str(exc))

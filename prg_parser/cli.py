from __future__ import annotations

import argparse
from pathlib import Path

from .config import DEFAULT_FORMATS, DEFAULT_LIST_URL, SUPPORTED_FORMATS
from .crawler import Crawler
from .listing import DocumentRef, fetch_listing_page, load_document_refs_from_file
from .http_client import PRGClient
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
        prog="prg-parser",
        description="Парсер бесплатных документов PRG.ZANGER.",
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
    parser.add_argument("--product", default="lawyer", help="Product для API PRG, по умолчанию lawyer.")
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
    subparsers.add_parser("menu", help="Открыть интерактивную cmd-панель.")
    return parser


def make_crawler(args: argparse.Namespace) -> Crawler:
    return Crawler(
        out_dir=args.out,
        formats=parse_formats(args.formats),
        product=args.product,
        only_free=not args.include_paid,
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


def run_args(args: argparse.Namespace) -> None:
    if args.command in {None, "menu"}:
        run_menu(default_out=args.out)
        return

    if args.command == "status":
        print_status(args.out)
        return

    if args.command == "list":
        listing = fetch_listing_page(PRGClient(timeout=args.timeout, retries=args.retries), args.page, args.list_url)
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
    return Crawler(
        out_dir=out_dir,
        formats=formats,
        delay=delay,
        workers=workers,
        force=force,
        only_free=True,
        follow_links_depth=follow_links_depth,
        max_linked_docs=max_linked_docs,
    )


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
        print("\nPRG Parser CMD")
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
                listing = fetch_listing_page(PRGClient(), page=page, list_url=list_url)
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


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        run_args(args)
    except ValueError as exc:
        parser.error(str(exc))

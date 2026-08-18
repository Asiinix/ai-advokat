from __future__ import annotations

from .settings import Settings


def main() -> None:
    settings = Settings.from_env()
    if settings.mode == "indexer":
        from .indexer import run_indexer

        run_indexer(settings)
        return

    from .mcp_server import run_mcp

    run_mcp()


if __name__ == "__main__":
    main()


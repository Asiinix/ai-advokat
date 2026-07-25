from __future__ import annotations

import os
import shlex
import time

from .cli import main as cli_main


def sleep_forever() -> None:
    print("[railway] PRG parser container is ready.")
    print("[railway] Set PRG_COMMAND to run a parser command on deploy.")
    print("[railway] Example: --out /tmp/prg-data --formats html,txt,json doc 35502996")
    while True:
        time.sleep(3600)


def main() -> None:
    command = os.environ.get("PRG_COMMAND", "").strip()
    if not command:
        sleep_forever()
        return

    print(f"[railway] running: python -m prg_parser {command}")
    cli_main(shlex.split(command))
    if os.environ.get("PRG_EXIT_AFTER_RUN", "").lower() in {"1", "true", "yes"}:
        return
    sleep_forever()


if __name__ == "__main__":
    main()

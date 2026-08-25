from __future__ import annotations

import os
import shlex
import time

from .cli import main as cli_main
from .config import AUTH_PASSWORD_ENV, AUTH_USERNAME_ENV


def redact_secrets(text: str, env: dict[str, str] | None = None) -> str:
    """Mask credential values that leaked into a command line before logging it."""
    source = os.environ if env is None else env
    for name in (AUTH_USERNAME_ENV, AUTH_PASSWORD_ENV):
        raw_value = source.get(name) or ""
        for value in {raw_value, raw_value.strip()}:
            if value and value in text:
                text = text.replace(value, "***")
    return text


def auth_mode(env: dict[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    configured = []
    if (source.get(AUTH_USERNAME_ENV) or "").strip():
        configured.append(AUTH_USERNAME_ENV)
    if source.get(AUTH_PASSWORD_ENV) or "":
        configured.append(AUTH_PASSWORD_ENV)
    if len(configured) == 2:
        return "PRG login configured"
    if configured:
        return f"incomplete PRG login: set both {AUTH_USERNAME_ENV} and {AUTH_PASSWORD_ENV}"
    return "anonymous"


def sleep_forever() -> None:
    print("[railway] AI Advokat parser container is ready.")
    print("[railway] Set AI_ADVOCAT_COMMAND to run a parser command on deploy.")
    print(f"[railway] Keep credentials in {AUTH_USERNAME_ENV}/{AUTH_PASSWORD_ENV}, never in AI_ADVOCAT_COMMAND.")
    print("[railway] Example: --out /tmp/ai-advokat-data --formats html,txt,json doc 35502996")
    while True:
        time.sleep(3600)


def main() -> None:
    command = os.environ.get("AI_ADVOCAT_COMMAND", "").strip()
    if not command:
        sleep_forever()
        return

    print(f"[railway] auth: {auth_mode()}")
    print(f"[railway] running: python -m ai_advokat_parser {redact_secrets(command)}")
    cli_main(shlex.split(command))
    if os.environ.get("AI_ADVOCAT_EXIT_AFTER_RUN", "").lower() in {"1", "true", "yes"}:
        return
    sleep_forever()


if __name__ == "__main__":
    main()

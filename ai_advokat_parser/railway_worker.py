from __future__ import annotations

import os
import re
import shlex
import time
from dataclasses import dataclass
from typing import Callable, Mapping

from .cli import build_parser, main as cli_main
from .config import (
    AUTH_PASSWORD_ENV,
    AUTH_USERNAME_ENV,
    CREDENTIAL_ENV_NAMES,
    SOT_PASSWORD_ENV,
    SOT_USERNAME_ENV,
)
from .sot import runtime as sot_runtime
from .sot.model import (
    PHASE_ABORTED,
    PHASE_COMPLETED,
    PHASE_PAUSED,
    PHASE_RATE_LIMITED,
    SotScanState,
)


AUTO_RESUME_ENV = "AI_ADVOCAT_SOT_AUTO_RESUME"
AUTO_RESUME_SAFETY_ENV = "AI_ADVOCAT_SOT_AUTO_RESUME_SAFETY_SECONDS"
AUTO_RESUME_PAUSED_ENV = "AI_ADVOCAT_SOT_AUTO_RESUME_PAUSED_SECONDS"
AUTO_RESUME_FALLBACK_ENV = "AI_ADVOCAT_SOT_AUTO_RESUME_FALLBACK_SECONDS"
AUTO_RESUME_MAX_WAIT_ENV = "AI_ADVOCAT_SOT_AUTO_RESUME_MAX_WAIT_SECONDS"

DEFAULT_AUTO_RESUME_SAFETY_SECONDS = 30.0
DEFAULT_AUTO_RESUME_PAUSED_SECONDS = 300.0
DEFAULT_AUTO_RESUME_FALLBACK_SECONDS = 3600.0
DEFAULT_AUTO_RESUME_MAX_WAIT_SECONDS = 8 * 24 * 3600.0
MAX_CONFIGURED_WAIT_SECONDS = 31 * 24 * 3600.0
MIN_WAIT_SECONDS = 1.0
TRUE_VALUES = frozenset({"1", "true", "yes"})
RATE_LIMIT_DELAY_RE = re.compile(r"(?:retry-after|reset-in)=(\d+(?:\.\d+)?)s", re.I)


class AutoResumeConfigError(ValueError):
    """The opt-in supervisor configuration is unsafe or incomplete."""


class AutoResumeFatalError(RuntimeError):
    """The supervisor stopped and requires an operator/configuration change."""


@dataclass(frozen=True)
class AutoResumeSettings:
    safety_seconds: float
    paused_seconds: float
    fallback_seconds: float
    max_wait_seconds: float


def redact_secrets(text: str, env: dict[str, str] | None = None) -> str:
    """Mask credential values that leaked into a command line before logging it."""
    source = os.environ if env is None else env
    for name in CREDENTIAL_ENV_NAMES:
        raw_value = source.get(name) or ""
        for value in {raw_value, raw_value.strip()}:
            if value and value in text:
                text = text.replace(value, "***")
    return text


def profile_auth_mode(username_env: str, password_env: str, env: dict[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    configured = []
    if (source.get(username_env) or "").strip():
        configured.append(username_env)
    if source.get(password_env) or "":
        configured.append(password_env)
    if len(configured) == 2:
        return "PRG login configured"
    if configured:
        return f"incomplete PRG login: set both {username_env} and {password_env}"
    return "anonymous"


def auth_mode(env: dict[str, str] | None = None) -> str:
    return profile_auth_mode(AUTH_USERNAME_ENV, AUTH_PASSWORD_ENV, env)


def sot_auth_mode(env: dict[str, str] | None = None) -> str:
    return profile_auth_mode(SOT_USERNAME_ENV, SOT_PASSWORD_ENV, env)


def _enabled(name: str, env: Mapping[str, str]) -> bool:
    return (env.get(name) or "").strip().lower() in TRUE_VALUES


def _bounded_seconds(
    name: str,
    default: float,
    env: Mapping[str, str],
    *,
    minimum: float,
    maximum: float,
) -> float:
    raw = (env.get(name) or str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise AutoResumeConfigError(f"{name} must be a number of seconds") from exc
    if not minimum <= value <= maximum:
        raise AutoResumeConfigError(f"{name} must be between {minimum:g} and {maximum:g} seconds")
    return value


def auto_resume_settings(env: Mapping[str, str]) -> AutoResumeSettings:
    max_wait = _bounded_seconds(
        AUTO_RESUME_MAX_WAIT_ENV,
        DEFAULT_AUTO_RESUME_MAX_WAIT_SECONDS,
        env,
        minimum=60.0,
        maximum=MAX_CONFIGURED_WAIT_SECONDS,
    )
    return AutoResumeSettings(
        safety_seconds=_bounded_seconds(
            AUTO_RESUME_SAFETY_ENV,
            DEFAULT_AUTO_RESUME_SAFETY_SECONDS,
            env,
            minimum=0.0,
            maximum=3600.0,
        ),
        paused_seconds=_bounded_seconds(
            AUTO_RESUME_PAUSED_ENV,
            DEFAULT_AUTO_RESUME_PAUSED_SECONDS,
            env,
            minimum=MIN_WAIT_SECONDS,
            maximum=max_wait,
        ),
        fallback_seconds=_bounded_seconds(
            AUTO_RESUME_FALLBACK_ENV,
            DEFAULT_AUTO_RESUME_FALLBACK_SECONDS,
            env,
            minimum=60.0,
            maximum=max_wait,
        ),
        max_wait_seconds=max_wait,
    )


def supervised_scan_args(command: str):
    """Parse and validate the only command the automatic loop may repeat."""
    try:
        argv = shlex.split(command)
        args = build_parser().parse_args(argv)
    except (ValueError, SystemExit) as exc:
        raise AutoResumeConfigError("AI_ADVOCAT_COMMAND is not a valid parser command") from exc
    if args.command != "sot-scan" or not getattr(args, "scan_id", None):
        raise AutoResumeConfigError(
            f"{AUTO_RESUME_ENV}=true requires AI_ADVOCAT_COMMAND with sot-scan --scan-id"
        )
    return argv, args


def load_scan_state(out_dir: str, scan_id: str, env: Mapping[str, str]) -> SotScanState | None:
    store = sot_runtime.open_store(out_dir, env=env)
    try:
        return store.get_scan(scan_id)
    finally:
        store.close()


def rate_limit_wait_seconds(note: str | None, settings: AutoResumeSettings) -> float:
    delays = [float(value) for value in RATE_LIMIT_DELAY_RE.findall(note or "")]
    requested = max(delays) if delays else settings.fallback_seconds
    return min(
        settings.max_wait_seconds,
        max(MIN_WAIT_SECONDS, requested + settings.safety_seconds),
    )


def supervise_sot_scan(
    command: str,
    *,
    env: Mapping[str, str] | None = None,
    runner: Callable[[list[str]], object] = cli_main,
    state_loader: Callable[[str, str, Mapping[str, str]], SotScanState | None] = load_scan_state,
    sleeper: Callable[[float], object] = time.sleep,
) -> SotScanState:
    """Run one durable SOT scan until completion, waiting for source resets.

    This loop never changes egress routing or attempts to turn a 429 into a
    successful response. Fatal/auth/config states are surfaced to ``main`` so
    production can park without Railway repeatedly restarting and hammering
    the login.
    """
    source = os.environ if env is None else env
    argv, args = supervised_scan_args(command)
    settings = auto_resume_settings(source)

    while True:
        try:
            runner(argv)
        except SystemExit as exc:
            raise AutoResumeFatalError(f"sot-scan exited with code {exc.code}") from exc
        except Exception as exc:
            raise AutoResumeFatalError(f"sot-scan failed: {type(exc).__name__}: {exc}") from exc

        state = state_loader(args.out, args.scan_id, source)
        if state is None:
            raise AutoResumeFatalError(f"sot-scan did not create durable state for {args.scan_id}")
        if state.phase == PHASE_COMPLETED:
            print(f"[railway] auto-resume: scan {args.scan_id} completed")
            return state
        if state.phase == PHASE_RATE_LIMITED:
            delay = rate_limit_wait_seconds(state.rate_limit_note, settings)
            print(
                f"[railway] auto-resume: scan {args.scan_id} rate-limited; "
                f"next attempt in {delay:.0f}s"
            )
            sleeper(delay)
            continue
        if state.phase == PHASE_PAUSED:
            print(
                f"[railway] auto-resume: scan {args.scan_id} paused; "
                f"recoverable retry in {settings.paused_seconds:.0f}s"
            )
            sleeper(settings.paused_seconds)
            continue
        if state.phase == PHASE_ABORTED:
            raise AutoResumeFatalError(
                f"scan {args.scan_id} is aborted; fix authorization/configuration before redeploy"
            )
        raise AutoResumeFatalError(
            f"scan {args.scan_id} returned unexpected phase {state.phase!r}; operator action required"
        )


def sleep_forever() -> None:
    print("[railway] AI Advokat parser container is ready.")
    print("[railway] Set AI_ADVOCAT_COMMAND to run a parser command on deploy.")
    print(f"[railway] Keep credentials in {AUTH_USERNAME_ENV}/{AUTH_PASSWORD_ENV}, never in AI_ADVOCAT_COMMAND.")
    print("[railway] Example: --out /tmp/ai-advokat-data --formats html,txt,json doc 35502996")
    while True:
        time.sleep(3600)


def main() -> None:
    source = os.environ
    command = source.get("AI_ADVOCAT_COMMAND", "").strip()
    if not command:
        sleep_forever()
        return

    print(f"[railway] auth PRG.ZANGER: {auth_mode()}")
    print(f"[railway] auth PRG.SOT: {sot_auth_mode()}")
    print(f"[railway] running: python -m ai_advokat_parser {redact_secrets(command)}")
    try:
        if _enabled(AUTO_RESUME_ENV, source):
            supervise_sot_scan(command, env=source)
        else:
            cli_main(shlex.split(command))
    except (AutoResumeConfigError, AutoResumeFatalError) as exc:
        print(f"[railway] auto-resume stopped: {redact_secrets(str(exc))}")
        if _enabled("AI_ADVOCAT_EXIT_AFTER_RUN", source):
            raise SystemExit(1) from exc
    if _enabled("AI_ADVOCAT_EXIT_AFTER_RUN", source):
        return
    sleep_forever()


if __name__ == "__main__":
    main()

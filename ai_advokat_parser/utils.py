from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def sanitize_filename(value: str, fallback: str = "document", max_len: int = 120) -> str:
    value = re.sub(r"<[^>]+>", "", value or "")
    value = re.sub(r"[\\/:*?\"<>|]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    if not value:
        value = fallback
    return value[:max_len].rstrip(" .")


def parse_formats(raw: str | Iterable[str]) -> tuple[str, ...]:
    from .config import SUPPORTED_FORMATS

    if isinstance(raw, str):
        parts = re.split(r"[,;\s]+", raw.strip())
    else:
        parts = list(raw)
    formats = []
    for item in parts:
        item = item.strip().lower()
        if not item:
            continue
        if item not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{item}'. Supported: {', '.join(SUPPORTED_FORMATS)}"
            )
        if item not in formats:
            formats.append(item)
    if not formats:
        raise ValueError("At least one output format is required.")
    return tuple(formats)

from __future__ import annotations

import re
from dataclasses import dataclass


PARAGRAPH_BREAK = re.compile(r"\n\s*\n")
HEADING = re.compile(
    r"^(?:статья|глава|раздел|параграф|article|chapter|section|"
    r"бап|тарау|бөлім|§|\d+[.)])\s*",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class TextChunk:
    number: int
    content: str
    char_start: int
    char_end: int
    heading: str


def _choose_end(text: str, start: int, target: int) -> int:
    if target >= len(text):
        return len(text)
    minimum = start + max(250, (target - start) // 2)
    candidates = [match.end() for match in PARAGRAPH_BREAK.finditer(text, minimum, target)]
    if candidates:
        return candidates[-1]
    newline = text.rfind("\n", minimum, target)
    if newline > start:
        return newline + 1
    space = text.rfind(" ", minimum, target)
    return space + 1 if space > start else target


def _heading_for(content: str) -> str:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    for line in lines[:12]:
        if len(line) <= 240 and HEADING.match(line):
            return line
    return lines[0][:240] if lines and len(lines[0]) <= 160 else ""


def chunk_text(text: str, max_chars: int = 3500, overlap_chars: int = 350) -> list[TextChunk]:
    if max_chars < 500:
        raise ValueError("max_chars must be >= 500")
    if overlap_chars < 0 or overlap_chars >= max_chars:
        raise ValueError("overlap_chars must be >= 0 and smaller than max_chars")

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    if not normalized.strip():
        return []

    chunks: list[TextChunk] = []
    start = 0
    while start < len(normalized):
        while start < len(normalized) and normalized[start].isspace():
            start += 1
        if start >= len(normalized):
            break
        end = _choose_end(normalized, start, start + max_chars)
        raw = normalized[start:end].rstrip()
        if raw:
            chunks.append(
                TextChunk(
                    number=len(chunks),
                    content=raw,
                    char_start=start,
                    char_end=start + len(raw),
                    heading=_heading_for(raw),
                )
            )
        if end >= len(normalized):
            break
        next_start = max(start + 1, end - overlap_chars)
        paragraph = normalized.find("\n\n", next_start, end)
        start = paragraph + 2 if paragraph >= 0 else next_start
    return chunks


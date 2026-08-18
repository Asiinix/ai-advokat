from __future__ import annotations

import html
import json
import os
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

from .document import DocumentData
from .utils import ensure_dir


class TextExtractor(HTMLParser):
    block_tags = {"p", "div", "section", "article", "tr", "table", "h1", "h2", "h3", "li"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in {"script", "style"}:
            self.skip_depth += 1
        elif tag == "br":
            self.parts.append("\n")
        elif tag == "td":
            self.parts.append("\t")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style"} and self.skip_depth:
            self.skip_depth -= 1
        elif tag in self.block_tags:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        if data:
            self.parts.append(data)

    def text(self) -> str:
        value = "".join(self.parts)
        lines = [" ".join(line.split()) for line in value.splitlines()]
        compact: list[str] = []
        blank = False
        for line in lines:
            if not line:
                if not blank:
                    compact.append("")
                blank = True
            else:
                compact.append(line)
                blank = False
        return "\n".join(compact).strip() + "\n"


def html_to_text(raw_html: str) -> str:
    parser = TextExtractor()
    parser.feed(raw_html)
    return parser.text()


def build_document_html(document: DocumentData) -> str:
    style = document.raw.get("style") or ""
    title = html.escape(document.title)
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{
      background: #f7f7f7;
      margin: 0;
      padding: 24px;
    }}
    .document {{
      background: #fff;
      box-sizing: border-box;
      margin: 0 auto;
      max-width: 860px;
      min-height: 100vh;
      padding: 48px 56px;
      box-shadow: 0 2px 16px rgba(0,0,0,.08);
    }}
    .meta {{
      color: #666;
      font-family: Arial, sans-serif;
      font-size: 12px;
      margin-bottom: 24px;
    }}
    a {{ color: #333399; }}
    {style}
    @media print {{
      body {{ background: #fff; padding: 0; }}
      .document {{ box-shadow: none; max-width: none; padding: 0; }}
      .meta {{ display: none; }}
    }}
  </style>
</head>
<body>
  <main class="document">
    <div class="meta">Источник: https://prg.kz/lawyer/document/?doc_id={html.escape(document.doc_id)}</div>
    {document.html}
  </main>
</body>
</html>
"""


def find_chrome_binary() -> str | None:
    env_path = os.environ.get("AI_ADVOCAT_CHROME")
    if env_path and Path(env_path).exists():
        return env_path

    candidates = [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "microsoft-edge",
        "brave-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        str(Path.home() / "Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    ]
    for candidate in candidates:
        resolved = shutil.which(candidate) if "/" not in candidate else candidate
        if resolved and Path(resolved).exists():
            return resolved
    return None


def export_pdf(html_path: Path, pdf_path: Path) -> None:
    chrome = find_chrome_binary()
    if not chrome:
        raise RuntimeError(
            "PDF export needs Google Chrome/Chromium. "
            "Install it or set AI_ADVOCAT_CHROME=/path/to/chrome."
        )
    command = [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        f"--print-to-pdf={pdf_path}",
        html_path.resolve().as_uri(),
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def export_document(document: DocumentData, out_dir: str | Path, formats: tuple[str, ...]) -> dict[str, Path]:
    doc_dir = ensure_dir(Path(out_dir) / "documents" / document.doc_id)
    result: dict[str, Path] = {}

    html_path = doc_dir / "document.html"
    if "html" in formats or "pdf" in formats:
        html_path.write_text(build_document_html(document), encoding="utf-8")
        result["html"] = html_path

    if "txt" in formats:
        txt_path = doc_dir / "document.txt"
        txt_path.write_text(html_to_text(document.html), encoding="utf-8")
        result["txt"] = txt_path

    if "json" in formats:
        json_path = doc_dir / "document.json"
        json_path.write_text(
            json.dumps(document.raw, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        result["json"] = json_path

    if "pdf" in formats:
        pdf_path = doc_dir / "document.pdf"
        export_pdf(html_path, pdf_path)
        result["pdf"] = pdf_path

    meta_path = doc_dir / "meta.json"
    meta_path.write_text(
        json.dumps(
            {
                "doc_id": document.doc_id,
                "title": document.title,
                "is_free": document.is_free,
                "paragraphs": len(document.paragraphs),
                "linked_doc_ids": document.linked_doc_ids,
                "outputs": {key: str(path) for key, path in result.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    result["meta"] = meta_path
    return result

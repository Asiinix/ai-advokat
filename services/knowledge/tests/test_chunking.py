from knowledge_service.chunking import chunk_text


def test_chunk_text_preserves_content_and_limits_size() -> None:
    text = "Статья 1. Общие положения\n\n" + ("Первый абзац закона. " * 80)
    text += "\n\nСтатья 2. Права сторон\n\n" + ("Второй абзац закона. " * 80)

    chunks = chunk_text(text, max_chars=700, overlap_chars=100)

    assert len(chunks) > 2
    assert all(chunk.content for chunk in chunks)
    assert all(len(chunk.content) <= 700 for chunk in chunks)
    assert chunks[0].heading == "Статья 1. Общие положения"
    assert chunks[0].char_start == 0
    assert chunks[-1].char_end <= len(text)


def test_chunk_text_handles_empty_input() -> None:
    assert chunk_text("\n  \n") == []


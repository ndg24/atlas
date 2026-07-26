from __future__ import annotations

from atlas_ai.retrieval.chunking import chunk_text


def test_empty_text_produces_no_chunks():
    assert chunk_text("") == []
    assert chunk_text("   ") == []


def test_short_text_is_a_single_chunk():
    assert chunk_text("a short passage", max_chars=1000) == ["a short passage"]


def test_long_text_splits_into_overlapping_chunks():
    text = "0123456789" * 30  # 300 chars
    chunks = chunk_text(text, max_chars=100, overlap=20)

    assert len(chunks) > 1
    # every char of the original text appears in some chunk (no gaps)
    assert "".join(chunks[:1])[:80] == text[:80]
    # consecutive chunks overlap by the requested amount
    assert chunks[0][-20:] == chunks[1][:20]


def test_chunks_never_exceed_max_chars():
    text = "word " * 500
    chunks = chunk_text(text, max_chars=200, overlap=50)
    assert all(len(c) <= 200 for c in chunks)

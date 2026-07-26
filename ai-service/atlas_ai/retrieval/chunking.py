"""Splits document text into fixed-size, overlapping chunks before
embedding -- keeps each vector focused on one passage rather than an entire
paper, and keeps embedding calls small at fixture/small-corpus scale.
"""

from __future__ import annotations


def chunk_text(text: str, max_chars: int = 1000, overlap: int = 100) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start = end - overlap
    return chunks

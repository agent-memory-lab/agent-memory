"""Versioned lexical terms shared by bounded BM25 and repository overlap scoring.

``ascii-han/1`` freezes the original optional BM25 analyzer: Unicode casefold,
ASCII alphanumeric runs, and U+3400–U+9FFF runs with unigrams and adjacent
bigrams. Separators are boundaries, never deleted before forming a bigram.
Frequency and order are retained for BM25; overlap callers may take a set.

This is a lexical heuristic, not linguistic Chinese word segmentation. There
is no normalization, stemming, stop-word removal, or token-budget estimation.
Persisted term indexes must bind this version and rebuild rather than mixing
query and document terms from incompatible analyzer versions.
"""

from __future__ import annotations

import re

LEXICAL_ANALYZER_VERSION = "ascii-han/1"

_WORDS = re.compile(r"[a-z0-9]+|[\u3400-\u9fff]+")


def lexical_terms(text: str) -> tuple[str, ...]:
    """Return deterministic terms, retaining frequency and contiguous Han runs."""
    tokens: list[str] = []
    for word in _WORDS.findall(text.casefold()):
        if "\u3400" <= word[0] <= "\u9fff":
            tokens.extend(word)
            tokens.extend(word[index : index + 2] for index in range(len(word) - 1))
        else:
            tokens.append(word)
    return tuple(tokens)

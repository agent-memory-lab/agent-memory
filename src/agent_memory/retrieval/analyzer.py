"""Versioned lexical terms shared by bounded BM25 and repository overlap scoring.

``ascii-han/1`` freezes the original optional BM25 analyzer: Unicode casefold,
ASCII alphanumeric runs, and U+3400–U+9FFF runs with unigrams and adjacent
bigrams. Separators are boundaries, never deleted before forming a bigram.
Frequency and order are retained for BM25; overlap callers may take a set.

The index analyzer is a lexical heuristic, not linguistic Chinese word
segmentation. It does no normalization, stemming, stop-word removal, or token
budget estimation. Request-only content anchors below never alter index terms.
Persisted term indexes must bind this version and rebuild rather than mixing
query and document terms from incompatible analyzer versions.
"""

from __future__ import annotations

import re

LEXICAL_ANALYZER_VERSION = "ascii-han/1"
QUERY_ANCHOR_POLICY_VERSION = "content-anchor/1"

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


# A bounded English question/function-word heuristic, not a POS tagger. These
# terms remain present in lexical scoring and the semantic query unchanged.
_FUNCTION_TERMS = frozenset("""
a an the what when where who whom whose which why how much many often
do does did is are am was were be being been have has had
can could may might must shall should will would
i me my mine myself you your yours yourself yourselves
he him his himself she her hers herself it its itself
we us our ours ourselves they them their theirs themselves
this that these those any all some each every both either another
at in on to from for of by with about as into onto over under than
through between during up down out and or but if because while so
there here then now ever s d ll m re ve t
""".split())
_NEGATION_TERMS = frozenset((
    "no", "not", "never", "without", "neither", "nor", "none", "nobody", "nothing", "nowhere",
))
_QUOTED = re.compile(r'"([^"\n]+)"|“([^”\n]+)”|(?<!\w)\'([^\'\n]+)\'(?!\w)|‘([^’\n]+)’')


def query_content_anchors(text: str) -> tuple[str, ...]:
    """Return request-time lexical admission anchors; empty means no extra gate.

    A number or negation can affect relevance without admitting an unrelated
    candidate by itself when a topic term exists. Queries containing only such
    terms keep ordinary retrieval. Quoted names protect otherwise functional
    words, e.g. ``"The Who"`` keeps ``who``; the leading article alone cannot
    admit every document. This does not resolve entities or numeric semantics.
    """
    protected = set()
    for match in _QUOTED.finditer(text):
        quoted = next(group for group in match.groups() if group is not None)
        words = set(lexical_terms(quoted))
        protected.update(words - {"a", "an", "the"} or words)
    return tuple(dict.fromkeys(
        term for term in lexical_terms(text)
        if term in protected or (
            term not in _FUNCTION_TERMS and term not in _NEGATION_TERMS and not term.isdigit()
        )
    ))

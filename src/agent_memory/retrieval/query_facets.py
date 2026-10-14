"""Small, request-only lexical probes for personal advice and event counting.

This experimental strategy requires ``enable_query_facets=True``; it has not
demonstrated a net evidence-coverage benefit and is disabled by default.
These probes are candidate discovery hints, never answers or eligibility rules.
They do not modify persisted analyzer terms and deliberately cover only explicit
English intents. Unknown or ambiguous intents keep ordinary lexical retrieval.
Each probe requires a typed term group; count probes also require a topic term.
No domain names, benchmark identifiers or answer vocabulary belong here.
"""

import re
from dataclasses import dataclass

from .analyzer import lexical_terms, query_content_anchors

QUERY_FACET_POLICY = "bounded-query-facets/1"
MAX_PROBE_TERMS = 128
MAX_PROBE_CANDIDATES = 16

_PERSONAL = frozenset(("i", "me", "my", "mine", "we", "us", "our", "ours"))
_ADVICE = re.compile(
    r"\b(?:any|some|need|want|your)\s+(?:advice|suggestions?)\b"
    r"|\b(?:can|could|would)\s+you\s+"
    r"(?:recommend|suggest|help\s+(?:me|us)\s+(?:choose|select|decide))\b"
    r"|\b(?:what|which|how)\s+should\s+(?:i|we)\b"
    r"|^\s*(?:please\s+)?(?:recommend|suggest)\b"
)
_PREFERENCES = tuple("""
preference preferences prefer prefers preferred favorite favourite
dislike dislikes avoid avoids constraint constraints requirement requirements
""".split())
_COUNT = re.compile(r"\bhow\s+many\b|\b(?:number|count|total)\s+of\b")
_COUNT_CONTEXT = frozenset("""
number count total time times last next previous current recent recently
today yesterday tomorrow day days week weeks month months year years
since until before after ago past within
""".split())
# Generic action classes, not a thesaurus for particular subject domains. A
# second topic gate prevents a purchase about another subject from being added.
_ACTIONS = (
    ("acquisition", tuple("""
        acquire acquired acquires acquiring acquisition acquisitions
        buy buys buying bought purchase purchases purchased purchasing
        receive receives received receiving obtain obtains obtained obtaining
        get got gotten
    """.split())),
    ("completion", tuple("""
        complete completes completed completing completion completions
        finish finishes finished finishing done
    """.split())),
    ("attendance", tuple("""
        attend attends attended attending attendance
        visit visits visited visiting participate participates participated participating
    """.split())),
)


@dataclass(frozen=True, slots=True)
class QueryFacet:
    name: str
    terms: tuple[str, ...]
    required_groups: tuple[tuple[str, ...], ...]

    @property
    def text(self):
        return " ".join(self.terms)


def _topic_forms(terms):
    """Conservative request-time noun variants; index/analyzer stay unchanged."""
    result = []
    for term in terms:
        result.append(term)
        if not term.isascii() or not term.isalpha():
            continue
        if len(term) > 4 and term.endswith("ies"):
            result.append(term[:-3] + "y")
        elif len(term) > 3 and term.endswith("s") and not term.endswith(("ss", "us", "is")):
            result.append(term[:-1])
    return tuple(dict.fromkeys(result))[:64]


def query_facets(text):
    """Return at most one probe, so every query has a fixed SQL/hydration bound.

    Preference evidence can omit the requested topic altogether (an allergy or
    a standing constraint may still affect advice). This lane gets a minority
    of the existing candidate budget, and does not assert topical relevance.
    Counts require both an action and a topic; a clock word alone is not a topic.
    """
    words = set(lexical_terms(text))
    if words & _PERSONAL and _ADVICE.search(text.casefold()):
        topic = _topic_forms(query_content_anchors(text))
        return (QueryFacet(
            "personal_context", tuple(dict.fromkeys((*_PREFERENCES, *topic)))[:MAX_PROBE_TERMS],
            (_PREFERENCES,),
        ),)
    if not _COUNT.search(text.casefold()):
        return ()
    for name, action_terms in _ACTIONS:
        # "Got" can mean acquired, became, or understood. It may match evidence
        # after an explicit acquisition request, but cannot select that intent.
        triggers = set(action_terms) - {"get", "got", "gotten"}
        if words.intersection(triggers):
            topic = _topic_forms(tuple(
                term for term in query_content_anchors(text)
                if term not in _COUNT_CONTEXT and term not in action_terms
            ))
            if topic:
                return (QueryFacet(
                    name, tuple(dict.fromkeys((*action_terms, *topic)))[:MAX_PROBE_TERMS],
                    (action_terms, topic),
                ),)
    return ()

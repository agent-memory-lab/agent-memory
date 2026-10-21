"""Conservative, request-only allocation hints for explicit coordinated topics."""
from collections import Counter
from dataclasses import dataclass
import re

from .analyzer import lexical_terms, query_content_anchors

POLICY = "explicit-coordination-rarest-anchor/1"
MAX_GROUPS = 4
_QUOTED = re.compile(r'"[^"\n]*"|“[^”\n]*”|‘[^’\n]*’')
_SEPARATOR = re.compile(r"\band\b|\bversus\b|\bvs\b|以及|与|、|[,，;；]", re.IGNORECASE)
_DETERMINER = re.compile(r"\b(?:a|an|the|my|our|your)\s+", re.IGNORECASE)
_DEPENDENT = re.compile(r"^(?:its|their|his|her|it|them|the\s+same)\b|^(?:其|它的|他们的)", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class CoverageGroup:
    id: str
    anchors: frozenset[str]


@dataclass(frozen=True, slots=True)
class CoveragePlan:
    groups: tuple[CoverageGroup, ...] = ()

    def matches(self, terms):
        return frozenset(group.id for group in self.groups if group.anchors.intersection(terms))

    def metadata(self, values):
        words = {term for value in values for term in lexical_terms(value.text)}
        covered = self.matches(words)
        return {"policy": POLICY, "status": "not_applicable" if not self.groups else
                "lexically_covered" if len(covered) == len(self.groups) else "partial",
                "groups": tuple({"id": group.id, "anchors": tuple(sorted(group.anchors)),
                                 "covered": group.id in covered} for group in self.groups),
                "covered_groups": len(covered), "group_count": len(self.groups),
                "answer_completeness_verified": False, "world_negative": False}


def refresh_coverage_metadata(metadata, values):
    """Recompute hints from surviving text; removed evidence cannot cover a group."""
    plan = CoveragePlan(tuple(CoverageGroup(group["id"], frozenset(group["anchors"]))
                              for group in metadata["groups"]))
    return plan.metadata(values)


def coverage_plan(text, candidates):
    """Split only explicit unquoted coordination; ordinary queries keep ranking.

    A rare observed query anchor distinguishes each clause without a subject
    vocabulary or answer-specific rules. This is lexical presence, not proof of
    factual support. Dependent pronouns and numeric-only clauses decline planning.
    """
    if not text or len(text) > 4096:
        return CoveragePlan()
    masked = list(text)
    for match in _QUOTED.finditer(text):
        masked[match.start():match.end()] = "_" * (match.end()-match.start())
    depth = 0
    for index, char in enumerate(text):
        if char in "([（":
            depth += 1
        if depth:
            masked[index] = "_"
        if char in ")]）":
            depth = max(0, depth-1)
    clauses, start = [], 0
    for match in _SEPARATOR.finditer("".join(masked)):
        clauses.append(text[start:match.start()].strip())
        start = match.end()
    clauses.append(text[start:].strip(" .?？"))
    clauses = [clause for clause in clauses if clause]
    if not 2 <= len(clauses) <= MAX_GROUPS or any(_DEPENDENT.match(c) for c in clauses[1:]):
        return CoveragePlan()
    terms = []
    for clause in clauses:
        protected = [(m.start(), m.end()) for m in _QUOTED.finditer(clause)]
        determiners = [m for m in _DETERMINER.finditer(clause)
                       if not any(a <= m.start() < b for a, b in protected)]
        phrase = clause[determiners[-1].end():] if determiners else clause
        terms.append(set(query_content_anchors(phrase)))
    if any(not group for group in terms):
        return CoveragePlan()
    memberships = Counter(term for group in terms for term in group)
    frequencies = Counter(term for item in candidates if item.metadata.get("context_only") is not True
                          for term in set(lexical_terms(item.text)))
    groups = []
    for index, words in enumerate(terms):
        unique = {term for term in words if memberships[term] == 1}
        if not unique:
            return CoveragePlan()
        observed = {term for term in unique if frequencies[term]}
        minimum = min((frequencies[term] for term in observed), default=0)
        anchors = {term for term in observed if frequencies[term] == minimum} or unique
        groups.append(CoverageGroup(f"clause:{index}", frozenset(anchors)))
    return CoveragePlan(tuple(groups))

"""Risk-driven synthetic challenges; these are not held-out benchmark samples."""
from dataclasses import asdict, replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps

from agent_memory.domain import MemoryItem, MemoryKind
from agent_memory.retrieval.candidate_identity import candidate_identity

NOW = datetime(2026, 10, 15, tzinfo=UTC)


def claim(identity, text, score=1.0):
    return MemoryItem(identity, MemoryKind.CLAIM, text, score, NOW,
                      {"source_event_ids": (identity,)})


def event(identity, source, *, start=0, end=None, score=1.0):
    end = len(source) if end is None else end
    revision = sha256(source.encode()).hexdigest()
    locator = sha256(dumps(["events", identity, revision, start, end]).encode()).hexdigest()
    return MemoryItem(identity, MemoryKind.EVENT, source[start:end], score, NOW, {
        "source_event_ids": (identity,), "source_revision": revision,
        "source_chars": len(source), "lexical_chunk_id": locator,
        "source_span": {"start": start, "end": end, "unit": "characters"},
    })


def challenges():
    answer = "The vehicle battery was replenished at Linden Garage, never at home."
    semantic = event("semantic", answer + " Maintenance records remain available." * 30)
    question = "Where did I recharge the electric car?"
    noise = claim("question-only", question + " The log must be reviewed.", .6)
    base = [("synonym-crop", question, (semantic, noise), (("semantic", answer),))]

    facts = (claim("permit", "Harbor workshop gate access is permitted.", 1.0),
             claim("revocation", "Harbor workshop gate access is not permitted; it was revoked.", .98),
             claim("planning", "Harbor workshop gate access opening schedule is under current review.", .94))
    base.append(("conflict", "What changed in Harbor workshop gate access and its current opening schedule?",
                 facts, (("permit", facts[0].text), ("revocation", facts[1].text))))

    first = "user: The quartz workshop address is in the next message.\n"
    second = "assistant: It is at Linden Gardens, never at home."
    source = first + second
    anchor = event("dialogue", source, end=len(first))
    neighbor = event("dialogue", source, start=len(first))
    neighbor = replace(neighbor, metadata={**neighbor.metadata, "context_only": True,
        "relevance_origin": "inherited_context_anchor",
        "context_anchor_candidate_id": candidate_identity(anchor)})
    base.append(("pronoun-bridge", "Where is the quartz workshop?",
        (anchor, neighbor, claim("address-topic", "Quartz workshop address is being discussed.", .91)),
        (("dialogue", first.rstrip()), ("dialogue", second))))

    purchases = tuple(claim(f"purchase-{i}", f"Workshop consumables purchase {i} cost {value} credits.",
                            1 - i*.01) for i, value in enumerate((17, 23, 31)))
    base.append(("same-topic-count", "What is the total workshop consumables purchase cost in the records?",
        (*purchases, claim("topic-overview", "Workshop consumables purchase cost total records are under review.", .94)),
        tuple((fact.id, fact.text) for fact in purchases)))

    chinese = "座驾在枫叶维修站充电，从未在家操作。"
    base.append(("chinese-synonym", "我给电动汽车补充能量的地点在哪里？",
        (event("cn-answer", chinese + "维修单据均已按规定归档。" * 35),
         claim("cn-question", "电动汽车补充能量的地点尚待整理。", .65)),
        (("cn-answer", chinese),)))

    repeated = tuple(claim(f"amber-{i}", f"Amber modules have inventory annotation {i}.",
                           1 - i*.008) for i in range(10))
    violet = claim("violet", "Violet modules use the isolated service bay.", .91)
    base.append(("distinct-topics", "Compare amber modules and violet modules inventory.",
                 (*repeated, violet), (("violet", violet.text),)))

    negation = "assistant: This is hypothetical, not approved. The archive gate must not open at 07:30."
    base.append(("role-negation", "archive gate open approval", (
        event("hypothetical", negation + "\nGeneral archive background." * 30),
        claim("audit", "Archive gate audit is pending.", .8)), (("hypothetical", negation),)))

    old = claim("old", "The archive now has seven crates.")
    correction = "user: The archive now has eleven crates, not seven."
    base.append(("numeric-update", "How many archive crates are there now?", (
        old, event("updated", correction + "\nAudit documents remain available." * 20, score=.98),
        claim("inventory-topic", "Archive crates inventory is under audit.", .93)),
        (("updated", correction),)))

    inputs, labels = [], []
    for name, query, evidence, required in base:
        slots = (1 if name in ("synonym-crop", "chinese-synonym")
                 else 3 if name == "same-topic-count" else 4 if name == "distinct-topics" else 2)
        for budget in (64, 128, 256):
            # Every arm receives the same extra candidates; none carry required facts.
            noise = tuple(claim(f"irrelevant-{i}", "The maintenance ledger was archived.", .05)
                          for i in range(12))
            cid = f"{name}-{budget}"
            candidates = []
            for value in (*evidence, *noise):
                row = asdict(value)
                row["occurred_at"], row["kind"] = value.occurred_at.isoformat(), value.kind.value
                candidates.append(row)
            inputs.append({"case_id": cid, "question": query, "candidates": candidates,
                           "token_budget": budget, "item_limit": slots})
            labels.append({"case_id": cid, "category": name, "required_evidence": [
                {"memory_id": identity, "text": text} for identity, text in required]})
    return inputs, labels

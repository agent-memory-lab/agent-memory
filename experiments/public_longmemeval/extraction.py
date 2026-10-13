import json

CLAIM_SCHEMA = {"type": "object", "properties": {"claims": {"type": "array", "maxItems": 8,
    "items": {"type": "object", "properties": {**{name: {"type": "string"} for name in ("key", "value", "text")},
                                               "confidence": {"type": "number", "minimum": 0, "maximum": 1}},
              "required": ["key", "value", "text", "confidence"], "additionalProperties": False}}},
    "required": ["claims"], "additionalProperties": False}


class ClaimGenerator:
    def __init__(self, client):
        self.client = client
        self.proposed = 0
        self.meeting_confidence_threshold = 0
    async def generate_claims(self, event):
        prompt = (
            "Extract at most eight concise facts useful to answer future questions from this conversation "
            "segment. Include user or assistant facts, named entities, dates, preferences, plans, and explicit "
            "corrections. Skip general advice and filler. Use stable keys for facts that may be corrected later. "
            "Each fact needs key, value, one short factual text, and confidence from 0 to 1 reflecting "
            "how strongly the conversation supports it. Return an empty claims array if there are "
            "no useful facts. Do not invent anything.\n\nConversation segment:\n" + event.content)
        result = json.loads(await self.client.chat(prompt, "extraction", format=CLAIM_SCHEMA, limit=1536))
        claims = result["claims"]
        if not isinstance(claims, list) or len(claims) > 8:
            raise ValueError("Malformed bounded extraction output")
        if any(not isinstance(claim, dict) or type(claim.get("confidence")) not in (int, float)
               or not 0 <= claim["confidence"] <= 1
               or any(not isinstance(claim.get(key), str) or not claim[key].strip() for key in ("key", "value", "text"))
               for claim in claims):
            raise ValueError("Every extracted claim must explicitly supply valid model confidence")
        self.proposed += len(claims)
        self.meeting_confidence_threshold += sum(claim["confidence"] >= .8 for claim in claims)
        return claims


def chunks(text, max_bytes=9000):
    current, length = [], 0
    for char in text:
        size = len(char.encode())
        if current and length + size > max_bytes:
            yield "".join(current)
            current, length = [], 0
        current.append(char)
        length += size
    if current:
        yield "".join(current)

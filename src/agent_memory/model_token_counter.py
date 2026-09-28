"""Host-supplied model tokenization without mandatory tokenizer dependencies."""
from hashlib import sha256
import json


class ModelTokenCounter:
    """Count a rendered model prompt plus reserved output/tool budget.

    encode is a trusted callable returning a sized token sequence. render must
    apply the target model's real message template, including special tokens.
    Tokenizer/template identities are pinned by the host, never inferred from
    a model name or silently downloaded. Counts are exact only for that input
    template; hidden provider additions remain the host's responsibility.
    """
    def __init__(self, encode, *, model_id, tokenizer_version, template_version,
                 render=None, framing_tokens=0, reserve_tokens=0,
                 max_prompt_bytes=1048576):
        if not callable(encode) or (render is not None and not callable(render)):
            raise TypeError("encode and render must be trusted callables")
        for value in (model_id, tokenizer_version, template_version):
            if not isinstance(value, str) or not 1 <= len(value) <= 256:
                raise ValueError("model, tokenizer and template identities are required")
        for value in (framing_tokens, reserve_tokens):
            if type(value) is not int or not 0 <= value <= 1000000:
                raise ValueError("token reservations must be bounded nonnegative integers")
        if type(max_prompt_bytes) is not int or not 1024 <= max_prompt_bytes <= 16777216:
            raise ValueError("invalid prompt byte limit")
        self.encode, self.render = encode, render
        self.framing_tokens, self.reserve_tokens = framing_tokens, reserve_tokens
        self.max_prompt_bytes = max_prompt_bytes
        self.model_id = model_id
        identity = dict(model=model_id, tokenizer=tokenizer_version, template=template_version,
                        framing=framing_tokens, reserve=reserve_tokens, max_bytes=max_prompt_bytes)
        self.counter_id = "model-tokens:" + sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()

    def measure(self, text):
        if not isinstance(text, str) or len(text.encode("utf-8")) > self.max_prompt_bytes:
            raise ValueError("counter input exceeds byte limit")
        rendered = self.render(text) if self.render is not None else text
        if not isinstance(rendered, str) or len(rendered.encode("utf-8")) > self.max_prompt_bytes:
            raise ValueError("rendered model prompt exceeds byte limit")
        tokens = self.encode(rendered)
        if isinstance(tokens, (str, bytes, dict)):
            raise TypeError("encoder must return a sized token sequence")
        count = len(tokens)
        if count > self.max_prompt_bytes * 4:
            raise ValueError("tokenizer output exceeds configured safety limit")
        return dict(prompt_tokens=count, framing_tokens=self.framing_tokens,
                    reserved_tokens=self.reserve_tokens,
                    total_budget_units=count + self.framing_tokens + self.reserve_tokens,
                    counter_id=self.counter_id, model_id=self.model_id)

    def count(self, text):
        return self.measure(text)["total_budget_units"]

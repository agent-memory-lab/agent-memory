"""Host-owned token counting: no model downloads or mandatory dependencies."""
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True, slots=True)
class TokenCounter:
    """Count a specified serialized payload using the host's chosen tokenizer.

    The identifier should include tokenizer/model version. This counts payload
    tokens only, not system prompts, chat envelopes or provider-internal tokens.
    """

    identifier: str
    count: Callable[[str], int]

    def __post_init__(self):
        if not isinstance(self.identifier, str) or not self.identifier.strip() or len(self.identifier) > 256:
            raise ValueError("tokenizer identifier must contain 1 to 256 characters")
        if not callable(self.count):
            raise TypeError("token count function must be callable")

    def measure(self, text: str) -> int:
        count = self.count(text)
        if type(count) is not int or count < 0 or (text and count == 0):
            raise ValueError("token counter must return a positive integer for nonempty text")
        return count

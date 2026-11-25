"""Validated native retrieval configuration shared by all composition roots."""

from dataclasses import dataclass

from ..ports import Reranker, implements_port_method
from .lossless_packing import LosslessBudget
from .packing_policy import packing_contract


@dataclass(frozen=True, slots=True)
class NativeRetrievalOptions:
    packing_version: str = "v2"
    passage_reranker: Reranker | None = None
    enable_source_context: bool = True
    lossless_packing_budget: LosslessBudget | None = None
    require_delivery_guard: bool = False

    def __post_init__(self):
        packing_contract(self.packing_version)
        if type(self.enable_source_context) is not bool:
            raise ValueError("enable_source_context must be a bool")
        if type(self.require_delivery_guard) is not bool:
            raise ValueError("require_delivery_guard must be a bool")
        if self.passage_reranker is not None and self.packing_version != "v5":
            raise ValueError("passage_reranker requires explicit packing_version=v5")
        if self.passage_reranker is not None and not implements_port_method(
            self.passage_reranker, Reranker, "rerank",
        ):
            raise ValueError("passage_reranker requires an implemented rerank method")
        if self.lossless_packing_budget is not None and (
            not isinstance(self.lossless_packing_budget, LosslessBudget)
            or self.packing_version != "v2"
            or self.passage_reranker is not None
        ):
            raise ValueError(
                "lossless packing requires explicit LosslessBudget and unmodified v2 upstream"
            )

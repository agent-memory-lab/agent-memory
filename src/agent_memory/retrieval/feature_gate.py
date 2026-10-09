"""Host-only activation receipts for controlled retrieval experiments.

A receipt is not an authority token. The host verifier must authenticate the
frozen evidence artifact and its qualification; callers cannot approve themselves
by constructing this value. Core neither downloads evidence nor enables features.
"""

import inspect
from collections.abc import Callable
from dataclasses import dataclass

_FEATURES = frozenset({"pair-reranker", "evidence-pack-reuse", "rank-and-reuse"})


def checked_digest(value: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("expected a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class RetrievalFeatureApproval:
    feature: str
    configuration_sha256: str
    evidence_sha256: str
    protocol_sha256: str
    rollback_id: str
    qualification: str = "controlled-real"
    schema: str = "retrieval-feature-approval/1"

    def __post_init__(self) -> None:
        if self.feature not in _FEATURES:
            raise ValueError("unknown retrieval feature")
        for name in ("configuration_sha256", "evidence_sha256", "protocol_sha256"):
            checked_digest(getattr(self, name))
        if type(self.rollback_id) is not str or not 1 <= len(self.rollback_id) <= 128:
            raise ValueError("bounded rollback identity required")
        if self.qualification != "controlled-real" or self.schema != "retrieval-feature-approval/1":
            raise ValueError("only qualified controlled-real evidence may approve activation")


def validate_feature_enablement(
    *,
    feature: str,
    configuration_sha256: str,
    enabled: bool = False,
    approval: RetrievalFeatureApproval | None = None,
    verify_approval: Callable[[RetrievalFeatureApproval], bool] | None = None,
    contract_test_only: bool = False,
) -> str:
    """Return an explicit activation mode, or reject incomplete authorization.

    contract_test_only is deliberately distinguishable in every resulting trace;
    it permits contract tests without claiming production efficacy or approval.
    The verifier is a trusted synchronous host boundary, not model-supplied code.
    """
    if feature not in _FEATURES:
        raise ValueError("unknown retrieval feature")
    checked_digest(configuration_sha256)
    if type(enabled) is not bool or type(contract_test_only) is not bool:
        raise TypeError("activation flags must be boolean")
    if not enabled:
        return "disabled"
    if contract_test_only:
        if approval is not None or verify_approval is not None:
            raise ValueError("contract-only mode cannot carry production approval")
        return "contract-test-only"
    if type(approval) is not RetrievalFeatureApproval or not callable(verify_approval):
        raise ValueError("qualified controlled evidence and a host verifier are required")
    approval.__post_init__()
    if inspect.iscoroutinefunction(verify_approval):
        raise TypeError("host approval verifier must be synchronous")
    if approval.feature != feature or approval.configuration_sha256 != configuration_sha256:
        raise ValueError("retrieval feature approval does not bind this configuration")
    verified = verify_approval(approval)
    if inspect.iscoroutine(verified):
        verified.close()
    if verified is not True:
        raise PermissionError("host did not verify retrieval feature approval")
    return "controlled-real"

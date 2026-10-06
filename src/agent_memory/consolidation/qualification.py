"""Host review of contextual assertions, persisted in the existing admission ledger.

This is not a model-callable authority grant. Sources, spans, target binding and
ordinary admission policy are checked within the version-fenced transaction.
"""

from dataclasses import replace
from hashlib import sha256

from .. import domain
from ..conditions import Condition, ProjectionPolicy, identifier
from ..domain import MemoryScope, canonical_json
from ..evidence_support import EvidenceLink, FieldSupport, evaluate_support
from ..lifecycle import is_memory_context
from ..operations.source_revisions import source_is_current
from ..serialization import to_jsonable
from .admission import draft_from_payload, draft_to_payload


def target_fingerprint(draft):
    return sha256(canonical_json(draft_to_payload(draft)).encode()).hexdigest()


class ContextualMemory:
    """Exact-scope host facade; authenticated principal and routing are bound at setup."""

    def __init__(self, engine, scope, *, principal):
        if not isinstance(scope, MemoryScope):
            raise ValueError("qualification requires an exact scope")
        identifier(principal)
        engine._require_support()
        if not callable(getattr(engine.repository.unit_of_work(), "get_source_event", None)):
            raise NotImplementedError("provider lacks exact-source qualification reads")
        self.engine, self.scope, self.principal = engine, scope, principal

    async def qualify(
        self,
        candidate_id,
        *,
        expected_version,
        admission_policy,
        projection_policy,
        applicability_id,
        conditions=(),
        exceptions=(),
        links=(),
        field_support=(),
    ):
        identifier(applicability_id)
        if type(expected_version) is not int or expected_version < 1:
            raise ValueError("expected contribution version is required")
        if not isinstance(projection_policy, ProjectionPolicy):
            raise ValueError("host projection policy required")
        conditions, exceptions, links, field_support = map(
            tuple, (conditions, exceptions, links, field_support)
        )
        if (
            len(conditions) > 16
            or len(exceptions) > 16
            or any(not isinstance(c, Condition) for c in (*conditions, *exceptions))
        ):
            raise ValueError("invalid host condition binding")
        if not 1 <= len(links) <= 64 or any(
            not isinstance(evidence, EvidenceLink) for evidence in links
        ):
            raise ValueError("expected bounded host evidence links")
        if len({evidence.id for evidence in links}) != len(links):
            raise ValueError("duplicate evidence link identity")
        if len(field_support) > 8 or any(not isinstance(f, FieldSupport) for f in field_support):
            raise ValueError("invalid field support expressions")
        if len({f.field for f in field_support}) != len(field_support):
            raise ValueError("duplicate field support expression")
        async with self.engine.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            row = await uow.get_admission_record(self.scope, candidate_id)
            if (
                row is None
                or row["scope"] != to_jsonable(self.scope)
                or row["version"] != expected_version
            ):
                raise ValueError("candidate unavailable or version changed")
            payload = row["payload"]
            if payload["action"] != "PENDING_VERIFICATION" or payload["claim_id"] is not None:
                raise ValueError("contextual qualification requires an unpublished candidate")
            draft = draft_from_payload(payload["draft"])
            if (
                draft.change_kind != "replace"
                or draft.negated
                or draft.modality != "asserted"
                or draft.kind not in {"fact", "preference"}
            ):
                raise ValueError(
                    "only ordinary non-negated fact/preference states support contextual projection"
                )
            if len(conditions) != len(draft.conditions) or len(exceptions) != len(draft.exceptions):
                raise ValueError("every original condition and exception requires a host binding")
            reports = payload.get("extraction", {}).get("reports", [])
            if reports and any(
                r.get("faithfulness") != "supported" or r.get("retention") != "durable"
                for r in reports
            ):
                raise ValueError("source review is not qualified")
            source = await uow.get_source_event(self.scope, row["event_id"])
            if source is None or (
                "_retention" in source.metadata and not await source_is_current(uow, source)
            ):
                raise ValueError("candidate source unavailable or superseded")
            if not draft.source_quote or draft.source_quote not in source.content:
                raise ValueError("candidate quote is not grounded in its primary source")
            specs = {s["predicate"]: s for s in admission_policy.config_payload()["predicates"]}
            if draft.predicate not in specs:
                raise ValueError("predicate not registered")
            spec = specs[draft.predicate]
            required = {
                "subject_id",
                "predicate",
                "value",
                *spec.get("required_evidence_fields", ()),
            }
            if draft.valid_from is not None:
                required.add("valid_from")
            if draft.valid_to is not None:
                required.add("valid_to")
            if draft.conditions:
                required.add("conditions")
            if draft.exceptions:
                required.add("exceptions")
            if not required <= {f.field for f in field_support}:
                raise ValueError("required fields or inseparable qualifiers lack support")
            sources, normalized, families = {}, [], {}
            target = target_fingerprint(draft)
            # Strip qualifiers only for ordinary type/authority validation; all
            # original semantics remain in the candidate and required proof fields.
            ordinary = replace(draft, conditions=(), exceptions=(), field_evidence=())
            for link in links:
                if link.target_sha256 != target:
                    raise ValueError("evidence target binding mismatch")
                event = await uow.get_source_event(self.scope, link.span.source_event_id)
                if (
                    event is None
                    or is_memory_context(event)
                    or event.metadata.get("lifecycle", {}).get("origin") == "model"
                ):
                    raise ValueError("source unavailable or not independent evidence")
                origin = event.metadata.get("lifecycle", {}).get("origin")
                if origin is not None and origin != {
                    "self_report": "user",
                    "tool_observation": "tool",
                    "document": "host",
                }.get(link.authority.kind):
                    raise ValueError("source origin does not match evidence authority")
                if "_retention" in event.metadata and not await source_is_current(uow, event):
                    raise ValueError("evidence source superseded")
                if event.content[
                    link.span.start : link.span.end
                ] != link.span.quote or link.span.end > len(event.content):
                    raise ValueError("evidence span mismatch")
                if (
                    draft.subject_id not in link.authority.subjects
                    or draft.predicate not in link.authority.predicates
                ):
                    raise ValueError("evidence authority mismatch")
                action, reasons = admission_policy.evaluate(
                    event, replace(ordinary, source_quote=link.span.quote), link.authority
                )
                # Field qualification is evaluated jointly below; quote matching
                # establishes provenance, not semantic support.
                if action != "ACCEPT" and set(reasons) != {"required_field_evidence_missing"}:
                    raise ValueError("evidence failed ordinary admission policy")
                family = link.source_family
                retained_family = event.metadata.get("_retention", {}).get("document_id")
                if retained_family:
                    if family is not None and family != retained_family:
                        raise ValueError("retained source family mismatch")
                    family = retained_family
                if event.id in families and families[event.id] != family:
                    raise ValueError("inconsistent source family")
                families[event.id] = family
                sources[event.id] = event
                normalized.append(
                    {
                        **to_jsonable(link),
                        "source_family": family,
                        "source_sha256": event.content_hash,
                    }
                )
            link_by_id = {evidence.id: evidence for evidence in links}
            for expression in field_support:
                for group in expression.alternatives:
                    if any(
                        i not in link_by_id or expression.field not in link_by_id[i].fields
                        for i in group
                    ):
                        raise ValueError("field support references an unrelated evidence link")
            qualification = {
                "schema": "contextual-qualification/1",
                "applicability_id": applicability_id,
                "conditions": to_jsonable(conditions),
                "exceptions": to_jsonable(exceptions),
                "links": normalized,
                "field_support": to_jsonable(field_support),
                "policy": to_jsonable(projection_policy),
                "policy_sha256": projection_policy.fingerprint,
                "admission_policy": admission_policy.config_payload(),
                "target_sha256": target,
                "principal": self.principal,
            }
            if len(canonical_json(qualification).encode()) > 128_000:
                raise ValueError("qualification payload budget exceeded")
            # The candidate stays pending for unconditional use. Only the explicit
            # context query may resolve a fully supported projection.
            # Validate full enumeration before persisting. No temporal support is
            # still an unknown projection, not an unconditional acceptance.
            evaluate_support(qualification, sources)
            payload.update(
                action="PENDING_VERIFICATION",
                reasons=["host_reviewed_contextual_projection"],
                qualification=qualification,
            )
            payload["decisions"].append(
                {
                    "action": "PENDING_VERIFICATION",
                    "reasons": payload["reasons"],
                    "recorded_at": domain.utc_now().isoformat(),
                    "policy_sha256": projection_policy.fingerprint,
                }
            )
            return await uow.save_admission_record(
                self.scope, row["id"], row["event_id"], row["slot_key"], payload, expected_version
            )

    async def query(self, context, *, predicate, policy):
        from ..retrieval.contextual_state import query

        return await query(self, context, predicate=predicate, policy=policy)

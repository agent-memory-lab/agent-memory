"""Host-authorized, exact-scope contribution operations in one admission transaction.

The closed slot version set fences both stale rows and phantom contributions.
Cross-slot, contextual and composite-source corrections remain unsupported.
"""

from dataclasses import replace
from datetime import datetime
from hashlib import sha256

from ..conditions import identifier
from ..contribution_state import enroll
from ..domain import MemoryScope, canonical_json
from ..lifecycle import is_memory_context
from ..serialization import to_jsonable
from .admission import (
    authority_from_payload,
    authority_to_payload,
    draft_from_payload,
    draft_to_payload,
    slot_key,
)


class ContributionMemory:
    """Bind an authenticated host principal; never expose this as model authority."""

    def __init__(self, engine, scope, *, principal):
        if not isinstance(scope, MemoryScope):
            raise ValueError("contribution operations require an exact scope")
        identifier(principal)
        engine._require_support()
        if not callable(getattr(engine.repository.unit_of_work(), "list_admission_barriers", None)):
            raise NotImplementedError("provider lacks contribution barrier reads")
        self.engine, self.scope, self.principal = engine, scope, principal

    async def snapshot(self, candidate_id):
        async with self.engine.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            row = await uow.get_admission_record(self.scope, candidate_id)
            if row is None or row["scope"] != to_jsonable(self.scope):
                raise ValueError("contribution unavailable in exact scope")
            rows = await uow.list_admission_records(self.scope, row["slot_key"])
            barriers = await uow.list_admission_barriers(self.scope, row["slot_key"])
            return {r["id"]: r["version"] for r in (*rows, *barriers)}

    async def _rows(self, uow, candidate_id, expected_versions):
        row = await uow.get_admission_record(self.scope, candidate_id)
        if row is None or row["scope"] != to_jsonable(self.scope):
            raise ValueError("contribution unavailable in exact scope")
        rows = list(await uow.list_admission_records(self.scope, row["slot_key"]))
        barriers = await uow.list_admission_barriers(self.scope, row["slot_key"])
        if (
            not isinstance(expected_versions, dict)
            or any(type(v) is not int or v < 1 for v in expected_versions.values())
            or expected_versions != {r["id"]: r["version"] for r in (*rows, *barriers)}
        ):
            raise ValueError("slot contribution versions changed")
        if len(rows) > 64:
            raise ValueError("contribution slot capacity exceeded")
        for item in rows:
            p = item["payload"]
            d = p["draft"]
            if (
                item["scope"] != to_jsonable(self.scope)
                or p.get("qualification")
                or p.get("termination")
                or p.get("interpretation_request")
                or p.get("corrects")
                or d["change_kind"] != "replace"
                or d.get("conditions")
                or d.get("exceptions")
                or d.get("negated")
                or d["kind"] not in {"fact", "preference"}
                or p["source_event_ids"] != [item["event_id"]]
                or p["action"] not in {"ACCEPT", "WITHDRAWN", "CONTESTED"}
            ):
                raise ValueError("unsupported contribution slot semantics")
        enroll(rows)
        return rows, next(r for r in rows if r["id"] == candidate_id)

    async def _command(self, uow, event, operation):
        if event.scope != self.scope or is_memory_context(event) or len(event.content) > 32_000:
            raise ValueError("operation evidence requires independent exact-scope source")
        fingerprint = sha256(
            canonical_json(
                {
                    "principal": self.principal,
                    "operation": operation,
                    "event": {
                        "content": event.content,
                        "actor": event.actor,
                        "occurred_at": event.occurred_at.isoformat(),
                        "source_uri": event.source_uri,
                    },
                }
            ).encode()
        ).hexdigest()
        event = replace(
            event,
            event_type="memory.atom.verification",
            content_hash="",
            idempotency_key=event.idempotency_key or event.id,
            metadata={"contribution_operation": fingerprint},
        )
        saved = await uow.find_event_by_idempotency(self.scope, event.idempotency_key)
        if saved:
            if saved.metadata.get("contribution_operation") != fingerprint:
                raise ValueError("contribution operation identity conflict")
            result = saved.metadata["contribution_result"]
            if not await uow.events_exist(self.scope, (saved.id,)):
                raise ValueError("operation evidence unavailable")
            for identity in result["candidate_ids"]:
                if await uow.get_admission_record(self.scope, identity) is None:
                    raise ValueError("operation result erased")
            return event, {**result, "duplicate": True}
        return event, None

    def _authorize(self, event, row, authority, policy, source_quote):
        draft = replace(draft_from_payload(row["payload"]["draft"]), source_quote=source_quote)
        action, reasons = policy.evaluate(event, draft, authority)
        if action != "ACCEPT":
            raise ValueError("operation evidence failed admission: " + ", ".join(reasons))

    async def _save(self, uow, rows, event, *, candidate_ids):
        for row in rows:
            await uow.save_admission_record(
                self.scope,
                row["id"],
                row["event_id"],
                row["slot_key"],
                row["payload"],
                row["version"],
            )
        result = {"operation_id": event.id, "candidate_ids": list(candidate_ids)}
        await uow.append_event(
            replace(
                event,
                metadata={
                    **event.metadata,
                    "contribution_result": result,
                },
                content_hash="",
            )
        )
        return {**result, "duplicate": False}

    async def withdraw(
        self,
        candidate_id,
        *,
        event,
        expected_versions,
        authority,
        policy,
        source_quote,
    ):
        """Withdraw one contribution, preserving independent peers and prior knowledge."""
        return await self._change(
            candidate_id,
            event=event,
            expected_versions=expected_versions,
            authority=authority,
            policy=policy,
            source_quote=source_quote,
        )

    async def correct(
        self,
        candidate_id,
        *,
        event,
        expected_versions,
        authority,
        policy,
        source_quote,
        replacement_event,
        replacement,
        replacement_authority,
    ):
        """Atomic same-slot withdrawal + newly evaluated contribution, without evidence transfer."""
        return await self._change(
            candidate_id,
            event=event,
            expected_versions=expected_versions,
            authority=authority,
            policy=policy,
            source_quote=source_quote,
            replacement_event=replacement_event,
            replacement=replacement,
            replacement_authority=replacement_authority,
        )

    async def add(
        self,
        anchor_id,
        *,
        event,
        expected_versions,
        authority,
        policy,
        source_quote,
        replacement_event,
        replacement,
        replacement_authority,
    ):
        """Add fresh evidence to a managed slot under the same closed-set version fence."""
        return await self._change(
            anchor_id,
            event=event,
            expected_versions=expected_versions,
            authority=authority,
            policy=policy,
            source_quote=source_quote,
            replacement_event=replacement_event,
            replacement=replacement,
            replacement_authority=replacement_authority,
            withdraw_target=False,
        )

    async def _requalify(self, uow, rows, policy):
        for row in rows:
            payload = row["payload"]
            if payload["action"] != "CONTESTED" or not payload["evidence_qualified"]:
                continue
            payload.update(action="ACCEPT", reasons=["independent_conflict_removed"])
            self.engine._reconcile(row, rows, ())
            if payload["action"] == "ACCEPT":
                source = await uow.get_source_event(self.scope, row["event_id"])
                if source is None:
                    raise ValueError("independent support unavailable")
                # Recheck against today's supplied host policy before publication.
                action, _ = policy.evaluate(
                    source,
                    draft_from_payload(payload["draft"]),
                    authority_from_payload(payload["authority"]),
                )
                if action != "ACCEPT":
                    raise ValueError("independent support requires renewed qualification")
                await self.engine._publish(uow, row, source)
                self.engine._decision(payload, policy)

    async def _change(
        self,
        candidate_id,
        *,
        event,
        expected_versions,
        authority,
        policy,
        source_quote,
        replacement_event=None,
        replacement=None,
        replacement_authority=None,
        withdraw_target=True,
    ):
        operation = {
            "kind": ("correct" if replacement is not None else "withdraw")
            if withdraw_target
            else "add",
            "target": candidate_id,
            "expected_versions": expected_versions,
            "authority": authority_to_payload(authority),
            "policy": policy.config_payload(),
            "quote": source_quote,
        }
        if replacement is not None:
            operation["replacement"] = {
                "draft": draft_to_payload(replacement),
                "event": to_jsonable(replacement_event),
                "authority": authority_to_payload(replacement_authority),
            }
        async with self.engine.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            event, duplicate = await self._command(uow, event, operation)
            if duplicate:
                return duplicate
            rows, row = await self._rows(uow, candidate_id, expected_versions)
            if withdraw_target and row["payload"]["action"] not in {"ACCEPT", "CONTESTED"}:
                raise ValueError("contribution already withdrawn")
            self._authorize(event, row, authority, policy, source_quote)
            if withdraw_target:
                row["payload"].update(action="WITHDRAWN", reasons=["host_withdrew_contribution"])
                row["payload"]["contribution"]["decision_event_id"] = event.id
                self.engine._decision(row["payload"], policy)
            await self._requalify(uow, rows, policy)
            # Save the withdrawn contribution before evaluating the replacement.
            # All changes, including source append and index publication, roll back together.
            for item in rows:
                saved = await uow.save_admission_record(
                    self.scope,
                    item["id"],
                    item["event_id"],
                    item["slot_key"],
                    item["payload"],
                    item["version"],
                )
                item["version"] = saved
            ids = [candidate_id]
            if replacement is not None:
                if len(rows) >= 64:
                    raise ValueError("contribution slot capacity exceeded")
                if (
                    replacement_event.scope != self.scope
                    or self.scope.project(replacement.scope_level) != self.scope
                    or slot_key(self.scope, replacement) != row["slot_key"]
                    or replacement.change_kind != "replace"
                    or replacement.conditions
                    or replacement.exceptions
                    or replacement.negated
                    or replacement.kind not in {"fact", "preference"}
                    or replacement_event.id == event.id
                ):
                    raise ValueError("cross-slot or complex correction is unsupported")
                barriers = await uow.list_admission_barriers(self.scope, row["slot_key"])
                start = replacement.valid_from or replacement_event.occurred_at
                if any(
                    start < datetime.fromisoformat(b["payload"]["valid_from"]) for b in barriers
                ):
                    raise ValueError("backfill across erased boundary requires continuity review")
                receipt = await self.engine.admit(
                    replacement_event,
                    [replacement],
                    authority=replacement_authority,
                    policy=policy,
                    _unit_of_work=uow,
                    _contribution_write=True,
                )
                if receipt.duplicate or receipt.decisions[0].action not in {"ACCEPT", "CONTESTED"}:
                    raise ValueError("correction requires a fresh qualified contribution")
                fresh = await uow.get_admission_record(self.scope, receipt.candidate_ids[0])
                rows.append(fresh)
                enroll(rows)
                ids.extend(receipt.candidate_ids)
            return await self._save(uow, rows, event, candidate_ids=ids)

    async def transition(
        self,
        successor_id,
        *,
        predecessor_ids,
        valid_from,
        event,
        expected_versions,
        authority,
        policy,
        source_quote,
    ):
        """Close the predecessor with end evidence separate from new-value support."""
        from .transitions import transition

        return await transition(
            self,
            successor_id,
            predecessor_ids=predecessor_ids,
            valid_from=valid_from,
            event=event,
            expected_versions=expected_versions,
            authority=authority,
            policy=policy,
            source_quote=source_quote,
        )

    async def correct_transition(
        self,
        predecessor_id,
        *,
        transition_id,
        valid_to,
        event,
        expected_versions,
        authority,
        policy,
        source_quote,
    ):
        """Reopen predecessor continuity only after a new authorized evidence review."""
        from .transitions import correct_transition

        return await correct_transition(
            self,
            predecessor_id,
            transition_id=transition_id,
            valid_to=valid_to,
            event=event,
            expected_versions=expected_versions,
            authority=authority,
            policy=policy,
            source_quote=source_quote,
        )

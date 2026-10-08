"""Durable multi-account monetary reservations, independent of resource-unit quotas.

All operations use one repository transaction and an explicit budget lock before
any lifecycle lock. No provider I/O occurs while this lock is held. Unknown debt
survives crashes, expiry, account-window changes, and failed delivery.
"""

from dataclasses import asdict, dataclass
from uuid import uuid4

from ..retrieval.model_contracts import ModelError, count, digest, hash_value, text

CONTRACT = "model-budget/1"


@dataclass(frozen=True, slots=True)
class BudgetAccount:
    identity: str
    window: str
    currency: str
    configuration: str
    hard_limit_microunits: int | None

    def __post_init__(self):
        for key in ("identity", "window", "currency", "configuration"):
            text(getattr(self, key))
        if len(self.currency) != 3 or not all("A" <= letter <= "Z" for letter in self.currency):
            raise ModelError("invalid_model_currency")
        count(self.hard_limit_microunits, optional=True)

    @property
    def key(self):
        return digest(asdict(self))


class ModelBudget:
    """Minimal finance metadata only. Source IDs and model bodies never enter it."""

    def __init__(self, repository):
        self.repository = repository

    @staticmethod
    async def lock(uow):
        if getattr(uow, "model_budget_contract", None) != CONTRACT:
            raise ModelError("model_budget_backend_unsupported")
        await uow.model_budget_lock()

    async def configure(self, accounts):
        accounts = tuple(accounts)
        if (
            not accounts
            or len(accounts) > 16
            or any(type(a) is not BudgetAccount for a in accounts)
        ):
            raise ModelError("invalid_model_accounts")
        if len({a.key for a in accounts}) != len(accounts):
            raise ModelError("duplicate_model_accounts")
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            for account in accounts:
                old = await uow.model_budget_get("account", account.key)
                if old is None:
                    # Do not persist potentially identifying display names.
                    await uow.model_budget_put(
                        "account",
                        account.key,
                        dict(
                            key=account.key,
                            currency=account.currency,
                            hard_limit=account.hard_limit_microunits,
                            settled=0,
                            reserved=0,
                            unknown_reservations=0,
                        ),
                    )
        return tuple(sorted(a.key for a in accounts))

    async def reserve(
        self,
        *,
        operation_id,
        attempt_id,
        request_sha256,
        account_keys,
        maximum_microunits,
        upper_bound_evidence=None,
        phase="foreground",
        provider="unspecified",
        model_role="generation_model",
        configuration_sha256=None,
    ):
        """A retry is a new attempt unless a provider proves idempotent billing.

        ``None`` is explicit unbounded/unknown debt and is allowed only on soft
        accounts. A hard budget additionally requires a documented maximum,
        not an average-price estimate or an unpriced local-model assumption.
        """
        if phase not in {
            "cold_start",
            "registration",
            "prewarm",
            "write",
            "dependency",
            "background",
            "foreground",
            "failure",
            "retry",
            "drain",
        }:
            raise ModelError("invalid_model_cost_phase")
        if model_role not in {"semantic_model", "generation_model"}:
            raise ModelError("invalid_model_cost_role")
        text(provider)
        if configuration_sha256 is not None:
            hash_value(configuration_sha256)
        text(operation_id)
        text(attempt_id)
        hash_value(request_sha256)
        count(maximum_microunits, optional=True)
        if maximum_microunits == 0:
            raise ModelError("unknown_model_cost_cannot_reserve_zero")
        keys = tuple(sorted(account_keys))
        if not keys or len(keys) > 16 or len(set(keys)) != len(keys):
            raise ModelError("invalid_model_accounts")
        for key in keys:
            hash_value(key)
        if upper_bound_evidence is not None:
            text(upper_bound_evidence, limit=2048)
        key = digest([operation_id, attempt_id])
        immutable = dict(
            request_sha256=request_sha256,
            accounts=list(keys),
            phase=phase,
            provider=provider,
            model_role=model_role,
            configuration_sha256=configuration_sha256,
            maximum_microunits=maximum_microunits,
            upper_bound_evidence_sha256=(
                digest(upper_bound_evidence) if upper_bound_evidence else None
            ),
        )
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            old = await uow.model_budget_get("call", key)
            if old is not None:
                if any(old.get(k) != v for k, v in immutable.items()):
                    raise ModelError("model_reservation_conflict")
                return old
            accounts = [await uow.model_budget_get("account", a) for a in keys]
            if any(a is None for a in accounts):
                raise ModelError("model_account_missing")
            if len({a["currency"] for a in accounts}) != 1:
                raise ModelError("model_account_currency_mismatch")
            for account in accounts:
                if account["hard_limit"] is not None:
                    if maximum_microunits is None or upper_bound_evidence is None:
                        raise ModelError("strict_model_cost_bound_unavailable")
                    if (
                        account["unknown_reservations"]
                        or account["settled"] + account["reserved"] + maximum_microunits
                        > account["hard_limit"]
                    ):
                        raise ModelError("model_budget_exhausted")
            for account in accounts:
                account["reserved"] += maximum_microunits or 0
                account["unknown_reservations"] += maximum_microunits is None
                await uow.model_budget_put("account", account["key"], account)
            row = dict(
                key=key,
                call_id=uuid4().hex,
                state="reserved",
                **immutable,
                currency=accounts[0]["currency"],
                actual_microunits=None,
                receipt_sha256=None,
                provider_request_sha256=None,
                input_tokens=None,
                output_tokens=None,
                total_duration_ns=None,
                outcome="not_started",
            )
            await uow.model_budget_put("call", key, row)
            return row

    async def intent(self, uow, key, request_sha256):
        """Called in the SAME transaction as current dispatch authorization."""
        await self.lock(uow)
        row = await uow.model_budget_get("call", key)
        if row is None or row["request_sha256"] != request_sha256:
            raise ModelError("model_reservation_missing")
        if row["state"] != "reserved":
            raise ModelError("model_dispatch_already_claimed")
        row["state"] = "dispatch_intent"
        row["outcome"] = "inflight"
        await uow.model_budget_put("call", key, row)
        return row

    async def pending(self, key, *, response=None):
        """No release on timeout, cancellation, unknown invoice, or a lost receipt."""
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            row = await uow.model_budget_get("call", key)
            if not row or row["state"] not in {
                "dispatch_intent",
                "reconciliation_pending",
                "settled",
            }:
                raise ModelError("invalid_model_settlement_state")
            if row["state"] != "settled":
                row["state"] = "reconciliation_pending"
            if response is not None:
                for field in ("input_tokens", "output_tokens", "total_duration_ns"):
                    value = getattr(response, field)
                    if row[field] is not None and value not in (None, row[field]):
                        raise ModelError("model_usage_receipt_conflict")
                    if value is not None:
                        row[field] = value
                if response.provider_request_id:
                    provider = digest(response.provider_request_id)
                    if row["state"] == "settled" and row["provider_request_sha256"] != provider:
                        raise ModelError("model_settled_identity_frozen")
                    if row["provider_request_sha256"] not in (None, provider):
                        raise ModelError("model_provider_request_mismatch")
                    row["provider_request_sha256"] = provider
            await uow.model_budget_put("call", key, row)
            return row

    async def settle(self, key, *, actual_microunits, receipt_id, provider_request_id=None):
        count(actual_microunits)
        text(receipt_id)
        if provider_request_id is not None:
            text(provider_request_id)
        receipt_hash = digest([receipt_id, provider_request_id])
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            row = await uow.model_budget_get("call", key)
            if not row:
                raise ModelError("model_reservation_missing")
            observed_provider = row["provider_request_sha256"]
            supplied_provider = digest(provider_request_id) if provider_request_id else None
            if observed_provider is not None and observed_provider != supplied_provider:
                raise ModelError("model_provider_request_mismatch")
            if row["state"] == "settled":
                if (row["actual_microunits"], row["receipt_sha256"]) != (
                    actual_microunits,
                    receipt_hash,
                ):
                    raise ModelError("model_settlement_conflict")
                return row
            if row["state"] not in {"dispatch_intent", "reconciliation_pending"}:
                raise ModelError("invalid_model_settlement_state")
            provider_hash = (
                digest(["provider-request", digest(provider_request_id)])
                if provider_request_id
                else None
            )
            if provider_hash:
                seen = await uow.model_budget_get("receipt", provider_hash)
                if seen is not None and seen["call_key"] != key:
                    raise ModelError("model_provider_request_reused")
            previous = await uow.model_budget_get("receipt", receipt_hash)
            if previous is not None and previous["call_key"] != key:
                raise ModelError("model_receipt_reused")
            for account_key in row["accounts"]:
                account = await uow.model_budget_get("account", account_key)
                account["reserved"] -= row["maximum_microunits"] or 0
                account["unknown_reservations"] -= row["maximum_microunits"] is None
                account["settled"] += actual_microunits
                await uow.model_budget_put("account", account_key, account)
            row.update(
                state="settled",
                actual_microunits=actual_microunits,
                receipt_sha256=receipt_hash,
                provider_request_sha256=digest(provider_request_id)
                if provider_request_id
                else None,
                bound_exceeded=(
                    row["maximum_microunits"] is not None
                    and actual_microunits > row["maximum_microunits"]
                ),
            )
            await uow.model_budget_put("call", key, row)
            await uow.model_budget_put("receipt", receipt_hash, {"call_key": key})
            if provider_hash:
                await uow.model_budget_put("receipt", provider_hash, {"call_key": key})
            return row

    async def release(self, key):
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            row = await uow.model_budget_get("call", key)
            if not row or row["state"] not in {"reserved", "released"}:
                raise ModelError("model_unknown_cost_cannot_release")
            if row["state"] == "released":
                return row
            for account_key in row["accounts"]:
                account = await uow.model_budget_get("account", account_key)
                account["reserved"] -= row["maximum_microunits"] or 0
                account["unknown_reservations"] -= row["maximum_microunits"] is None
                await uow.model_budget_put("account", account_key, account)
            row["state"] = "released"
            await uow.model_budget_put("call", key, row)
            return row

    async def outcome(self, key, value):
        if value not in {"completed", "failed", "output_rejected", "publication_rejected"}:
            raise ModelError("invalid_model_execution_outcome")
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            row = await uow.model_budget_get("call", key)
            if not row or row["state"] in {"reserved", "released"}:
                raise ModelError("invalid_model_execution_state")
            row["outcome"] = value
            await uow.model_budget_put("call", key, row)

    async def snapshot(self):
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            return tuple(await uow.model_budget_records("call"))

    async def export(self):
        """Host-only current finance snapshot; pin checkpoint outside the backup.

        Restore is an offline procedure. A database backup is not the authority
        for costs incurred after that backup. The host must export the current
        money authority and replay it before reopening model dispatch.
        """
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            body = dict(
                schema="model-budget-checkpoint/1",
                accounts=await uow.model_budget_records("account"),
                calls=await uow.model_budget_records("call"),
            )
            return {**body, "checkpoint": digest(body)}

    async def replay(self, snapshot, *, expected_checkpoint):
        """Merge a separately pinned current checkpoint; never reduce local debt."""
        import json

        from ..retrieval.model_contracts import canonical

        hash_value(expected_checkpoint)
        # Own immutable JSON before checking it. Only the trusted host supplies
        # the external checkpoint; accepting a hash from the same backup is unsafe.
        data = canonical(snapshot)
        if len(data.encode()) > 16 * 1024 * 1024:
            raise ModelError("model_budget_restore_capacity")
        snapshot = json.loads(data)
        body = {key: value for key, value in snapshot.items() if key != "checkpoint"}
        if (
            set(body) != {"schema", "accounts", "calls"}
            or body["schema"] != "model-budget-checkpoint/1"
            or digest(body) != expected_checkpoint
            or snapshot.get("checkpoint") != expected_checkpoint
        ):
            raise ModelError("model_budget_checkpoint_mismatch")
        accounts, calls = body["accounts"], body["calls"]
        if (
            not isinstance(accounts, list)
            or not isinstance(calls, list)
            or len(accounts) > 4096
            or len(calls) > 4096
        ):
            raise ModelError("model_budget_restore_capacity")
        phases = {
            "reserved": 0,
            "dispatch_intent": 1,
            "reconciliation_pending": 2,
            "settled": 3,
            "released": 3,
        }
        try:
            if len({r["key"] for r in accounts}) != len(accounts) or len(
                {r["key"] for r in calls}
            ) != len(calls):
                raise ValueError
            for row in (*accounts, *calls):
                hash_value(row["key"])
            for row in accounts:
                if set(row) != {
                    "key",
                    "currency",
                    "hard_limit",
                    "settled",
                    "reserved",
                    "unknown_reservations",
                }:
                    raise ValueError
                if (
                    type(row["currency"]) is not str
                    or len(row["currency"]) != 3
                    or not all("A" <= letter <= "Z" for letter in row["currency"])
                ):
                    raise ValueError
                count(row["hard_limit"], optional=True)
                for field in ("settled", "reserved", "unknown_reservations"):
                    count(row[field])
            known = {r["key"] for r in accounts}
            for row in calls:
                if row["state"] not in phases or not set(row["accounts"]) <= known:
                    raise ValueError
                count(row["maximum_microunits"], optional=True)
                count(row["actual_microunits"], optional=True)
                if row["maximum_microunits"] == 0 or len(set(row["accounts"])) != len(
                    row["accounts"]
                ):
                    raise ValueError
                if not 1 <= len(row["accounts"]) <= 16:
                    raise ValueError
                hash_value(row["request_sha256"])
                if row["state"] != "settled" and row["actual_microunits"] is not None:
                    raise ValueError
                if row["state"] == "settled" and (
                    row["actual_microunits"] is None or not row["receipt_sha256"]
                ):
                    raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ModelError("model_budget_checkpoint_invalid") from None
        async with self.repository.unit_of_work() as uow:
            await self.lock(uow)
            existing = {r["key"]: r for r in await uow.model_budget_records("account")}
            for row in accounts:
                old = existing.get(row["key"])
                if old and any(old[k] != row[k] for k in ("currency", "hard_limit")):
                    raise ModelError("model_budget_restore_conflict")
                existing[row["key"]] = {
                    **row,
                    "settled": 0,
                    "reserved": 0,
                    "unknown_reservations": 0,
                }
            merged = {r["key"]: r for r in await uow.model_budget_records("call")}
            immutable = (
                "call_id",
                "request_sha256",
                "accounts",
                "phase",
                "provider",
                "model_role",
                "configuration_sha256",
                "maximum_microunits",
                "upper_bound_evidence_sha256",
                "currency",
            )
            for row in calls:
                old = merged.get(row["key"])
                if old:
                    if any(old[k] != row[k] for k in immutable):
                        raise ModelError("model_budget_restore_conflict")
                    if old["state"] == row["state"] == "settled" and any(
                        old[k] != row[k] for k in ("actual_microunits", "receipt_sha256")
                    ):
                        raise ModelError("model_budget_restore_conflict")
                    if "released" in (old["state"], row["state"]) and old["state"] != row["state"]:
                        if {old["state"], row["state"]} != {"reserved", "released"}:
                            raise ModelError("model_budget_restore_conflict")
                    observed_provider = old["provider_request_sha256"]
                    supplied_provider = row["provider_request_sha256"]
                    if old["state"] == "settled" and supplied_provider not in (
                        None,
                        observed_provider,
                    ):
                        raise ModelError("model_budget_restore_conflict")
                    if observed_provider is not None and supplied_provider not in (
                        None,
                        observed_provider,
                    ):
                        raise ModelError("model_budget_restore_conflict")
                    if phases[old["state"]] > phases[row["state"]]:
                        continue
                    if observed_provider is not None and supplied_provider is None:
                        if row["state"] == "settled":
                            raise ModelError("model_budget_restore_conflict")
                        row["provider_request_sha256"] = observed_provider
                    if phases[old["state"]] == phases[row["state"]]:
                        for field in (
                            "input_tokens",
                            "output_tokens",
                            "total_duration_ns",
                            "provider_request_sha256",
                        ):
                            if old.get(field) is not None:
                                if row.get(field) not in (None, old[field]):
                                    raise ModelError("model_budget_restore_conflict")
                                row[field] = old[field]
                        if old["outcome"] not in {"not_started", "inflight"}:
                            row["outcome"] = old["outcome"]
                merged[row["key"]] = row
            # Recompute all account debt exactly once per call per account;
            # older checkpoint totals never overwrite later local obligations.
            for account in existing.values():
                account.update(settled=0, reserved=0, unknown_reservations=0)
            receipts = {}
            for row in merged.values():
                for key in row["accounts"]:
                    account = existing[key]
                    if row["state"] == "settled":
                        account["settled"] += row["actual_microunits"]
                    elif row["state"] != "released":
                        account["reserved"] += row["maximum_microunits"] or 0
                        account["unknown_reservations"] += row["maximum_microunits"] is None
                if row["state"] == "settled":
                    receipt_keys = [row["receipt_sha256"]]
                    if row["provider_request_sha256"]:
                        receipt_keys.append(
                            digest(["provider-request", row["provider_request_sha256"]])
                        )
                    for key in receipt_keys:
                        prior = receipts.get(key) or await uow.model_budget_get("receipt", key)
                        if prior and prior["call_key"] != row["key"]:
                            raise ModelError("model_receipt_reused")
                        receipts[key] = {"call_key": row["key"]}
                await uow.model_budget_put("call", row["key"], row)
            for key, row in existing.items():
                await uow.model_budget_put("account", key, row)
            for key, row in receipts.items():
                await uow.model_budget_put("receipt", key, row)
            return dict(checkpoint=expected_checkpoint, calls=len(merged), accounts=len(existing))

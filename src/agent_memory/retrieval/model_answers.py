"""Exact governed answer cache and buffered model execution; opt-in host API only."""

import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from hashlib import sha256
from time import monotonic
from uuid import uuid4

from ..operations.model_budget import ModelBudget
from .model_contracts import ModelAnswer, ModelError, ModelResponse, canonical, digest


class GovernedModelAnswers:
    """One immutable input per execution; every waiter gets a fresh delivery guard.

    The cache never substitutes for a query proof. QuestionModelAuthority binds
    the input to a real registered question and its current certificate.
    """

    def __init__(
        self,
        authority,
        port,
        *,
        account_keys,
        maximum_microunits=None,
        upper_bound_evidence=None,
        validate_output=None,
        cache_seconds=300,
        max_cache_entries=128,
        cost_phase="foreground",
        model_role="generation_model",
    ):
        if port.configuration != authority.configuration:
            raise ModelError("model_configuration_mismatch")
        if not callable(validate_output):
            raise ModelError("model_output_validator_required")
        if (
            type(cache_seconds) is not int
            or not 1 <= cache_seconds <= 86400
            or type(max_cache_entries) is not int
            or not 1 <= max_cache_entries <= 4096
        ):
            raise ModelError("invalid_model_cache_limits")
        self.authority, self.port = authority, port
        self.repository, self.scope, self.clock = (
            authority.repository,
            authority.scope,
            authority.clock,
        )
        self.budget = ModelBudget(self.repository)
        self.account_keys = tuple(account_keys)
        self.maximum_microunits, self.upper_bound_evidence = (
            maximum_microunits,
            upper_bound_evidence,
        )
        self.validate_output = validate_output
        self.cache_seconds, self.max_cache_entries = cache_seconds, max_cache_entries
        self.cost_phase, self.model_role = cost_phase, model_role
        self._tasks = {}

    async def _cached(self, sealed):
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            # Only metadata is loaded before the complete authority proof.
            row = await uow.derived_get(self.scope, "model_cache_header", sealed.key)
            if not row or row.get("state") in {"erased", "expired"}:
                return None
            if datetime.fromisoformat(row["expires_at"]) <= self.clock():
                await self._expire(uow, sealed.key)
                return None
            if row.get("state") == "reserved":
                return None
            await self.authority.validate(uow, sealed)
            if (
                row["configuration_sha256"] != sealed.configuration.fingerprint
                or row["manifest_sha256"] != digest(json.loads(sealed.manifest_json))
                or row["key"] != sealed.key
            ):
                raise ModelError("model_cache_proof_invalid")
            body = await uow.derived_get(self.scope, "model_cache_body", sealed.key)
            if not body or digest(body) != row["body_sha256"]:
                raise ModelError("model_cache_body_invalid")
            response = ModelResponse(**body["response"])
            await self.authority.validate(uow, sealed)
            return (
                response,
                row["call_id"],
                row.get("reservation_key"),
                row.get("delivery_reservation"),
            )

    async def _expire(self, uow, key):
        await uow.lock_admission_scope(self.scope)
        header = await uow.derived_get(self.scope, "model_cache_header", key)
        if header:
            await self.authority.release_delivery(uow, header.get("delivery_reservation"))
        await uow.derived_put(self.scope, "model_cache_header", key, {"state": "expired"})
        if header and header.get("state") != "reserved":
            await uow.derived_put(self.scope, "model_cache_body", key, {"state": "expired"})
        await uow.derived_edges(self.scope, "model-cache:" + key, ())

    async def sweep_expired(self):
        """Bounded host maintenance; body-free inspection, physical body scrubbing."""
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            rows = await uow.derived_records(self.scope, "model_cache_header")
            expired = [
                row["identity"]
                for row in rows
                if row["payload"].get("state") not in {"expired", "erased"}
                and datetime.fromisoformat(row["payload"]["expires_at"]) <= self.clock()
            ]
            for key in expired:
                await self._expire(uow, key)
            return len(expired)

    async def _claim(self, sealed):
        async with self.repository.unit_of_work() as uow:
            await self.authority.validate(uow, sealed)
            row = await uow.derived_get(self.scope, "model_flight", sealed.key)
            if (
                row
                and row.get("state") == "running"
                and datetime.fromisoformat(row["until"]) > self.clock()
            ):
                return None
            if row is None and len(await uow.derived_records(self.scope, "model_flight")) >= 4096:
                raise ModelError("model_flight_capacity")
            token = uuid4().hex
            until = self.clock() + timedelta(seconds=sealed.configuration.timeout_seconds * 8 + 1)
            await uow.derived_put(
                self.scope,
                "model_flight",
                sealed.key,
                dict(
                    state="running",
                    token=token,
                    until=until.isoformat(),
                    sources=list(json.loads(sealed.manifest_json)["sources"]),
                    parents=self.authority.parents(sealed),
                ),
            )
            return token

    async def _reserve_cache(self, sealed, token):
        """Claim a durable publication slot before reserving money or dispatching.

        A count-only preflight is insufficient: different keys and processes
        must see each other's in-flight slots. The flight lease bounds abandoned
        reservations, while its token fences late publication and cleanup.
        """
        async with self.repository.unit_of_work() as uow:
            await self.authority.validate(uow, sealed)
            flight = await uow.derived_get(self.scope, "model_flight", sealed.key)
            if (
                not flight
                or flight.get("state") != "running"
                or flight.get("token") != token
                or datetime.fromisoformat(flight["until"]) <= self.clock()
            ):
                raise ModelError("model_execution_fenced")
            rows = await uow.derived_records(self.scope, "model_cache_header")
            if not any(r["identity"] == sealed.key for r in rows) and len(rows) >= 4096:
                raise ModelError("model_cache_audit_capacity")
            live = [
                r
                for r in rows
                if r["payload"].get("state") not in {"erased", "expired"}
                and datetime.fromisoformat(r["payload"]["expires_at"]) > self.clock()
            ]
            if (
                not any(r["identity"] == sealed.key for r in live)
                and len(live) >= self.max_cache_entries
            ):
                raise ModelError("model_cache_capacity")
            # Do not append a released finance row on every deterministic retry.
            # The real audit reservation is still atomic with dispatch below.
            await self.authority.dispatch_capacity(uow, sealed)
            old = next((r["payload"] for r in rows if r["identity"] == sealed.key), None)
            if old is not None:
                await self._expire(uow, sealed.key)
            await uow.derived_put(
                self.scope,
                "model_cache_header",
                sealed.key,
                dict(
                    state="reserved",
                    token=token,
                    expires_at=flight["until"],
                    sources=list(json.loads(sealed.manifest_json)["sources"]),
                    parents=self.authority.parents(sealed),
                ),
            )

    async def _complete_flight(self, sealed, token, state, *, delivery_reservation=None):
        async with self.repository.unit_of_work() as uow:
            # Follow the same source/scope lock order, even on failed authorization.
            await uow.lock_admission_scope(self.scope)
            row = await uow.derived_get(self.scope, "model_flight", sealed.key)
            if row and row.get("state") != "erased" and row.get("token") == token:
                row["state"] = state
                await uow.derived_put(self.scope, "model_flight", sealed.key, row)
            header = await uow.derived_get(self.scope, "model_cache_header", sealed.key)
            if header and header.get("state") == "reserved" and header.get("token") == token:
                await self._expire(uow, sealed.key)
            # A publication commit can succeed while its acknowledgment fails.
            # Consult durable state before releasing that answer's delivery slot.
            if not (
                header
                and header.get("state") not in {"reserved", "expired", "erased"}
                and header.get("delivery_reservation") == delivery_reservation
            ):
                await self.authority.release_delivery(uow, delivery_reservation)

    async def _input_guard(self, sealed):
        input_guard = getattr(self.port, "validate_input", None)
        if callable(input_guard):
            # Tokenization is processing too. Validate current source rights
            # while holding the lifecycle transaction before invoking the counter.
            async with self.repository.unit_of_work() as uow:
                await self.authority.validate(uow, sealed)
                input_guard(sealed)

    async def _execute(self, sealed):
        await self._input_guard(sealed)
        token = await self._claim(sealed)
        if token is None:
            # Other processes share the same durable flight. A dead leader never
            # frees cost debt; a later lease may try again with a NEW reservation.
            deadline = monotonic() + sealed.configuration.timeout_seconds * 8 + 1
            while monotonic() < deadline:
                cached = await self._cached(sealed)
                if cached is not None:
                    return (*cached, True)
                async with self.repository.unit_of_work() as uow:
                    await self.authority.validate(uow, sealed)
                    row = await uow.derived_get(self.scope, "model_flight", sealed.key)
                    if not row or row.get("state") != "running":
                        raise ModelError("model_shared_execution_unavailable")
                await asyncio.sleep(0.02)
            raise ModelError("model_shared_execution_pending")
        reservation = None
        delivery_reservation = None
        dispatched = False
        published = False
        try:
            cached = await self._cached(sealed)
            if cached is not None:
                return (*cached, True)
            await self._reserve_cache(sealed, token)
            reservation = await self.budget.reserve(
                operation_id=sealed.key,
                attempt_id=token,
                request_sha256=sealed.payload_sha256,
                account_keys=self.account_keys,
                maximum_microunits=self.maximum_microunits,
                upper_bound_evidence=self.upper_bound_evidence,
                phase=self.cost_phase,
                provider=sealed.configuration.provider,
                model_role=self.model_role,
                configuration_sha256=sealed.configuration.fingerprint,
            )
            async with self.repository.unit_of_work() as uow:
                await self.budget.lock(uow)
                await self.authority.validate(uow, sealed)
                flight = await uow.derived_get(self.scope, "model_flight", sealed.key)
                if (
                    not flight
                    or flight.get("state") != "running"
                    or flight.get("token") != token
                    or datetime.fromisoformat(flight["until"]) <= self.clock()
                ):
                    raise ModelError("model_execution_fenced")
                header = await uow.derived_get(self.scope, "model_cache_header", sealed.key)
                if (
                    not header
                    or header.get("state") != "reserved"
                    or header.get("token") != token
                    or datetime.fromisoformat(header["expires_at"]) <= self.clock()
                ):
                    raise ModelError("model_execution_fenced")
                delivery_reservation = await self.authority.reserve_delivery(
                    uow,
                    sealed,
                    call_id=reservation["call_id"],
                    token=token,
                    expires_at=datetime.fromisoformat(flight["until"]),
                )
                await self.authority.record(
                    uow,
                    sealed,
                    "dispatch",
                    payload_sha256=sealed.payload_sha256,
                    call_id=reservation["call_id"],
                )
                await self.budget.intent(uow, reservation["key"], sealed.payload_sha256)
                await self.authority.validate(uow, sealed)
                now = self.clock()
                if (
                    datetime.fromisoformat(flight["until"]) <= now
                    or datetime.fromisoformat(header["expires_at"]) <= now
                ):
                    raise ModelError("model_execution_fenced")
            dispatched = True
            response = await asyncio.wait_for(
                self.port.generate(sealed), sealed.configuration.timeout_seconds * 8
            )
            if type(response) is not ModelResponse:
                raise ModelError("invalid_model_response")
            await self.budget.pending(reservation["key"], response=response)
            if response.actual_microunits is not None:
                await self.budget.settle(
                    reservation["key"],
                    actual_microunits=response.actual_microunits,
                    receipt_id=response.cost_evidence,
                    provider_request_id=response.provider_request_id,
                )
            if (
                len(response.text.encode()) > sealed.configuration.max_output_bytes
                or self.validate_output(response.text, sealed) is not True
            ):
                raise ModelError("model_output_validation_failed")
            async with self.repository.unit_of_work() as uow:
                await self.authority.validate(uow, sealed)
                flight = await uow.derived_get(self.scope, "model_flight", sealed.key)
                if (
                    not flight
                    or flight.get("state") != "running"
                    or flight.get("token") != token
                    or datetime.fromisoformat(flight["until"]) <= self.clock()
                ):
                    raise ModelError("model_execution_fenced")
                slot = await uow.derived_get(self.scope, "model_cache_header", sealed.key)
                if (
                    not slot
                    or slot.get("state") != "reserved"
                    or slot.get("token") != token
                    or datetime.fromisoformat(slot["expires_at"]) <= self.clock()
                ):
                    raise ModelError("model_execution_fenced")
                sources = list(json.loads(sealed.manifest_json)["sources"])
                body = dict(
                    response=asdict(response),
                    sources=sources,
                    parents=self.authority.parents(sealed),
                )
                expires_at = self.clock() + timedelta(seconds=self.cache_seconds)
                await self.authority.retain_delivery(
                    uow,
                    sealed,
                    call_id=reservation["call_id"],
                    reservation=delivery_reservation,
                    expires_at=expires_at,
                )
                header = dict(
                    key=sealed.key,
                    call_id=reservation["call_id"],
                    reservation_key=reservation["key"],
                    delivery_reservation=delivery_reservation,
                    sources=sources,
                    parents=self.authority.parents(sealed),
                    configuration_sha256=sealed.configuration.fingerprint,
                    manifest_sha256=digest(json.loads(sealed.manifest_json)),
                    generation_manifest=json.loads(sealed.manifest_json),
                    body_sha256=digest(body),
                    expires_at=expires_at.isoformat(),
                )
                await uow.derived_put(self.scope, "model_cache_body", sealed.key, body)
                await uow.derived_put(self.scope, "model_cache_header", sealed.key, header)
                await uow.derived_edges(
                    self.scope,
                    "model-cache:" + sealed.key,
                    tuple(
                        ("processing", p)
                        for p in [
                            *("source:" + s for s in sources),
                            *self.authority.parents(sealed),
                        ]
                    ),
                )
                await self.authority.validate(uow, sealed)
                # Storage awaits cannot stretch an expired flight into a fresh
                # publication or first-delivery lease. Roll the whole commit back.
                now = self.clock()
                if (
                    datetime.fromisoformat(flight["until"]) <= now
                    or datetime.fromisoformat(slot["expires_at"]) <= now
                    or expires_at <= now
                ):
                    raise ModelError("model_execution_fenced")
            published = True
            await self.budget.outcome(reservation["key"], "completed")
            return response, reservation["call_id"], reservation["key"], delivery_reservation, False
        except BaseException as error:
            if reservation is not None:
                if dispatched:
                    # Already settled calls stay settled; unknown dispatch keeps debt.
                    try:
                        await asyncio.shield(self.budget.pending(reservation["key"]))
                        await asyncio.shield(self.budget.outcome(reservation["key"], "failed"))
                    except ModelError:
                        pass
                else:
                    # If the dispatch transaction commit was uncertain, this fails
                    # closed instead of releasing an intent.
                    try:
                        await asyncio.shield(self.budget.release(reservation["key"]))
                    except ModelError:
                        pass
            from ..derived.model import DerivedError

            if isinstance(error, (ModelError, DerivedError, asyncio.CancelledError)):
                raise
            raise ModelError("model_execution_failed") from None
        finally:
            await asyncio.shield(
                self._complete_flight(
                    sealed,
                    token,
                    "finished",
                    delivery_reservation=delivery_reservation if not published else None,
                )
            )

    async def answer(self, sealed, *, serialize=None):
        observed = await self.authority.clock_barrier()
        try:
            return await self._answer(sealed, serialize=serialize)
        except BaseException:
            await self.authority.failure_barrier(observed)
            raise

    async def _answer(self, sealed, *, serialize=None):
        await self._input_guard(sealed)
        cached = await self._cached(sealed)
        if cached is None:
            task = self._tasks.get(sealed.key)
            joined = task is not None
            if task is None:
                task = asyncio.create_task(self._execute(sealed))
                self._tasks[sealed.key] = task

                def finished(done, key=sealed.key):
                    if self._tasks.get(key) is done:
                        self._tasks.pop(key, None)
                    if not done.cancelled():
                        done.exception()  # Consume failures even when all waiters cancel.

                task.add_done_callback(finished)
            (
                response,
                call_id,
                reservation_key,
                delivery_reservation,
                cache_hit,
            ) = await asyncio.shield(task)
            cache_hit = cache_hit or joined
        else:
            response, call_id, reservation_key, delivery_reservation = cached
            cache_hit = True
        # The final serialized return object is fixed before authorization; there
        # is no later await between commit and handing it to the controlled host.
        async with self.repository.unit_of_work() as uow:
            # Finance locks precede lifecycle locks. The ledger, slot claim,
            # exact serialized bytes and final authorization share one commit.
            cost_status = await self.budget.cost_status(uow, call_id=call_id, key=reservation_key)
            delivery_id, reservation_token = await self.authority.delivery_id(
                uow, sealed, call_id=call_id, reservation=delivery_reservation
            )
            answer = ModelAnswer(
                response.text, sealed.key, call_id, delivery_id, cache_hit, cost_status
            )
            result = serialize(answer) if serialize is not None else answer
            encoded = result if serialize is not None else asdict(answer)
            serialized = canonical(encoded).encode()
            if len(serialized) > sealed.configuration.max_output_bytes:
                raise ModelError("model_delivery_budget_exceeded")
            # The serializer may return a graph owned by another task.
            # Own the exact bytes before an await and final authorization.
            if serialize is not None:
                result = json.loads(serialized)
            payload_hash = sha256(serialized).hexdigest()
            await self.authority.record(
                uow,
                sealed,
                "delivery",
                payload_sha256=payload_hash,
                call_id=call_id,
                authorization_id=delivery_id,
                reservation_token=reservation_token,
            )
        return result

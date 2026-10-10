"""Immutable published points, with certified valid windows and current security.

A known-time point is never extended across unobserved writes. The result is the
published answer, not a reconstruction of arbitrary past database states.
"""

from contextlib import nullcontext
from datetime import UTC, datetime

from .model import DerivedError, digest, identity
from .question_materialize import budget

KINDS = ("question_history_header", "question_history_body")


def sources(value):
    result = set()

    def walk(node):
        if isinstance(node, dict):
            if node.get("kind") == "source" and isinstance(node.get("id"), str):
                result.add(node["id"])
            for key, item in node.items():
                if key == "source_event_id" and isinstance(item, str):
                    result.add(item)
                elif key == "source_event_ids" and isinstance(item, (list, tuple)):
                    result.update(item)
                elif isinstance(item, (dict, list, tuple)):
                    walk(item)
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item)

    walk(value)
    if len(result) > 256 or any(type(item) is not str for item in result):
        raise DerivedError("question_history_source_capacity")
    return tuple(sorted(result))


def instant(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if type(value) is not datetime or value.utcoffset() is None:
        raise DerivedError("question_history_timezone_required")
    return value.astimezone(UTC)


def point_key(scope, kind, label, known_at):
    return "question-history:" + digest(
        [scope.partition_key(), kind, label, instant(known_at).isoformat()]
    )


class QuestionHistory:
    def __init__(self, service):
        self.service, self.scope, self.repository = service, service.scope, service.repository

    async def capture(self, label, *, actor, kind="question", unit_of_work=None):
        """Explicit host capture; enabled question publications also call this atomically."""
        if not self.service.history_points:
            raise DerivedError("question_historical_unsupported")
        identity(label)
        identity(actor)
        if unit_of_work is not None:
            return await self._capture(label, actor=actor, kind=kind, unit_of_work=unit_of_work)
        observed = await self.service._clock_barrier()
        try:
            return await self._capture(label, actor=actor, kind=kind)
        except BaseException:
            await self.service._failed_batch_clock(observed)
            raise

    async def _capture(self, label, *, actor, kind, unit_of_work=None):
        if not self.service.history_points:
            raise DerivedError("question_historical_unsupported")
        identity(label)
        identity(actor)
        controls = self._controls()
        transaction = (
            nullcontext(unit_of_work)
            if unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with transaction as uow:
            await self.service._open(uow)
            if kind == "question":
                definition = await self.service._registration(uow, label, actor)
                body = await self.service._read_in_uow(uow, label, actor=actor, record_usage=False)
                body["refresh_status"] = "idle"  # Scheduler state is not part of historical truth.
                head = await uow.derived_get(self.scope, "question_head", definition["facet_id"])
                known, end = head["validated_at"], body["valid_until"]
                readers, purpose = definition["spec"]["readers"], definition["spec"]["purpose"]
                parents = [
                    "derived:" + definition["facet_id"],
                    "derived:" + body["content_revision_id"],
                    "derived:" + body["certificate_revision_id"],
                ]
                maximum = definition["spec"]["instance"]["definition"]["max_output_bytes"]
            elif kind == "page":
                pages = self.service.pages
                registration = await pages._registration(uow, label, actor)
                head = await uow.derived_get(
                    self.scope, "question_page_head", registration["instance_id"]
                )
                await pages._guard(uow, label, actor, registration, head)
                content, certificate, blocks = await pages._load(
                    uow, head, registration, actor, current=True
                )
                body = pages._response(content, certificate, blocks)
                # Page certificates have no system-time field. A capture records
                # the actual current observation, never invents a past timestamp.
                known, end = self.service.clock().isoformat(), head["valid_until"]
                readers, purpose = registration["readers"], registration["purpose"]
                parents = [
                    "derived:" + registration["instance_id"],
                    "derived:" + content["id"],
                    "derived:" + certificate["id"],
                ]
                maximum = registration["max_output_bytes"]
                await pages._guard(uow, label, actor, registration, head)
            else:
                raise DerivedError("question_history_kind_unsupported")
            source_ids = sources(head)
            known, end = instant(known).isoformat(), instant(end).isoformat()
            guard = await self._permission(uow, source_ids, actor, purpose, controls)
            key = point_key(self.scope, kind, label, known)
            header = dict(
                schema="published-question-history/1",
                kind=kind,
                identity=label,
                known_at=known,
                valid_from=known,
                valid_to=end,
                sources=list(source_ids),
                parents=parents,
                readers=readers,
                purpose=purpose,
                epoch=await uow.retention_epoch(self.scope),
                body_sha256=digest(body),
                max_output_bytes=maximum,
                state="published",
            )
            header["sha256"] = digest(header)
            old = await uow.derived_get(self.scope, KINDS[0], key)
            if old is not None:
                if old != header:
                    raise DerivedError("question_history_point_conflict")
                await self._finish(uow, controls, guard)
                if self.service.clock() >= instant(end):
                    raise DerivedError("question_history_time_coverage_unavailable")
                return key
            if len(await uow.derived_records(self.scope, KINDS[0])) >= 4096:
                raise DerivedError("question_history_capacity")
            budget(body, maximum)
            await uow.derived_put(self.scope, KINDS[0], key, header)
            await uow.derived_put(
                self.scope,
                KINDS[1],
                key,
                dict(sources=list(source_ids), parents=parents, result=body),
            )
            await uow.derived_edges(
                self.scope,
                key,
                [("processing", "source:" + s) for s in source_ids]
                + [("processing", p) for p in parents],
            )
            guard = await self._permission(uow, source_ids, actor, purpose, controls)
            await self._finish(uow, controls, guard)
            if self.service.clock() >= instant(end):
                raise DerivedError("question_history_time_coverage_unavailable")
            return key

    def _controls(self):
        return dict(
            context=self.service._context(),
            registration_fingerprint=self.service.admission.registration_fingerprint,
        )

    async def _finish(self, uow, controls, guard):
        from ..operations.refresh_demand import observed_clock

        observed = await observed_clock(uow, self.scope, self.service.clock)
        self.service._input_guard(controls, observed)
        guard()

    async def _permission(self, uow, source_ids, actor, purpose, controls):
        from ..operations.refresh_demand import observed_clock

        observed = await observed_clock(uow, self.scope, self.service.clock)
        self.service._input_guard(controls, observed)
        if purpose != self.service.admission.purpose:
            raise DerivedError("question_history_purpose_changed")
        authority = await self.service.registry.authority(uow, self.service.admission.authority_id)
        self.service.registry.permission(authority, (actor,), (purpose,))
        grants = await self.service.admission._grants(uow, source_ids, self.service.clock())
        if any(actor not in grant["readers"] for grant in grants.values()):
            raise DerivedError("derived_read_denied")
        for source_id in source_ids:
            if await uow.derived_project_source_proof(self.scope, source_id) is None:
                raise DerivedError("question_history_source_unavailable")
        deadlines = [
            instant(grant["expires_at"]) for grant in grants.values() if grant.get("expires_at")
        ]
        if authority is not None:
            deadlines.append(instant(authority["spec"]["expires_at"]))

        def guard():
            # Historical business facts stay immutable, but their delivery never
            # inherits an earlier context, host registration or expired grant.
            self.service._input_guard(controls, observed)
            if authority is not None:
                self.service.registry._authority_floor(authority)
            now = instant(self.service.clock())
            if now < observed:
                raise DerivedError("refresh_clock_discontinuity")
            if any(now >= deadline for deadline in deadlines):
                raise DerivedError("project_processing_grant_expired")

        guard()
        return guard

    async def read(self, label, *, actor, known_at, valid_at, kind="question"):
        if not self.service.history_points:
            raise DerivedError("question_historical_unsupported")
        identity(label)
        identity(actor)
        known, valid = instant(known_at), instant(valid_at)
        observed = await self.service._clock_barrier()
        try:
            return await self._read(label, actor, known, valid, kind)
        except BaseException:
            await self.service._clock_barrier(observed_at=max(observed, self.service.clock()))
            raise

    async def _read(self, label, actor, known, valid, kind):
        controls = self._controls()
        if known > self.service.clock():
            raise DerivedError("question_history_future_known_time")
        key = point_key(self.scope, kind, label, known)
        async with self.repository.unit_of_work() as uow:
            await self.service._open(uow)
            if kind == "question":
                await self.service._registration(uow, label, actor)
            elif kind == "page":
                await self.service.pages._registration(uow, label, actor)
            else:
                raise DerivedError("question_history_kind_unsupported")
            head = await uow.derived_get(self.scope, KINDS[0], key)
            if not head or head.get("state") != "published":
                raise DerivedError("question_history_point_unavailable")
            if (
                head["epoch"] != await uow.retention_epoch(self.scope)
                or actor not in head["readers"]
                or digest({k: v for k, v in head.items() if k != "sha256"}) != head["sha256"]
                or head["known_at"] != known.isoformat()
                or head["kind"] != kind
                or head["identity"] != label
            ):
                raise DerivedError("question_history_integrity_or_access_failed")
            if not instant(head["valid_from"]) <= valid < instant(head["valid_to"]):
                raise DerivedError("question_history_time_coverage_unavailable")
            guard = await self._permission(uow, head["sources"], actor, head["purpose"], controls)
            stored = await uow.derived_get(self.scope, KINDS[1], key)
            guard()
            if (
                not stored
                or "result" not in stored
                or digest(stored["result"]) != head["body_sha256"]
            ):
                raise DerivedError("question_history_body_unavailable")
            result = {
                **stored["result"],
                "historical": dict(
                    mode="published_point",
                    known_at=known.isoformat(),
                    valid_at=valid.isoformat(),
                    current_security_checked_at=self.service.clock().isoformat(),
                ),
            }
            budget(result, head["max_output_bytes"])
            guard = await self._permission(uow, head["sources"], actor, head["purpose"], controls)
            if await uow.derived_get(self.scope, KINDS[0], key) != head:
                raise DerivedError("question_history_changed")
            await self._finish(uow, controls, guard)
            return result

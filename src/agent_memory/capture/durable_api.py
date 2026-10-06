"""One host-bound durable transport contract shared by Embedded SDK and MCP."""

import json
from dataclasses import replace
from hashlib import sha256

from ..lifecycle import LifecycleEvent, LifecycleOrigin
from ..operations.retention import RetentionError
from .policy import CaptureSanitizer
from .producer import ProducerSession
from .profile import CAPTURE_METADATA_KEY


class DurableCaptureAPI:
    def __init__(self, producer, *, trusted_origin=LifecycleOrigin.MODEL, sanitizer=None):
        self.producer = producer
        self.origin = LifecycleOrigin(trusted_origin)
        self.sanitizer = sanitizer or CaptureSanitizer()

    async def call(self, operation, payload, context):
        try:
            return await self._call(operation, payload, context)
        except RetentionError:
            raise
        except (KeyError, TypeError, ValueError, OverflowError):
            raise RetentionError("invalid_durable_payload") from None

    async def _call(self, operation, payload, context):
        """Registration and worker authority remain out-of-band host configuration."""
        if not isinstance(payload, dict):
            raise RetentionError("invalid_durable_payload")
        session = ProducerSession(**payload["session"])
        if operation == "cursor":
            return await self.producer.cursor(context.scope, session, actor=context.actor)
        if operation == "status":
            return await self.producer.status(
                context.scope, session, sequence=payload["sequence"], actor=context.actor
            )
        if operation not in {"append", "revise"}:
            raise RetentionError("unsupported_durable_operation")
        data = {**payload["event"], "actor": context.actor}
        lifecycle = LifecycleEvent.from_dict(data, trusted_scope=context.scope)
        if CAPTURE_METADATA_KEY in lifecycle.payload:
            raise RetentionError("reserved_capture_metadata")
        # Source role belongs to the authenticated adapter, not JSON tags.
        lifecycle = replace(lifecycle, origin=self.origin)
        safe = await self.sanitizer.prepare(lifecycle)
        source_id = (
            "source:"
            + sha256(
                json.dumps(
                    [context.scope.partition_key(), safe.event_id], separators=(",", ":")
                ).encode()
            ).hexdigest()
        )
        source = replace(safe.to_memory_event(), id=source_id)
        revision = None
        if operation == "revise":
            revision = {
                "base_event_id": payload["base_event_id"],
                "expected_revision": payload["expected_revision"],
            }
            response = await self.producer.revise(
                source, session, sequence=payload["sequence"], actor=context.actor, **revision
            )
        else:
            response = await self.producer.append(
                source, session, sequence=payload["sequence"], actor=context.actor
            )
        response.update(
            operation=operation,
            revision=revision,
            producer_id=session.producer_id,
            epoch=session.epoch,
            event_sha256=sha256(
                json.dumps(
                    payload["event"], sort_keys=True, ensure_ascii=False, allow_nan=False
                ).encode()
            ).hexdigest(),
        )
        return response

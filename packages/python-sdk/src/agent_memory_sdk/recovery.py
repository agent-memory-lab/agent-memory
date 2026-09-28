"""Optional recovery methods shared by embedded and remote clients."""
from agent_memory.serialization import to_jsonable


class RecoveryClientOperations:
    async def recovery_call(self, operation, payload=None):
        return await self._call("memory_recovery", {"operation": operation,
                                                   "payload": to_jsonable(payload or {})})

    async def capture_confirmed(self, **event):
        return await self.recovery_call("capture", event)

    async def capture_receipt(self, event_id):
        return await self.recovery_call("receipt", dict(event_id=event_id))

    async def save_recovery(self, state, *, expected_version=0):
        return await self.recovery_call("save", dict(state=state, expected_version=expected_version))

    async def load_recovery(self, run_id):
        return await self.recovery_call("load", dict(run_id=run_id))

    async def propose_compression(self, plan):
        return await self.recovery_call("compress", plan)

    async def load_compression(self, summary_id):
        return await self.recovery_call("load_compression", dict(summary_id=summary_id))

    async def validate_compression(self, summary_id, current_plan):
        return await self.recovery_call("validate_compression",
            dict(summary_id=summary_id, current_plan=current_plan))

    async def enqueue_capture(self, **event):
        return await self.recovery_call("enqueue", event)

    async def process_next_capture(self):
        return await self.recovery_call("process_one")

    async def retry_capture(self, event_id):
        return await self.recovery_call("retry", dict(event_id=event_id))

    async def complete_recovery(self, run_id, *, expected_version):
        return await self.recovery_call("complete", dict(run_id=run_id, expected_version=expected_version))

    async def set_recovery_expiry(self, run_id, *, expires_at):
        return await self.recovery_call("expire", dict(run_id=run_id, expires_at=expires_at))

    async def cleanup_recovery(self, *, limit=100):
        return await self.recovery_call("cleanup", dict(limit=limit))

    async def recovery_stats(self):
        return await self.recovery_call("stats")

    async def forget_sources(self, source_event_ids=(), *, all_in_scope=False, erase=True):
        return await self.recovery_call("forget", dict(source_event_ids=source_event_ids,
            all_in_scope=all_in_scope, erase=erase))

    async def resume_deletion(self):
        return await self.recovery_call("resume_deletion")

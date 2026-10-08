"""Sequential experiment harness for complete B0 phase accounting.

Runtime never imports this module. Process CPU deltas require a dedicated,
nonconcurrent experiment process; they do not measure GPU energy or remote cost.
Absent tariffs stay unknown. A phase marked observed is not necessarily priced.
"""

from contextlib import contextmanager
from time import perf_counter_ns, process_time_ns
from uuid import uuid4

from .model_cost import runtime_model_costs
from .question_cost import CostEntry, CostPhase, CostSnapshot


class ModelExperimentAccounting:
    def __init__(self, currency):
        CostSnapshot(currency, (), (), ())
        self.currency = currency
        self._active = False
        self._entries = []
        self.observations = []
        self._phases = set()

    @contextmanager
    def measure(self, phase, *, request_ids=(), evidence):
        """Measure real performed work, including raised failures and zero hits.

        Call once around every phase actually inspected. An empty phase requires
        an explicit evidence reason (e.g. a drained queue), never a blanket claim
        that all unobserved phases cost zero. Overlap is prohibited so setup,
        retrieval, cache guards and cleanup CPU are not multiply attributed.
        """
        phase = CostPhase(phase)
        if self._active:
            raise ValueError("overlapping experiment accounting")
        if type(evidence) is not str or not evidence or len(evidence) > 2048:
            raise ValueError("phase evidence required")
        self._active = True
        cpu, wall = process_time_ns(), perf_counter_ns()
        failed = False
        try:
            yield
        except BaseException:
            failed = True
            raise
        finally:
            elapsed_cpu = (process_time_ns() - cpu) / 1_000_000
            elapsed_wall = (perf_counter_ns() - wall) / 1_000_000
            identity = "resources:" + uuid4().hex
            # Unpriced runtime resources never become a fabricated free bill.
            self._entries.append(CostEntry(identity, phase, tuple(request_ids), cpu_ms=elapsed_cpu))
            self.observations.append(
                dict(
                    operation_id=identity,
                    phase=phase.value,
                    evidence=evidence,
                    cpu_ms=elapsed_cpu,
                    wall_ms=elapsed_wall,
                    failed=failed,
                    pricing="unknown",
                    unmeasured_resources=["gpu", "io", "storage", "network"],
                )
            )
            self._phases.add(phase)
            self._active = False

    def snapshot(self, runtime_calls, *, request_bindings=None, pending=()):
        if self._active:
            raise ValueError("experiment accounting still active")
        model, reserved = runtime_model_costs(runtime_calls, request_bindings=request_bindings)
        required = {entry.phase for entry in model} | {item.phase for item in (*reserved, *pending)}
        if not required <= self._phases:
            raise ValueError("runtime phase has no complete resource observation")
        phases = self._phases
        return CostSnapshot(
            self.currency, (*self._entries, *model), (*reserved, *pending), tuple(sorted(phases))
        )

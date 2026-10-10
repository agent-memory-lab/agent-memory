"""Repeatable instrumented four-view workload, also executable on frozen B0.

Run with AGENT_MEMORY_SHARED_BENCHMARK_OUTPUT=/absolute/result.json and set
PYTHONPATH to the desired frozen tree's src, packages/postgres/src and tests.
No model, network service, persistent cache or wall-clock speed claim is used.
"""

import asyncio
from collections import Counter
import cProfile
from datetime import timedelta
import json
import os
from pathlib import Path
from time import process_time

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, register, runtime

store = project.store
QUESTIONS = ("owner", "status", "commitments", "risks")


def test_instrumented_frozen_contract_comparison(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            cls = type(engine.repository.unit_of_work())
            counts = Counter()
            for name in ("derived_project_candidates", "get_source_event", "get_admission_record",
                         "derived_project_source_proof", "retention_epoch"):
                original = getattr(cls, name)
                async def counted(self, *args, _name=name, _original=original, **kwargs):
                    counts[_name] += 1
                    return await _original(self, *args, **kwargs)
                monkeypatch.setattr(cls, name, counted)
            original_get = cls.derived_get
            async def get(self, scope, kind, key):
                counts["derived_get:" + kind] += 1
                return await original_get(self, scope, kind, key)
            monkeypatch.setattr(cls, "derived_get", get)
            original_review = svc.admission._review_valid
            def review(row):
                counts["semantic_review_validation"] += 1
                return original_review(row)
            monkeypatch.setattr(svc.admission, "_review_valid", review)
            if hasattr(cls, "connection"):
                original_enter = cls.__aenter__
                async def enter(self):
                    result = await original_enter(self)
                    def sql(statement):
                        counts["sql:" + statement.lstrip().split()[0].upper()] += 1
                    self.connection.set_trace_callback(sql)
                    return result
                monkeypatch.setattr(cls, "__aenter__", enter)
            report = {"schema": "question-shared-workload/1", "views": 4, "qualified_inputs": 8,
                      "shared_batches": hasattr(svc, "snapshot_many"), "stages": {}}
            async def measure(name, operation):
                counts.clear()
                profiler = cProfile.Profile()
                start = process_time()
                profiler.enable()
                result = await operation()
                profiler.disable()
                elapsed = process_time() - start
                entries = profiler.getstats()
                report["stages"][name] = dict(counts=counts.copy(), cpu_seconds=elapsed,
                    python_calls=sum(e.callcount for e in entries),
                    primitive_calls=sum(e.callcount - e.reccallcount for e in entries),
                    support_range_evaluations=sum(e.callcount for e in entries
                        if getattr(e.code, "co_name", None) == "evaluate_support"))
                return result
            async def setup():
                # Eight qualified inputs, two entities, four maintained operators.
                facts = [("project-a", "a", "project.owner", "Alice"),
                         ("project-a", "a", "project.status", "active"),
                         ("promise-1", "promise-a", "commitment.promisor", "Alice"),
                         ("promise-1", "promise-a", "commitment.action", "ship"),
                         ("promise-1", "promise-a", "commitment.state", "open"),
                         ("promise-1", "promise-a", "commitment.deadline", "2026-10-05T00:00:00+00:00"),
                         ("project-a", "a", "risk.label", "supplier"),
                         ("project-a", "a", "risk.state", "open")]
                for i, (subject, membership, predicate, value) in enumerate(facts):
                    item = await project.stage(svc.admission, scope, identity=f"source-{i}",
                        subject=subject, membership=membership, predicate=predicate, value=value)
                    await project.qualify(svc.admission, *item)
                for question in QUESTIONS:
                    await register(svc, question)
                clock[0] += timedelta(microseconds=100)
                leases = []
                for question in QUESTIONS:
                    receipt = await svc.request("project-a:" + question, actor=ACTOR, dedupe_key=question)
                    lease = await svc.queue.claim("benchmark", lease_seconds=30, target_id=receipt["target_id"])
                    assert lease
                    leases.append(lease)
                tasks = [lease.task for lease in leases]
                return tasks, leases
            tasks, leases = await measure("setup", setup)
            async def snapshot():
                if report["shared_batches"]:
                    return await svc.snapshot_many(tasks)
                return [await svc.snapshot(task) for task in tasks]
            snapshots = await measure("snapshot", snapshot)
            async def prepare():
                return [svc.prepare(s) for s in snapshots]
            prepared = await measure("prepare", prepare)
            async def publish():
                if report["shared_batches"]:
                    return await svc.publish_many(tasks, snapshots, prepared)
                return [await svc.publish(t, s, p) for t, s, p in zip(tasks, snapshots, prepared)]
            await measure("publish", publish)
            async def complete():
                for lease in leases:
                    await svc.queue.complete(lease)
            await measure("complete", complete)
            # A real advancing host clock must not defeat metadata sharing.
            def advancing():
                clock[0] += timedelta(microseconds=1)
                return clock[0]
            svc.clock = svc.admission.clock = svc.queue.clock = advancing
            async def read():
                if report["shared_batches"]:
                    return await svc.read_many(["project-a:" + q for q in QUESTIONS], actor=ACTOR)
                return [await svc.read("project-a:" + q, actor=ACTOR) for q in QUESTIONS]
            answers = await measure("read_advancing_clock", read)
            assert all(a["model_calls"] == 0 for a in answers)
            report["answers"] = [dict(question=q, schema=a["schema"],
                answer_status=a["answer_status"], result_schema=a["result"]["schema"],
                contract_fingerprint=a["result"]["contract_fingerprint"],
                rows=a["result"]["rows"], matched_ids=a["result"]["matched_ids"],
                source_basis=a["result"]["coverage"]["source_basis"],
                reasons=a["result"]["reasons"], model_calls=a["model_calls"],
                value_digest=a["digests"]["value"], citations=a["citations"])
                for q, a in zip(QUESTIONS, answers)]
            target = os.environ.get("AGENT_MEMORY_SHARED_BENCHMARK_OUTPUT")
            if target:
                Path(target).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    asyncio.run(run())

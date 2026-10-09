#!/usr/bin/env python3
"""Synthetic regression-only evidence, stdlib runner. No network/model access.

Run in a fresh process for each immutable repo snapshot. SQLite costs below are
SQL statements and application-materialized result rows/bytes, not physical I/O.
Model quality, answer correctness, real embedding and monetary costs are untested.
"""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
from pathlib import Path
import platform
import subprocess
import sys
from time import perf_counter, process_time
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument('--source-root', '--repo', dest='repo', type=Path, required=True, help='Source checkout to import; use a clean immutable worktree')
parser.add_argument('--out', type=Path, required=True, help='Output evidence directory outside the source checkout')
parser.add_argument('--label', required=True)
parser.add_argument('--repeats', type=int, default=9)
args = parser.parse_args()
if not 1 <= args.repeats <= 100: parser.error('repeats must be 1..100')
repo = args.repo.resolve()
sys.path.insert(0, str(repo / 'src'))
from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope, MemoryEvent, MemoryQuery, MemoryKind, MemoryChannel, MemoryItem
from agent_memory.retrieval.lexical import lexical_candidates, EvidenceItem, _terms
from agent_memory.retrieval.lexical_plugin import LexicalCandidatePlugin
from agent_memory.retrieval.governed import GovernedRecallPipeline
from agent_memory.retrieval.guard import GovernedCandidate
from agent_memory.retrieval.bundle import BundleBudget
from agent_memory.retrieval.diversity import DiversityBudget
from agent_memory.retrieval.parallel import ParallelRetrieverOrchestrator
from agent_memory.extensions.loader import LoadedPlugin, PluginCandidateReference
from agent_memory.extensions.protocol import PluginContext, PluginHealth, PluginHealthStatus, RetrievalCandidate
from agent_memory.extensions.registry import PluginKind, PluginManifest, PluginResourceLimits, PluginFailureMode
from agent_memory.retrieval.entity import EntityReference, EntityRetrieverPlugin, ScopedEntityMatch
from agent_memory.retrieval.temporal import TemporalRetrieverPlugin, ScopedTemporalMatch, TemporalWindow
import agent_memory.sqlite as sqlite_module

UTC = timezone.utc
NOW = datetime(2026, 10, 9, tzinfo=UTC)
SCOPE = MemoryScope('synthetic-retrieval-tenant', session_id='fixture-session')


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + '\n')


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest()


def git(*cmd):
    return subprocess.check_output(['git', '-C', str(repo), *cmd], text=True).strip()


def fixtures():
    recent = (NOW - timedelta(days=1)).isoformat()
    records = [
        {'id': 'han-exact', 'text': '数据库迁移回滚流程已确认。', 'at': recent},
        {'id': 'han-distractor', 'text': '网络监控告警与值班交接。', 'at': recent},
        {'id': 'punctuation-contiguous', 'text': '北京', 'at': recent},
        {'id': 'punctuation-separated', 'text': '北，京', 'at': recent},
        {'id': 'mixed-version', 'text': '项目使用Python 3.13，数据库 PostgreSQL；迁移已完成。', 'at': recent},
        {'id': 'mixed-old-version', 'text': '旧项目使用 Python 3.10 和 SQLite。', 'at': recent},
        {'id': 'entity-owner', 'text': 'Acme 凤凰计划负责人是陈晨。', 'at': recent},
        {'id': 'entity-distractor', 'text': 'Acme 白鹭计划负责人是李华。', 'at': recent},
        {'id': 'meeting-current', 'text': '陈晨于2026年10月2日15:00在杭州西湖参加发布复盘。', 'at': recent},
        {'id': 'meeting-distractor', 'text': '李华于2026年10月3日09:00在苏州参加例会。', 'at': recent},
        {'id': 'long-tail', 'text': '例行设备维护与状态检查。' * 280 + '\n雪鸮凭证的恢复标记是 amber-47。', 'at': recent},
        {'id': 'old-evidence', 'text': '灯塔项目的归档校验代号是 cobalt-73。', 'at': '2025-01-01T00:00:00+00:00'},
        {'id': 'scope-forbidden', 'text': '隔离专用 sealed-92 资料。', 'at': recent, 'foreign': True},
        {'id': 'archived-forbidden', 'text': '归档专用 retired-88 资料。', 'at': recent, 'archived': True},
    ]
    records += [
        {'id': f'filler-{index:04d}', 'text': f'例行设备维护日志编号 maintenance-{index:04d}。',
         'at': (datetime(2026, 9, 1, tzinfo=UTC) + timedelta(seconds=index)).isoformat()}
        for index in range(520)
    ]
    cases = [
        {'id': 'han-contiguous', 'query': '迁移回滚', 'relevant': ['han-exact']},
        {'id': 'mixed-punctuation', 'query': '北京', 'relevant': ['punctuation-contiguous']},
        {'id': 'mixed-version', 'query': 'Python3.13 PostgreSQL迁移', 'relevant': ['mixed-version']},
        {'id': 'entity-text', 'query': 'Acme 凤凰计划负责人', 'relevant': ['entity-owner']},
        {'id': 'time-text', 'query': '2026-10-02 15:00 西湖 陈晨', 'relevant': ['meeting-current']},
        {'id': 'long-document-tail', 'query': '雪鸮 amber-47 恢复', 'relevant': ['long-tail'], 'required_text': ['amber-47']},
        {'id': 'old-evidence', 'query': 'cobalt-73', 'relevant': ['old-evidence'], 'required_text': ['cobalt-73']},
        {'id': 'scope-isolation', 'query': 'sealed-92', 'relevant': [], 'forbidden': ['scope-forbidden']},
        {'id': 'archive-isolation', 'query': 'retired-88', 'relevant': [], 'forbidden': ['archived-forbidden']},
    ]
    return {'schema': 'synthetic-retrieval-fixtures-v1', 'records': records, 'cases': cases}


class SQLMeter:
    def __init__(self): self.reset()
    def reset(self):
        self.read_statements = 0
        self.rows_materialized = 0
        self.text_bytes_materialized = 0
        self.statement_kinds = Counter()
    def observe_sql(self, sql):
        first = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ''
        self.statement_kinds[first] += 1
        read = first in ('SELECT', 'WITH')
        self.read_statements += int(read)
        return read
    def observe_rows(self, rows, read):
        if read:
            self.rows_materialized += len(rows)
            self.text_bytes_materialized += sum(
                len(v.encode('utf-8')) for row in rows for v in row if isinstance(v, str)
            )
        return rows
    def snapshot(self):
        return {'sql_read_statements': self.read_statements,
                'sql_result_rows_materialized': self.rows_materialized,
                'sql_result_text_bytes_materialized': self.text_bytes_materialized,
                'statement_kinds': dict(sorted(self.statement_kinds.items()))}


class CursorProxy:
    def __init__(self, cursor, meter, read): self.cursor, self.meter, self.read = cursor, meter, read
    def fetchall(self): return self.meter.observe_rows(self.cursor.fetchall(), self.read)
    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None: self.meter.observe_rows([row], self.read)
        return row
    def fetchmany(self, *a): return self.meter.observe_rows(self.cursor.fetchmany(*a), self.read)
    def __iter__(self):
        for row in self.cursor:
            self.meter.observe_rows([row], self.read)
            yield row
    def __getattr__(self, name): return getattr(self.cursor, name)


class ConnectionProxy:
    def __init__(self, connection, meter): self.connection, self.meter = connection, meter
    def execute(self, sql, *a):
        read = self.meter.observe_sql(sql)
        return CursorProxy(self.connection.execute(sql, *a), self.meter, read)
    def __enter__(self): self.connection.__enter__(); return self
    def __exit__(self, *a): return self.connection.__exit__(*a)
    def __getattr__(self, name): return getattr(self.connection, name)


def percentile(values, fraction): return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def item_observation(items, *, token_estimate=None, sources=None, trace=None):
    return {'memory_ids': [item.id for item in items],
            'source_event_ids': sorted(set(sources or [source for item in items for source in item.metadata.get('source_event_ids', ())])),
            'token_estimate': token_estimate,
            'returned_text_chars': sum(len(item.text) for item in items),
            'returned_items': [{'id':item.id,'text':item.text,'text_sha256':hashlib.sha256(item.text.encode()).hexdigest()} for item in items],
            'trace': trace}


def candidate_observation(result):
    return item_observation([c.item for c in result.candidates],
        sources=[s for c in result.candidates for s in c.source_event_ids], trace=asdict(result.trace))


def bundle_observation(bundle):
    return item_observation(list(bundle.relevant_memories) + list(bundle.episodes) + list(bundle.procedures),
        token_estimate=bundle.token_estimate, sources=[s for c in bundle.citations for s in c.source_event_ids],
        trace={k:v for k,v in bundle.retrieval_metadata.items() if k not in ('elapsed_ms',)})


async def measured(fn, meter, calls):
    results, timings, costs = [], [], []
    for repeat in range(args.repeats):
        meter.reset(); calls.clear()
        begin = perf_counter()
        try:
            result = await fn()
        except Exception as exc:
            result = {'error_type': type(exc).__name__, 'error': str(exc)}
        timings.append((perf_counter() - begin) * 1000)
        results.append(result)
        costs.append({**meter.snapshot(), **dict(calls)})
    return {'observation': results[0], 'observations_stable': all(r == results[0] for r in results),
            'latency_ms_samples': timings, 'latency_p50_ms': percentile(timings, .5),
            'latency_p95_ms': percentile(timings, .95), 'cost_samples': costs,
            'costs_stable': all(c == costs[0] for c in costs),
            'first_observation_ms': timings[0],
            'warm_repeat_ms_samples': timings[1:],
            'cold_os_cache_measured': False}


class FixedClock:
    def now(self): return NOW


class ScriptedRetriever:
    def __init__(self, values, calls): self.values, self.calls, self.queries = values, calls, []
    async def retrieve(self, query, context):
        self.calls['fixture_retriever_calls'] += 1
        self.queries.append(query)
        return tuple(self.values[:query.limit])


def loaded(instance, *, name='synthetic-fixture', capabilities=('lexical.search',), max_candidates=32):
    limits = PluginResourceLimits(max_candidates=max_candidates, max_batch_size=128)
    manifest = PluginManifest(name=name, version='0.1.0', kind=PluginKind.RETRIEVER,
        capabilities=capabilities, requires={'core': '>=0.1,<1.0'}, config_schema={'type':'object'},
        resource_limits=limits, failure_mode=PluginFailureMode.FAIL_CLOSED)
    context = PluginContext(scope=SCOPE, resource_limits=limits, request_id='fixture-context', clock=FixedClock())
    return LoadedPlugin(PluginCandidateReference(name, PluginKind.RETRIEVER, 'test', 'external:fixture', None),
        manifest, instance, context, PluginHealth(PluginHealthStatus.READY))


def candidate(index, text=None):
    mid = f'candidate-{index:02d}'
    return RetrievalCandidate(item=MemoryItem(mid, MemoryKind.EVENT, text or f'有效证据 {index}', .5, NOW),
        channel=MemoryChannel.SEMANTIC, rank=index, source_event_ids=(f'source-{index:02d}',),
        retriever='synthetic-fixture', retrieval_method='lexical')


class Governance:
    def __init__(self, blocked, calls): self.blocked, self.calls = blocked, calls
    async def resolve(self, scope, candidates):
        self.calls['fixture_governance_calls'] += 1
        self.calls['fixture_governance_records'] += len(candidates)
        return tuple(GovernedCandidate(scope, c, archived=c.item.id in self.blocked) for c in candidates)


async def governed_fixtures():
    outputs = {}
    for name, values, blocked, limit, tokens in (
        ('guard-headroom', [candidate(i) for i in range(1, 13)], {f'candidate-{i:02d}' for i in range(1, 9)}, 4, 256),
        ('packing-backfill', [candidate(1, '超长文本' * 200), candidate(2)], set(), 1, 64),
    ):
        calls, meter = Counter(), SQLMeter()
        plugin = ScriptedRetriever(values, calls)
        pipeline = GovernedRecallPipeline((loaded(plugin),), Governance(blocked, calls),
            diversity_budget=DiversityBudget(max_items=8, max_per_kind={MemoryKind.EVENT:8}),
            bundle_budget=BundleBudget(max_items=8, max_tokens=1200))
        async def run():
            calls['pipeline_calls'] += 1
            bundle = await pipeline.retrieve(MemoryQuery(SCOPE, '有效证据', limit=limit, token_budget=tokens), ())
            result = bundle_observation(bundle)
            result['observed_retriever_limit'] = plugin.queries[-1].limit
            return result
        outputs[name] = await measured(run, meter, calls)
        outputs[name]['fixture_kind'] = 'scripted pipeline mechanics; not semantic or real-data efficacy'
        outputs[name]['expected_ids'] = ['candidate-09','candidate-10','candidate-11','candidate-12'] if name == 'guard-headroom' else ['candidate-02']
        outputs[name]['expected_hit_count'] = len(set(outputs[name]['expected_ids']) & set(outputs[name]['observation'].get('memory_ids', [])))
    # Query field preservation and explicit historical capability handling.
    for name, capabilities in (('query-field-forwarding', ('temporal.search', 'temporal.bitemporal')),
                               ('historical-unsupported-boundary', ('lexical.search',))):
        calls = Counter(); plugin = ScriptedRetriever([], calls)
        query = MemoryQuery(SCOPE, '历史请求', limit=2, token_budget=256, include_current_state=False,
            channels=(MemoryChannel.EPISODIC,), request_id='frozen-request', run_id='frozen-run',
            policy_version='frozen-policy', trace_enabled=False,
            valid_at=NOW - timedelta(days=30), known_at=NOW - timedelta(days=29))
        fields = ('valid_at','known_at','channels','include_current_state','request_id','run_id','policy_version','trace_enabled')
        try:
            await ParallelRetrieverOrchestrator().retrieve(query, (loaded(plugin, capabilities=capabilities),))
            received = plugin.queries[-1] if plugin.queries else None
            outputs[name] = {'error_type': None, 'retriever_called': bool(received),
                'fields_equal': {field: getattr(query,field) == getattr(received,field) if received else False for field in fields},
                'requested': {field:getattr(query,field) for field in fields},
                'received': {field:(getattr(received,field) if field != 'request_id' or received.request_id == query.request_id else '<generated-mismatch>') for field in fields} if received else None,
                'calls':dict(calls)}
        except Exception as exc:
            outputs[name] = {'error_type':type(exc).__name__, 'error':str(exc), 'retriever_called':bool(plugin.queries), 'calls':dict(calls)}
    return outputs


async def module_contracts():
    calls = Counter()
    entity = EntityReference('organization','Acme')
    class Resolver:
        async def resolve(self,text,scope):
            calls['entity_resolver_calls'] += 1
            return (entity,) if 'acme' in text.casefold() else ()
    class Index:
        async def search(self,entities,scope,*,limit):
            calls['entity_index_calls'] += 1
            return (ScopedEntityMatch(SCOPE, MemoryItem('entity-owner',MemoryKind.EVENT,'Acme 凤凰计划负责人是陈晨。',.5,NOW), MemoryChannel.SEMANTIC, ('entity-owner',), (entity,), .9),)
    ep = EntityRetrieverPlugin(Resolver(),Index()); ctx = loaded(None).context
    await ep.initialize(ctx)
    er = await ep.retrieve(MemoryQuery(SCOPE,'Acme 凤凰计划负责人'),ctx)
    await ep.close()
    class TimeResolver:
        def resolve(self,query,context):
            calls['temporal_resolver_calls'] += 1
            return TemporalWindow(query.valid_at,query.known_at)
    class TimeIndex:
        async def search(self,text,scope,window,*,limit):
            calls['temporal_index_calls'] += 1
            return (ScopedTemporalMatch(SCOPE,MemoryItem('historical-v1',MemoryKind.EVENT,'居住地杭州',.5,NOW),MemoryChannel.SEMANTIC,('history-source',),NOW-timedelta(days=60),NOW-timedelta(days=10),NOW-timedelta(days=59),.8),)
    tp = TemporalRetrieverPlugin(TimeIndex(),TimeResolver()); await tp.initialize(ctx)
    tr = await tp.retrieve(MemoryQuery(SCOPE,'居住地',valid_at=NOW-timedelta(days=30),known_at=NOW-timedelta(days=29)),ctx)
    await tp.close()
    return {'kind':'scripted authoritative adapters, plugin contract checks only',
        'entity_memory_ids':[v.item.id for v in er], 'temporal_memory_ids':[v.item.id for v in tr], 'calls':dict(calls)}


async def main():
    fixture = fixtures(); args.out.mkdir(parents=True, exist_ok=True)
    dump(args.out/'fixture.json', fixture)
    tracked = git('ls-files','-z').split('\0')
    hashes = {p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in tracked if p and (repo/p).is_file()}
    source = {'repo':str(repo), 'commit':git('rev-parse','HEAD'), 'tree':git('rev-parse','HEAD^{tree}'),
        'status':git('status','--porcelain'), 'tracked_sha256':hashes,
        'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'fixture_sha256':digest(fixture), 'python':sys.version,'platform':platform.platform(),
        'module_file':sqlite_module.__file__, 'python_hash_seed':'use PYTHONHASHSEED=0 for process-stable floating-point set traversal'}
    dump(args.out/'source-manifest.json', source)
    outcomes = []
    maintenance = {}
    with tempfile.TemporaryDirectory(prefix='retrieval-frozen-') as td:
        begin, cpu_begin = perf_counter(), process_time()
        kernel = build_local_kernel(Path(td)/'fixture.db'); await kernel.initialize()
        maintenance['fresh_initialize'] = {'elapsed_ms': (perf_counter()-begin)*1000, 'process_cpu_ms': (process_time()-cpu_begin)*1000}
        repository = kernel._repository
        try:
            begin, cpu_begin = perf_counter(), process_time()
            async with repository.unit_of_work() as uow:
                for row in fixture['records']:
                    scope = MemoryScope('foreign-tenant', session_id='fixture-session') if row.get('foreign') else SCOPE
                    event = MemoryEvent(scope,'user.message',row['text'],id=row['id'],occurred_at=datetime.fromisoformat(row['at']),ingested_at=NOW)
                    await uow.append_event(event)
            maintenance['bulk_ingest'] = {'event_count':len(fixture['records']), 'elapsed_ms':(perf_counter()-begin)*1000, 'process_cpu_ms':(process_time()-cpu_begin)*1000}
            begin, cpu_begin = perf_counter(), process_time()
            with repository._connection() as con:
                con.execute('UPDATE events SET archived_at=? WHERE id=?',(NOW.isoformat(),'archived-forbidden'))
            maintenance['archive_event'] = {'elapsed_ms':(perf_counter()-begin)*1000, 'process_cpu_ms':(process_time()-cpu_begin)*1000}
            with repository._connection() as con:
                maintenance['allocated_database_bytes'] = con.execute('PRAGMA page_count').fetchone()[0] * con.execute('PRAGMA page_size').fetchone()[0]
            maintenance['limits'] = ['Single synthetic process; uncontrolled host and OS caches.', 'Bulk-ingest includes incremental index maintenance when present.', 'Archive measures one fixture event, not complete erasure/restore workload.', 'Migration/backfill, long-run maintenance, GPU and money costs remain unmeasured.']
            meter, calls = SQLMeter(), Counter()
            original_connect = repository._connect
            repository._connect = lambda: ConnectionProxy(original_connect(), meter)
            lexical = LexicalCandidatePlugin.from_sqlite(repository)
            authorized = [EvidenceItem(MemoryItem(r['id'],MemoryKind.EVENT,r['text'],.5,datetime.fromisoformat(r['at'])),MemoryChannel.EPISODIC,(r['id'],)) for r in fixture['records'] if not r.get('foreign') and not r.get('archived')]
            # In-memory lexical maximum 512; this arm explicitly exposes that cap.
            bounded_authorized = sorted(authorized,key=lambda e:(e.item.occurred_at,e.item.id),reverse=True)[:512]
            for case in fixture['cases']:
                for path in ('no-memory','repository-default','kernel-default','recent-lexical','in-memory-lexical-512'):
                    async def run(path=path, case=case):
                        q = MemoryQuery(SCOPE,case['query'],limit=8,token_budget=256,include_current_state=False,request_id='fixture-query',trace_enabled=False)
                        if path == 'no-memory': return item_observation([],token_estimate=0,sources=[])
                        if path == 'repository-default':
                            calls['repository_search_calls'] += 1
                            return item_observation(await repository.search(q,8))
                        if path == 'kernel-default':
                            calls['kernel_retrieve_calls'] += 1
                            return bundle_observation(await kernel.retrieve(q))
                        if path == 'recent-lexical':
                            calls['lexical_candidate_calls'] += 1
                            return candidate_observation(await lexical.candidates(q.text,q.scope))
                        calls['in_memory_lexical_calls'] += 1
                        calls['application_items_supplied'] += len(bounded_authorized)
                        return candidate_observation(lexical_candidates(q.text,bounded_authorized,max_items=512))
                    measured_result = await measured(run,meter,calls)
                    observation = measured_result['observation']; ids = observation.get('memory_ids',[])
                    relevant, required = set(case['relevant']), set(case['relevant'])
                    sources = set(observation.get('source_event_ids',[]))
                    measured_result.update({'case_id':case['id'],'path':path,'relevant_ids':sorted(relevant),
                        'relevant_hits':len(relevant & set(ids)), 'required_source_hits':len(required & sources),
                        'forbidden_hits':len(set(case.get('forbidden',[])) & set(ids)),
                        'reciprocal_rank':next((1/(i+1) for i,v in enumerate(ids) if v in relevant),0.0),
                        'precision_at_returned':len(relevant & set(ids))/len(ids) if ids else 0.0,
                        'recall':len(relevant & set(ids))/len(relevant) if relevant else None,
                        'required_text_hit':all(t in '\n'.join(v['text'] for v in observation.get('returned_items',[])) for t in case.get('required_text',[])) if case.get('required_text') else None})
                    outcomes.append(measured_result)
        finally:
            await kernel.close()
    token_samples = ('北，京','北京','Python3.13 PostgreSQL迁移','版本 v1_2 到 v1-3','猫 A 狗')
    tokenizer = {s:{'repository_tokens':sorted(sqlite_module._tokens(s)), 'lexical_terms':list(_terms(s))} for s in token_samples}
    pipeline = await governed_fixtures()
    contracts = await module_contracts()
    totals = {}
    for path in sorted({r['path'] for r in outcomes}):
        rs=[r for r in outcomes if r['path']==path]
        totals[path]={'case_count':len(rs),'relevant_count':sum(len(r['relevant_ids']) for r in rs),
            'relevant_hits':sum(r['relevant_hits'] for r in rs),'forbidden_hits':sum(r['forbidden_hits'] for r in rs),
            'errors':sum('error_type' in r['observation'] for r in rs),
            'mean_reciprocal_rank_over_positive_cases':sum(r['reciprocal_rank'] for r in rs)/sum(bool(r['relevant_ids']) for r in rs),
            'all_observations_stable':all(r['observations_stable'] for r in rs)}
    report={'schema':'synthetic-retrieval-regression-v1','label':args.label,'source_commit':source['commit'],
        'source_tree':source['tree'],'fixture_sha256':source['fixture_sha256'], 'repeats':args.repeats,
        'classification':'deterministic synthetic regression evidence only',
        'limits':['No model endpoint called; no answer generation, embeddings, reranking logits, vendor quality, or financial costs measured.',
          'Model and embedding call count is zero by construction: this harness contains no model adapter or network path.',
          'Latency samples include Python scheduling and local SQLite, excluding fixture setup; trace persistence is disabled, OS caches and host contention are uncontrolled.',
          'SQL read counters count executed SELECT/WITH statements and rows/text bytes returned to Python, not rows scanned, pages, disk I/O, or server work.',
          'Scripted entity/temporal/governed fixtures exercise contracts and mechanics, never semantic efficacy.',
          'In-memory lexical receives only the most recent 512 authorized events; shipped lexical uses its source adapter at this source revision (indexed when available).',
          'No baseline ranking metric is real-data efficacy or a product quality claim.'],
        'model_endpoint_calls':0,'embedding_calls':0,'generation_calls':0,'reranker_calls':0,
        'maintenance':maintenance,'totals':totals,'retrieval_results':outcomes,'tokenizer_samples':tokenizer,
        'pipeline_fixtures':pipeline,'plugin_contracts':contracts}
    dump(args.out/'results.json',report)
    print(json.dumps({'out':str(args.out),'source_tree':source['tree'],'totals':totals,'pipeline':{k:v.get('expected_hit_count',v.get('error_type')) for k,v in pipeline.items()}},ensure_ascii=False,indent=2))

asyncio.run(main())

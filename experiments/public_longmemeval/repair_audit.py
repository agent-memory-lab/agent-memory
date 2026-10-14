"""Offline versioned evidence audit on copies of an interrupted benchmark.

No extraction, answer generation, judge, credentials, or model SDK is used.
Select all completed cases, never only the failures. The original score remains
immutable. Source-session coverage is a diagnostic proxy, not answer accuracy.
Run baseline and repaired in separate processes so their core imports cannot mix.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def records(path):
    with Path(path).open(encoding='utf-8') as stream:
        return [json.loads(line) for line in stream if line.strip()]


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)+'\n', encoding='utf-8')


def isolated_destination(original, core, output):
    original, core, output = (Path(p).resolve() for p in (original, core, output))
    if (output.is_relative_to(original) or output.is_relative_to(core)
            or original.is_relative_to(output) or core.is_relative_to(output)):
        raise ValueError('Audit output must be separate from original evidence and source checkout')
    if output.exists():
        raise ValueError('Audit output already exists; never overwrite a prior version')
    return original, core, output


def copy_closed_database(source, target):
    source, target = Path(source).resolve(), Path(target).resolve()
    if target.exists():
        raise ValueError('Database copy destination already exists')
    for suffix in ('-wal', '-journal'):
        path = Path(str(source)+suffix)
        if path.exists() and path.stat().st_size:
            raise ValueError('Original database has uncheckpointed writes')
    before = digest(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Immutable read avoids recovery/checkpoint and sidecar writes at the source.
    with closing(sqlite3.connect(source.as_uri()+'?mode=ro&immutable=1', uri=True)) as old:
        if old.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('Original database is corrupt')
        with closing(sqlite3.connect(target)) as new:
            old.backup(new)
    if digest(source) != before:
        raise ValueError('Original database changed during audit copy')
    return before


def stored_counts(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro&immutable=1', uri=True)) as db:
        return {table: db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
                for table in ('events', 'claims', 'claim_versions')}


def source_ids(items):
    return sorted({sid for item in items for sid in item['source_ids']})


def delivery_metrics(candidates, delivered):
    """Use ordered candidate identities, not public IDs (one source has spans)."""
    before = [item.get('candidate_id', item['id']) for item in candidates]
    after = [item.get('candidate_id', item['id']) for item in delivered]
    return {'selected_items': len(candidates), 'delivered_items': len(delivered),
            'exact_order_and_text_preserved': before == after and
                [x['text'] for x in candidates] == [x['text'] for x in delivered],
            'selected_text_utf8_bytes': sum(len(x['text'].encode()) for x in candidates),
            'delivered_text_utf8_bytes': sum(len(x['text'].encode()) for x in delivered),
            'partial_delivered_items': sum(bool(x.get('partial')) for x in delivered),
            'selected_source_ids': source_ids(candidates), 'delivered_source_ids': source_ids(delivered)}


def coverage(expected, items):
    expected, actual = set(expected), set(source_ids(items))
    return {'expected_sessions': len(expected), 'covered_sessions': len(expected & actual),
            'session_recall': len(expected & actual)/len(expected) if expected else None}


async def recall_copy(case_id, query, database, journal, *, enable_query_facets=False):
    from agent_memory import MemoryScope
    from agent_memory.composition import build_local_kernel
    from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory
    configuration = {'enable_query_facets': True} if enable_query_facets else {}
    memory = UnifiedMemory(build_local_kernel(database, **configuration),
        MemoryScope('qwen38-public-longmemeval', user_id=case_id, session_id='one-question'),
        journal=SQLiteDeletionJournal(journal))
    try:
        await memory.initialize()
        started = time.perf_counter()
        bundle = await memory.recall(query, limit=8, token_budget=2048, include_current_state=False)
        elapsed = (time.perf_counter()-started)*1000
        return bundle, elapsed
    finally:
        await memory.close()


async def collect(original, core, output, variant, *, enable_query_facets=False):
    if enable_query_facets and variant != 'repaired':
        raise ValueError('Query facets may only be enabled in a separate repaired experiment')
    original, core, output = isolated_destination(original, core, output)
    sys.path.insert(0, str(core/'src'))
    import agent_memory
    if Path(agent_memory.__file__).resolve().parent != core/'src/agent_memory':
        raise RuntimeError('Wrong core imported; use a fresh process for each variant')
    from experiments.public_longmemeval.reader_context import core_reader_context, legacy_reader_context
    from agent_memory.retrieval.packing import ordered_bundle_evidence
    from agent_memory.retrieval.candidate_identity import candidate_identity
    manifest = json.loads((original/'run/manifest.json').read_text())
    for name in ('queries.jsonl',):
        if digest(original/'prepared'/name) != manifest['inputs'][name]:
            raise ValueError('Original blind input changed')
    queries = {row['case_id']: row for row in records(original/'prepared/queries.jsonl')}
    rows = {p.parent.name: json.loads(p.read_text()) for p in sorted((original/'run/cases').glob('*/result.json'))}
    selected = [cid for cid in queries if rows.get(cid, {}).get('status') == 'completed']
    output.mkdir(parents=True)
    identity = {'schema': 'memory-reliability-offline/2', 'variant': variant,
        'benchmark_calls': 0, 'new_answers': 0, 'new_judgments': 0,
        'original_manifest_sha256': digest(original/'run/manifest.json'),
        'blind_queries_sha256': digest(original/'prepared/queries.jsonl'),
        'case_ids': selected, 'selection': 'all original completed cases, independent of correctness',
        'core_files': {str(p.relative_to(core)): digest(p) for p in sorted((core/'src/agent_memory').rglob('*'))
                       if p.suffix in ('.py', '.sql')},
        'audit_implementation_sha256': digest(__file__),
        'reader_implementation_sha256': digest(Path(__file__).with_name('reader_context.py')),
        'recall': {'limit': 8, 'token_budget': 2048, 'include_current_state': False,
                   'valid_at': None, 'known_at': None, 'embedding_mode': 'none',
                   'enable_query_facets': enable_query_facets},
        'scope_note': 'uses current-mode recall on immutable copies; wall-clock capture times are recorded',
        'accuracy_claim': None}
    write(output/'manifest.json', identity)
    results = []
    for cid in selected:
        old = rows[cid]
        old_dir, sandbox = original/'run/cases'/cid, output/'sandboxes'/cid
        hashes = {'result.json': digest(old_dir/'result.json')}
        for name in ('agent-memory.sqlite3', 'agent-memory.sqlite3.deletions.db'):
            hashes[name] = copy_closed_database(old_dir/name, sandbox/name)
        database = sandbox/'agent-memory.sqlite3'
        before_counts = stored_counts(database)
        captured_at = datetime.now(timezone.utc).isoformat()
        bundle, elapsed = await recall_copy(cid, queries[cid]['question'], database,
                                             str(database)+'.deletions.db',
                                             enable_query_facets=enable_query_facets)
        if stored_counts(database) != before_counts:
            raise RuntimeError('Read-only replay unexpectedly changed evidence counts')
        source_map = old['provider_event_source_map']
        if variant in ('packing', 'repaired'):
            packed = core_reader_context(bundle, source_map, token_budget=2048, item_limit=8)
        else:
            citations = {c.memory_id: list(c.source_event_ids) for c in bundle.citations}
            candidates = []
            for item in ordered_bundle_evidence(bundle)[:8]:
                ids = citations.get(item.id, [])
                if any(eid not in source_map for eid in ids):
                    raise ValueError('Foreign source in baseline bundle')
                candidates.append({'id': item.id, 'text': item.text, 'kind': item.kind.value,
                    'score': item.score, 'candidate_id': candidate_identity(item),
                    'source_event_ids': ids, 'source_ids': sorted({source_map[eid] for eid in ids})})
            packed = legacy_reader_context(candidates, byte_budget=6000)
        # Labels are consumed only after retrieval/packing and never passed in.
        expected = old.get('evidence', {}).get('gold_session_ids', [])
        result = {'case_id': cid, 'question_id': old['question_id'], 'variant': variant,
            'captured_at_utc': captured_at, 'retrieval_ms': elapsed,
            'original_score_unchanged': old['judge_correct'],
            'new_answer': None, 'new_judgment': None, 'original_hashes': hashes,
            'candidates': packed.candidates, 'delivered': packed.delivered,
            'reader_context': packed.context, 'reader_context_utf8_bytes': len(packed.context.encode()),
            'reader_diagnostics': packed.diagnostics, 'retrieval_metadata': dict(bundle.retrieval_metadata),
            'delivery': delivery_metrics(packed.candidates, packed.delivered),
            'selected_session_coverage': coverage(expected, packed.candidates),
            'delivered_session_coverage': coverage(expected, packed.delivered),
            'coverage_caveat': 'session coverage does not establish key-fact coverage or answer correctness',
            'evidence_counts_unchanged': True}
        if any(digest(old_dir/name) != expected_hash for name, expected_hash in hashes.items()):
            raise ValueError('Original case changed during audit')
        write(output/'cases'/cid/'result.json', result)
        results.append(result)
    summary = {'schema': identity['schema'], 'variant': variant, 'cases': len(results),
        'new_model_calls': 0, 'new_answers': 0, 'new_judgments': 0,
        'original_correct': sum(r['original_score_unchanged'] is True for r in results),
        'fully_preserved_reader_cases': sum(r['delivery']['exact_order_and_text_preserved'] for r in results),
        'selected_items': sum(r['delivery']['selected_items'] for r in results),
        'delivered_items': sum(r['delivery']['delivered_items'] for r in results),
        'partial_items': sum(r['delivery']['partial_delivered_items'] for r in results),
        'delivered_expected_sessions': sum(r['delivered_session_coverage']['covered_sessions'] for r in results),
        'expected_sessions': sum(r['delivered_session_coverage']['expected_sessions'] for r in results)}
    write(output/'summary.json', summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', type=Path, required=True)
    parser.add_argument('--core', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--variant', choices=('baseline', 'packing', 'repaired'), required=True)
    parser.add_argument('--enable-query-facets', action='store_true',
                        help='Opt into the experimental candidate route; not a default quality improvement')
    args = parser.parse_args()
    print(json.dumps(asyncio.run(collect(args.original, args.core, args.output, args.variant,
                                        enable_query_facets=args.enable_query_facets)), ensure_ascii=False))

if __name__ == '__main__':
    main()

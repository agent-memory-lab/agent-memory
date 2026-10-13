"""Real LongMemEval development diagnostic. The user runs authenticated calls."""
from __future__ import annotations

if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse, asyncio, getpass, hashlib, json, sqlite3, sys, time, warnings
from contextlib import closing
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from experiments.public_longmemeval.core import core_identity, input_identity
from experiments.public_longmemeval.extraction import ClaimGenerator, chunks
from experiments.public_longmemeval.public_retrieval import REPO_REVISION, sha, blind_sessions, BlindSession, bm25

ROOT = Path(__file__).resolve().parent
ARMS = ('no-memory', 'bm25-raw-chunk', 'recent-raw-chunk', 'agent-memory')
CONTEXT_BYTES = 6000

def verify():
    if sys.version_info < (3,13):
        raise RuntimeError('Requires installed Python 3.13+; nothing is installed automatically')
    if any(not (ROOT/"smoke"/name).is_file() for name in ("corpus.json", "query.json", "gold.evaluator-only.json")):
        raise RuntimeError("Optional sample is absent; run download_full and prepare_smoke explicitly first")
    identity=core_identity()
    inputs=input_identity(ROOT/"smoke")
    manifest=json.loads((ROOT/'smoke/manifest.json').read_text(encoding='utf-8'))
    for name,digest in manifest['artifacts'].items():
        if sha(ROOT/'smoke'/name)!=digest: raise ValueError('Dataset artifact integrity check failed')
    record=json.loads((ROOT/'smoke/corpus.json').read_text(encoding='utf-8'))
    query=json.loads((ROOT/'smoke/query.json').read_text(encoding='utf-8'))
    blind_sessions(record)
    if set(query)!={'case_id','question','question_date'} or query['case_id']!=record['case_id']:
        raise ValueError('Unexpected query fields')
    parts=sum(len(list(chunks('\n'.join(t['role']+': '+t['content'] for t in s['turns'])))) for s in record['sessions'])
    return record,query,{'sessions':len(record['sessions']),'turns':sum(len(s['turns']) for s in record['sessions']),
                        'extraction_calls':parts,'answer_calls':4,'max_calls_needed':parts+4,'core_identity':identity,'input_artifacts':inputs,
                        'dataset_revision':manifest['dataset_revision'],'context_utf8_byte_cap':CONTEXT_BYTES}

def pack(memories):
    """Only count actually delivered evidence; final item may be explicitly partial."""
    delivered=[]; remaining=CONTEXT_BYTES
    for m in memories:
        prefix='- '; suffix='\n'
        budget=remaining-len((prefix+suffix).encode())
        if budget<=0: break
        body=m['text'].encode()[:budget].decode('utf-8',errors='ignore')
        if not body: break
        text=prefix+body+suffix
        delivered.append({**m,'text':body,'partial':body!=m['text']})
        remaining-=len(text.encode())
        if body!=m['text']: break
    return ''.join('- '+m['text']+'\n' for m in delivered) or '(no memory retrieved)',delivered

async def experiment(client, output):
    from experiments.public_longmemeval.transport import atomic_write_json
    record,query,plan=verify()
    output.mkdir(parents=True,exist_ok=False)
    from agent_memory import MemoryScope
    from agent_memory.capture.policy import CaptureSanitizer
    from agent_memory.composition import build_local_kernel
    from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter, RawInteraction
    from agent_memory.providers import GeneratedTrajectoryClaimExtractor
    from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory
    database=output/'agent-memory.sqlite3'; generator=ClaimGenerator(client)
    memory=UnifiedMemory(build_local_kernel(database,extractor=GeneratedTrajectoryClaimExtractor(generator,
        provider='openai-compatible-user-selected',model=client.model,
        event_types=('user.message','agent.model.completed','agent.tool.completed'),fail_open=False)),
        MemoryScope('public-longmemeval-real',user_id=record['case_id'],session_id='one-question'),
        journal=SQLiteDeletionJournal(str(database)+'.deletions.db'),sanitizer=CaptureSanitizer(redact_builtin=False))
    result={'status':'running','evidence_class':'real_public_dataset_user_run','official_score_equivalent':False,
      'case_id':query['case_id'],'question':query['question'],'question_date':query['question_date'],
      'input_artifacts':plan['input_artifacts'],'protocol':plan,'runner_sha256':sha(Path(__file__)),'model':client.model,'embedding_mode':'default-no-embedding',
      'core_recall_estimated_token_budget':2048,'arms':{},'events_completed':0,'extraction_seconds':None,'provider_event_source_map':{},
      'purpose':'Single selected development case diagnostic; not a representative accuracy estimate'}
    start=time.monotonic(); initialized=False
    def checkpoint():
        result['seconds']=time.monotonic()-start
        result['model_calls']=len(client.receipts)
        result['receipts']=client.receipts
        if hasattr(client,'budget_summary'):
            result['cumulative_budget']=client.budget_summary()
        atomic_write_json(output/'result.json',result)
    async def answer(name, candidates, retrieval_seconds):
        context,delivered=pack(candidates)
        result['arms'][name]={'status':'answering','retrieved_candidates':candidates,'delivered_memories':delivered,
          'reader_context':context,'context_utf8_bytes':len(context.encode()),'retrieval_seconds':retrieval_seconds}
        checkpoint(); started=time.monotonic()
        answer=await client.chat('Answer the question using only the retrieved memory. Do not use outside knowledge. '
          'If the memory does not provide enough information, say you do not know. Answer concisely.\n'
          f"Question date: {query['question_date']}\nQuestion: {query['question']}\nRetrieved memory:\n{context}",
          'answer:'+name,limit=384)
        result['arms'][name].update(status='completed',hypothesis=answer,answer_seconds=time.monotonic()-started)
        checkpoint()
    try:
        raw=[BlindSession(s['source_id']+f'-part-{i:03d}',s['occurred_at'],'['+s['occurred_at']+'] '+c)
          for s in record['sessions'] for i,c in enumerate(chunks('\n'.join(t['role']+': '+t['content'] for t in s['turns'])))]
        byid={x.source_id:x for x in raw}
        def item(x): return {'id':x.source_id,'text':x.text,'source_ids':[x.source_id.split('-part-')[0]]}
        await answer('no-memory',[],0)
        started=time.monotonic();ranked=bm25(query['question'],raw,8)
        await answer('bm25-raw-chunk',[item(byid[k]) for k in ranked],time.monotonic()-started)
        started=time.monotonic();recent=sorted(raw,key=lambda x:(x.occurred_at,x.source_id),reverse=True)[:8]
        await answer('recent-raw-chunk',[item(x) for x in recent],time.monotonic()-started)
        await memory.initialize();initialized=True; adapter=AgentMemoryComparisonAdapter(memory);started=time.monotonic()
        for s in record['sessions']:
            for i,c in enumerate(chunks('\n'.join(t['role']+': '+t['content'] for t in s['turns']))):
                capture=await adapter.add(RawInteraction(s['source_id']+f'-part-{i:03d}','user',c,datetime.fromisoformat(s['occurred_at'])))
                if not capture.provider_event_id: raise RuntimeError('Core returned no provider event identifier')
                result['provider_event_source_map'][capture.provider_event_id]=s['source_id']
                result['events_completed']+=1;checkpoint()
                print('Saved memory segment',result['events_completed'],'/',plan['extraction_calls'],flush=True)
        result['extraction_seconds']=time.monotonic()-started
        result['extraction_claims_proposed']=generator.proposed
        result['extraction_claims_meeting_confidence_threshold']=generator.meeting_confidence_threshold
        with closing(sqlite3.connect(database)) as conn:
            result['persisted_claims']=conn.execute('SELECT COUNT(*) FROM claims').fetchone()[0]
        if not result['persisted_claims']: raise RuntimeError('No facts persisted; write path is not validated')
        started=time.monotonic()
        bundle=await memory.recall(query['question'],limit=8,token_budget=2048,include_current_state=False)
        citations={c.memory_id:list(c.source_event_ids) for c in bundle.citations}
        items=sorted((*bundle.relevant_memories,*bundle.episodes,*bundle.procedures),key=lambda x:(-x.score,x.id))[:8]
        source_map=result['provider_event_source_map']
        if any(sid not in source_map for x in items for sid in citations.get(x.id,[])):
            raise RuntimeError('Recall cited an event outside the captured public history')
        candidates=[{'id':x.id,'text':x.text,'kind':x.kind.value,'source_event_ids':citations.get(x.id,[]),
                     'source_ids':sorted({source_map[sid] for sid in citations.get(x.id,[])})} for x in items]
        await answer('agent-memory',candidates,time.monotonic()-started)
        result['status']='completed';result['answer_accuracy']=None
    except BaseException as e:
        result['status']='incomplete';result['error_type']=type(e).__name__
        raise
    finally:
        try:
            if initialized: await memory.close()
        except Exception as e:
            result['status']='incomplete'
            result['cleanup_error_type']=type(e).__name__
            raise
        finally:
            checkpoint()
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['preflight','run'])
    p.add_argument('--config',type=Path,default=ROOT/'config.local.json')
    args=p.parse_args();_,_,plan=verify()
    print(json.dumps(plan,ensure_ascii=False,indent=2))
    if args.command=='preflight': return
    from experiments.public_longmemeval.transport import Config, Client, prompt_api_key
    if not args.config.is_file():
        print('Copy config.example.json to config.local.json and fill your endpoint and verified tariff first.',file=sys.stderr)
        raise SystemExit(1)
    config=Config(**json.loads(args.config.read_text(encoding='utf-8')))
    if config.max_calls<plan['max_calls_needed']: raise ValueError('Configured call cap is too small for full history')
    print('Public corpus and prompts will be sent to your configured provider. No gold answer is sent.')
    print('Keep state/ in this folder across every run. Never delete/copy it to reset accounting.')
    if input('Type RUN REAL PUBLIC EXPERIMENT to proceed: ').strip()!='RUN REAL PUBLIC EXPERIMENT': return
    key=prompt_api_key()
    if not key: raise ValueError('No key supplied')
    with Client(config,key,ROOT/'state') as client:
        client.accept_budget()
        asyncio.run(experiment(client,ROOT/'runs'/('run-'+datetime.now().strftime('%Y%m%d-%H%M%S'))))
    print('Saved answers and receipts. Run evaluate.py on the saved run directory for independent manual review.')
if __name__=='__main__':
    try: main()
    except Exception as e:
        from experiments.public_longmemeval.transport import TransportError
        if isinstance(e,TransportError): print(str(e),file=sys.stderr)
        print('Stopped safely:',type(e).__name__,'Inspect persisted result and state; do not retry unresolved requests.',file=sys.stderr)
        raise SystemExit(1)

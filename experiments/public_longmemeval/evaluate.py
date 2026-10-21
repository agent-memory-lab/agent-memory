"""Offline evaluator only: gold is opened AFTER completed blind answers exist."""
if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse, hashlib, json, random, re
from experiments.public_longmemeval.core import input_identity
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def reader_comparison(result):
    """Carry recorded run semantics; never infer a newer protocol for old results."""
    protocol=result.get('protocol')
    contract=protocol.get('reader_contract') if isinstance(protocol,dict) else None
    if not isinstance(contract,dict):
        return {'reader_contract':None,'reader_contract_status':'unknown',
                'cross_arm_budget_equivalence':None,
                'reader_comparison_note':'No reader contract was recorded; cross-arm context-budget equivalence is unknown.'}
    equivalent=contract.get('cross_arm_budget_equivalence')
    if type(equivalent) is not bool: equivalent=None
    note=contract.get('comparison_note')
    if equivalent is None:
        note='The recorded contract does not declare a boolean cross-arm budget equivalence; equivalence is unknown.'
    elif not isinstance(note,str) or not note.strip():
        note='Cross-arm budget equivalence is a recorded protocol declaration, not independently established by scoring.'
    return {'reader_contract':contract,'reader_contract_status':'recorded',
            'cross_arm_budget_equivalence':equivalent,'reader_comparison_note':note}

def evaluate(run):
    result=json.loads((run/'result.json').read_text(encoding='utf-8'))
    names=('no-memory','bm25-raw-chunk','recent-raw-chunk','agent-memory')
    if result['status']!='completed' or set(result['arms'])!=set(names) or any(result['arms'][a]['status']!='completed' for a in names):
        raise ValueError('Incomplete run: retain diagnostics but do not score as a complete comparison')
    if result.get('input_artifacts') != input_identity(ROOT/'smoke'):
        raise ValueError('Run input binding differs from the current evaluator artifacts')
    manifest=json.loads((ROOT/'smoke/manifest.json').read_text(encoding='utf-8')); gold_path=ROOT/'smoke/gold.evaluator-only.json'
    if hashlib.sha256(gold_path.read_bytes()).hexdigest()!=manifest['artifacts']['gold.evaluator-only.json']:
        raise ValueError('Gold integrity check failed')
    gold=json.loads(gold_path.read_text(encoding='utf-8'))
    if result['case_id']!=gold['case_id']: raise ValueError('Case mismatch')
    relevant=set(gold['answer_source_ids']); shuffled=list(names)
    random.Random(hashlib.sha256((run/'result.json').read_bytes()).hexdigest()).shuffle(shuffled)
    scores={};review={'case_id':gold['case_id'],'question':result['question'],'reference_answer':gold['answer'],
      'rubric':'Judge meaning, not exact words. 1 = fully answers the question with the updated fact and no contradiction; 0 = incorrect, missing, outdated, or abstaining on this answerable question; null = uncertain, request a second independent human. Do not infer quality from source recall. Reviewer should not inspect arm_map.json until judgments are saved.',
      'reviewer':'','answers':{},**reader_comparison(result)}
    for i,name in enumerate(shuffled):
        arm=result['arms'][name]
        sources={sid for m in arm['delivered_memories'] for sid in m['source_ids']}
        scores[name]={'source_session_recall_fraction':len(sources&relevant)/len(relevant) if relevant else None,
                     'source_session_recall_all':int(relevant<=sources) if relevant else None,
                     'delivered_source_ids':sorted(sources),'required_source_count':len(relevant),
                     'note':'Source provenance/session coverage only; a partially delivered chunk or lossy extracted fact may omit the answer-bearing span.'}
        review['answers'][chr(65+i)]={'answer':arm['hypothesis'],'correct':None,'reason':''}
    evidence={'case_id':gold['case_id'],'arms':scores,'qa_accuracy':None,'official_score_equivalent':False,
              'scope':'Selected one-question development diagnostic; no population-quality conclusion',
              **reader_comparison(result)}
    for name,value in [('evidence_recall.json',evidence),('manual_review.json',review),('arm_map.json',dict(zip('ABCD',shuffled)))]:
        path=run/name
        if path.exists(): raise FileExistsError('Do not overwrite an existing review: '+name)
    for name,value in [('evidence_recall.json',evidence),('manual_review.json',review),('arm_map.json',dict(zip('ABCD',shuffled)))]:
        (run/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8', newline="\n")
    return evidence

def summarize(run):
    review=json.loads((run/'manual_review.json').read_text(encoding='utf-8')); mapping=json.loads((run/'arm_map.json').read_text(encoding='utf-8'))
    if not review.get('reviewer','').strip(): raise ValueError('Name independent reviewer before summarizing')
    if set(review['answers'])!=set(mapping): raise ValueError('Review arms changed')
    scores={}
    for letter,arm in mapping.items():
        row=review['answers'][letter]
        if type(row['correct']) is not int or row['correct'] not in (0,1) or not row['reason'].strip():
            raise ValueError('Each arm needs explicit integer 0/1 and a reason; uncertain is unscored')
        scores[arm]={'manual_correct':row['correct'],'reason':row['reason']}
    output={'case_id':review['case_id'],'reviewer':review['reviewer'],'arms':scores,
            'scope':'One selected development question only, not benchmark accuracy or evidence of a novel mechanism',
            **reader_comparison(json.loads((run/'result.json').read_text(encoding='utf-8'))
                                if (run/'result.json').is_file() else {})}
    (run/'manual_summary.json').write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n',encoding='utf-8', newline="\n")
    return output
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('command',choices=['prepare','summarize']);p.add_argument('run',type=Path)
    a=p.parse_args();print(json.dumps(evaluate(a.run) if a.command=='prepare' else summarize(a.run),ensure_ascii=False,indent=2))

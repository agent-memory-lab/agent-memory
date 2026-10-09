#!/usr/bin/env python3
"""Compare observed synthetic regressions. Never infer real-data efficacy."""
import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--baseline', type=Path, required=True)
p.add_argument('--candidate', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
b, c = [json.loads(path.read_text()) for path in (a.baseline, a.candidate)]
if b['fixture_sha256'] != c['fixture_sha256']:
    raise SystemExit('Fixture snapshots differ; comparison refused.')

def values(report):
    return {(r['case_id'],r['path']):r for r in report['retrieval_results']}

bv, cv = values(b), values(c)
rows=[]
for key in sorted(bv.keys() & cv.keys()):
    x,y=bv[key],cv[key]
    row={'case_id':key[0],'path':key[1],
         'baseline_ids':x['observation'].get('memory_ids',[]),
         'candidate_ids':y['observation'].get('memory_ids',[]),
         'relevant_hits_delta':y['relevant_hits']-x['relevant_hits'],
         'forbidden_hits_delta':y['forbidden_hits']-x['forbidden_hits'],
         'baseline_text_hit':x.get('required_text_hit'),
         'candidate_text_hit':y.get('required_text_hit'),
         'baseline_cost_samples':x['cost_samples'],'candidate_cost_samples':y['cost_samples'],
         'baseline_latency_p50_ms':x['latency_p50_ms'],'candidate_latency_p50_ms':y['latency_p50_ms'],
         'baseline_latency_p95_ms':x['latency_p95_ms'],'candidate_latency_p95_ms':y['latency_p95_ms'],
         'baseline_error':x['observation'].get('error_type'),'candidate_error':y['observation'].get('error_type')}
    rows.append(row)
report={'classification':'Synthetic mechanism comparison only; no real quality or real benefit measured.',
        'fixture_sha256':b['fixture_sha256'],'baseline_source_tree':b['source_tree'],'candidate_source_tree':c['source_tree'],
        'baseline_label':b['label'],'candidate_label':c['label'],
        'baseline_totals':b['totals'],'candidate_totals':c['totals'],
        'baseline_pipeline':b['pipeline_fixtures'],'candidate_pipeline':c['pipeline_fixtures'],
        'matched_cases':len(rows),'baseline_only':sorted(bv.keys()-cv.keys()),'candidate_only':sorted(cv.keys()-bv.keys()),
        'observed_forbidden_hits':sum(v['forbidden_hits'] for v in cv.values()),
        'observed_errors':sum('error_type' in v['observation'] for v in cv.values()),
        'case_deltas':rows,
        'limits':['Reported timing differences include uncontrolled OS caches and host contention.',
                  'SQL counts are logical result materialization, never rows scanned or physical I/O.',
                  'A synthetic regression pass is not promotion evidence for a learned policy.']}
a.out.parent.mkdir(parents=True,exist_ok=True)
a.out.write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
print(json.dumps({'output':str(a.out),'matched_cases':len(rows),'forbidden_hits':report['observed_forbidden_hits'],'errors':report['observed_errors']}))

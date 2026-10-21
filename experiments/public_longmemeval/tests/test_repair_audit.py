"""Synthetic checks for preserving source evidence during offline comparisons."""
import sqlite3
import asyncio
from contextlib import closing

import pytest

from experiments.public_longmemeval.repair_audit import (
    collect, copy_closed_database, coverage, delivery_metrics, digest,
    isolated_destination, records,
)


def test_copy_is_independent_and_preserves_source_bytes(tmp_path):
    source, target = tmp_path/'source.db', tmp_path/'copy.db'
    with closing(sqlite3.connect(source)) as db, db:
        db.execute('CREATE TABLE evidence (text TEXT)')
        db.execute('INSERT INTO evidence VALUES (?)', ('authored synthetic fact',))
    before = digest(source)
    assert copy_closed_database(source, target) == before
    with closing(sqlite3.connect(target)) as db, db:
        assert db.execute('SELECT text FROM evidence').fetchone()[0] == 'authored synthetic fact'
        db.execute('DELETE FROM evidence')
    assert digest(source) == before
    assert not (tmp_path/'source.db-wal').exists()
    assert not (tmp_path/'source.db-shm').exists()


def test_existing_destination_cannot_be_overwritten(tmp_path):
    source, target = tmp_path/'source.db', tmp_path/'target.db'
    target.write_bytes(b'previous experiment')
    with pytest.raises(ValueError, match='already exists'):
        copy_closed_database(source, target)
    assert target.read_bytes() == b'previous experiment'


@pytest.mark.parametrize('suffix', ['-wal', '-journal'])
def test_uncheckpointed_source_is_refused(tmp_path, suffix):
    source = tmp_path/'source.db'
    source.write_bytes(b'not read before sidecar check')
    (tmp_path/('source.db'+suffix)).write_bytes(b'pending changes')
    with pytest.raises(ValueError, match='uncheckpointed'):
        copy_closed_database(source, tmp_path/'target.db')
    assert not (tmp_path/'target.db').exists()


def test_output_cannot_overlap_frozen_source_or_core(tmp_path):
    original, core = tmp_path/'original', tmp_path/'core'
    original.mkdir()
    core.mkdir()
    for output in (original, original/'new', core/'new', tmp_path):
        with pytest.raises(ValueError, match='separate'):
            isolated_destination(original, core, output)
    symlink = tmp_path/'alias'
    symlink.symlink_to(original, target_is_directory=True)
    with pytest.raises(ValueError, match='separate'):
        isolated_destination(original, core, symlink/'new')
    output = tmp_path/'separate'
    assert isolated_destination(original, core, output)[2] == output
    output.mkdir()
    with pytest.raises(ValueError, match='already exists'):
        isolated_destination(original, core, output)


def test_delivery_compares_occurrences_and_text_not_just_source_presence():
    candidates = [
        {'id': 'shared', 'candidate_id': 'first-span', 'text': 'old statement', 'source_ids': ['s']},
        {'id': 'shared', 'candidate_id': 'second-span', 'text': 'latest statement', 'source_ids': ['s']},
    ]
    clipped = [{**candidates[0], 'partial': False},
               {**candidates[1], 'text': 'latest', 'partial': True}]
    assert coverage(['s'], clipped)['session_recall'] == 1.0
    assert not delivery_metrics(candidates, clipped)['exact_order_and_text_preserved']
    assert not delivery_metrics(candidates, candidates[::-1])['exact_order_and_text_preserved']
    assert delivery_metrics(candidates, candidates)['exact_order_and_text_preserved']
    assert delivery_metrics(candidates, clipped)['partial_delivered_items'] == 1


def test_jsonl_preserves_unicode_line_separator_inside_string(tmp_path):
    path = tmp_path/'records.jsonl'
    path.write_text('{"text":"first\u2028second"}\n', encoding='utf-8')
    assert records(path) == [{'text': 'first\u2028second'}]


def test_experimental_route_cannot_be_enabled_in_baseline(tmp_path):
    output = tmp_path/'output'
    with pytest.raises(ValueError, match='separate repaired experiment'):
        asyncio.run(collect(tmp_path/'original', tmp_path/'core', output, 'baseline',
                            enable_query_facets=True))
    assert not output.exists()

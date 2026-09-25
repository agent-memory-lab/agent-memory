import asyncio
from dataclasses import replace
from datetime import timedelta, timezone

import pytest

from test_ontology_queries import NOW, SCHEMA, SCOPE, prepare, projection, store


def options(at_time):
    return dict(ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version,
                at_time=at_time)


@pytest.mark.parametrize("offset", [-7, 0, 8])
def test_search_matches_exact_query_across_offsets(store, offset):
    async def scenario():
        await prepare(store)
        value = projection("person:a", "person:b")
        value = replace(value, assertion=replace(value.assertion,
            valid_from=NOW.astimezone(timezone(timedelta(hours=offset))),
            valid_to=(NOW + timedelta(hours=1)).astimezone(timezone(timedelta(hours=-offset)))))
        await store.upsert_projection(value)
        for query_offset in (-7, 0, 8):
            at_time = NOW.astimezone(timezone(timedelta(hours=query_offset)))
            found = await store.search("shared", SCOPE, limit=8, max_scan=16, **options(at_time))
            exact = await store.get_assertions(SCOPE, (value.assertion.assertion_id,), **options(at_time))
            assert {item.item.id for item in found} == {item.assertion_id for item in exact} == {value.assertion.assertion_id}
    asyncio.run(scenario())


def test_half_open_interval_preserves_microseconds(store):
    async def scenario():
        await prepare(store)
        start = NOW + timedelta(microseconds=123456)
        end = start + timedelta(microseconds=2)
        value = projection("person:a", "person:b")
        value = replace(value, assertion=replace(value.assertion,
            valid_from=start.astimezone(timezone(timedelta(hours=8))),
            valid_to=end.astimezone(timezone(timedelta(hours=-7)))))
        await store.upsert_projection(value)
        for at_time, visible in ((start-timedelta(microseconds=1), False),
                                 (start, True), (end-timedelta(microseconds=1), True),
                                 (end, False), (end+timedelta(microseconds=1), False)):
            found = await store.search("shared", SCOPE, limit=8, max_scan=16, **options(at_time))
            exact = await store.get_assertions(SCOPE, (value.assertion.assertion_id,), **options(at_time))
            neighbors = await store.neighbors(SCOPE, ("person:a",), **options(at_time))
            assert bool(found) is visible
            assert bool(exact) is visible
            assert bool(neighbors) is visible
    asyncio.run(scenario())


def test_search_scan_order_uses_instant_not_local_clock(store):
    async def scenario():
        await prepare(store)
        older, newer = projection("person:a", "person:b"), projection("person:c", "person:d")
        older = replace(older, assertion=replace(older.assertion,
            valid_from=NOW.astimezone(timezone(timedelta(hours=8)))))
        newer = replace(newer, assertion=replace(newer.assertion,
            valid_from=(NOW+timedelta(minutes=1)).astimezone(timezone(timedelta(hours=-7)))))
        await store.upsert_projection(older)
        await store.upsert_projection(newer)
        found = await store.search("shared", SCOPE, limit=1, max_scan=1,
            **options(NOW+timedelta(hours=1)))
        assert [item.item.id for item in found] == [newer.assertion.assertion_id]
    asyncio.run(scenario())


def test_search_rejects_naive_query_time(store):
    async def scenario():
        await prepare(store)
        with pytest.raises(ValueError, match="timezone aware"):
            await store.search("shared", SCOPE, limit=8, max_scan=16,
                **options(NOW.replace(tzinfo=None)))
    asyncio.run(scenario())

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from orbit_search import SearchOperationError, SearchQuery

from orbit_search_elasticsearch import ElasticsearchSearchStore


class FakeClient:
    def __init__(self) -> None:
        self.pages: list[dict[str, Any]] = []
        self.requests: list[dict[str, Any]] = []
        self.closed_pits: list[str] = []
        self.closed = False
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None

    def options(self, **_kwargs: object) -> FakeClient:
        """Mirror the official client's transport-options view for tests."""
        return self

    async def open_point_in_time(self, *, index: str, keep_alive: str) -> dict[str, str]:
        assert index == "articles" and keep_alive == "1m"
        return {"id": "snapshot-one"}

    async def close_point_in_time(self, *, id: str) -> dict[str, bool]:
        self.closed_pits.append(id)
        return {"succeeded": True}

    async def search(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        if self.entered and self.release:
            self.entered.set()
            await self.release.wait()
        return self.pages.pop(0)

    async def close(self) -> None:
        self.closed = True


def hit(number: int) -> dict[str, Any]:
    return {
        "_id": f"id-{number}",
        "_score": 1.25,
        "_source": {"title": "hello"},
        "sort": [1.25, number],
    }


@pytest.mark.asyncio
async def test_pit_cursor_is_bound_to_query_and_closed_after_final_page() -> None:
    client = FakeClient()
    client.pages.extend(
        [
            {"hits": {"hits": [hit(1), hit(2), hit(3)]}, "took": 2, "pit_id": "snapshot-two"},
            {"hits": {"hits": [hit(3)]}, "took": 1},
        ]
    )
    store = ElasticsearchSearchStore(client, cursor_secret=b"x" * 32)
    query = SearchQuery(text="hello", fields=("title",), page_size=2, filters={"public": True})
    first = await store.search("articles", query)
    assert [item.id for item in first.hits] == ["id-1", "id-2"]
    assert first.next_cursor
    assert client.requests[0]["query"]["bool"]["filter"] == [{"term": {"public": True}}]
    assert client.requests[0]["pit"]["id"] == "snapshot-one"
    second = await store.search("articles", query.model_copy(update={"cursor": first.next_cursor}))
    assert second.next_cursor is None
    assert client.requests[1]["pit"]["id"] == "snapshot-two"
    assert client.requests[1]["search_after"] == [1.25, 2]
    assert client.closed_pits == ["snapshot-two"]

    with pytest.raises(SearchOperationError, match="cursor is invalid"):
        await store.search(
            "articles", SearchQuery(text="different", fields=("title",), cursor=first.next_cursor)
        )
    await store.aclose()
    assert client.closed


@pytest.mark.asyncio
async def test_close_drains_searches_and_rejects_new_work() -> None:
    client = FakeClient()
    client.entered = asyncio.Event()
    client.release = asyncio.Event()
    client.pages.append({"hits": {"hits": [hit(1)]}, "took": 0})
    store = ElasticsearchSearchStore(client, cursor_secret=b"x" * 32)
    running = asyncio.create_task(
        store.search("articles", SearchQuery(text="hello", fields=("title",)))
    )
    await client.entered.wait()
    closing = asyncio.create_task(store.aclose())
    await asyncio.sleep(0)
    assert not client.closed
    with pytest.raises(SearchOperationError, match="closing"):
        await store.search("articles", SearchQuery(text="hello", fields=("title",)))
    client.release.set()
    await running
    await closing
    assert client.closed


@pytest.mark.asyncio
async def test_partial_and_malformed_provider_responses_fail_safely() -> None:
    client = FakeClient()
    client.pages.append({"timed_out": True, "hits": {"hits": []}})
    store = ElasticsearchSearchStore(client, cursor_secret=b"x" * 32)
    with pytest.raises(SearchOperationError, match="timed out"):
        await store.search("articles", SearchQuery(text="hello", fields=("title",)))
    assert client.closed_pits == ["snapshot-one"]

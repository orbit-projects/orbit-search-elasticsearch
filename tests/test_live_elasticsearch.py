# Copyright 2026-present Orbit Contributors.
# Licensed under the Apache License, Version 2.0.
"""Opt-in provider checks against a disposable Elasticsearch index."""

from __future__ import annotations

import os
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from orbit_search import SearchQuery

from orbit_search_elasticsearch import ElasticsearchSearchStore


@pytest.mark.asyncio
async def test_live_elasticsearch_search_and_pit_pagination() -> None:
    """Exercise the official SDK and signed PIT cursor against a real server."""
    endpoint = os.environ.get("ORBIT_ELASTICSEARCH_URL")
    if not endpoint:
        pytest.skip("Set ORBIT_ELASTICSEARCH_URL to run the live Elasticsearch check.")
    parsed = urlsplit(endpoint)
    local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if not local and os.environ.get("ORBIT_ELASTICSEARCH_ALLOW_REMOTE_TESTS") != "1":
        pytest.skip("Set ORBIT_ELASTICSEARCH_ALLOW_REMOTE_TESTS=1 for a remote test service.")

    from elasticsearch import AsyncElasticsearch

    index = f"orbit-live-{uuid4().hex}"
    api_key = os.environ.get("ORBIT_ELASTICSEARCH_API_KEY")
    ca_certs = os.environ.get("ORBIT_ELASTICSEARCH_CA_CERTS")
    setup_options: dict[str, object] = {"hosts": [endpoint], "request_timeout": 10}
    if api_key is not None:
        setup_options["api_key"] = api_key
    if ca_certs is not None:
        setup_options["ca_certs"] = ca_certs
    setup_client = AsyncElasticsearch(**setup_options)
    store: ElasticsearchSearchStore | None = None
    try:
        await setup_client.indices.create(
            index=index,
            mappings={
                "properties": {
                    "title": {"type": "text"},
                    "visibility": {"type": "keyword"},
                }
            },
        )
        await setup_client.index(
            index=index,
            id="public-one",
            document={"title": "async search pagination", "visibility": "public"},
        )
        await setup_client.index(
            index=index,
            id="public-two",
            document={"title": "async search adapters", "visibility": "public"},
        )
        await setup_client.index(
            index=index,
            id="private",
            document={"title": "async search private", "visibility": "private"},
        )
        await setup_client.indices.refresh(index=index)

        store = ElasticsearchSearchStore.connect(
            endpoint,
            api_key=api_key,
            ca_certs=ca_certs,
            cursor_secret=b"e" * 32,
            request_timeout=5,
        )
        query = SearchQuery(
            text="async search",
            fields=("title",),
            filters={"visibility": "public"},
            page_size=1,
        )
        first = await store.search(index, query)
        assert len(first.hits) == 1
        assert first.next_cursor is not None
        second = await store.search(index, query.model_copy(update={"cursor": first.next_cursor}))
        assert len(second.hits) == 1
        assert second.next_cursor is None
        assert {first.hits[0].id, second.hits[0].id} == {"public-one", "public-two"}
    finally:
        if store is not None:
            await store.aclose()
        await setup_client.indices.delete(index=index, ignore_unavailable=True)
        await setup_client.close()

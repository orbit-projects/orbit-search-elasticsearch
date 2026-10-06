# Copyright 2026-present Orbit Contributors.
# Licensed under the Apache License, Version 2.0.
"""Bounded asynchronous Elasticsearch implementation of ``orbit_search.SearchStore``."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import importlib
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from orbit_search import (
    SearchHit,
    SearchIndexNotFound,
    SearchOperationError,
    SearchPage,
    SearchQuery,
    validate_collection_name,
)

_PIT_TTL = "1m"
_MAX_CURSOR = 4_096
_MAX_RESPONSE_HITS = 101


class _AsyncClient(Protocol):
    def options(self, **kwargs: Any) -> _AsyncClient:
        """Return a client view with bounded transport-level request settings."""

    async def open_point_in_time(self, *, index: str, keep_alive: str) -> Any:
        """Open a point-in-time snapshot for the selected index."""

    async def close_point_in_time(self, *, id: str) -> Any:
        """Release a previously opened point-in-time snapshot."""

    async def search(self, **kwargs: Any) -> Any:
        """Execute a bounded search request against an index or snapshot."""

    async def close(self) -> Any:
        """Close the owned async Elasticsearch transport."""


def _body(response: Any) -> Mapping[str, Any]:
    value = getattr(response, "body", response)
    if not isinstance(value, Mapping):
        raise SearchOperationError("Elasticsearch returned an invalid response.")
    return value


def _stable_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class ElasticsearchSearchStore:
    """Search Elasticsearch with async I/O, PIT consistency, and signed opaque cursors.

    The store owns the client passed to it and closes it from :meth:`aclose`. Cursors are bound
    to the configured secret so workers can share cursors, and expire with the one-minute PIT.
    """

    def __init__(
        self,
        client: _AsyncClient,
        *,
        cursor_secret: bytes,
        request_timeout: float = 10.0,
    ) -> None:
        if not math.isfinite(request_timeout) or not 0.1 <= request_timeout <= 120:
            raise ValueError("request_timeout must be between 0.1 and 120 seconds.")
        if not isinstance(cursor_secret, bytes) or not 32 <= len(cursor_secret) <= 64:
            raise ValueError("cursor_secret must contain 32 through 64 secret bytes.")
        self._client = client
        self._request_timeout = request_timeout
        self._signing_key = cursor_secret
        self._condition = asyncio.Condition()
        self._active = 0
        self._closing = False
        self._closed = False
        self._close_lock = asyncio.Lock()

    @classmethod
    def connect(
        cls,
        hosts: str | Sequence[str],
        *,
        api_key: str | None = None,
        basic_auth: tuple[str, str] | None = None,
        ca_certs: str | None = None,
        cursor_secret: bytes,
        request_timeout: float = 10.0,
    ) -> ElasticsearchSearchStore:
        """Create and own an official async client; configure TLS/auth explicitly as needed."""
        if api_key is not None and basic_auth is not None:
            raise ValueError("Choose either API key or basic authentication.")
        try:
            elasticsearch = importlib.import_module("elasticsearch")
            async_client = elasticsearch.AsyncElasticsearch
        except (ImportError, AttributeError) as exc:
            raise RuntimeError(
                "Install orbit-search-elasticsearch[elasticsearch] to use this adapter."
            ) from exc
        options: dict[str, Any] = {"hosts": hosts, "request_timeout": request_timeout}
        if api_key is not None:
            options["api_key"] = api_key
        if basic_auth is not None:
            options["basic_auth"] = basic_auth
        if ca_certs is not None:
            options["ca_certs"] = ca_certs
        return cls(
            async_client(**options), cursor_secret=cursor_secret, request_timeout=request_timeout
        )

    async def search(self, collection: str, query: SearchQuery) -> SearchPage:
        """Run a match query with exact filters and stable PIT-backed forward pagination."""
        index = validate_collection_name(collection)
        if not isinstance(query, SearchQuery):
            raise TypeError("query must be a SearchQuery.")
        await self._enter()
        pit_id: str | None = None
        keep_pit = False
        try:
            fingerprint = self._fingerprint(index, query)
            search_after: list[object] | None = None
            if query.cursor is None:
                try:
                    opened = _body(
                        await self._client.open_point_in_time(index=index, keep_alive=_PIT_TTL)
                    )
                except asyncio.CancelledError:
                    raise
                except SearchOperationError:
                    raise
                except Exception as exc:
                    self._raise_provider_error(exc)
                pit_id = opened.get("id")
                if not isinstance(pit_id, str) or not pit_id:
                    raise SearchOperationError("Elasticsearch did not create a search snapshot.")
            else:
                pit_id, search_after = self._decode_cursor(query.cursor, fingerprint)

            request: dict[str, Any] = {
                "pit": {"id": pit_id, "keep_alive": _PIT_TTL},
                "query": self._query_body(query),
                "size": min(query.page_size + 1, _MAX_RESPONSE_HITS),
                "sort": [{"_score": "desc"}, {"_shard_doc": "asc"}],
                "_source": True,
                "allow_partial_search_results": False,
            }
            if search_after is not None:
                request["search_after"] = search_after
            try:
                result = _body(
                    await self._client.options(request_timeout=self._request_timeout).search(
                        **request
                    )
                )
            except Exception as exc:
                self._raise_provider_error(exc)
            if result.get("timed_out") is True:
                raise SearchOperationError("Elasticsearch search timed out.", retryable=True)
            shard_info = result.get("_shards")
            if isinstance(shard_info, Mapping) and shard_info.get("failed", 0) != 0:
                raise SearchOperationError("Elasticsearch search was incomplete.", retryable=True)
            hits_container = result.get("hits")
            raw_hits = hits_container.get("hits") if isinstance(hits_container, Mapping) else None
            if not isinstance(raw_hits, list) or len(raw_hits) > _MAX_RESPONSE_HITS:
                raise SearchOperationError("Elasticsearch returned an invalid result page.")
            has_more = len(raw_hits) > query.page_size
            page_hits = raw_hits[: query.page_size]
            hits = tuple(self._hit(item) for item in page_hits)
            next_cursor = None
            new_pit = result.get("pit_id", pit_id)
            if has_more:
                last_sort = page_hits[-1].get("sort")
                if not isinstance(last_sort, list) or len(last_sort) != 2:
                    raise SearchOperationError("Elasticsearch returned invalid pagination data.")
                if not isinstance(new_pit, str) or not new_pit:
                    raise SearchOperationError("Elasticsearch returned invalid snapshot data.")
                next_cursor = self._encode_cursor(new_pit, last_sort, fingerprint)
            took_ms = result.get("took", 0)
            if isinstance(took_ms, bool) or not isinstance(took_ms, (int, float)) or took_ms < 0:
                raise SearchOperationError("Elasticsearch returned invalid timing metadata.")
            keep_pit = has_more
            return SearchPage(hits=hits, next_cursor=next_cursor, took_ms=int(took_ms))
        finally:
            try:
                if pit_id is not None and not keep_pit:
                    await self._close_pit(pit_id)
            finally:
                await self._leave()

    async def aclose(self) -> None:
        """Stop accepting searches, drain active requests, then close the owned client."""
        async with self._close_lock:
            async with self._condition:
                if self._closed:
                    return
                self._closing = True
                await self._condition.wait_for(lambda: self._active == 0)
            try:
                await self._client.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise SearchOperationError(
                    "Elasticsearch client shutdown failed.", retryable=True
                ) from None
            else:
                async with self._condition:
                    self._closed = True

    async def _enter(self) -> None:
        async with self._condition:
            if self._closing or self._closed:
                raise SearchOperationError("Elasticsearch search store is closing.")
            self._active += 1

    async def _leave(self) -> None:
        async with self._condition:
            self._active -= 1
            if self._active == 0:
                self._condition.notify_all()

    async def _close_pit(self, pit_id: str) -> None:
        try:
            await self._client.close_point_in_time(id=pit_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            return  # PIT expires server-side after one minute; cleanup must not mask search result.

    @staticmethod
    def _query_body(query: SearchQuery) -> dict[str, object]:
        filters: list[dict[str, object]] = []
        for field, value in query.filters.items():
            if value is None:
                filters.append({"bool": {"must_not": [{"exists": {"field": field}}]}})
            else:
                filters.append({"term": {field: value}})
        return {
            "bool": {
                "must": [{"multi_match": {"query": query.text, "fields": list(query.fields)}}],
                "filter": filters,
            }
        }

    @staticmethod
    def _hit(raw: object) -> SearchHit:
        if not isinstance(raw, Mapping):
            raise SearchOperationError("Elasticsearch returned an invalid document.")
        identifier, score, source = raw.get("_id"), raw.get("_score"), raw.get("_source")
        if (
            not isinstance(identifier, str)
            or not isinstance(score, (int, float))
            or isinstance(score, bool)
        ):
            raise SearchOperationError("Elasticsearch returned invalid document metadata.")
        if not isinstance(source, Mapping):
            raise SearchOperationError("Elasticsearch returned an invalid document source.")
        try:
            return SearchHit(id=identifier, score=float(score), source=source)
        except (TypeError, ValueError):
            raise SearchOperationError(
                "Elasticsearch returned an invalid document source."
            ) from None

    @staticmethod
    def _raise_provider_error(exc: Exception) -> None:
        status = getattr(exc, "status_code", None)
        status = getattr(getattr(exc, "meta", None), "status", status)
        if status == 404:
            body = getattr(exc, "body", None)
            error = body.get("error") if isinstance(body, Mapping) else None
            if isinstance(error, Mapping) and error.get("type") == "index_not_found_exception":
                raise SearchIndexNotFound("Search collection was not found.") from None
            raise SearchOperationError("Search collection or snapshot was not found.") from None
        retryable = status in {408, 429, 500, 502, 503, 504}
        raise SearchOperationError(
            "Elasticsearch search request failed.", retryable=retryable
        ) from None

    def _fingerprint(self, index: str, query: SearchQuery) -> str:
        payload = {
            "index": index,
            "text": query.text,
            "fields": query.fields,
            "filters": dict(query.filters),
            "page_size": query.page_size,
        }
        return hashlib.sha256(_stable_json(payload)).hexdigest()

    def _encode_cursor(self, pit_id: str, sort: list[object], fingerprint: str) -> str:
        payload = _stable_json({"pit": pit_id, "sort": sort, "query": fingerprint})
        signature = hmac.digest(self._signing_key, payload, "sha256")
        token = base64.urlsafe_b64encode(payload + signature).decode("ascii").rstrip("=")
        if len(token) > _MAX_CURSOR:
            raise SearchOperationError("Elasticsearch pagination state exceeds the cursor limit.")
        return token

    def _decode_cursor(self, token: str, fingerprint: str) -> tuple[str, list[object]]:
        try:
            raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
            payload, signature = raw[:-32], raw[-32:]
            if len(signature) != 32 or not hmac.compare_digest(
                signature, hmac.digest(self._signing_key, payload, "sha256")
            ):
                raise ValueError
            decoded = json.loads(payload)
            if (
                not isinstance(decoded, dict)
                or decoded.get("query") != fingerprint
                or not isinstance(decoded.get("pit"), str)
                or not isinstance(decoded.get("sort"), list)
                or len(decoded["sort"]) != 2
            ):
                raise ValueError
            return decoded["pit"], decoded["sort"]
        except (ValueError, TypeError, json.JSONDecodeError):
            raise SearchOperationError("Search cursor is invalid or expired.") from None

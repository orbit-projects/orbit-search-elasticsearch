# Orbit Elasticsearch: architecture and boundaries

## Responsibility

`orbit-search-elasticsearch` is the optional Elasticsearch provider for the provider-neutral
`orbit-search` capability. Install both explicitly:

```bash
python -m pip install 'orbit-search' 'orbit-search-elasticsearch[elasticsearch]'
```

```python
from orbit_search_elasticsearch import ElasticsearchSearchStore
from orbit_search import SearchQuery

store = ElasticsearchSearchStore.connect(
    ["https://search.example.internal:9200"],
    api_key=api_key,  # load from a secret manager
    ca_certs="/etc/ssl/certs/search-ca.pem",
    cursor_secret=cursor_secret_from_secret_manager,
)
try:
    page = await store.search("articles", SearchQuery(text="async", fields=("title",)))
finally:
    await store.aclose()
```

The adapter translates the portable text and exact-match filters into Elasticsearch Query DSL. It
uses the official async client, PIT plus `search_after` for consistent bounded pagination, signs
opaque cursors with the configured shared secret, limits pages to 100 hits, rejects partial/time-out results,
sanitizes vendor failures, propagates cancellation, and drains active searches before closing its
owned client. PITs expire after one minute; all workers serving a cursor must share the same high-entropy
secret from a secret manager. Do not log cursors because they contain snapshot identifiers.

TLS verification uses the SDK's secure defaults; configure credentials with API keys or explicit
Basic Auth via a secret manager. There are no default credentials. This adapter does not provision
indexes, ingest documents, manage mappings, or retry searches. Validate server permissions,
version compatibility, and query mappings in the deployment environment before release.

This pre-alpha package supports Python 3.11–3.14. The official client supports async/await and
requires explicit closure; the provider owns these lifecycle details. Current client compatibility
is documented by Elastic: its 8.x Python client supports Elasticsearch 8.x and 9.x (not 10.x).
The package suite passes on Python 3.11–3.14. Its local live integration check passed on Python 3.11
against Elasticsearch 8.17.4 with `elasticsearch` 8.19.3; this does not establish production
topology or a full server/client compatibility matrix.

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `orbit-search>=0.1.0a1,<0.2`
- Optional `elasticsearch` group: `elasticsearch[async]>=8.17,<9`.
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_search_elasticsearch/__init__.py`
- `src/orbit_search_elasticsearch/store.py`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.

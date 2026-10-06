# Orbit Elasticsearch

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

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev,elasticsearch]'
pytest
ruff check src tests
ruff format --check src tests
mypy
```

Run the opt-in live test against a disposable local Elasticsearch service. It provisions and removes
one uniquely named index and tests exact filters, point-in-time pagination, and client cleanup:

```bash
ORBIT_ELASTICSEARCH_URL='http://127.0.0.1:9200' pytest -q tests/test_live_elasticsearch.py
```

Remote test services require `ORBIT_ELASTICSEARCH_ALLOW_REMOTE_TESTS=1`; pass credentials with
`ORBIT_ELASTICSEARCH_API_KEY` and a private trust root with `ORBIT_ELASTICSEARCH_CA_CERTS`.

Licensed under Apache-2.0.


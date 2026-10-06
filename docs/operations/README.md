# Orbit Elasticsearch: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

- `ORBIT_ELASTICSEARCH_ALLOW_REMOTE_TESTS`
- `ORBIT_ELASTICSEARCH_API_KEY`
- `ORBIT_ELASTICSEARCH_CA_CERTS`
- `ORBIT_ELASTICSEARCH_URL`

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

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

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.

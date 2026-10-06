# Contributing

Contributions are welcome. Open an issue before a large change so the capability contract,
provider behavior, and compatibility impact can be discussed in the open. Keep provider-specific
behavior inside this adapter and preserve the `orbit-search` contract.

Run the documented development commands before submitting a pull request. Add tests for query
translation, cursor integrity, cancellation, error mapping, and client lifecycle changes. Never
include credentials or raw provider errors in fixtures, logs, or exception messages. Changes to
supported Elasticsearch/client versions must include compatibility evidence.

By submitting a contribution, you agree that it will be licensed under Apache-2.0.


## Package documentation

Use the [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md), and [development](docs/development/README.md) guides when changing package contracts or operator behavior.

# glacon

Route records from inputs, through a middleware, to outputs: protobuf files
from S3 buckets into Iceberg tables, for example. A route reads N inputs,
transforms every batch once, and hands it to M outputs, each keeping what it
wants; a SQLite ledger lets a restarted route carry on without losing data.

| Crate | Read |
|---|---|
| [`glacon-core`](glacon-core/README.md) | The route: architecture, the traits you implement, delivery guarantees, metrics. Knows nothing of S3 or Iceberg. |
| [`glacon-helium`](glacon-helium/README.md) | The implementations: `FileStoreInput` (S3 buckets via `file_store`) and `IcebergOutput` (Iceberg tables via `helium_iceberg`), with a full example. Apps depend on this crate alone. |

[docs/runtime.md](docs/runtime.md) follows one record through every task,
from startup to shutdown.

## Development

```bash
docker compose up -d --wait   # RustFS, Polaris, Trino for glacon-helium's tests
cargo nextest run --workspace
```

Each crate's README says more about its tests. CI
(`.github/workflows/CI.yml`) runs fmt, clippy with the docs, and every test,
in the `base` image of the `Dockerfile`.

# Fluss connector E2E suite

End-to-end proof-of-concept and test harness for the Apache Fluss data
connector (`crates/data-connectors/connector-fluss`). Everything runs in
podman: a real Fluss 0.9.x cluster, a deterministic Rust data producer, and a
from-source `spiced` build queried through the SpiceAI HTTP API.

## Layout

| Path | Purpose |
|---|---|
| `podman-compose.yaml` | ZooKeeper + Fluss coordinator + 2 tablet servers + spiced |
| `Containerfile.spiced` | Builds `spiced` (with connector-fluss) from the repo |
| `Containerfile.producer` | Builds the deterministic data producer |
| `producer/` | Rust producer: `setup` / `append` / `cdc` / `mixed` workloads |
| `spicepod/spicepod.yaml` | Two datasets: log table (`append`) + PK table (`changes`), duckdb file acceleration |
| `run-e2e.sh` | Scenario runner + assertions via `POST /v1/sql` |

## Prerequisites

- podman with a running machine (`podman machine start`), podman-compose
- Network access to pull `zookeeper`, `apache/fluss`, `rust`, `debian`, `curlimages/curl`

## Usage

```sh
cd e2e/fluss
./run-e2e.sh build   # build spiced + producer images (first spiced build is slow)
./run-e2e.sh all     # full suite: cluster up -> s1..s7 -> summary
./run-e2e.sh clean   # teardown including the spiced state volume
```

## Scenarios

| # | Name | What it proves |
|---|---|---|
| s1 | bootstrap | Data produced BEFORE spiced starts is fully replayed: append count exact; CDC updates/deletes applied (late-consumer bootstrap) |
| s2 | realtime append | Live appends become SQL-visible within a 30s budget |
| s3 | cdc live | Live PK insert/update/delete stream to an exact final state |
| s4 | graceful resume | SIGTERM + restart: counts exact (checkpoint resume, no re-ingest), stream still live |
| s5 | crash resume | SIGKILL + restart: appends at-least-once (no loss), PK state exact (idempotent), stream still live |
| s6 | tablet fault | Tablet-server restart under continuous load: producer retries survive, dataset converges, spiced stays healthy |
| s7 | chaos | Coordinator paused 15s + tablet restart under load: convergence + health |

Scenario state is cumulative when run via `all` (each scenario's expected
counts build on the previous ones). Individual scenario invocations assume the
prior state exists.

## Delivery semantics being asserted

- **Log tables (`refresh_mode: append`)**: at-least-once. Offsets are
  committed to the accelerator sidecar only after a batch is durably applied,
  so a hard crash may re-deliver the tail (s5 asserts `>=`), while a graceful
  shutdown resumes exactly (s4 asserts `==`).
- **PK tables (`refresh_mode: changes`)**: exact final state in all cases —
  replayed changelog operations are idempotent upserts/deletes by primary key.
- **Bootstrap**: the PK changelog is replayed from `EARLIEST_OFFSET`; this
  requires the Fluss table's changelog retention (`table.log.ttl`) to cover
  its history (default 7 days — fine for tests).

## Notes / quirks

- Host networking everywhere: rootless podman-on-WSL2 bridge networking is
  unreliable (aardvark-dns); all services listen on localhost inside the
  machine VM.
- No compose healthchecks (need systemd under podman) — the runner polls:
  cluster readiness is probed with the idempotent `producer setup`, spiced
  with `GET /health`.
- The spiced image bakes the spicepod in; `.spice` (acceleration files +
  checkpoint sidecars) lives in the `spice-state` named volume so restart and
  crash scenarios keep state. `./run-e2e.sh clean` removes it.
- Network-partition chaos (e.g. `podman network disconnect`) is not possible
  with host networking; chaos uses `pause`/`restart` instead.

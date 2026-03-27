# AegisRaft

AegisRaft is a Java 17 implementation of the Raft consensus algorithm that runs each node as an independent HTTP service and exposes a strongly consistent replicated key-value store.

## What is implemented

- Follower, candidate, and leader roles with randomized election timeouts
- Majority-based leader election with retries after split votes
- AppendEntries replication and heartbeat RPCs over HTTP
- InstallSnapshot replication for followers that fall behind compaction
- Commit only after majority replication
- File-backed persistence for `currentTerm`, `votedFor`, and the WAL
- Snapshot-backed log compaction with crash recovery from persisted state
- Dynamic membership changes through replicated joint-to-final configuration entries
- Fault-injection hooks for crash, partition, and delay testing
- Prometheus-style metrics and structured election/term transition logging
- Dockerized 3-node cluster deployment

## Build and run

```powershell
mvn -q -DskipTests package
java -jar target\aegisraft-1.0.0.jar --config config\node1.properties
```

Start the full cluster with Docker:

```powershell
docker compose up --build
```

## External API

- `POST /kv/put`
- `GET /kv/get?key=<key>`
- `GET /metrics`

Example write:

```json
{"key":"region","value":"ap-south-1"}
```

## Internal RPCs

- `POST /raft/request-vote`
- `POST /raft/append-entries`
- `POST /raft/install-snapshot`

## Operational endpoints

- `GET /admin/state`
- `POST /admin/network`
- `POST /admin/membership`

`/admin/network` is intended for test and chaos workflows. It can block peer communication or inject delay so leader failover and log catch-up paths can be exercised repeatedly.

`/admin/membership` accepts `{ "action": "add" | "remove", "nodeId": "...", "baseUrl": "..." }` and drives a replicated joint-consensus style configuration change.

## Test suite

The integration suite starts a real cluster in-process, uses HTTP for inter-node replication, and validates:

- leader election
- replicated writes
- snapshot compaction
- snapshot-based catch-up for a newly added node
- dynamic membership add and remove flows

Run it with:

```powershell
.\scripts\run-integration-tests.ps1
```

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/FAILURE_SCENARIOS.md`

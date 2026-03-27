# Raft Architecture

## Core design

AegisRaft implements the Raft paper's core safety and liveness mechanics around a replicated append-only log. Each node persists term metadata, compaction metadata, and its WAL on local disk, communicates with peers over HTTP, and applies committed log entries to a deterministic key-value state machine.

## Request flow

1. A client sends `POST /kv/put` to the leader.
2. The leader appends the command to its WAL before replying.
3. The leader sends `AppendEntries` RPCs to followers.
4. Once a majority has acknowledged the entry, the leader advances `commitIndex`.
5. Each node applies committed entries in order to the KV state machine.

## State transitions

```text
          election timeout
FOLLOWER -----------------> CANDIDATE
   ^                           |
   |   higher term /           | majority votes
   |   valid AppendEntries     v
   +------------------------ LEADER
                higher term / AppendEntries
```

Follower:
- accepts heartbeats and log replication
- grants at most one vote per term
- becomes candidate when the election timeout fires

Candidate:
- increments term and votes for itself
- requests votes from peers
- becomes leader after majority votes
- falls back to follower if a higher term is observed

Leader:
- sends periodic heartbeats
- tracks `nextIndex` and `matchIndex` per follower
- commits entries only when replicated on a majority and from the current term
- ships snapshots when a follower falls behind the compacted prefix

## Snapshot compaction

- each applied entry is reflected in `state-machine.json`
- once the applied index advances past the configured threshold, the node records a snapshot boundary and truncates the WAL prefix
- followers that need entries older than the current snapshot receive `InstallSnapshot` before normal `AppendEntries` resumes

## Dynamic membership

- membership changes are stored as replicated log entries
- the leader first appends a joint configuration containing the old and new voter sets
- after the joint entry commits, the leader appends the final stable configuration
- quorum and election decisions use the active replicated configuration rather than static startup peers

## Persistence model

- `metadata.json`: stores `currentTerm`, `votedFor`, snapshot boundary, and replicated membership metadata
- `wal.log`: write-ahead log suffix after the latest compacted snapshot
- `state-machine.json`: persisted KV snapshot with the last applied index

This gives crash recovery for committed state while preserving Raft's required persistent metadata and the active cluster configuration.

## Concurrency model

- HTTP server threads handle inbound RPCs
- a scheduled election loop checks timeouts
- a heartbeat loop drives replication
- an apply loop advances the state machine
- a lock serializes consensus-state transitions

## Observability

- election and term transitions are logged through `java.util.logging`
- `/metrics` exposes leader changes, commit latency, replication lag, rejected requests, and RPC failures

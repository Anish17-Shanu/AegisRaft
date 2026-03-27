# Failure Scenarios

## Leader crash

1. The leader stops sending heartbeats.
2. Followers time out independently using randomized election windows.
3. One follower wins a majority and becomes the new leader.
4. The old leader restarts as a follower and catches up from the new leader's log.

## Split brain attempt

Raft prevents permanent split brain by term monotonicity and quorum rules. During a partition, an isolated old leader may still believe it is leader, but it cannot commit new entries without a majority. The majority side can elect a new leader, and once connectivity returns, the higher-term leader forces the stale node to step down.

## Log inconsistency

Followers reject `AppendEntries` when `prevLogIndex` or `prevLogTerm` does not match. The leader backs up `nextIndex` and retries until it finds the last shared prefix, then overwrites the divergent suffix. This preserves the Raft log matching property.

## Delayed messages

Heartbeat delay increases failover latency and can trigger elections when extreme, but safety remains intact because stale leaders cannot commit without quorum and higher terms always dominate older ones.

## Snapshot catch-up

When a follower or newly added node trails behind the compacted log prefix, the leader sends `InstallSnapshot` with the latest state-machine image and replicated membership metadata. The follower persists the snapshot, truncates any superseded WAL prefix, and then resumes normal `AppendEntries` from the snapshot boundary forward.

## Dynamic membership changes

Membership changes are replicated through a joint configuration entry followed by a final configuration entry. This keeps quorum calculations aligned during transitions so add and remove operations do not rely on static startup peer lists.

## Trade-offs

- HTTP keeps the implementation dependency-light and easy to operate, but gRPC would be more efficient at higher throughput.
- Persisting the full KV snapshot after every apply keeps recovery simple, but incremental snapshotting would reduce write amplification further.
- Membership reconfiguration currently supports one in-flight change at a time, which keeps the state machine simple but limits operational batching.

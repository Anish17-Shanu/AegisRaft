package com.aegisraft.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class ClusterMetrics {
    private final AtomicLong leaderChanges = new AtomicLong();
    private final AtomicLong commitLatencyMillis = new AtomicLong();
    private final AtomicLong committedEntries = new AtomicLong();
    private final AtomicLong rejectedClientRequests = new AtomicLong();
    private final AtomicLong rpcFailures = new AtomicLong();
    private final AtomicLong maxReplicationLag = new AtomicLong();

    public void recordLeaderChange() {
        leaderChanges.incrementAndGet();
    }

    public void recordCommitLatency(long latencyMillis) {
        commitLatencyMillis.updateAndGet(previous -> previous == 0 ? latencyMillis : Math.min(30_000, (previous + latencyMillis) / 2));
        committedEntries.incrementAndGet();
    }

    public void recordRejectedClientRequest() {
        rejectedClientRequests.incrementAndGet();
    }

    public void recordRpcFailure() {
        rpcFailures.incrementAndGet();
    }

    public void recordReplicationLag(long lag) {
        maxReplicationLag.accumulateAndGet(Math.max(0, lag), Math::max);
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("leaderChanges", leaderChanges.get());
        metrics.put("avgCommitLatencyMillis", commitLatencyMillis.get());
        metrics.put("committedEntries", committedEntries.get());
        metrics.put("rejectedClientRequests", rejectedClientRequests.get());
        metrics.put("rpcFailures", rpcFailures.get());
        metrics.put("maxReplicationLag", maxReplicationLag.get());
        return metrics;
    }
}

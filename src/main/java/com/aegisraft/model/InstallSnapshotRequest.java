package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record InstallSnapshotRequest(
    String leaderId,
    long term,
    long lastIncludedIndex,
    long lastIncludedTerm,
    long leaderCommit,
    ClusterMembership membership,
    Map<String, String> state
) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("leaderId", leaderId);
        payload.put("term", term);
        payload.put("lastIncludedIndex", lastIncludedIndex);
        payload.put("lastIncludedTerm", lastIncludedTerm);
        payload.put("leaderCommit", leaderCommit);
        payload.put("membership", membership.toMap());
        payload.put("state", new LinkedHashMap<>(state));
        return payload;
    }

    @SuppressWarnings("unchecked")
    public static InstallSnapshotRequest fromMap(Map<String, Object> payload) {
        return new InstallSnapshotRequest(
            (String) payload.get("leaderId"),
            ((Number) payload.get("term")).longValue(),
            ((Number) payload.get("lastIncludedIndex")).longValue(),
            ((Number) payload.get("lastIncludedTerm")).longValue(),
            ((Number) payload.get("leaderCommit")).longValue(),
            ClusterMembership.fromMap((Map<String, Object>) payload.get("membership")),
            castState((Map<String, Object>) payload.get("state"))
        );
    }

    private static Map<String, String> castState(Map<String, Object> payload) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        payload.forEach((key, value) -> values.put(key, String.valueOf(value)));
        return Map.copyOf(values);
    }
}

package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record ClusterStateResponse(
    String nodeId,
    String leaderId,
    String role,
    long currentTerm,
    long commitIndex,
    long lastApplied,
    long lastLogIndex,
    long lastLogTerm,
    int peerCount
) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("nodeId", nodeId);
        payload.put("leaderId", leaderId);
        payload.put("role", role);
        payload.put("currentTerm", currentTerm);
        payload.put("commitIndex", commitIndex);
        payload.put("lastApplied", lastApplied);
        payload.put("lastLogIndex", lastLogIndex);
        payload.put("lastLogTerm", lastLogTerm);
        payload.put("peerCount", peerCount);
        return payload;
    }
}

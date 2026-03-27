package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record InstallSnapshotResponse(long term, boolean success, long lastIncludedIndex) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("term", term);
        payload.put("success", success);
        payload.put("lastIncludedIndex", lastIncludedIndex);
        return payload;
    }

    public static InstallSnapshotResponse fromMap(Map<String, Object> payload) {
        return new InstallSnapshotResponse(
            ((Number) payload.get("term")).longValue(),
            (Boolean) payload.get("success"),
            ((Number) payload.get("lastIncludedIndex")).longValue()
        );
    }
}

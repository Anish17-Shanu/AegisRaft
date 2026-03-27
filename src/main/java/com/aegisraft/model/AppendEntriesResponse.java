package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex, long conflictTerm) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("term", term);
        payload.put("success", success);
        payload.put("matchIndex", matchIndex);
        payload.put("conflictIndex", conflictIndex);
        payload.put("conflictTerm", conflictTerm);
        return payload;
    }

    public static AppendEntriesResponse fromMap(Map<String, Object> payload) {
        return new AppendEntriesResponse(
            ((Number) payload.get("term")).longValue(),
            (Boolean) payload.get("success"),
            ((Number) payload.get("matchIndex")).longValue(),
            ((Number) payload.get("conflictIndex")).longValue(),
            ((Number) payload.get("conflictTerm")).longValue()
        );
    }
}

package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record ClientPutResponse(boolean accepted, boolean committed, String leaderId, String message, long logIndex) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("accepted", accepted);
        payload.put("committed", committed);
        payload.put("leaderId", leaderId);
        payload.put("message", message);
        payload.put("logIndex", logIndex);
        return payload;
    }

    public static ClientPutResponse fromMap(Map<String, Object> payload) {
        return new ClientPutResponse(
            Boolean.TRUE.equals(payload.get("accepted")),
            Boolean.TRUE.equals(payload.get("committed")),
            payload.get("leaderId") == null ? null : String.valueOf(payload.get("leaderId")),
            String.valueOf(payload.get("message")),
            ((Number) payload.get("logIndex")).longValue()
        );
    }
}

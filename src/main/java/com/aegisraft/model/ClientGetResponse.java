package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record ClientGetResponse(String key, String value, boolean found, String leaderId) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("key", key);
        payload.put("value", value);
        payload.put("found", found);
        payload.put("leaderId", leaderId);
        return payload;
    }

    public static ClientGetResponse fromMap(Map<String, Object> payload) {
        return new ClientGetResponse(
            String.valueOf(payload.get("key")),
            payload.get("value") == null ? null : String.valueOf(payload.get("value")),
            Boolean.TRUE.equals(payload.get("found")),
            payload.get("leaderId") == null ? null : String.valueOf(payload.get("leaderId"))
        );
    }
}

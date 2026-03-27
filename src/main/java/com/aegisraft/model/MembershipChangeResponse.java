package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record MembershipChangeResponse(boolean accepted, boolean committed, String leaderId, String message) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("accepted", accepted);
        payload.put("committed", committed);
        payload.put("leaderId", leaderId);
        payload.put("message", message);
        return payload;
    }

    public static MembershipChangeResponse fromMap(Map<String, Object> payload) {
        return new MembershipChangeResponse(
            (Boolean) payload.get("accepted"),
            (Boolean) payload.get("committed"),
            payload.get("leaderId") == null ? null : String.valueOf(payload.get("leaderId")),
            String.valueOf(payload.get("message"))
        );
    }
}

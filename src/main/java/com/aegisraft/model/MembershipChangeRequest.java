package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record MembershipChangeRequest(String action, String nodeId, String baseUrl) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("action", action);
        payload.put("nodeId", nodeId);
        payload.put("baseUrl", baseUrl);
        return payload;
    }

    public static MembershipChangeRequest fromMap(Map<String, Object> payload) {
        return new MembershipChangeRequest(
            String.valueOf(payload.get("action")),
            String.valueOf(payload.get("nodeId")),
            payload.get("baseUrl") == null ? null : String.valueOf(payload.get("baseUrl"))
        );
    }
}

package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record RequestVoteResponse(long term, boolean voteGranted) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("term", term);
        payload.put("voteGranted", voteGranted);
        return payload;
    }

    public static RequestVoteResponse fromMap(Map<String, Object> payload) {
        return new RequestVoteResponse(
            ((Number) payload.get("term")).longValue(),
            (Boolean) payload.get("voteGranted")
        );
    }
}

package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record RequestVoteRequest(String candidateId, long term, long lastLogIndex, long lastLogTerm) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("candidateId", candidateId);
        payload.put("term", term);
        payload.put("lastLogIndex", lastLogIndex);
        payload.put("lastLogTerm", lastLogTerm);
        return payload;
    }

    public static RequestVoteRequest fromMap(Map<String, Object> payload) {
        return new RequestVoteRequest(
            (String) payload.get("candidateId"),
            ((Number) payload.get("term")).longValue(),
            ((Number) payload.get("lastLogIndex")).longValue(),
            ((Number) payload.get("lastLogTerm")).longValue()
        );
    }
}

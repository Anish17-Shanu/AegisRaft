package com.aegisraft.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record AppendEntriesRequest(
    String leaderId,
    long term,
    long prevLogIndex,
    long prevLogTerm,
    long leaderCommit,
    List<LogEntry> entries
) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("leaderId", leaderId);
        payload.put("term", term);
        payload.put("prevLogIndex", prevLogIndex);
        payload.put("prevLogTerm", prevLogTerm);
        payload.put("leaderCommit", leaderCommit);
        List<Map<String, Object>> entryPayloads = new ArrayList<>();
        for (LogEntry entry : entries) {
            entryPayloads.add(entry.toMap());
        }
        payload.put("entries", entryPayloads);
        return payload;
    }

    @SuppressWarnings("unchecked")
    public static AppendEntriesRequest fromMap(Map<String, Object> payload) {
        List<LogEntry> entries = new ArrayList<>();
        for (Map<String, Object> entryPayload : (List<Map<String, Object>>) payload.get("entries")) {
            entries.add(LogEntry.fromMap(entryPayload));
        }
        return new AppendEntriesRequest(
            (String) payload.get("leaderId"),
            ((Number) payload.get("term")).longValue(),
            ((Number) payload.get("prevLogIndex")).longValue(),
            ((Number) payload.get("prevLogTerm")).longValue(),
            ((Number) payload.get("leaderCommit")).longValue(),
            entries
        );
    }
}

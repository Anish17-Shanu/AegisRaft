package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record LogEntry(
    long index,
    long term,
    String operation,
    String key,
    String value,
    ClusterMembership membership,
    long timestampEpochMillis
) {
    public static final String OPERATION_PUT = "put";
    public static final String OPERATION_CONFIG_JOINT = "config_joint";
    public static final String OPERATION_CONFIG_FINAL = "config_final";

    public static LogEntry put(long index, long term, String key, String value, long timestampEpochMillis) {
        return new LogEntry(index, term, OPERATION_PUT, key, value, null, timestampEpochMillis);
    }

    public static LogEntry jointConfiguration(long index, long term, ClusterMembership membership, long timestampEpochMillis) {
        return new LogEntry(index, term, OPERATION_CONFIG_JOINT, null, null, membership, timestampEpochMillis);
    }

    public static LogEntry finalConfiguration(long index, long term, ClusterMembership membership, long timestampEpochMillis) {
        return new LogEntry(index, term, OPERATION_CONFIG_FINAL, null, null, membership, timestampEpochMillis);
    }

    public boolean isPut() {
        return OPERATION_PUT.equals(operation);
    }

    public boolean isConfigurationEntry() {
        return OPERATION_CONFIG_JOINT.equals(operation) || OPERATION_CONFIG_FINAL.equals(operation);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("index", index);
        payload.put("term", term);
        payload.put("operation", operation);
        payload.put("key", key);
        payload.put("value", value);
        payload.put("membership", membership == null ? null : membership.toMap());
        payload.put("timestampEpochMillis", timestampEpochMillis);
        return payload;
    }

    @SuppressWarnings("unchecked")
    public static LogEntry fromMap(Map<String, Object> payload) {
        Object membershipPayload = payload.get("membership");
        String operation = payload.get("operation") == null ? OPERATION_PUT : String.valueOf(payload.get("operation"));
        return new LogEntry(
            ((Number) payload.get("index")).longValue(),
            ((Number) payload.get("term")).longValue(),
            operation,
            (String) payload.get("key"),
            (String) payload.get("value"),
            membershipPayload instanceof Map<?, ?> membershipMap ? ClusterMembership.fromMap((Map<String, Object>) membershipMap) : null,
            ((Number) payload.get("timestampEpochMillis")).longValue()
        );
    }
}

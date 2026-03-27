package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.Map;

public record ClientPutRequest(String key, String value) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("key", key);
        payload.put("value", value);
        return payload;
    }

    public static ClientPutRequest fromMap(Map<String, Object> payload) {
        return new ClientPutRequest((String) payload.get("key"), (String) payload.get("value"));
    }
}

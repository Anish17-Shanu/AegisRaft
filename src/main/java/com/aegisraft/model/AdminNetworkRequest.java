package com.aegisraft.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record AdminNetworkRequest(List<String> blockedPeerIds, long artificialDelayMillis, boolean dropAllOutgoing) {

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("blockedPeerIds", blockedPeerIds);
        payload.put("artificialDelayMillis", artificialDelayMillis);
        payload.put("dropAllOutgoing", dropAllOutgoing);
        return payload;
    }

    @SuppressWarnings("unchecked")
    public static AdminNetworkRequest fromMap(Map<String, Object> payload) {
        List<String> blockedPeerIds = new ArrayList<>();
        Object peers = payload.get("blockedPeerIds");
        if (peers instanceof List<?> peerList) {
            for (Object peer : peerList) {
                blockedPeerIds.add(String.valueOf(peer));
            }
        }
        return new AdminNetworkRequest(
            blockedPeerIds,
            ((Number) payload.getOrDefault("artificialDelayMillis", 0)).longValue(),
            Boolean.TRUE.equals(payload.get("dropAllOutgoing"))
        );
    }
}

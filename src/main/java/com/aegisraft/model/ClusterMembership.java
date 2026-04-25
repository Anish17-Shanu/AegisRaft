package com.aegisraft.model;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record ClusterMembership(Map<String, String> voters, Map<String, String> nextVoters) {
    public ClusterMembership {
        voters = normalize(voters);
        nextVoters = normalize(nextVoters);
    }

    public static ClusterMembership stable(Map<String, String> voters) {
        return new ClusterMembership(voters, Map.of());
    }

    public static ClusterMembership joint(Map<String, String> voters, Map<String, String> nextVoters) {
        return new ClusterMembership(voters, nextVoters);
    }

    public boolean jointConsensus() {
        return !nextVoters.isEmpty();
    }

    public boolean containsNode(String nodeId) {
        return voters.containsKey(nodeId) || nextVoters.containsKey(nodeId);
    }

    public Map<String, String> finalVoters() {
        return jointConsensus() ? nextVoters : voters;
    }

    public Set<String> allNodeIds() {
        LinkedHashSet<String> ids = new LinkedHashSet<>(voters.keySet());
        ids.addAll(nextVoters.keySet());
        return Set.copyOf(ids);
    }

    public List<PeerAddress> peerAddresses(String localNodeId) {
        LinkedHashMap<String, String> addresses = new LinkedHashMap<>(voters);
        addresses.putAll(nextVoters);
        addresses.remove(localNodeId);
        return addresses.entrySet().stream()
            .map(entry -> new PeerAddress(entry.getKey(), entry.getValue()))
            .toList();
    }

    public boolean hasQuorum(Set<String> acknowledgements) {
        return hasMajority(voters, acknowledgements)
            && (!jointConsensus() || hasMajority(nextVoters, acknowledgements));
    }

    public Map<String, Object> toMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("voters", new LinkedHashMap<>(voters));
        payload.put("nextVoters", new LinkedHashMap<>(nextVoters));
        return payload;
    }

    public static ClusterMembership fromMap(Map<String, Object> payload) {
        Object votersValue = payload.get("voters");
        Object nextVotersValue = payload.get("nextVoters");
        return new ClusterMembership(
            votersValue instanceof Map<?, ?> votersMap ? castStringMap((Map<?, ?>) votersMap) : Map.of(),
            nextVotersValue instanceof Map<?, ?> nextVotersMap ? castStringMap((Map<?, ?>) nextVotersMap) : Map.of()
        );
    }

    private static boolean hasMajority(Map<String, String> voters, Set<String> acknowledgements) {
        int granted = 0;
        for (String nodeId : voters.keySet()) {
            if (acknowledgements.contains(nodeId)) {
                granted++;
            }
        }
        return granted >= quorumSize(voters.size());
    }

    private static int quorumSize(int members) {
        return (members / 2) + 1;
    }

    private static Map<String, String> normalize(Map<String, String> source) {
        LinkedHashMap<String, String> normalized = new LinkedHashMap<>();
        if (source != null) {
            source.forEach((key, value) -> {
                if (key != null && !key.isBlank() && value != null && !value.isBlank()) {
                    normalized.put(key, value);
                }
            });
        }
        return Map.copyOf(normalized);
    }

    private static Map<String, String> castStringMap(Map<?, ?> source) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        source.forEach((key, value) -> values.put(String.valueOf(key), String.valueOf(value)));
        return values;
    }
}

package com.aegisraft.config;

import com.aegisraft.model.PeerAddress;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class ClusterConfigLoader {
    private ClusterConfigLoader() {
    }

    public static NodeConfig load(Path configPath) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(configPath)) {
            properties.load(inputStream);
        }
        String nodeId = require(properties, "node.id");
        String host = properties.getProperty("server.host", "0.0.0.0");
        int port = Integer.parseInt(require(properties, "server.port"));
        Path dataDir = Path.of(properties.getProperty("data.dir", "data/" + nodeId)).toAbsolutePath();

        List<PeerAddress> peers = new ArrayList<>();
        String peersProperty = properties.getProperty("cluster.peers", "");
        if (!peersProperty.isBlank()) {
            for (String peerEntry : peersProperty.split(",")) {
                String trimmed = peerEntry.trim();
                if (trimmed.isBlank()) {
                    continue;
                }
                String[] parts = trimmed.split("=", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid peer entry: " + trimmed);
                }
                peers.add(new PeerAddress(parts[0].trim(), parts[1].trim()));
            }
        }
        peers.removeIf(peer -> peer.id().equals(nodeId));

        return new NodeConfig(
            nodeId,
            host,
            port,
            dataDir,
            List.copyOf(peers),
            Long.parseLong(properties.getProperty("raft.election.timeout.min.ms", "900")),
            Long.parseLong(properties.getProperty("raft.election.timeout.max.ms", "1600")),
            Long.parseLong(properties.getProperty("raft.heartbeat.interval.ms", "250")),
            Long.parseLong(properties.getProperty("raft.client.commit.timeout.ms", "5000")),
            Integer.parseInt(properties.getProperty("raft.replication.batch.size", "128")),
            Integer.parseInt(properties.getProperty("raft.snapshot.threshold.entries", "64"))
        );
    }

    private static String require(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing config key: " + key);
        }
        return value.trim();
    }
}

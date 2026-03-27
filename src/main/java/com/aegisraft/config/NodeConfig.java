package com.aegisraft.config;

import com.aegisraft.model.PeerAddress;
import java.nio.file.Path;
import java.util.List;

public record NodeConfig(
    String nodeId,
    String host,
    int port,
    Path dataDirectory,
    List<PeerAddress> peers,
    long electionTimeoutMinMillis,
    long electionTimeoutMaxMillis,
    long heartbeatIntervalMillis,
    long clientCommitTimeoutMillis,
    int replicationBatchSize,
    int snapshotThresholdEntries
) {

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}

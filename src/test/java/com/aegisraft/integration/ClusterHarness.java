package com.aegisraft.integration;

import com.aegisraft.api.HttpApiServer;
import com.aegisraft.config.NodeConfig;
import com.aegisraft.core.RaftNode;
import com.aegisraft.model.AdminNetworkRequest;
import com.aegisraft.model.MembershipChangeRequest;
import com.aegisraft.model.MembershipChangeResponse;
import com.aegisraft.model.PeerAddress;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

final class ClusterHarness implements AutoCloseable {
    private final List<NodeRuntime> nodes = new ArrayList<>();
    private final int initiallyStartedNodes;

    ClusterHarness(Path rootDirectory, int clusterSize, int basePort) throws IOException {
        this(rootDirectory, clusterSize, clusterSize, basePort);
    }

    ClusterHarness(Path rootDirectory, int clusterSize, int initiallyStartedNodes, int basePort) throws IOException {
        this.initiallyStartedNodes = initiallyStartedNodes;
        List<PeerAddress> addresses = new ArrayList<>();
        for (int index = 1; index <= clusterSize; index++) {
            addresses.add(new PeerAddress("node-" + index, "http://127.0.0.1:" + (basePort + index)));
        }
        for (int index = 1; index <= clusterSize; index++) {
            String nodeId = "node-" + index;
            List<PeerAddress> peers;
            if (index <= initiallyStartedNodes) {
                peers = addresses.stream()
                    .limit(initiallyStartedNodes)
                    .filter(peer -> !peer.id().equals(nodeId))
                    .toList();
            } else {
                peers = addresses.stream()
                    .limit(initiallyStartedNodes)
                    .toList();
            }
            NodeConfig config = new NodeConfig(
                nodeId,
                "127.0.0.1",
                basePort + index,
                rootDirectory.resolve(nodeId),
                peers,
                700,
                1100,
                175,
                15000,
                128,
                10
            );
            nodes.add(new NodeRuntime(config));
        }
    }

    void startAll() throws IOException {
        for (int index = 0; index < initiallyStartedNodes; index++) {
            nodes.get(index).start();
        }
    }

    void startNode(String nodeId) throws IOException {
        node(nodeId).start();
    }

    void startRemaining() throws IOException {
        for (NodeRuntime node : nodes) {
            node.start();
        }
    }

    NodeRuntime waitForLeader(Duration timeout) throws Exception {
        return waitForLeaderExcluding(null, timeout);
    }

    NodeRuntime waitForLeaderExcluding(String excludedNodeId, Duration timeout) throws Exception {
        return waitFor(timeout, () -> nodes.stream()
            .filter(NodeRuntime::running)
            .map(NodeRuntime::state)
            .filter(state -> "LEADER".equals(state.role()))
            .filter(state -> excludedNodeId == null || !state.nodeId().equals(excludedNodeId))
            .findFirst()
            .flatMap(state -> findNode(state.nodeId()))
            .orElse(null), "leader election");
    }

    void waitForValueOnAll(String key, String expectedValue, Duration timeout) throws Exception {
        waitFor(timeout, () -> {
            for (NodeRuntime node : nodes) {
                if (!node.running()) {
                    continue;
                }
                String value = node.getValue(key);
                if (!Objects.equals(expectedValue, value)) {
                    return false;
                }
            }
            return true;
        }, "value replication for " + key);
    }

    void healNetwork() throws Exception {
        for (NodeRuntime node : nodes) {
            if (node.running()) {
                node.applyNetwork(new AdminNetworkRequest(List.of(), 0, false));
            }
        }
    }

    NodeRuntime node(String nodeId) {
        return findNode(nodeId).orElseThrow();
    }

    List<NodeRuntime> runningNodes() {
        return nodes.stream().filter(NodeRuntime::running).toList();
    }

    @Override
    public void close() {
        List<NodeRuntime> reversed = new ArrayList<>(nodes);
        reversed.sort(Comparator.comparing(runtime -> runtime.config().nodeId()));
        for (NodeRuntime node : reversed) {
            node.close();
        }
    }

    private Optional<NodeRuntime> findNode(String nodeId) {
        return nodes.stream().filter(node -> node.config().nodeId().equals(nodeId)).findFirst();
    }

    private <T> T waitFor(Duration timeout, Supplier<T> supplier, String description) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            T value = supplier.get();
            if (value instanceof Boolean flag) {
                if (flag) {
                    return value;
                }
            } else if (value != null) {
                return value;
            }
            Thread.sleep(150);
        }
        throw new IllegalStateException("Timed out waiting for " + description);
    }

    static final class NodeRuntime implements AutoCloseable {
        private final NodeConfig config;
        private RaftNode node;
        private HttpApiServer server;
        private boolean running;

        NodeRuntime(NodeConfig config) {
            this.config = config;
        }

        NodeConfig config() {
            return config;
        }

        boolean running() {
            return running;
        }

        void start() throws IOException {
            if (running) {
                return;
            }
            Files.createDirectories(config.dataDirectory());
            node = new RaftNode(config);
            server = new HttpApiServer(config.host(), config.port(), node);
            node.start();
            server.start();
            running = true;
        }

        void stop() {
            if (!running) {
                return;
            }
            server.stop();
            node.close();
            running = false;
        }

        void restart() throws IOException {
            stop();
            start();
        }

        com.aegisraft.model.ClusterStateResponse state() {
            return node.clusterState();
        }

        com.aegisraft.model.ClientPutResponse put(String key, String value) {
            return node.handleClientPut(new com.aegisraft.model.ClientPutRequest(key, value));
        }

        String getValue(String key) {
            com.aegisraft.model.ClientGetResponse response = node.handleClientGet(key);
            return response.found() ? response.value() : null;
        }

        void applyNetwork(AdminNetworkRequest request) {
            node.applyNetwork(request);
        }

        MembershipChangeResponse membershipChange(String action, String nodeId, String baseUrl) {
            return node.handleMembershipChange(new MembershipChangeRequest(action, nodeId, baseUrl));
        }

        @Override
        public void close() {
            stop();
        }
    }
}

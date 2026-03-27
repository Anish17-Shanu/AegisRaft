package com.aegisraft.integration;

import com.aegisraft.model.ClientPutResponse;
import com.aegisraft.model.MembershipChangeResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

public final class AegisRaftIntegrationSuite {
    private AegisRaftIntegrationSuite() {
    }

    public static void main(String[] args) throws Exception {
        Path tempDirectory = Files.createTempDirectory("aegisraft-it");
        try (ClusterHarness cluster = new ClusterHarness(tempDirectory, 4, 3, 9500)) {
            cluster.startAll();

            ClusterHarness.NodeRuntime leader = cluster.waitForLeader(Duration.ofSeconds(10));
            ClientPutResponse alpha = leader.put("alpha", "1");
            assertCommitted(alpha, "alpha");
            cluster.waitForValueOnAll("alpha", "1", Duration.ofSeconds(10));

            for (int index = 0; index < 15; index++) {
                ClientPutResponse response = cluster.waitForLeader(Duration.ofSeconds(10)).put("bulk-" + index, "value-" + index);
                assertCommitted(response, "bulk-" + index);
            }
            cluster.waitForValueOnAll("bulk-14", "value-14", Duration.ofSeconds(12));
            long walLinesAfterCompaction = countNonBlankLines(tempDirectory.resolve("node-2").resolve("wal.log"));
            assert walLinesAfterCompaction <= 10 : "Expected compacted WAL on node-2 but found " + walLinesAfterCompaction + " lines";

            cluster.startNode("node-4");
            ClusterHarness.NodeRuntime membershipLeader = cluster.waitForLeader(Duration.ofSeconds(10));
            MembershipChangeResponse addNode = membershipLeader.membershipChange("add", "node-4", cluster.node("node-4").config().baseUrl());
            assert addNode.accepted() : "Membership add was not accepted";
            assert addNode.committed() : "Membership add was not committed";

            cluster.waitForValueOnAll("bulk-14", "value-14", Duration.ofSeconds(12));
            ClientPutResponse epsilon = cluster.waitForLeader(Duration.ofSeconds(10)).put("epsilon", "5");
            assertCommitted(epsilon, "epsilon");
            cluster.waitForValueOnAll("epsilon", "5", Duration.ofSeconds(12));

            MembershipChangeResponse removeNode = cluster.waitForLeader(Duration.ofSeconds(10)).membershipChange("remove", "node-4", null);
            assert removeNode.accepted() : "Membership remove was not accepted";
            assert removeNode.committed() : "Membership remove was not committed";
        }
    }

    private static void assertCommitted(ClientPutResponse response, String key) {
        assert response.accepted() : "Request for " + key + " was not accepted";
        assert response.committed() : "Request for " + key + " did not commit";
    }

    private static long countNonBlankLines(Path path) throws Exception {
        if (!Files.exists(path)) {
            return 0;
        }
        return Files.readAllLines(path).stream().filter(line -> !line.isBlank()).count();
    }
}

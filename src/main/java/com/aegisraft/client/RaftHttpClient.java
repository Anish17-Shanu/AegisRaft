package com.aegisraft.client;

import com.aegisraft.codec.JsonCodec;
import com.aegisraft.core.FaultController;
import com.aegisraft.metrics.ClusterMetrics;
import com.aegisraft.model.AdminNetworkRequest;
import com.aegisraft.model.AppendEntriesRequest;
import com.aegisraft.model.AppendEntriesResponse;
import com.aegisraft.model.ClientGetResponse;
import com.aegisraft.model.ClientPutRequest;
import com.aegisraft.model.ClientPutResponse;
import com.aegisraft.model.ClusterStateResponse;
import com.aegisraft.model.InstallSnapshotRequest;
import com.aegisraft.model.InstallSnapshotResponse;
import com.aegisraft.model.MembershipChangeRequest;
import com.aegisraft.model.MembershipChangeResponse;
import com.aegisraft.model.RequestVoteRequest;
import com.aegisraft.model.RequestVoteResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public final class RaftHttpClient {
    private final String localNodeId;
    private final HttpClient httpClient;
    private final FaultController faultController;
    private final ClusterMetrics metrics;

    public RaftHttpClient(String localNodeId, FaultController faultController, ClusterMetrics metrics) {
        this.localNodeId = localNodeId;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        this.faultController = faultController;
        this.metrics = metrics;
    }

    public Optional<RequestVoteResponse> requestVote(String peerId, String baseUrl, RequestVoteRequest request) {
        return post(peerId, baseUrl + "/raft/request-vote", request.toMap()).map(RequestVoteResponse::fromMap);
    }

    public Optional<AppendEntriesResponse> appendEntries(String peerId, String baseUrl, AppendEntriesRequest request) {
        return post(peerId, baseUrl + "/raft/append-entries", request.toMap()).map(AppendEntriesResponse::fromMap);
    }

    public Optional<InstallSnapshotResponse> installSnapshot(String peerId, String baseUrl, InstallSnapshotRequest request) {
        return post(peerId, baseUrl + "/raft/install-snapshot", request.toMap()).map(InstallSnapshotResponse::fromMap);
    }

    public ClientPutResponse put(String baseUrl, ClientPutRequest request) throws IOException, InterruptedException {
        return ClientPutResponse.fromMap(postOrThrow(baseUrl + "/kv/put", request.toMap()));
    }

    public ClientGetResponse get(String baseUrl, String key) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/kv/get?key=" + key))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build();
        HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        return ClientGetResponse.fromMap(JsonCodec.parseObject(response.body()));
    }

    public ClusterStateResponse clusterState(String baseUrl) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/admin/state"))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        Map<String, Object> payload = JsonCodec.parseObject(response.body());
        return new ClusterStateResponse(
            String.valueOf(payload.get("nodeId")),
            payload.get("leaderId") == null ? null : String.valueOf(payload.get("leaderId")),
            String.valueOf(payload.get("role")),
            ((Number) payload.get("currentTerm")).longValue(),
            ((Number) payload.get("commitIndex")).longValue(),
            ((Number) payload.get("lastApplied")).longValue(),
            ((Number) payload.get("lastLogIndex")).longValue(),
            ((Number) payload.get("lastLogTerm")).longValue(),
            ((Number) payload.get("peerCount")).intValue()
        );
    }

    public void applyNetwork(String baseUrl, AdminNetworkRequest request) throws IOException, InterruptedException {
        postOrThrow(baseUrl + "/admin/network", request.toMap());
    }

    public MembershipChangeResponse membershipChange(String baseUrl, MembershipChangeRequest request) throws IOException, InterruptedException {
        return MembershipChangeResponse.fromMap(postOrThrow(baseUrl + "/admin/membership", request.toMap()));
    }

    public String metrics(String baseUrl) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/metrics"))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private Optional<Map<String, Object>> post(String peerId, String url, Map<String, Object> payload) {
        if (faultController.shouldDrop(peerId)) {
            metrics.recordRpcFailure();
            return Optional.empty();
        }
        maybeDelay();
        try {
            return Optional.of(postOrThrow(url, payload));
        } catch (IOException exception) {
            metrics.recordRpcFailure();
            return Optional.empty();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    private Map<String, Object> postOrThrow(String url, Map<String, Object> payload) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(4))
            .header("Content-Type", "application/json")
            .header("X-Raft-Source", localNodeId)
            .POST(HttpRequest.BodyPublishers.ofString(JsonCodec.toJson(payload)))
            .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IOException("RPC failed with status " + response.statusCode() + ": " + response.body());
        }
        return JsonCodec.parseObject(response.body());
    }

    private void maybeDelay() {
        long delay = faultController.artificialDelayMillis();
        if (delay <= 0) {
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}

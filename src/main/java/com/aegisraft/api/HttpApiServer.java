package com.aegisraft.api;

import com.aegisraft.codec.JsonCodec;
import com.aegisraft.core.RaftNode;
import com.aegisraft.model.AdminNetworkRequest;
import com.aegisraft.model.AppendEntriesRequest;
import com.aegisraft.model.ClientPutRequest;
import com.aegisraft.model.InstallSnapshotRequest;
import com.aegisraft.model.MembershipChangeRequest;
import com.aegisraft.model.RequestVoteRequest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public final class HttpApiServer {
    private final RaftNode node;
    private final HttpServer server;

    public HttpApiServer(String host, int port, RaftNode node) throws IOException {
        this.node = node;
        server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/raft/request-vote", exchange -> handleJson(exchange, body -> node.handleRequestVote(RequestVoteRequest.fromMap(body)).toMap()));
        server.createContext("/raft/append-entries", exchange -> handleJson(exchange, body -> node.handleAppendEntries(AppendEntriesRequest.fromMap(body)).toMap()));
        server.createContext("/raft/install-snapshot", exchange -> handleJson(exchange, body -> node.handleInstallSnapshot(InstallSnapshotRequest.fromMap(body)).toMap()));
        server.createContext("/kv/put", exchange -> handleJson(exchange, body -> node.handleClientPut(ClientPutRequest.fromMap(body)).toMap()));
        server.createContext("/kv/get", exchange -> {
            Map<String, String> query = parseQuery(exchange.getRequestURI());
            writeJson(exchange, 200, node.handleClientGet(query.getOrDefault("key", "")).toMap());
        });
        server.createContext("/admin/state", exchange -> writeJson(exchange, 200, node.clusterState().toMap()));
        server.createContext("/admin/network", exchange -> handleJson(exchange, body -> {
            node.applyNetwork(AdminNetworkRequest.fromMap(body));
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "ok");
            return response;
        }));
        server.createContext("/admin/membership", exchange -> handleJson(exchange, body -> node.handleMembershipChange(MembershipChangeRequest.fromMap(body)).toMap()));
        server.createContext("/metrics", exchange -> {
            byte[] response = node.metricsText().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(response);
            }
        });
        server.setExecutor(Executors.newCachedThreadPool());
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    private void handleJson(HttpExchange exchange, JsonHandler handler) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            writeJson(exchange, 405, Map.of("error", "method_not_allowed"));
            return;
        }
        String sourceNodeId = exchange.getRequestHeaders().getFirst("X-Raft-Source");
        if (nodeRejects(exchange, sourceNodeId)) {
            writeJson(exchange, 503, Map.of("error", "network_partition"));
            return;
        }
        try (InputStream inputStream = exchange.getRequestBody()) {
            String requestBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            Map<String, Object> response = handler.handle(JsonCodec.parseObject(requestBody));
            writeJson(exchange, 200, response);
        } catch (Exception exception) {
            writeJson(exchange, 500, Map.of("error", exception.getMessage()));
        }
    }

    private void writeJson(HttpExchange exchange, int statusCode, Map<String, Object> body) throws IOException {
        byte[] response = JsonCodec.toJson(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(response);
        }
    }

    private Map<String, String> parseQuery(URI uri) {
        Map<String, String> query = new LinkedHashMap<>();
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isBlank()) {
            return query;
        }
        for (String entry : rawQuery.split("&")) {
            String[] pair = entry.split("=", 2);
            query.put(pair[0], pair.length > 1 ? pair[1] : "");
        }
        return query;
    }

    private boolean nodeRejects(HttpExchange exchange, String sourceNodeId) {
        return node.shouldRejectIncoming(sourceNodeId);
    }

    @FunctionalInterface
    private interface JsonHandler {
        Map<String, Object> handle(Map<String, Object> payload) throws Exception;
    }
}

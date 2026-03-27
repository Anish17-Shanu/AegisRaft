package com.aegisraft.persistence;

import com.aegisraft.codec.JsonCodec;
import com.aegisraft.model.ClusterMembership;
import com.aegisraft.model.LogEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RaftStorage {
    private final Path metadataFile;
    private final Path walFile;

    public RaftStorage(Path dataDirectory) throws IOException {
        Files.createDirectories(dataDirectory);
        this.metadataFile = dataDirectory.resolve("metadata.json");
        this.walFile = dataDirectory.resolve("wal.log");
    }

    public synchronized StorageState load() throws IOException {
        long currentTerm = 0L;
        String votedFor = null;
        long snapshotIndex = 0L;
        long snapshotTerm = 0L;
        ClusterMembership snapshotMembership = null;
        if (Files.exists(metadataFile)) {
            Map<String, Object> metadata = JsonCodec.parseObject(Files.readString(metadataFile, StandardCharsets.UTF_8));
            currentTerm = ((Number) metadata.getOrDefault("currentTerm", 0L)).longValue();
            Object votedForValue = metadata.get("votedFor");
            if (votedForValue != null) {
                votedFor = String.valueOf(votedForValue);
            }
            snapshotIndex = ((Number) metadata.getOrDefault("snapshotIndex", 0L)).longValue();
            snapshotTerm = ((Number) metadata.getOrDefault("snapshotTerm", 0L)).longValue();
            Object membershipValue = metadata.get("snapshotMembership");
            if (membershipValue instanceof Map<?, ?> membershipMap) {
                @SuppressWarnings("unchecked")
                Map<String, Object> membershipPayload = (Map<String, Object>) membershipMap;
                snapshotMembership = ClusterMembership.fromMap(membershipPayload);
            }
        }

        List<LogEntry> logEntries = new ArrayList<>();
        if (Files.exists(walFile)) {
            for (String line : Files.readAllLines(walFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    logEntries.add(LogEntry.fromMap(JsonCodec.parseObject(line)));
                }
            }
        }
        return new StorageState(currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshotMembership, List.copyOf(logEntries));
    }

    public synchronized void persistMetadata(long currentTerm, String votedFor) throws IOException {
        StorageState existing = load();
        writeMetadata(currentTerm, votedFor, existing.snapshotIndex(), existing.snapshotTerm(), existing.snapshotMembership());
    }

    public synchronized void updateSnapshotMetadata(long currentTerm, String votedFor, long snapshotIndex, long snapshotTerm, ClusterMembership membership) throws IOException {
        writeMetadata(currentTerm, votedFor, snapshotIndex, snapshotTerm, membership);
    }

    public synchronized void compactLog(
        long currentTerm,
        String votedFor,
        long snapshotIndex,
        long snapshotTerm,
        ClusterMembership membership,
        List<LogEntry> retainedEntries
    ) throws IOException {
        writeMetadata(currentTerm, votedFor, snapshotIndex, snapshotTerm, membership);
        replaceLog(retainedEntries);
    }

    private void writeMetadata(
        long currentTerm,
        String votedFor,
        long snapshotIndex,
        long snapshotTerm,
        ClusterMembership membership
    ) throws IOException {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("currentTerm", currentTerm);
        metadata.put("votedFor", votedFor);
        metadata.put("snapshotIndex", snapshotIndex);
        metadata.put("snapshotTerm", snapshotTerm);
        metadata.put("snapshotMembership", membership == null ? null : membership.toMap());
        Files.writeString(metadataFile, JsonCodec.toJson(metadata), StandardCharsets.UTF_8);
    }

    public synchronized void append(LogEntry entry) throws IOException {
        Files.writeString(
            walFile,
            JsonCodec.toJson(entry.toMap()) + System.lineSeparator(),
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        );
    }

    public synchronized void replaceLog(List<LogEntry> entries) throws IOException {
        StringBuilder walContent = new StringBuilder();
        for (LogEntry entry : entries) {
            walContent.append(JsonCodec.toJson(entry.toMap())).append(System.lineSeparator());
        }
        Files.writeString(walFile, walContent.toString(), StandardCharsets.UTF_8);
    }
}

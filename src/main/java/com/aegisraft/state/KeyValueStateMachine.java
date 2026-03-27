package com.aegisraft.state;

import com.aegisraft.codec.JsonCodec;
import com.aegisraft.model.LogEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class KeyValueStateMachine {
    private final Path snapshotFile;
    private final ConcurrentHashMap<String, String> state = new ConcurrentHashMap<>();
    private volatile long lastAppliedIndex;

    public KeyValueStateMachine(Path dataDirectory) throws IOException {
        this.snapshotFile = dataDirectory.resolve("state-machine.json");
        Files.createDirectories(dataDirectory);
        load();
    }

    public synchronized void apply(LogEntry entry) throws IOException {
        if (entry.isPut()) {
            state.put(entry.key(), entry.value());
        }
        lastAppliedIndex = entry.index();
        persist();
    }

    public synchronized void installSnapshot(Map<String, String> snapshotState, long snapshotIndex) throws IOException {
        state.clear();
        state.putAll(snapshotState);
        lastAppliedIndex = snapshotIndex;
        persist();
    }

    public String get(String key) {
        return state.get(key);
    }

    public boolean containsKey(String key) {
        return state.containsKey(key);
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public Map<String, String> snapshotState() {
        return Map.copyOf(state);
    }

    private void load() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        Map<String, Object> snapshot = JsonCodec.parseObject(Files.readString(snapshotFile, StandardCharsets.UTF_8));
        lastAppliedIndex = ((Number) snapshot.getOrDefault("lastAppliedIndex", 0L)).longValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> values = (Map<String, Object>) snapshot.getOrDefault("state", Map.of());
        state.clear();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            state.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
    }

    private void persist() throws IOException {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("lastAppliedIndex", lastAppliedIndex);
        snapshot.put("state", new LinkedHashMap<>(state));
        Files.writeString(snapshotFile, JsonCodec.toJson(snapshot), StandardCharsets.UTF_8);
    }
}

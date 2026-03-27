package com.aegisraft.persistence;

import com.aegisraft.model.ClusterMembership;
import com.aegisraft.model.LogEntry;
import java.util.List;

public record StorageState(
    long currentTerm,
    String votedFor,
    long snapshotIndex,
    long snapshotTerm,
    ClusterMembership snapshotMembership,
    List<LogEntry> logEntries
) {
}

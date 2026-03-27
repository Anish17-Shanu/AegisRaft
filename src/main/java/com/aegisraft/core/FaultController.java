package com.aegisraft.core;

import com.aegisraft.model.AdminNetworkRequest;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class FaultController {
    private final Set<String> blockedPeers = ConcurrentHashMap.newKeySet();
    private volatile long artificialDelayMillis;
    private volatile boolean dropAllOutgoing;
    private volatile boolean dropAllIncoming;

    public void apply(AdminNetworkRequest request) {
        blockedPeers.clear();
        blockedPeers.addAll(request.blockedPeerIds());
        artificialDelayMillis = Math.max(0, request.artificialDelayMillis());
        dropAllOutgoing = request.dropAllOutgoing();
        dropAllIncoming = request.dropAllOutgoing();
    }

    public boolean shouldDrop(String peerId) {
        return dropAllOutgoing || blockedPeers.contains(peerId);
    }

    public boolean shouldDropIncoming(String peerId) {
        return dropAllIncoming || (peerId != null && blockedPeers.contains(peerId));
    }

    public long artificialDelayMillis() {
        return artificialDelayMillis;
    }
}

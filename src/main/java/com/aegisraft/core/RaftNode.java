package com.aegisraft.core;

import com.aegisraft.client.RaftHttpClient;
import com.aegisraft.config.NodeConfig;
import com.aegisraft.metrics.ClusterMetrics;
import com.aegisraft.model.AdminNetworkRequest;
import com.aegisraft.model.AppendEntriesRequest;
import com.aegisraft.model.AppendEntriesResponse;
import com.aegisraft.model.ClientGetResponse;
import com.aegisraft.model.ClientPutRequest;
import com.aegisraft.model.ClientPutResponse;
import com.aegisraft.model.ClusterMembership;
import com.aegisraft.model.ClusterStateResponse;
import com.aegisraft.model.InstallSnapshotRequest;
import com.aegisraft.model.InstallSnapshotResponse;
import com.aegisraft.model.LogEntry;
import com.aegisraft.model.MembershipChangeRequest;
import com.aegisraft.model.MembershipChangeResponse;
import com.aegisraft.model.NodeRole;
import com.aegisraft.model.PeerAddress;
import com.aegisraft.model.RequestVoteRequest;
import com.aegisraft.model.RequestVoteResponse;
import com.aegisraft.persistence.RaftStorage;
import com.aegisraft.persistence.StorageState;
import com.aegisraft.state.KeyValueStateMachine;
import com.aegisraft.util.Clock;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class RaftNode implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());

    private final NodeConfig config;
    private final Clock clock;
    private final Random random = new Random();
    private final ReentrantLock lock = new ReentrantLock();
    private final ClusterMetrics metrics = new ClusterMetrics();
    private final FaultController faultController = new FaultController();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final RaftStorage storage;
    private final KeyValueStateMachine stateMachine;
    private final RaftHttpClient httpClient;
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<Long, CompletableFuture<Void>> pendingCommits = new ConcurrentHashMap<>();
    private final List<LogEntry> logEntries = new ArrayList<>();

    private volatile NodeRole role = NodeRole.FOLLOWER;
    private volatile String leaderId;
    private volatile long electionDeadlineMillis;
    private volatile boolean running;

    private long currentTerm;
    private String votedFor;
    private long commitIndex;
    private long lastApplied;
    private long snapshotIndex;
    private long snapshotTerm;
    private ClusterMembership snapshotMembership;
    private ClusterMembership committedMembership;
    private ClusterMembership effectiveMembership;

    public RaftNode(NodeConfig config) throws IOException {
        this(config, Clock.system());
    }

    public RaftNode(NodeConfig config, Clock clock) throws IOException {
        this.config = config;
        this.clock = clock;
        this.storage = new RaftStorage(config.dataDirectory());
        this.stateMachine = new KeyValueStateMachine(config.dataDirectory());
        this.httpClient = new RaftHttpClient(config.nodeId(), faultController, metrics);
        restorePersistentState();
    }

    public void start() {
        running = true;
        resetElectionDeadline();
        scheduler.scheduleWithFixedDelay(this::runElectionLoop, 75, 75, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::runHeartbeatLoop, 50, config.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::runApplyLoop, 25, 25, TimeUnit.MILLISECONDS);
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        lock.lock();
        try {
            if (request.term() < currentTerm) {
                return new RequestVoteResponse(currentTerm, false);
            }
            if (request.term() > currentTerm) {
                transitionToFollower(request.term(), null);
            }
            if (!effectiveMembership.containsNode(request.candidateId())) {
                return new RequestVoteResponse(currentTerm, false);
            }

            boolean logUpToDate = isCandidateLogUpToDate(request.lastLogIndex(), request.lastLogTerm());
            boolean canVote = votedFor == null || Objects.equals(votedFor, request.candidateId());
            boolean grant = canVote && logUpToDate;
            if (grant) {
                votedFor = request.candidateId();
                persistMetadata();
                resetElectionDeadline();
                LOGGER.info(() -> config.nodeId() + " granted vote to " + request.candidateId() + " for term " + currentTerm);
            }
            return new RequestVoteResponse(currentTerm, grant);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to persist vote", exception);
        } finally {
            lock.unlock();
        }
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        lock.lock();
        try {
            if (request.term() < currentTerm) {
                return new AppendEntriesResponse(currentTerm, false, lastLogIndex(), lastLogIndex() + 1, -1);
            }
            if (request.term() > currentTerm || role != NodeRole.FOLLOWER) {
                transitionToFollower(request.term(), request.leaderId());
            } else {
                leaderId = request.leaderId();
            }
            resetElectionDeadline();

            if (request.prevLogIndex() < snapshotIndex) {
                return new AppendEntriesResponse(currentTerm, false, lastLogIndex(), snapshotIndex + 1, snapshotTerm);
            }
            if (request.prevLogIndex() > lastLogIndex()) {
                return new AppendEntriesResponse(currentTerm, false, lastLogIndex(), lastLogIndex() + 1, -1);
            }
            if (request.prevLogIndex() > 0 && logTermAt(request.prevLogIndex()) != request.prevLogTerm()) {
                long conflictingTerm = logTermAt(request.prevLogIndex());
                return new AppendEntriesResponse(currentTerm, false, lastLogIndex(), firstIndexOfTerm(conflictingTerm), conflictingTerm);
            }

            appendOrOverwriteEntries(request.entries());
            leaderId = request.leaderId();
            if (request.leaderCommit() > commitIndex) {
                commitIndex = Math.min(request.leaderCommit(), lastLogIndex());
            }
            return new AppendEntriesResponse(currentTerm, true, lastLogIndex(), 0, 0);
        } finally {
            lock.unlock();
        }
    }

    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        lock.lock();
        try {
            if (request.term() < currentTerm) {
                return new InstallSnapshotResponse(currentTerm, false, snapshotIndex);
            }
            if (request.term() > currentTerm || role != NodeRole.FOLLOWER) {
                transitionToFollower(request.term(), request.leaderId());
            } else {
                leaderId = request.leaderId();
            }
            resetElectionDeadline();

            installSnapshotLocally(request.lastIncludedIndex(), request.lastIncludedTerm(), request.membership(), request.state());
            commitIndex = Math.max(commitIndex, request.lastIncludedIndex());
            lastApplied = Math.max(lastApplied, request.lastIncludedIndex());
            leaderId = request.leaderId();
            return new InstallSnapshotResponse(currentTerm, true, snapshotIndex);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to install snapshot", exception);
        } finally {
            lock.unlock();
        }
    }

    public ClientPutResponse handleClientPut(ClientPutRequest request) {
        long startedAt = clock.now();
        CompletableFuture<Void> commitFuture;
        long newIndex;
        String currentLeader;

        lock.lock();
        try {
            currentLeader = leaderId;
            if (role != NodeRole.LEADER || !committedMembership.containsNode(config.nodeId())) {
                metrics.recordRejectedClientRequest();
                return new ClientPutResponse(false, false, currentLeader, "not_leader", -1);
            }

            newIndex = lastLogIndex() + 1;
            LogEntry entry = LogEntry.put(newIndex, currentTerm, request.key(), request.value(), clock.now());
            appendLocalEntry(entry);
            pendingCommits.put(newIndex, new CompletableFuture<>());
            commitFuture = pendingCommits.get(newIndex);
        } catch (IOException exception) {
            return new ClientPutResponse(false, false, leaderId, "persistence_failure:" + exception.getMessage(), -1);
        } finally {
            lock.unlock();
        }

        replicateOnceToAll();
        try {
            commitFuture.get(config.clientCommitTimeoutMillis(), TimeUnit.MILLISECONDS);
            metrics.recordCommitLatency(clock.now() - startedAt);
            return new ClientPutResponse(true, true, config.nodeId(), "committed", newIndex);
        } catch (TimeoutException exception) {
            pendingCommits.remove(newIndex);
            return new ClientPutResponse(true, false, config.nodeId(), "commit_timeout", newIndex);
        } catch (Exception exception) {
            pendingCommits.remove(newIndex);
            return new ClientPutResponse(true, false, config.nodeId(), "commit_failed", newIndex);
        }
    }

    public MembershipChangeResponse handleMembershipChange(MembershipChangeRequest request) {
        String action = request.action() == null ? "" : request.action().trim().toLowerCase();
        long startedAt = clock.now();
        try {
            lock.lock();
            try {
                if (role != NodeRole.LEADER) {
                    return new MembershipChangeResponse(false, false, leaderId, "not_leader");
                }
                if (effectiveMembership.jointConsensus()) {
                    return new MembershipChangeResponse(false, false, leaderId, "membership_change_in_progress");
                }
            } finally {
                lock.unlock();
            }

            return switch (action) {
                case "add" -> reconfigureMembership(addMember(request.nodeId(), request.baseUrl()), startedAt);
                case "remove" -> reconfigureMembership(removeMember(request.nodeId()), startedAt);
                default -> new MembershipChangeResponse(false, false, leaderId, "unsupported_action");
            };
        } catch (IllegalArgumentException exception) {
            return new MembershipChangeResponse(false, false, leaderId, exception.getMessage());
        } catch (IOException exception) {
            return new MembershipChangeResponse(false, false, leaderId, "persistence_failure:" + exception.getMessage());
        }
    }

    public ClientGetResponse handleClientGet(String key) {
        return new ClientGetResponse(key, stateMachine.get(key), stateMachine.containsKey(key), leaderId);
    }

    public void applyNetwork(AdminNetworkRequest request) {
        faultController.apply(request);
    }

    public boolean shouldRejectIncoming(String sourceNodeId) {
        return faultController.shouldDropIncoming(sourceNodeId);
    }

    public ClusterStateResponse clusterState() {
        lock.lock();
        try {
            return new ClusterStateResponse(
                config.nodeId(),
                leaderId,
                role.name(),
                currentTerm,
                commitIndex,
                lastApplied,
                lastLogIndex(),
                lastLogTerm(),
                Math.max(0, effectiveMembership.finalVoters().size() - 1)
            );
        } finally {
            lock.unlock();
        }
    }

    public String metricsText() {
        StringBuilder builder = new StringBuilder();
        metrics.snapshot().forEach((name, value) -> builder.append("aegisraft_").append(name).append(' ').append(value).append('\n'));
        return builder.toString();
    }

    private void restorePersistentState() throws IOException {
        StorageState state = storage.load();
        currentTerm = state.currentTerm();
        votedFor = state.votedFor();
        snapshotIndex = state.snapshotIndex();
        snapshotTerm = state.snapshotTerm();
        snapshotMembership = state.snapshotMembership() == null ? bootstrapMembership() : state.snapshotMembership();
        logEntries.clear();
        logEntries.addAll(state.logEntries());
        lastApplied = stateMachine.lastAppliedIndex();
        commitIndex = lastApplied;
        committedMembership = membershipAt(lastApplied);
        effectiveMembership = membershipAt(lastLogIndex());
        leaderId = null;
        syncPeerTracking();
    }

    private MembershipChangeResponse reconfigureMembership(Map<String, String> newVoters, long startedAt) throws IOException {
        ClusterMembership currentMembership;
        ClusterMembership jointMembership;
        ClusterMembership finalMembership;
        long jointIndex;

        lock.lock();
        try {
            currentMembership = effectiveMembership;
            if (Objects.equals(currentMembership.finalVoters(), newVoters)) {
                return new MembershipChangeResponse(true, true, config.nodeId(), "membership_unchanged");
            }
            jointMembership = ClusterMembership.joint(currentMembership.finalVoters(), newVoters);
            finalMembership = ClusterMembership.stable(newVoters);
            jointIndex = appendConfigEntry(LogEntry.jointConfiguration(lastLogIndex() + 1, currentTerm, jointMembership, clock.now()));
            pendingCommits.put(jointIndex, new CompletableFuture<>());
        } finally {
            lock.unlock();
        }

        replicateOnceToAll();
        if (!awaitCommit(jointIndex)) {
            return new MembershipChangeResponse(true, false, config.nodeId(), "joint_configuration_commit_timeout");
        }

        long finalIndex;
        lock.lock();
        try {
            finalIndex = appendConfigEntry(LogEntry.finalConfiguration(lastLogIndex() + 1, currentTerm, finalMembership, clock.now()));
            pendingCommits.put(finalIndex, new CompletableFuture<>());
        } finally {
            lock.unlock();
        }

        replicateOnceToAll();
        boolean committed = awaitCommit(finalIndex);
        if (committed) {
            metrics.recordCommitLatency(clock.now() - startedAt);
            return new MembershipChangeResponse(true, true, config.nodeId(), "committed");
        }
        return new MembershipChangeResponse(true, false, config.nodeId(), "final_configuration_commit_timeout");
    }

    private boolean awaitCommit(long index) {
        CompletableFuture<Void> future = pendingCommits.get(index);
        if (future == null) {
            return false;
        }
        try {
            future.get(config.clientCommitTimeoutMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception exception) {
            pendingCommits.remove(index);
            return false;
        }
    }

    private Map<String, String> addMember(String nodeId, String baseUrl) {
        if (nodeId == null || nodeId.isBlank() || baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("invalid_add_request");
        }
        LinkedHashMap<String, String> voters = new LinkedHashMap<>(effectiveMembership.finalVoters());
        voters.put(config.nodeId(), config.baseUrl());
        voters.put(nodeId, baseUrl);
        return Map.copyOf(voters);
    }

    private Map<String, String> removeMember(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("invalid_remove_request");
        }
        LinkedHashMap<String, String> voters = new LinkedHashMap<>(effectiveMembership.finalVoters());
        voters.remove(nodeId);
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("cannot_remove_last_voter");
        }
        return Map.copyOf(voters);
    }

    private void runElectionLoop() {
        if (!running || role == NodeRole.LEADER || !effectiveMembership.containsNode(config.nodeId())) {
            return;
        }
        if (clock.now() >= electionDeadlineMillis) {
            startElection();
        }
    }

    private void runHeartbeatLoop() {
        if (!running || role != NodeRole.LEADER) {
            return;
        }
        replicateOnceToAll();
    }

    private void runApplyLoop() {
        while (running) {
            LogEntry entryToApply = null;
            lock.lock();
            try {
                if (lastApplied < commitIndex) {
                    entryToApply = entryAt(lastApplied + 1);
                    lastApplied++;
                } else {
                    return;
                }
            } finally {
                lock.unlock();
            }

            if (entryToApply == null) {
                return;
            }

            try {
                stateMachine.apply(entryToApply);
                lock.lock();
                try {
                    if (entryToApply.isConfigurationEntry()) {
                        committedMembership = entryToApply.membership();
                        if (!committedMembership.containsNode(config.nodeId()) && role == NodeRole.LEADER) {
                            role = NodeRole.FOLLOWER;
                            leaderId = null;
                        }
                    }
                    maybeCompact();
                } finally {
                    lock.unlock();
                }
                CompletableFuture<Void> future = pendingCommits.remove(entryToApply.index());
                if (future != null) {
                    future.complete(null);
                }
            } catch (IOException exception) {
                LOGGER.log(Level.SEVERE, "Failed to apply entry " + entryToApply.index(), exception);
                return;
            }
        }
    }

    private void startElection() {
        long electionTerm;
        long localLastLogIndex;
        long localLastLogTerm;
        Set<String> grantedVotes = new HashSet<>();
        List<PeerAddress> peers;

        lock.lock();
        try {
            role = NodeRole.CANDIDATE;
            currentTerm++;
            votedFor = config.nodeId();
            leaderId = null;
            persistMetadata();
            resetElectionDeadline();
            electionTerm = currentTerm;
            localLastLogIndex = lastLogIndex();
            localLastLogTerm = lastLogTerm();
            grantedVotes.add(config.nodeId());
            peers = effectiveMembership.peerAddresses(config.nodeId());
            LOGGER.info(() -> config.nodeId() + " became CANDIDATE for term " + electionTerm);
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Unable to persist election metadata", exception);
            return;
        } finally {
            lock.unlock();
        }

        for (PeerAddress peer : peers) {
            Optional<RequestVoteResponse> response = httpClient.requestVote(
                peer.id(),
                peer.baseUrl(),
                new RequestVoteRequest(config.nodeId(), electionTerm, localLastLogIndex, localLastLogTerm)
            );
            if (response.isEmpty()) {
                continue;
            }

            RequestVoteResponse vote = response.get();
            lock.lock();
            try {
                if (vote.term() > currentTerm) {
                    transitionToFollower(vote.term(), null);
                    return;
                }
                if (role != NodeRole.CANDIDATE || currentTerm != electionTerm) {
                    return;
                }
                if (vote.voteGranted()) {
                    grantedVotes.add(peer.id());
                }
                if (effectiveMembership.hasQuorum(grantedVotes)) {
                    becomeLeader();
                    return;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void becomeLeader() {
        role = NodeRole.LEADER;
        leaderId = config.nodeId();
        metrics.recordLeaderChange();
        nextIndex.clear();
        matchIndex.clear();
        long next = lastLogIndex() + 1;
        for (PeerAddress peer : effectiveMembership.peerAddresses(config.nodeId())) {
            nextIndex.put(peer.id(), next);
            matchIndex.put(peer.id(), 0L);
        }
        LOGGER.info(() -> config.nodeId() + " became LEADER for term " + currentTerm);
        replicateOnceToAll();
    }

    private void replicateOnceToAll() {
        List<PeerAddress> peers;
        lock.lock();
        try {
            peers = effectiveMembership.peerAddresses(config.nodeId());
        } finally {
            lock.unlock();
        }
        for (PeerAddress peer : peers) {
            scheduler.execute(() -> replicateToPeer(peer));
        }
    }

    private void replicateToPeer(PeerAddress peer) {
        long peerNextIndex;
        long sentTerm;
        AppendEntriesRequest appendRequest = null;
        InstallSnapshotRequest snapshotRequest = null;

        lock.lock();
        try {
            if (role != NodeRole.LEADER) {
                return;
            }
            peerNextIndex = nextIndex.getOrDefault(peer.id(), lastLogIndex() + 1);
            sentTerm = currentTerm;
            if (peerNextIndex <= snapshotIndex) {
                snapshotRequest = new InstallSnapshotRequest(
                    config.nodeId(),
                    currentTerm,
                    snapshotIndex,
                    snapshotTerm,
                    commitIndex,
                    committedMembership,
                    stateMachine.snapshotState()
                );
            } else {
                long prevLogIndex = peerNextIndex - 1;
                long prevLogTerm = prevLogIndex == 0 ? 0 : logTermAt(prevLogIndex);
                List<LogEntry> entries = entriesFrom(peerNextIndex, config.replicationBatchSize());
                appendRequest = new AppendEntriesRequest(config.nodeId(), currentTerm, prevLogIndex, prevLogTerm, commitIndex, entries);
            }
        } finally {
            lock.unlock();
        }

        if (snapshotRequest != null) {
            Optional<InstallSnapshotResponse> responseOptional = httpClient.installSnapshot(peer.id(), peer.baseUrl(), snapshotRequest);
            if (responseOptional.isEmpty()) {
                return;
            }
            InstallSnapshotResponse response = responseOptional.get();
            lock.lock();
            try {
                if (response.term() > currentTerm) {
                    transitionToFollower(response.term(), null);
                    return;
                }
                if (role != NodeRole.LEADER || sentTerm != currentTerm || !response.success()) {
                    return;
                }
                nextIndex.put(peer.id(), response.lastIncludedIndex() + 1);
                matchIndex.put(peer.id(), response.lastIncludedIndex());
                advanceCommitIndex();
            } finally {
                lock.unlock();
            }
            return;
        }

        Optional<AppendEntriesResponse> responseOptional = httpClient.appendEntries(peer.id(), peer.baseUrl(), appendRequest);
        if (responseOptional.isEmpty()) {
            return;
        }

        AppendEntriesResponse response = responseOptional.get();
        lock.lock();
        try {
            if (response.term() > currentTerm) {
                transitionToFollower(response.term(), null);
                return;
            }
            if (role != NodeRole.LEADER || sentTerm != currentTerm) {
                return;
            }
            if (response.success()) {
                nextIndex.put(peer.id(), response.matchIndex() + 1);
                matchIndex.put(peer.id(), response.matchIndex());
                metrics.recordReplicationLag(lastLogIndex() - response.matchIndex());
                advanceCommitIndex();
            } else {
                long newNextIndex = response.conflictTerm() <= 0
                    ? response.conflictIndex()
                    : findLastIndexOfTerm(response.conflictTerm()).orElse(response.conflictIndex());
                nextIndex.put(peer.id(), Math.max(1, newNextIndex));
            }
        } finally {
            lock.unlock();
        }
    }

    private void advanceCommitIndex() {
        for (long candidateCommit = lastLogIndex(); candidateCommit > commitIndex; candidateCommit--) {
            if (logTermAt(candidateCommit) != currentTerm) {
                continue;
            }
            Set<String> acknowledgements = new HashSet<>();
            acknowledgements.add(config.nodeId());
            for (Map.Entry<String, Long> entry : matchIndex.entrySet()) {
                if (entry.getValue() >= candidateCommit) {
                    acknowledgements.add(entry.getKey());
                }
            }
            if (membershipAt(candidateCommit).hasQuorum(acknowledgements)) {
                commitIndex = candidateCommit;
                return;
            }
        }
    }

    private void appendOrOverwriteEntries(List<LogEntry> incomingEntries) {
        if (incomingEntries.isEmpty()) {
            return;
        }
        boolean modified = false;
        for (LogEntry incoming : incomingEntries) {
            int listIndex = (int) (incoming.index() - snapshotIndex - 1);
            if (listIndex < 0) {
                continue;
            }
            if (listIndex < logEntries.size()) {
                LogEntry existing = logEntries.get(listIndex);
                if (existing.term() != incoming.term()) {
                    while (logEntries.size() > listIndex) {
                        logEntries.remove(logEntries.size() - 1);
                    }
                    logEntries.add(incoming);
                    modified = true;
                }
            } else {
                logEntries.add(incoming);
                modified = true;
            }
        }
        if (modified) {
            refreshEffectiveMembership();
            try {
                storage.replaceLog(logEntries);
            } catch (IOException exception) {
                throw new IllegalStateException("Failed to persist WAL", exception);
            }
        }
    }

    private void transitionToFollower(long newTerm, String newLeaderId) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
            try {
                persistMetadata();
            } catch (IOException exception) {
                throw new IllegalStateException("Failed to persist follower transition", exception);
            }
        }
        role = NodeRole.FOLLOWER;
        leaderId = newLeaderId;
        resetElectionDeadline();
        LOGGER.info(() -> config.nodeId() + " transitioned to FOLLOWER for term " + currentTerm);
    }

    private void persistMetadata() throws IOException {
        storage.updateSnapshotMetadata(currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshotMembership);
    }

    private boolean isCandidateLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long localLastTerm = lastLogTerm();
        if (candidateLastTerm != localLastTerm) {
            return candidateLastTerm > localLastTerm;
        }
        return candidateLastIndex >= lastLogIndex();
    }

    private List<LogEntry> entriesFrom(long startIndexInclusive, int limit) {
        if (startIndexInclusive > lastLogIndex()) {
            return List.of();
        }
        int from = (int) Math.max(0, startIndexInclusive - snapshotIndex - 1);
        int to = Math.min(logEntries.size(), from + limit);
        return List.copyOf(logEntries.subList(from, to));
    }

    private long lastLogIndex() {
        return snapshotIndex + logEntries.size();
    }

    private long lastLogTerm() {
        if (!logEntries.isEmpty()) {
            return logEntries.get(logEntries.size() - 1).term();
        }
        return snapshotIndex == 0 ? 0 : snapshotTerm;
    }

    private long logTermAt(long index) {
        if (index == 0) {
            return 0;
        }
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        if (index < snapshotIndex) {
            throw new IllegalArgumentException("Index " + index + " compacted at snapshot " + snapshotIndex);
        }
        return logEntries.get((int) (index - snapshotIndex - 1)).term();
    }

    private LogEntry entryAt(long index) {
        if (index <= snapshotIndex) {
            throw new IllegalArgumentException("No WAL entry available at compacted index " + index);
        }
        return logEntries.get((int) (index - snapshotIndex - 1));
    }

    private long firstIndexOfTerm(long term) {
        if (snapshotIndex > 0 && snapshotTerm == term) {
            return snapshotIndex;
        }
        for (LogEntry entry : logEntries) {
            if (entry.term() == term) {
                return entry.index();
            }
        }
        return snapshotIndex + 1;
    }

    private Optional<Long> findLastIndexOfTerm(long term) {
        for (int index = logEntries.size() - 1; index >= 0; index--) {
            if (logEntries.get(index).term() == term) {
                return Optional.of(logEntries.get(index).index() + 1);
            }
        }
        if (snapshotIndex > 0 && snapshotTerm == term) {
            return Optional.of(snapshotIndex + 1);
        }
        return Optional.empty();
    }

    private void resetElectionDeadline() {
        long min = config.electionTimeoutMinMillis();
        long max = config.electionTimeoutMaxMillis();
        electionDeadlineMillis = clock.now() + min + random.nextLong(max - min + 1);
    }

    private void maybeCompact() throws IOException {
        if (lastApplied - snapshotIndex < config.snapshotThresholdEntries()) {
            return;
        }
        long newSnapshotIndex = lastApplied;
        long newSnapshotTerm = logTermAt(lastApplied);
        ClusterMembership newSnapshotMembership = committedMembership;
        List<LogEntry> retainedEntries = logEntries.stream()
            .filter(entry -> entry.index() > newSnapshotIndex)
            .toList();
        snapshotTerm = newSnapshotTerm;
        snapshotIndex = newSnapshotIndex;
        snapshotMembership = newSnapshotMembership;
        logEntries.clear();
        logEntries.addAll(retainedEntries);
        storage.compactLog(currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshotMembership, logEntries);
        syncPeerTracking();
    }

    private void installSnapshotLocally(
        long newSnapshotIndex,
        long newSnapshotTerm,
        ClusterMembership newSnapshotMembership,
        Map<String, String> snapshotState
    ) throws IOException {
        stateMachine.installSnapshot(snapshotState, newSnapshotIndex);
        snapshotIndex = newSnapshotIndex;
        snapshotTerm = newSnapshotTerm;
        snapshotMembership = newSnapshotMembership;
        committedMembership = newSnapshotMembership;
        logEntries.removeIf(entry -> entry.index() <= newSnapshotIndex);
        storage.compactLog(currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshotMembership, logEntries);
        refreshEffectiveMembership();
    }

    private void appendLocalEntry(LogEntry entry) throws IOException {
        logEntries.add(entry);
        storage.append(entry);
    }

    private long appendConfigEntry(LogEntry entry) throws IOException {
        appendLocalEntry(entry);
        refreshEffectiveMembership();
        syncPeerTracking();
        return entry.index();
    }

    private void refreshEffectiveMembership() {
        effectiveMembership = membershipAt(lastLogIndex());
        syncPeerTracking();
    }

    private ClusterMembership membershipAt(long indexInclusive) {
        ClusterMembership membership = snapshotMembership == null ? bootstrapMembership() : snapshotMembership;
        for (LogEntry entry : logEntries) {
            if (entry.index() > indexInclusive) {
                break;
            }
            if (entry.isConfigurationEntry()) {
                membership = entry.membership();
            }
        }
        return membership;
    }

    private ClusterMembership bootstrapMembership() {
        LinkedHashMap<String, String> voters = new LinkedHashMap<>();
        voters.put(config.nodeId(), config.baseUrl());
        for (PeerAddress peer : config.peers()) {
            voters.put(peer.id(), peer.baseUrl());
        }
        return ClusterMembership.stable(voters);
    }

    private void syncPeerTracking() {
        Set<String> peerIds = effectiveMembership.peerAddresses(config.nodeId()).stream()
            .map(PeerAddress::id)
            .collect(Collectors.toSet());
        nextIndex.entrySet().removeIf(entry -> !peerIds.contains(entry.getKey()));
        matchIndex.entrySet().removeIf(entry -> !peerIds.contains(entry.getKey()));
        for (String peerId : peerIds) {
            nextIndex.putIfAbsent(peerId, 1L);
            matchIndex.putIfAbsent(peerId, 0L);
        }
    }

    @Override
    public void close() {
        running = false;
        scheduler.shutdownNow();
    }
}

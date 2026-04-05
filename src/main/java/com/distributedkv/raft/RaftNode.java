package com.distributedkv.raft;

import com.distributedkv.proto.*;
import com.distributedkv.storage.InMemoryStore;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    // --- Node identity ---
    private final RaftConfig config;
    private final InMemoryStore store;

    // --- Raft persistent state ---
    private volatile int currentTerm = 0;
    private volatile String votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    // --- Raft volatile state ---
    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;

    // --- Leader volatile state (reinitialized after election) ---
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    // --- Node role ---
    public enum Role { FOLLOWER, CANDIDATE, LEADER }
    private volatile Role role = Role.FOLLOWER;
    private volatile String currentLeaderId = null;

    // --- Vote tracking ---
    private final AtomicInteger votesReceived = new AtomicInteger(0);

    // --- Timers ---
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    // --- gRPC channels to peers ---
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> peerStubs =
            new ConcurrentHashMap<>();

    public RaftNode(RaftConfig config, InMemoryStore store) {
        this.config = config;
        this.store = store;
        initializePeerConnections();
    }

    // -------------------------------------------------------------------------
    // Startup
    // -------------------------------------------------------------------------

    private void initializePeerConnections() {
        for (RaftConfig.PeerConfig peer : config.getPeers()) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(peer.getHost(), peer.getPort())
                    .usePlaintext()
                    .build();
            peerStubs.put(peer.getNodeId(),
                    RaftServiceGrpc.newBlockingStub(channel));
            logger.info("Connected to peer: {}", peer);
        }
    }

    public void start() {
        logger.info("Node {} starting as FOLLOWER, term={}", config.getNodeId(), currentTerm);
        resetElectionTimer();
    }

    // -------------------------------------------------------------------------
    // Election Timer
    // -------------------------------------------------------------------------

    private synchronized void resetElectionTimer() {
        if (electionTimer != null && !electionTimer.isDone()) {
            electionTimer.cancel(false);
        }

        int timeout = RaftConfig.ELECTION_TIMEOUT_MIN_MS +
                new Random().nextInt(
                        RaftConfig.ELECTION_TIMEOUT_MAX_MS - RaftConfig.ELECTION_TIMEOUT_MIN_MS
                );

        electionTimer = scheduler.schedule(
                this::startElection,
                timeout,
                TimeUnit.MILLISECONDS
        );
    }

    // -------------------------------------------------------------------------
    // Leader Election
    // -------------------------------------------------------------------------

    private synchronized void startElection() {
        if (role == Role.LEADER) return;

        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = config.getNodeId();
        votesReceived.set(1); // vote for self

        logger.info("Node {} starting election for term {}", config.getNodeId(), currentTerm);

        int lastLogIndex = log.size();
        int lastLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();

        if (config.getPeers().isEmpty() || votesReceived.get() >= config.getQuorumSize()) {
            becomeLeader();
            return;
        }

        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(currentTerm)
                .setCandidateId(config.getNodeId())
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();

        for (Map.Entry<String, RaftServiceGrpc.RaftServiceBlockingStub> entry : peerStubs.entrySet()) {
            String peerId = entry.getKey();
            RaftServiceGrpc.RaftServiceBlockingStub stub = entry.getValue();

            scheduler.submit(() -> sendRequestVote(peerId, stub, request));
        }

        resetElectionTimer();
    }

    private void sendRequestVote(
            String peerId,
            RaftServiceGrpc.RaftServiceBlockingStub stub,
            RequestVoteRequest request) {
        try {
            RequestVoteResponse response = stub.requestVote(request);

            synchronized (this) {
                if (response.getTerm() > currentTerm) {
                    stepDown(response.getTerm());
                    return;
                }

                if (role == Role.CANDIDATE && response.getVoteGranted()) {
                    int votes = votesReceived.incrementAndGet();
                    logger.info("Node {} received vote from {}, total votes={}",
                            config.getNodeId(), peerId, votes);

                    if (votes >= config.getQuorumSize()) {
                        becomeLeader();
                    }
                }
            }
        } catch (StatusRuntimeException e) {
            logger.warn("RequestVote RPC to {} failed: {}", peerId, e.getStatus());
        }
    }

    // -------------------------------------------------------------------------
    // Become Leader
    // -------------------------------------------------------------------------

    private synchronized void becomeLeader() {
        if (role != Role.CANDIDATE) return;

        role = Role.LEADER;
        currentLeaderId = config.getNodeId();
        logger.info("Node {} became LEADER for term {}", config.getNodeId(), currentTerm);

        // Initialize nextIndex and matchIndex for each peer
        for (RaftConfig.PeerConfig peer : config.getPeers()) {
            nextIndex.put(peer.getNodeId(), log.size() + 1);
            matchIndex.put(peer.getNodeId(), 0);
        }

        // Cancel election timer - leaders don't need it
        if (electionTimer != null) electionTimer.cancel(false);

        // Start sending heartbeats
        startHeartbeatTimer();

        // Append a NOOP entry to commit previous term entries
        appendToLog("NOOP");
    }

    // -------------------------------------------------------------------------
    // Heartbeat Timer
    // -------------------------------------------------------------------------

    private void startHeartbeatTimer() {
        heartbeatTimer = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                0,
                RaftConfig.HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    private void sendHeartbeats() {
        if (role != Role.LEADER) {
            if (heartbeatTimer != null) heartbeatTimer.cancel(false);
            return;
        }

        for (Map.Entry<String, RaftServiceGrpc.RaftServiceBlockingStub> entry : peerStubs.entrySet()) {
            scheduler.submit(() -> sendAppendEntries(entry.getKey(), entry.getValue(), List.of()));
        }
    }

    // -------------------------------------------------------------------------
    // Log Replication
    // -------------------------------------------------------------------------

    /**
     * Called by the gRPC service layer when a client sends a PUT or DELETE.
     * Only the leader can accept writes.
     * Returns true if committed by quorum, false otherwise.
     */
    public synchronized boolean appendToLog(String command) {
        if (role != Role.LEADER) {
            logger.warn("Node {} is not leader, rejecting write", config.getNodeId());
            return false;
        }

        int index = log.size() + 1;
        LogEntry entry = new LogEntry(currentTerm, index, command);
        log.add(entry);
        logger.info("Leader appended entry: {}", entry);

        // Replicate to peers and wait for quorum
        return replicateAndWaitForQuorum(entry);
    }

    private boolean replicateAndWaitForQuorum(LogEntry entry) {
        AtomicInteger acks = new AtomicInteger(1); // leader counts itself
        CountDownLatch quorumLatch = new CountDownLatch(config.getQuorumSize() - 1);

        for (Map.Entry<String, RaftServiceGrpc.RaftServiceBlockingStub> peer : peerStubs.entrySet()) {
            String peerId = peer.getKey();
            RaftServiceGrpc.RaftServiceBlockingStub stub = peer.getValue();

            scheduler.submit(() -> {
                boolean success = sendAppendEntries(peerId, stub, List.of(entry));
                if (success) {
                    acks.incrementAndGet();
                    quorumLatch.countDown();
                }
            });
        }

        try {
            boolean reached = quorumLatch.await(500, TimeUnit.MILLISECONDS);
            if (reached) {
                commitIndex = entry.getIndex();
                applyCommittedEntries();
                logger.info("Entry committed at index={}, acks={}", entry.getIndex(), acks.get());
                return true;
            } else {
                logger.warn("Quorum not reached for entry index={}", entry.getIndex());
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean sendAppendEntries(
            String peerId,
            RaftServiceGrpc.RaftServiceBlockingStub stub,
            List<LogEntry> entries) {
        try {
            int prevLogIndex = log.size() - entries.size();
            int prevLogTerm = prevLogIndex > 0 ? log.get(prevLogIndex - 1).getTerm() : 0;

            AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setLeaderId(config.getNodeId())
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .setLeaderCommit(commitIndex);

            for (LogEntry entry : entries) {
                builder.addEntries(
                        com.distributedkv.proto.LogEntry.newBuilder()
                                .setTerm(entry.getTerm())
                                .setIndex(entry.getIndex())
                                .setCommand(entry.getCommand())
                                .build()
                );
            }

            AppendEntriesResponse response = stub.appendEntries(builder.build());

            synchronized (this) {
                if (response.getTerm() > currentTerm) {
                    stepDown(response.getTerm());
                    return false;
                }
            }

            return response.getSuccess();

        } catch (StatusRuntimeException e) {
            logger.warn("AppendEntries RPC to {} failed: {}", peerId, e.getStatus());
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Apply committed entries to state machine
    // -------------------------------------------------------------------------

    private synchronized void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied - 1);
            store.applyEntry(entry);
            logger.debug("Applied entry to state machine: {}", entry);
        }
    }

    // -------------------------------------------------------------------------
    // Handle incoming RPCs (called by RaftServiceImpl)
    // -------------------------------------------------------------------------

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        // Reject if request is from old term
        if (request.getTerm() < currentTerm) {
            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        }

        // Valid leader - reset election timer
        if (request.getTerm() > currentTerm) {
            stepDown(request.getTerm());
        }

        currentLeaderId = request.getLeaderId();
        resetElectionTimer();

        // Consistency check - verify prevLogIndex and prevLogTerm match
        int prevLogIndex = request.getPrevLogIndex();
        if (prevLogIndex > 0) {
            if (log.size() < prevLogIndex) {
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();
            }
            LogEntry prevEntry = log.get(prevLogIndex - 1);
            if (prevEntry.getTerm() != request.getPrevLogTerm()) {
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();
            }
        }

        // Append new entries
        for (com.distributedkv.proto.LogEntry protoEntry : request.getEntriesList()) {
            int index = protoEntry.getIndex();

            // Remove conflicting entries
            if (log.size() >= index) {
                LogEntry existing = log.get(index - 1);
                if (existing.getTerm() != protoEntry.getTerm()) {
                    while (log.size() >= index) {
                        log.remove(log.size() - 1);
                    }
                }
            }

            if (log.size() < index) {
                log.add(new LogEntry(protoEntry.getTerm(), index, protoEntry.getCommand()));
            }
        }

        // Update commit index
        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), log.size());
            applyCommittedEntries();
        }

        return AppendEntriesResponse.newBuilder()
                .setTerm(currentTerm)
                .setSuccess(true)
                .setMatchIndex(log.size())
                .build();
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        // Reject if candidate is behind
        if (request.getTerm() < currentTerm) {
            return RequestVoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }

        if (request.getTerm() > currentTerm) {
            stepDown(request.getTerm());
        }

        boolean logIsUpToDate = isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());
        boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId()));

        if (canVote && logIsUpToDate) {
            votedFor = request.getCandidateId();
            resetElectionTimer();
            logger.info("Node {} granting vote to {} for term {}",
                    config.getNodeId(), request.getCandidateId(), request.getTerm());
            return RequestVoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(true)
                    .build();
        }

        return RequestVoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(false)
                .build();
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    private boolean isLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
        int myLastLogIndex = log.size();

        if (candidateLastLogTerm != myLastLogTerm) {
            return candidateLastLogTerm > myLastLogTerm;
        }
        return candidateLastLogIndex >= myLastLogIndex;
    }

    private synchronized void stepDown(int newTerm) {
        currentTerm = newTerm;
        role = Role.FOLLOWER;
        votedFor = null;
        currentLeaderId = null;
        if (heartbeatTimer != null) heartbeatTimer.cancel(false);
        logger.info("Node {} stepping down to FOLLOWER, new term={}", config.getNodeId(), newTerm);
    }

    // -------------------------------------------------------------------------
    // Public getters (used by gRPC service layer)
    // -------------------------------------------------------------------------

    public boolean isLeader() { return role == Role.LEADER; }
    public String getLeaderId() { return currentLeaderId; }
    public String getNodeId() { return config.getNodeId(); }
    public Role getRole() { return role; }
    public int getCurrentTerm() { return currentTerm; }
    public InMemoryStore getStore() { return store; }

    public void shutdown() {
        logger.info("Node {} shutting down", config.getNodeId());
        scheduler.shutdownNow();
    }
}
package com.distributedkv.raft;

import java.util.List;

public class RaftConfig {

    // Raft timing constants (milliseconds)
    public static final int ELECTION_TIMEOUT_MIN_MS = 150;
    public static final int ELECTION_TIMEOUT_MAX_MS = 300;
    public static final int HEARTBEAT_INTERVAL_MS = 50;

    // Cluster configuration
    private final String nodeId;
    private final int port;
    private final List<PeerConfig> peers;

    // Quorum size - for 5 nodes this is 3
    private final int quorumSize;

    public RaftConfig(String nodeId, int port, List<PeerConfig> peers) {
        this.nodeId = nodeId;
        this.port = port;
        this.peers = peers;
        this.quorumSize = (peers.size() + 1) / 2 + 1;
    }

    public String getNodeId() { return nodeId; }
    public int getPort() { return port; }
    public List<PeerConfig> getPeers() { return peers; }
    public int getQuorumSize() { return quorumSize; }

    // Represents a single peer node's address
    public static class PeerConfig {
        private final String nodeId;
        private final String host;
        private final int port;

        public PeerConfig(String nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

        public String getNodeId() { return nodeId; }
        public String getHost() { return host; }
        public int getPort() { return port; }

        @Override
        public String toString() {
            return nodeId + "@" + host + ":" + port;
        }
    }
}
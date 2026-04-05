package com.distributedkv;

import com.distributedkv.grpc.GrpcServer;
import com.distributedkv.raft.RaftConfig;
import com.distributedkv.raft.RaftNode;
import com.distributedkv.raft.StateMachine;
import com.distributedkv.storage.InMemoryStore;
import com.distributedkv.util.LatencyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        // --- Read configuration from environment variables ---
        // These are set in docker-compose.yml or start-cluster.sh
        String nodeId = getEnv("NODE_ID", "node1");
        int port = Integer.parseInt(getEnv("PORT", "50051"));
        String peersEnv = getEnv("PEERS", "");

        logger.info("Starting node: id={}, port={}, peers={}", nodeId, port, peersEnv);

        // --- Parse peer list ---
        // Format: "node2:localhost:50052,node3:localhost:50053"
        List<RaftConfig.PeerConfig> peers = parsePeers(peersEnv);

        // --- Build config ---
        RaftConfig config = new RaftConfig(nodeId, port, peers);

        // --- Build storage layer ---
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);

        // --- Build Raft node ---
        RaftNode raftNode = new RaftNode(config, store);

        // --- Build latency tracker ---
        LatencyTracker latencyTracker = new LatencyTracker();

        // --- Start gRPC server ---
        GrpcServer grpcServer = new GrpcServer(port, raftNode, latencyTracker);
        grpcServer.start();

        // --- Start Raft (begins election timer) ---
        raftNode.start();

        logger.info("Node {} is up and running on port {}", nodeId, port);

        // --- Block main thread until shutdown ---
        grpcServer.blockUntilShutdown();
    }

    /**
     * Parse peer string into PeerConfig list.
     * Input format: "node2:localhost:50052,node3:localhost:50053"
     */
    private static List<RaftConfig.PeerConfig> parsePeers(String peersEnv) {
        List<RaftConfig.PeerConfig> peers = new ArrayList<>();

        if (peersEnv == null || peersEnv.isBlank()) {
            return peers;
        }

        for (String peerStr : peersEnv.split(",")) {
            String[] parts = peerStr.trim().split(":");
            if (parts.length == 3) {
                String peerId = parts[0];
                String host = parts[1];
                int peerPort = Integer.parseInt(parts[2]);
                peers.add(new RaftConfig.PeerConfig(peerId, host, peerPort));
                logger.info("Registered peer: {}@{}:{}", peerId, host, peerPort);
            } else {
                logger.warn("Skipping malformed peer config: {}", peerStr);
            }
        }

        return peers;
    }

    /**
     * Read an environment variable with a fallback default.
     */
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
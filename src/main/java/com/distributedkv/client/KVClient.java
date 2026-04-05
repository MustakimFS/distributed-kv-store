package com.distributedkv.client;

import com.distributedkv.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class KVClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KVClient.class);
    private static final int MAX_RETRIES = 3;

    private ManagedChannel channel;
    private KVServiceGrpc.KVServiceBlockingStub stub;
    private String currentHost;
    private int currentPort;

    // All known nodes in the cluster for fallback
    private final List<String[]> clusterNodes; // each entry: [host, port]

    /**
     * Connect to a single node to start with.
     * The client will follow redirects to the leader automatically.
     *
     * @param host         initial node host
     * @param port         initial node port
     * @param clusterNodes all nodes as [host, port] pairs for fallback
     */
    public KVClient(String host, int port, List<String[]> clusterNodes) {
        this.clusterNodes = clusterNodes;
        connect(host, port);
    }

    private void connect(String host, int port) {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdownNow();
        }
        this.currentHost = host;
        this.currentPort = port;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = KVServiceGrpc.newBlockingStub(channel);
        logger.info("KVClient connected to {}:{}", host, port);
    }

    // -------------------------------------------------------------------------
    // PUT
    // -------------------------------------------------------------------------

    public boolean put(String key, String value) {
        return put(key, value, ConsistencyLevel.STRONG);
    }

    public boolean put(String key, String value, ConsistencyLevel consistency) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(value)
                .setConsistency(mapConsistency(consistency))
                .build();

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                PutResponse response = stub.put(request);

                if (response.getSuccess()) {
                    return true;
                }

                // Follow leader redirect
                if (!response.getLeaderId().isEmpty()) {
                    logger.info("Redirecting PUT to leader: {}", response.getLeaderId());
                    redirectToLeader(response.getLeaderId());
                    continue;
                }

                logger.warn("PUT failed: {}", response.getError());
                return false;

            } catch (StatusRuntimeException e) {
                logger.warn("PUT attempt {} failed: {}", attempt + 1, e.getStatus());
                tryNextNode();
            }
        }

        logger.error("PUT failed after {} retries", MAX_RETRIES);
        return false;
    }

    // -------------------------------------------------------------------------
    // GET
    // -------------------------------------------------------------------------

    public Optional<String> get(String key) {
        return get(key, ConsistencyLevel.STRONG);
    }

    public Optional<String> get(String key, ConsistencyLevel consistency) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(key)
                .setConsistency(mapConsistency(consistency))
                .build();

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                GetResponse response = stub.get(request);

                if (response.getFound()) {
                    return Optional.of(response.getValue());
                }

                // Follow leader redirect for strong reads
                if (!response.getLeaderId().isEmpty() && !response.getError().isEmpty()) {
                    logger.info("Redirecting GET to leader: {}", response.getLeaderId());
                    redirectToLeader(response.getLeaderId());
                    continue;
                }

                // Key not found - not an error
                return Optional.empty();

            } catch (StatusRuntimeException e) {
                logger.warn("GET attempt {} failed: {}", attempt + 1, e.getStatus());
                tryNextNode();
            }
        }

        logger.error("GET failed after {} retries", MAX_RETRIES);
        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // DELETE
    // -------------------------------------------------------------------------

    public boolean delete(String key) {
        DeleteRequest request = DeleteRequest.newBuilder()
                .setKey(key)
                .setConsistency(com.distributedkv.proto.ConsistencyLevel.STRONG)
                .build();

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                DeleteResponse response = stub.delete(request);

                if (response.getSuccess()) {
                    return true;
                }

                if (!response.getLeaderId().isEmpty()) {
                    logger.info("Redirecting DELETE to leader: {}", response.getLeaderId());
                    redirectToLeader(response.getLeaderId());
                    continue;
                }

                logger.warn("DELETE failed: {}", response.getError());
                return false;

            } catch (StatusRuntimeException e) {
                logger.warn("DELETE attempt {} failed: {}", attempt + 1, e.getStatus());
                tryNextNode();
            }
        }

        logger.error("DELETE failed after {} retries", MAX_RETRIES);
        return false;
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    /**
     * Redirect to a specific leader by matching its nodeId to known cluster nodes.
     * Leader ID format is "node1", "node2" etc - we map that to port 50051, 50052 etc.
     */
    private void redirectToLeader(String leaderId) {
        // leaderId is like "node1", "node2" - extract the number
        try {
            int nodeNumber = Integer.parseInt(leaderId.replace("node", ""));
            int leaderPort = 50050 + nodeNumber; // node1 -> 50051, node2 -> 50052
            connect("localhost", leaderPort);
        } catch (NumberFormatException e) {
            logger.warn("Could not parse leaderId: {}", leaderId);
        }
    }

    /**
     * Try connecting to the next available node in the cluster.
     * Used when current node is unreachable.
     */
    private void tryNextNode() {
        for (String[] node : clusterNodes) {
            String host = node[0];
            int port = Integer.parseInt(node[1]);
            if (!host.equals(currentHost) || port != currentPort) {
                logger.info("Trying next node: {}:{}", host, port);
                connect(host, port);
                return;
            }
        }
    }

    private com.distributedkv.proto.ConsistencyLevel mapConsistency(ConsistencyLevel level) {
        return level == ConsistencyLevel.STRONG
                ? com.distributedkv.proto.ConsistencyLevel.STRONG
                : com.distributedkv.proto.ConsistencyLevel.EVENTUAL;
    }

    @Override
    public void close() throws Exception {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            logger.info("KVClient disconnected");
        }
    }
}
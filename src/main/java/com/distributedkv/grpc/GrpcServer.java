package com.distributedkv.grpc;

import com.distributedkv.raft.RaftNode;
import com.distributedkv.util.LatencyTracker;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(GrpcServer.class);

    private final int port;
    private final Server server;

    public GrpcServer(int port, RaftNode raftNode, LatencyTracker latencyTracker) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new RaftServiceImpl(raftNode))
                .addService(new KVServiceImpl(raftNode, latencyTracker))
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("gRPC server started on port {}", port);

        // Shutdown hook - cleanly stop server on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down gRPC server on port {}", port);
            try {
                GrpcServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted during shutdown", e);
            }
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            logger.info("gRPC server on port {} stopped", port);
        }
    }

    /**
     * Block the calling thread until the server shuts down.
     * Called from Main to keep the process alive.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
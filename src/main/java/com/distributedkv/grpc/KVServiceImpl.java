package com.distributedkv.grpc;

import com.distributedkv.proto.*;
import com.distributedkv.raft.RaftNode;
import com.distributedkv.util.LatencyTracker;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(KVServiceImpl.class);

    private final RaftNode raftNode;
    private final LatencyTracker latencyTracker;

    public KVServiceImpl(RaftNode raftNode, LatencyTracker latencyTracker) {
        this.raftNode = raftNode;
        this.latencyTracker = latencyTracker;
    }

    // -------------------------------------------------------------------------
    // PUT
    // -------------------------------------------------------------------------

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        long start = System.currentTimeMillis();

        try {
            String key = request.getKey();
            String value = request.getValue();

            if (key == null || key.isBlank()) {
                responseObserver.onNext(PutResponse.newBuilder()
                        .setSuccess(false)
                        .setError("Key cannot be blank")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Writes always go through the leader
            if (!raftNode.isLeader()) {
                String leaderId = raftNode.getLeaderId();
                logger.info("Node {} is not leader, redirecting PUT to leader={}",
                        raftNode.getNodeId(), leaderId);
                responseObserver.onNext(PutResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(leaderId != null ? leaderId : "")
                        .setError("Not the leader - redirect to: " + leaderId)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            String command = "PUT " + key + " " + value;
            boolean success = raftNode.appendToLog(command);

            latencyTracker.record("PUT", System.currentTimeMillis() - start);

            responseObserver.onNext(PutResponse.newBuilder()
                    .setSuccess(success)
                    .setLeaderId(raftNode.getNodeId())
                    .setError(success ? "" : "Failed to reach quorum")
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling PUT: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    // -------------------------------------------------------------------------
    // GET
    // -------------------------------------------------------------------------

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        long start = System.currentTimeMillis();

        try {
            String key = request.getKey();
            ConsistencyLevel consistency = request.getConsistency();

            if (key == null || key.isBlank()) {
                responseObserver.onNext(GetResponse.newBuilder()
                        .setFound(false)
                        .setError("Key cannot be blank")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            if (consistency == ConsistencyLevel.STRONG) {
                // CP - only the leader can serve strongly consistent reads
                if (!raftNode.isLeader()) {
                    String leaderId = raftNode.getLeaderId();
                    logger.info("Strong read on non-leader, redirecting to leader={}",
                            leaderId);
                    responseObserver.onNext(GetResponse.newBuilder()
                            .setFound(false)
                            .setLeaderId(leaderId != null ? leaderId : "")
                            .setError("Not the leader - redirect to: " + leaderId)
                            .build());
                    responseObserver.onCompleted();
                    return;
                }

                Optional<String> value = raftNode.getStore().get(key);
                latencyTracker.record("GET_STRONG", System.currentTimeMillis() - start);

                responseObserver.onNext(GetResponse.newBuilder()
                        .setFound(value.isPresent())
                        .setValue(value.orElse(""))
                        .setLeaderId(raftNode.getNodeId())
                        .build());

            } else {
                // AP - any node can serve eventual reads directly from local state
                Optional<String> value = raftNode.getStore().get(key);
                latencyTracker.record("GET_EVENTUAL", System.currentTimeMillis() - start);

                responseObserver.onNext(GetResponse.newBuilder()
                        .setFound(value.isPresent())
                        .setValue(value.orElse(""))
                        .setLeaderId(raftNode.getLeaderId() != null ? raftNode.getLeaderId() : "")
                        .build());
            }

            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling GET: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    // -------------------------------------------------------------------------
    // DELETE
    // -------------------------------------------------------------------------

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        long start = System.currentTimeMillis();

        try {
            String key = request.getKey();

            if (key == null || key.isBlank()) {
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setSuccess(false)
                        .setError("Key cannot be blank")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // Deletes always go through the leader
            if (!raftNode.isLeader()) {
                String leaderId = raftNode.getLeaderId();
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(leaderId != null ? leaderId : "")
                        .setError("Not the leader - redirect to: " + leaderId)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            String command = "DELETE " + key;
            boolean success = raftNode.appendToLog(command);

            latencyTracker.record("DELETE", System.currentTimeMillis() - start);

            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setSuccess(success)
                    .setLeaderId(raftNode.getNodeId())
                    .setError(success ? "" : "Failed to reach quorum")
                    .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling DELETE: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
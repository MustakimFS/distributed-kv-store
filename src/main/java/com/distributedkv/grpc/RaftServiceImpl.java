package com.distributedkv.grpc;

import com.distributedkv.proto.*;
import com.distributedkv.raft.RaftNode;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(RaftServiceImpl.class);

    private final RaftNode raftNode;

    public RaftServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    /**
     * Called by the leader to replicate log entries or send heartbeats.
     * Delegates directly to RaftNode which handles all the Raft logic.
     */
    @Override
    public void appendEntries(
            AppendEntriesRequest request,
            StreamObserver<AppendEntriesResponse> responseObserver) {

        try {
            logger.debug("Node {} received AppendEntries from leader={}, term={}, entries={}",
                    raftNode.getNodeId(),
                    request.getLeaderId(),
                    request.getTerm(),
                    request.getEntriesCount());

            AppendEntriesResponse response = raftNode.handleAppendEntries(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling AppendEntries on node {}: {}",
                    raftNode.getNodeId(), e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Called by a candidate requesting votes during an election.
     * Delegates directly to RaftNode which handles vote granting logic.
     */
    @Override
    public void requestVote(
            RequestVoteRequest request,
            StreamObserver<RequestVoteResponse> responseObserver) {

        try {
            logger.debug("Node {} received RequestVote from candidate={}, term={}",
                    raftNode.getNodeId(),
                    request.getCandidateId(),
                    request.getTerm());

            RequestVoteResponse response = raftNode.handleRequestVote(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling RequestVote on node {}: {}",
                    raftNode.getNodeId(), e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
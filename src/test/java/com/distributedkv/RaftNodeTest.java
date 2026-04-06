package com.distributedkv;

import com.distributedkv.raft.LogEntry;
import com.distributedkv.raft.RaftConfig;
import com.distributedkv.raft.RaftNode;
import com.distributedkv.raft.StateMachine;
import com.distributedkv.storage.InMemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RaftNodeTest {

    private RaftNode node;

    @BeforeEach
    void setUp() {
        // Single node cluster - it will elect itself as leader
        RaftConfig config = new RaftConfig("node1", 59001, List.of());
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);
        node = new RaftNode(config, store);
        node.start();
    }

    @AfterEach
    void tearDown() {
        node.shutdown();
    }

    @Test
    void testNodeStartsAsFollower() {
        // Node starts as follower before election timeout
        assertNotNull(node.getNodeId());
        assertEquals("node1", node.getNodeId());
    }

    @Test
    void testSingleNodeBecomesLeader() throws InterruptedException {
        // Single node cluster - should elect itself within 300ms
        Thread.sleep(500);
        assertTrue(node.isLeader(),
                "Single node should elect itself as leader");
    }

    @Test
    void testLeaderCanWrite() throws InterruptedException {
        // Wait for election
        Thread.sleep(800);
        assertTrue(node.isLeader());

        boolean result = node.appendToLog("PUT testKey testValue");
        assertTrue(result, "Leader should successfully commit a write");
    }

    @Test
    void testStateMachineAppliesWrite() throws InterruptedException {
        // Wait for election
        Thread.sleep(800);
        assertTrue(node.isLeader());

        node.appendToLog("PUT hello world");

        // Give state machine time to apply
        Thread.sleep(100);

        String value = node.getStore().get("hello").orElse(null);
        assertEquals("world", value, "State machine should have applied the PUT");
    }

    @Test
    void testStateMachineAppliesDelete() throws InterruptedException {
        Thread.sleep(800);
        assertTrue(node.isLeader());

        node.appendToLog("PUT toDelete someValue");
        Thread.sleep(100);
        assertNotNull(node.getStore().get("toDelete").orElse(null));

        node.appendToLog("DELETE toDelete");
        Thread.sleep(200);
        assertFalse(node.getStore().exists("toDelete"),
                "Key should be deleted from state machine");
    }

    @Test
    void testLogEntryToString() {
        LogEntry entry = new LogEntry(1, 1, "PUT foo bar");
        assertTrue(entry.toString().contains("PUT foo bar"));
        assertFalse(entry.isNoOp());
    }

    @Test
    void testNoOpEntry() {
        LogEntry noop = new LogEntry(1, 1, "NOOP");
        assertTrue(noop.isNoOp());
    }
}
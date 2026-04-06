package com.distributedkv;

import com.distributedkv.raft.RaftConfig;
import com.distributedkv.raft.RaftNode;
import com.distributedkv.raft.StateMachine;
import com.distributedkv.storage.InMemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    @Test
    void testNonLeaderRejectsWrites() throws InterruptedException {
        // Start node but don't wait for election - it starts as FOLLOWER
        RaftConfig config = new RaftConfig("node1", 59003, List.of());
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);
        node = new RaftNode(config, store);
        // Do NOT call node.start() - stays as follower

        // A follower should reject writes
        boolean result = node.appendToLog("PUT shouldFail value");
        assertFalse(result, "Non-leader should reject writes");
    }

    @Test
    void testDataPersistsAfterMultipleWrites() throws InterruptedException {
        RaftConfig config = new RaftConfig("node1", 59004, List.of());
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);
        node = new RaftNode(config, store);
        node.start();

        Thread.sleep(800);
        assertTrue(node.isLeader());

        // Write multiple keys
        for (int i = 0; i < 10; i++) {
            boolean success = node.appendToLog("PUT key" + i + " value" + i);
            assertTrue(success, "Write " + i + " should succeed");
        }

        Thread.sleep(100);

        // Verify all keys are present
        for (int i = 0; i < 10; i++) {
            Optional<String> value = node.getStore().get("key" + i);
            assertTrue(value.isPresent(), "key" + i + " should exist");
            assertEquals("value" + i, value.get());
        }
    }

    @Test
    void testOverwriteKey() throws InterruptedException {
        RaftConfig config = new RaftConfig("node1", 59005, List.of());
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);
        node = new RaftNode(config, store);
        node.start();

        Thread.sleep(800);
        assertTrue(node.isLeader());

        node.appendToLog("PUT myKey firstValue");
        Thread.sleep(100);
        assertEquals("firstValue", node.getStore().get("myKey").orElse(null));

        node.appendToLog("PUT myKey secondValue");
        Thread.sleep(100);
        assertEquals("secondValue", node.getStore().get("myKey").orElse(null),
                "Key should be overwritten with new value");
    }

    @Test
    void testNodeTermIncrementsOnElection() throws InterruptedException {
        RaftConfig config = new RaftConfig("node1", 59006, List.of());
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);
        node = new RaftNode(config, store);

        int initialTerm = node.getCurrentTerm();
        assertEquals(0, initialTerm);

        node.start();
        Thread.sleep(800);

        assertTrue(node.getCurrentTerm() > 0,
                "Term should have incremented after election");
    }
}
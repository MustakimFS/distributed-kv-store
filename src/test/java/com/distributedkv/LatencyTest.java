package com.distributedkv;

import com.distributedkv.raft.RaftConfig;
import com.distributedkv.raft.RaftNode;
import com.distributedkv.raft.StateMachine;
import com.distributedkv.storage.InMemoryStore;
import com.distributedkv.util.LatencyTracker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LatencyTest {

    private RaftNode node;
    private LatencyTracker tracker;

    @BeforeEach
    void setUp() throws InterruptedException {
        RaftConfig config = new RaftConfig("node1", 59002, List.of());
        StateMachine stateMachine = new StateMachine();
        InMemoryStore store = new InMemoryStore(stateMachine);
        node = new RaftNode(config, store);
        tracker = new LatencyTracker();
        node.start();

        // Wait for leader election
        Thread.sleep(800);
        assertTrue(node.isLeader(), "Node should be leader before latency tests");
    }

    @AfterEach
    void tearDown() {
        tracker.printReport();
        node.shutdown();
    }

    @Test
    void testReadLatencyUnder10ms() throws InterruptedException {
        // Seed some data
        node.appendToLog("PUT latencyKey latencyValue");
        Thread.sleep(100);

        // Measure 100 reads
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            long start = System.currentTimeMillis();
            Optional<String> value = node.getStore().get("latencyKey");
            long latency = System.currentTimeMillis() - start;
            tracker.record("GET_STRONG", latency);
            assertTrue(value.isPresent());
        }

        double p99 = tracker.p99("GET_STRONG");
        double avg = tracker.average("GET_STRONG");

        System.out.println("Read latency - avg: " + avg + "ms, p99: " + p99 + "ms");

        // In-memory reads should be well under 10ms
        assertTrue(p99 <= 10,
                "p99 read latency should be under 10ms but was: " + p99 + "ms");
    }

    @Test
    void testWriteLatencyUnder50ms() throws InterruptedException {
        int iterations = 50;

        for (int i = 0; i < iterations; i++) {
            long start = System.currentTimeMillis();
            node.appendToLog("PUT key" + i + " value" + i);
            long latency = System.currentTimeMillis() - start;
            tracker.record("PUT", latency);
        }

        double avg = tracker.average("PUT");
        double p99 = tracker.p99("PUT");

        System.out.println("Write latency - avg: " + avg + "ms, p99: " + p99 + "ms");

        // Single node writes (no network) should be fast
        assertTrue(avg <= 50,
                "Average write latency should be under 50ms but was: " + avg + "ms");
    }

    @Test
    void testThroughput() throws InterruptedException {
        int operations = 1000;
        long start = System.currentTimeMillis();

        for (int i = 0; i < operations; i++) {
            node.appendToLog("PUT throughputKey" + i + " value" + i);
        }

        long elapsed = System.currentTimeMillis() - start;
        double opsPerSec = (operations / (double) elapsed) * 1000;

        System.out.println("Throughput: " + String.format("%.0f", opsPerSec) + " ops/sec");
        System.out.println("Total time for " + operations + " ops: " + elapsed + "ms");

        assertTrue(opsPerSec >= 100,
                "Should achieve at least 100 ops/sec on single node, got: " + opsPerSec);
    }

    @Test
    void testLatencyTrackerPercentiles() {
        tracker.record("TEST_OP", 1L);
        tracker.record("TEST_OP", 2L);
        tracker.record("TEST_OP", 3L);
        tracker.record("TEST_OP", 100L);

        assertEquals(4, tracker.count("TEST_OP"));
        assertTrue(tracker.p99("TEST_OP") >= 100);
        assertTrue(tracker.p50("TEST_OP") <= 3);
    }
}
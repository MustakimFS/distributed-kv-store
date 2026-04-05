package com.distributedkv.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class LatencyTracker {

    private static final Logger logger = LoggerFactory.getLogger(LatencyTracker.class);

    // Stores latency samples per operation type e.g. "GET_STRONG", "PUT", "DELETE"
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Long>> samples =
            new ConcurrentHashMap<>();

    /**
     * Record a latency sample for a given operation.
     * @param operation  e.g. "GET_STRONG", "GET_EVENTUAL", "PUT", "DELETE"
     * @param latencyMs  how long the operation took in milliseconds
     */
    public void record(String operation, long latencyMs) {
        samples.computeIfAbsent(operation, k -> new CopyOnWriteArrayList<>())
                .add(latencyMs);
    }

    /**
     * Returns p50 (median) latency for the given operation in ms.
     */
    public double p50(String operation) {
        return percentile(operation, 50);
    }

    /**
     * Returns p99 latency for the given operation in ms.
     */
    public double p99(String operation) {
        return percentile(operation, 99);
    }

    /**
     * Returns average latency for the given operation in ms.
     */
    public double average(String operation) {
        List<Long> data = samples.getOrDefault(operation, new CopyOnWriteArrayList<>());
        if (data.isEmpty()) return 0.0;
        return data.stream().mapToLong(Long::longValue).average().orElse(0.0);
    }

    /**
     * Returns total number of recorded samples for an operation.
     */
    public int count(String operation) {
        return samples.getOrDefault(operation, new CopyOnWriteArrayList<>()).size();
    }

    /**
     * Prints a summary report for all tracked operations to the logger.
     */
    public void printReport() {
        if (samples.isEmpty()) {
            logger.info("No latency data recorded yet.");
            return;
        }

        logger.info("=== Latency Report ===");
        for (String operation : samples.keySet()) {
            logger.info("Operation: {} | count={} | avg={:.2f}ms | p50={:.2f}ms | p99={:.2f}ms",
                    operation,
                    count(operation),
                    average(operation),
                    p50(operation),
                    p99(operation));
        }
        logger.info("======================");
    }

    /**
     * Clears all recorded samples - useful between test runs.
     */
    public void reset() {
        samples.clear();
    }

    // --- Private helpers ---

    private double percentile(String operation, int percentile) {
        List<Long> data = new ArrayList<>(
                samples.getOrDefault(operation, new CopyOnWriteArrayList<>())
        );
        if (data.isEmpty()) return 0.0;

        Collections.sort(data);
        int index = (int) Math.ceil(percentile / 100.0 * data.size()) - 1;
        return data.get(Math.max(0, index));
    }
}
package com.distributedkv.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(StateMachine.class);

    // The actual key-value store
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    /**
     * Apply a committed log entry to the state machine.
     * Commands are in the format:
     *   "PUT key value"
     *   "DELETE key"
     *   "NOOP"
     */
    public synchronized void apply(LogEntry entry) {
        String command = entry.getCommand();

        if (command == null || command.isBlank()) {
            logger.warn("Received blank command in log entry at index {}", entry.getIndex());
            return;
        }

        String[] parts = command.split(" ", 3);

        switch (parts[0].toUpperCase()) {
            case "PUT" -> {
                if (parts.length < 3) {
                    logger.error("Malformed PUT command: {}", command);
                    return;
                }
                store.put(parts[1], parts[2]);
                logger.debug("Applied PUT {} = {}", parts[1], parts[2]);
            }
            case "DELETE" -> {
                if (parts.length < 2) {
                    logger.error("Malformed DELETE command: {}", command);
                    return;
                }
                store.remove(parts[1]);
                logger.debug("Applied DELETE {}", parts[1]);
            }
            case "NOOP" -> logger.debug("Applied NOOP at index {}", entry.getIndex());
            default -> logger.warn("Unknown command type: {}", parts[0]);
        }
    }

    public String get(String key) {
        return store.get(key);
    }

    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    public int size() {
        return store.size();
    }

    // Used for debugging and testing
    public ConcurrentHashMap<String, String> getSnapshot() {
        return new ConcurrentHashMap<>(store);
    }

    @Override
    public String toString() {
        return "StateMachine{size=" + store.size() + ", data=" + store + "}";
    }
}
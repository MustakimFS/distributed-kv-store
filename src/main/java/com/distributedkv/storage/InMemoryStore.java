package com.distributedkv.storage;

import com.distributedkv.raft.LogEntry;
import com.distributedkv.raft.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class InMemoryStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

    private final StateMachine stateMachine;

    public InMemoryStore(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    /**
     * Read a value directly from the state machine.
     * Used for both STRONG and EVENTUAL consistency reads.
     */
    public Optional<String> get(String key) {
        if (key == null || key.isBlank()) {
            logger.warn("Attempted to get a null or blank key");
            return Optional.empty();
        }
        String value = stateMachine.get(key);
        return Optional.ofNullable(value);
    }

    /**
     * Apply a committed log entry to the state machine.
     * Only called after Raft confirms the entry is committed.
     */
    public void applyEntry(LogEntry entry) {
        stateMachine.apply(entry);
    }

    /**
     * Check if a key exists in the store.
     */
    public boolean exists(String key) {
        return stateMachine.containsKey(key);
    }

    /**
     * Returns the number of keys currently stored.
     * Useful for metrics and debugging.
     */
    public int size() {
        return stateMachine.size();
    }

    /**
     * Dump the entire store - used for debugging and testing only.
     */
    public String dump() {
        return stateMachine.toString();
    }
}
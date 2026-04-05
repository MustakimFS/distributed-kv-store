package com.distributedkv.client;

public enum ConsistencyLevel {

    /**
     * CP mode - strongly consistent reads.
     * All reads go through the leader.
     * Guarantees linearizability - you always read the latest committed value.
     */
    STRONG,

    /**
     * AP mode - eventually consistent reads.
     * Reads served from any node's local state.
     * Lower latency but may return stale data during network partitions.
     */
    EVENTUAL
}
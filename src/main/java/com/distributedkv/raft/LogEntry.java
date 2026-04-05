package com.distributedkv.raft;

public class LogEntry {

    private final int term;         // which election term this entry was created in
    private final int index;        // position in the log
    private final String command;   // "PUT key value" or "DELETE key"

    public LogEntry(int term, int index, String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    public int getTerm() { return term; }
    public int getIndex() { return index; }
    public String getCommand() { return command; }

    // Helper to check if this is a no-op entry (sent by new leader on election)
    public boolean isNoOp() {
        return command.equals("NOOP");
    }

    @Override
    public String toString() {
        return "LogEntry{term=" + term + ", index=" + index + ", command='" + command + "'}";
    }
}
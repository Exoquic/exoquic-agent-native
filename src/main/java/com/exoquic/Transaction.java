package com.exoquic;

import com.exoquic.pgoutput.core.ReplicationMessage;
import org.postgresql.replication.LogSequenceNumber;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Transaction {
    private final long transactionId;
    private final Instant beginTime;
    private final ConcurrentLinkedQueue<TransactionEvent> events;
    private LogSequenceNumber beginLsn;
    private LogSequenceNumber commitLsn;
    private Instant commitTime;
    private volatile boolean isCommitted = false;

    public Transaction(long transactionId, Instant beginTime, LogSequenceNumber beginLsn) {
        this.transactionId = transactionId;
        this.beginTime = beginTime;
        this.beginLsn = beginLsn;
        this.events = new ConcurrentLinkedQueue<>();
    }

    public void addEvent(ReplicationMessage message, LogSequenceNumber lsn) {
        if (isCommitted) {
            throw new IllegalStateException("Cannot add events to committed transaction: " + transactionId);
        }
        events.offer(new TransactionEvent(message, lsn));
    }

    public void commit(LogSequenceNumber commitLsn, Instant commitTime) {
        this.commitLsn = commitLsn;
        this.commitTime = commitTime;
        this.isCommitted = true;
    }

    public List<TransactionEvent> getOrderedEvents() {
        return events.stream()
                .sorted(Comparator.comparing(TransactionEvent::getLsn))
                .toList();
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Instant getBeginTime() {
        return beginTime;
    }

    public LogSequenceNumber getBeginLsn() {
        return beginLsn;
    }

    public LogSequenceNumber getCommitLsn() {
        return commitLsn;
    }

    public Instant getCommitTime() {
        return commitTime;
    }

    public boolean isCommitted() {
        return isCommitted;
    }

    public int getEventCount() {
        return events.size();
    }

    public static class TransactionEvent {
        private final ReplicationMessage message;
        private final LogSequenceNumber lsn;

        public TransactionEvent(ReplicationMessage message, LogSequenceNumber lsn) {
            this.message = message;
            this.lsn = lsn;
        }

        public ReplicationMessage getMessage() {
            return message;
        }

        public LogSequenceNumber getLsn() {
            return lsn;
        }
    }
}
package com.exoquic;

import com.exoquic.pgoutput.core.ReplicationMessage;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class TransactionBuffer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionBuffer.class);
    
    private final ConcurrentMap<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final Consumer<Transaction> onTransactionCommit;
    private volatile long oldestActiveTransactionId = Long.MAX_VALUE;

    public TransactionBuffer(Consumer<Transaction> onTransactionCommit) {
        this.onTransactionCommit = onTransactionCommit;
    }

    public void handleBegin(long transactionId, Instant beginTime, LogSequenceNumber lsn) {
        Transaction transaction = new Transaction(transactionId, beginTime, lsn);
        activeTransactions.put(transactionId, transaction);
        
        if (transactionId < oldestActiveTransactionId) {
            oldestActiveTransactionId = transactionId;
        }
        
        logger.debug("Started transaction: {} at LSN: {}", transactionId, lsn);
    }

    public void handleEvent(long transactionId, ReplicationMessage message, LogSequenceNumber lsn) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            logger.warn("Received event for unknown transaction: {}. Creating implicit transaction.", transactionId);
            handleBegin(transactionId, Instant.now(), lsn);
            transaction = activeTransactions.get(transactionId);
        }
        
        transaction.addEvent(message, lsn);
        logger.debug("Added event to transaction: {} (table: {}, operation: {})", 
            transactionId, message.getTable(), message.getOperation());
    }

    public void handleCommit(long transactionId, Instant commitTime, LogSequenceNumber lsn) {
        Transaction transaction = activeTransactions.remove(transactionId);
        if (transaction == null) {
            logger.warn("Received commit for unknown transaction: {}", transactionId);
            return;
        }
        
        transaction.commit(lsn, commitTime);
        
        logger.info("Committing transaction: {} with {} events at LSN: {}", 
            transactionId, transaction.getEventCount(), lsn);
        
        try {
            onTransactionCommit.accept(transaction);
        } catch (Exception e) {
            logger.error("Error processing committed transaction: {}", transactionId, e);
        }
        
        updateOldestActiveTransaction();
    }

    public void handleRollback(long transactionId) {
        Transaction transaction = activeTransactions.remove(transactionId);
        if (transaction != null) {
            logger.info("Rolling back transaction: {} with {} events", 
                transactionId, transaction.getEventCount());
        } else {
            logger.warn("Received rollback for unknown transaction: {}", transactionId);
        }
        
        updateOldestActiveTransaction();
    }

    private void updateOldestActiveTransaction() {
        if (activeTransactions.isEmpty()) {
            oldestActiveTransactionId = Long.MAX_VALUE;
        } else {
            oldestActiveTransactionId = activeTransactions.keySet().stream()
                .min(Long::compareTo)
                .orElse(Long.MAX_VALUE);
        }
    }

    public int getActiveTransactionCount() {
        return activeTransactions.size();
    }

    public long getOldestActiveTransactionId() {
        return oldestActiveTransactionId == Long.MAX_VALUE ? -1 : oldestActiveTransactionId;
    }

    public List<Long> getActiveTransactionIds() {
        return activeTransactions.keySet().stream().sorted().toList();
    }

    public void clear() {
        int count = activeTransactions.size();
        activeTransactions.clear();
        oldestActiveTransactionId = Long.MAX_VALUE;
        
        if (count > 0) {
            logger.warn("Cleared {} active transactions from buffer", count);
        }
    }
}
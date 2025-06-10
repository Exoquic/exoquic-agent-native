/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.core;

import com.exoquic.pgoutput.config.Operation;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

public class PgOutputReplicationMessage implements ReplicationMessage {

    private final Operation operation;
    private final Instant commitTimestamp;
    private final Long transactionId;
    private final String table;
    private final List<Column> oldColumns;
    private final List<Column> newColumns;

    public PgOutputReplicationMessage(Operation operation, String table, Instant commitTimestamp, 
                                    Long transactionId, List<Column> oldColumns, List<Column> newColumns) {
        this.operation = operation;
        this.commitTimestamp = commitTimestamp;
        this.transactionId = transactionId;
        this.table = table;
        this.oldColumns = oldColumns;
        this.newColumns = newColumns;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Instant getCommitTime() {
        return commitTimestamp;
    }

    @Override
    public OptionalLong getTransactionId() {
        return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    @Override
    public String toString() {
        return "PgOutputReplicationMessage{" +
                "operation=" + operation +
                ", table='" + table + '\'' +
                ", commitTimestamp=" + commitTimestamp +
                ", transactionId=" + transactionId +
                '}';
    }

    /**
     * Transaction message implementation.
     */
    public static class TransactionMessageImpl implements TransactionMessage {
        private final Operation operation;
        private final Long transactionId;
        private final Instant commitTimestamp;

        public TransactionMessageImpl(Operation operation, Long transactionId, Instant commitTimestamp) {
            this.operation = operation;
            this.transactionId = transactionId;
            this.commitTimestamp = commitTimestamp;
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        public Instant getCommitTime() {
            return commitTimestamp;
        }

        @Override
        public OptionalLong getTransactionId() {
            return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
        }

        @Override
        public String getTable() {
            return null; // Transaction messages don't have tables
        }

        @Override
        public List<Column> getOldTupleList() {
            return null;
        }

        @Override
        public List<Column> getNewTupleList() {
            return null;
        }

        @Override
        public boolean isLastEventForLsn() {
            return true;
        }

        @Override
        public String toString() {
            return "TransactionMessage{" +
                    "operation=" + operation +
                    ", transactionId=" + transactionId +
                    ", commitTimestamp=" + commitTimestamp +
                    '}';
        }
    }

    /**
     * Logical decoding message implementation.
     */
    public static class LogicalDecodingMessageImpl implements LogicalDecodingMessage {
        private final Operation operation;
        private final Instant commitTimestamp;
        private final Long transactionId;
        private final boolean isTransactional;
        private final String prefix;
        private final byte[] content;

        public LogicalDecodingMessageImpl(Operation operation, Instant commitTimestamp, Long transactionId,
                                        boolean isTransactional, String prefix, byte[] content) {
            this.operation = operation;
            this.commitTimestamp = commitTimestamp;
            this.transactionId = transactionId;
            this.isTransactional = isTransactional;
            this.prefix = prefix;
            this.content = content;
        }

        @Override
        public boolean isTransactional() {
            return isTransactional;
        }

        @Override
        public String getPrefix() {
            return prefix;
        }

        @Override
        public byte[] getContent() {
            return content;
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        public Instant getCommitTime() {
            return commitTimestamp;
        }

        @Override
        public OptionalLong getTransactionId() {
            return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
        }

        @Override
        public String getTable() {
            return null;
        }

        @Override
        public List<Column> getOldTupleList() {
            return null;
        }

        @Override
        public List<Column> getNewTupleList() {
            return null;
        }

        @Override
        public boolean isLastEventForLsn() {
            return true;
        }

        @Override
        public String toString() {
            return "LogicalDecodingMessage{" +
                    "operation=" + operation +
                    ", isTransactional=" + isTransactional +
                    ", prefix='" + prefix + '\'' +
                    ", contentLength=" + (content != null ? content.length : 0) +
                    '}';
        }
    }

    /**
     * Truncate message implementation.
     */
    public static class TruncateMessageImpl extends PgOutputReplicationMessage implements TruncateMessage {
        private final boolean lastTableInTruncate;

        public TruncateMessageImpl(Operation operation, String table, Instant commitTimestamp,
                                 Long transactionId, boolean lastTableInTruncate) {
            super(operation, table, commitTimestamp, transactionId, null, null);
            this.lastTableInTruncate = lastTableInTruncate;
        }

        @Override
        public boolean isLastTableInTruncate() {
            return lastTableInTruncate;
        }

        @Override
        public String toString() {
            return "TruncateMessage{" +
                    "operation=" + getOperation() +
                    ", table='" + getTable() + '\'' +
                    ", lastTableInTruncate=" + lastTableInTruncate +
                    '}';
        }
    }
}
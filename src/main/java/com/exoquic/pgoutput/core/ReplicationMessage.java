/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.core;

import com.exoquic.pgoutput.config.Operation;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

public interface ReplicationMessage {

    /**
     * @return The type of operation (INSERT, UPDATE, DELETE, etc.)
     */
    Operation getOperation();

    /**
     * @return The commit timestamp of the transaction
     */
    Instant getCommitTime();

    /**
     * @return The transaction ID, or empty if not available
     */
    OptionalLong getTransactionId();

    /**
     * @return The fully qualified table name
     */
    String getTable();

    /**
     * @return List of old column values (for UPDATE/DELETE operations)
     */
    List<Column> getOldTupleList();

    /**
     * @return List of new column values (for INSERT/UPDATE operations)
     */
    List<Column> getNewTupleList();

    /**
     * @return true if this is the last event for the current LSN
     */
    boolean isLastEventForLsn();

    /**
     * Represents a column in a replication message.
     */
    interface Column {
        /**
         * @return The column name
         */
        String getName();

        /**
         * @return The PostgreSQL type of the column
         */
        PostgresType getType();

        /**
         * @return The type expression (including precision/scale)
         */
        String getTypeExpression();

        /**
         * @return true if the column allows NULL values
         */
        boolean isOptional();

        /**
         * @return The column value, or null if NULL
         */
        Object getValue();

        /**
         * @return true if the value is NULL
         */
        boolean isNull();

        /**
         * @return true if this is an unchanged TOAST value
         */
        boolean isToastedColumn();
    }

    /**
     * Represents transaction metadata (BEGIN/COMMIT).
     */
    interface TransactionMessage extends ReplicationMessage {
        // Transaction-specific methods can be added here if needed
    }

    /**
     * Represents a logical decoding message (PG 14+).
     */
    interface LogicalDecodingMessage extends ReplicationMessage {
        /**
         * @return true if this message is transactional
         */
        boolean isTransactional();

        /**
         * @return The message prefix
         */
        String getPrefix();

        /**
         * @return The message content
         */
        byte[] getContent();
    }

    /**
     * Represents a TRUNCATE operation.
     */
    interface TruncateMessage extends ReplicationMessage {
        /**
         * @return true if this is the last table in a multi-table truncate
         */
        boolean isLastTableInTruncate();
    }
}
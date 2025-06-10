/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.core;

import com.exoquic.pgoutput.config.Operation;
import com.exoquic.pgoutput.config.PgOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;

public class PgOutputMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgOutputMessageDecoder.class);
    private static final Instant PG_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    private final PgOutputConfig config;
    private final TypeRegistry typeRegistry;
    private final Map<Integer, TableMetadata> relationCache = new HashMap<>();

    private Instant commitTimestamp;
    private Long transactionId;

    /**
     * Enumeration of pgoutput message types.
     */
    public enum MessageType {
        RELATION('R'),
        BEGIN('B'),
        COMMIT('C'),
        INSERT('I'),
        UPDATE('U'),
        DELETE('D'),
        TYPE('Y'),
        ORIGIN('O'),
        TRUNCATE('T'),
        LOGICAL_DECODING_MESSAGE('M');

        private final char typeChar;

        MessageType(char typeChar) {
            this.typeChar = typeChar;
        }

        public static MessageType forType(char type) {
            for (MessageType messageType : values()) {
                if (messageType.typeChar == type) {
                    return messageType;
                }
            }
            throw new IllegalArgumentException("Unsupported message type: " + type);
        }
    }

    /**
     * Metadata for a table/relation.
     */
    public static class TableMetadata {
        private final int relationId;
        private final String schema;
        private final String table;
        private final List<ColumnMetadata> columns;
        private final List<String> primaryKeys;

        public TableMetadata(int relationId, String schema, String table, 
                           List<ColumnMetadata> columns, List<String> primaryKeys) {
            this.relationId = relationId;
            this.schema = schema;
            this.table = table;
            this.columns = columns;
            this.primaryKeys = primaryKeys;
        }

        public int getRelationId() { return relationId; }
        public String getSchema() { return schema; }
        public String getTable() { return table; }
        public List<ColumnMetadata> getColumns() { return columns; }
        public List<String> getPrimaryKeys() { return primaryKeys; }
        public String getFullTableName() { return schema + "." + table; }
    }

    /**
     * Metadata for a column.
     */
    public static class ColumnMetadata {
        private final String name;
        private final PostgresType type;
        private final boolean isKey;
        private final boolean isOptional;

        public ColumnMetadata(String name, PostgresType type, boolean isKey, boolean isOptional) {
            this.name = name;
            this.type = type;
            this.isKey = isKey;
            this.isOptional = isOptional;
        }

        public String getName() { return name; }
        public PostgresType getType() { return type; }
        public boolean isKey() { return isKey; }
        public boolean isOptional() { return isOptional; }
    }

    public PgOutputMessageDecoder(PgOutputConfig config, TypeRegistry typeRegistry) {
        this.config = Objects.requireNonNull(config, "Config is required");
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "TypeRegistry is required");
    }

    /**
     * Decode a message from the replication stream.
     * 
     * @param buffer The message buffer
     * @return Optional containing the decoded ReplicationMessage, or empty if the message should be skipped
     * @throws SQLException if database error occurs
     */
    public Optional<ReplicationMessage> decode(ByteBuffer buffer) throws SQLException {
        if (!buffer.hasRemaining()) {
            return Optional.empty();
        }

        final MessageType messageType = MessageType.forType((char) buffer.get());
        LOGGER.trace("Decoding message type: {}", messageType);

        switch (messageType) {
            case BEGIN:
                return Optional.ofNullable(handleBeginMessageAndReturnResult(buffer));
            case COMMIT:
                return Optional.ofNullable(handleCommitMessageAndReturnResult(buffer));
            case RELATION:
                handleRelationMessage(buffer);
                return Optional.empty(); // Relation messages don't produce output messages
            case LOGICAL_DECODING_MESSAGE:
                if (config.includeLogicalDecodingMessages()) {
                    return Optional.ofNullable(handleLogicalDecodingMessageAndReturnResult(buffer));
                }
                return Optional.empty();
            case INSERT:
                return Optional.ofNullable(decodeInsertAndReturnResult(buffer));
            case UPDATE:
                return Optional.ofNullable(decodeUpdateAndReturnResult(buffer));
            case DELETE:
                return Optional.ofNullable(decodeDeleteAndReturnResult(buffer));
            case TRUNCATE:
                if (!config.getSkippedOperations().contains(Operation.TRUNCATE)) {
                    return Optional.ofNullable(decodeTruncateAndReturnResult(buffer));
                }
                return Optional.empty();
            case TYPE:
            case ORIGIN:
                // Skip these message types
                LOGGER.trace("Skipping message type: {}", messageType);
                return Optional.empty();
            default:
                LOGGER.trace("Unknown message type: {}", messageType);
                return Optional.empty();
        }
    }

    /**
     * Process a message from the replication stream.
     * 
     * @param buffer The message buffer
     * @param messageHandler Handler for processed messages
     * @throws SQLException if database error occurs
     */
    public void processMessage(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) throws SQLException {
        Optional<ReplicationMessage> message = decode(buffer);
        message.ifPresent(messageHandler);
    }

    private ReplicationMessage handleBeginMessageAndReturnResult(ByteBuffer buffer) {
        final long lsn = buffer.getLong(); // LSN
        this.commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        this.transactionId = Integer.toUnsignedLong(buffer.getInt());
        
        LOGGER.trace("BEGIN: LSN={}, commitTimestamp={}, transactionId={}", lsn, commitTimestamp, transactionId);

        if (config.includeTransactionMetadata()) {
            return new PgOutputReplicationMessage.TransactionMessageImpl(
                    Operation.BEGIN, transactionId, commitTimestamp);
        }
        return null;
    }

    private void handleBeginMessage(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        ReplicationMessage message = handleBeginMessageAndReturnResult(buffer);
        if (message != null) {
            messageHandler.accept(message);
        }
    }

    private ReplicationMessage handleCommitMessageAndReturnResult(ByteBuffer buffer) {
        int flags = buffer.get(); // flags, currently unused
        final long lsn = buffer.getLong(); // LSN of the commit
        final long endLsn = buffer.getLong(); // End LSN of the transaction
        Instant commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        
        LOGGER.trace("COMMIT: flags={}, LSN={}, endLSN={}, commitTimestamp={}", flags, lsn, endLsn, commitTimestamp);

        if (config.includeTransactionMetadata()) {
            return new PgOutputReplicationMessage.TransactionMessageImpl(
                    Operation.COMMIT, transactionId, commitTimestamp);
        }
        return null;
    }

    private void handleCommitMessage(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        ReplicationMessage message = handleCommitMessageAndReturnResult(buffer);
        if (message != null) {
            messageHandler.accept(message);
        }
    }

    private void handleRelationMessage(ByteBuffer buffer) throws SQLException {
        int relationId = buffer.getInt();
        String schemaName = readString(buffer);
        String tableName = readString(buffer);
        int replicaIdentityId = buffer.get();
        short columnCount = buffer.getShort();

        LOGGER.trace("RELATION: relationId={}, schema={}, table={}, replicaIdentity={}, columns={}", 
                relationId, schemaName, tableName, replicaIdentityId, columnCount);

        // Check if table should be filtered
        if (!config.getTableFilter().matches(null, schemaName, tableName)) {
            LOGGER.trace("Table {}.{} filtered out", schemaName, tableName);
            // Still need to consume the rest of the message
            for (short i = 0; i < columnCount; i++) {
                buffer.get(); // flags
                readString(buffer); // column name
                buffer.getInt(); // column type
                buffer.getInt(); // attypmod
            }
            return;
        }

        // Get table metadata from database
        List<String> primaryKeys = getPrimaryKeyColumns(schemaName, tableName);
        Set<String> primaryKeySet = new HashSet<>(primaryKeys);

        List<ColumnMetadata> columns = new ArrayList<>();
        for (short i = 0; i < columnCount; i++) {
            byte flags = buffer.get();
            String columnName = readString(buffer);
            int columnTypeOid = buffer.getInt();
            int attypmod = buffer.getInt();

            // Check if column should be filtered
            if (!config.getColumnFilter().matches(null, schemaName, tableName, columnName)) {
                LOGGER.trace("Column {}.{}.{} filtered out", schemaName, tableName, columnName);
                continue;
            }

            PostgresType columnType = typeRegistry.get(columnTypeOid);
            boolean isKey = primaryKeySet.contains(columnName);
            boolean isOptional = true; // Default to optional, could be enhanced with actual nullability info

            columns.add(new ColumnMetadata(columnName, columnType, isKey, isOptional));
        }

        TableMetadata tableMetadata = new TableMetadata(relationId, schemaName, tableName, columns, primaryKeys);
        relationCache.put(relationId, tableMetadata);
        
        LOGGER.debug("Cached relation metadata for {}.{} (relationId={})", schemaName, tableName, relationId);
    }

    private ReplicationMessage decodeInsertAndReturnResult(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get(); // Always 'N' for inserts

        LOGGER.trace("INSERT: relationId={}, tupleType={}", relationId, tupleType);

        TableMetadata table = relationCache.get(relationId);
        if (table == null) {
            LOGGER.trace("Unknown relation ID: {}", relationId);
            return null;
        }

        List<ReplicationMessage.Column> columns = resolveColumnsFromTupleData(buffer, table);
        return new PgOutputReplicationMessage(
                Operation.INSERT,
                table.getFullTableName(),
                commitTimestamp,
                transactionId,
                null,
                columns);
    }

    private void decodeInsert(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        ReplicationMessage message = decodeInsertAndReturnResult(buffer);
        if (message != null) {
            messageHandler.accept(message);
        }
    }

    private ReplicationMessage decodeUpdateAndReturnResult(ByteBuffer buffer) {
        int relationId = buffer.getInt();

        LOGGER.trace("UPDATE: relationId={}", relationId);

        TableMetadata table = relationCache.get(relationId);
        if (table == null) {
            LOGGER.trace("Unknown relation ID: {}", relationId);
            return null;
        }

        List<ReplicationMessage.Column> oldColumns = null;
        char tupleType = (char) buffer.get();
        if ('O' == tupleType || 'K' == tupleType) {
            oldColumns = resolveColumnsFromTupleData(buffer, table);
            tupleType = (char) buffer.get(); // Read the 'N' tuple type
        }

        List<ReplicationMessage.Column> newColumns = resolveColumnsFromTupleData(buffer, table);
        return new PgOutputReplicationMessage(
                Operation.UPDATE,
                table.getFullTableName(),
                commitTimestamp,
                transactionId,
                oldColumns,
                newColumns);
    }

    private void decodeUpdate(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        ReplicationMessage message = decodeUpdateAndReturnResult(buffer);
        if (message != null) {
            messageHandler.accept(message);
        }
    }

    private ReplicationMessage decodeDeleteAndReturnResult(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get();

        LOGGER.trace("DELETE: relationId={}, tupleType={}", relationId, tupleType);

        TableMetadata table = relationCache.get(relationId);
        if (table == null) {
            LOGGER.trace("Unknown relation ID: {}", relationId);
            return null;
        }

        List<ReplicationMessage.Column> columns = resolveColumnsFromTupleData(buffer, table);
        return new PgOutputReplicationMessage(
                Operation.DELETE,
                table.getFullTableName(),
                commitTimestamp,
                transactionId,
                columns,
                null);
    }

    private void decodeDelete(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        ReplicationMessage message = decodeDeleteAndReturnResult(buffer);
        if (message != null) {
            messageHandler.accept(message);
        }
    }

    private ReplicationMessage decodeTruncateAndReturnResult(ByteBuffer buffer) {
        int numberOfRelations = buffer.getInt();
        int optionBits = buffer.get();
        int[] relationIds = new int[numberOfRelations];
        for (int i = 0; i < numberOfRelations; i++) {
            relationIds[i] = buffer.getInt();
        }

        LOGGER.trace("TRUNCATE: relations={}, optionBits={}", Arrays.toString(relationIds), optionBits);

        // For the single message API, return the first table's truncate message
        for (int relationId : relationIds) {
            TableMetadata table = relationCache.get(relationId);
            if (table != null) {
                return new PgOutputReplicationMessage.TruncateMessageImpl(
                        Operation.TRUNCATE,
                        table.getFullTableName(),
                        commitTimestamp,
                        transactionId,
                        true); // Mark as last table for single message API
            }
        }
        return null;
    }

    private void decodeTruncate(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        int numberOfRelations = buffer.getInt();
        int optionBits = buffer.get();
        int[] relationIds = new int[numberOfRelations];
        for (int i = 0; i < numberOfRelations; i++) {
            relationIds[i] = buffer.getInt();
        }

        LOGGER.trace("TRUNCATE: relations={}, optionBits={}", Arrays.toString(relationIds), optionBits);

        for (int i = 0; i < numberOfRelations; i++) {
            TableMetadata table = relationCache.get(relationIds[i]);
            if (table != null) {
                boolean lastTable = (i + 1) == numberOfRelations;
                messageHandler.accept(new PgOutputReplicationMessage.TruncateMessageImpl(
                        Operation.TRUNCATE,
                        table.getFullTableName(),
                        commitTimestamp,
                        transactionId,
                        lastTable));
            }
        }
    }

    private ReplicationMessage handleLogicalDecodingMessageAndReturnResult(ByteBuffer buffer) {
        boolean isTransactional = buffer.get() == 1;
        final long lsn = buffer.getLong();
        String prefix = readString(buffer);
        int contentLength = buffer.getInt();
        byte[] content = new byte[contentLength];
        buffer.get(content);

        // Non-transactional messages don't have transaction context
        Long txId = isTransactional ? transactionId : null;
        Instant timestamp = isTransactional ? commitTimestamp : null;

        LOGGER.trace("LOGICAL_DECODING_MESSAGE: transactional={}, LSN={}, prefix={}", isTransactional, lsn, prefix);

        return new PgOutputReplicationMessage.LogicalDecodingMessageImpl(
                Operation.MESSAGE,
                timestamp,
                txId,
                isTransactional,
                prefix,
                content);
    }

    private void handleLogicalDecodingMessage(ByteBuffer buffer, Consumer<ReplicationMessage> messageHandler) {
        ReplicationMessage message = handleLogicalDecodingMessageAndReturnResult(buffer);
        if (message != null) {
            messageHandler.accept(message);
        }
    }

    private List<ReplicationMessage.Column> resolveColumnsFromTupleData(ByteBuffer buffer, TableMetadata table) {
        short numberOfColumns = buffer.getShort();
        List<ReplicationMessage.Column> columns = new ArrayList<>(numberOfColumns);

        for (short i = 0; i < numberOfColumns && i < table.getColumns().size(); i++) {
            ColumnMetadata columnMeta = table.getColumns().get(i);
            char type = (char) buffer.get();

            switch (type) {
                case 't': // Text value
                    String valueStr = readColumnValueAsString(buffer);
                    Object value = convertValue(valueStr, columnMeta.getType());
                    columns.add(new PgOutputColumn(columnMeta.getName(), columnMeta.getType(),
                            columnMeta.getType().getName(), columnMeta.isOptional(), value));
                    break;
                case 'u': // Unchanged TOAST value
                    columns.add(new PgOutputColumn(columnMeta.getName(), columnMeta.getType(),
                            columnMeta.getType().getName(), columnMeta.isOptional(), null, true));
                    break;
                case 'n': // NULL value
                    columns.add(new PgOutputColumn(columnMeta.getName(), columnMeta.getType(),
                            columnMeta.getType().getName(), true, null));
                    break;
                default:
                    LOGGER.warn("Unsupported column type '{}' for column: '{}'", type, columnMeta.getName());
                    break;
            }
        }

        return columns;
    }

    private Object convertValue(String valueStr, PostgresType type) {
        if (valueStr == null) {
            return null;
        }

        try {
            switch (type.getName().toLowerCase()) {
                case "bool":
                case "boolean":
                    return "t".equalsIgnoreCase(valueStr);
                case "int2":
                case "smallint":
                    return Short.valueOf(valueStr);
                case "int4":
                case "integer":
                    return Integer.valueOf(valueStr);
                case "int8":
                case "bigint":
                    return Long.valueOf(valueStr);
                case "float4":
                case "real":
                    return Float.valueOf(valueStr);
                case "float8":
                case "double precision":
                    return Double.valueOf(valueStr);
                case "numeric":
                case "decimal":
                    return new java.math.BigDecimal(valueStr);
                case "bytea":
                    if (valueStr.startsWith("\\x")) {
                        return hexStringToByteArray(valueStr.substring(2));
                    }
                    return valueStr.getBytes();
                default:
                    return valueStr; // Return as string for other types
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to convert value '{}' for type '{}', returning as string", valueStr, type.getName());
            return valueStr;
        }
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private List<String> getPrimaryKeyColumns(String schema, String table) {
        try {
            DatabaseMetaData metaData = config.getDatabaseConnection().getMetaData();
            List<String> primaryKeys = new ArrayList<>();
            
            try (ResultSet rs = metaData.getPrimaryKeys(null, schema, table)) {
                while (rs.next()) {
                    primaryKeys.add(rs.getString("COLUMN_NAME"));
                }
            }
            
            return primaryKeys;
        } catch (SQLException e) {
            LOGGER.warn("Failed to get primary keys for {}.{}", schema, table, e);
            return Collections.emptyList();
        }
    }

    private static String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while ((b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    private static String readColumnValueAsString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] value = new byte[length];
        buffer.get(value, 0, length);
        return new String(value, StandardCharsets.UTF_8);
    }
}
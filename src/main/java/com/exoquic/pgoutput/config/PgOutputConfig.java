/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.config;

import java.sql.Connection;
import java.util.Collections;
import java.util.Set;

public class PgOutputConfig {
    
    private final Connection databaseConnection;
    private final String publicationName;
    private final Set<Operation> skippedOperations;
    private final boolean includeTransactionMetadata;
    private final boolean includeLogicalDecodingMessages;
    private final ColumnFilter columnFilter;
    private final TableFilter tableFilter;
    
    private PgOutputConfig(Builder builder) {
        if (builder.databaseConnection == null) {
            throw new IllegalStateException("Database connection is required");
        }
        if (builder.publicationName == null) {
            throw new IllegalStateException("Publication name is required");
        }
        this.databaseConnection = builder.databaseConnection;
        this.publicationName = builder.publicationName;
        this.skippedOperations = Set.copyOf(builder.skippedOperations);
        this.includeTransactionMetadata = builder.includeTransactionMetadata;
        this.includeLogicalDecodingMessages = builder.includeLogicalDecodingMessages;
        this.columnFilter = builder.columnFilter != null ? builder.columnFilter : ColumnFilter.includeAll();
        this.tableFilter = builder.tableFilter != null ? builder.tableFilter : TableFilter.includeAll();
    }
    
    /**
     * @return The database connection for metadata queries
     */
    public Connection getDatabaseConnection() {
        return databaseConnection;
    }
    
    /**
     * @return The PostgreSQL publication name to subscribe to
     */
    public String getPublicationName() {
        return publicationName;
    }
    
    /**
     * @return Set of operations that should be skipped during parsing
     */
    public Set<Operation> getSkippedOperations() {
        return skippedOperations;
    }
    
    /**
     * @return Whether to include transaction metadata (BEGIN/COMMIT messages)
     */
    public boolean includeTransactionMetadata() {
        return includeTransactionMetadata;
    }
    
    /**
     * @return Whether to include logical decoding messages (PG 14+)
     */
    public boolean includeLogicalDecodingMessages() {
        return includeLogicalDecodingMessages;
    }
    
    /**
     * @return Column filter for determining which columns to include
     */
    public ColumnFilter getColumnFilter() {
        return columnFilter;
    }
    
    /**
     * @return Table filter for determining which tables to include
     */
    public TableFilter getTableFilter() {
        return tableFilter;
    }
    
    /**
     * Create a new builder.
     * 
     * @return A new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for PgOutputConfig.
     */
    public static class Builder {
        private Connection databaseConnection;
        private String publicationName;
        private Set<Operation> skippedOperations = Collections.emptySet();
        private boolean includeTransactionMetadata = true;
        private boolean includeLogicalDecodingMessages = false;
        private ColumnFilter columnFilter;
        private TableFilter tableFilter;
        
        private Builder() {}
        
        /**
         * Set the database connection for metadata queries.
         * 
         * @param connection The database connection
         * @return This builder
         */
        public Builder databaseConnection(Connection connection) {
            this.databaseConnection = connection;
            return this;
        }
        
        /**
         * Set the PostgreSQL publication name to subscribe to.
         * 
         * @param publicationName The publication name
         * @return This builder
         */
        public Builder publicationName(String publicationName) {
            this.publicationName = publicationName;
            return this;
        }
        
        /**
         * Set operations that should be skipped during parsing.
         * 
         * @param operations Operations to skip
         * @return This builder
         */
        public Builder skipOperations(Operation... operations) {
            this.skippedOperations = Set.of(operations);
            return this;
        }
        
        /**
         * Set operations that should be skipped during parsing.
         * 
         * @param operations Operations to skip
         * @return This builder
         */
        public Builder skipOperations(Set<Operation> operations) {
            this.skippedOperations = Set.copyOf(operations);
            return this;
        }
        
        /**
         * Set whether to include transaction metadata (BEGIN/COMMIT messages).
         * Default is true.
         * 
         * @param include Whether to include transaction metadata
         * @return This builder
         */
        public Builder includeTransactionMetadata(boolean include) {
            this.includeTransactionMetadata = include;
            return this;
        }
        
        /**
         * Set whether to include logical decoding messages (PG 14+).
         * Default is false.
         * 
         * @param include Whether to include logical decoding messages
         * @return This builder
         */
        public Builder includeLogicalDecodingMessages(boolean include) {
            this.includeLogicalDecodingMessages = include;
            return this;
        }
        
        /**
         * Set the column filter for determining which columns to include.
         * 
         * @param filter The column filter
         * @return This builder
         */
        public Builder columnFilter(ColumnFilter filter) {
            this.columnFilter = filter;
            return this;
        }
        
        /**
         * Set the table filter for determining which tables to include.
         * 
         * @param filter The table filter
         * @return This builder
         */
        public Builder tableFilter(TableFilter filter) {
            this.tableFilter = filter;
            return this;
        }
        
        /**
         * Build the configuration.
         * 
         * @return A new PgOutputConfig instance
         */
        public PgOutputConfig build() {
            return new PgOutputConfig(this);
        }
    }
}
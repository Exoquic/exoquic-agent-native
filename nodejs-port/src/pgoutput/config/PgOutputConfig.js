import { ColumnFilter } from './ColumnFilter.js';
import { TableFilter } from './TableFilter.js';

export class PgOutputConfig {
  constructor(builder) {
    if (!builder.databaseConnection) {
      throw new Error('Database connection is required');
    }
    if (!builder.publicationName) {
      throw new Error('Publication name is required');
    }
    
    this.databaseConnection = builder.databaseConnection;
    this.publicationName = builder.publicationName;
    this.skippedOperations = new Set(builder.skippedOperations);
    this.includeTransactionMetadata = builder._includeTransactionMetadata;
    this.includeLogicalDecodingMessages = builder._includeLogicalDecodingMessages;
    this.columnFilter = builder.columnFilter || ColumnFilter.includeAll();
    this.tableFilter = builder.tableFilter || TableFilter.includeAll();
  }

  getDatabaseConnection() {
    return this.databaseConnection;
  }

  getPublicationName() {
    return this.publicationName;
  }

  getSkippedOperations() {
    return this.skippedOperations;
  }

  shouldIncludeTransactionMetadata() {
    return this.includeTransactionMetadata;
  }

  shouldIncludeLogicalDecodingMessages() {
    return this.includeLogicalDecodingMessages;
  }

  getColumnFilter() {
    return this.columnFilter;
  }

  getTableFilter() {
    return this.tableFilter;
  }

  static builder() {
    return new PgOutputConfigBuilder();
  }
}

export class PgOutputConfigBuilder {
  constructor() {
    this.skippedOperations = [];
    this._includeTransactionMetadata = true;
    this._includeLogicalDecodingMessages = false;
  }

  databaseConnection(connection) {
    this.databaseConnection = connection;
    return this;
  }

  publicationName(name) {
    this.publicationName = name;
    return this;
  }

  skipOperations(...operations) {
    this.skippedOperations = operations;
    return this;
  }

  includeTransactionMetadata(include = true) {
    this._includeTransactionMetadata = include;
    return this;
  }

  includeLogicalDecodingMessages(include = true) {
    this._includeLogicalDecodingMessages = include;
    return this;
  }

  columnFilter(filter) {
    this.columnFilter = filter;
    return this;
  }

  tableFilter(filter) {
    this.tableFilter = filter;
    return this;
  }

  build() {
    return new PgOutputConfig(this);
  }
}
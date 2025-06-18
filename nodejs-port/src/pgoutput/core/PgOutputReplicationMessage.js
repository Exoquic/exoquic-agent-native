import { ReplicationMessage } from './ReplicationMessage.js';

export class PgOutputReplicationMessage extends ReplicationMessage {
  constructor(operation, table, commitTimestamp, transactionId, oldColumns, newColumns) {
    super(operation, commitTimestamp, transactionId, table, oldColumns, newColumns);
  }

  toString() {
    return `PgOutputReplicationMessage{operation=${this.operation}, table='${this.table}', commitTimestamp=${this.commitTimestamp}, transactionId=${this.transactionId}}`;
  }
}

export class TransactionMessageImpl extends ReplicationMessage {
  constructor(operation, transactionId, commitTimestamp) {
    super(operation, commitTimestamp, transactionId, null, null, null);
  }

  getTable() {
    return null; // Transaction messages don't have tables
  }

  getOldTupleList() {
    return null;
  }

  getNewTupleList() {
    return null;
  }

  toString() {
    return `TransactionMessage{operation=${this.operation}, transactionId=${this.transactionId}, commitTimestamp=${this.commitTimestamp}}`;
  }
}

export class LogicalDecodingMessageImpl extends ReplicationMessage {
  constructor(operation, commitTimestamp, transactionId, isTransactional, prefix, content) {
    super(operation, commitTimestamp, transactionId, null, null, null);
    this.isTransactional = isTransactional;
    this.prefix = prefix;
    this.content = content;
  }

  isTransactional() {
    return this.isTransactional;
  }

  getPrefix() {
    return this.prefix;
  }

  getContent() {
    return this.content;
  }

  getTable() {
    return null;
  }

  getOldTupleList() {
    return null;
  }

  getNewTupleList() {
    return null;
  }

  toString() {
    return `LogicalDecodingMessage{operation=${this.operation}, isTransactional=${this.isTransactional}, prefix='${this.prefix}', contentLength=${this.content ? this.content.length : 0}}`;
  }
}

export class TruncateMessageImpl extends PgOutputReplicationMessage {
  constructor(operation, table, commitTimestamp, transactionId, lastTableInTruncate) {
    super(operation, table, commitTimestamp, transactionId, null, null);
    this.lastTableInTruncate = lastTableInTruncate;
  }

  isLastTableInTruncate() {
    return this.lastTableInTruncate;
  }

  toString() {
    return `TruncateMessage{operation=${this.getOperation()}, table='${this.getTable()}', lastTableInTruncate=${this.lastTableInTruncate}}`;
  }
}
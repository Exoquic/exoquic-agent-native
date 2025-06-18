import { Transaction } from './Transaction.js';

export class TransactionBuffer {
  constructor(onTransactionCommit) {
    this.activeTransactions = new Map();
    this.onTransactionCommit = onTransactionCommit;
    this.oldestActiveTransactionId = Number.MAX_SAFE_INTEGER;
  }

  handleBegin(transactionId, beginTime, lsn) {
    const transaction = new Transaction(transactionId, beginTime, lsn);
    this.activeTransactions.set(transactionId, transaction);
    
    if (transactionId < this.oldestActiveTransactionId) {
      this.oldestActiveTransactionId = transactionId;
    }
    
    console.debug(`Started transaction: ${transactionId} at LSN: ${lsn}`);
  }

  handleEvent(transactionId, message, lsn) {
    let transaction = this.activeTransactions.get(transactionId);
    if (!transaction) {
      console.warn(`Received event for unknown transaction: ${transactionId}. Creating implicit transaction.`);
      this.handleBegin(transactionId, new Date(), lsn);
      transaction = this.activeTransactions.get(transactionId);
    }
    
    transaction.addEvent(message, lsn);
    console.debug(`Added event to transaction: ${transactionId} (table: ${message.table}, operation: ${message.operation})`);
  }

  handleCommit(transactionId, commitTime, lsn) {
    const transaction = this.activeTransactions.get(transactionId);
    if (!transaction) {
      console.warn(`Received commit for unknown transaction: ${transactionId}`);
      return;
    }
    
    this.activeTransactions.delete(transactionId);
    transaction.commit(lsn, commitTime);
    
    console.info(`Committing transaction: ${transactionId} with ${transaction.getEventCount()} events at LSN: ${lsn}`);
    
    try {
      this.onTransactionCommit(transaction);
    } catch (error) {
      console.error(`Error processing committed transaction: ${transactionId}`, error);
    }
    
    this.updateOldestActiveTransaction();
  }

  handleRollback(transactionId) {
    const transaction = this.activeTransactions.get(transactionId);
    if (transaction) {
      console.info(`Rolling back transaction: ${transactionId} with ${transaction.getEventCount()} events`);
      this.activeTransactions.delete(transactionId);
    } else {
      console.warn(`Received rollback for unknown transaction: ${transactionId}`);
    }
    
    this.updateOldestActiveTransaction();
  }

  updateOldestActiveTransaction() {
    if (this.activeTransactions.size === 0) {
      this.oldestActiveTransactionId = Number.MAX_SAFE_INTEGER;
    } else {
      this.oldestActiveTransactionId = Math.min(...this.activeTransactions.keys());
    }
  }

  getActiveTransactionCount() {
    return this.activeTransactions.size;
  }

  getOldestActiveTransactionId() {
    return this.oldestActiveTransactionId === Number.MAX_SAFE_INTEGER ? -1 : this.oldestActiveTransactionId;
  }

  getActiveTransactionIds() {
    return Array.from(this.activeTransactions.keys()).sort((a, b) => a - b);
  }

  clear() {
    const count = this.activeTransactions.size;
    this.activeTransactions.clear();
    this.oldestActiveTransactionId = Number.MAX_SAFE_INTEGER;
    
    if (count > 0) {
      console.warn(`Cleared ${count} active transactions from buffer`);
    }
  }
}
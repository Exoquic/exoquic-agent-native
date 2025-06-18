export class Transaction {
  constructor(transactionId, beginTime, beginLsn) {
    this.transactionId = transactionId;
    this.beginTime = beginTime;
    this.beginLsn = beginLsn;
    this.events = [];
    this.commitLsn = null;
    this.commitTime = null;
    this.isCommitted = false;
  }

  addEvent(message, lsn) {
    if (this.isCommitted) {
      throw new Error(`Cannot add events to committed transaction: ${this.transactionId}`);
    }
    this.events.push(new TransactionEvent(message, lsn));
  }

  commit(commitLsn, commitTime) {
    this.commitLsn = commitLsn;
    this.commitTime = commitTime;
    this.isCommitted = true;
  }

  getOrderedEvents() {
    return this.events.slice().sort((a, b) => {
      // Compare LSN values for ordering
      return a.lsn.localeCompare(b.lsn);
    });
  }

  getTransactionId() {
    return this.transactionId;
  }

  getBeginTime() {
    return this.beginTime;
  }

  getBeginLsn() {
    return this.beginLsn;
  }

  getCommitLsn() {
    return this.commitLsn;
  }

  getCommitTime() {
    return this.commitTime;
  }

  getIsCommitted() {
    return this.isCommitted;
  }

  getEventCount() {
    return this.events.length;
  }
}

export class TransactionEvent {
  constructor(message, lsn) {
    this.message = message;
    this.lsn = lsn;
  }

  getMessage() {
    return this.message;
  }

  getLsn() {
    return this.lsn;
  }
}
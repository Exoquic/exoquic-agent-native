export class ReplicationMessage {
  constructor(operation, commitTime, transactionId, table, oldTupleList = [], newTupleList = []) {
    this.operation = operation;
    this.commitTime = commitTime;
    this.transactionId = transactionId;
    this.table = table;
    this.oldTupleList = oldTupleList;
    this.newTupleList = newTupleList;
    this.isLastEventForLsn = false;
  }

  getOperation() {
    return this.operation;
  }

  getCommitTime() {
    return this.commitTime;        
  }

  getTransactionId() {
    return this.transactionId;
  }

  getTable() {
    return this.table;
  }

  getOldTupleList() {
    return this.oldTupleList;
  }

  getNewTupleList() {
    return this.newTupleList;
  }

  isLastEventForLsn() {
    return this.isLastEventForLsn;
  }

  setLastEventForLsn(isLast) {
    this.isLastEventForLsn = isLast;
  }
}

export class Column {
  constructor(name, type, typeExpression, isOptional, value, isNull = false, isToastedColumn = false) {
    this.name = name;
    this.type = type;
    this.typeExpression = typeExpression;
    this.isOptional = isOptional;
    this.value = value;
    this.isNull = isNull;
    this.isToastedColumn = isToastedColumn;
  }

  getName() {
    return this.name;
  }

  getType() {
    return this.type;
  }

  getTypeExpression() {
    return this.typeExpression;
  }

  isOptional() {
    return this.isOptional;
  }

  getValue() {
    return this.value;
  }

  isNull() {
    return this.isNull;
  }

  isToastedColumn() {
    return this.isToastedColumn;
  }
}
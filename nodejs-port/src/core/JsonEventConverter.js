export class JsonEventConverter {
  convertToJson(message) {
    const eventMap = {};
    
    const operationType = this.mapOperationType(message.operation);
    eventMap.type = operationType;
    
    const dataMap = {};
    
    const columns = this.getColumnsForOperation(message);
    if (columns) {
      for (const column of columns) {
        if (column.isToastedColumn) {
          continue; // Skip unchanged TOAST columns
        }
        
        const columnName = column.name;
        const value = column.value;
        
        if (column.isNull) {
          dataMap[columnName] = null;
        } else {
          dataMap[columnName] = value;
        }
      }
    }
    
    eventMap.data = dataMap;
    
    return JSON.stringify(eventMap);
  }
  
  mapOperationType(operation) {
    switch (operation) {
      case 'INSERT':
        return 'created';
      case 'UPDATE':
        return 'updated';
      case 'DELETE':
        return 'deleted';
      default:
        return operation.toLowerCase();
    }
  }
  
  getColumnsForOperation(message) {
    switch (message.operation) {
      case 'INSERT':
      case 'UPDATE':
        return message.newTupleList;
      case 'DELETE':
        return message.oldTupleList;
      default:
        return null;
    }
  }
}
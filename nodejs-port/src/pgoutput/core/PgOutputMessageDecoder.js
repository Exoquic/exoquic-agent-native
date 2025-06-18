import { Operation } from '../config/Operation.js';
import { PgOutputReplicationMessage, TransactionMessageImpl, LogicalDecodingMessageImpl, TruncateMessageImpl } from './PgOutputReplicationMessage.js';
import { PgOutputColumn } from './PgOutputColumn.js';

export class PgOutputMessageDecoder {
  static PG_EPOCH = new Date('2000-01-01T00:00:00.000Z');

  constructor(config, typeRegistry) {
    this.config = config;
    this.typeRegistry = typeRegistry;
    this.relationCache = new Map();
    this.commitTimestamp = null;
    this.transactionId = null;
  }

  decode(buffer) {
    if (!buffer || buffer.length === 0) {
      return null;
    }

    const view = new DataView(buffer.buffer || buffer);
    let offset = 0;

    const messageType = String.fromCharCode(view.getUint8(offset));
    offset += 1;

    console.trace(`Decoding message type: ${messageType}`);

    switch (messageType) {
      case 'B': // BEGIN
        return this.handleBeginMessage(view, offset);
      case 'C': // COMMIT
        return this.handleCommitMessage(view, offset);
      case 'R': // RELATION
        this.handleRelationMessage(view, offset);
        return null; // Relation messages don't produce output
      case 'M': // LOGICAL_DECODING_MESSAGE
        if (this.config.shouldIncludeLogicalDecodingMessages()) {
          return this.handleLogicalDecodingMessage(view, offset);
        }
        return null;
      case 'I': // INSERT
        return this.decodeInsert(view, offset);
      case 'U': // UPDATE
        return this.decodeUpdate(view, offset);
      case 'D': // DELETE
        return this.decodeDelete(view, offset);
      case 'T': // TRUNCATE
        if (!this.config.getSkippedOperations().has(Operation.TRUNCATE)) {
          return this.decodeTruncate(view, offset);
        }
        return null;
      case 'Y': // TYPE
      case 'O': // ORIGIN
        console.trace(`Skipping message type: ${messageType}`);
        return null;
      default:
        console.trace(`Unknown message type: ${messageType}`);
        return null;
    }
  }

  handleBeginMessage(view, offset) {
    const lsn = this.readLSN(view, offset);
    offset += 8;
    
    const commitTimestampMicros = this.readBigInt64(view, offset);
    offset += 8;
    this.commitTimestamp = new Date(PgOutputMessageDecoder.PG_EPOCH.getTime() + Number(commitTimestampMicros) / 1000);
    
    this.transactionId = view.getUint32(offset);
    offset += 4;

    console.trace(`BEGIN: LSN=${lsn}, commitTimestamp=${this.commitTimestamp}, transactionId=${this.transactionId}`);

    if (this.config.shouldIncludeTransactionMetadata()) {
      return new TransactionMessageImpl(Operation.BEGIN, this.transactionId, this.commitTimestamp);
    }
    return null;
  }

  handleCommitMessage(view, offset) {
    const flags = view.getUint8(offset);
    offset += 1;
    
    const lsn = this.readLSN(view, offset);
    offset += 8;
    
    const endLsn = this.readLSN(view, offset);
    offset += 8;
    
    const commitTimestampMicros = this.readBigInt64(view, offset);
    offset += 8;
    const commitTimestamp = new Date(PgOutputMessageDecoder.PG_EPOCH.getTime() + Number(commitTimestampMicros) / 1000);

    console.trace(`COMMIT: flags=${flags}, LSN=${lsn}, endLSN=${endLsn}, commitTimestamp=${commitTimestamp}`);

    if (this.config.shouldIncludeTransactionMetadata()) {
      return new TransactionMessageImpl(Operation.COMMIT, this.transactionId, commitTimestamp);
    }
    return null;
  }

  handleRelationMessage(view, offset) {
    const relationId = view.getUint32(offset);
    offset += 4;
    
    const { value: schemaName, offset: newOffset1 } = this.readString(view, offset);
    offset = newOffset1;
    
    const { value: tableName, offset: newOffset2 } = this.readString(view, offset);
    offset = newOffset2;
    
    const replicaIdentityId = view.getUint8(offset);
    offset += 1;
    
    const columnCount = view.getUint16(offset);
    offset += 2;

    console.trace(`RELATION: relationId=${relationId}, schema=${schemaName}, table=${tableName}, replicaIdentity=${replicaIdentityId}, columns=${columnCount}`);

    // Check if table should be filtered
    if (!this.config.getTableFilter().shouldInclude(`${schemaName}.${tableName}`)) {
      console.trace(`Table ${schemaName}.${tableName} filtered out`);
      // Still need to consume the rest of the message
      for (let i = 0; i < columnCount; i++) {
        offset += 1; // flags
        const { offset: newOffset } = this.readString(view, offset); // column name
        offset = newOffset;
        offset += 4; // column type
        offset += 4; // attypmod
      }
      return;
    }

    const primaryKeys = []; // We'll simplify this for now
    const primaryKeySet = new Set(primaryKeys);

    const columns = [];
    for (let i = 0; i < columnCount; i++) {
      const flags = view.getUint8(offset);
      offset += 1;
      
      const { value: columnName, offset: newOffset3 } = this.readString(view, offset);
      offset = newOffset3;
      
      const columnTypeOid = view.getUint32(offset);
      offset += 4;
      
      const attypmod = view.getInt32(offset);
      offset += 4;

      // Check if column should be filtered
      if (!this.config.getColumnFilter().shouldInclude(`${schemaName}.${tableName}`, columnName)) {
        console.trace(`Column ${schemaName}.${tableName}.${columnName} filtered out`);
        continue;
      }

      const columnType = this.typeRegistry.get(columnTypeOid);
      const isKey = primaryKeySet.has(columnName);
      const isOptional = true; // Default to optional

      columns.push({
        name: columnName,
        type: columnType,
        isKey,
        isOptional
      });
    }

    const tableMetadata = {
      relationId,
      schema: schemaName,
      table: tableName,
      columns,
      primaryKeys,
      getFullTableName: () => `${schemaName}.${tableName}`
    };

    this.relationCache.set(relationId, tableMetadata);
    console.debug(`Cached relation metadata for ${schemaName}.${tableName} (relationId=${relationId})`);
  }

  decodeInsert(view, offset) {
    const relationId = view.getUint32(offset);
    offset += 4;
    
    const tupleType = String.fromCharCode(view.getUint8(offset)); // Always 'N' for inserts
    offset += 1;

    console.trace(`INSERT: relationId=${relationId}, tupleType=${tupleType}`);

    const table = this.relationCache.get(relationId);
    if (!table) {
      console.trace(`Unknown relation ID: ${relationId}`);
      return null;
    }

    const { columns } = this.resolveColumnsFromTupleData(view, offset, table);
    return new PgOutputReplicationMessage(
      Operation.INSERT,
      table.getFullTableName(),
      this.commitTimestamp,
      this.transactionId,
      null,
      columns
    );
  }

  decodeUpdate(view, offset) {
    const relationId = view.getUint32(offset);
    offset += 4;

    console.trace(`UPDATE: relationId=${relationId}`);

    const table = this.relationCache.get(relationId);
    if (!table) {
      console.trace(`Unknown relation ID: ${relationId}`);
      return null;
    }

    let oldColumns = null;
    let tupleType = String.fromCharCode(view.getUint8(offset));
    offset += 1;

    if (tupleType === 'O' || tupleType === 'K') {
      const result = this.resolveColumnsFromTupleData(view, offset, table);
      oldColumns = result.columns;
      offset = result.offset;
      tupleType = String.fromCharCode(view.getUint8(offset)); // Read the 'N' tuple type
      offset += 1;
    }

    const { columns: newColumns } = this.resolveColumnsFromTupleData(view, offset, table);
    return new PgOutputReplicationMessage(
      Operation.UPDATE,
      table.getFullTableName(),
      this.commitTimestamp,
      this.transactionId,
      oldColumns,
      newColumns
    );
  }

  decodeDelete(view, offset) {
    const relationId = view.getUint32(offset);
    offset += 4;
    
    const tupleType = String.fromCharCode(view.getUint8(offset));
    offset += 1;

    console.trace(`DELETE: relationId=${relationId}, tupleType=${tupleType}`);

    const table = this.relationCache.get(relationId);
    if (!table) {
      console.trace(`Unknown relation ID: ${relationId}`);
      return null;
    }

    const { columns } = this.resolveColumnsFromTupleData(view, offset, table);
    return new PgOutputReplicationMessage(
      Operation.DELETE,
      table.getFullTableName(),
      this.commitTimestamp,
      this.transactionId,
      columns,
      null
    );
  }

  decodeTruncate(view, offset) {
    const numberOfRelations = view.getUint32(offset);
    offset += 4;
    
    const optionBits = view.getUint8(offset);
    offset += 1;
    
    const relationIds = [];
    for (let i = 0; i < numberOfRelations; i++) {
      relationIds.push(view.getUint32(offset));
      offset += 4;
    }

    console.trace(`TRUNCATE: relations=${relationIds}, optionBits=${optionBits}`);

    // For simplicity, return the first table's truncate message
    for (const relationId of relationIds) {
      const table = this.relationCache.get(relationId);
      if (table) {
        return new TruncateMessageImpl(
          Operation.TRUNCATE,
          table.getFullTableName(),
          this.commitTimestamp,
          this.transactionId,
          true
        );
      }
    }
    return null;
  }

  handleLogicalDecodingMessage(view, offset) {
    const isTransactional = view.getUint8(offset) === 1;
    offset += 1;
    
    const lsn = this.readLSN(view, offset);
    offset += 8;
    
    const { value: prefix, offset: newOffset } = this.readString(view, offset);
    offset = newOffset;
    
    const contentLength = view.getUint32(offset);
    offset += 4;
    
    const content = new Uint8Array(view.buffer, offset, contentLength);

    const txId = isTransactional ? this.transactionId : null;
    const timestamp = isTransactional ? this.commitTimestamp : null;

    console.trace(`LOGICAL_DECODING_MESSAGE: transactional=${isTransactional}, LSN=${lsn}, prefix=${prefix}`);

    return new LogicalDecodingMessageImpl(
      Operation.MESSAGE,
      timestamp,
      txId,
      isTransactional,
      prefix,
      content
    );
  }

  resolveColumnsFromTupleData(view, offset, table) {
    const numberOfColumns = view.getUint16(offset);
    offset += 2;
    
    const columns = [];

    for (let i = 0; i < numberOfColumns && i < table.columns.length; i++) {
      const columnMeta = table.columns[i];
      const type = String.fromCharCode(view.getUint8(offset));
      offset += 1;

      switch (type) {
        case 't': // Text value
          const { value: valueStr, offset: newOffset } = this.readColumnValueAsString(view, offset);
          offset = newOffset;
          const value = this.convertValue(valueStr, columnMeta.type);
          columns.push(new PgOutputColumn(
            columnMeta.name,
            columnMeta.type,
            columnMeta.type.getName(),
            columnMeta.isOptional,
            value
          ));
          break;
        case 'u': // Unchanged TOAST value
          columns.push(new PgOutputColumn(
            columnMeta.name,
            columnMeta.type,
            columnMeta.type.getName(),
            columnMeta.isOptional,
            null,
            true
          ));
          break;
        case 'n': // NULL value
          columns.push(new PgOutputColumn(
            columnMeta.name,
            columnMeta.type,
            columnMeta.type.getName(),
            true,
            null
          ));
          break;
        default:
          console.warn(`Unsupported column type '${type}' for column: '${columnMeta.name}'`);
          break;
      }
    }

    return { columns, offset };
  }

  convertValue(valueStr, type) {
    if (valueStr === null) return null;

    try {
      const typeName = type.getName().toLowerCase();
      switch (typeName) {
        case 'bool':
        case 'boolean':
          return valueStr.toLowerCase() === 't';
        case 'int2':
        case 'smallint':
          return parseInt(valueStr, 10);
        case 'int4':
        case 'integer':
          return parseInt(valueStr, 10);
        case 'int8':
        case 'bigint':
          return parseInt(valueStr, 10);
        case 'float4':
        case 'real':
          return parseFloat(valueStr);
        case 'float8':
        case 'double precision':
          return parseFloat(valueStr);
        case 'numeric':
        case 'decimal':
          return parseFloat(valueStr);
        case 'bytea':
          if (valueStr.startsWith('\\x')) {
            return this.hexStringToByteArray(valueStr.substring(2));
          }
          return new TextEncoder().encode(valueStr);
        default:
          return valueStr; // Return as string for other types
      }
    } catch (error) {
      console.warn(`Failed to convert value '${valueStr}' for type '${type.getName()}', returning as string`);
      return valueStr;
    }
  }

  hexStringToByteArray(hexString) {
    const result = new Uint8Array(hexString.length / 2);
    for (let i = 0; i < hexString.length; i += 2) {
      result[i / 2] = parseInt(hexString.substr(i, 2), 16);
    }
    return result;
  }

  readString(view, offset) {
    let str = '';
    while (offset < view.byteLength) {
      const byte = view.getUint8(offset);
      if (byte === 0) {
        offset += 1;
        break;
      }
      str += String.fromCharCode(byte);
      offset += 1;
    }
    return { value: str, offset };
  }

  readColumnValueAsString(view, offset) {
    const length = view.getUint32(offset);
    offset += 4;
    
    const bytes = new Uint8Array(view.buffer, offset, length);
    const value = new TextDecoder('utf-8').decode(bytes);
    offset += length;
    
    return { value, offset };
  }

  readLSN(view, offset) {
    // LSN is a 64-bit value, return as string for simplicity
    const high = view.getUint32(offset);
    const low = view.getUint32(offset + 4);
    return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`;
  }

  readBigInt64(view, offset) {
    // Read 64-bit integer as BigInt
    const high = view.getUint32(offset);
    const low = view.getUint32(offset + 4);
    return BigInt(high) << 32n | BigInt(low);
  }
}
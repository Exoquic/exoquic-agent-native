import { PostgresType } from './PostgresType.js';

export class TypeRegistry {
  static NO_TYPE_MODIFIER = -1;
  static UNKNOWN_LENGTH = -1;
  static DOMAIN_TYPE = 2001; // Types.DISTINCT equivalent

  constructor(connection) {
    this.connection = connection;
    this.oidToType = new Map();
    this.nameToType = new Map();
    this.longTypeNames = this.getLongTypeNames();
  }

  static async create(connection) {
    const registry = new TypeRegistry(connection);
    await registry.loadTypes();
    return registry;
  }

  getLongTypeNames() {
    return new Map([
      ['bigint', 'int8'],
      ['bit varying', 'varbit'],
      ['boolean', 'bool'],
      ['character', 'bpchar'],
      ['character varying', 'varchar'],
      ['double precision', 'float8'],
      ['integer', 'int4'],
      ['real', 'float4'],
      ['smallint', 'int2'],
      ['timestamp without time zone', 'timestamp'],
      ['timestamp with time zone', 'timestamptz'],
      ['time without time zone', 'time'],
      ['time with time zone', 'timetz']
    ]);
  }

  get(oidOrName) {
    if (typeof oidOrName === 'number') {
      return this.getByOid(oidOrName);
    } else {
      return this.getByName(oidOrName);
    }
  }

  getByOid(oid) {
    let type = this.oidToType.get(oid);
    if (!type) {
      // Try to load dynamically (simplified for demo)
      type = PostgresType.UNKNOWN;
    }
    return type;
  }

  getByName(name) {
    if (!name) return PostgresType.UNKNOWN;

    const shortName = this.longTypeNames.get(name.toLowerCase()) || name;
    let type = this.nameToType.get(shortName);
    if (!type) {
      // Try to load dynamically (simplified for demo)
      type = PostgresType.UNKNOWN;
    }
    return type;
  }

  contains(oidOrName) {
    if (typeof oidOrName === 'number') {
      return this.oidToType.has(oidOrName);
    } else {
      if (!oidOrName) return false;
      const shortName = this.longTypeNames.get(oidOrName.toLowerCase()) || oidOrName;
      return this.nameToType.has(shortName);
    }
  }

  getTypeNames() {
    return new Set(this.nameToType.keys());
  }

  getTypeOids() {
    return new Set(this.oidToType.keys());
  }

  async loadTypes() {
    console.debug('Loading PostgreSQL types from database');

    const SQL_TYPES = `
      SELECT t.oid AS oid, t.typname AS name, t.typelem AS element, 
             t.typbasetype AS parentoid, t.typtypmod as modifiers, 
             t.typcategory as category
      FROM pg_catalog.pg_type t 
      JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) 
      WHERE n.nspname != 'pg_toast'
    `;

    try {
      const result = await this.connection.query(SQL_TYPES);
      const typeBuilders = new Map();
      const elementOids = new Map();
      const parentOids = new Map();

      // First pass: collect all types
      for (const row of result.rows) {
        const oid = row.oid;
        const name = row.name;
        const elementOid = row.element;
        const parentOid = row.parentoid;
        const category = row.category;

        const jdbcType = this.getJdbcType(name, category);
        const builder = PostgresType.builder(name, oid).jdbcType(jdbcType);

        typeBuilders.set(oid, builder);

        if (elementOid !== 0) {
          elementOids.set(oid, elementOid);
        }
        if (parentOid !== 0) {
          parentOids.set(oid, parentOid);
        }
      }

      // Second pass: resolve relationships and build types
      for (const [oid, builder] of typeBuilders) {
        // Set element type for arrays
        const elementOid = elementOids.get(oid);
        if (elementOid && typeBuilders.has(elementOid)) {
          const elementType = typeBuilders.get(elementOid).build();
          if (builder.elementType) {
            builder.elementType(elementType);
          }
        }

        // Set parent type for derived types
        const parentOid = parentOids.get(oid);
        if (parentOid && typeBuilders.has(parentOid)) {
          const parentType = typeBuilders.get(parentOid).build();
          if (builder.parentType) {
            builder.parentType(parentType);
          }
        }

        const type = builder.build();
        this.registerType(type);
      }

      console.debug(`Loaded ${this.oidToType.size} PostgreSQL types`);
    } catch (error) {
      console.warn('Failed to load types from database:', error);
      // Load basic types as fallback
      this.loadBasicTypes();
    }
  }

  loadBasicTypes() {
    const basicTypes = [
      ['bool', 16, 'BOOLEAN'],
      ['int2', 21, 'SMALLINT'],
      ['int4', 23, 'INTEGER'],
      ['int8', 20, 'BIGINT'],
      ['float4', 700, 'REAL'],
      ['float8', 701, 'DOUBLE'],
      ['numeric', 1700, 'NUMERIC'],
      ['varchar', 1043, 'VARCHAR'],
      ['text', 25, 'VARCHAR'],
      ['char', 1042, 'CHAR'],
      ['date', 1082, 'DATE'],
      ['time', 1083, 'TIME'],
      ['timestamp', 1114, 'TIMESTAMP'],
      ['timestamptz', 1184, 'TIMESTAMP_WITH_TIMEZONE'],
      ['bytea', 17, 'BINARY'],
      ['json', 114, 'OTHER'],
      ['jsonb', 3802, 'OTHER']
    ];

    for (const [name, oid, jdbcType] of basicTypes) {
      const type = PostgresType.builder(name, oid).jdbcType(jdbcType).build();
      this.registerType(type);
    }
  }

  registerType(type) {
    this.oidToType.set(type.getOid(), type);
    this.nameToType.set(type.getName(), type);
  }

  getJdbcType(typeName, category) {
    const name = typeName.toLowerCase();
    
    switch (name) {
      case 'bool':
      case 'boolean':
        return 'BOOLEAN';
      case 'int2':
      case 'smallint':
        return 'SMALLINT';
      case 'int4':
      case 'integer':
        return 'INTEGER';
      case 'int8':
      case 'bigint':
        return 'BIGINT';
      case 'float4':
      case 'real':
        return 'REAL';
      case 'float8':
      case 'double precision':
        return 'DOUBLE';
      case 'numeric':
      case 'decimal':
        return 'NUMERIC';
      case 'varchar':
      case 'character varying':
        return 'VARCHAR';
      case 'text':
        return 'VARCHAR';
      case 'char':
      case 'character':
      case 'bpchar':
        return 'CHAR';
      case 'date':
        return 'DATE';
      case 'time':
      case 'time without time zone':
        return 'TIME';
      case 'timetz':
      case 'time with time zone':
        return 'TIME_WITH_TIMEZONE';
      case 'timestamp':
      case 'timestamp without time zone':
        return 'TIMESTAMP';
      case 'timestamptz':
      case 'timestamp with time zone':
        return 'TIMESTAMP_WITH_TIMEZONE';
      case 'bytea':
        return 'BINARY';
      case 'json':
      case 'jsonb':
        return 'OTHER';
      default:
        if (category === 'A') return 'ARRAY';
        if (category === 'E') return 'VARCHAR'; // Enums as varchar
        return 'OTHER';
    }
  }
}
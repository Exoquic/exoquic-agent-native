export class PostgresType {
  static UNKNOWN = new PostgresType('unknown', -1, null, null, null, null, null);

  constructor(name, oid, jdbcId = null, enumValues = null, parentType = null, elementType = null, modifiers = null) {
    this.name = name;
    this.oid = oid;
    this.jdbcId = jdbcId;
    this.enumValues = enumValues;
    this.parentType = parentType;
    this.elementType = elementType;
    this.modifiers = modifiers;
  }

  isArrayType() {
    return this.elementType !== null;
  }

  isRootType() {
    return this.parentType === null;
  }

  isEnumType() {
    return this.enumValues !== null;
  }

  getName() {
    return this.name;
  }

  getOid() {
    return this.oid;
  }

  getJdbcId() {
    return this.jdbcId;
  }

  getElementType() {
    return this.elementType;
  }

  getParentType() {
    return this.parentType;
  }

  getRootType() {
    return this.isRootType() ? this : this.parentType.getRootType();
  }

  getEnumValues() {
    return this.enumValues;
  }

  getModifiers() {
    return this.modifiers;
  }

  equals(other) {
    if (this === other) return true;
    if (!other || other.constructor !== PostgresType) return false;
    return this.oid === other.oid && this.name === other.name;
  }

  toString() {
    return `PostgresType{name='${this.name}', oid=${this.oid}, jdbcId=${this.jdbcId}}`;
  }

  static builder(name, oid) {
    return new PostgresTypeBuilder(name, oid);
  }
}

class PostgresTypeBuilder {
  constructor(name, oid) {
    this.name = name;
    this.oid = oid;
    this.jdbcId = null;
    this.enumValues = null;
    this.parentType = null;
    this.elementType = null;
    this.modifiers = null;
  }

  jdbcType(jdbcId) {
    this.jdbcId = jdbcId;
    return this;
  }

  enumValues(values) {
    this.enumValues = values;
    return this;
  }

  parentType(parent) {
    this.parentType = parent;
    return this;
  }

  elementType(element) {
    this.elementType = element;
    return this;
  }

  modifiers(mods) {
    this.modifiers = mods;
    return this;
  }

  build() {
    return new PostgresType(
      this.name,
      this.oid,
      this.jdbcId,
      this.enumValues,
      this.parentType,
      this.elementType,
      this.modifiers
    );
  }
}
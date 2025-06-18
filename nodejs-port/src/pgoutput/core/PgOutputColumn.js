export class PgOutputColumn {
  constructor(name, type, typeExpression, optional, value, isToasted = false) {
    this.name = name;
    this.type = type;
    this.typeExpression = typeExpression;
    this.optional = optional;
    this.value = value;
    this.isToasted = isToasted;
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
    return this.optional;
  }

  getValue() {
    return this.value;
  }

  isNull() {
    return this.value === null;
  }

  isToastedColumn() {
    return this.isToasted;
  }

  asString() {
    return this.value !== null ? this.value.toString() : null;
  }

  asBoolean() {
    if (this.value === null) return null;
    if (typeof this.value === 'boolean') return this.value;
    if (typeof this.value === 'string') {
      const str = this.value.toLowerCase();
      return str === 't' || str === 'true';
    }
    throw new Error(`Cannot convert ${typeof this.value} to boolean`);
  }

  asInteger() {
    if (this.value === null) return null;
    if (typeof this.value === 'number') return Math.floor(this.value);
    if (typeof this.value === 'string') return parseInt(this.value, 10);
    throw new Error(`Cannot convert ${typeof this.value} to integer`);
  }

  asLong() {
    if (this.value === null) return null;
    if (typeof this.value === 'number') return Math.floor(this.value);
    if (typeof this.value === 'string') return parseInt(this.value, 10);
    throw new Error(`Cannot convert ${typeof this.value} to long`);
  }

  asFloat() {
    if (this.value === null) return null;
    if (typeof this.value === 'number') return this.value;
    if (typeof this.value === 'string') return parseFloat(this.value);
    throw new Error(`Cannot convert ${typeof this.value} to float`);
  }

  asDouble() {
    return this.asFloat(); // JavaScript numbers are all double precision
  }

  asDecimal() {
    if (this.value === null) return null;
    if (typeof this.value === 'number') return this.value;
    if (typeof this.value === 'string') return parseFloat(this.value);
    throw new Error(`Cannot convert ${typeof this.value} to decimal`);
  }

  asByteArray() {
    if (this.value === null) return null;
    if (this.value instanceof Uint8Array) return this.value;
    if (typeof this.value === 'string') {
      if (this.value.startsWith('\\x')) {
        return this.hexStringToByteArray(this.value.substring(2));
      }
      return new TextEncoder().encode(this.value);
    }
    throw new Error(`Cannot convert ${typeof this.value} to byte array`);
  }

  hexStringToByteArray(hexString) {
    const result = new Uint8Array(hexString.length / 2);
    for (let i = 0; i < hexString.length; i += 2) {
      result[i / 2] = parseInt(hexString.substr(i, 2), 16);
    }
    return result;
  }

  equals(other) {
    if (this === other) return true;
    if (!other || other.constructor !== PgOutputColumn) return false;
    return this.name === other.name &&
           this.optional === other.optional &&
           this.isToasted === other.isToasted &&
           this.type === other.type &&
           this.typeExpression === other.typeExpression &&
           this.value === other.value;
  }

  toString() {
    if (this.isToasted) {
      return `${this.name}(${this.typeExpression}) - Unchanged TOAST value`;
    }
    return `${this.name}(${this.typeExpression})=${this.value}`;
  }
}
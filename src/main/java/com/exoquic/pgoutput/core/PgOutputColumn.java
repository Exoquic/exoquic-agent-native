/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.core;

import java.math.BigDecimal;
import java.util.Objects;

public class PgOutputColumn implements ReplicationMessage.Column {

    private final String name;
    private final PostgresType type;
    private final String typeExpression;
    private final boolean optional;
    private final Object value;
    private final boolean isToasted;

    public PgOutputColumn(String name, PostgresType type, String typeExpression, boolean optional, Object value) {
        this(name, type, typeExpression, optional, value, false);
    }

    public PgOutputColumn(String name, PostgresType type, String typeExpression, boolean optional, Object value, boolean isToasted) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.typeExpression = typeExpression;
        this.optional = optional;
        this.value = value;
        this.isToasted = isToasted;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PostgresType getType() {
        return type;
    }

    @Override
    public String getTypeExpression() {
        return typeExpression;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public boolean isToastedColumn() {
        return isToasted;
    }

    /**
     * Get the value as a string.
     * 
     * @return The value as string, or null if null
     */
    public String asString() {
        return value != null ? value.toString() : null;
    }

    /**
     * Get the value as a boolean.
     * 
     * @return The value as boolean
     * @throws IllegalStateException if value is null or not convertible
     */
    public Boolean asBoolean() {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            String str = (String) value;
            return "t".equalsIgnoreCase(str) || "true".equalsIgnoreCase(str);
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to boolean");
    }

    /**
     * Get the value as an integer.
     * 
     * @return The value as integer
     * @throws IllegalStateException if value is null or not convertible
     */
    public Integer asInteger() {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.valueOf((String) value);
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to integer");
    }

    /**
     * Get the value as a long.
     * 
     * @return The value as long
     * @throws IllegalStateException if value is null or not convertible
     */
    public Long asLong() {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.valueOf((String) value);
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to long");
    }

    /**
     * Get the value as a float.
     * 
     * @return The value as float
     * @throws IllegalStateException if value is null or not convertible
     */
    public Float asFloat() {
        if (value == null) {
            return null;
        }
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof String) {
            return Float.valueOf((String) value);
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to float");
    }

    /**
     * Get the value as a double.
     * 
     * @return The value as double
     * @throws IllegalStateException if value is null or not convertible
     */
    public Double asDouble() {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.valueOf((String) value);
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to double");
    }

    /**
     * Get the value as a BigDecimal.
     * 
     * @return The value as BigDecimal
     * @throws IllegalStateException if value is null or not convertible
     */
    public BigDecimal asDecimal() {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to BigDecimal");
    }

    /**
     * Get the value as a byte array.
     * 
     * @return The value as byte array
     * @throws IllegalStateException if value is null or not convertible
     */
    public byte[] asByteArray() {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            String str = (String) value;
            if (str.startsWith("\\x")) {
                return hexStringToByteArray(str.substring(2));
            }
            return str.getBytes();
        }
        throw new IllegalStateException("Cannot convert " + value.getClass() + " to byte array");
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PgOutputColumn that = (PgOutputColumn) o;
        return optional == that.optional &&
                isToasted == that.isToasted &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(typeExpression, that.typeExpression) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, typeExpression, optional, value, isToasted);
    }

    @Override
    public String toString() {
        if (isToasted) {
            return name + "(" + typeExpression + ") - Unchanged TOAST value";
        }
        return name + "(" + typeExpression + ")=" + value;
    }
}
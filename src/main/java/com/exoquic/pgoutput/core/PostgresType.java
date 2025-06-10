/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.core;

import org.postgresql.core.TypeInfo;

import java.sql.Types;
import java.util.List;
import java.util.Objects;

public class PostgresType {

    public static final PostgresType UNKNOWN = new PostgresType("unknown", -1, Integer.MIN_VALUE, null, null, null, null);

    private final String name;
    private final int oid;
    private final int jdbcId;
    private final PostgresType parentType;
    private final PostgresType elementType;
    private final TypeInfo typeInfo;
    private final int modifiers;
    private final List<String> enumValues;

    private PostgresType(String name, int oid, int jdbcId, TypeInfo typeInfo, List<String> enumValues, PostgresType parentType, PostgresType elementType) {
        this(name, oid, jdbcId, TypeRegistry.NO_TYPE_MODIFIER, typeInfo, enumValues, parentType, elementType);
    }

    private PostgresType(String name, int oid, int jdbcId, int modifiers, TypeInfo typeInfo, List<String> enumValues, PostgresType parentType, PostgresType elementType) {
        Objects.requireNonNull(name);
        this.name = name;
        this.oid = oid;
        this.jdbcId = jdbcId;
        this.typeInfo = typeInfo;
        this.parentType = parentType;
        this.elementType = elementType;
        this.modifiers = modifiers;
        this.enumValues = enumValues;
    }

    /**
     * @return true if this type is an array
     */
    public boolean isArrayType() {
        return elementType != null;
    }

    /**
     * The type system allows for the creation of user defined types (UDTs) which can be based
     * on any existing type. When a type does not extend another type, it is considered to be
     * a base or root type in the type hierarchy.
     *
     * @return true if this type is a base/root type
     */
    public boolean isRootType() {
        return parentType == null;
    }

    /**
     * @return true if this type is an enum type
     */
    public boolean isEnumType() {
        return enumValues != null;
    }

    /**
     * @return symbolic name of the type
     */
    public String getName() {
        return name;
    }

    /**
     * @return PostgreSQL OID of the type
     */
    public int getOid() {
        return oid;
    }

    /**
     * @return JDBC type id of the type
     */
    public int getJdbcId() {
        return jdbcId;
    }

    /**
     * @return type info from PostgreSQL driver
     */
    public TypeInfo getTypeInfo() {
        return typeInfo;
    }

    /**
     * @return element type for array types, null otherwise
     */
    public PostgresType getElementType() {
        return elementType;
    }

    /**
     * @return parent type for derived types, null for root types
     */
    public PostgresType getParentType() {
        return parentType;
    }

    /**
     * @return root type in the inheritance hierarchy
     */
    public PostgresType getRootType() {
        return isRootType() ? this : parentType.getRootType();
    }

    /**
     * @return enum values for enum types, null otherwise
     */
    public List<String> getEnumValues() {
        return enumValues;
    }

    /**
     * @return type modifiers
     */
    public int getModifiers() {
        return modifiers;
    }

    /**
     * @return default length for this type
     */
    public int getDefaultLength() {
        if (typeInfo != null) {
            return typeInfo.getDisplaySize(oid, TypeRegistry.NO_TYPE_MODIFIER);
        }
        return TypeRegistry.UNKNOWN_LENGTH;
    }

    /**
     * @return default scale for this type
     */
    public int getDefaultScale() {
        if (typeInfo != null) {
            return typeInfo.getScale(oid, TypeRegistry.NO_TYPE_MODIFIER);
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PostgresType that = (PostgresType) obj;
        return oid == that.oid && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, oid);
    }

    @Override
    public String toString() {
        return "PostgresType{" +
                "name='" + name + '\'' +
                ", oid=" + oid +
                ", jdbcId=" + jdbcId +
                '}';
    }

    /**
     * Builder for creating PostgresType instances.
     */
    public static class Builder {
        private String name;
        private int oid;
        private int jdbcId = Types.OTHER;
        private int modifiers = TypeRegistry.NO_TYPE_MODIFIER;
        private TypeInfo typeInfo;
        private List<String> enumValues;
        private PostgresType parentType;
        private PostgresType elementType;

        public Builder(String name, int oid) {
            this.name = Objects.requireNonNull(name);
            this.oid = oid;
        }

        public Builder jdbcType(int jdbcId) {
            this.jdbcId = jdbcId;
            return this;
        }

        public Builder modifiers(int modifiers) {
            this.modifiers = modifiers;
            return this;
        }

        public Builder typeInfo(TypeInfo typeInfo) {
            this.typeInfo = typeInfo;
            return this;
        }

        public Builder enumValues(List<String> enumValues) {
            this.enumValues = enumValues;
            return this;
        }

        public Builder parentType(PostgresType parentType) {
            this.parentType = parentType;
            return this;
        }

        public Builder elementType(PostgresType elementType) {
            this.elementType = elementType;
            return this;
        }

        public PostgresType build() {
            return new PostgresType(name, oid, jdbcId, modifiers, typeInfo, enumValues, parentType, elementType);
        }
    }
}
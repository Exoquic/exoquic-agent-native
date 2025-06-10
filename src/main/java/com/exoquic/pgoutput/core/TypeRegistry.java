/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.core;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class TypeRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeRegistry.class);

    public static final String TYPE_NAME_GEOGRAPHY = "geography";
    public static final String TYPE_NAME_GEOMETRY = "geometry";
    public static final String TYPE_NAME_CITEXT = "citext";
    public static final String TYPE_NAME_HSTORE = "hstore";
    public static final String TYPE_NAME_LTREE = "ltree";
    public static final String TYPE_NAME_ISBN = "isbn";
    public static final String TYPE_NAME_VECTOR = "vector";
    public static final String TYPE_NAME_HALF_VECTOR = "halfvec";
    public static final String TYPE_NAME_SPARSE_VECTOR = "sparsevec";

    public static final String TYPE_NAME_HSTORE_ARRAY = "_hstore";
    public static final String TYPE_NAME_GEOGRAPHY_ARRAY = "_geography";
    public static final String TYPE_NAME_GEOMETRY_ARRAY = "_geometry";
    public static final String TYPE_NAME_CITEXT_ARRAY = "_citext";
    public static final String TYPE_NAME_LTREE_ARRAY = "_ltree";

    public static final int NO_TYPE_MODIFIER = -1;
    public static final int UNKNOWN_LENGTH = -1;

    // PostgreSQL driver reports user-defined Domain types as Types.DISTINCT
    public static final int DOMAIN_TYPE = Types.DISTINCT;

    private static final String CATEGORY_ARRAY = "A";
    private static final String CATEGORY_ENUM = "E";

    private static final String SQL_ENUM_VALUES = "SELECT t.enumtypid as id, array_agg(t.enumlabel) as values "
            + "FROM pg_catalog.pg_enum t GROUP BY id";

    private static final String SQL_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typelem AS element, t.typbasetype AS parentoid, t.typtypmod as modifiers, t.typcategory as category, e.values as enum_values "
            + "FROM pg_catalog.pg_type t "
            + "JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "LEFT JOIN (" + SQL_ENUM_VALUES + ") e ON (t.oid = e.id) "
            + "WHERE n.nspname != 'pg_toast'";

    private static final String SQL_NAME_LOOKUP = SQL_TYPES + " AND t.typname = ?";
    private static final String SQL_OID_LOOKUP = SQL_TYPES + " AND t.oid = ?";

    private static final Map<String, String> LONG_TYPE_NAMES = Collections.unmodifiableMap(getLongTypeNames());

    private static Map<String, String> getLongTypeNames() {
        Map<String, String> longTypeNames = new HashMap<>();
        longTypeNames.put("bigint", "int8");
        longTypeNames.put("bit varying", "varbit");
        longTypeNames.put("boolean", "bool");
        longTypeNames.put("character", "bpchar");
        longTypeNames.put("character varying", "varchar");
        longTypeNames.put("double precision", "float8");
        longTypeNames.put("integer", "int4");
        longTypeNames.put("real", "float4");
        longTypeNames.put("smallint", "int2");
        longTypeNames.put("timestamp without time zone", "timestamp");
        longTypeNames.put("timestamp with time zone", "timestamptz");
        longTypeNames.put("time without time zone", "time");
        longTypeNames.put("time with time zone", "timetz");
        return longTypeNames;
    }

    private final Map<Integer, PostgresType> oidToType = new HashMap<>();
    private final Map<String, PostgresType> nameToType = new HashMap<>();
    private final Connection connection;
    private final TypeInfo typeInfo;

    /**
     * Create a type registry from a database connection.
     * 
     * @param connection The database connection
     * @return A new type registry
     * @throws SQLException if database error occurs
     */
    public static TypeRegistry create(Connection connection) throws SQLException {
        return new TypeRegistry(connection);
    }

    private TypeRegistry(Connection connection) throws SQLException {
        this.connection = connection;
        
        // Get TypeInfo from PostgreSQL connection
        if (connection.isWrapperFor(BaseConnection.class)) {
            BaseConnection baseConnection = connection.unwrap(BaseConnection.class);
            this.typeInfo = baseConnection.getTypeInfo();
        } else {
            this.typeInfo = null;
            LOGGER.warn("Connection is not a PostgreSQL BaseConnection, type info will be limited");
        }
        
        loadTypes();
    }

    /**
     * Get a type by its OID.
     * 
     * @param oid The type OID
     * @return The PostgresType or UNKNOWN if not found
     */
    public PostgresType get(int oid) {
        PostgresType type = oidToType.get(oid);
        if (type == null) {
            // Try to load the type dynamically
            type = loadTypeByOid(oid);
        }
        return type != null ? type : PostgresType.UNKNOWN;
    }

    /**
     * Get a type by its name.
     * 
     * @param name The type name
     * @return The PostgresType or UNKNOWN if not found
     */
    public PostgresType get(String name) {
        if (name == null) {
            return PostgresType.UNKNOWN;
        }

        // Normalize long type names to short names
        String shortName = LONG_TYPE_NAMES.getOrDefault(name.toLowerCase(), name);
        
        PostgresType type = nameToType.get(shortName);
        if (type == null) {
            // Try to load the type dynamically
            type = loadTypeByName(shortName);
        }
        return type != null ? type : PostgresType.UNKNOWN;
    }

    /**
     * Check if the registry contains a type with the given OID.
     * 
     * @param oid The type OID
     * @return true if the type exists
     */
    public boolean contains(int oid) {
        return oidToType.containsKey(oid);
    }

    /**
     * Check if the registry contains a type with the given name.
     * 
     * @param name The type name
     * @return true if the type exists
     */
    public boolean contains(String name) {
        if (name == null) {
            return false;
        }
        String shortName = LONG_TYPE_NAMES.getOrDefault(name.toLowerCase(), name);
        return nameToType.containsKey(shortName);
    }

    /**
     * Get all registered type names.
     * 
     * @return Set of type names
     */
    public Set<String> getTypeNames() {
        return Collections.unmodifiableSet(nameToType.keySet());
    }

    /**
     * Get all registered type OIDs.
     * 
     * @return Set of type OIDs
     */
    public Set<Integer> getTypeOids() {
        return Collections.unmodifiableSet(oidToType.keySet());
    }

    private void loadTypes() throws SQLException {
        LOGGER.debug("Loading PostgreSQL types from database");
        
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SQL_TYPES)) {
            
            Map<Integer, PostgresType.Builder> typeBuilders = new HashMap<>();
            Map<Integer, Integer> elementOids = new HashMap<>();
            Map<Integer, Integer> parentOids = new HashMap<>();
            
            // First pass: collect all types
            while (rs.next()) {
                int oid = rs.getInt("oid");
                String name = rs.getString("name");
                int elementOid = rs.getInt("element");
                int parentOid = rs.getInt("parentoid");
                String category = rs.getString("category");
                Array enumArray = rs.getArray("enum_values");
                
                int jdbcType = getJdbcType(name, category);
                
                PostgresType.Builder builder = new PostgresType.Builder(name, oid)
                        .jdbcType(jdbcType)
                        .typeInfo(typeInfo);
                
                // Handle enum values
                if (enumArray != null) {
                    String[] enumValues = (String[]) enumArray.getArray();
                    builder.enumValues(Arrays.asList(enumValues));
                }
                
                typeBuilders.put(oid, builder);
                
                if (elementOid != 0) {
                    elementOids.put(oid, elementOid);
                }
                if (parentOid != 0) {
                    parentOids.put(oid, parentOid);
                }
            }
            
            // Second pass: resolve relationships and build types
            for (Map.Entry<Integer, PostgresType.Builder> entry : typeBuilders.entrySet()) {
                int oid = entry.getKey();
                PostgresType.Builder builder = entry.getValue();
                
                // Set element type for arrays
                Integer elementOid = elementOids.get(oid);
                if (elementOid != null && typeBuilders.containsKey(elementOid)) {
                    PostgresType elementType = typeBuilders.get(elementOid).build();
                    builder.elementType(elementType);
                }
                
                // Set parent type for derived types
                Integer parentOid = parentOids.get(oid);
                if (parentOid != null && typeBuilders.containsKey(parentOid)) {
                    PostgresType parentType = typeBuilders.get(parentOid).build();
                    builder.parentType(parentType);
                }
                
                PostgresType type = builder.build();
                registerType(type);
            }
        }
        
        LOGGER.debug("Loaded {} PostgreSQL types", oidToType.size());
    }

    private PostgresType loadTypeByOid(int oid) {
        try (PreparedStatement ps = connection.prepareStatement(SQL_OID_LOOKUP)) {
            ps.setInt(1, oid);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return buildTypeFromResultSet(rs);
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to load type by OID {}", oid, e);
        }
        return null;
    }

    private PostgresType loadTypeByName(String name) {
        try (PreparedStatement ps = connection.prepareStatement(SQL_NAME_LOOKUP)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return buildTypeFromResultSet(rs);
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Failed to load type by name {}", name, e);
        }
        return null;
    }

    private PostgresType buildTypeFromResultSet(ResultSet rs) throws SQLException {
        int oid = rs.getInt("oid");
        String name = rs.getString("name");
        int elementOid = rs.getInt("element");
        int parentOid = rs.getInt("parentoid");
        String category = rs.getString("category");
        Array enumArray = rs.getArray("enum_values");
        
        int jdbcType = getJdbcType(name, category);
        
        PostgresType.Builder builder = new PostgresType.Builder(name, oid)
                .jdbcType(jdbcType)
                .typeInfo(typeInfo);
        
        // Handle enum values
        if (enumArray != null) {
            String[] enumValues = (String[]) enumArray.getArray();
            builder.enumValues(Arrays.asList(enumValues));
        }
        
        // Handle element type
        if (elementOid != 0) {
            PostgresType elementType = get(elementOid);
            if (elementType != PostgresType.UNKNOWN) {
                builder.elementType(elementType);
            }
        }
        
        // Handle parent type
        if (parentOid != 0) {
            PostgresType parentType = get(parentOid);
            if (parentType != PostgresType.UNKNOWN) {
                builder.parentType(parentType);
            }
        }
        
        PostgresType type = builder.build();
        registerType(type);
        return type;
    }

    private void registerType(PostgresType type) {
        oidToType.put(type.getOid(), type);
        nameToType.put(type.getName(), type);
    }

    private int getJdbcType(String typeName, String category) {
        // Map common PostgreSQL types to JDBC types
        switch (typeName.toLowerCase()) {
            case "bool":
            case "boolean":
                return Types.BOOLEAN;
            case "int2":
            case "smallint":
                return Types.SMALLINT;
            case "int4":
            case "integer":
                return Types.INTEGER;
            case "int8":
            case "bigint":
                return Types.BIGINT;
            case "float4":
            case "real":
                return Types.REAL;
            case "float8":
            case "double precision":
                return Types.DOUBLE;
            case "numeric":
            case "decimal":
                return Types.NUMERIC;
            case "varchar":
            case "character varying":
                return Types.VARCHAR;
            case "text":
                return Types.VARCHAR;
            case "char":
            case "character":
            case "bpchar":
                return Types.CHAR;
            case "date":
                return Types.DATE;
            case "time":
            case "time without time zone":
                return Types.TIME;
            case "timetz":
            case "time with time zone":
                return Types.TIME_WITH_TIMEZONE;
            case "timestamp":
            case "timestamp without time zone":
                return Types.TIMESTAMP;
            case "timestamptz":
            case "timestamp with time zone":
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case "bytea":
                return Types.BINARY;
            case "json":
            case "jsonb":
                return Types.OTHER;
            default:
                if (CATEGORY_ARRAY.equals(category)) {
                    return Types.ARRAY;
                } else if (CATEGORY_ENUM.equals(category)) {
                    return Types.VARCHAR; // Treat enums as varchar
                }
                return Types.OTHER;
        }
    }
}
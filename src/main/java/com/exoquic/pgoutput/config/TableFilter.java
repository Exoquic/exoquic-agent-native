/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.config;

import java.util.function.Predicate;
import java.util.regex.Pattern;

@FunctionalInterface
public interface TableFilter {
    
    /**
     * Test whether a table should be included.
     * 
     * @param catalog The catalog name (typically null for PostgreSQL)
     * @param schema The schema name
     * @param table The table name
     * @return true if the table should be included
     */
    boolean matches(String catalog, String schema, String table);
    
    /**
     * Create a filter that includes all tables.
     * 
     * @return A filter that includes all tables
     */
    static TableFilter includeAll() {
        return (catalog, schema, table) -> true;
    }
    
    /**
     * Create a filter that excludes all tables.
     * 
     * @return A filter that excludes all tables
     */
    static TableFilter excludeAll() {
        return (catalog, schema, table) -> false;
    }
    
    /**
     * Create a filter for a specific schema.
     * 
     * @param schemaName The schema name to include
     * @return A table filter
     */
    static TableFilter forSchema(String schemaName) {
        return (catalog, schema, table) -> schemaName.equals(schema);
    }
    
    /**
     * Create a filter based on a predicate function.
     * 
     * @param predicate The predicate function that takes "schema.table" format
     * @return A table filter
     */
    static TableFilter fromPredicate(Predicate<String> predicate) {
        return (catalog, schema, table) -> predicate.test(schema + "." + table);
    }
    
    /**
     * Create a filter that includes tables matching a regular expression.
     * The pattern is applied to "schema.table" format.
     * 
     * @param pattern The regular expression pattern
     * @return A table filter
     */
    static TableFilter includePattern(String pattern) {
        Pattern regex = Pattern.compile(pattern);
        return (catalog, schema, table) -> regex.matcher(schema + "." + table).matches();
    }
    
    /**
     * Create a filter that excludes tables matching a regular expression.
     * The pattern is applied to "schema.table" format.
     * 
     * @param pattern The regular expression pattern
     * @return A table filter
     */
    static TableFilter excludePattern(String pattern) {
        Pattern regex = Pattern.compile(pattern);
        return (catalog, schema, table) -> !regex.matcher(schema + "." + table).matches();
    }
}
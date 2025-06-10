/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.config;

import java.util.function.Predicate;
import java.util.regex.Pattern;

@FunctionalInterface
public interface ColumnFilter {
    
    /**
     * Test whether a column should be included.
     * 
     * @param catalog The catalog name (typically null for PostgreSQL)
     * @param schema The schema name
     * @param table The table name
     * @param column The column name
     * @return true if the column should be included
     */
    boolean matches(String catalog, String schema, String table, String column);
    
    /**
     * Create a filter that includes all columns.
     * 
     * @return A filter that includes all columns
     */
    static ColumnFilter includeAll() {
        return (catalog, schema, table, column) -> true;
    }
    
    /**
     * Create a filter that excludes all columns.
     * 
     * @return A filter that excludes all columns
     */
    static ColumnFilter excludeAll() {
        return (catalog, schema, table, column) -> false;
    }
    
    /**
     * Create a filter based on a predicate function.
     * 
     * @param predicate The predicate function
     * @return A column filter
     */
    static ColumnFilter fromPredicate(Predicate<String> predicate) {
        return (catalog, schema, table, column) -> predicate.test(column);
    }
    
    /**
     * Create a filter that includes columns matching a regular expression.
     * 
     * @param pattern The regular expression pattern
     * @return A column filter
     */
    static ColumnFilter includePattern(String pattern) {
        Pattern regex = Pattern.compile(pattern);
        return (catalog, schema, table, column) -> regex.matcher(column).matches();
    }
    
    /**
     * Create a filter that excludes columns matching a regular expression.
     * 
     * @param pattern The regular expression pattern
     * @return A column filter
     */
    static ColumnFilter excludePattern(String pattern) {
        Pattern regex = Pattern.compile(pattern);
        return (catalog, schema, table, column) -> !regex.matcher(column).matches();
    }
}
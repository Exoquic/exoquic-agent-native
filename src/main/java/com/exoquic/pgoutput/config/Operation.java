/*
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.exoquic.pgoutput.config;

public enum Operation {
    /** Row insertion */
    INSERT,
    
    /** Row update */
    UPDATE,
    
    /** Row deletion */
    DELETE,
    
    /** Table truncation */
    TRUNCATE,
    
    /** Transaction begin marker */
    BEGIN,
    
    /** Transaction commit marker */
    COMMIT,
    
    /** Logical decoding message (PG 14+) */
    MESSAGE
}
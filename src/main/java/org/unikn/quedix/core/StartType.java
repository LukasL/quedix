package org.unikn.quedix.core;

/**
 * Start type for command parsing.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 * 
 */
public enum StartType {

    /** Distribution with REST. */
    DISTRIBUTION_REST,
    /** Distribution with sockets. */
    DISTRIBUTION_SOCKETS,
    /** Map with REST. */
    MAP_REST,
    /** Map with sockets. */
    MAP_SOCKETS,
    /** Map and Reduce with sockets. */
    MAP_AND_REDUCE_SOCKETS,
    /** Map and Reduce with REST. */
    MAP_AND_REDUCE_REST

}

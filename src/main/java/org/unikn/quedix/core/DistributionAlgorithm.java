package org.unikn.quedix.core;

/**
 * This enumeration is responsible to differentiate between the available distribution algorithms.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public enum DistributionAlgorithm {
    /** Round robin with creating connection for each document. */
    ROUND_ROBIN_SIMPLE,
    /** Round robin with creating connection per server/chunk. */
    ROUND_ROBIN_CHUNK,
    /** Advanced with creating connection per server/chunk. */
    ADVANCED_CHUNK,
    /** Advanced with creating connection for each document. */
    ADVANCED,
    /** Partitioning. */
    PARTITIONING

}

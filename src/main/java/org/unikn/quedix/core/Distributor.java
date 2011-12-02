package org.unikn.quedix.core;

/**
 * This interface identifies the unified distribution method.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public interface Distributor {

    /**
     * Distributes an XML collection to the available servers.
     * 
     * @param collection
     *            Path of XML directory.
     * @param name
     *            The name of XML collection.
     * @param algorithm
     *            Distribution algorithm.
     * @return <code>true</code> if the distribution was successful, <code>false</code> otherwise.
     * @exception Exception
     *                Exception occurred.
     */
    public boolean distributeCollection(final String collection, final String name,
        final DistributionAlgorithm algorithm) throws Exception;

}

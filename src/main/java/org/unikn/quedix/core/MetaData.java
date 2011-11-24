package org.unikn.quedix.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class holds meta information to our data servers.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class MetaData {

    /** Server database mapping. */
    private Map<String, List<String>> mServerDbMapping;
    /** Server storage occupied size. */
    private Map<String, Long> mServerStorageSize;

    /**
     * Creates new maps.
     */
    public MetaData() {
        mServerDbMapping = new HashMap<String, List<String>>();
        mServerStorageSize = new HashMap<String, Long>();
    }

    /**
     * Returns database list corresponding to an existing data server.
     * 
     * @param server
     *            Server.
     * @return Corresponding databases.
     */
    public List<String> getDbList(final String server) {
        return mServerDbMapping.get(server);
    }

    /**
     * Returns occupied storage size corresponding to a given data server.
     * 
     * @param server
     *            Existing server.
     * @return Occupied storage size.
     */
    public long getOccupiedStorageSize(final String server) {
        return mServerStorageSize.get(server);
    }

    /**
     * Adds a new allocated server.
     * 
     * @param server
     *            Server.
     */
    public void addServer(final String server) {
        if (!mServerDbMapping.containsKey(server))
            mServerDbMapping.put(server, new ArrayList<String>());
    }

    /**
     * Adds a database to our server mapping.
     * 
     * @param server
     *            Server.
     * @param dbName
     *            Name of database or collection.
     */
    public void addDb(final String server, final String dbName) {
        List<String> dbs;
        if (mServerDbMapping.containsKey(server)) {
            dbs = mServerDbMapping.get(server);
            dbs.add(dbName);
        } else {
            dbs = new ArrayList<String>();
            dbs.add(dbName);
            mServerDbMapping.put(server, dbs);
        }
    }

    /**
     * Updates storage size corresponding to a server name.
     * 
     * @param server
     *            Server.
     * @param size
     *            Storage size occupation in byte.
     */
    public void updateOccupiedStorage(final String server, final long size) {
        mServerStorageSize.put(server, size);
    }

}

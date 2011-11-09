/**
 * Copyright (c) 2010, Distributed Systems Group, University of Konstanz
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED AS IS AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * 
 */

package org.unikn.quedix;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.unikn.quedix.map.MapClient;
import org.unikn.quedix.rest.RestClient;
import org.unikn.quedix.socket.BaseXClient;
import org.unikn.quedix.socket.SocketClient;

/**
 * This class is responsible to initiate the map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class Runner {
	
    /** document for importing and querying. */
    public static final String DOC = "factbook";
    /** Example collection name. */
    public static final String REST_COL = "rest/";
    /** HTTP String. */
    public static final String HTTP = "http://";
    /** Host name. */
    public static final String HOST = "aalto.disy.inf.uni-konstanz.de";
    /** User name. */
    public static final String USER = "admin";
    /** Password. */
    public static final String PW = "admin";

    /**
     * Main.
     * 
     * @param args
     *            Program arguments are input paths to map and reduce XQuery files or XML collection.
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        if (args.length == 1)
            // Map
            new Runner(args[0]);
        else
            // Distribution of collection
            new Runner(args[0], args[1]);

    }

    /**
     * Default constructor
     * 
     * @throws IOException
     */
    public Runner(final String xmlDir, final String name) throws IOException {
        Client cl = new RestClient(initHttpDataServers());
        try {
            cl.distributeCollection(xmlDir, name);
        } catch (final Exception exc) {
            exc.printStackTrace();
        }
    }

    /**
     * Default constructor
     * 
     * @throws IOException
     */
    public Runner(final String xq) throws IOException {
        long start = System.nanoTime();
        // Mapper
        // MapClient map = new MapClient(new RestClient(initHttpDataServers()), new File(xq));
        MapClient map = new MapClient(new SocketClient(initBaseXClients()), new File(xq));
        map.distribute();
        map.execute();
        map.cleanup();
        long time = System.nanoTime() - start;
        System.out.println("\nComplete mapper execution time: " + time / 1000000 + " ms \n");

    }

    /**
     * Initializes the example servers.
     * 
     * @return {@link Map} of server mappings.
     */
    public Map<String, String> initHttpDataServers() {
        Map<String, String> dataServers = new HashMap<String, String>();
        dataServers.put(HTTP + HOST + ":8984/", REST_COL);
        dataServers.put(HTTP + HOST + ":8986/", REST_COL);
        dataServers.put(HTTP + HOST + ":8988/", REST_COL);
        return dataServers;
    }

    /**
     * Initialization of BaseX clients.
     * 
     * @return {@link Map} of connected BaseX clients.
     * @throws IOException
     *             Exception occurred, e.g. server are not running.
     */
    public Map<String, BaseXClient> initBaseXClients() throws IOException {
        Map<String, BaseXClient> mClients = new HashMap<String, BaseXClient>();
        mClients.put("site1", new BaseXClient(HOST, 1980, USER, PW));
        mClients.put("site2", new BaseXClient(HOST, 1981, USER, PW));
        mClients.put("site3", new BaseXClient(HOST, 1982, USER, PW));
        return mClients;
    }

}

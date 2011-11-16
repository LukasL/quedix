package org.unikn.quedix;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.basex.query.QueryException;
import org.unikn.quedix.core.Client;
import org.unikn.quedix.core.ClientType;
import org.unikn.quedix.map.MapClient;
import org.unikn.quedix.reduce.ReduceClient;
import org.unikn.quedix.rest.RestClient;
import org.unikn.quedix.socket.BaseXClient;
import org.unikn.quedix.socket.SocketClient;

/**
 * This class is responsible to initiate the distribution, map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class Runner {

    /** Example document for importing and querying. */
    public static final String DOC = "factbook";
    /** REST path. */
    public static final String REST_COL = "rest/";
    /** HTTP path. */
    public static final String HTTP = "http://";
    /** Host name. */
    public static final String HOST = "aalto.disy.inf.uni-konstanz.de";
    /** User name. */
    public static final String USER = "admin";
    /** User password. */
    public static final String PW = "admin";

    /**
     * Main.
     * 
     * @param args
     *            Program arguments are input paths to map and reduce XQuery
     *            files or XML collection.
     * @throws IOException
     *             File not found.
     * @throws QueryException
     *             Query exception.
     */
    public static void main(final String[] args) throws IOException, QueryException {
        long start = System.nanoTime();

        if (args.length == 1)
            // Map
            new Runner(args[0], ClientType.SOCKETS);
        else if (args.length == 2)
            // Distribution of collection
            new Runner(args[0], args[1]);
        else if (args.length == 3)
            // Map & Reduce
            new Runner(args[0], args[1], ClientType.REST);
        else
            // Error message because false user input parameters
            System.err.println("False input parameters.");
        long end = System.nanoTime() - start;
        System.out.println("\nComplete execution time: " + end / 1000000 + " ms \n");

    }

    /**
     * Constructor distributes an XML collection.
     * 
     * @throws IOException
     *             XML directory not found.
     */
    public Runner(final String xmlDir, final String name) throws IOException {
        // Client cl = new RestClient(initHttpDataServers());
        Client cl = new SocketClient(initBaseXClients());
        try {
            cl.distributeCollection(xmlDir, name);
        } catch (final Exception exc) {
            exc.printStackTrace();
        }
    }

    /**
     * Constructor creates and executes a map job.
     * 
     * @param xq
     *            XQuery file.
     * @param type
     *            Client type, either {@link ClientType#REST} or {@link ClientType#SOCKETS}.
     * @throws IOException
     *             XQ file not found.
     */
    public Runner(final String xq, final ClientType type) throws IOException {
        if (type == ClientType.REST)
            map(new MapClient(new RestClient(initHttpDataServers()), new File(xq)));
        else {
            SocketClient client = new SocketClient(initBaseXClients());
            map(new MapClient(client, new File(xq)));
            client.shutdownClients();
        }
    }

    /**
     * Constructor creates and executes a map and a reduce job.
     * 
     * @param mapXq
     *            XQuery map file.
     * @param reduceXq
     *            XQuery reduce file.
     * @param type
     *            Client type, either {@link ClientType#REST} or {@link ClientType#SOCKETS}.
     * @throws IOException
     *             XQ file not found.
     * @throws QueryException
     *             Query exception.
     */
    public Runner(final String mapXq, final String reduceXq, final ClientType type) throws IOException,
        QueryException {
        if (type == ClientType.REST)
            map(new MapClient(new RestClient(initHttpDataServers()), new File(mapXq), new ReduceClient(
                new File(reduceXq))));
        else {
            SocketClient client = new SocketClient(initBaseXClients());
            map(new MapClient(client, new File(mapXq), new ReduceClient(new File(reduceXq))));
            client.shutdownClients();
        }

    }

    /**
     * Executes mapping functions.
     * 
     * @param mapper
     *            {@link MapClient} instance.
     */
    private void map(final MapClient mapper) {
        mapper.distribute();
        mapper.execute();
        mapper.cleanup();
    }

    /**
     * Initializes the example servers for REST calls.
     * 
     * @return {@link Map} of server mappings.
     */
    private Map<String, String> initHttpDataServers() {
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
     *             Exception occurred, e.g. servers are not running.
     */
    private Map<String, BaseXClient> initBaseXClients() throws IOException {
        Map<String, BaseXClient> clients = new HashMap<String, BaseXClient>();
        clients.put("site1", new BaseXClient(HOST, 1980, USER, PW));
        clients.put("site2", new BaseXClient(HOST, 1981, USER, PW));
        clients.put("site3", new BaseXClient(HOST, 1982, USER, PW));
        return clients;
    }

}

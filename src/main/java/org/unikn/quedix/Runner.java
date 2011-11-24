package org.unikn.quedix;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.basex.query.QueryException;
import org.unikn.quedix.core.Arg;
import org.unikn.quedix.core.Client;
import org.unikn.quedix.core.ClientType;
import org.unikn.quedix.core.DistributionAlgorithm;
import org.unikn.quedix.core.MetaData;
import org.unikn.quedix.core.StartType;
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
    /** Mond HTTP port 20002. */
    public static final String MOND_HTTP_PORT = "20002";
    /** Mond socket client port 20000. */
    public static final int MOND_SOCKET_PORT = 20000;
    /** Mond01. */
    public static final String MOND_1 = "mond01.inf.uni-konstanz.de";
    /** Mond02. */
    public static final String MOND_2 = "mond02.inf.uni-konstanz.de";
    /** Mond03. */
    public static final String MOND_3 = "mond03.inf.uni-konstanz.de";
    /** Mond04. */
    public static final String MOND_4 = "mond04.inf.uni-konstanz.de";
    /** Mond05. */
    public static final String MOND_5 = "mond05.inf.uni-konstanz.de";
    /** Colon. */
    public static final String COLON = ":";
    /** Slash. */
    public static final String SLASH = "/";

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

        Arg a = parseArgs(args);
        Runner run = new Runner();
        if (a == null)
            printValidArgs();
        else {
            long start = System.nanoTime();
            switch (a.getType()) {
            case DISTRIBUTION_REST:
                run.distributeCollection(a.getPar().get(Arg.Paramter.INPUT), a.getPar()
                    .get(Arg.Paramter.NAME), ClientType.REST);
                break;
            case DISTRIBUTION_SOCKETS:
                run.distributeCollection(a.getPar().get(Arg.Paramter.INPUT), a.getPar()
                    .get(Arg.Paramter.NAME), ClientType.SOCKETS);
                break;

            case MAP_REST:
                run.executeMap(a.getPar().get(Arg.Paramter.MAP), ClientType.REST);
                break;
            case MAP_SOCKETS:
                run.executeMap(a.getPar().get(Arg.Paramter.MAP), ClientType.SOCKETS);
                break;

            case MAP_AND_REDUCE_REST:
                run.executeMapReduce(a.getPar().get(Arg.Paramter.MAP), a.getPar().get(Arg.Paramter.REDUCE),
                    ClientType.REST);
                break;

            case MAP_AND_REDUCE_SOCKETS:
                run.executeMapReduce(a.getPar().get(Arg.Paramter.MAP), a.getPar().get(Arg.Paramter.REDUCE),
                    ClientType.SOCKETS);
                break;

            default:
                printValidArgs();
                break;
            }

            long end = System.nanoTime() - start;
            System.out.println("\nComplete execution time: " + end / 1000000 + " ms \n");
        }

    }

    /**
     * Default.
     */
    public Runner() {
        // default.
    }

    /**
     * Constructor distributes an XML collection.
     * 
     * @throws IOException
     *             XML directory not found.
     */
    public void distributeCollection(final String xmlDir, final String name, final ClientType type)
        throws IOException {
        try {
            Client cl;
            if (type == ClientType.REST)
                cl = new RestClient(initHttpDataServersMonds());
            else
                cl = new SocketClient(initBaseXClientsMonds(), new MetaData());
            cl.distributeCollection(xmlDir, name, DistributionAlgorithm.ROUND_ROBIN_CHUNK);
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
    public void executeMap(final String xq, final ClientType type) throws IOException {
        if (type == ClientType.REST)
            map(new MapClient(new RestClient(initHttpDataServersMonds()), new File(xq)));
        else {
            SocketClient client = new SocketClient(initBaseXClientsMonds(), new MetaData());
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
    public void executeMapReduce(final String mapXq, final String reduceXq, final ClientType type)
        throws IOException, QueryException {
        if (type == ClientType.REST)
            map(new MapClient(new RestClient(initHttpDataServersMonds()), new File(mapXq), new ReduceClient(
                new File(reduceXq))));
        else {
            SocketClient client = new SocketClient(initBaseXClientsMonds(), new MetaData());
            map(new MapClient(client, new File(mapXq), new ReduceClient(new File(reduceXq))));
            client.shutdownClients();
        }

    }

    /**
     * Initializes the example servers for REST calls.
     * 
     * @return {@link Map} of server mappings.
     */
    public Map<String, String> initHttpDataServers() {
        Map<String, String> dataServers = new HashMap<String, String>();
        dataServers.put(HTTP + HOST + COLON + "8984" + SLASH, REST_COL);
        dataServers.put(HTTP + HOST + COLON + "8986" + SLASH, REST_COL);
        dataServers.put(HTTP + HOST + COLON + "8988" + SLASH, REST_COL);
        return dataServers;
    }

    /**
     * Initialization of BaseX clients.
     * 
     * @return {@link Map} of connected BaseX clients.
     * @throws IOException
     *             Exception occurred, e.g. servers are not running.
     */
    public Map<String, BaseXClient> initBaseXClients() throws IOException {
        Map<String, BaseXClient> clients = new HashMap<String, BaseXClient>();
        clients.put("site1", new BaseXClient(HOST, 1980, USER, PW));
        clients.put("site2", new BaseXClient(HOST, 1981, USER, PW));
        clients.put("site3", new BaseXClient(HOST, 1982, USER, PW));
        return clients;
    }

    /**
     * Initializes the example on mond servers for REST calls.
     * 
     * @return {@link Map} of server mappings.
     */
    public Map<String, String> initHttpDataServersMonds() {
        Map<String, String> dataServers = new HashMap<String, String>();
        dataServers.put(HTTP + MOND_1 + COLON + MOND_HTTP_PORT + SLASH, REST_COL);
        dataServers.put(HTTP + MOND_2 + COLON + MOND_HTTP_PORT + SLASH, REST_COL);
        dataServers.put(HTTP + MOND_3 + COLON + MOND_HTTP_PORT + SLASH, REST_COL);
        dataServers.put(HTTP + MOND_4 + COLON + MOND_HTTP_PORT + SLASH, REST_COL);
        dataServers.put(HTTP + MOND_5 + COLON + MOND_HTTP_PORT + SLASH, REST_COL);
        return dataServers;
    }

    /**
     * Initialization of BaseX clients on mond servers.
     * 
     * @return {@link Map} of connected BaseX clients.
     * @throws IOException
     *             Exception occurred, e.g. servers are not running.
     */
    public Map<String, BaseXClient> initBaseXClientsMonds() throws IOException {
        Map<String, BaseXClient> clients = new HashMap<String, BaseXClient>();
        clients.put("mond01", new BaseXClient(MOND_1, MOND_SOCKET_PORT, USER, PW));
        clients.put("mond02", new BaseXClient(MOND_2, MOND_SOCKET_PORT, USER, PW));
        clients.put("mond03", new BaseXClient(MOND_3, MOND_SOCKET_PORT, USER, PW));
        clients.put("mond04", new BaseXClient(MOND_4, MOND_SOCKET_PORT, USER, PW));
        clients.put("mond05", new BaseXClient(MOND_5, MOND_SOCKET_PORT, USER, PW));
        return clients;
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
     * Input arguments parsing.
     * 
     * @param args
     *            Arguments.
     * @return Valid argument mapping.
     */
    private static Arg parseArgs(final String[] args) {
        StartType type = null;
        boolean isRest = false;
        Arg res = null;
        Map<Arg.Paramter, String> params = new HashMap<Arg.Paramter, String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-m")) {
                // map
                params.put(Arg.Paramter.MAP, args[i + 1]);

            } else if (args[i].equals("-r")) {
                // reduce
                params.put(Arg.Paramter.REDUCE, args[i + 1]);

            } else if (args[i].equals("-d")) {
                // distribution
                params.put(Arg.Paramter.INPUT, args[i + 1]);

            } else if (args[i].equals("-n")) {
                // name
                params.put(Arg.Paramter.NAME, args[i + 1]);

            } else if (args[i].equals("-R")) {
                // REST type
                isRest = true;
            } else if (args[i].equals("-S")) {
                // Socket type
                isRest = false;
            }
        }
        if (params.containsKey(Arg.Paramter.INPUT) && params.containsKey(Arg.Paramter.NAME)) {
            type = isRest ? StartType.DISTRIBUTION_REST : StartType.DISTRIBUTION_SOCKETS;
        } else if (params.containsKey(Arg.Paramter.MAP) && !params.containsKey(Arg.Paramter.REDUCE)) {
            type = isRest ? StartType.MAP_REST : StartType.MAP_SOCKETS;
        } else if (params.containsKey(Arg.Paramter.MAP) && params.containsKey(Arg.Paramter.REDUCE)) {
            type = isRest ? StartType.MAP_AND_REDUCE_REST : StartType.MAP_AND_REDUCE_SOCKETS;
        }

        if (type != null)
            res = new Arg(type, params);

        return res;
    }

    /**
     * Printing of valid arguments.
     */
    private static void printValidArgs() {
        StringBuilder sb = new StringBuilder();
        sb.append("Following arguments are valid:\n");
        sb.append("-d PATH -n NAME (Distribution of collection with name)\n");
        sb.append("-m PATH (Map execution with PATH to map.xq function.)\n");
        sb.append("-m PATH -r PATH(Map and reduce execution with PATH to map.xq/reduce.xq function.)\n");
        sb.append("-S (Execution via Java client - sockets)\n");
        sb.append("-R (Execution via HTTP REST)\n");
        System.out.println(sb.toString());
    }
}

package org.unikn.quedix.socket;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.unikn.quedix.Client;
import org.unikn.quedix.query.BaseXClient;
import org.unikn.quedix.query.BaseXClient.Query;

/**
 * This client class connects clients to BaseX server to simulate a distributed
 * environment.
 * 
 * @author Lukas Lewandowski, University of Konstanz, Germany.
 */
public class SocketClient implements Client{

    /** Host name. */
    private static final String HOST = "aalto.disy.inf.uni-konstanz.de";
    /** User name. */
    private static final String USER = "admin";
    /** Password. */
    private static final String PW = "admin";
    /** document name for import and querying. */
    public static final String DOC = "factbook";
    /** Example query 1. */
    public static final String EQ1 = "doc('MyL')//user";
    /** Example query 2. */
    public static final String EQ2 = "sum(for $i in (1 to 1000000) return $i)";
    /** Example query 3. */
    public static final String EQ3 = "1";
    /** Example query 4. */
    public static final String EQ4 = "count(doc('factbook')/descendant::text())*2";

    /** client instances. */
    private Map<String, BaseXClient> mClients;

    /**
     * Starts the client application.
     * 
     * @param args
     *            arguments.
     */
    public static void main(final String[] args) {
        try {
            SocketClient c = new SocketClient();
            // store an XML file with name MyL
            c.exampleImporter(c.getClients());
            // query MyL
            System.out.println("QUERY RESULT: ***");
            long start = System.nanoTime();
            c.queryParallel(c.getClients(), EQ4);
            // c.querySequential(c.getClients(), EQ2);
            c.shutdownClients();
            long end = System.nanoTime() - start;
            System.out.println("Complete Time: " + end / 1000000 + " ms");
        } catch (final IOException exce) {
            exce.printStackTrace();
        }

    }

    /**
     * Constructor connects clients to BaseX server.
     * 
     * @throws IOException
     */
    public SocketClient() throws IOException {
        this.mClients = connectClients();
    }

    /**
     * Querying of all {@link BaseXClient} instances.
     * 
     * @param clients
     *            The clients.
     * @param query
     *            The query.
     * @throws IOException
     *             Exception occurred.
     */
    public void querySequential(final Map<String, BaseXClient> clients, final String query)
        throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, BaseXClient> cls : clients.entrySet()) {
            long start = System.nanoTime();
            BaseXClient bx = cls.getValue();
            Query q = bx.query(query);
            while(q.more()) {
                sb.append(q.next());
            }
            long time = System.nanoTime() - start;
            System.out.println("Time for " + cls.getKey() + ": " + time / 1000000 + " ms");
        }
        System.out.println("Complete Result: \n" + sb.toString());
    }

    /**
     * Querying of all {@link BaseXClient} instances.
     * 
     * @param clients
     *            The clients.
     * @param query
     *            The query.
     * @throws IOException
     *             Exception occurred.
     */
    public void queryParallel(final Map<String, BaseXClient> clients, final String query) throws IOException {
        List<Future<String>> stringResults = new ArrayList<Future<String>>();
        ExecutorService executor = Executors.newFixedThreadPool(clients.size());
        for (Map.Entry<String, BaseXClient> cls : clients.entrySet()) {
            final Map.Entry<String, BaseXClient> cl = cls;

            Callable<String> task = new Callable<String>() {

                @Override
                public String call() throws Exception {
                    System.out.println("Result from: " + cl.getKey());
                    long start = System.nanoTime();
                    BaseXClient bx = cl.getValue();
                    Query q;
                    q = bx.query(query);
                    StringBuilder sb = new StringBuilder();
                    while(q.more()) {
                        sb.append(q.next());
                    }
                    long time = System.nanoTime() - start;
                    System.out.println("Time for " + cl.getKey() + ": " + time / 1000000 + " ms");

                    return sb.toString();
                }
            };
            stringResults.add(executor.submit(task));
        }
        // This will make the executor accept no new threads
        // and finish all existing threads in the queue
        executor.shutdown();
        // Wait until all threads are finish
        while(!executor.isTerminated())
            ;

        for (Future<String> future : stringResults) {
            try {
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Example importer, creates a new database with name "MyL"
     * 
     * @param cls
     * @throws IOException
     */
    public void exampleImporter(final Map<String, BaseXClient> cls) throws IOException {
        for (Map.Entry<String, BaseXClient> c : cls.entrySet()) {
            InputStream is = SocketClient.class.getResourceAsStream("/" + DOC + ".xml");
            create(c.getValue(), DOC, is);
            is.close();
        }
    }

    /**
     * Creates a new document.
     * 
     * @param c
     *            Client instance.
     * @param dbName
     *            Database name.
     * @param xml
     *            Input.
     * @throws IOException
     *             Exception occurred.
     */
    public void create(final BaseXClient c, final String dbName, final InputStream xml) throws IOException {
        BaseXClient bx = c;
        bx.create(dbName, xml);
    }

    /**
     * Initialization of BaseX clients.
     * 
     * @return {@link Map} of connected BaseX clients.
     * @throws IOException
     *             Exception occurred, e.g. server are not running.
     */
    public Map<String, BaseXClient> connectClients() throws IOException {
        mClients = new HashMap<String, BaseXClient>();
        mClients.put("site1", new BaseXClient(HOST, 1980, USER, PW));
        mClients.put("site2", new BaseXClient(HOST, 1981, USER, PW));
        mClients.put("site3", new BaseXClient(HOST, 1982, USER, PW));
        return mClients;
    }

    /**
     * Simple getter.
     * 
     * @return mClients.
     */
    public Map<String, BaseXClient> getClients() {
        return mClients;
    }

    /**
     * Shutdowns connected clients.
     * 
     * @throws IOException
     *             Exception occurred.
     */
    public void shutdownClients() throws IOException {
        if (mClients != null) {
            for (Map.Entry<String, BaseXClient> cl : mClients.entrySet()) {
                BaseXClient c = cl.getValue();
                c.close();
            }
        }
    }
}

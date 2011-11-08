package org.unikn.quedix.socket;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

/**
 * This client class connects clients to BaseX server to simulate a distributed
 * environment.
 * 
 * @author Lukas Lewandowski, University of Konstanz, Germany.
 */
public class SocketClient implements Client {

    /** Mapper database for map file distribution. */
    public static final String MAPPER_DB = "MapperDb2";
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
    /** Map names. */
    private Map<String, String> mMapNames;

    /**
     * Constructor connects clients to BaseX server.
     * 
     * @param clients
     *            {@link Map} of clients to server mapping.
     * @throws IOException
     *             Exception occurred.
     */
    public SocketClient(final Map<String, BaseXClient> clients) throws IOException {
        this.mClients = clients;
        mMapNames = new HashMap<String, String>();
        for (Map.Entry<String, BaseXClient> cls : clients.entrySet()) {
            mMapNames.put(cls.getKey(), "map" + System.nanoTime() + ".xq");
        }
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

    @Override
    public boolean distributeXq(final byte[] xq) {
        boolean isSuccessful = true;
        if (mClients != null) {
            List<Future<Boolean>> booleanResults = new ArrayList<Future<Boolean>>();
            ExecutorService executor = Executors.newFixedThreadPool(mClients.size());
            for (Map.Entry<String, BaseXClient> cls : mClients.entrySet()) {
                final Map.Entry<String, BaseXClient> cl = cls;

                Callable<Boolean> task = new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        try {
                            long start = System.nanoTime();
                            BaseXClient c = cl.getValue();
                            c.execute("open " + MAPPER_DB);
                            ByteArrayInputStream bais = new ByteArrayInputStream(xq);
                            c.store(mMapNames.get(cl.getKey()), bais);
                            long time = System.nanoTime() - start;
                            System.out.println("Time for distribution of map file to" + cl.getKey() + ": "
                            + time / 1000000 + " ms");
                            return true;

                        } catch (final IOException exc) {
                            exc.printStackTrace();
                        }
                        return null;
                    }
                };
                booleanResults.add(executor.submit(task));
            }
            executor.shutdown();
            while(!executor.isTerminated())
                ;
            for (Future<Boolean> future : booleanResults) {
                try {
                    if (!future.get())
                        isSuccessful = false;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        return isSuccessful;
    }

    @Override
    public String[] execute(final String xq) {
        String[] results = {};
        if (mClients != null) {
            results = new String[mClients.size()];
            List<Future<String>> stringResults = new ArrayList<Future<String>>();
            ExecutorService executor = Executors.newFixedThreadPool(mClients.size());
            for (Map.Entry<String, BaseXClient> cls : mClients.entrySet()) {
                final Map.Entry<String, BaseXClient> cl = cls;

                Callable<String> task = new Callable<String>() {

                    @Override
                    public String call() throws Exception {
                        try {
                            long start = System.nanoTime();
                            BaseXClient c = cl.getValue();
                            // final OutputStream out = System.out;
                            String result =
                                c.execute("run ../data/" + MAPPER_DB + "/raw/" + mMapNames.get(cl.getKey()));
                            long time = System.nanoTime() - start;
                            System.out.println("Time for execution the map query at " + cl.getKey() + ": "
                            + time / 1000000 + " ms");
                            return result;

                        } catch (final IOException exc) {
                            exc.printStackTrace();
                        }
                        return null;
                    }
                };
                stringResults.add(executor.submit(task));
            }
            executor.shutdown();
            while(!executor.isTerminated())
                ;

            int i = 0;
            for (Future<String> future : stringResults) {
                try {
                    results[i++] = future.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        return results;
    }

    @Override
    public boolean delete() {
        boolean isSuccessful = true;
        if (mClients != null) {
            List<Future<Boolean>> booleanResults = new ArrayList<Future<Boolean>>();
            ExecutorService executor = Executors.newFixedThreadPool(mClients.size());
            for (Map.Entry<String, BaseXClient> cls : mClients.entrySet()) {
                final Map.Entry<String, BaseXClient> cl = cls;

                Callable<Boolean> task = new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        try {
                            long start = System.nanoTime();
                            BaseXClient c = cl.getValue();
                            c.execute("open " + MAPPER_DB);
                            c.execute("delete " + mMapNames.get(cl.getKey()));
                            long time = System.nanoTime() - start;
                            System.out.println("Time for deletion of map file at" + cl.getKey() + ": " + time
                            / 1000000 + " ms");
                            return true;

                        } catch (final IOException exc) {
                            exc.printStackTrace();
                        }
                        return null;
                    }
                };
                booleanResults.add(executor.submit(task));
            }
            executor.shutdown();
            while(!executor.isTerminated())
                ;
            for (Future<Boolean> future : booleanResults) {
                try {
                    if (!future.get())
                        isSuccessful = false;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        return isSuccessful;
    }

    @Override
    public List<String> checkMapperDb() {
        List<String> result = new ArrayList<String>();
        if (mClients != null) {
            for (Map.Entry<String, BaseXClient> cl : mClients.entrySet()) {
                BaseXClient c = cl.getValue();
                try {
                    c.execute("list " + MAPPER_DB);
                } catch (final IOException exc) {
                    exc.printStackTrace();
                    result.add(cl.getKey());
                    System.out.println(cl.getKey() + " will be prepared for creation process.");
                }
            }
        }
        return result;
    }

    @Override
    public void createMapperDb(final String dataServer) {
        BaseXClient c = mClients.get(dataServer);
        try {
            c.execute("create db " + MAPPER_DB);
        } catch (final IOException exc) {
            exc.printStackTrace();
        }

    }

    @Override
    public boolean distributeCollection(final String collection, final String name) {
        // TODO Auto-generated method stub
        return false;
    }
}

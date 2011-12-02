package org.unikn.quedix.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.basex.core.Context;
import org.basex.core.cmd.Run;
import org.basex.util.Token;
import org.unikn.quedix.core.Client;
import org.unikn.quedix.core.DistributionAlgorithm;
import org.unikn.quedix.socket.BaseXClient.Query;

import static org.unikn.quedix.rest.Constants.UTF8;

/**
 * This client class connects clients to BaseX server to simulate a distributed
 * environment.
 * 
 * @author Lukas Lewandowski, University of Konstanz, Germany.
 */
public class SocketClient implements Client {

    /** Mapper database for map file distribution. */
    public static final String MAPPER_DB = "MapperDb2";
    /** Document name for import and querying. */
    public static final String DOC = "factbook";
    /** Example query 1. */
    public static final String EQ1 = "doc('MyL')//user";
    /** Example query 2. */
    public static final String EQ2 = "sum(for $i in (1 to 1000000) return $i)";
    /** Example query 3. */
    public static final String EQ3 = "1";
    /** Example query 4. */
    public static final String EQ4 = "count(doc('factbook')/descendant::text())*2";
    /** XML file ending. */
    protected static final String XML = ".xml";
    /** Open command. */
    protected static final String OPEN = "Open ";
    /** Create command. */
    private static final String CREATE_DB = "Create db ";
    /** List command. */
    private static final String LIST = "list ";
    /** Delete command . */
    private static final String DELETE = "delete ";


    /** Client instances. */
    protected Map<String, BaseXClient> mClients;
    /** Map names. */
    private Map<String, String> mMapNames;
    /** Client database Mapping. */
    private Map<BaseXClient, List<String>> mDbClientMapping;

    /** Client. */
    protected BaseXClient mClient = null;
    /** Meta data. */
    protected org.unikn.quedix.core.MetaData mMeta;
    /** Amount of parts for the partitioned distribution algorithm. */
    protected long mPartitionedPackage = 0;
    /** Check if first document. */
    protected boolean mIsFirst;
    /** Set used to start refactoring operations. */
    protected Set<BaseXClient> mRefactoring = new HashSet<BaseXClient>();
    private String mRefactorXq;

    /**
     * Constructor connects clients to BaseX server.
     * 
     * @param clients
     *            {@link Map} of clients to server mapping.
     * @throws IOException
     *             Exception occurred.
     */
    public SocketClient(final Map<String, BaseXClient> clients, final org.unikn.quedix.core.MetaData meta)
        throws IOException {
        this.mClients = clients;
        mMapNames = new HashMap<String, String>();
        mMeta = meta;
        mDbClientMapping = new HashMap<BaseXClient, List<String>>();
        for (Map.Entry<String, BaseXClient> cls : clients.entrySet()) {
            mMapNames.put(cls.getKey(), "map" + System.nanoTime() + ".xq");
            List<String> dbs = new ArrayList<String>();
            dbs.add("coli");
            mDbClientMapping.put(cls.getValue(), dbs);
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
                            c.execute(OPEN + MAPPER_DB);
                            ByteArrayInputStream bais = new ByteArrayInputStream(xq);
                            c.store(mMapNames.get(cl.getKey()), bais);
                            long time = System.nanoTime() - start;
                            System.out.println("Time for distribution of map file to " + cl.getKey() + ": "
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

                            String query =
                                "let $raw := db:retrieve($db, $map) " + "let $query := util:to-string($raw) "
                                + "return util:eval($raw)";

                            Query q = c.query(query);
                            q.bind("db", MAPPER_DB);
                            q.bind("map", mMapNames.get(cl.getKey()));
                            System.out.println("go");
                            String result = q.execute();

                            // final OutputStream out = System.out;
                            // String result = c.execute(RUN + "../data/"
                            // + MAPPER_DB + "/raw/"
                            // + mMapNames.get(cl.getKey()));
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
                            c.execute(OPEN + MAPPER_DB);
                            c.execute(DELETE + mMapNames.get(cl.getKey()));
                            long time = System.nanoTime() - start;
                            System.out.println("Time for deletion of map file at " + cl.getKey() + ": "
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
    public List<String> checkMapperDb() {
        List<String> result = new ArrayList<String>();
        if (mClients != null) {
            for (Map.Entry<String, BaseXClient> cl : mClients.entrySet()) {
                BaseXClient c = cl.getValue();
                try {
                    c.execute(LIST + MAPPER_DB);
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
            c.execute(CREATE_DB + MAPPER_DB);
        } catch (final IOException exc) {
            exc.printStackTrace();
        }

    }
    
    @Override
    public void execute(final String xq, final OutputStream output) {
        if (mClients != null) {
            ExecutorService executor = Executors.newFixedThreadPool(mClients.size());
            for (Map.Entry<String, BaseXClient> cls : mClients.entrySet()) {
                final Map.Entry<String, BaseXClient> cl = cls;

                Callable<Void> task = new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {
                        try {
                            long start = System.nanoTime();
                            BaseXClient c = cl.getValue();

                            String query =
                                "let $raw := db:retrieve($db, $map) " + "let $query := util:to-string($raw) "
                                + "return util:eval($query)";

                            Query q = c.query(query);
                            q.bind("db", MAPPER_DB);
                            q.bind("map", mMapNames.get(cl.getKey()));
                            q.execute(output);

                            long time = System.nanoTime() - start;
                            System.out.println("Time for execution the map query at " + cl.getKey() + ": "
                            + time / 1000000 + " ms");

                        } catch (final IOException exc) {
                            exc.printStackTrace();
                        }
                        return null;
                    }
                };
                executor.submit(task);
            }
            executor.shutdown();
            while(!executor.isTerminated())
                ;
        }
    }

    /**
     * This method return the next client in a round robin manner to support
     * uniform distribution.
     * 
     * @param server
     *            servers.
     * @param loop
     *            runner.
     * @return next BaseXClient instance.
     */
    protected BaseXClient next(String[] server, final int runner) {
        return mClients.get(server[runner % server.length]);
    }
    
    /**
     * Emit the size of a folder.
     * 
     * @param directory
     *            Input collection directory.
     * @return directory size in bytes.
     */
    protected long folderSize(final File directory) {
        // check auf XML
        long length = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML))
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }
    
    /**
     * Creates a collection out of sub collection.
     * 
     * @param client
     *            BaseX client instance.
     * @param tempName
     *            Temporary database.
     * @param name
     *            Name of new collection.
     * @throws IOException
     */
    protected void runRefactoring(final BaseXClient client, final String tempName, final String name)
        throws IOException {
        client.createCol(name);
        Query q = client.query(getRefactorXq());
        q.bind("subcollection", tempName);
        q.bind("collectionname", name);
        q.execute();
        q.close();
        client.execute("Drop database " + tempName);
    }

    /**
     * Setter.
     * 
     * @param refactorXq
     *            The refactorXq to set.
     */
    public void setRefactorXq(final String refactorXq) {
        mRefactorXq = refactorXq;
    }

    /**
     * Getter.
     * 
     * @return Returns the refactorXq.
     */
    public String getRefactorXq() {
        return mRefactorXq;
    }
}

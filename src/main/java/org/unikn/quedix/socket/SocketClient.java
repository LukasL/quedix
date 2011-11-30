package org.unikn.quedix.socket;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.basex.data.MetaData;
import org.unikn.quedix.core.Client;
import org.unikn.quedix.core.DistributionAlgorithm;
import org.unikn.quedix.socket.BaseXClient.Query;

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
    /** Package size. */
    private static final int PACKAGE_SIZE = 67108864;
    /** XML file ending. */
    private static final String XML = ".xml";
    /** Open command. */
    private static final String OPEN = "Open ";
    /** Create command. */
    private static final String CREATE_DB = "Create db ";
    /** List command. */
    private static final String LIST = "list ";
    /** Delete command . */
    private static final String DELETE = "delete ";

    /** Client instances. */
    private Map<String, BaseXClient> mClients;
    /** Map names. */
    private Map<String, String> mMapNames;
    /** Client database Mapping. */
    private Map<BaseXClient, List<String>> mDbClientMapping;
    /** Written chunks. */
    private long mOutSize = 0;
    /** Index. */
    private int mInd = 0;
    /** Client. */
    private BaseXClient mClient = null;
    /** Last user feedback check. */
    private long mLast = 0;
    /** Meta data. */
    private org.unikn.quedix.core.MetaData mMeta;
    /** Amount of parts for the partitioned distribution algorithm. */
    private long mPartitionedPackage = 0;

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
    public boolean distributeCollection(final String collection, final String name,
        final DistributionAlgorithm algorithm) throws IOException {
        boolean isSuccessful = true;
        long start = System.nanoTime();
        // input folder containing XML documents to be stored.
        final File inputDir = new File(collection);
        String[] serverIds = new String[mClients.size()];
        int i = 0;
        for (Map.Entry<String, BaseXClient> entry : mClients.entrySet())
            serverIds[i++] = entry.getKey();
        if (inputDir.isDirectory()) {
            System.out.println("Start import collection...");
            long sum = -1;
            switch (algorithm) {
            case ROUND_ROBIN_SIMPLE:
                System.out.println("Execute round robin simple");
                sum = distributeRoundRobin(inputDir, name, serverIds);
                break;
            case ROUND_ROBIN_CHUNK:
                System.out.println("Execute round robin simple");
                sum = distributeRoundRobin(inputDir, name, serverIds);
                break;
            case ADVANCED:
                System.out.println("Execute round robin advanced");
                sum = distributeAdvanced(inputDir, name, serverIds);
                break;
            case ADVANCED_CHUNK:
                System.out.println("Execute round robin advanced");
                sum = distributeAdvanced(inputDir, name, serverIds);
                break;
            case PARTITIONING:
                System.out.println("Execute partitioned");
                long completeSize = folderSize(inputDir);
                long amountPackages;
                double a = completeSize / mMeta.getServerMeta().getRam();
                if ((completeSize % mMeta.getServerMeta().getRam()) == 0)
                    amountPackages = (long)a;
                else
                    amountPackages = (long)a + 1;
                mPartitionedPackage = (long)(completeSize / amountPackages);
                System.out.println("Directory size: " + completeSize);

                sum = distributePartitioned(inputDir, name, serverIds);
                break;

            default:
                System.out.println("Not supported");
                break;
            }
            System.out.println("Amount of imported files: " + sum);
        } else if (inputDir.getAbsolutePath().endsWith(XML)) {
            System.out.println("Start import single XML file");
            BaseXClient client = next(serverIds, 0);
            System.out.println("Distributed to: " + client.ehost);
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputDir));
            distributeXml(client, name, bis, inputDir);
            bis.close();
        } else
            System.err.println("False input path. Try again.");
        System.out.println("Progress: 100.0 %.");
        long end = System.nanoTime() - start;
        System.out.println("Done in " + ((double)end / 1000000000.0) + " s");

        return isSuccessful;
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
    private BaseXClient next(String[] server, final int runner) {
        return mClients.get(server[runner % server.length]);
    }

    /**
     * Checks existence of collection.
     * 
     * @param client
     *            {@link BaseXClient} instance.
     * @param collectionName
     *            Collection name.
     * @return <code>true</code> if collection exists, <code>false</code> otherwise.
     */
    private boolean checkCollectionExistence(final BaseXClient client, final String collectionName) {
        if (mMeta.containsServer(client.ehost)) {
            List<String> dbs = mMeta.getDbList(client.ehost);
            for (String db : dbs) {
                if (db.equals(collectionName))
                    return true;
            }
        }
        return false;
    }

    /**
     * Distributes file.
     * 
     * @param client
     *            {@link BaseXClient} instance.
     * @param name
     *            Collection file.
     * @param bis
     *            {@link BufferedInputStream} holding XML file.
     * @param file
     *            {@link File} reference.
     * @throws IOException
     *             Exception while writing with client.
     */
    private void distributeXml(final BaseXClient client, final String name, final BufferedInputStream bis,
        final File file) throws IOException {
        if (checkCollectionExistence(client, name)) {
            client.execute(OPEN + name);
            client.add(file.getAbsolutePath(), bis);
        } else {
            client.createCol(name);
            client.add(file.getAbsolutePath(), bis);
            mMeta.addDb(client.ehost, name);
        }
    }

    /**
     * Traverses an input directory for distribution of collection.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     *             Exception occurred.
     */
    private long distributeRoundRobin(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                mClient = next(serverIds, mInd++);
                distributeXml(mClient, name, bis, file);
                bis.close();
                count++;
            } else if (file.isDirectory()) {
                count += distributeRoundRobin(file, name, serverIds);
            }

            // user feedback
            if ((count % 10 == 0) && count != mLast) {
                System.out.print(".");
                mLast = count;
            }
        }
        return count;
    }

    /**
     * Distributes collection using the advanced algorithm using addition meta information.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     *             Exception occurred.
     */
    private long distributeAdvanced(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                // nur beim start ausgefuehrt;
                if (mOutSize == 0) {
                    mClient = next(serverIds, mInd++);
                    distributeXml(mClient, name, bis, file);
                } else if ((mOutSize + file.length()) > mMeta.getServerMeta().getRam()) {
                    mOutSize = 0;
                    mClient = next(serverIds, mInd++);
                    distributeXml(mClient, name, bis, file);
                } else {
                    distributeXml(mClient, name, bis, file);
                }
                mOutSize += file.length();
                bis.close();
                count++;
            } else if (file.isDirectory()) {
                count += distributeAdvanced(file, name, serverIds);
            }

            // user feedback
            if ((count % 10 == 0) && count != mLast) {
                System.out.print(".");
                mLast = count;
            }
        }
        return count;
    }

    /**
     * Distributes collection using the partitioning algorithm using addition meta information.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     *             Exception occurred.
     */
    private long distributePartitioned(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                // nur beim start ausgefuehrt;
                if (mOutSize == 0) {
                    mClient = next(serverIds, mInd++);
                    distributeXml(mClient, name, bis, file);
                } else if ((mOutSize + file.length()) > mPartitionedPackage) {
                    mOutSize = 0;
                    mClient = next(serverIds, mInd++);
                    distributeXml(mClient, name, bis, file);
                } else {
                    distributeXml(mClient, name, bis, file);
                }
                mOutSize += file.length();
                bis.close();
                count++;
            } else if (file.isDirectory()) {
                count += distributeAdvanced(file, name, serverIds);
            }

            // user feedback
            if ((count % 10 == 0) && count != mLast) {
                System.out.print(".");
                mLast = count;
            }
        }
        return count;
    }

    /**
     * Updates meta information from all servers.
     * 
     * @throws IOException
     *             Exception occurred.
     */
    private void updateMeta() throws IOException {
        for (Map.Entry<String, BaseXClient> cls : mClients.entrySet()) {
            final BaseXClient c = cls.getValue();
            mMeta.addServer(c.ehost);
            c.execute(LIST);
        }
    }

    /**
     * Emit the size of a folder.
     * 
     * @param directory
     *            Input collection directory.
     * @return directory size in bytes.
     */
    private long folderSize(final File directory) {
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

}

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

import org.unikn.quedix.core.Client;

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
    /** Run command . */
    private static final String RUN = "run ";

    /** Client instances. */
    private Map<String, BaseXClient> mClients;
    /** Map names. */
    private Map<String, String> mMapNames;
    /** Client database Mapping. */
    private Map<BaseXClient, List<String>> mDbClientMapping;

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
                                c.execute(RUN + "../data/" + MAPPER_DB + "/raw/" + mMapNames.get(cl.getKey()));
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
    public boolean distributeCollection(final String collection, final String name) throws IOException {
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
            int runner = 0;
            File[] files = inputDir.listFiles();
            int filesCount = files.length;
            System.out.println("Amount of files to import: " + filesCount);
            long outSize = 0;
            int ind = 0;
            BaseXClient client = null;
            for (File file : files) {
                if (file.getAbsolutePath().endsWith(XML)) {
                    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                    // print progress
                    int per = (filesCount / 10);
                    int div = runner % (per == 0 ? 1 : per);
                    if (div < 1) {
                        double progress = (double)runner / filesCount * 100;
                        System.out.println("Progress: " + progress + " %.");
                    }
                    // nur beim start ausgefuehrt;
                    if (outSize == 0) {
                        client = next(serverIds, ind++);
                        distributeXml(client, name, bis, file);
                    } else if ((outSize + file.length()) > PACKAGE_SIZE) {
                        outSize = 0;
                        client = next(serverIds, ind++);
                        distributeXml(client, name, bis, file);
                    } else {
                        distributeXml(client, name, bis, file);
                    }
                    outSize += file.length();
                    bis.close();
                    runner++;
                }
            }
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
                            // final OutputStream out = System.out;
                            c.execute(RUN + "../data/" + MAPPER_DB + "/raw/" + mMapNames.get(cl.getKey()),
                                output);
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
        return mDbClientMapping.get(client).contains(collectionName);
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
            client.add(name + "/" + file.getName(), bis);
        } else {
            client.create(name, bis);
            List<String> l = mDbClientMapping.get(client);
            if (!l.contains(name))
                l.add(name);
        }
    }

}

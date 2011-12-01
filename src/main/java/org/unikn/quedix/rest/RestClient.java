package org.unikn.quedix.rest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.basex.util.Token;
import org.unikn.quedix.core.Client;
import org.unikn.quedix.core.DistributionAlgorithm;
import org.unikn.quedix.core.MetaData;

import static org.unikn.quedix.rest.Constants.DELETE;
import static org.unikn.quedix.rest.Constants.PUT;
import static org.unikn.quedix.rest.Constants.UTF8;
import static org.unikn.quedix.rest.Constants.XML_TYPE;

/**
 * This class is responsible to execute parallel queries over HTTP.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class RestClient implements Client {

    /** Example query 1. */
    public static final String EQ1 = "//user";
    /** Example query 2. */
    public static final String EQ2 = "sum(for $i in (1 to 1000000) return $i)";
    /** Example query 3. */
    public static final String EQ3 = "1";
    /** Example query 4. */
    public static final String EQ4 = "count(/descendant::text())*2";
    /** document for importing and querying. */
    public static final String DOC = "factbook";
    /** Example collection name. */
    public static final String COL = "rest/" + DOC;
    /** Package size. */
    private static final int PACKAGE_SIZE = 67108864;
    /** Start subcollection name. */
    private static final String SUB_COLLECTION_NAME = "subcollection";
    /** Start subcollection root tag name. */
    private static final String START = "<" + SUB_COLLECTION_NAME + ">";
    /** End subcollection root tag name. */
    private static final String END = "</" + SUB_COLLECTION_NAME + ">";
    /** MapperDb name for holding mapping query files. */
    private final static String MAPPER_DB = "rest/MapperDb";
    /** Lock for writing sequential into the {@link OutputStream}. */
    private static final Semaphore LOCK = new Semaphore(1);
    private static final byte[] COL_START = Token.token(START);
    private static final byte[] COL_END = Token.token(END);

    /** Registered data servers. */
    private Map<String, String> mDataServers;
    /** Mappers located at destinations. */
    private List<String> mDestinationMappers;
    /** Map of executed server files inclusive state information. */
    private Map<String, Integer> mStates;
    /** Data servers array for distribution. */
    private String[] mDataServersArray;
    /** Written chunks. */
    private long mOutSize = 0;
    /** Last user feedback check. */
    private long mLast = 0;
    /** Distribution service. */
    private DistributionService mDistributionService = null;
    /** Output stream. */
    private BufferedOutputStream mBos = null;
    /** Transformer. */
    private Transformer mTrans = null;
    /** Height of method. */
    private int mH = 0;
    /** Runner object for iterating in round robin. */
    private int runner = 0;
    /** Meta data. */
    private MetaData mMeta;
    /** Host for calling server. */
    private String mHost = null;
    /** Dynamic package size. */
    private long mPackageSize = 0;
    /** Refactoring servers. */
    private List<String> mRefactoringServers = new ArrayList<String>();

    /**
     * Default constructor.
     */
    public RestClient(final Map<String, String> dataServers, final org.unikn.quedix.core.MetaData meta) {
        mDataServers = dataServers;
        mDataServersArray = new String[mDataServers.size()];
        int i = 0;
        for (Map.Entry<String, String> serverEntry : mDataServers.entrySet())
            mDataServersArray[i++] = serverEntry.getKey() + "rest";

        mDestinationMappers = new ArrayList<String>();
        mStates = new ConcurrentHashMap<String, Integer>();
        mMeta = meta;
        mPackageSize = mMeta.getServerMeta().getRam();
    }

    /**
     * Starts the importing and distributing process.
     */
    public void importExample() {
        for (Map.Entry<String, String> serverEntry : mDataServers.entrySet()) {
            put(serverEntry.getKey() + serverEntry.getValue());
        }
    }

    /**
     * Simple getter.
     * 
     * @return Available data servers.
     */
    public Map<String, String> getDataServers() {
        return mDataServers;
    }

    @Override
    public boolean distributeXq(final byte[] xq) {
        boolean isSuccessful = true;
        ExecutorService executor = Executors.newFixedThreadPool(getDataServers().size());
        for (Map.Entry<String, String> dataServer : getDataServers().entrySet()) {
            final String destinationPath =
                dataServer.getKey() + MAPPER_DB + "/map" + System.nanoTime() + ".xq";
            mDestinationMappers.add(destinationPath);
            mStates.put(destinationPath, 0);
            Callable<Void> task = new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    long start = System.nanoTime();

                    try {
                        SendMapperService mapperService = new SendMapperService(destinationPath);
                        OutputStream outputStream = new BufferedOutputStream(mapperService.prepareOutput());
                        byte[] mapper = new byte[xq.length];
                        System.arraycopy(xq, 0, mapper, 0, xq.length);
                        outputStream.write(mapper);
                        outputStream.close();
                        mapperService.executeService();
                        mStates.put(destinationPath, 100);
                    } catch (final IOException exc) {
                        exc.printStackTrace();
                    }

                    long time = System.nanoTime() - start;
                    System.out.println("Time for " + destinationPath + ": " + time / 1000000 + " ms");
                    return null;
                }
            };
            executor.submit(task);

        }
        // This will make the executor accept no new threads
        // and finish all existing threads in the queue
        executor.shutdown();
        // Wait until all threads are finish
        while(!executor.isTerminated())
            ;
        return isSuccessful;
    }

    @Override
    public void createMapperDb(final String targetResource) {
        URL url;
        try {
            url = new URL(targetResource + getMapperDb());
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(PUT);

            if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                // TODO Exception werfen, da ausf�hrung irgendwie nicht m�glich
            }
            System.out.println("\n* HTTP response: " + conn.getResponseCode() + " ("
            + conn.getResponseMessage() + ")");
            conn.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String[] execute(final String xq) {
        List<Future<String>> stringResults = new ArrayList<Future<String>>();
        ExecutorService executor = Executors.newFixedThreadPool(mDestinationMappers.size());
        for (String mapperFile : mDestinationMappers) {
            final String entry = mapperFile;
            Callable<String> task = new Callable<String>() {

                @Override
                public String call() throws Exception {
                    long start = System.nanoTime();
                    String result = runQuery(entry);
                    long time = System.nanoTime() - start;
                    System.out.println("Time for " + entry + ": " + time / 1000000 + " ms");

                    return result;
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
        String[] results = new String[stringResults.size()];
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
        return results;
    }

    @Override
    public boolean delete() {
        boolean isSuccessful = true;
        ExecutorService executor = Executors.newFixedThreadPool(mDestinationMappers.size());
        List<Future<Boolean>> booleanResults = new ArrayList<Future<Boolean>>();
        for (String mapperFile : mDestinationMappers) {
            final String entry = mapperFile;
            Callable<Boolean> task = new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    boolean isSuccessful = false;
                    URL url;
                    try {
                        url = new URL(entry);
                        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                        conn.setRequestMethod(DELETE);
                        int code = conn.getResponseCode();
                        if (code == HttpURLConnection.HTTP_OK) {
                            BufferedReader br =
                                new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF8));

                            StringBuffer sb = new StringBuffer();
                            for (String line; (line = br.readLine()) != null;) {
                                sb.append(line);
                            }
                            br.close();
                            System.out.println(sb.toString());
                            isSuccessful = true;
                        } else {
                            BufferedReader br =
                                new BufferedReader(new InputStreamReader(conn.getErrorStream(), UTF8));
                            for (String line; (line = br.readLine()) != null;) {
                                System.out.println(line);
                            }
                            br.close();
                            isSuccessful = false;
                        }

                        conn.disconnect();
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return isSuccessful;
                }
            };
            booleanResults.add(executor.submit(task));
        }
        // This will make the executor accept no new threads
        // and finish all existing threads in the queue
        executor.shutdown();
        // Wait until all threads are finish
        while(!executor.isTerminated())
            ;
        for (Future<Boolean> future : booleanResults) {
            try {
                if (!future.get()) {
                    isSuccessful = false;
                    break;
                }
            } catch (final InterruptedException exc) {
                exc.printStackTrace();
            } catch (final ExecutionException exc) {
                exc.printStackTrace();
            }
        }

        return isSuccessful;
    }

    public String getMapperDb() {
        return MAPPER_DB;
    }

    @Override
    public List<String> checkMapperDb() {
        List<String> notExistingMapperDbs = new ArrayList<String>();
        for (Map.Entry<String, String> dataServers : getDataServers().entrySet()) {
            URL url;
            try {
                url = new URL(dataServers.getKey() + getMapperDb());
                HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                if (conn.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND)
                    notExistingMapperDbs.add(dataServers.getKey());
                conn.disconnect();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return notExistingMapperDbs;
    }

    @Override
    public boolean distributeCollection(final String collection, final String name,
        final DistributionAlgorithm algorithm) throws Exception {
        mRefactoringServers.clear();
        boolean isSuccessful = true;
        long start = System.nanoTime();
        // input folder containing XML documents to be stored.
        String tempName = name + "-temp";
        final File inputDir = new File(collection);
        mTrans = TransformerFactory.newInstance().newTransformer();
        mTrans.setOutputProperty(OutputKeys.INDENT, "no");
        mTrans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        if (inputDir.isDirectory()) {
            System.out.println("Start import collection...");
            long sum = -1;
            runner = 0;
            switch (algorithm) {
            case ROUND_ROBIN_SIMPLE:
                System.out.println("Execute round robin simple");
                sum = distributeRoundRobinSimple(inputDir, name);
                break;
            case ROUND_ROBIN_CHUNK:
                System.out.println("Execute round robin chunk");
                sum = distributeRoundRobinChunked(inputDir, tempName);
                for (String server : mRefactoringServers) {
                    mDistributionService = new DistributionService(server);
                    mDistributionService.createEmptyCollection(name);
                    mDistributionService.runRefactoring(tempName, name);
                    mDistributionService.deleteTemporaryCollection(tempName);
                }
                break;
            case ADVANCED:
                System.out.println("Execute advanced");
                sum = distributeAdvancedSimple(inputDir, name);
                break;
            case ADVANCED_CHUNK:
                System.out.println("Execute advanced chunk");
                sum = distributeAdvancedChunked(inputDir, tempName);
                for (String server : mRefactoringServers) {
                    mDistributionService = new DistributionService(server);
                    mDistributionService.createEmptyCollection(name);
                    mDistributionService.runRefactoring(tempName, name);
                    mDistributionService.deleteTemporaryCollection(tempName);
                }
                break;
            case PARTITIONING:
                System.out.println("Execute partitioned");

                long completeSize = folderSize(inputDir);
                System.out.println("Ram size: " + mMeta.getServerMeta().getRam());
                long amountPackages;
                double a = completeSize / mMeta.getServerMeta().getRam();
                if ((completeSize % mMeta.getServerMeta().getRam()) == 0)
                    amountPackages = (long)a;
                else
                    amountPackages = (long)a + 1;
                System.out.println("Packages: " + amountPackages);
                mPackageSize = (long)((completeSize / amountPackages) * 1.1);
                System.out.println("Package size: " + mPackageSize);
                System.out.println("Directory size: " + completeSize);
                sum = distributePartitioned(inputDir, tempName);
                for (String server : mRefactoringServers) {
                    mDistributionService = new DistributionService(server);
                    mDistributionService.createEmptyCollection(name);
                    mDistributionService.runRefactoring(tempName, name);
                    mDistributionService.deleteTemporaryCollection(tempName);
                }
                break;
            default:
                System.out.println("Not supported");
                break;
            }
            System.out.println("\nAmount of imported files: " + sum);
        } else if (inputDir.getAbsolutePath().endsWith(XML_TYPE)) {
            System.out.println("Distributing one single XML file");
            // start subcollection tag
            mDistributionService = new DistributionService(next(mDataServersArray, 1));
            // init
            mDistributionService.initUpdate(tempName);
            mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
            BufferedInputStream is = new BufferedInputStream(new FileInputStream(inputDir));
            mTrans.transform(new StreamSource(is), new StreamResult(mBos));
            is.close();
            mBos.close();
            isSuccessful = mDistributionService.execUpdate();
        } else
            System.err.println("False input path. Try again.");
        System.out.println("Progress: 100.0 %.");
        System.out.println("Import finished.");
        long end = System.nanoTime() - start;
        System.out.println("Done in " + ((double)end / 1000000000.0) + " s");
        return isSuccessful;
    }

    /**
     * Gets all current states of executed XQuery files.
     * 
     * @return states.
     */
    public Map<String, Integer> getStates() {
        return mStates;
    }

    @Override
    public void execute(final String xq, final OutputStream output) {

        ExecutorService executor = Executors.newFixedThreadPool(mDestinationMappers.size());

        for (String mapperFile : mDestinationMappers) {
            final String entry = mapperFile;
            Callable<Void> task = new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    long start = System.nanoTime();
                    runQuery(entry, output);
                    long time = System.nanoTime() - start;
                    System.out.println("Time for " + entry + ": " + time / 1000000 + " ms");

                    return null;
                }
            };
            executor.submit(task);
        }
        // This will make the executor accept no new threads
        // and finish all existing threads in the queue
        executor.shutdown();
        // Wait until all threads are finish
        while(!executor.isTerminated())
            ;
    }

    /**
     * Executes an HTTP PUT request.
     * 
     * @param targetResource
     *            The URL resource address.
     * @return <code>true</code> if the creation process is successful, <code>false</code> otherwise.
     */
    private boolean put(final String targetResource) {
        boolean isCreated = false;
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(PUT);

            OutputStream out = new BufferedOutputStream(conn.getOutputStream());
            InputStream in =
                new BufferedInputStream(RestClient.class.getResourceAsStream("/" + DOC + XML_TYPE));
            for (int i; (i = in.read()) != -1;)
                out.write(i);
            in.close();
            out.close();

            System.out.println("\n* HTTP response: " + conn.getResponseCode() + " ("
            + conn.getResponseMessage() + ")");
            conn.disconnect();
            isCreated = true;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isCreated;
    }

    /**
     * Executes a query file over HTTP GET.
     * 
     * @param targetResource
     *            URL address.
     * @return Query result or <code>null</code> if an error occurred.
     */
    private String runQuery(final String targetResource) {
        String result = null;
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF8));

                StringBuffer sb = new StringBuffer();
                for (String line; (line = br.readLine()) != null;) {
                    sb.append(line);
                }
                br.close();
                result = sb.toString();
            } else {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), UTF8));
                for (String line; (line = br.readLine()) != null;) {
                    System.out.println(line);
                }
                br.close();
            }

            conn.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Executes a query file over HTTP GET.
     * 
     * @param targetResource
     *            URL address.
     * @param output
     *            {@link OutputStream} for writing results in.
     * @throws InterruptedException
     */
    private void runQuery(final String targetResource, final OutputStream output) throws InterruptedException {
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK) {
                BufferedInputStream bis = new BufferedInputStream(conn.getInputStream());
                LOCK.acquire();
                int i;
                while((i = bis.read()) != -1)
                    output.write(i);
                LOCK.release();
                bis.close();
            } else {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), UTF8));
                for (String line; (line = br.readLine()) != null;) {
                    System.out.println(line);
                }
                br.close();
            }

            conn.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
     * @throws TransformerException
     */
    private long distributeRoundRobinChunked(final File dir, final String name) throws IOException,
        TransformerException {
        File[] files = dir.listFiles();
        long count = 0;

        // name of collection in distributed storage.
        final String collectionName = name;
        int creator = 0;
        for (File file : files) {
            if (file.getAbsolutePath().endsWith(XML_TYPE)) {
                // nur beim start ausgef�hrt;
                if (mOutSize == 0) {
                    // start subcollection tag
                    System.out.println("start col");
                    String host = next(mDataServersArray, runner++);
                    mRefactoringServers.add(host);
                    mDistributionService = new DistributionService(host);
                    // init
                    if (creator < mDataServersArray.length) {
                        mDistributionService.createEmptyCollection(collectionName);
                    }
                    mDistributionService.initAdd(collectionName, file.getName());
                    mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
                    mBos.write(COL_START);
                }

                else if ((mOutSize + file.length() > PACKAGE_SIZE)) {
                    // file to big. close first and write new
                    mBos.write(COL_END);
                    mBos.close();
                    if (creator < mDataServersArray.length) {
                        creator++;
                    }
                    mDistributionService.execAdd();
                    mOutSize = 0;
                    // start subcollection tag
                    String host = next(mDataServersArray, runner++);
                    mRefactoringServers.add(host);
                    mDistributionService = new DistributionService(host);
                    // init
                    if (creator < mDataServersArray.length) {
                        mDistributionService.createEmptyCollection(collectionName);
                    }
                    mDistributionService.initAdd(collectionName, file.getName());
                    mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
                    mBos.write(COL_START);
                }
                byte[] startDoc = Token.token("<document path='" + file.getAbsolutePath() + "'>");
                mBos.write(startDoc);
                BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
                // mTrans.transform(new StreamSource(is), new StreamResult(mBos));
                writeFileToOutput(is, mBos);
                is.close();
                byte[] endDoc = Token.token("</document>");
                mBos.write(endDoc);
                mOutSize = mOutSize + file.length();
                count++;
            } else if (file.isDirectory()) {
                mH++;
                count += distributeRoundRobinChunked(file, name);
            }
        }
        if (mH < 1) {
            mBos.write(COL_END);
            mBos.close();
            if (creator < mDataServersArray.length) {
                creator++;
            }
            mDistributionService.execAdd();
        }
        mH--;
        // user feedback
        if ((count / 10 > 0) && count != mLast) {
            System.out.print(".");
            mLast = count;
        }
        return count;
    }

    /**
     * Traverses an input directory for distribution of collection via one connection per document.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     * @throws TransformerException
     */
    private long distributeRoundRobinSimple(final File dir, final String name) throws IOException {
        File[] files = dir.listFiles();
        long count = 0;

        // name of collection in distributed storage.
        for (File file : files) {
            if (file.getAbsolutePath().endsWith(XML_TYPE)) {
                String host = next(mDataServersArray, runner++);
                mDistributionService = new DistributionService(host);
                if (!mMeta.containsServer(host) || !existDbOnServer(mMeta.getDbList(host), name)) {
                    mMeta.addDb(host, name);
                    mDistributionService.createEmptyCollection(name);
                }
                mDistributionService.initAdd(name, file.getAbsolutePath());
                BufferedOutputStream bos = (BufferedOutputStream)mDistributionService.getOutputStream();
                BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
                int i;
                while((i = is.read()) != -1) {
                    bos.write(i);
                }
                is.close();
                mDistributionService.execAdd();
                count++;

            } else if (file.isDirectory()) {
                count += distributeRoundRobinSimple(file, name);
            }
        }
        // user feedback
        if ((count / 20 > 0) && count != mLast) {
            System.out.print(".");
            mLast = count;
        }
        return count;
    }

    /**
     * Traverses an input directory for distribution of collection via Advanced algorithm via one connection
     * per document.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     */
    private long distributeAdvancedSimple(final File dir, final String name) throws IOException {
        File[] files = dir.listFiles();
        long count = 0;

        // name of collection in distributed storage.
        for (File file : files) {
            if (file.getAbsolutePath().endsWith(XML_TYPE)) {
                if (mOutSize == 0) {
                    mHost = next(mDataServersArray, runner++);
                    mDistributionService = new DistributionService(mHost);
                } else if (mOutSize + file.length() > mMeta.getServerMeta().getRam()) {
                    mHost = next(mDataServersArray, runner++);
                    mDistributionService = new DistributionService(mHost);
                    mOutSize = 0;
                }
                if (!mMeta.containsServer(mHost) || !existDbOnServer(mMeta.getDbList(mHost), name)) {
                    System.out.println(mHost);
                    mMeta.addDb(mHost, name);
                    mDistributionService.createEmptyCollection(name);
                }
                mDistributionService.initAdd(name, file.getAbsolutePath());
                BufferedOutputStream bos = (BufferedOutputStream)mDistributionService.getOutputStream();
                BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
                int i;
                while((i = is.read()) != -1) {
                    bos.write(i);
                }
                is.close();
                mDistributionService.execAdd();
                count++;
                mOutSize += file.length();

            } else if (file.isDirectory()) {
                count += distributeAdvancedSimple(file, name);
            }
        }
        // user feedback
        if ((count / 20 > 0) && count != mLast) {
            System.out.print(".");
            mLast = count;
        }
        return count;
    }

    /**
     * Traverses an input directory for distribution of collection via Advanced algorithm via one connection
     * per chunk.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     * @throws TransformerException
     */
    private long distributeAdvancedChunked(final File dir, final String name) throws IOException,
        TransformerException {
        File[] files = dir.listFiles();
        long count = 0;

        // name of collection in distributed storage.
        final String collectionName = name;
        int creator = 0;
        for (File file : files) {
            if (file.getAbsolutePath().endsWith(XML_TYPE)) {
                // nur beim start ausgef�hrt;
                if (mOutSize == 0) {
                    // start subcollection tag
                    System.out.println("start col");
                    String host = next(mDataServersArray, runner++);
                    System.out.println(host);
                    mRefactoringServers.add(host);
                    mDistributionService = new DistributionService(host);
                    // init
                    if (creator < mDataServersArray.length) {
                        mDistributionService.createEmptyCollection(collectionName);
                    }
                    mDistributionService.initAdd(collectionName, file.getName());
                    mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
                    mBos.write(COL_START);
                }

                else if ((mOutSize + file.length() > mPackageSize)) {
                    // file to big. close first and write new
                    mBos.write(COL_END);
                    mBos.close();
                    if (creator < mDataServersArray.length) {
                        creator++;
                    }
                    mDistributionService.execAdd();
                    mOutSize = 0;
                    // start subcollection tag
                    String host = next(mDataServersArray, runner++);
                    mRefactoringServers.add(host);
                    mDistributionService = new DistributionService(host);
                    // init
                    if (creator < mDataServersArray.length) {
                        mDistributionService.createEmptyCollection(collectionName);
                    }
                    mDistributionService.initAdd(collectionName, file.getName());
                    mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
                    mBos.write(COL_START);
                }
                byte[] startDoc = Token.token("<document path='" + file.getAbsolutePath() + "'>");
                mBos.write(startDoc);
                BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
                // mTrans.transform(new StreamSource(is), new StreamResult(mBos));
                writeFileToOutput(is, mBos);
                is.close();
                byte[] endDoc = Token.token("</document>");
                mBos.write(endDoc);
                mOutSize = mOutSize + file.length();
                count++;
            } else if (file.isDirectory()) {
                mH++;
                count += distributeAdvancedChunked(file, name);
            }
        }
        if (mH < 1) {
            mBos.write(COL_END);
            mBos.close();
            if (creator < mDataServersArray.length) {
                creator++;
            }
            mDistributionService.execAdd();
        }
        mH--;
        // user feedback
        if ((count / 10 > 0) && count != mLast) {
            System.out.print(".");
            mLast = count;
        }
        return count;
    }

    /**
     * Traverses an input directory for distribution of collection via Partitioning algorithm.
     * 
     * @param dir
     *            Input directory.
     * @param name
     *            Name of collection.
     * @param serverIds
     *            Server IDs.
     * @return Distributed files count.
     * @throws IOException
     * @throws TransformerException
     */
    private long distributePartitioned(final File dir, final String name) throws IOException,
        TransformerException {
        File[] files = dir.listFiles();
        long count = 0;

        // name of collection in distributed storage.
        final String collectionName = name;
        int creator = 0;
        for (File file : files) {
            if (file.getAbsolutePath().endsWith(XML_TYPE)) {
                // nur beim start ausgef�hrt;
                if (mOutSize == 0) {
                    // start subcollection tag
                    System.out.println("start col");
                    String host = next(mDataServersArray, runner++);
                    System.out.println(host);
                    mRefactoringServers.add(host);
                    mDistributionService = new DistributionService(host);
                    // init
                    if (creator < mDataServersArray.length) {
                        mDistributionService.createEmptyCollection(collectionName);
                    }
                    mDistributionService.initAdd(collectionName, file.getName());
                    mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
                    mBos.write(COL_START);
                }

                else if ((mOutSize + file.length() > mPackageSize)) {
                    // file to big. close first and write new
                    mBos.write(COL_END);
                    mBos.close();
                    if (creator < mDataServersArray.length) {
                        creator++;
                    }
                    mDistributionService.execAdd();
                    mOutSize = 0;
                    // start subcollection tag
                    String host = next(mDataServersArray, runner++);
                    mRefactoringServers.add(host);
                    mDistributionService = new DistributionService(host);
                    // init
                    if (creator < mDataServersArray.length) {
                        mDistributionService.createEmptyCollection(collectionName);
                    }
                    mDistributionService.initAdd(collectionName, file.getName());
                    mBos = new BufferedOutputStream(mDistributionService.getOutputStream());
                    mBos.write(COL_START);
                }
                byte[] startDoc = Token.token("<document path='" + file.getAbsolutePath() + "'>");
                mBos.write(startDoc);
                BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
                // mTrans.transform(new StreamSource(is), new StreamResult(mBos));
                writeFileToOutput(is, mBos);
                is.close();
                byte[] endDoc = Token.token("</document>");
                mBos.write(endDoc);
                mOutSize = mOutSize + file.length();
                count++;
            } else if (file.isDirectory()) {
                mH++;
                count += distributePartitioned(file, name);
            }
        }
        if (mH < 1) {
            mBos.write(COL_END);
            mBos.close();
            if (creator < mDataServersArray.length) {
                creator++;
            }
            // mDistributionService.execAdd();
        }
        mH--;
        // user feedback
        // if ((count / 10 > 0) && count != mLast) {
        // System.out.print(".");
        // mLast = count;
        // }
        return count;
    }

    /**
     * Checks if the searched database exists already.
     * 
     * @param dbs
     *            Available databases.
     * @param name
     *            Name of the searched database.
     * @return <code>true</code> if the database exists, <code>false</code> otherwise.
     */
    private boolean existDbOnServer(final List<String> dbs, final String name) {
        boolean result = false;
        for (String db : dbs) {
            if (db.equals(name)) {
                result = true;
                break;
            }
        }
        return result;
    }

    /**
     * This method return the next host in a round robin manner to support
     * uniform distribution.
     * 
     * @param server
     *            servers.
     * @param loop
     *            runner.
     * @return next host name.
     */
    private String next(final String[] server, final int runner) {
        return server[runner % server.length];
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
            if (file.isFile() && file.getAbsolutePath().endsWith(XML_TYPE))
                length += file.length();
            else if (file.isDirectory())
                length += folderSize(file);
        }
        return length;
    }

    /**
     * Writes input file to output stream.
     * 
     * @param file
     *            Input file.
     * @param output
     *            {@link BufferedOutputStream} for HTTP execution.
     * @throws IOException
     */
    private void writeFileToOutput(final InputStream file, final BufferedOutputStream output)
        throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(file, UTF8));
        br.readLine();
        br.readLine();
        // int c;
        String l;
        while((l = br.readLine()) != null) {
            output.write(Token.token(l));
        }
        br.close();

    }

}

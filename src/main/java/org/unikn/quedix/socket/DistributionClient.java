package org.unikn.quedix.socket;

import static org.unikn.quedix.rest.Constants.UTF8;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.basex.util.Token;
import org.unikn.quedix.core.DistributionAlgorithm;

/**
 * This class is responsible for distribution of XML collections using BaseX'
 * client API (directly over Sockets).
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class DistributionClient extends SocketClient implements org.unikn.quedix.core.Distributor {

    /** Package size. */
    private static final int PACKAGE_SIZE = 67108864;
    /** Start subcollection name. */
    private static final String SUB_COLLECTION_NAME = "subcollection";
    /** Start subcollection root tag name. */
    private static final String START = "<" + SUB_COLLECTION_NAME + ">";
    /** End subcollection root tag name. */
    private static final String END = "</" + SUB_COLLECTION_NAME + ">";
    private static final byte[] COL_START = Token.token(START);
    private static final byte[] COL_END = Token.token(END);
    private static final byte[] DOC_END = Token.token("</document>");
    private static final String DOC_START_A = "<document path='";
    private static final String DOC_START_B = "'>";
    /** XQ for refactoring subcollection workaround. */
    private static final String RQ = "refactor2.xq";
    /** Autoflush. */
    private static final String SET_AUTO_FLUSH_FALSE = "SET AUTOFLUSH false";
    /** Execute flush. */
    private static final String EXE_FLUSH = "FLUSH";

    /** Written chunks. */
    private long mOutSize = 0;
    /** Index. */
    private int mInd = 0;
    /** Height of method. */
    private int mH = 0;
    /** Output stream. */
    private BufferedOutputStream mBos;
    /** Last user feedback check. */
    private long mLast = 0;
    private List<BaseXClient> mClientsForFlushing = new ArrayList<BaseXClient>();

    /**
     * Constructor connects clients to BaseX server.
     * 
     * @param clients
     *            {@link Map} of clients to server mapping.
     * @throws IOException
     *             Exception occurred.
     */
    public DistributionClient(final Map<String, BaseXClient> clients,
        final org.unikn.quedix.core.MetaData meta) throws IOException {
        super(clients, meta);
        buildRefactoringString();
    }

    @Override
    public boolean distributeCollection(final String collection, final String name,
        final DistributionAlgorithm algorithm) throws IOException {
        boolean isSuccessful = true;
        mClientsForFlushing.clear();
        long start = System.nanoTime();
        // input folder containing XML documents to be stored.
        final File inputDir = new File(collection);
        String[] serverIds = new String[mClients.size()];
        // input folder containing XML documents to be stored.
        String tempName = name + "-temp";
        int i = 0;
        for (Map.Entry<String, BaseXClient> entry : mClients.entrySet())
            serverIds[i++] = entry.getKey();
        if (inputDir.isDirectory()) {
            System.out.println("Start import collection...");
            long sum = -1;
            switch (algorithm) {
            case ROUND_ROBIN_SIMPLE:
                mIsFirst = true;
                System.out.println("Execute round robin simple");
                sum = distributeRoundRobin(inputDir, name, serverIds);
                exeFlush();
                break;
            case ROUND_ROBIN_CHUNK:
                System.out.println("Execute round robin chunked");
                mIsFirst = true;
                sum = distributeRoundRobinChunked(inputDir, tempName, serverIds);
                for (BaseXClient server : mRefactoring) {
                    runRefactoring(server, tempName, name);
                }
                break;
            case ADVANCED:
                mIsFirst = true;
                System.out.println("Execute round robin advanced");
                sum = distributeAdvanced(inputDir, name, serverIds);
                exeFlush();
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
    public void distributeXml(final BaseXClient client, final String name, final BufferedInputStream bis,
        final File file) throws IOException {
        if (mIsFirst) {
            if (checkCollectionExistence(client, name)) {
                client.execute(OPEN + name);
                client.execute(SET_AUTO_FLUSH_FALSE);
            } else {
                client.createCol(name);
                client.execute(SET_AUTO_FLUSH_FALSE);
                mMeta.addDb(client.ehost, name);
            }
        }
        client.add(file.getAbsolutePath(), bis);
        mIsFirst = false;
    }

    /**
     * Distributes file chunked.
     * 
     * @param client
     *            {@link BaseXClient} instance.
     * @param name
     *            Collection file.
     * @param bis
     *            {@link BufferedReader} holding XML file.
     * @param file
     *            {@link File} reference.
     * @throws IOException
     *             Exception while writing with client.
     */
    private void distributeXmlChunked(final BaseXClient client, final String name, final BufferedReader br,
        final File file, final BufferedOutputStream os) throws IOException {
        if (mIsFirst) {
            if (checkCollectionExistence(client, name)) {
                client.execute(OPEN + name);
                client.execute(SET_AUTO_FLUSH_FALSE);
            } else {
                client.createCol(name);
                client.execute(SET_AUTO_FLUSH_FALSE);
                mMeta.addDb(client.ehost, name);
            }
            client.addPrepare(file.getAbsolutePath());
        }
        String l;
        br.readLine();
        br.readLine();
        while((l = br.readLine()) != null) {
            os.write(Token.token(l));
        }
        // os.flush();
        mIsFirst = false;
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
    public long distributeRoundRobin(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                mClient = next(serverIds, mInd++);
                mIsFirst = true;
                mClientsForFlushing.add(mClient);
                distributeXml(mClient, name, bis, file);
                bis.close();
                count++;
            } else if (file.isDirectory()) {
                count += distributeRoundRobin(file, name, serverIds);
            }

            // user feedback
            if ((count % 100 == 0) && count != mLast) {
                System.out.print(".");
                mLast = count;
            }
        }
        return count;
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
    public long distributeRoundRobinChunked(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {

                if (mOutSize == 0) {
                    mClient = next(serverIds, mInd++);
                    System.out.println("Distribution to " + mClient.ehost);
                    mRefactoring.add(mClient);
                    mBos = new BufferedOutputStream(mClient.getOutputStream());
                    mBos.write(COL_START);

                } else if (mOutSize + file.length() > PACKAGE_SIZE) {
                    mBos.write(COL_END);
                    mBos.write(0);
                    mBos.flush();
                    String info = mClient.receive();
                    if (!mClient.ok())
                        System.err.println(info);
                    mClient = next(serverIds, mInd++);
                    System.out.println("Distribution to " + mClient.ehost);
                    mBos = new BufferedOutputStream(mClient.getOutputStream());
                    mBos.write(COL_START);
                    mRefactoring.add(mClient);
                    mIsFirst = true;
                    mOutSize = 0;
                }

                BufferedReader br =
                    new BufferedReader(new InputStreamReader(new FileInputStream(file), UTF8));
                byte[] startDoc = Token.token(DOC_START_A + file.getAbsolutePath() + DOC_START_B);
                mBos.write(startDoc);
                distributeXmlChunked(mClient, name, br, file, mBos);
                mBos.write(DOC_END);
                br.close();
                count++;
                mOutSize += file.length();
            } else if (file.isDirectory()) {
                mH++;
                count += distributeRoundRobinChunked(file, name, serverIds);
            }

        }
        if (mH < 1) {
            mBos.write(COL_END);
            mBos.write(0);
            mBos.flush();
            System.out.println("Executed to " + mClient.ehost);
            String info = mClient.receive();
            if (!mClient.ok())
                System.err.println(info);
            // mBos.close();
        }
        mH--;

        // user feedback
        if ((count % 100 == 0) && count != mLast) {
            System.out.print(".");
            mLast = count;
        }
        return count;
    }

    /**
     * Distributes collection using the advanced algorithm using addition meta
     * information.
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
    public long distributeAdvanced(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                // nur beim start ausgefuehrt;
                if (mOutSize == 0) {
                    mClient = next(serverIds, mInd++);
                    mIsFirst = true;
                    mClientsForFlushing.add(mClient);
                    distributeXml(mClient, name, bis, file);
                } else if ((mOutSize + file.length()) > mMeta.getServerMeta().getRam()) {
                    mOutSize = 0;
                    mClient = next(serverIds, mInd++);
                    mIsFirst = true;
                    mClientsForFlushing.add(mClient);
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
            if ((count % 100 == 0) && count != mLast) {
                System.out.print(".");
                mLast = count;
            }
        }
        return count;
    }

    /**
     * Distributes collection using the partitioning algorithm using addition
     * meta information.
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
    public long distributePartitioned(final File dir, final String name, final String[] serverIds)
        throws IOException {
        File[] files = dir.listFiles();
        long count = 0;
        for (File file : files) {
            if (file.isFile() && file.getAbsolutePath().endsWith(XML)) {
                BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                // nur beim start ausgefuehrt;
                if (mOutSize == 0) {
                    mClient = next(serverIds, mInd++);
                    mIsFirst = true;
                } else if ((mOutSize + file.length()) > mPartitionedPackage) {
                    mOutSize = 0;
                    mClient = next(serverIds, mInd++);
                    mIsFirst = true;
                }
                distributeXml(mClient, name, bis, file);
                mOutSize += file.length();
                bis.close();
                count++;
            } else if (file.isDirectory()) {
                count += distributePartitioned(file, name, serverIds);
            }

            // user feedback
            if ((count % 100 == 0) && count != mLast) {
                System.out.print(".");
                mLast = count;
            }
        }
        return count;
    }

    /**
     * Build refactoring String.
     * 
     * @throws IOException
     */
    private void buildRefactoringString() throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br =
            new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/" + RQ)));
        String l;
        while((l = br.readLine()) != null) {
            sb.append(l + "\n");
        }
        setRefactorXq(sb.toString());
    }

    /**
     * Forces to flush.
     * 
     * @throws IOException
     */
    private void exeFlush() throws IOException {
        for (BaseXClient baseXClient : mClientsForFlushing) {
            baseXClient.execute(EXE_FLUSH);
        }
    }

}

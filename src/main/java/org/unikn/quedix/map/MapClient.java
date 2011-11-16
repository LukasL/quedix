package org.unikn.quedix.map;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.basex.query.QueryException;
import org.basex.util.Token;
import org.unikn.quedix.core.Client;
import org.unikn.quedix.reduce.ReduceClient;

/**
 * This class is the client representation for executing/sending map XQuery
 * scripts.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class MapClient {

    /** Start tag. */
    private static final String START = "<results>";
    /** End tag. */
    private static final String END = "</results>";
    /** Client. */
    private Client mClient;
    /** XQ file name for mapping. */
    private String mMappingXq;
    /** XQ file for mapping. */
    private File mMappingFile;
    /** Reducer. */
    private ReduceClient mReducer;

    /**
     * Constructor sets existing {@link Client} instance.
     * 
     * @param client
     *            {@link Client} instance.
     * @param xq
     *            XQ file for mapping.
     */
    public MapClient(final Client client, final File xq) {
        mMappingXq = xq.getName();
        System.out.println("XQ file name: " + mMappingXq);
        mMappingFile = xq;
        mClient = client;
        for (String updateDataServer : mClient.checkMapperDb())
            mClient.createMapperDb(updateDataServer);
    }

    /**
     * Constructor sets existing {@link Client} instance.
     * 
     * @param client
     *            {@link Client} instance.
     * @param xq
     *            XQ file for mapping.
     * @param reducer
     *            Reducer instance.
     */
    public MapClient(final Client client, final File xq, final ReduceClient reducer) {
        mMappingXq = xq.getName();
        mReducer = reducer;
        System.out.println("XQ file name: " + mMappingXq);
        mMappingFile = xq;
        mClient = client;
        for (String updateDataServer : mClient.checkMapperDb())
            mClient.createMapperDb(updateDataServer);
    }

    /**
     * Distributes a XQ file.
     */
    public void distribute() {
        try {
            mClient.distributeXq(readByteArray(mMappingFile));
        } catch (final IOException exce) {
            exce.printStackTrace();
        }
    }

    /**
     * Executes query files in parallel.
     * 
     * @param xq
     *            XQ which has to be executed in parallel.
     */
    public void execute() {
        try {
            final PipedOutputStream pos = new PipedOutputStream();
            final PipedInputStream pis = new PipedInputStream(pos);
            executeReadPipeThread(pis);
            mReducer.sendReducerTask();
            final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(pos));
            out.write(Token.token(START));
            mClient.execute(mMappingXq, out);
            out.write(Token.token(END));
            out.close();
            pos.close();
        } catch (final IOException exc) {
            exc.printStackTrace();
        } catch (final QueryException exc) {
            exc.printStackTrace();
        }
    }

    /**
     * Deletes a query file over HTTP DELETE.
     * 
     * @param targetResource
     *            URL address.
     */
    public void cleanup() {
        mClient.delete();
    }

    /**
     * Reads input file and writes it to a byte array.
     * 
     * @param file
     *            File name.
     * @return Byte array representation of file.
     * @throws IOException
     *             Exception occurred.
     */
    private byte[] readByteArray(final File file) throws IOException {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int i;
        while((i = input.read()) != -1)
            bos.write(i);
        input.close();
        byte[] content = bos.toByteArray();
        bos.close();
        return content;
    }

    /**
     * Reads data out of {@link InputStream}.
     * 
     * @param is
     *            {@link InputStream} instance.
     */
    private void readData(final InputStream is) {
        DataInputStream in = new DataInputStream(new BufferedInputStream(is));

        try {
            mReducer.execute(in, null);
            in.close();
        } catch (final IOException exc) {
            exc.printStackTrace();
        } catch (final QueryException exc) {
            exc.printStackTrace();
        }
    }

    /**
     * Executes a reader thread for receiving items from {@link PipedOutputStream}.
     * 
     * @param pis
     *            {@link PipedInputStream} instance.
     */
    private void executeReadPipeThread(final InputStream pis) {
        ExecutorService es = Executors.newFixedThreadPool(1);
        Callable<Void> task = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                readData(pis);
                return null;
            }
        };
        es.submit(task);
        es.shutdown();

    }
}

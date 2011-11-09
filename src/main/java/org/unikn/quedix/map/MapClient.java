package org.unikn.quedix.map;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.unikn.quedix.core.Client;

/**
 * This class is the client representation for executing/sending map XQuery scripts.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class MapClient {

    /** Client. */
    private Client mClient;
    /** XQ file name for mapping. */
    private String mMappingXq;
    /** XQ file for mapping. */
    private File mMappingFile;

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
        for (String res : mClient.execute(mMappingXq)) {
            System.out.println(res);
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
}

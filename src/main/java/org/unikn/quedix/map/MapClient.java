/**
 * Copyright (c) 2010, Distributed Systems Group, University of Konstanz
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED AS IS AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * 
 */

package org.unikn.quedix.map;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.unikn.quedix.Client;
import org.unikn.quedix.rest.RestClient;

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
     * Default constructor.
     * 
     * @param xq
     *            XQ file for mapping.
     * 
     */
    public MapClient(final File xq, final Map<String, String> servers) {
        this(new RestClient(servers), xq);
    }

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
            mClient.distribute(readByteArray(mMappingFile));
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
        mClient.delete(mMappingXq);
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

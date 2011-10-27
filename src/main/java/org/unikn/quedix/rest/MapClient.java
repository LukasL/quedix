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

package org.unikn.quedix.rest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is the client representation for executing/sending map XQuery scripts.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class MapClient {

    /** MapperDb name for holding mapping query files. */
    private static final String MAPPER_DB = "MapperDb";
    /** Collection or database context for the mapper execution tasks. */
    private String mCollectionContext;
    /** REST client. */
    private RestClient mClient;

    /**
     * Default constructor.
     */
    public MapClient() {
        this(new RestClient());
    }

    /**
     * Constructor sets existing {@link RestClient} instance.
     * 
     * @param client
     *            {@link RestClient} instance.
     */
    public MapClient(final RestClient client) {
        mClient = client;
    }

    public void setContextCollection(final String collection) {
        mCollectionContext = collection;
    }

    public void sendMapperTask(final InputStream xQueryMapper) {
        for (String dataServer : checkMapperDb())
            createMapperDb(dataServer + "/" + MAPPER_DB);
        //TODO send xquery file to the data servers

    }

    public void sendReducerTask(final InputStream xQueryReducer) {

    }

    private void createMapperDb(final String targetResource) {
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");

            if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                // TODO Exception werfen, da ausführung irgendwie nicht möglich
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

    private List<String> checkMapperDb() {
        List<String> notExistingMapperDbs = new ArrayList<String>();
        for (Map.Entry<String, String> dataServers : mClient.getDataServers().entrySet()) {
            URL url;
            try {
                url = new URL(dataServers.getKey() + "/" + MAPPER_DB);
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

    public String getContextCollection() {
        return mCollectionContext;
    }

}

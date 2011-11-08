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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * This class is responsible to distribute a collection of XML documents to the data servers via HTTP.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class DistributionService {

    /** HTTP PUT string. */
    private final String PUT = "PUT";
    /** HTTP POST string. */
    private final String POST = "POST";
    /** Content type string. */
    private final String CONTENT_TYPE_STRING = "Content-type";
    /** Content type text/xml. */
    private final String TEXT_XML = "application/xml";

    /** Registered servers to connect to. */
    private String[] mServers;
    /** {@link OutputStream} from REST request. */
    private OutputStream output;
    /** {@link HttpURLConnection} connection instance. */
    private HttpURLConnection conn;

    /**
     * Constructor creates REST calls to one server.
     * 
     * @param server
     *            server address.
     */
    public DistributionService(final String server) {
        this(new String[] {
            server
        });
    }

    /**
     * Constructor creates a service for REST calls to several servers, e.g., for replication
     * reasons.
     * 
     * @param servers
     *            Registered servers.
     */
    public DistributionService(final String[] servers) {
        mServers = servers;
    }

    /**
     * This method creates a new document, or if one is existing with this name, the old one will be replaced.
     * 
     * @param name
     *            Name of document.
     * @param document
     *            Document content.
     * @return <code>true</code> if call was successful, <code>false</code> otherwise.
     * @throws Exception
     *             occurred with remote call.
     */
    public boolean update(final String name, final InputStream document) throws IOException {
        for (final String host : mServers) {

            URL url = new URL(host + "/" + name);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
            conn.setRequestMethod(PUT);
            OutputStream out = new BufferedOutputStream(conn.getOutputStream());
            int i;
            while((i = document.read()) != -1)
                out.write(i);
            out.close();
            int code = conn.getResponseCode();
            conn.disconnect();
            return code == 201 ? true : false;

        }
        return false;
    }

    /**
     * This method adds a document to an existing collection.
     * 
     * @param name
     *            Name of document.
     * @param document
     *            Document content.
     * @return <code>true</code> if call was successful, <code>false</code> otherwise.
     * @throws Exception
     *             occurred with remote call.
     */
    public boolean add(final String name, final InputStream document) throws IOException {
        for (final String host : mServers) {

            URL url = new URL(host + "/" + name);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
            conn.setRequestMethod(POST);
            OutputStream out = new BufferedOutputStream(conn.getOutputStream());
            int i;
            while((i = document.read()) != -1)
                out.write(i);
            out.close();
            int code = conn.getResponseCode();
            if (code == 200)
                while(conn.getInputStream().read() != -1)
                    ;
            conn.disconnect();
            return code == 200 ? true : false;

        }
        return false;
    }

    /**
     * This method is responsible to prepare a PUT call to the server.
     * 
     * @param name
     *            The collection name.
     * @throws IOException
     *             Exception occurred.
     */
    public void initUpdate(final String name) throws IOException {
        URL url = new URL(mServers[0] + "/" + name);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(PUT);
        conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
        output = new BufferedOutputStream(conn.getOutputStream());
        this.conn = conn;
    }

    /**
     * This method is responsible to execute the prepared PUT call to the server.
     * 
     * @return <code>true</code> if the HTTP request has been successful, <code>false</code> otherwise.
     * @throws IOException
     *             Exception occurred.
     */
    public boolean execUpdate() throws IOException {
        output.close();
        conn.connect();
        int code = conn.getResponseCode();
        System.out.println("code: " + code);
        if (code == 201){
            BufferedReader r = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String l;
            while((l=r.readLine())!= null)
                System.out.println(l);
            r.close();
        }
        else {
            String l;
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            while((l = reader.readLine()) != null) {
                System.out.println(l);
            }
        }
        conn.disconnect();
        return code == 201 ? true : false;
    }

    /**
     * This method is responsible to prepare a POST request to add documents to an existing collection.
     * 
     * @param name
     *            The name of the collection.
     * @throws IOException
     *             Exception occurred.
     */
    public void initAdd(final String name) throws IOException {
        URL url = new URL(mServers[0] + "/" + name);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
        conn.setRequestMethod(POST);
        OutputStream out = new BufferedOutputStream(conn.getOutputStream());
        output = out;
        this.conn = conn;
    }

    /**
     * This method is responsible to execute the prepared HTTP POST request.
     * 
     * @return <code>true</code> if the call has been successful, <code>false</code> otherwise.
     * @throws IOException
     *             Exception occurred.
     */
    public boolean execAdd() throws IOException {
        output.close();
        int code = conn.getResponseCode();
        if (code == 200)
            while(conn.getInputStream().read() != -1)
                ;
        conn.disconnect();
        return code == 200 ? true : false;
    }

    /**
     * {@link OutputStream} from REST request.
     * 
     * @return {@link OutputStream} from REST request.
     */
    public OutputStream getOutputStream() {
        return output;
    }

}

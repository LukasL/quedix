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

/**
 * This class is the client representation for executing/sending map XQuery scripts.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class MapClient {

    /** UTF-8 string. */
    private static final String UTF8 = "UTF-8";
    /** MapperDb name for holding mapping query files. */
    private static final String MAPPER_DB = "rest/MapperDb";
    /** PUT HTTP method string. */
    private static final String PUT = "PUT";
    /** Content type string. */
    // private static final String CONTENT_TYPE_STRING = "Content-Type";
    /** Collection or database context for the mapper execution tasks. */
    private String mCollectionContext;
    /** REST client. */
    private RestClient mClient;
    /** Mappers located at destinations. */
    private List<String> mDestinationMappers;
    /** Map of executed server files inclusive state information. */
    private Map<String, Integer> mStates;

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
        for (String updateDataServer : checkMapperDb())
            createMapperDb(updateDataServer + MAPPER_DB);
        mDestinationMappers = new ArrayList<String>();
        mStates = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * Sets collection context.
     * 
     * @param collection
     *            collection for the mapper execution.
     */
    public void setContextCollection(final String collection) {
        mCollectionContext = collection;
    }

    /**
     * This method sends the user implemented XQuery mapper file to the MapperDb, where it will be executed.
     * 
     * @param xQueryMapper
     *            The XQuery mapper file as byte array.
     */
    public void sendMapperTask(final byte[] xQueryMapper) {
        ExecutorService executor = Executors.newFixedThreadPool(mClient.getDataServers().size());
        for (Map.Entry<String, String> dataServer : mClient.getDataServers().entrySet()) {
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
                        byte[] mapper = new byte[xQueryMapper.length];
                        System.arraycopy(xQueryMapper, 0, mapper, 0, xQueryMapper.length);
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

    }

    /**
     * Creates a MapperDb for holding mapper xquery files.
     * 
     * @param targetResource
     *            The resource location of the MapperDB.
     */
    private void createMapperDb(final String targetResource) {
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(PUT);

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

    /**
     * This method checks if the MapperDb exists already, to decide if we need to create a db for the mapper
     * files.
     * 
     * @return A {@link List} of data servers, where we have to create the MapperDb.
     */
    private List<String> checkMapperDb() {
        List<String> notExistingMapperDbs = new ArrayList<String>();
        for (Map.Entry<String, String> dataServers : mClient.getDataServers().entrySet()) {
            URL url;
            try {
                url = new URL(dataServers.getKey() + MAPPER_DB);
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

    /**
     * Returns the context collection for execution of the mappers.
     * 
     * @return Collection context.
     */
    public String getContextCollection() {
        return mCollectionContext;
    }

    /**
     * Executes query files in parallel.
     * 
     */
    public void executeQueryParallel() {
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

        for (Future<String> future : stringResults) {
            try {
                System.out.println(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Delete query files in parallel.
     */
    public void deleteQueryParallel() {
        ExecutorService executor = Executors.newFixedThreadPool(mDestinationMappers.size());
        for (String mapperFile : mDestinationMappers) {
            final String entry = mapperFile;
            Callable<Void> task = new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    deleteQuery(entry);
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
     * Deletes a query file over HTTP DELETE.
     * 
     * @param targetResource
     *            URL address.
     */
    private void deleteQuery(final String targetResource) {
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("DELETE");
            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF8));

                StringBuffer sb = new StringBuffer();
                for (String line; (line = br.readLine()) != null;) {
                    sb.append(line);
                }
                br.close();
                System.out.println(sb.toString());
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
     * Gets all current states of executed XQuery files.
     * 
     * @return states.
     */
    public Map<String, Integer> getStates() {
        return mStates;
    }

}

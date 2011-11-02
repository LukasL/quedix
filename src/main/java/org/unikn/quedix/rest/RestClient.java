package org.unikn.quedix.rest;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.unikn.quedix.Client;

/**
 * This class is responsible to execute parallel queries over HTTP.
 */
public class RestClient implements Client{

    /** Host name. */
    private static final String HOST = "aalto.disy.inf.uni-konstanz.de";

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
    /** Registered data servers. */
    private Map<String, String> mDataServers;

    /**
     * Main method.
     * 
     * @param args
     */
    public static void main(String[] args) {

        RestClient c = new RestClient();
        c.importExample();
        System.out.println("QUERY RESULT: ***");
        long start = System.nanoTime();

        // serial
        // c.executeQuerySerial(EQ2);
        // parallel
        c.executeQueryParallel(EQ4);
        long end = System.nanoTime() - start;
        System.out.println("Complete Time: " + end / 1000000 + " ms");

    }

    /**
     * Default constructor.
     */
    public RestClient() {
        mDataServers = initDataServers();
    }

    /**
     * Initializes the example servers.
     * 
     * @return {@link Map} of server mappings.
     */
    private Map<String, String> initDataServers() {
        Map<String, String> dataServers = new HashMap<String, String>();
        dataServers.put("http://" + HOST + ":8984/", COL);
        dataServers.put("http://" + HOST + ":8986/", COL);
        dataServers.put("http://" + HOST + ":8988/", COL);
        return dataServers;
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
     * Executes queries sequential.
     * 
     * @param query
     *            XQuery expression.
     */
    public void executeQuerySerial(final String query) {
        for (Map.Entry<String, String> serverEntry : mDataServers.entrySet()) {
            long start = System.nanoTime();
            System.out.println(query(serverEntry.getKey() + serverEntry.getValue(), query));
            long time = System.nanoTime() - start;
            System.out.println("Time for " + serverEntry.getKey() + ": " + time / 1000000 + " ms");

        }
    }

    /**
     * Executes queries in parallel.
     * 
     * @param query
     *            XQuery expression.
     */
    public void executeQueryParallel(final String query) {
        List<Future<String>> stringResults = new ArrayList<Future<String>>();
        ExecutorService executor = Executors.newFixedThreadPool(mDataServers.size());
        for (Map.Entry<String, String> serverEntry : mDataServers.entrySet()) {
            final Map.Entry<String, String> entry = serverEntry;
            Callable<String> task = new Callable<String>() {

                @Override
                public String call() throws Exception {
                    System.out.println("Result from: " + entry.getKey());
                    long start = System.nanoTime();
                    String result = query(entry.getKey() + entry.getValue(), query);
                    long time = System.nanoTime() - start;
                    System.out.println("Time for " + entry.getKey() + ": " + time / 1000000 + " ms");

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
            conn.setRequestMethod("PUT");

            OutputStream out = new BufferedOutputStream(conn.getOutputStream());
            InputStream in =
                new BufferedInputStream(RestClient.class.getResourceAsStream("/" + DOC + ".xml"));
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
     * Executes a query over HTTP POST.
     * 
     * @param targetResource
     *            URL address.
     * @param query
     *            Query expression.
     * @return Query result or <code>null</code> if an error occurred.
     */
    private String query(final String targetResource, final String query) {
        String result = null;
        URL url;
        try {
            url = new URL(targetResource);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/xml");
            OutputStream out = conn.getOutputStream();
            String request = "<query xmlns=\"http://www.basex.org/rest\"><text>" + query + "</text></query>";
            out.write(request.getBytes("UTF-8"));
            out.close();
            int code = conn.getResponseCode();
            if (code == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

                StringBuffer sb = new StringBuffer();
                for (String line; (line = br.readLine()) != null;) {
                    sb.append(line);
                }
                br.close();
                result = sb.toString();
            } else {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "UTF-8"));
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
     * Simple getter.
     * 
     * @return Available data servers.
     */
    public Map<String, String> getDataServers() {
        return mDataServers;
    }

}

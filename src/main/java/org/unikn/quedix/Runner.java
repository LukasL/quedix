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

package org.unikn.quedix;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.unikn.quedix.map.MapClient;

/**
 * This class is responsible to initiate the map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class Runner {
    /** document for importing and querying. */
    public static final String DOC = "factbook";
    /** Example collection name. */
    public static final String REST_COL = "rest/" + DOC;
    /** Host name. */
    public static final String HOST = "aalto.disy.inf.uni-konstanz.de";

    /**
     * Main.
     * 
     * @param args
     *            Program arguments are input paths to map and reduce XQuery files.
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
        new Runner(args[0]);

    }

    /**
     * Default constructor
     */
    public Runner(final String xq) {
        long start = System.nanoTime();
        // Mapper
        MapClient map = new MapClient(new File(xq), initHttpDataServers());
        map.distribute();
        map.execute();
        map.cleanup();
        long time = System.nanoTime() - start;
        System.out.println("\nComplete query execution time: " + time / 1000000 + " ms \n");

    }

    /**
     * Initializes the example servers.
     * 
     * @return {@link Map} of server mappings.
     */
    private Map<String, String> initHttpDataServers() {
        Map<String, String> dataServers = new HashMap<String, String>();
        dataServers.put("http://" + HOST + ":8984/", REST_COL);
        dataServers.put("http://" + HOST + ":8986/", REST_COL);
        dataServers.put("http://" + HOST + ":8988/", REST_COL);
        return dataServers;
    }

}

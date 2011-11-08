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

import java.util.List;

/**
 * This interface abstracts the available methods for execution of distribution, querying and deletion of our
 * map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public interface Client {

    /**
     * This method distributes a map.xq or a reduce.xq to the data server.
     * 
     * @param xq
     *            An XQ file as byte array.
     * @return <code>true</code> if the distribution has been successful, <code>false</code> otherwise.
     */
    public boolean distributeXq(final byte[] xq);

    /**
     * Executes XQ files on the server and receives results.
     * 
     * @param xq
     *            XQ file - map oder reduce file.
     * @return Results of XQ files.
     */
    public String[] execute(final String xq);

    /**
     * This method delete map.xq or reduce.xq files from the data server.
     * 
     * @return <code>true</code> if the deletion has been successful, <code>false</code> otherwise.
     */
    public boolean delete();

    /**
     * Checks if MapperDb exists already.
     * 
     * @return List of servers where MapperDb do not exist.
     */
    public List<String> checkMapperDb();

    /**
     * Creates MapperDb on server.
     * 
     * @param dataServer
     *            Server.
     */
    public void createMapperDb(final String dataServer);

    /**
     * Distributes an XML collection to the available servers.
     * 
     * @param collection
     *            Path of XML directory.
     * @param name
     *            The name of XML collection.
     * @return <code>true</code> if the distribution was successful, <code>false</code> otherwise.
     * @exception Exception
     *                Exception occurred.
     */
    public boolean distributeCollection(final String collection, final String name) throws Exception;

}

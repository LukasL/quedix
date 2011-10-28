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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * This class is responsible to initiate the map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class Runner {

    /**
     * Main.
     * 
     * @param args
     *            Program arguments are input paths to map and reduce XQuery files.
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
        Runner i = new Runner();
        byte[] mapInputFile = i.readByteArray(args[0]);
        MapClient map = new MapClient();
        map.sendMapperTask(mapInputFile);
        System.out.println("Execute XQuery files");
        map.executeQueryParallel();
        // TODO check if everything is successful and if so, delete all distributed scripts.
        System.out.println("Delete XQuery files");
        map.deleteQueryParallel();

    }

    /**
     * Reads input file and writes it to a byte array.
     * 
     * @param fileName
     *            File name.
     * @return Byte array representation of file.
     * @throws IOException
     *             Exception occurred.
     */
    private byte[] readByteArray(final String fileName) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FileInputStream fileInputStream = new FileInputStream(fileName);
        byte[] buffer = new byte[16384];
        for (int len = fileInputStream.read(buffer); len > 0; len = fileInputStream.read(buffer))
            byteArrayOutputStream.write(buffer, 0, len);
        fileInputStream.close();
        byte[] result = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        return result;

    }

}

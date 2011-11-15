package org.unikn.quedix.reduce;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.transform.sax.SAXSource;

import org.xml.sax.InputSource;

import org.basex.build.Builder;
import org.basex.build.Parser;
import org.basex.build.xml.SAXWrapper;
import org.basex.core.Context;
import org.basex.core.cmd.CreateDB;
import org.basex.data.MemData;
import org.basex.data.Result;
import org.basex.io.IO;
import org.basex.io.IOContent;
import org.basex.io.in.BufferInput;
import org.basex.query.QueryException;
import org.basex.query.QueryProcessor;

/**
 * This class is responsible to distribute the reducer task.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class ReduceClient {

    /** {@link InputStream} for local execution of reduce process. */
    private InputStream mInputStream;
    /** Context. */
    private Context mCtx;
    /** reduce process query. */
    private byte[] mReduceFile;

    /**
     * Sets input stream.
     * 
     * @param input
     *            {@link InputStream} stream.
     */
    public ReduceClient(final InputStream input) {
        mInputStream = input;
    }

    /**
     * This method sends the user implemented XQuery reducer file to the ReducerDb, where it will be executed.
     * 
     * @param xQueryReducer
     *            The XQuery reducer file as byte array.
     * @throws QueryException
     *             Query exception.
     * @throws IOException
     *             XQuery processor exception.
     */
    public void sendReducerTask(final File xQueryReducer) throws QueryException, IOException {

        // local
        mCtx = new Context();
        mReduceFile = readByteArray(xQueryReducer);
    }

    /**
     * Executes reduce query on mapping results.
     * 
     * @throws IOException
     *             Error creation of database.
     * @throws QueryException
     *             Query exception.
     */
    public void execute() throws IOException, QueryException {
        mCtx = new Context();

        // Nur well-formed XML contents erlaubt, dh keine Sequenz von Knoten ohne wrapping node :(
        SAXSource sax = new SAXSource(new InputSource(mInputStream));
        Parser p = new SAXWrapper(sax, mCtx.prop);
        MemData memData = CreateDB.mainMem(p, mCtx);
        mCtx.openDB(memData);
        QueryProcessor proc = new QueryProcessor(new String(mReduceFile), mCtx);
        Result result = proc.execute();
        System.out.println("Complete reduce result: " + result);
        memData.close();
        mCtx.close();
        proc.close();
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

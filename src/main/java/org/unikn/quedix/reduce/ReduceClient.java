package org.unikn.quedix.reduce;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.transform.sax.SAXSource;

import org.basex.build.Parser;
import org.basex.build.xml.SAXWrapper;
import org.basex.core.Context;
import org.basex.core.cmd.CreateDB;
import org.basex.data.MemData;
import org.basex.data.Result;
import org.basex.query.QueryException;
import org.basex.query.QueryProcessor;
import org.basex.util.Token;
import org.xml.sax.InputSource;

/**
 * This class is responsible to distribute the reducer task.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class ReduceClient {

	/** Context. */
	private Context mCtx;
	/** reduce process query. */
	private byte[] mReduceFile;
	/** start time. */
	private long mStart;

	/**
	 * Default.
	 * 
	 * @param xQueryReducer
	 *            reduce XQ file.
	 * @throws IOException
	 */
	public ReduceClient(final File xQueryReducer) throws IOException {

		mReduceFile = readByteArray(xQueryReducer);
	}

	/**
	 * Default 2.
	 * 
	 * @param xQueryReducer
	 *            reduce XQ file.
	 * @param start
	 *            start time.
	 * @throws IOException
	 */
	public ReduceClient(final File xQueryReducer, final long start)
			throws IOException {
		this(xQueryReducer);
		mStart = start;

	}

	/**
	 * This method sends the user implemented XQuery reducer file to the
	 * ReducerDb, where it will be executed.
	 * 
	 * @param xQueryReducer
	 *            The XQuery reducer file as byte array.
	 * @throws QueryException
	 *             Query exception.
	 * @throws IOException
	 *             XQuery processor exception.
	 */
	public void sendReducerTask() throws QueryException, IOException {

		// local
		mCtx = new Context();
	}

	/**
	 * Executes reduce query on mapping results.
	 * 
	 * @param input
	 *            {@link InputStream} containing map results.
	 * @param output
	 *            {@link OutputStream} for writing results.
	 * 
	 * @throws IOException
	 *             Error creation of database.
	 * 
	 * @throws QueryException
	 *             Query exception.
	 */
	public void execute(final InputStream input, final OutputStream output)
			throws IOException, QueryException {
		mCtx = new Context();
		// Nur well-formed XML contents erlaubt, dh keine Sequenz von Knoten
		// ohne wrapping node :(
		SAXSource sax = new SAXSource(new InputSource(input));
		Parser p = new SAXWrapper(sax, mCtx.prop);
		MemData memData = CreateDB.mainMem(p, mCtx);
		mCtx.openDB(memData);
		QueryProcessor proc = new QueryProcessor(Token.string(mReduceFile),
				mCtx);
		Result result = proc.execute();
		System.out.println("Complete reduce result: " + result);
		memData.close();
		mCtx.close();
		proc.close();

		long end = System.nanoTime() - mStart;
		System.out.println("\nComplete map and reduce execution time: " + end
				/ 1000000 + " ms \n");
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
		BufferedInputStream input = new BufferedInputStream(
				new FileInputStream(file));
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		int i;
		while ((i = input.read()) != -1)
			bos.write(i);
		input.close();
		byte[] content = bos.toByteArray();
		bos.close();
		return content;

	}

}

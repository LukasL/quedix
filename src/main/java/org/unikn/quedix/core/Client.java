package org.unikn.quedix.core;

import java.io.OutputStream;
import java.util.List;

/**
 * This interface abstracts the available methods for execution of distribution,
 * querying and deletion of our map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public interface Client {

	/**
	 * This method distributes a map.xq or a reduce.xq to the data server.
	 * 
	 * @param xq
	 *            An XQ file as byte array.
	 * @return <code>true</code> if the distribution has been successful,
	 *         <code>false</code> otherwise.
	 */
	public boolean distributeXq(final byte[] xq);

	/**
	 * Executes XQ files on the server and receives results.
	 * 
	 * @param xq
	 *            XQ file - map oder reduce file.
	 * @return Complete results of XQ files.
	 */
	public String[] execute(final String xq);

	/**
	 * Executes XQ files on the server and receives results.
	 * 
	 * @param xq
	 *            XQ file - map oder reduce file.
	 * @param output
	 *            Stream for writing results.
	 * @return Complete results of XQ files.
	 */
	public void execute(final String xq, final OutputStream output);

	/**
	 * This method delete map.xq or reduce.xq files from the data server.
	 * 
	 * @return <code>true</code> if the deletion has been successful,
	 *         <code>false</code> otherwise.
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

}

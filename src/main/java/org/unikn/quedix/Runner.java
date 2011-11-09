package org.unikn.quedix;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.unikn.quedix.map.MapClient;
import org.unikn.quedix.rest.RestClient;
import org.unikn.quedix.socket.BaseXClient;
import org.unikn.quedix.socket.SocketClient;

/**
 * This class is responsible to initiate the distribution, map and reduce tasks.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class Runner {

	/** Example document for importing and querying. */
	public static final String DOC = "factbook";
	/** REST path. */
	public static final String REST_COL = "rest/";
	/** HTTP path. */
	public static final String HTTP = "http://";
	/** Host name. */
	public static final String HOST = "aalto.disy.inf.uni-konstanz.de";
	/** User name. */
	public static final String USER = "admin";
	/** User password. */
	public static final String PW = "admin";

	/**
	 * Main.
	 * 
	 * @param args
	 *            Program arguments are input paths to map and reduce XQuery
	 *            files or XML collection.
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {

		if (args.length == 1)
			// Map
			new Runner(args[0]);
		else
			// Distribution of collection
			new Runner(args[0], args[1]);

	}

	/**
	 * Constructor distributes an XML collection.
	 * 
	 * @throws IOException
	 *             XML directory not found.
	 */
	public Runner(final String xmlDir, final String name) throws IOException {
		Client cl = new RestClient(initHttpDataServers());
		try {
			cl.distributeCollection(xmlDir, name);
		} catch (final Exception exc) {
			exc.printStackTrace();
		}
	}

	/**
	 * Constructor creates and executes a map job.
	 * 
	 * @throws IOException
	 *             XQ file not found.
	 */
	public Runner(final String xq) throws IOException {
		long start = System.nanoTime();
		// Mapper
		// MapClient map = new MapClient(new RestClient(initHttpDataServers()),
		// new File(xq));
		MapClient map = new MapClient(new SocketClient(initBaseXClients()),
				new File(xq));
		map.distribute();
		map.execute();
		map.cleanup();
		long time = System.nanoTime() - start;
		System.out.println("\nComplete mapper execution time: " + time
				/ 1000000 + " ms \n");

	}

	/**
	 * Initializes the example servers.
	 * 
	 * @return {@link Map} of server mappings.
	 */
	public Map<String, String> initHttpDataServers() {
		Map<String, String> dataServers = new HashMap<String, String>();
		dataServers.put(HTTP + HOST + ":8984/", REST_COL);
		dataServers.put(HTTP + HOST + ":8986/", REST_COL);
		dataServers.put(HTTP + HOST + ":8988/", REST_COL);
		return dataServers;
	}

	/**
	 * Initialization of BaseX clients.
	 * 
	 * @return {@link Map} of connected BaseX clients.
	 * @throws IOException
	 *             Exception occurred, e.g. servers are not running.
	 */
	public Map<String, BaseXClient> initBaseXClients() throws IOException {
		Map<String, BaseXClient> clients = new HashMap<String, BaseXClient>();
		clients.put("site1", new BaseXClient(HOST, 1980, USER, PW));
		clients.put("site2", new BaseXClient(HOST, 1981, USER, PW));
		clients.put("site3", new BaseXClient(HOST, 1982, USER, PW));
		return clients;
	}

}

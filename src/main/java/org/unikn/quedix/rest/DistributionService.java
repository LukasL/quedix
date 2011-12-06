package org.unikn.quedix.rest;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.unikn.quedix.rest.Constants.CONTENT_TYPE_STRING;
import static org.unikn.quedix.rest.Constants.DELETE;
import static org.unikn.quedix.rest.Constants.POST;
import static org.unikn.quedix.rest.Constants.PUT;
import static org.unikn.quedix.rest.Constants.TEXT_XML;

/**
 * This class is responsible to distribute a collection of XML documents to the
 * data servers via HTTP.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class DistributionService {

	/** XQ for refactoring subcollection workaround. */
	private static final String RQ = "refactor2.xq";
	/** Registered servers to connect to. */
	private String[] mServers;
	/** {@link OutputStream} from REST request. */
	private OutputStream mOutput;
	/** {@link HttpURLConnection} connection instance. */
	private HttpURLConnection mConn;

	/**
	 * Constructor creates REST calls to one server.
	 * 
	 * @param server
	 *            server address.
	 */
	public DistributionService(final String server) {
		this(new String[] { server });
	}

	/**
	 * Constructor creates a service for REST calls to several servers, e.g.,
	 * for replication reasons.
	 * 
	 * @param servers
	 *            Registered servers.
	 */
	public DistributionService(final String[] servers) {
		mServers = servers;
	}

	/**
	 * This method creates a new document, or if one is existing with this name,
	 * the old one will be replaced.
	 * 
	 * @param name
	 *            Name of document.
	 * @param document
	 *            Document content.
	 * @return <code>true</code> if call was successful, <code>false</code>
	 *         otherwise.
	 * @throws Exception
	 *             occurred with remote call.
	 */
	public boolean update(final String name, final InputStream document)
			throws IOException {
		for (final String host : mServers) {

			URL url = new URL(host + "/" + name);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
			conn.setRequestMethod(PUT);
			OutputStream out = new BufferedOutputStream(conn.getOutputStream());
			int i;
			while ((i = document.read()) != -1)
				out.write(i);
			out.close();
			int code = conn.getResponseCode();
			conn.disconnect();
			return code == 201;

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
	 * @return <code>true</code> if call was successful, <code>false</code>
	 *         otherwise.
	 * @throws Exception
	 *             occurred with remote call.
	 */
	public boolean add(final String name, final InputStream document)
			throws IOException {
		for (final String host : mServers) {

			URL url = new URL(host + "/" + name);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
			conn.setRequestMethod(POST);
			OutputStream out = new BufferedOutputStream(conn.getOutputStream());
			int i;
			while ((i = document.read()) != -1)
				out.write(i);
			out.close();
			int code = conn.getResponseCode();
			if (code == 200)
				while (conn.getInputStream().read() != -1)
					;
			conn.disconnect();
			return code == 200;

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
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(PUT);
		conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
		mOutput = new BufferedOutputStream(conn.getOutputStream());
		this.mConn = conn;
	}

	/**
	 * This method is responsible to execute the prepared PUT call to the
	 * server.
	 * 
	 * @return <code>true</code> if the HTTP request has been successful,
	 *         <code>false</code> otherwise.
	 * @throws IOException
	 *             Exception occurred.
	 */
	public boolean execUpdate() throws IOException {
		mOutput.close();
		mConn.connect();
		int code = mConn.getResponseCode();
		System.out.println("code: " + code);
		if (code == 201) {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					mConn.getInputStream()));
			String l;
			while ((l = r.readLine()) != null)
				System.out.println(l);
			r.close();
		} else {
			String l;
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					mConn.getErrorStream()));
			while ((l = reader.readLine()) != null) {
				System.out.println(l);
			}
		}
		mConn.disconnect();
		return code == 201;
	}

	/**
	 * This method is responsible to prepare a PUT request to add documents to
	 * an existing collection.
	 * 
	 * @param collection
	 *            Collection name.
	 * @param name
	 *            The name of the collection.
	 * @throws IOException
	 *             Exception occurred.
	 */
	public void initAdd(final String collection, final String name)
			throws IOException {
		URL url = new URL(mServers[0] + "/" + collection + "/" + name);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(PUT);
		conn.setRequestProperty(CONTENT_TYPE_STRING, TEXT_XML);
		conn.setChunkedStreamingMode(1);
		OutputStream out = new BufferedOutputStream(conn.getOutputStream());
		mOutput = out;
		this.mConn = conn;
	}

	/**
	 * This method is responsible to execute the prepared HTTP PUT request.
	 * 
	 * @return <code>true</code> if the call has been successful,
	 *         <code>false</code> otherwise.
	 * @throws IOException
	 *             Exception occurred.
	 */
	public boolean execAdd() throws IOException {
		mOutput.close();
		int code = mConn.getResponseCode();
		if (code == 201)
			while (mConn.getInputStream().read() != -1)
				;
		else {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					mConn.getErrorStream()));
			String l;
			while ((l = br.readLine()) != null) {
				System.out.println(l);
			}
		}
		mConn.disconnect();
		return code == 201;
	}

	/**
	 * {@link OutputStream} from REST request.
	 * 
	 * @return {@link OutputStream} from REST request.
	 */
	public OutputStream getOutputStream() {
		return mOutput;
	}

	/**
	 * Creates an empty collection for refactoring of subcollection workaround.
	 * 
	 * @param name
	 *            Name of the collection.
	 * @return <code>true</code> if the creation was successful,
	 *         <code>false</code> otherwise.
	 * @throws IOException
	 *             Exception occurred.
	 */
	public boolean createEmptyCollection(final String name) throws IOException {
		URL url = new URL(mServers[0] + "/" + name);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(PUT);
		int code = conn.getResponseCode();
		if (code == 201) {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String s;
			while ((s = r.readLine()) != null) {
				System.out.println(s);
			}
			r.close();
		} else {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					conn.getErrorStream()));
			String s;
			while ((s = r.readLine()) != null) {
				System.out.println(s);
			}
			r.close();
		}
		conn.disconnect();
		return code == 201;
	}

	/**
	 * Executes refactoring of subcollection workaround.
	 * 
	 * @param collection
	 *            Name of the collection.
	 * @param sub
	 *            Name of subcollection.
	 * @return <code>true</code> if the creation was successful,
	 *         <code>false</code> otherwise.
	 * @throws IOException
	 *             Exception occurred.
	 */
	public boolean runRefactoring(final String sub, final String collection)
			throws IOException {
		URL url = new URL(mServers[0] + "?run=" + RQ + "&subcollection=" + sub
				+ "&collectionname=" + collection);
		System.out.println(url.toString());
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		int code = conn.getResponseCode();
		if (code == 200) {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String s;
			while ((s = r.readLine()) != null) {
				System.out.println(s);
			}
			r.close();
		} else {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					conn.getErrorStream()));
			String s;
			while ((s = r.readLine()) != null) {
				System.out.println(s);
			}
			r.close();
		}

		conn.disconnect();
		return code == 200;
	}

	/**
	 * Deletes the temporary document of subcollection workaround.
	 * 
	 * @param name
	 *            Name of the collection.
	 * @return <code>true</code> if the deletion was successful,
	 *         <code>false</code> otherwise.
	 * @throws IOException
	 *             Exception occurred.
	 */
	public boolean deleteTemporaryCollection(final String name)
			throws IOException {
		URL url = new URL(mServers[0] + "/" + name);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(DELETE);
		int code = conn.getResponseCode();
		if (code == 200) {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String s;
			while ((s = r.readLine()) != null) {
				System.out.println(s);
			}
			r.close();
		} else {
			BufferedReader r = new BufferedReader(new InputStreamReader(
					conn.getErrorStream()));
			String s;
			while ((s = r.readLine()) != null) {
				System.out.println(s);
			}
			r.close();
		}
		conn.disconnect();
		return code == 200;
	}

}

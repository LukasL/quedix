package org.unikn.quedix;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.unikn.quedix.query.BaseXClient;
import org.unikn.quedix.query.BaseXClient.Query;

/**
 * This client class connects clients to BaseX server to simulate a distributed environment.
 * 
 * @author Lukas Lewandowski, University of Konstanz, Germany.
 */
public class Client {

    /** Host name. */
    private static final String HOST = "aalto.disy.inf.uni-konstanz.de";
    /** User name. */
    private static final String USER = "admin";
    /** Password. */
    private static final String PW = "admin";
    /** Example query. */
    private static final String EQ = "doc('MyL')//user";

    /** client instances. */
    private Map<String, BaseXClient> mClients;

    /**
     * Starts the client application.
     * 
     * @param args
     *            arguments.
     */
    public static void main(final String[] args) {
        try {
            Client c = new Client();
            // store an XML file with name MyL
            c.exampleImporter(c.getClients());
            // query MyL
            System.out.println("QUERY RESULT: ***");
            c.query(c.getClients(), EQ);
            c.shutdownClients();
        } catch (final IOException exce) {
            exce.printStackTrace();
        }

    }

    /**
     * Constructor connects clients to BaseX server.
     * 
     * @throws IOException
     */
    public Client() throws IOException {
        this.mClients = connectClients();
    }

    /**
     * Querying of all {@link BaseXClient} instances.
     * 
     * @param clients
     *            The clients.
     * @param query
     *            The query.
     * @throws IOException
     *             Exception occurred.
     */
    public void query(final Map<String, BaseXClient> clients, final String query) throws IOException {
        for (Map.Entry<String, BaseXClient> cls : clients.entrySet()) {
            System.out.println("Result from: " + cls.getKey());
            BaseXClient bx = cls.getValue();
            Query q = bx.query(query);
            while(q.more()) {
                System.out.println(q.next());
            }
        }
    }

    /**
     * Example importer, creates a new database with name "MyL"
     * 
     * @param cls
     * @throws IOException
     */
    public void exampleImporter(final Map<String, BaseXClient> cls) throws IOException {
        for (Map.Entry<String, BaseXClient> c : cls.entrySet()) {
            InputStream is = Client.class.getResourceAsStream("/lexus.xml");
            create(c.getValue(), "MyL", is);
            System.out.println("MyL stored on: " + c.getKey());
            is.close();
        }
    }

    /**
     * Creates a new document.
     * 
     * @param c
     *            Client instance.
     * @param dbName
     *            Database name.
     * @param xml
     *            Input.
     * @throws IOException
     *             Exception occurred.
     */
    public void create(final BaseXClient c, final String dbName, final InputStream xml) throws IOException {
        BaseXClient bx = c;
        bx.create(dbName, xml);
    }

    /**
     * Initialization of BaseX clients.
     * 
     * @return {@link Map} of connected BaseX clients.
     * @throws IOException
     *             Exception occurred, e.g. server are not running.
     */
    public Map<String, BaseXClient> connectClients() throws IOException {
        mClients = new HashMap<String, BaseXClient>();
        mClients.put("site1", new BaseXClient(HOST, 1980, USER, PW));
        mClients.put("site2", new BaseXClient(HOST, 1981, USER, PW));
        mClients.put("site3", new BaseXClient(HOST, 1982, USER, PW));
        return mClients;
    }

    /**
     * Simple getter.
     * 
     * @return mClients.
     */
    public Map<String, BaseXClient> getClients() {
        return mClients;
    }

    /**
     * Shutdowns connected clients.
     * 
     * @throws IOException
     *             Exception occurred.
     */
    public void shutdownClients() throws IOException {
        if (mClients != null) {
            for (Map.Entry<String, BaseXClient> cl : mClients.entrySet()) {
                BaseXClient c = cl.getValue();
                c.close();
            }
        }
    }
}

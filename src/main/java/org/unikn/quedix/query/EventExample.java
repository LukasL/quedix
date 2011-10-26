package org.unikn.quedix.query;

import java.io.IOException;

/**
 * This example shows how to use the event feature.
 * 
 * Documentation: http://docs.basex.org/wiki/Clients
 * 
 * @author BaseX Team 2005-11, BSD License
 */
public final class EventExample {
    /** Hidden default constructor. */
    private EventExample() {
    }

    /**
     * Main method.
     * 
     * @param args
     *            command-line arguments
     */
    public static void main(final String[] args) {
        try {
            final BaseXClient session1 = new BaseXClient("localhost", 1984, "admin", "admin");
            final BaseXClient session2 = new BaseXClient("localhost", 1984, "admin", "admin");

            session1.execute("create event messenger");
            session2.watch("messenger", new Notifier());
            session2.query("for $i in 1 to 1000000 where $i = 0 return $i").execute();
            session1.query("db:event('messenger', 'Hello World!')").execute();
            session2.unwatch("messenger");
            session1.execute("drop event messenger");
            session1.close();
            session2.close();

        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implementation of the event notifier interface.
     */
    private static class Notifier implements BaseXClient.EventNotifier {
        /** Constructor. */
        public Notifier() {
        }

        @Override
        public void notify(final String value) {
            System.out.println("Message received: " + value);
        }
    }
}

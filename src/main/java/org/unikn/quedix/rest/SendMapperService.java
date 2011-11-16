package org.unikn.quedix.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import static org.unikn.quedix.rest.Constants.CONTENT_TYPE_STRING;
import static org.unikn.quedix.rest.Constants.PUT;
import static org.unikn.quedix.rest.Constants.RAW;

/**
 * This class sends mapper XQuery files to the MapperDb.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class SendMapperService {

    /** Connection reference. */
    private HttpURLConnection mConnection;
    /** Resource location. */
    private String mLocation;

    /**
     * Constructor sets resource target.
     * 
     * @param resourceTarget
     *            Location where the mapper file has to be stored
     */
    public SendMapperService(final String resourceTarget) {
        mLocation = resourceTarget;
    }

    /**
     * This method prepares the output for the sending the XQ file to the
     * MapperDb.
     * 
     * @return The {@link OutputStream} for sending data.
     */
    public OutputStream prepareOutput() {
        URL url;
        try {
            url = new URL(mLocation);
            mConnection = (HttpURLConnection)url.openConnection();
            mConnection.setDoOutput(true);
            mConnection.setRequestMethod(PUT);
            mConnection.setRequestProperty(CONTENT_TYPE_STRING, RAW);
            return mConnection.getOutputStream();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This method executes the service and listens for the response code.
     */
    public void executeService() {
        try {
            mConnection.connect();
            if (mConnection.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                // TODO error
            }
            mConnection.disconnect();
        } catch (final IOException exc) {
            exc.printStackTrace();
        }
    }

}

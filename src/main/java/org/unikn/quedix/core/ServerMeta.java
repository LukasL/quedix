package org.unikn.quedix.core;

/**
 * This class contains information about data server meta information.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class ServerMeta {

    /** Main memory of a data server. */
    private long mRam;

    /**
     * Setter.
     * 
     * @param ram
     *            The ram to set.
     */
    public void setRam(final long ram) {
        this.mRam = ram;
    }

    /**
     * Getter.
     * 
     * @return Returns the ram.
     */
    public long getRam() {
        return mRam;
    }

}

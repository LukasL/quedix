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

package org.unikn.quedix.core;

import java.util.Map;

/**
 * Arguments with type.
 */
public class Arg {

    /** Parameter type. */
    public enum Paramter {
        NAME, MAP, REDUCE, INPUT, TYPE
    };

    /** Map of parameters. */
    private Map<Paramter, String> mPar;
    /** Start type. */
    private StartType mType;

    /**
     * Constructor sets necessary fields.
     * 
     * @param type
     *            {@link StartType} value.
     * @param par
     *            Additional parameters.
     */
    public Arg(final StartType type, final Map<Paramter, String> par) {
        mPar = par;
        mType = type;
    }

    /**
     * Getter.
     * 
     * @return par.
     */
    public Map<Paramter, String> getPar() {
        return mPar;
    }

    /**
     * Getter.
     * 
     * @return starttype.
     */
    public StartType getType() {
        return mType;
    }

}

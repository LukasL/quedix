package org.unikn.quedix.core;

import java.util.Map;

/**
 * Arguments with type.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 * 
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

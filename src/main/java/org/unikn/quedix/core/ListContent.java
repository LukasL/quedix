package org.unikn.quedix.core;

import java.util.ArrayList;
import java.util.List;

/**
 * This class holds information from a BaseX list command.
 * 
 * @author Lukas Lewandowski, University of Konstanz.
 */
public class ListContent {

	/** Occupied size at server. */
	private long mSize;
	/** Databases on server. */
	private List<String> mDbs;

	/**
	 * Parses result of BaseX' List command and builds size and database
	 * mapping.
	 * 
	 * @param listResult
	 *            Result of List command.
	 */
	public ListContent(final String listResult) {
		mSize = -1;
		mDbs = new ArrayList<String>();
		parse(listResult);
	}

	/**
	 * Getter.
	 * 
	 * @return size.
	 */
	public long getSize() {
		return mSize;
	}

	/**
	 * Getter.
	 * 
	 * @return databases.
	 */
	public List<String> getDbs() {
		return mDbs;
	}

	/**
	 * Parses result of BaseX' List command and builds the interesting
	 * properties.
	 * 
	 * @param listResult
	 *            Result of List command.
	 */
	private void parse(final String listResult) {
		System.out.println(listResult);
		long sum = 0;
		String[] lines = listResult.split("/n");
		for (int i = 2; i < lines.length; i++) {
			String[] columns = lines[i].split("/t");
			mDbs.add(columns[0]);
			System.out.println("size: " + columns[2]);
			sum += Long.parseLong(columns[2]);
		}
		for (String dbs : mDbs)
			System.out.println("Db: " + dbs);
		System.out.println("complete result: " + sum);
	}
}

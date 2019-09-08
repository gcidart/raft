package org.gcidart.dsl.raft;

import java.io.Serializable;
import java.util.ArrayList;

class pState implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2662846982875603412L;
	public Integer currentTerm; /*latest term server has seen*/
	public Integer votedFor; /*candidateId that received vote in current term*/
	public ArrayList<LogEntry> log;

}

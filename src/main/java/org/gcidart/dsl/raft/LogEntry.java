package org.gcidart.dsl.raft;

import java.io.Serializable;

public class LogEntry implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3427702094641763224L;
	int command;
	int arg0;
	int arg1;
	int rcvTerm;
	LogEntry(int command, int arg0, int arg1, int rcvTerm)
	{
		this.command = command;
		this.arg0 = arg0;
		this.arg1 = arg1;
		this.rcvTerm = rcvTerm;
	}
}

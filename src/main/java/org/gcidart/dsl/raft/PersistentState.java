package org.gcidart.dsl.raft;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class PersistentState {
	byte[] raftState;
	//Byte[] snapshot;
	String fileName;
	
	
	PersistentState(int id)
	{
		this.fileName = String.format("%s//rs-%d.per", System.getProperty("java.io.tmpdir"), id);
		this.raftState = null;
	}
	
	void saveRaftState(byte[] state) 
	{
		try {
			this.raftState = state;
			FileOutputStream fos = new FileOutputStream(this.fileName);
			fos.write(this.raftState);
			fos.flush();
			fos.close();
		
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
	}
	
	byte[] readRaftState() 
	{
		try {
			FileInputStream in = new FileInputStream(this.fileName);
			this.raftState = in.readAllBytes();
			in.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return raftState;
		
		
	}
	
	
	

}

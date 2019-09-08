package org.gcidart.dsl.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.gcidart.dsl.raft.LogEntry;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.WebServer;

public class RaftServer {
	
	private enum ServerState {FOLLOWER, CANDIDATE, LEADER};
	ServerState selfState;	
	
	/*Persistent state on all servers*/
	pState ps;
	
	
	/*Volatile state on all servers*/
	int commitIndex;	/*index of highest log entry known to be committed */
	int lastApplied;	/*index of highest log entry applied to state machine*/
	
	/*Volatile state on leader*/
	ArrayList<Integer> nextIndex; /*index of the next log entry to send to that server*/
	ArrayList<Integer> matchIndex; /*index of highest log entry known to be replicated on server*/
	
	private ArrayList<XmlRpcClient> clients;
	private PersistentState persister;
	private int id;
	
	private class SharedInteger
	{
		Integer numPosResp;
		SharedInteger(int init)
		{
			this.numPosResp = init;
		}
	}
	private WebServer server; /*RPC Server*/
	private SharedInteger votes; /*Used to count votes from peers during RequestVote */
	private long electionTimerStart; /*ElectionTimer start time*/
	private long heartbeatTimerStart; /*HeartbeatTimer start time*/	
	private long electionTimeout ;
	private long heartbeatTimeout = 500;
	private int rpcServerOn; /*For Testing only: when set to 0, RPC Server won't respond to RequestVote or AppendEntries */
	
	RaftServer(int id, int restore, ArrayList<String> ports) throws ClassNotFoundException, IOException
	{
		this.id = id;
		this.ps = new pState();
		this.ps.log = new ArrayList<LogEntry>();

		this.persister = new PersistentState(id);
		this.clients = new ArrayList<XmlRpcClient>();
		this.votes = new SharedInteger(0);
		
		this.ps.currentTerm = -1;
		this.ps.votedFor = -1;
		
		this.commitIndex = -1;
		this.lastApplied = -1;
		
		
		
		this.selfState = ServerState.FOLLOWER; /*Start as Follower */
		
		for(String port:ports)
		{
			XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
			String peerName = String.format("http://127.0.0.1:%s", port);
			config.setServerURL( new URL( peerName ) );
			XmlRpcClient xrc = new XmlRpcClient();
			xrc.setConfig( config );
			if(xrc!=null)
			{
				this.clients.add(xrc);
				XmlRpcClientConfigImpl c=(XmlRpcClientConfigImpl) xrc.getClientConfig();
				System.out.println("Peer  "+port+": "+c.getServerURL().toString());
			}
			
		}
		
		this.nextIndex = new ArrayList<Integer>(this.clients.size());
		this.matchIndex = new ArrayList<Integer>(this.clients.size());
		
		this.rpcServerOn = 1;
		if(restore==1)
		{
			readPersist();
			String persistDebug = String.format("After restoring state: Current Term: %d, Log Size: %d", this.ps.currentTerm, this.ps.log.size());
			System.out.println(persistDebug);
		}
		else
		{
			persist();
		}
		
	}
	
	/*
	 * RPC to be used by Tester
	 * Returns : [currentTerm, isLeader]	
	 */
	public Object[] getState()
	{
		Object[] st = new Object[2];
		st[0] = (this.ps.currentTerm);
		int isLeader = (this.selfState ==  ServerState.LEADER) ? 1:0;
		st[1] = (isLeader);
		return st;
	}
	
	/*RPC for testing only */
	public int connect()
	{
		this.rpcServerOn = 1;
		return 1;
	}
	
	/*RPC for testing only */
	public int disconnect()
	{
		this.rpcServerOn = 0;
		return 1;
	}
	
	/*RPC for testing only */
	public int stopServer() throws InterruptedException
	{
		System.out.println("stopServer RPC received");		
		this.server.shutdown();
		Thread.sleep(1000);
		System.exit(0);
		return 1;
	}
	
	/*
	 * RPC for testing only
	 * Returns elements of LogEntry at Index idx as an array
	 * If idx is not present LogEntry.cmd is returned as -1000
	 */
	public Object[] getLogEntry(int idx)
	{
		Object[] entry = new Object[4];
		entry[0]=-1000;
		entry[1]=-1000;
		entry[2]=-1000;
		entry[3]=-1000;
		if((idx>=0)&&(this.ps.log.size()>idx) && (idx <= this.commitIndex))
		{
			LogEntry le = this.ps.log.get(idx);
			entry[0] = le.command;
			entry[1] = le.arg0;
			entry[2] = le.arg1;
			entry[3] = le.rcvTerm;
			
		}
		String debug = String.format("idx %d commitIndex %d arg0 %d rcvTerm %d", idx, this.commitIndex, entry[1], entry[2]);
		System.out.println(debug);
		return entry;
	}
	
	void persist() 
	{
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(this.ps);
		    oos.flush();
		    byte [] data = bos.toByteArray();
			persister.saveRaftState(data);		
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	void readPersist() throws IOException, ClassNotFoundException 
	{
		byte[] data = persister.readRaftState();
		if(data!=null)
		{
			ByteArrayInputStream in = new ByteArrayInputStream(data);
		    ObjectInputStream is = new ObjectInputStream(in);
		    ps = (pState) is.readObject();
		}
	}
	
	void initNextIndexPostElection()
	{
		synchronized(this.nextIndex)
		{
			this.nextIndex.clear();
			for(int i=0 ; i < this.clients.size(); i++)
			{
				this.nextIndex.add(this.ps.log.size()) ;
			}
		}
		synchronized(this.matchIndex)
		{
			this.matchIndex.clear();
			for(int i=0 ; i < this.clients.size(); i++)
			{
				this.matchIndex.add(-1) ;
			}
		}
	}
	
	/*
	 * Update commitIndex
	 * Called only when server is the leader
	 */
private void updateCommitIndex()
	{
		int newCommitIndex = this.commitIndex;
		for(int logIndex = Integer.max(0,this.ps.log.size()-1); logIndex > this.commitIndex; logIndex--)
		{
			if(this.ps.log.size()==0)
				break;
			if(this.ps.log.get(logIndex).rcvTerm!=this.ps.currentTerm)
				break;
			int cnt = 0;
			for(int clientId = 0; clientId < this.clients.size();clientId++)
			{
				if(clientId==this.id)
					cnt++;
				else if(this.matchIndex.get(clientId)==logIndex)
					cnt++;
			}
			if(2*cnt>=this.clients.size())
			{
				newCommitIndex = logIndex;
				break;
			}
			
		}
		if(newCommitIndex > this.commitIndex)
		{
			System.out.println("newCommitIndex: "+ Integer.toString(newCommitIndex) +" oldCommitIndex: "+ Integer.toString(this.commitIndex));
			this.commitIndex = newCommitIndex;
		}
		return ;
	}
	
	private boolean isMoreUpdated(int peerLastIndex, int peerLastTerm)
	{
		if(this.ps.log.size()==0)
			return false;
		int myLastTerm = this.ps.log.get(this.ps.log.size()-1).rcvTerm;
		if(myLastTerm > peerLastTerm)
			return true;
		else if ((myLastTerm==peerLastTerm) && (this.ps.log.size()-1 > peerLastIndex))
		{
			return true;
		}
		else 
			return false;
	}
	
	/* Request Vote RPC: Invoked by candidates to gather votes*/
	public Integer[] requestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm ) throws IOException
	{
		
		Integer[] res = null;
		/*If Tester has disabled RPC on this server, then return null*/
		if(this.rpcServerOn==0)
			return res;
		res = new Integer[2];
		int voteGranted = 0;

		synchronized(this.ps) {
			if(term == this.ps.currentTerm)
			{
				if(((this.ps.votedFor==-1) || (this.ps.votedFor==candidateId))
						&& (!isMoreUpdated(lastLogIndex, lastLogTerm)))
				{
					this.ps.votedFor = candidateId;
					voteGranted = 1;
					this.ps.currentTerm = term; 
					this.selfState = ServerState.FOLLOWER;
					System.out.println("1: Became Follower here");
					this.electionTimerStart = System.currentTimeMillis();
					 
					persist();
				}
			}
			else if(term > this.ps.currentTerm)
			{
				if(!isMoreUpdated(lastLogIndex, lastLogTerm))
				{
					this.ps.votedFor = candidateId;
					voteGranted = 1;
				}
				this.ps.currentTerm = term; 
				this.selfState = ServerState.FOLLOWER;
				System.out.println("2: Became Follower here");
				this.electionTimerStart = System.currentTimeMillis();
				persist();
			}
			
		}
			
		res[0] = this.ps.currentTerm;
		res[1] = voteGranted;
		String requestVoteDebug = String.format("MyID: %d MyTerm: %d requestVote called by Raft Server:%d,%d  %d %d %d", 
												this.id, this.ps.currentTerm, candidateId, term, lastLogIndex, this.commitIndex, voteGranted);
		System.out.println(requestVoteDebug);
		return res;
		
	}
	
	
	boolean sendRequestVote(int index ) throws XmlRpcException
	{
		int lastLogIndex = -1;
		int lastLogTerm = -1;
		int logSize = this.ps.log.size();
		if(logSize!=0)
		{
			lastLogTerm = this.ps.log.get(logSize-1).rcvTerm;
			lastLogIndex = logSize;
		}
		Object[] params = { this.ps.currentTerm, this.id, lastLogIndex, lastLogTerm};        
        Object[] result = ( Object[] ) clients.get(index).execute( "RaftServer.requestVote", params );
        int srvTerm = (Integer) result[0];
        int voteRcvd = (Integer) result[1];
        if(srvTerm>this.ps.currentTerm)
        {
        	synchronized(this.ps)
			{
				this.ps.currentTerm = srvTerm;
				this.selfState = ServerState.FOLLOWER;
				System.out.println("3: Became Follower here");
			}							
			this.electionTimerStart = System.currentTimeMillis();
        }
        if(voteRcvd==1)
        	return true;
        else
        	return false;
	}
	
	private int tryGetLeadership() throws InterruptedException
	{
		
		System.out.println("Inside tryGetLeadership");
		this.ps.currentTerm++;
		synchronized (this.ps) {
			if(this.selfState == ServerState.CANDIDATE)
			{
				this.ps.votedFor = id; /*Vote for Self*/
				this.votes.numPosResp = 1; /*Account for own vote*/
			}
			else
				this.votes.numPosResp = 0;
		}	
		
		/*If Tester has disabled RPC from this server, then return 0*/
		if(this.rpcServerOn==0)
		{
			System.out.println("Disconnected from network by Tester");
			return 0;
		}
			
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		for(int i =0; i < this.clients.size(); i++)
		{
			final int clientIndex = i;
			if(i==this.id)
			{
				continue;
			}
			else
			{
				executor.submit(() -> {
					
					
					try
					{
						boolean result = sendRequestVote(clientIndex);
						System.out.println("Vote from " + Integer.toString(clientIndex)+ ": " + result);
						if(result)
						{
							synchronized (this.votes) {
								this.votes.numPosResp++;
								System.out.println("Votes received" + Integer.toString(this.votes.numPosResp));
								this.votes.notifyAll();
							}
						}
					} catch (XmlRpcException e)
					{
						System.out.println("Error calling requestVote RPC on ServerID: "+Integer.toString(clientIndex)+" "+e);
						//e.printStackTrace();
					}
				
					
				});
			}
		}
		synchronized (this.votes) {
			while((2*this.votes.numPosResp)<clients.size())
			{
				this.votes.wait(this.electionTimeout);
				long now = System.currentTimeMillis();
				long timeElapsed = now-this.electionTimerStart;
				if(timeElapsed > this.electionTimeout) /*Majority votes should come within electionTimeout*/
					break;
				
			}
		}
		if((2*this.votes.numPosResp)<clients.size())
		{
			return 0;
		}
		else 
		{
			this.selfState = ServerState.LEADER;
			/*Initialize nextIndex[] and matchIndex[] for peer servers*/
			initNextIndexPostElection();
			String leaderMsg = String.format("Raft Peer with id %d: becomes leader", this.id);
			System.out.println(leaderMsg);
			return 1;
		}
	}
	
	/*Append Entries RPC: Invoked by leader to replicate log entries; also used as heartbeat */
	public Integer[] appendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm, 
			int cmd, int arg0, int arg1, int eterm, int leaderCommit ) throws IOException
	{
		Integer[] res = null;
		/*If Tester has disabled RPC on this server, then return null*/
		if(this.rpcServerOn==0)
			return res;
		res = new Integer[2];
		int entryFound = 0;
		LogEntry entry = new LogEntry(cmd, arg0, arg1, eterm); /*This should be with eterm and not term*/
		String appendEntriesDebug = String.format("appendEntries rcvd from Leader Id %d(with term %d) - arg0=%d with prevLog=(%d,%d); myLogSize=%d myTerm=%d",
				leaderId, term, arg0, prevLogIndex, prevLogTerm, this.ps.log.size(), this.ps.currentTerm);
		System.out.println(appendEntriesDebug);
		
		if(term >= this.ps.currentTerm)
		{
			this.ps.currentTerm = term;
			this.selfState = ServerState.FOLLOWER;
			System.out.println("4: Became Follower here");
			this.electionTimerStart = System.currentTimeMillis();
			int myLogTerm = -1;
			System.out.println("test1");
			if((prevLogIndex >=0) && (this.ps.log.size()>prevLogIndex))
				myLogTerm = this.ps.log.get(prevLogIndex).rcvTerm;
			System.out.println("test2 "+Integer.toString(myLogTerm));
			if(((prevLogIndex==-1) && (prevLogTerm==-1)) ||( myLogTerm== prevLogTerm))
			{
				if(cmd!=0) /* "cmd = 0" denotes heartbeat RPC*/
				{
					if(prevLogIndex+1 < this.ps.log.size())
					{
						this.ps.log.set(prevLogIndex+1, entry);
						System.out.println("Changed entry at index: " + (prevLogIndex+1));
					}
					else
					{
						this.ps.log.add(entry);
					}
				}
				entryFound = 1;				
			}
			else
			{
				if((prevLogIndex >=0) && (this.ps.log.size()>prevLogIndex))
				{
					int logSize = this.ps.log.size();
					for(int index = logSize-1; index >= prevLogIndex ; index--)
						this.ps.log.remove(index);
				}
					
			}
			System.out.println("test3");	
			if(leaderCommit > this.commitIndex)
			{
				this.commitIndex = Integer.min(leaderCommit, this.ps.log.size()-1);
			}
			persist();					
		}
		System.out.println("Received appendEntries RPC from LeaderID:" + Integer.toString(leaderId));
		res[0] = this.ps.currentTerm;
		res[1] = entryFound;
		
		return res;
	}
	
	private int appendEntriesPeriodic() throws InterruptedException
	{
		this.heartbeatTimerStart = System.currentTimeMillis();
		/*If Tester has disabled RPC from this server, then return 0*/
		if(this.rpcServerOn==0)
			return 0;
		System.out.println("Inside appendEntriesPeriodic");
		int myTerm;
		ServerState myState;
		synchronized(this.ps)
		{
			myTerm = this.ps.currentTerm;
			myState = this.selfState;
		}
		final int myfTerm = myTerm;
		if(myState!=ServerState.LEADER)
		{
			System.out.println("Exiting from appendEntriesPeriodic after losing Leadership");
			return 0;
		}
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		for(int i =0; i < this.clients.size(); i++)
		{
			final int clientIndex = i;
			if(clientIndex==this.id)
			{
				continue;
			}
			else
			{
				executor.submit(() -> {
					//System.out.println("Inside appendEntriesPeriodic1");
					int prevLogIndex = this.nextIndex.get(clientIndex) - 1;
					int prevLogTerm = (prevLogIndex >=0)?this.ps.log.get(prevLogIndex).rcvTerm:-1;
					int cmd = 0; /*Default Heartbeat*/
					int arg0 = 0;
					int arg1 = 0;
					int eterm = 0;
					String debug = String.format("Before prevLogIndex %d log size %d ; Client %d:nextIndex%d matchIndex%d", 
							prevLogIndex, this.ps.log.size(), clientIndex, nextIndex.get(clientIndex), matchIndex.get(clientIndex));
					System.out.println(debug);
					if( (prevLogIndex==this.matchIndex.get(clientIndex)) && (prevLogIndex+1 < this.ps.log.size()) && (prevLogIndex+1>= 0) )
					{
						LogEntry entry = this.ps.log.get(prevLogIndex+1);
						cmd = entry.command;
						arg0 = entry.arg0;
						arg1 = entry.arg1;
						eterm = entry.rcvTerm;
					}
					//System.out.println("Inside appendEntriesPeriodic2");
					XmlRpcClient xrc = this.clients.get(clientIndex);
					try
					{
						System.out.println("Calling appendEntries on Server: "+Integer.toString(clientIndex)+" with cmd "+ Integer.toString(cmd));
						Object[] param = { myfTerm, this.id, prevLogIndex, prevLogTerm, cmd, arg0, arg1, eterm, this.commitIndex};
						Object[] result = ( Object[] ) xrc.execute(xrc.getClientConfig(),"RaftServer.appendEntries", param);
						int peerTerm = (Integer) result[0];
						if(peerTerm > myfTerm) /*TODO:Check this*/
						{
							synchronized(this.ps)
							{
								this.ps.currentTerm = Integer.max(this.ps.currentTerm, peerTerm);
								this.selfState = ServerState.FOLLOWER;
								System.out.println("5: Became Follower here");
							}							
							this.electionTimerStart = System.currentTimeMillis();
						}
						else
						{
							int lastEntryFound = (Integer) result[1];
							if(lastEntryFound==0)
							{
								this.nextIndex.set(clientIndex, prevLogIndex);
								int oldMatchIndex = this.matchIndex.get(clientIndex);
								oldMatchIndex = Integer.max(-1, oldMatchIndex-1);
								this.matchIndex.set(clientIndex, oldMatchIndex);
							}
							else
							{
								if(cmd==0)
								{
									this.matchIndex.set(clientIndex, prevLogIndex);
								}
								else
								{
									this.nextIndex.set(clientIndex, prevLogIndex+2);
									this.matchIndex.set(clientIndex, prevLogIndex+1);							
								}
							}
						}
						
					} catch (XmlRpcException e)
					{
						System.out.println("Error calling appendEntries RPC on ServerID: "+Integer.toString(clientIndex)+" "+e);
						//e.printStackTrace();
					}
					debug = String.format("after prevLogIndex %d log size %d ; Client %d:nextIndex%d matchIndex%d", 
							prevLogIndex, this.ps.log.size(), clientIndex, nextIndex.get(clientIndex), matchIndex.get(clientIndex));
					System.out.println(debug);
				
					
				});
			}
		}
		//Thread.sleep(this.heartbeatTimeout); testBasicAgree() was failing with this for Index 2
		return 1;
	}
	

	/*
	 * RPC from client to execute a command
	 */
	public ArrayList<Integer> start(int cmd, int arg0, int arg1) throws InterruptedException, XmlRpcException
	{
		String debug = String.format("Received  Command: %d  Argument0: %d  Argument1: %d", cmd, arg0, arg1);
		System.out.println(debug);
		ArrayList<Integer> res = new ArrayList<Integer>();
		int isLeader = (this.selfState ==  ServerState.LEADER) ? 1:0;
		if(isLeader==1)
		{
			LogEntry entry = new LogEntry(cmd, arg0, arg1, this.ps.currentTerm);
			this.ps.log.add(entry);
			//tryCommit(cmd,arg0,arg1);
		}
		res.add(commitIndex+1); /*index that the command will appear at if it's ever committed*/
		res.add(this.ps.currentTerm);
		res.add(isLeader);
		return res;
	}
	
	
	public void startStateMachine() throws InterruptedException, XmlRpcException, MalformedURLException
	{
		this.electionTimerStart = System.currentTimeMillis();
		Random random = new Random();
		random.setSeed(this.electionTimerStart);
		
		this.electionTimeout = (long) (1000+ random.nextInt(1000));
		
		while(true)
		{
			if(commitIndex > lastApplied)
			{
				lastApplied++;
				//TODO: Execute command
			}
			if(this.selfState == ServerState.FOLLOWER)
			{
				long now = System.currentTimeMillis();
				long timeElapsed = now-this.electionTimerStart;
				if(timeElapsed > this.electionTimeout)
				{
					System.out.println("Follower->Candidate transition");
					this.selfState = ServerState.CANDIDATE;
				}
			}
			else if(this.selfState == ServerState.CANDIDATE)
			{
				long now = System.currentTimeMillis();
				long timeElapsed = now-this.electionTimerStart;
				if(timeElapsed > this.electionTimeout)
				{
					this.heartbeatTimerStart = System.currentTimeMillis();	
					System.out.println("I am a Candidate");
					this.electionTimeout = (long) (1000+ random.nextInt(1000));
					this.electionTimerStart = System.currentTimeMillis();
					System.out.println("My term before trying to be a Leader   " + Integer.toString(this.ps.currentTerm));
					tryGetLeadership();
				}
			}
			else if(this.selfState == ServerState.LEADER)
			{
				long now = System.currentTimeMillis();
				long timeElapsed = now-this.heartbeatTimerStart;
				updateCommitIndex();
				if(timeElapsed > this.heartbeatTimeout)
				{
					System.out.println("I am the LEADER");
					appendEntriesPeriodic();
					//tryCommit(0,-1,-1);
				}
			} 
		}
	}
	
	public static void main( String[] args ) throws XmlRpcException, IOException, InterruptedException, ClassNotFoundException {
		/* 
		 * args[0] = index
		 * args[1] = restore the state from disk
		 * args[2...] = port numbers for all peers
		 */
		String outfile = String.format("%s//systemOut-%s.out", System.getProperty("java.io.tmpdir"), args[0]);
		System.out.println("Log file: " + outfile);
		MyPrintStream o = new MyPrintStream(new File(outfile));
		System.setOut(o); 
		int portNum = Integer.parseInt(args[0]);
		int restore = Integer.parseInt(args[1]);
		System.out.println("Raftserver ID "+ args[0]);
		ArrayList<String> ports = new ArrayList<String>();
		for(int i=2; i < args.length; i++)
        {
			ports.add(args[i]);
		}
        RaftServer raft = new RaftServer(portNum, restore, ports);
        PropertyHandlerMapping mapping = new PropertyHandlerMapping();
        mapping.setRequestProcessorFactoryFactory(new RaftServerFactoryFactory(raft));
        mapping.addHandler( "RaftServer", org.gcidart.dsl.raft.RaftServer.class );
        raft.server = new WebServer( Integer.parseInt(ports.get(portNum)) );
        raft.server.getXmlRpcServer().setHandlerMapping( mapping );
        XmlRpcServerConfigImpl serverConfig = (XmlRpcServerConfigImpl) raft.server.getXmlRpcServer().getConfig();
        serverConfig.setEnabledForExtensions(true);
        serverConfig.setContentLengthOptional(false);
        raft.server.start();
        Thread.sleep(2000);
        
		raft.startStateMachine();
	}
	

}

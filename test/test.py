import subprocess
import os
import re
import sys
import xmlrpc.client
import time
import random
from time import perf_counter



masterPortBase = 8090
peerCom = "start java -jar ../target/raftServer.jar-jar-with-dependencies.jar {} {} {}" 
addr = "http://127.0.0.1:{}"
disconnected = set()
proxies=list()
numPeers = 0
	
"""def checkOneLeader(proxies):
	leader = -1
	idx = 0
	for proxy in proxies:
		state = list(proxy.RaftServer.getState())
		#print(state)
		if int(state[1])==1:
			if leader==-1:
				leader = idx
			else:
				leader = -2
		idx = idx+1
	return leader
"""


def checkOneLeader(proxies):
	for iter in range(10):
		time.sleep((random.randint(450, 550))/1000)
		leaders = {}
		idx = 0
		for proxy in proxies:
			if idx not in disconnected:
				state = list(proxy.RaftServer.getState())
				#print("{} {}".format(idx, state))
				term = int(state[0])
				if int(state[1])==1:
					if term not in leaders:
						leaders[term] = []
					leaders[term].append(idx)
			idx = idx+1
		lastTerm = -10
		for term, leaderl in leaders.items():
			if (len(leaderl) > 1):
				print("FATAL:Term{} has {} leaders".format(term, len(leaderl)))
			if term > lastTerm:
				lastTerm = term

		if len(leaders)!=0:
			return leaders[lastTerm][0]
	print("No Leader")
	return -1



def checkTerm(proxies):
	term = -10
	idx = 0
	for proxy in proxies:
		if idx in disconnected:
			idx = idx+1
			continue
		state = list(proxy.RaftServer.getState())
		#print(state)
		if term==-10:
			term = int(state[0])
		elif term!=(int(state[0])):
			term = -100
		idx = idx+1
	return term

def disconnect(id):
	disconnected.add(id)
	#print(disconnected)
	proxies[id].RaftServer.disconnect()

def connect(id):
	disconnected.remove(id)
	proxies[id].RaftServer.connect()

def nCommitted(idx):
	#print("Log index {}".format(idx))
	count = 0
	cmd = -1
	arg0 = -1
	arg1 = -1
	res = list()
	pidx = 0
	for proxy in proxies:
		if pidx in disconnected:
			pidx = pidx+1
			continue
		logEntry = list(proxy.RaftServer.getLogEntry(idx))
		#print(logEntry)
		cmd1 = int(logEntry[0])
		arg01 = int(logEntry[1])
		arg11 = int(logEntry[2])
		if cmd1 != -1000:
			if count > 0 and ((cmd!=cmd1) or (arg0 !=arg01) or (arg1 != arg11)):
				print("FATAL: Expected({},{},{}) but got ({},{},{})".format(cmd, arg0, arg1, cmd1, arg01, arg11))
				return res
			count = count+1
			cmd = cmd1
			arg0 = arg01
			arg1 = arg11
		pidx = pidx+1
	res.append(count)
	res.append(cmd)
	res.append(arg0)
	res.append(arg1)
	#print("nCommitted result:{}".format(res))
	return res
				
def one(cmd, arg0, arg1, expectedServers, retry):
	print("Commit Cmd:{}, arg0:{}, arg1:{} on {} servers".format(cmd, arg0, arg1, expectedServers))
	t1_start = perf_counter()  
	idx = 0
	#Try for 10 seconds
	while (perf_counter() - t1_start) < 10:
		logIndex = -1
		#Check which server is the leader and ready to commit a new entry
		for i in range(len(proxies)):
			if idx not in disconnected:
				itl = list(proxies[i].RaftServer.start(cmd, arg0, arg1))
				#print("For ID{}, start result {}".format(idx, itl))
				if int(itl[2])==1:
					logIndex = int(itl[0])
					break
			idx = (idx+1)%numPeers
		if logIndex !=-1:
			print("Index:{}".format(logIndex))
			#Leader found and cmd is submitted
			t2_start = perf_counter()
			while (perf_counter() - t2_start) < 2:
				nc = nCommitted(logIndex)
				if nc[0] >= expectedServers:
					if nc[1]==cmd and nc[2]==arg0 and nc[3]==arg1:
						print("Cmd:{}, arg0:{}, arg1:{} at logIndex {}".format(cmd, arg0, arg1, logIndex))
						return logIndex
				time.sleep(20/1000)
			if retry==False:
				print("FATAL:({},{},{}) failed to reach agreement".format(cmd,arg0,arg1))
				return -1
		else:
			time.sleep(50/1000)
	print("FATAL:({},{},{}) failed to reach agreement".format(cmd,arg0,arg1))
	return -1
	







def testInitialElection():
	global numPeers
	numPeers= 3
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: Initial Election")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)

	print("Leader {}".format(checkOneLeader(proxies)))
	term1 = checkTerm(proxies) 
	print("Term {}".format(term1))

	print("Sleep for 5 seconds")
	time.sleep(5)
	print("Leader {}".format(checkOneLeader(proxies)))
	term2 = checkTerm(proxies) 
	print("Term {}".format(term2))
	if term1!=term2:
		print("WARN: Terms changed without any failure")

	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1

def testReElection():
	global numPeers
	numPeers= 3
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: Election after Network Failure")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)
	leader1 = checkOneLeader(proxies)
	print("Leader {}".format(leader1))
	
	print("Disconnecting Server{}".format(leader1))
	disconnect(leader1)
	print("Leader {}".format(checkOneLeader(proxies)))

	print("Connecting back Server{}".format(leader1))
	connect(leader1)

	leader2 = checkOneLeader(proxies)
	print("Leader {}".format(leader2))

	print("Disconnecting Server{}".format(leader2))
	disconnect(leader2)

	print("Disconnecting Server{}".format((leader2+1)%numPeers))
	disconnect((leader2+1)%numPeers)

	print("Leader {}".format(checkOneLeader(proxies)))

	print("Connecting back Server{}".format((leader2+1)%numPeers))
	connect((leader2+1)%numPeers)
	print("Leader {}".format(checkOneLeader(proxies)))

	print("Connecting back Server{}".format(leader2))
	connect(leader2)
	print("Leader {}".format(checkOneLeader(proxies)))

	

	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1

def testBasicAgree():
	global numPeers
	numPeers= 5
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: Basic Agreement")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)

	iterations = 3
	for index in range(iterations):
		nc = nCommitted(index)
		if nc[0] > 0:
			print("FATAL: Unxpected commands committed")
		cmd = 1
		arg0 = (index+1)*100
		arg1 = 0
		xindex = one(cmd, arg0, arg1, numPeers, False)
		if xindex!= index:
			print("FATAL: Expected Index {} but got index {}".format(index, xindex))


	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1

def testFailAgree():
	global numPeers
	numPeers= 3
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: Agreement despite Follower disconnection")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)

	cmd = 1
	arg0 = 101
	arg1 = 0
	one(cmd, arg0, arg1, numPeers, False)

	leader = checkOneLeader(proxies)
	print("Leader {}".format(leader))


	print("Disconnecting Server{}".format((leader+1)%numPeers))
	disconnect((leader+1)%numPeers)

	arg0=102
	one(cmd, arg0, arg1, numPeers-1, False)
	arg0=103
	one(cmd, arg0, arg1, numPeers-1, False)
	time.sleep(1) #Raft election timeout
	arg0=104
	one(cmd, arg0, arg1, numPeers-1, False)
	arg0=105
	one(cmd, arg0, arg1, numPeers-1, False)

	print("Connecting back Server{}".format((leader+1)%numPeers))
	connect((leader+1)%numPeers)

	arg0=106
	one(cmd, arg0, arg1, numPeers, True)
	arg0=107
	one(cmd, arg0, arg1, numPeers, True)

	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1

def testFailNoAgree():
	global numPeers
	numPeers= 5
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: No agreement if too many Followers disconnect")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)

	cmd = 1
	arg0 = 10
	arg1 = 0
	one(cmd, arg0, arg1, numPeers, False)

	leader = checkOneLeader(proxies)
	print("Leader {}".format(leader))

	print("Disconnecting Server{}".format((leader+1)%numPeers))
	disconnect((leader+1)%numPeers)
	print("Disconnecting Server{}".format((leader+2)%numPeers))
	disconnect((leader+2)%numPeers)
	print("Disconnecting Server{}".format((leader+3)%numPeers))
	disconnect((leader+3)%numPeers)

	arg0 = 20
	print("Sending Cmd:{}, arg0:{}, arg1:{} directly to Leader {}".format(cmd, arg0, arg1, leader))
	itl = list(proxies[leader].RaftServer.start(cmd, arg0, arg1))
	#print("Start result {}".format(itl))
	if int(itl[2])!=1:
		print("FATAL: Leader={} rejected Start()".format(leader))
	logIndex = int(itl[0])
	if logIndex!=1:
		print("FATAL: Expected Index 2 but got {}".format(logIndex))

	time.sleep(4) #Sleep for 2*RaftElectionTimeout

	nc = nCommitted(logIndex)
	if nc[0] > 0:
		print("FATAL: {} servers committed but no majority".format(nc))

	print("Connecting back Server{}".format((leader+1)%numPeers))
	connect((leader+1)%numPeers)
	print("Connecting back Server{}".format((leader+2)%numPeers))
	connect((leader+2)%numPeers)
	print("Connecting back Server{}".format((leader+3)%numPeers))
	connect((leader+3)%numPeers)

	leader2 = checkOneLeader(proxies)
	print("Leader {}".format(leader2))

	arg0 = 30
	print("Sending Cmd:{}, arg0:{}, arg1:{} directly to Leader {}".format(cmd, arg0, arg1, leader2))
	itl2 = list(proxies[leader2].RaftServer.start(cmd, arg0, arg1))
	#print("Start result {}".format(itl2))
	if int(itl2[2])!=1:
		print("FATAL: Leader={} rejected Start()".format(leader2))
	logIndex2 = int(itl2[0])
	if logIndex<1 or logIndex > 2:
		print("FATAL: Unexpected Index {}".format(logIndex2))

	arg0=1000
	one(cmd, arg0, arg1, numPeers, True)

	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1


def testRejoin():
	global numPeers
	numPeers= 3
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: Rejoining of Partitioned Leader")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)

	cmd = 1
	arg0 = 101
	arg1 = 0
	one(cmd, arg0, arg1, numPeers, True)

	leader1 = checkOneLeader(proxies)
	print("Leader {}".format(leader1))

	print("Disconnecting Server{}".format((leader1)))
	disconnect((leader1))


	arg0 = 102
	print("Sending Cmd:{}, arg0:{}, arg1:{} directly to Old Leader {}".format(cmd, arg0, arg1, leader1))
	itl = list(proxies[leader1].RaftServer.start(cmd, arg0, arg1))

	arg0 = 103
	print("Sending Cmd:{}, arg0:{}, arg1:{} directly to Old Leader {}".format(cmd, arg0, arg1, leader1))
	itl = list(proxies[leader1].RaftServer.start(cmd, arg0, arg1))

	arg0 = 104
	print("Sending Cmd:{}, arg0:{}, arg1:{} directly to Old Leader {}".format(cmd, arg0, arg1, leader1))
	itl = list(proxies[leader1].RaftServer.start(cmd, arg0, arg1))

	#New Leader should commit at Index1
	arg0=103
	one(cmd, arg0, arg1, numPeers-1, True)

	leader2 = checkOneLeader(proxies)
	print("Leader {}".format(leader2))

	print("Disconnecting Server{}".format((leader2)))
	disconnect((leader2))

	print("Connecting back Server{}".format((leader1)))
	connect((leader1))
	
	#Wait for Leadership to get resolved
	print("Leader {}".format(checkOneLeader(proxies)))

	arg0=104
	one(cmd, arg0, arg1, numPeers-1, True)

	print("Connecting back Server{}".format((leader2)))
	connect((leader2))

	#Wait for leader2 to understand that it is not the leader anymore
	print("Leader {}".format(checkOneLeader(proxies)))

	arg0=105
	one(cmd, arg0, arg1, numPeers, True)



	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1



def testBasicPersistence():
	global numPeers
	numPeers= 3
	arg=""
	proxies.clear()
	disconnected.clear()
	print("Test Case: Basic Persistence")
	for i in range(numPeers):
		masterPort = masterPortBase + i
		proxies.append(xmlrpc.client.ServerProxy(addr.format(masterPort)))
		arg = arg + " " + str(masterPort)


	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 0, arg), shell=True)

	print("Sleep for 5 seconds")
	time.sleep(5)

	cmd = 1
	arg0 = 11
	arg1 = 0
	one(cmd, arg0, arg1, numPeers, True)

	for i in range(numPeers):
		try:
			proxies[i].RaftServer.stopServer()
		except:
			print("Server {} closed".format(i))
	time.sleep(5)
	print("Restarting Servers")
	for i in range(numPeers):
		subprocess.call(peerCom.format(i, 1, arg), shell=True)

	time.sleep(5)
	arg0 = 12
	one(cmd, arg0, arg1, numPeers, True)
	#arg0=12 is getting stored at Index 2 instead of Index1
	#This is happening as commitIndex after restart is initialized to -1
	#On restart, the term increments and hence commitIndex is not updated to 0
	#And first Start call to RPC server gives the nextLog position as 0
	#Only on next Start call, the commitIndex gets updated


	leader1 = checkOneLeader(proxies)
	print("Leader {}".format(leader1))

	try:
		proxies[leader1].RaftServer.stopServer()
	except:
		print("Server {} closed".format(leader1))
	time.sleep(2)
	print("Restarting Server{}".format((leader1)))
	subprocess.call(peerCom.format(leader1, 1, arg), shell=True)
	time.sleep(2)
	arg0 = 13
	one(cmd, arg0, arg1, numPeers, True)



	leader2 = checkOneLeader(proxies)
	print("Leader {}".format(leader2))

	disconnect(leader2)
	try:
		proxies[leader2].RaftServer.stopServer()
	except:
		print("Server {} closed".format(leader2))

	arg0 = 14
	one(cmd, arg0, arg1, numPeers-1, True)

	print("Restarting Server{}".format((leader2)))
	subprocess.call(peerCom.format(leader2, 1, arg), shell=True)
	connect(leader2)

	time.sleep(2)

	leader3 = checkOneLeader(proxies)
	print("Leader {}".format(leader3))

	i3 = (leader3+1)%numPeers

	disconnect(i3)
	try:
		proxies[i3].RaftServer.stopServer()
	except:
		print("Server {} closed".format(i3))

	arg0 = 15
	one(cmd, arg0, arg1, numPeers-1, True)

	print("Restarting Server{}".format((i3)))
	subprocess.call(peerCom.format(i3, 1, arg), shell=True)
	connect(i3)

	arg0 = 16
	one(cmd, arg0, arg1, numPeers, True)
	#input("Enter any key ")
	idx =0 
	for proxy in proxies:
		try:
			proxy.RaftServer.stopServer()
		except:
			print("Server {} closed".format(idx))
		idx = idx+1



if __name__ == '__main__':
    globals()[sys.argv[1]]()



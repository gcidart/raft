# raft
Raft implementation in Java

Raft is a consensus algorithm for replicated state machine with result equivalent to Paxos.

This project is based on:
1) Lab 2 for the 6.824 Distributed Systems course: https://pdos.csail.mit.edu/6.824/labs/lab-raft.html
2) Extended Raft paper: https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf


To build the jar file, please run Maven build package


Testcases in Python are available in ./test
These can be run by "python test.py testName".

Log files for each Raft Server will be created in system's temp directory

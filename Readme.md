# Map Reduce Lab Introduction
A distributed MapReduce, consisting of two programs, the coordinator and the worker.  
There will be just one coordinator process, and one or more worker processes executing in parallel.  
The workers will talk to the coordinator via RPC.  
Each worker process will, in a loop, ask the coordinator for a task, read the task's input from one or more files, execute the task, write the task's output to one or more files, and again ask the coordinator for a new task.  
The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (ten seconds), and give the same task to a different worker.  
The implementation recovers from workers that crash while running tasks.  

Registering the coordinator as an RPC server checks if all its methods are suitable for RPCs.  

The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks.  
Each mapper should create nReduce intermediate files for consumption by the reduce tasks.  
The worker implementation should put the output of the X'th reduce task in the file mr-out-X.  

## Note
The worker are separate processes, starting workers by running the provided mrworker program, which connects to the cordinator via RPC.  
In Go's net/rpc, the server handles multiple RPCs concurrently. If two workers make RPC calls to the coordinator at the same time, the coordinator will process them in parallel goroutines.  

## Desgin


# Lab 2: Key/ Value Server


# Lab 3: Raft
A server can be in three states: candidate, follower and leader.  

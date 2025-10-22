package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	// interface{} is the empty interface—a type that can hold a value of any concrete type.
	Command interface{}
}

const (
	Candidate = 0
	Follower  = 1
	Leader    = 2
	HEART_BEAT_TIMEOUT = 80
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm  int
	votedFor     int
	log          []LogEntry

	// volatile state on all servers
	commitIndex                  int
	lastApplied                  int

	// volatile state on leaders
	// The index of the next log entry the leader will send to that follower.
	nextIndex []int
	matchIndex []int

	// other auxiliary states
	currentState int
	
	voteCount int

	applyCh chan raftapi.ApplyMsg
	winElectCh chan bool
	stepDownCh chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
	lastTimeReceiveAppendEntries time.Time
}

// get the index of the last log entry.
// lock must be held before calling this.
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

// get the term of the last log entry.
// lock must be held before calling this.
func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
	Entries []LogEntry
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

// try send value to an un-buffered channel without blocking
func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	// select with a default case makes the whole operation non-blocking
	select {
	// case ch <- value will only run if the send can proceed immediately.
	// For an unbuffered channel, that means there is already a receiver waiting in a corresponding <- ch 
	// operation at that exact moment.
	case ch <- value:
	default:
	}
}

// If a candidate or leader discovers that its term is out of date,
// it immediately step down to follower.
// lock must be held before calling this.
func (rf *Raft) stepDownToFollower(term int) {
	state := rf.currentState
	rf.currentState = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	// step down if not follower, this check is needed
	// to prevent race where state is already follower
	if state != Follower {
		rf.sendToChannel(rf.stepDownCh, true)
	}
}

// check if the candidate's log is at least as up-to-date as ours
// lock must be held before calling this.
func (rf *Raft) isLogUpToDate(cLastIndex int, cLastTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastIndex(), rf.getLastTerm()

	if cLastTerm == myLastTerm {
		return cLastIndex >= myLastIndex
	}

	return cLastTerm > myLastTerm
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	//If a server receives a request with a stale term number, it rejects the request.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	// The RPC includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Raft use the voting process to prevent a candidate from winning an election unless
	// its log contains all commited entries.
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateID) &&
			rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.sendToChannel(rf.grantVoteCh, true)
	}
}

// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// discard stale replies
	if rf.currentState != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	// If the peer reports a higher term, step down to follower, and update term
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		// only send once when vote count just reaches majority
		if rf.voteCount == len(rf.peers) / 2 + 1 {
			rf.sendToChannel(rf.winElectCh, true)
		}
	}
	return ok
}

// broadcast RequestVote RPCs to all peers in parallel for best performance.
// lock must be held before calling this.
func (rf *Raft) broadcastRequestVote() {
	if rf.currentState != Candidate {
		return
	}

	args := RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateID: rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm: rf.getLastTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

// apply the committed logs on server's state machine, in log-index order once
// entries are committed.
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// commitIndex is the highest log index known to be committed (stored on a majority).
	// lastApplied is highest log index already applied to the state machine.
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- raftapi.ApplyMsg {
			CommandValid: true,
			Command: rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}



// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//If a server receives a request with a stale term number, it rejects the request.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}

	// If the leader's term is at least as large as the candidate's current term, then
	// the candidate recognizes the leader as legitimate and returns to follower state.
	if args.Term >= rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	rf.sendToChannel(rf.heartbeatCh, true)

	// Initialze reply
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// follower log is shorter than leader
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	// log consistency check fails, i.e. different term at prevLogIndex
	if cfTerm := rf.log[args.PrevLogIndex].Term; cfTerm != args.PrevLogTerm {
		reply.ConflictTerm = cfTerm
		// Move ConflictIndex to the first index whose term == cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.log[i].Term == cfTerm; i-- {
			reply.ConflictIndex = i
		}
		reply.Success = false
		return
	}

	// only truncate log if an existing entry conflicts with a new one
	// PrevLogindex is the index of the entry immediately before the new entries a leader
	// wants to follower to match/ append
	i := args.PrevLogIndex + 1
	j := 0
	for ; i < lastIndex + 1 && j < len(args.Entries); i, j = i + 1, j + 1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}

	rf.log = rf.log[:i]  // drop any conflicting suffix
	args.Entries = args.Entries[j:]
	// skip over the already-matching prefix, append the remainder from the leader
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true
	
	// update commit index to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}

		// Apply newly committed entries to the state machine.
		go rf.applyLogs()
	}
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.currentState != Leader) {
		isLeader = false
		return index, term, isLeader
	}
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{term, command})
	index = rf.getLastIndex()
	return index, term, isLeader
}


// convert the raft state to leader.
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.currentState != Candidate {
		return
	}

	rf.resetChannels()
	rf.currentState = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastIndex() + 1
	// When a leader first comes to power, it initializes all nextIndex values to the index just
	// after the last one in its log
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}

	rf.broadcastAppendEntries()
}

// reset the channels, needed when converting server state.
// lock must be held before calling this.
//
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

// convert the raft state to candidate.
func (rf *Raft) convertToCandidate(fromState int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while follower waiting on multiple channels, those signals can arrive nearly simultaneously
	// from different goroutines
	// Suppose a follower times out and calls convertToCandidate(Follower). Before it grabs the lock, 
	// an AppendEntries heartbeat might arrive and set/reset state; or another path already promoted it to Candidate.
	// Without the check, it could apply a stale transition (e.g., flip to Candidate even though 
	// a heartbeat just confirmed a leader), causing incorrect behavior.
	if rf.currentState != fromState {
		return
	}

	// To begin an election, a follower increments its current term and transitions to candidate state
	rf.resetChannels()
	rf.currentState = Candidate
	rf.currentTerm++

	// It then votes for itself
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	// Issues RequestVots RPCs in parallel to each of the other servers in the cluster.
	rf.broadcastRequestVote()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.currentState = Candidate
	rf.mu.Unlock()
}

// Send a AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	// If leader discovers that its term is out of date, it immediately reverts to follower state.
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		return
	}

	// update matchIndex and nextIndex of the follower
	if reply.Success {
		// match index should not regress in case of stale rpc response
		newMatchIndex := args.PrevLogIndex + len(args.Entries)

		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}

		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		// follower's log shorter than leader's
		// After a rejection, the leader update the nextIndex and reties the AppendEntries RPC
		// Eventually nextIndex will reach a point where the leader and follower logs match.
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// try to find the conflictTerm in log
		newNextIndex := rf.getLastIndex()
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.log[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}

		// if not found, set nextIndex to conflictIndex
		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = newNextIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm, set commitIndex = N
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		
		if count > len(rf.peers) / 2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

// broadcast AppendEntries RPCs to all peers in parallel.
// lock must be held before calling this.
func (rf *Raft) broadcastAppendEntries() {
	if rf.currentState != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			// highest idex leader knows to be committed.
			args.LeaderCommit = rf.commitIndex
			entries := rf.log[rf.nextIndex[server]:]
			args.Entries = make([]LogEntry, len(entries))
			// make a deep copy of the entries to send
			copy(args.Entries, entries)
			go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
		}
	}
}

// get the randomized election timeout.
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

// drives Raft's timeouts and state transitions.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// If a follower receives no communication over a period of time
		// called the election timeout, then it assumes there is no viable
		// leader and begins an election to choose a new leader.
		rf.mu.Lock()
		state := rf.currentState
		rf.mu.Unlock()
		
		switch state {
		// A server remains in follower state as long as it receives valid RPCs from a leader or candidate.
		case Follower:
			// select lets a goroutine wait on multiple channel ops at once, whichever case can proceed first runs.
			// When there is no default, the goroutine is parked by the runtime (it doestn't busy-wait or hold an 
			// OS thread). It only blocks the goroutine, not the process or OS thread, other goroutines keep running.
			select {
			// valid vote request from candidate
			case <-rf.grantVoteCh:
			// heartbeats from leaders
			case <-rf.heartbeatCh:
			// If follower receives no communication over election timeout, it begins an election
			// to choose a new leader
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-rf.winElectCh:
				rf.convertToLeader()
			// election timeout, start a new election
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Candidate)
			}
		case Leader:
			select {
			case <-rf.stepDownCh:
			// state should already be follower
			case <-time.After(120 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// When servers start up, they begin as followers.
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.log = append(rf.log, LogEntry{Term: 0})

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

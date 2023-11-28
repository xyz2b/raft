package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type LogEntry struct {
	Term         int
	Command      interface{}
	CommandValid bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	// only leaders
	nextIndex  []int
	matchIndex []int

	// used for election loop
	electionStart   time.Time
	electionTimeout time.Duration

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

func (rf *Raft) LogCountLocked() int {
	return len(rf.log) - 1
}

func PrintLogsLocked(log []LogEntry) []string {
	var printLogEntries []string
	for index, logEntry := range log {
		printLogEntries = append(printLogEntries, fmt.Sprintf("[%d]T%d(%d)", index, logEntry.Term, logEntry.Command))
	}
	return printLogEntries
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
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
	// Your code here (PartC).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

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
/*
Raft协议的服务（例如键/值服务器）希望开始就要添加到Raft日志的下一条命令的达成一致。如果此服务器不是领导者，则返回false。
否则，启动达成一致并立即返回。无法保证此命令将永远提交到Raft日志，因为领导者可能会失败或失去选举。即使Raft实例已被终止，该函数也应以优雅的方式返回。

第一个返回值是该命令如果被提交将出现的索引。第二个返回值是当前任期。第三个返回值是如果该服务器认为它是领导者则为true。
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == Leader

	if !isLeader {
		return index, term, isLeader
	}

	term = rf.currentTerm
	index = rf.LogCountLocked() + 1

	rf.log = append(rf.log, LogEntry{Term: term, Command: command, CommandValid: true})
	rf.matchIndex[rf.me]++
	LOG(rf.me, rf.currentTerm, DLeader, "Append log entry, [%d]T%d(%d), logs: %v", index, term, command, PrintLogsLocked(rf.log))

	return index, term, isLeader
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

const (
	logsInitialCapacity = 10000
)

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	// log 第一个索引是从1开始
	rf.log = make([]LogEntry, 0, logsInitialCapacity)
	// 第一个空的entry，方便边界处理
	rf.log = append(rf.log, LogEntry{Term: 0, CommandValid: false})
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// start application goroutine to start application
	go rf.applicationTicker()

	return rf
}

func (rf *Raft) becomeCandidateLocked() {
	// the leader cannot be to candidate
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s -> Candidate, For T%d->T%d",
		rf.role, rf.currentTerm, rf.currentTerm+1)

	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

// become a follower in `term`, term could not be decreased
func (rf *Raft) becomeFollowerLocked(term int) {
	// if currentTerm is large cannot be to follower
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term")
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s -> Follower, For T%d->T%d",
		rf.role, rf.currentTerm, term)

	// important! Could only reset the `votedFor` when term increased
	// 只有 candidate 收到 leader 心跳之后转换为 follower 才会在等于（term == rf.currentTerm）的情况下进入 becomeFollowerLocked 逻辑
	// 因为 candidate 选举一开始肯定已经投过自己了，同 term 不能再投票了。所以不能重置它的投票。
	if term > rf.currentTerm {
		// unset vote
		rf.votedFor = -1
	}
	rf.role = Follower
	rf.currentTerm = term
}

func (rf *Raft) becomeLeaderLocked() {
	// Only candidate can be to leader
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DLeader, "%s, Only candidate can become Leader", rf.role)
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "%s -> Leader, For T%d, commitIndex: %d, logs: %v",
		rf.role, rf.currentTerm, rf.commitIndex, PrintLogsLocked(rf.log))

	rf.role = Leader

	for peer := 0; peer < len(rf.peers); peer++ {
		// 发送给某个server的下一条日志的索引（初始化为leader最后一条日志索引+1）
		rf.nextIndex[peer] = rf.LogCountLocked() + 1
		// 已知某个server上已经复制的日志的最大索引（初始化为0，自动增加）
		rf.matchIndex[peer] = 0
	}
}

/*
这里面有个检查“上下文”是否丢失的关键函数：contextLostLocked 。上下文，在不同的地方有不同的指代。
在我们的 Raft 的实现中，“上下文”就是指 Term 和 Role。即在一个任期内，只要你的角色没有变化，就能放心地推进状态机。

在多线程环境中，只有通过锁保护起来的临界区内的代码块才可以认为被原子地执行了。
由于在 Raft 实现中，我们使用了大量的 goroutine，因此每当线程新进入一个临界区时，要进行 Raft 上下文的检查。
如果 Raft 的上下文已经被更改，要及时终止 goroutine，避免对状态机做出错误的改动。
*/
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

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

const (
	InvalidIndex int = 0
	InvalidTerm  int = 0
)

type LogEntry struct {
	Term         int
	Command      interface{}
	CommandValid bool
}

// A Go object implementing a single Raft peer.
/*
1. 初始化时，给一条空日志作为哨兵，可以减少很多边界判断：
  1. 可以无脑取最后一条日志，而不用担心是否为空
  2. 在试探后退时，可以退到 0 的位置，而非 -1
2. Leader 的两个数组，本质上是 Leader 对全局 Peer 的 Log 的两个视图：
  1. nextIndex：试探点视图，用于寻找 Leader 和 Follower 的日志匹配点
  2. matchIndex：匹配点视图，收到成功的 AppendEntriesReply 后更新，进而计算 CommitIndex
3. 日志同步分两个阶段（两个阶段都有心跳的作用），分别对应上面两个数组，分水岭是第一次同步成功：
  1. Backtracking：探测匹配点
  2. Appending：正常同步日志
*/
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
	index := 0
	term := 0
	isLeader := false

	// Your code here (PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == Leader

	// Append  日志前一点要先检查自己是否仍然为 Leader：只有 Leader 才能直接 Append 日志到本地，也即整个 Raft Group 只有一个外界数据接收点——那就是 Leader；不遵循此原则，会出现日志冲突。
	if !isLeader {
		return index, term, isLeader
	}

	term = rf.currentTerm
	index = rf.LogCountLocked() + 1

	rf.log = append(rf.log, LogEntry{Term: term, Command: command, CommandValid: true})
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d(%d), logs: %v", index, term, command, PrintLogsLocked(rf.log))

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

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// start apply goroutine to start apply
	go rf.applyTicker()

	return rf
}

/*
这里面有个检查“上下文”是否丢失的关键函数：contextLostLocked 。上下文，在不同的地方有不同的指代。
在我们的 Raft 的实现中，“上下文”就是指 Term 和 Role。即在一个任期内，只要你的角色没有变化，就能放心地推进状态机。

在多线程环境中，只有通过锁保护起来的临界区内的代码块才可以认为被原子地执行了。
由于在 Raft 实现中，我们使用了大量的 goroutine，因此每当线程新进入一个临界区时，要进行 Raft 上下文的检查。
如果 Raft 的上下文已经被更改，要及时终止 goroutine，避免对状态机做出错误的改动。
*/
/*
需要 Context 检查的主要有四个地方：
1. startReplication 前，检查自己仍然是给定 term 的 Leader
2. replicateToPeer 处理 reply 时，检查自己仍然是给定 term 的 Leader
3. startElection 前，检查自己仍然是给定 term 的 Candidate
4. askVoteFromPeer 处理 reply 时，检查自己仍然是给定 term 的 Candidate
由于我们 replication 和 election 实现的对称性，可以发现前两个和后两个是对称的，因此很好记忆。
*/
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

func (rf *Raft) stateString() string {
	return fmt.Sprintf("currentTerm: %d, votedFor: %d, length of log: %d", rf.currentTerm, rf.votedFor, len(rf.log))
}

// 在日志数组中找指定 term 第一条日志的索引
func (rf *Raft) firstIndexFor(term int) int {
	for i, entry := range rf.log {
		if entry.Term == term {
			return i
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

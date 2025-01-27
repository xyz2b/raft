package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d T%d, Last:[%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	lastLogEntryIndex := rf.log.size()
	lastLogEntryTerm := rf.log.at(lastLogEntryIndex).Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastLogEntryIndex, lastLogEntryTerm, candidateIndex, candidateTerm)

	if lastLogEntryTerm != candidateTerm {
		return lastLogEntryTerm > candidateTerm //  local last log entry term is newer than candidate
	}
	return lastLogEntryIndex > candidateIndex //  local last log entry index is newer than candidate
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	// align the term
	/*
		在收到 RPC（回调函数）和收到 RPC 返回值时，第一件事就是要对齐 term。在 Raft 中，term 是一个非常关键的设定，只有在相同 term 内，一切对话才能展开。对齐 term 的逻辑就是：
		  1. 你 term 大，我无条件转 Follower
		  2. 你 term 小，不理会你的请求
	*/
	// 回调函数实现的一个关键点，还是要先对齐 Term。不仅是因为这是之后展开“两个 Peer 对话”的基础，
	// 还是因为在对齐 Term 的过程中，Peer 有可能重置 votedFor。这样即使本来由于已经投过票了而不能再投票，但提高任期重置后，在新的 Term 里，就又有一票可以投了。
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject vote, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	// check the votedFor
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted S%d", args.CandidateId, rf.votedFor)
		return
	}

	// 比较日志的新旧
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject Vote, S%d's log less up-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	/*
		重置时钟本质上是认可对方权威，且承诺自己之后一段时间内不在发起选举。在代码中有两处：
		  1. 接收到心跳 RPC，并且认可其为 Leader
		  2. 接受到选举 RPC，并且给出自己的选票
	*/
	// 还有一点，论文里很隐晦地提到过：只有投票给对方后，才能重置选举 timer。换句话说，在没有投出票时，是不允许重置选举 timer 的。
	// 从感性上来理解，只有“认可对方的权威”（发现对方是 Leader 或者投票给对方）时，才会重置选举 timer —— 本质上是一种“承诺”：认可对方后，短时间就不再去发起选举争抢领导权。
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d vote", args.CandidateId)
}

// example code to send a RequestVote RPC to a server.
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
	return ok
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 50ms - 350ms 之内一次选举还没选举完成，就继续下次选举
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

/*
在 startElection 函数内只使用传入的参数 term
 1. 每次加锁后，都要先校验 contextLost，即状态是否发生变化。
 2. 校验后，统一使用 term，表明 term 没有 change 过。
*/
func (rf *Raft) startElection(term int) bool {
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		// send RPC
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		// handle the response
		// 多个goroutine之间有加锁，所以对votes的修改不会出现问题
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())
		// align the term
		/*
			在接受到 RPC 或者处理 RPC 返回值时的第一步，就是要对齐 Term。因为 Term 在 Raft 中本质上是一种“优先级”或者“权力等级”的体现。
			Peer 的 Term 相同，是对话展开的基础，否则就要先对齐 Term：
			1. 如果对方 Term 比自己小：无视请求，通过返回值“亮出”自己的 Term
			2. 如果对方 Term 比自己大：乖乖跟上对方 Term，变成最“菜”的 Follower
		*/
		// 两个 RPC，candidate 和 leader 处理 reply 的时候，一定要对齐 term，而不是先判断  contextLost
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check the context
		// 对齐 Term 之后，还要检查上下文，即处理 RPC （RPC 回调函数也是在其他线程调用的）返回值和处理多线程本质上一样：都要首先确保上下文没有丢失，才能驱动状态机。
		// 注意：contextLostLocked 始终都要使用传入 startElection 函数的 term
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
			return
		}

		// count votes
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// every time locked
	if rf.contextLostLocked(Candidate, term) {
		return false
	}

	// 注意：要在 for 循环前先获取现在目前最后一条log的索引
	lastLogIndex := rf.log.size()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.log.at(lastLogIndex).Term,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())
		go askVoteFromPeer(peer, args)
	}

	return true
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
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
	rf.persistLocked()
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

	// term 是有可能不变的。在 term 不变时，并不需要 persist——因为 term 不变，votedFor 一定不会被重新赋值。
	shouldPersist := term != rf.currentTerm

	// important! Could only reset the `votedFor` when term increased
	// 只有 candidate 收到 leader 心跳之后转换为 follower 才会在等于（term == rf.currentTerm）的情况下进入 becomeFollowerLocked 逻辑
	// 因为 candidate 选举一开始肯定已经投过自己了，同 term 不能再投票了。所以不能重置它的投票。
	if term > rf.currentTerm {
		// unset vote
		rf.votedFor = -1
	}
	rf.role = Follower
	rf.currentTerm = term
	if shouldPersist {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeLeaderLocked() {
	// Only candidate can be to leader
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DLeader, "%s, Only candidate can become Leader", rf.role)
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "%s -> Leader, For T%d, commitIndex: %d, log len: %d",
		rf.role, rf.currentTerm, rf.commitIndex, rf.log.size())

	rf.role = Leader

	for peer := 0; peer < len(rf.peers); peer++ {
		// 发送给某个server的下一条日志的索引（初始化为leader最后一条日志索引+1）
		rf.nextIndex[peer] = rf.log.size() + 1
		// 已知某个server上已经复制的日志的最大索引（初始化为0，自动增加）
		rf.matchIndex[peer] = 0
	}
}

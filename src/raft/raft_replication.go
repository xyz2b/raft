package raft

import (
	"math"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 心跳接收方在收到心跳时，只要 Leader 的 term 不小于自己，就对其进行认可，变为 Follower，并重置选举时钟，承诺一段时间内不发起选举。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// align the term
	/*
		在收到 RPC（回调函数）和收到 RPC 返回值时，第一件事就是要对齐 term。在 Raft 中，term 是一个非常关键的设定，只有在相同 term 内，一切对话才能展开。对齐 term 的逻辑就是：
		  1. 你 term 大，我无条件转 Follower
		  2. 你 term 小，不理会你的请求
	*/
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}
	// 等于是 candidate如果从新的领导者收到AppendEntries RPC：转换为追随者 的情况
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Receive log entry, entries: %v, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d",
		args.LeaderId, PrintLogsLocked(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	lastLogIndex := rf.LogCountLocked()
	// 如果不包含 在 prevLogIndex 处 任期为 prevLogTerm 的日志条目，返回 false
	if (lastLogIndex < args.PrevLogIndex) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, doesn’t contain an entry at [%d]T%d",
			args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	// 如果现有日志条目与新日志条目冲突（相同索引，但不同的任期），删除现有日志条目及其后面的所有日志条目
	// 然后将新的 leader logs 添加到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 如果 leaderCommit > commitIndex, 设置 commitIndex = min(leaderCommit, 最新日志条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.LogCountLocked())))
		LOG(rf.me, rf.currentTerm, DLog2, "Update commitIndex: %d->%d actively", oldCommitIndex, rf.commitIndex)

		// Update commitIndex 唤醒 apply goroutine
		rf.applyCond.Signal()
	}

	reply.Success = true

	// reset the timer
	/*
		重置时钟本质上是认可对方权威，且承诺自己之后一段时间内不在发起选举。在代码中有两处：
		  1. 接收到心跳 RPC，并且认可其为 Leader
		  2. 接受到选举 RPC，并且给出自己的选票
	*/
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

const (
	replicateInterval = 100 * time.Millisecond
)

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		// 与选举 Loop 不同的是，这里的 startReplication 有个返回值，
		// 主要是检测“上下文”是否还在（ ContextLost ）——一旦发现 Raft Peer 已经不是这个 term 的 Leader 了，就立即退出 Loop。
		ok := rf.startReplication(term)
		if !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}

		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		// align the term
		/*
			在收到 RPC（回调函数）和收到 RPC 返回值时，第一件事就是要对齐 term。在 Raft 中，term 是一个非常关键的设定，只有在相同 term 内，一切对话才能展开。对齐 term 的逻辑就是：
			  1. 你 term 大，我无条件转 Follower
			  2. 你 term 小，不理会你的请求
		*/
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		/*
			如果leader当前最后一条日志的索引 >= follower 的 nextIndex（leader 的 nextIndex[] 中记录的）: 向 follower 发送带有从nextIndex开始的日志条目的AppendEntries RPC
			        如果成功：更新follower在leader本地nextIndex[]中的nextIndex和matchIndex[]中的matchIndex
			        如果由于日志不一致而导致follower追加日志失败: 减小nextIndex并重试
					日志不一致两种情况：
						1.follower 的日志索引还没有到达 PrevLogIndex 处（需要将 leader 的 follower 最新日志索引+1 到 PrevLogIndex 处的日志都发来，然后追加到 follower 中）
						2.follower 在 PrevLogIndex 处的日志 term 和 leader PrevLogIndex 处的日志 term 不一致
							（需要将 leader 的 PrevLogIndex 对应的 term（PrevLogTerm） 的所有日志以及其后term的所有日志都发来，
							然后将 follower PrevLogIndex 对应的 term（PrevLogTerm） 的第一条日志（包含）之后的所有日志删除，然后将leader后面发来的日志追加进去）
					（这里实现直接减到 nextIndex-1（PrevLogIndex）对应的 term（PrevLogTerm） 的第一条日志处，可能会比实际需要的多一些，但是无影响）
		*/
		if !reply.Success {
			idx := rf.nextIndex[peer] - 1
			term := rf.log[idx].Term
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		rf.nextIndex[peer] = rf.nextIndex[peer] + len(args.Entries)
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Accept, Update matchIndex=%v, update nextIndex=%v", peer, rf.matchIndex, rf.nextIndex)

		// 在收到多数 Follower 同步成功的请求后，Leader 要推进 CommitIndex，并让所有 Peer Apply
		// 如果存在一个N，使得 N > commitIndex，并且matchIndex[i]的大多数都 >= N，并且log[N].term == currentTerm: 设置commitIndex = N
		lastLogIndex := rf.LogCountLocked()
		for N := rf.commitIndex + 1; N <= lastLogIndex; N++ {
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= N {
					count++
				}
			}

			if rf.log[N].Term == rf.currentTerm && count > len(rf.peers)/2 {
				oldCommitIndex := rf.commitIndex
				rf.commitIndex = N
				LOG(rf.me, rf.currentTerm, DLog, "Update commitIndex: %d->%d actively", oldCommitIndex, rf.commitIndex)

				// Update commitIndex 唤醒 apply goroutine
				rf.applyCond.Signal()
				break
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果该 raft 已经不是 leader 了，就需要退出 replicationTicker，因为它不是leader了，也不需要再发送心跳以及复制日志了
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	lastLogIndex := rf.LogCountLocked()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// Don't forget to update Leader's matchIndex
			rf.matchIndex[peer] = rf.LogCountLocked()
			rf.nextIndex[peer] = rf.LogCountLocked() + 1
			continue
		}

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		// 领导者收到应用层发来日志后（raft.Start），要通过心跳同步给所有 Follower
		// 如果leader当前最后一条日志的索引 >= follower 的 nextIndex（leader 的 nextIndex[] 中记录的）:
		// 	向 follower 发送带有从nextIndex开始的日志条目的AppendEntries RPC
		if lastLogIndex >= rf.nextIndex[peer] {
			args.Entries = rf.log[rf.nextIndex[peer]:]
		}
		args.PrevLogTerm = rf.log[rf.nextIndex[peer]-1].Term
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send log, Prev=[%d]T%d, Len()=%d", peer, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

		go replicateToPeer(peer, args)
	}

	return true
}

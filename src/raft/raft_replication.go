package raft

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	replicateInterval = 70 * time.Millisecond
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
	Term          int
	Success       bool
	ConflictIndex int // Follower 日志长度（当ConflictTerm为0时） 或者 ConflictTerm 的第一个 entry 的 index（当ConflictTerm不为0时）
	ConflictTerm  int // 0 或者 Follower 与 Leader PrevLog 冲突 entry 所在的 term
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

// 心跳接收方在收到心跳时，只要 Leader 的 term 不小于自己，就对其进行认可，变为 Follower，并重置选举时钟，承诺一段时间内不发起选举。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

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

	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Receive log entry, entries len: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d",
		args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	// reset the timer
	/*
		重置时钟本质上是认可对方权威，且承诺自己之后一段时间内不在发起选举。在代码中有两处：
		  1. 接收到心跳 RPC，并且认可其为 Leader
		  2. 接受到选举 RPC，并且给出自己的选票
	*/
	// 在收到 AppendEntries RPC 时，无论 Follower 接受还是拒绝日志，只要认可对方是 Leader 就要重置时钟。
	// 但在我们之前的实现，只有接受日志才会重置时钟。这是不对的，如果 Leader 和 Follower 匹配日志所花时间特别长，Follower 一直不重置选举时钟，就有可能错误的选举超时触发选举。
	// 这里我们可以用一个 defer 函数来在合适位置之后来无论如何都要重置时钟
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.logString())
		}
	}()

	lastLogIndex := rf.LogCountLocked()
	// 如果不包含 在 prevLogIndex 处 任期为 prevLogTerm 的日志条目，返回 false
	if lastLogIndex < args.PrevLogIndex {
		// 1.如果 Follower 日志过短，则ConflictTerm 置空， ConflictIndex 置为 日志的长度。
		reply.ConflictIndex = rf.LogCountLocked()
		reply.ConflictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, lastLogIndex:%d < PrevLogIndex:%d", args.LeaderId, lastLogIndex, args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 2.否则，将 ConflictTerm 设置为 Follower 在 Leader.PrevLogIndex 处日志的 term
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// ConflictIndex 设置为 ConflictTerm 的第一条日志。
		//idx := args.PrevLogIndex
		//term := rf.log[idx].Term
		//for idx > 0 && rf.log[idx].Term == term {
		//	idx--
		//}
		//reply.ConflictIndex = idx + 1
		reply.ConflictIndex = rf.firstIndexFor(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 如果现有日志条目与新日志条目冲突（相同索引，但不同的任期），删除现有日志条目及其后面的所有日志条目
	// 然后将新的 leader logs 添加到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Server:%d, Follower append logs: (%d, %d]", rf.me, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 如果 leaderCommit > commitIndex, 设置 commitIndex = min(leaderCommit, 最新日志条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.LogCountLocked())))
		LOG(rf.me, rf.currentTerm, DLog2, "Follower update the commit index %d->%d", oldCommitIndex, rf.commitIndex)

		// Update commitIndex 唤醒 apply goroutine
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

/*
在 startReplication 函数内只使用传入的参数 Term：leaderTerm
 1. 每次加锁后，都要先校验 contextLost，即是否为 Leader，是否 rf.currentTerm == leaderTerm 。
 2. 校验后，统一使用 leaderTerm，表明 term 没有 change 过。
*/
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
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// align the term
		/*
			在收到 RPC（回调函数）和收到 RPC 返回值时，第一件事就是要对齐 term。在 Raft 中，term 是一个非常关键的设定，只有在相同 term 内，一切对话才能展开。对齐 term 的逻辑就是：
			  1. 你 term 大，我无条件转 Follower
			  2. 你 term 小，不理会你的请求
		*/
		// 两个 RPC，candidate 和 leader 处理 reply 的时候，一定要对齐 term，而不是先判断  contextLost
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		// 注意：contextLostLocked 始终都要使用传入 startReplication 函数的 term
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// handle the reply, probe the lower index if the prevLog not matched
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]

			// 1. 如果 ConflictTerm 为空，说明 Follower 日志太短，直接将 nextIndex 赋值为 ConflictIndex 迅速回退到 Follower 日志末尾。
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex + 1
			} else {
				// 2. 否则，以 Leader 日志为准，跳过 ConflictTerm 的所有日志；
				firstTermIndex := rf.firstIndexFor(reply.ConflictTerm)
				if firstTermIndex != InvalidIndex {
					rf.nextIndex[peer] = firstTermIndex
				} else { // 如果发现 Leader 日志中不存在 ConflictTerm 的任何日志，则以 Follower 为准跳过 ConflictTerm，即使用 ConflictIndex
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}

			// 匹配探测期比较长时，会有多个探测的 RPC，如果 RPC 结果乱序回来：一个先发出去的探测 RPC 后回来了，
			// 其中所携带的 XTerm、XIndex 和 XLen 就有可能造成 rf.next 的“反复横跳”。为此，我们可以强制 rf.next 单调递减
			// avoid the late reply move the nextIndex forward again
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term)
			LOG(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.logString())
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Accept, Update matchIndex=%v, update nextIndex=%v", peer, rf.matchIndex, rf.nextIndex)

		// 在每次更新 rf.matchIndex 后，依据此全局匹配点视图，我们可以算出多数 Peer 的匹配点，进而更新 Leader 的 CommitIndex。我们可以使用排序后找中位数的方法来计算。
		// 中位数正好是超过半数的 peer 都提交过的 Index（越大的matchIndex说明已经提交的日志越多，所以需要找中位数）
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex {
			// 这里的判断是 图8 中所强调的，不要提交前面任期的日志的关键（只能通过提交本任期的日志来间接提交前面任期的日志）
			// 如果要提交的最后一条日志条目的任期小于当前任期（日志是顺序提交的，保证不会乱序），就不做提交操作，即不提交前面任期的日志
			// 只能提交本任期的日志，不能提交前面任期的日志
			if rf.log[majorityMatched].Term == rf.currentTerm {
				LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
				rf.commitIndex = majorityMatched
				rf.applyCond.Signal()
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

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// Don't forget to update Leader's matchIndex
			// 注意这里要更新 leader 的 matchIndex = len(rf.log) - 1，因为可能 rf.log 可能由于 Start 的调用而改变了。
			rf.matchIndex[peer] = rf.LogCountLocked()
			rf.nextIndex[peer] = rf.LogCountLocked() + 1
			continue
		}

		// 领导者收到应用层发来日志后（raft.Start），要通过心跳同步给所有 Follower
		// 如果leader当前最后一条日志的索引 >= follower 的 nextIndex（leader 的 nextIndex[] 中记录的）:
		// 	向 follower 发送带有从nextIndex开始的日志条目的AppendEntries RPC
		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, %v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}

func (rf *Raft) getMajorityIndexLocked() int {
	// TODO(spw): may could be avoid copying
	tmpIndexes := make([]int, len(rf.peers))
	// tmpIndexes := rf.matchIndex[:] 不会复制 Slice 底层数组。得新建一个 Slice，然后使用 copy 函数才能避免 sort 对 matchIndex 的影响。
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	// 在 Sort 之后，顺序是从小到大的，因此在计算 CommitIndex 时要取中位数偏左边那个数
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx] // min -> max
}

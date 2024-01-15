package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DSnap, "Snap on %d", index)

	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIdx)
		return
	}

	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

// --- raft_log.go
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	// since idx() will use rl.snapLastIdx, so we should keep it first
	idx := rl.idx(index)

	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapLastIdx = index
	rl.snapshot = snapshot

	// allocate a new slice
	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx+1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	SnapLastIdx  int
	SnapLastTerm int
	Snapshot     []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, LastInclude:[%d]T%d, Snapshot length: %d",
		args.LeaderId, args.Term, args.SnapLastIdx, args.SnapLastTerm, len(args.Snapshot))
}
func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnap, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Higher Term, T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// check if it is a RPC which is out of order
	if rf.log.snapLastIdx >= args.SnapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Already installed, Last: %d>=%d", args.LeaderId, rf.log.snapLastIdx, args.SnapLastIdx)
		return
	}
	/*
		在 Follower InstallSnapshot 到 Raft 层时，需要做两件事：
		1. 利用该 snapshot 重置 rf.log ：installSnapshot
		2. 对该 snapshot 进行持久化，以供宕机重启后载入：rf.persistLocked()
		两者顺序不能颠倒，因为 rf.persistLocked() 时需要用到 rf.log 中的新保存的 snapshot，这算个隐式依赖，其实风格不太好，在工程实践中尽量避免，或者增加一些详细注释。
	*/
	// install the snapshot
	rf.log.installSnapshot(args.SnapLastIdx, args.SnapLastTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installOnPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DSnap, "-> S%d, Lost or crashed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, InstallSnap, Reply=%v", peer, reply.String())

	// align the term
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	// check context lost
	// 注意：contextLostLocked 始终都要使用传入 installOnPeer 函数的 term
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
		return
	}

	/*
		1. 记得更新：在处理 InstallSnapshotReply 时，很容易忘了更新 matchIndex 和 nextIndex，这会造成不断重复发 InstallSnapshot RPC。
		2. 条件更新：主要为了处理 RPC Reply 乱序返回的情况。仅仅在 args.LastIncludedIndex > rf.matchIndex[peer] 才更新，
			这是因为，如果有多个 InstallSnapshotReply 乱序回来，且较小的 args.LastIncludedIndex 后回来的话，如果不加判断，会造成matchIndex 和 nextIndex 的反复横跳。
	*/
	// update the match and next
	if args.SnapLastIdx > rf.matchIndex[peer] { // to avoid disorder reply
		rf.matchIndex[peer] = args.SnapLastIdx
		rf.nextIndex[peer] = args.SnapLastIdx + 1
	}

	// note: we need not try to update the commitIndex again,
	// because the snapshot included indexes are all committed
}

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

	if index <= rf.log.snapLastIdx || index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Could not snapshot beyond [%d, %d]", rf.log.snapLastIdx+1, rf.commitIndex)
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
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, InstallSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm

	rf.log.snapLastIdx = args.SnapLastIdx
	rf.log.snapLastTerm = args.SnapLastTerm
	rf.log.snapshot = make([]byte, len(args.Snapshot))
	copy(rf.log.snapshot, args.Snapshot)

	if rf.log.size() >= args.SnapLastIdx {
		idx := rf.log.idx(rf.log.snapLastIdx)

		// allocate a new slice
		newLog := make([]LogEntry, 0, rf.log.size()-rf.log.snapLastIdx+1)
		newLog = append(newLog, LogEntry{
			Term: rf.log.snapLastTerm,
		})
		newLog = append(newLog, rf.log.tailLog[idx+1:]...)
		rf.log.tailLog = newLog
	}
	rf.persistLocked()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

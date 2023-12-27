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

	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "snapshot index: %d is longer than commitIndex: %d", index, rf.commitIndex)
		return
	}

	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.log[index].Term
	rf.snapshot = snapshot
	// 删除 index 之前的日志，使用copy的方式。
	// 使用 Golang 中的切片进行截断时，底层可能并没有真正的丢弃数据，因此需要使用：比如新建+拷贝替代切片，以保证 GC 真正会回收相应空间。
	newLogInitLength := (rf.LogCountLocked()-rf.lastIncludeIndex)*2 + 1
	newLog := make([]LogEntry, newLogInitLength)
	newLog = append(newLog, LogEntry{Term: InvalidTerm, CommandValid: false})
	copy(newLog, rf.log[rf.lastIncludeIndex+1:])
	rf.log = newLog
	rf.persistLocked()
}

func (rf *Raft) readSnapshot(snapshot []byte) {
	rf.snapshot = snapshot

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotIndex: rf.lastIncludeIndex,
		SnapshotTerm:  rf.lastIncludeTerm,
	}

	LOG(rf.me, rf.currentTerm, DSnap, "Read Snapshot, len: %d, SnapshotIndex: %d, SnapshotTerm: %d", len(snapshot), rf.lastIncludeIndex, rf.lastIncludeTerm)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, LastInclude:[%d]T%d, Snapshot length: %d",
		args.LeaderId, args.Term, args.LastIncludeIndex, args.LastIncludeTerm, len(args.Snapshot))
}
func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, InstallSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.snapshot = make([]byte, len(args.Snapshot))
	copy(rf.snapshot, args.Snapshot)

	if rf.LogCountLocked() >= args.LastIncludeIndex {
		// 删除 index 之前的日志，使用copy的方式。
		// 使用 Golang 中的切片进行截断时，底层可能并没有真正的丢弃数据，因此需要使用：比如新建+拷贝替代切片，以保证 GC 真正会回收相应空间。
		newLogInitLength := (rf.LogCountLocked()-rf.lastIncludeIndex)*2 + 1
		newLog := make([]LogEntry, newLogInitLength)
		newLog = append(newLog, LogEntry{Term: InvalidTerm, CommandValid: false})
		copy(newLog, rf.log[rf.lastIncludeIndex+1:])
		rf.log = newLog
	}
	rf.persistLocked()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

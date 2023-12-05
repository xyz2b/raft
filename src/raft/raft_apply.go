package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 只要我们保证全局就只有这一个 apply 的地方，那我们这样分成三个部分问题就不大。尤其是需要注意，当后面增加  snapshot  apply 的逻辑时，也要放到该函数里。
func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.mu.Unlock()

		// 在给 applyCh 发送 ApplyMsg 时，不要在加锁的情况下进行。因为我们并不知道这个操作会耗时多久（即应用层多久会取走数据），因此不能让其在 apply 的时候持有锁。
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i, // must be cautious
			}
		}

		rf.mu.Lock()
		oldLastApplied := rf.lastApplied
		rf.lastApplied += len(entries)
		LOG(rf.me, rf.currentTerm, DApply, "Apply log as %s, idx=[%d, %d]", rf.role, oldLastApplied+1, rf.lastApplied)
		rf.mu.Unlock()
	}
}

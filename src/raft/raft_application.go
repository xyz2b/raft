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
		snapPendingInstall := rf.snapPending

		if !snapPendingInstall {
			// 避免 apply 到和 snapshot 有交叠的日志，snapshot 的 apply 在 snapPendingInstall 为 true 的逻辑里会进行
			//（乱序？不会，因为有收到snapshot时候一定会置位snapPendingInstall，设置了snapPendingInstall就一定会先apply snapshot，只有当snapPendingInstall置位取消，才会去apply 现有日志）
			// 什么时候会走到这个逻辑，当snapshot已经apply了，但是还未修改lastApplied的时候，crash了（因为真正apply到channel的时候是没有加锁的），重启之后会走到这里。
			//if rf.lastApplied < rf.log.snapLastIdx {
			//	rf.lastApplied = rf.log.snapLastIdx
			//}

			// make sure that the rf.log have all the entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end > rf.log.size() {
				end = rf.log.size()
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		if !snapPendingInstall {
			// 在给 applyCh 发送 ApplyMsg 时，不要在加锁的情况下进行。因为我们并不知道这个操作会耗时多久（即应用层多久会取走数据），因此不能让其在 apply 的时候持有锁。
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i, // must be cautious
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		rf.mu.Lock()
		if !snapPendingInstall {
			oldLastApplied := rf.lastApplied
			rf.lastApplied += len(entries)
			LOG(rf.me, rf.currentTerm, DApply, "Apply log as %s, server=%d, appleIdx=[%d, %d], commitIdx=%d, applyLog=%s", rf.role, rf.me, oldLastApplied+1, rf.lastApplied, rf.commitIndex, rf.printLog(entries))
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Install Snapshot for [0, %d]", rf.log.snapLastIdx)
			/*
				需要注意的是 apply 了 snapshot 之后，要：
				1. 更新 commitIndex 和 lastApplied ：避免 apply 到和 snapshot 有交叠的日志
				2. 清除 snapPending 标记位：避免重复 apply snapshot
			*/
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}

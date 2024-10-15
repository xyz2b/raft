// Package sentinel 提供了表示本 Sentinel 的实例
package sentinel

import (
	"github.com/google/uuid"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ConfigDefaultHZ     int = 10
	RandMax             int = math.MaxInt
	SENTINEL_MAX_DESYNC int = 1000
)

// SentinelState 表示本 Sentinel 实例
type Sentinel struct {
	mu sync.Mutex

	// 本 Sentinel 所监控的 MySQL Master 节点
	master    *MysqlInstance
	slaves    map[string]*MysqlInstance
	sentinels map[string]*SentinelInstance

	me      string
	address *net.TCPAddr

	// 本 Sentinel 当前任期号
	currentEpoch int64

	dead int32 // set by Kill()
}

func (sentinel *Sentinel) getAddress() *net.TCPAddr {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	return sentinel.address
}

func (sentinel *Sentinel) getMaster() *MysqlInstance {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	return sentinel.master
}

func (sentinel *Sentinel) getSentinels() map[string]*SentinelInstance {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	return sentinel.sentinels
}

func (sentinel *Sentinel) sentinelForceHelloUpdateForSentinels() {
	for _, sentinel := range sentinel.sentinels {
		sentinel.forceHelloUpdate()
	}
}

func (sentinel *Sentinel) getCurrentEpoch() int64 {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	return sentinel.currentEpoch
}

func (sentinel *Sentinel) setCurrentEpoch(currentEpoch int64) {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	sentinel.currentEpoch = currentEpoch
}

func (sentinel *Sentinel) Kill() {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	atomic.StoreInt32(&sentinel.dead, 1)
}

func (sentinel *Sentinel) killed() bool {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	z := atomic.LoadInt32(&sentinel.dead)
	return z == 1
}

func (sentinel *Sentinel) Make() {
	sentinel.me = uuid.New().String()
	sentinel.ticker()
}

func (sentinel *Sentinel) ticker() {
	// 第一次立即运行
	ticker := time.NewTicker(1 * time.Nanosecond)
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	for !sentinel.killed() {
		select {
		case _ = <-ticker.C:
			go sentinel.serverCron()

			next := 1000 / (ConfigDefaultHZ + r.Intn(RandMax)%ConfigDefaultHZ)
			ticker = time.NewTicker(time.Duration(next) * time.Millisecond)
		}
	}
}

func (sentinel *Sentinel) serverCron() {
	sentinel.handleInstances()
}

func (sentinel *Sentinel) handleInstances() {
	var switchToPromoted *MysqlInstance

	LOG(sentinel.master.GetFlags(), sentinel.master.getAddress().String(), DDebug, "Start handle instance")
	go sentinel.master.HandleInstance()

	for slaveIp, slaveInstance := range sentinel.slaves {
		LOG(slaveInstance.GetFlags(), slaveIp, DDebug, "Start handle instance")
		go slaveInstance.HandleInstance()
	}

	for sentinelIp, sentinelInstance := range sentinel.sentinels {
		LOG(sentinelInstance.GetFlags(), sentinelIp, DDebug, "Start handle instance")
		go sentinelInstance.HandleInstance()
	}

	if sentinel.master.getFailoverState() == FailoverStateUpdateConfig {
		switchToPromoted = sentinel.master
	}

	if switchToPromoted != nil {
		go switchToPromoted.FailoverSwitchToPromotedSlave()
	}
}

/**
 * 从 master 的 Sentinel 字典中根据 ip port RunId 获取对应的 Sentinel instance
 * 1.如果 ip 为空并且 port 为0，则只比较 RunId
 * 2.如果 RunId 为空，则只比较 ip 和 port
 * 3.如果都不为空，都需要进行比较
 * */
func (sentinel *Sentinel) getSentinelInstanceByAddrAndRunID(peerSentinelIP string, peerSentinelPort int, peerSentinelRunId string) *SentinelInstance {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	sentinels := sentinel.sentinels

	if peerSentinelIP == "" && peerSentinelPort == 0 && peerSentinelRunId != "" { // 只比较 RunId
		sentinel, ok := sentinels[peerSentinelRunId]
		if !ok {
			return nil
		}
		return sentinel
	}

	for _, sentinel := range sentinels {
		ip := sentinel.getAddress().IP.String()
		port := sentinel.getAddress().Port
		runId := sentinel.getRunId()

		if peerSentinelIP != "" && peerSentinelPort != 0 && peerSentinelRunId != "" { // 比较 ip、 port 和 RunId
			if peerSentinelIP == ip && peerSentinelPort == port && peerSentinelRunId == runId {
				return sentinel
			}
		} else if peerSentinelIP != "" && peerSentinelPort != 0 { // 比较 ip 和 port
			if peerSentinelIP == ip && peerSentinelPort == port {
				return sentinel
			}
		}
	}
	return nil
}

/**
 * 从本地 Sentinel 的 Sentinel map 中删除指定 RunId 的 Sentinel instance，并关闭本 Sentinel 到该 Sentinel 的连接
 * @param peerSentinelRunId sentinel RunId
 * @return 删除的 Sentinel instance 数量
 * */
func (sentinel *Sentinel) removeMatchingSentinelFromMaster(peerSentinelRunId string) int {

	sentinels := sentinel.sentinels
	if oldSentinel, ok := sentinels[peerSentinelRunId]; ok {
		delete(sentinels, peerSentinelRunId)

		// 关闭老的 Sentinel连接
		if oldSentinel != nil && oldSentinel.connection != nil {
			oldSentinel.connection.close()
		}

		return 1
	}
	return 0
}

/**
 * 重置 master，将其地址改为所指定的 ip 和 port，因为我们的 Sentinel 只监控一个 master，所以不需要给 master 定义名字，来保持名字不变
 * 可能会有 slave 的晋升，也有可能是 master 换了地址
 * */
func (sentinel *Sentinel) resetMasterAndChangeAddress(peerSentinelMasterIp string, peerSentinelMasterPort int) {

	peerSentinelMasterAddrStr := IpPortToAddrString(peerSentinelMasterIp, peerSentinelMasterPort)
	// 将字符串地址解析为net.TCPAddr
	newAdrr, err := StringToAddr(peerSentinelMasterAddrStr)
	if err != nil {
		LOG(SENTINEL, peerSentinelMasterAddrStr, DError, "ResolveTCPAddr failed, addr: %s, err: %s", peerSentinelMasterAddrStr, err.Error())
		return
	}

	var promotedSlave *net.TCPAddr
	for _, instance := range sentinel.slaves {
		if AddrOrHostnameEqual(instance.Address, newAdrr) {
			promotedSlave = newAdrr
		}
	}

	var promotedSlaveRunId string
	// slave 晋升 master
	if promotedSlave != nil {
		if _, ok := sentinel.slaves[peerSentinelMasterAddrStr]; ok {
			promotedSlaveRunId = sentinel.slaves[peerSentinelMasterAddrStr].getRunId()
			delete(sentinel.slaves, peerSentinelMasterAddrStr)
		}
	}

	var newSlave *net.TCPAddr
	var newSlaveRunId string
	// 老 master 的地址和要切换的新 master 地址不同，则老 master 要变成 slave
	if !AddrOrHostnameEqual(sentinel.master.getAddress(), newAdrr) {
		newSlave = sentinel.master.getAddress()
		newSlaveRunId = sentinel.master.getRunId()
	}

	sentinel.resetMaster(newAdrr, promotedSlaveRunId)

	if newSlave != nil {
		slave := MakeMysqlInstance(SLAVE, newSlave, newSlaveRunId, sentinel)
		sentinel.slaves[AddrToString(newSlave)] = slave
	}
}

func (sentinel *Sentinel) resetMaster(newAdrr *net.TCPAddr, promotedSlaveRunId string) {
	sentinel.master.connection.close()
	sentinel.master = MakeMysqlInstance(MASTER, newAdrr, promotedSlaveRunId, sentinel)

}

func (sentinel *Sentinel) Hello(args *HelloArgs, reply *HelloReply) {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	peerSentinelMasterIp := args.MasterIp
	peerSentinelMasterPort := args.MasterPort
	peerSentinelCurrentEpoch := args.CurrentEpoch
	peerSentinelMasterConfigEpoch := args.MasterConfigEpoch

	// 1. 首先看下本 Sentinel 是否已经拥有了发送 HELLO 消息的 Sentinel 的信息
	peerSentinelIP := args.Ip
	peerSentinelPort := args.Port
	peerSentinelRunId := args.RunId

	peerSentinel := sentinel.getSentinelInstanceByAddrAndRunID(peerSentinelIP, peerSentinelPort, peerSentinelRunId)

	if peerSentinel == nil {
		// 如果本 Sentinel 本地没有发送 HELLO 消息的 Sentinel 的信息，
		// 则删除本地所有具有相同 RunId 的 Sentinel，因为这些 Sentinel 的地址发生了变化（RunId 相同，地址不同）
		// 添加一个具有相同 RunId 以及新的地址的 Sentinel 到本地 Sentinel 的 map 中
		removed := sentinel.removeMatchingSentinelFromMaster(peerSentinelRunId)
		if removed > 0 {
			LOG(SENTINEL, sentinel.getAddress().String(), DInfo, "+sentinel-address-switch,  master: %s ip: %s port: %s for %s",
				sentinel.master.getAddress().String(), peerSentinelIP, peerSentinelPort, peerSentinelRunId)
		} else {
			// 如果本地没有 相同 RunId 不同 ip 和 port 的 Sentinel
			// 此时需要检查是否有另一个 Sentinel 与 发送 hello 消息的 Sentinel 的地址相同，但是 RunId 不同
			// 如果发生这种情况，表示已经存在的地址相同的那个 Sentinel 信息比较老，就将其删除
			other := sentinel.getSentinelInstanceByAddrAndRunID(peerSentinelIP, peerSentinelPort, "")
			if other != nil {
				// 如果已经存在具有相同地址不同 RunId 的 Sentinel，代表它的信息比较老，就移除它
				LOG(SENTINEL, sentinel.address.String(), DWarn, "+sentinel-invalid-addr, sentinel: %s",
					other.getAddress().String())
				runId := other.getRunId()
				sentinel.removeMatchingSentinelFromMaster(runId)
			}
		}

		// 流程走到这里，就代表它是一个新的 Sentinel，就添加一个新的 Sentinel
		peerSentinel := MakeSentinelInstance(peerSentinelRunId, peerSentinelIP, peerSentinelPort, sentinel)
		peerSentinelAddrStr := peerSentinelIP + ":" + strconv.Itoa(peerSentinelPort)
		sentinel.mu.Lock()
		sentinel.sentinels[peerSentinelAddrStr] = peerSentinel
		sentinel.mu.Unlock()

		if peerSentinel != nil {
			if removed == 0 {
				LOG(SENTINEL, sentinel.address.String(), DWarn, "+sentinel: %s", peerSentinelAddrStr)
			}
		}
	}

	// 如果收到的 HELLO 消息中的 currentEpoch 更新，就更新本地的 currentEpoch
	if peerSentinelCurrentEpoch > sentinel.getCurrentEpoch() {
		sentinel.setCurrentEpoch(peerSentinelCurrentEpoch)
	}

	// peerSentinel发过来的masterConfigEpoch更新，同时他发过来的master地址和当前Sentinel的master地址不同，
	// 则需要更新当前Sentinel master以及slave的信息。因为当前Sentinel认为的master已经是老的了，它可能已经变成了slave了
	if peerSentinel != nil && sentinel.master.getConfigEpoch() < peerSentinelMasterConfigEpoch {
		sentinel.master.setConfigEpoch(peerSentinelMasterConfigEpoch)
		if sentinel.master.getAddress().Port != peerSentinelMasterPort || sentinel.master.getAddress().IP.String() != peerSentinelMasterIp {
			LOG(SENTINEL, sentinel.address.String(), DWarn, "+config-update-from: %s", peerSentinel.getAddress().String())
			LOG(SENTINEL, sentinel.address.String(), DWarn, "+switch-master: %s to %s", sentinel.master.getAddress().String(), peerSentinelMasterIp+":"+strconv.Itoa(peerSentinelMasterPort))
			sentinel.resetMasterAndChangeAddress(peerSentinelMasterIp, peerSentinelMasterPort)
		}
	}

	if peerSentinel != nil {
		// peer sentinel 最后一次 发送 hello 给 本sentinel 的时间
		peerSentinel.setLastReceiveHelloTime(NowUnixMillisecond())
	}

	return
}

/**
 * 如果给的 ip 和 port 是当前 master 的，就返回 master instance，否则返回 null
 * */
func (sentinel *Sentinel) sentinelGetMasterByAddress(ip string, port int) *MysqlInstance {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	masterAddress := sentinel.master.getAddress()
	if masterAddress.IP.String() == ip && masterAddress.Port == port {
		return sentinel.master
	}
	return nil
}

func (sentinel *Sentinel) sentinelVoteLeader(instance *MysqlInstance, reqEpoch int64, reqRunId string) (string, int64) {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()

	// 新的纪元
	if reqEpoch > sentinel.currentEpoch {
		sentinel.currentEpoch = reqEpoch
		LOG(SENTINEL, sentinel.address.String(), DWarn, "+new-epoch, master: %s, currentEpoch: %s", sentinel.master.getAddress().String(), sentinel.getCurrentEpoch())
	}

	if sentinel.master.getLeaderEpoch() < reqEpoch && sentinel.currentEpoch <= reqEpoch {
		sentinel.master.setLeader(reqRunId)
		sentinel.master.setLeaderEpoch(sentinel.currentEpoch)
		LOG(SENTINEL, sentinel.address.String(), DWarn, "+vote-for-leader, master: %s, leader:%s, leaderEpoch: %d", sentinel.master.getAddress().String(), sentinel.master.getLeader(), sentinel.master.getLeaderEpoch())

		// 如果我们不投票给自己，那么设置 master 的 failover 开始时间为现在并且加上一个随机时间，为了对同一个 master 进行 failover 之前，能够有一个延迟
		if sentinel.master.getLeader() != sentinel.me {
			source := rand.NewSource(time.Now().UnixNano())
			r := rand.New(source)
			sentinel.master.setFailoverStartTime(NowUnixMillisecond() + int64(r.Intn(RandMax)%SENTINEL_MAX_DESYNC))
		}
	}

	return sentinel.master.getLeader(), sentinel.master.getLeaderEpoch()
}

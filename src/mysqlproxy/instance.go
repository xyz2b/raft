// Package instance 提供了表示本 Sentinel 所监控的 MySQL 以及其他 Sentinel 的实例
package sentinel

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net"
	"strconv"
	"sync"
)

const (
	SHOW_SLAVE_STATUS_SQL Sql = "show slave status;"
	STOP_SLAVE_SQL        Sql = "stop slave;"
	START_SLAVE_SQL       Sql = "start slave;"
	RESET_SLAVE_SQL       Sql = "reset slave;"
	RESET_SALVE_ALL_SQL   Sql = "reset slave all;"
	CHANGE_MASTER_TO      Sql = "change master to master_host='%s', master_port=%d, master_user='%s', master_password='%s', master_auto_position=1;"
	SET_READONLY          Sql = "set global read_only=1;"
	UNSET_READONLY        Sql = "set global read_only=0;"
	SET_SUPER_READONLY    Sql = "set global super_read_only=1;"
	UNSET_SUPER_READONLY  Sql = "set global super_read_only=0;"
)

type Flags int

func (flags *Flags) Str() string {
	var role string
	if flags.Is(MASTER) {
		role = "Master"
	} else if flags.Is(SLAVE) {
		role = "Slave"
	} else if flags.Is(SENTINEL) {
		role = "Sentinel"
	} else if flags.Is(PROMOTED) {
		role = "Promoted"
	} else {
		role = "Unknown"
	}
	return role
}

func (flags *Flags) String() string {
	var flagsStr string

	if flags.Is(MASTER) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "MASTER")
	} else if flags.Is(SLAVE) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "SLAVE")
	} else if flags.Is(SENTINEL) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "SENTINEL")
	} else if flags.Is(PROMOTED) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "PROMOTED")
	} else if flags.Is(S_DOWN) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "S_DOWN")
	} else if flags.Is(O_DOWN) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "O_DOWN")
	} else if flags.Is(MASTER_DOWN) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "MASTER_DOWN")
	} else if flags.Is(FAILOVER_IN_PROGRESS) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "FAILOVER_IN_PROGRESS")
	} else if flags.Is(ReconfSent) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "ReconfSent")
	} else if flags.Is(ReconfInprog) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "ReconfInprog")
	} else if flags.Is(ReconfDone) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "ReconfDone")
	} else if flags.Is(ForceFailover) {
		flagsStr = fmt.Sprintf("%s|%s", flagsStr, "ForceFailover")
	} else {
		flagsStr = "Unknown"
	}
	return flagsStr
}

const (
	MASTER               Flags = 1 << 0
	SLAVE                Flags = 1 << 1
	SENTINEL             Flags = 1 << 2
	S_DOWN               Flags = 1 << 3
	O_DOWN               Flags = 1 << 4
	MASTER_DOWN          Flags = 1 << 5
	FAILOVER_IN_PROGRESS Flags = 1 << 6
	PROMOTED             Flags = 1 << 7
	ReconfSent           Flags = 1 << 8
	ReconfInprog         Flags = 1 << 9
	ReconfDone           Flags = 1 << 10
	ForceFailover        Flags = 1 << 11
)

func (f Flags) Is(flags Flags) bool {
	return int(f)&int(flags) != 0
}

type FailoverState int

const (
	FailoverStateNone FailoverState = iota
	FailoverStateWaitStart
	FailoverStateSelectSlave
	FailoverStateSendSlaveofNoOne
	FailoverStateWaitPromotion
	FailoverStateReconfSlaves
	FailoverStateUpdateConfig
)

type SentinelMasterLinkStatus uint8

const (
	SENTINEL_MASTER_LINK_STATUS_UP SentinelMasterLinkStatus = iota
	SENTINEL_MASTER_LINK_STATUS_DOWN
)

const (
	PingPeriod int64 = 1000
	// ping 请求默认发送的周期，单位 ms
	SENTINEL_PING_PERIOD int64 = 1000
	// show slave status 请求默认发送的周期，单位 ms
	SENTINEL_SHOW_SLAVE_STATUS_PERIOD int64 = 10000
	// hello 请求默认发送的周期，单位 ms
	SENTINEL_HELLO_PERIOD int64 = 2000

	SENTINEL_SEND_HELLO_PERIOD int64 = 2000

	SENTINEL_DEFAULT_DOWN_AFTER int64 = 30000

	SENTINEL_DEFAULT_FAILOVER_TIMEOUT int64 = 60 * 3 * 1000

	SENTINEL_MIN_LINK_RECONNECT_PERIOD int64 = 15000
)

type Handler interface {
	HandleInstance()
}

type ConnectionManager interface {
	ReconnectInstance() error
}

type PingCommandExecutor interface {
	sendPingCommand()
}

type HelloCommandExecutor interface {
	sendHelloCommand()
}

type ShowSlavesStatusCommandExecutor interface {
	sendShowSlavesStatusCommand()
}

type SentinelCommandExecutor interface {
	PingCommandExecutor
	HelloCommandExecutor
}

type MysqlCommandExecutor interface {
	PingCommandExecutor
	ShowSlavesStatusCommandExecutor
}

type Instance struct {
	mu sync.Mutex

	RunId      string
	flags      Flags
	Address    *net.TCPAddr
	connection Connection
	// 实例无响应多少毫秒之后才会被判断为主观下线，配置文件的配置
	downAfterPeriod int64

	// 认为其主观下线的时间
	sDownSinceTime int64

	// 如果该 instance 是 master，则其为进行 failover 的领导者 Sentinel 的 runID
	// 如果该 instance 是 Sentinel， 则其为该 Sentinel 所投票给的那个 Sentinel 的 runID
	leader      string
	leaderEpoch int64 /* Epoch of the 'leader' field. */

	sentinel *Sentinel

	ConnectionManager
	PingCommandExecutor
}

func (instance *Instance) getLeaderEpoch() int64 {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	return instance.leaderEpoch
}

func (instance *Instance) setLeaderEpoch(leaderEpoch int64) {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	instance.leaderEpoch = leaderEpoch
}

func (instance *Instance) getLeader() string {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	return instance.leader
}

func (instance *Instance) setLeader(leader string) {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	instance.leader = leader
}

func (instance *Instance) getDownAfterPeriod() int64 {
	return instance.downAfterPeriod
}

func (instance *Instance) getRunId() string {
	return instance.RunId
}

func (instance *Instance) getAddress() *net.TCPAddr {
	//instance.mu.Lock()
	//defer instance.mu.Unlock()
	return instance.Address
}

func (instance *Instance) deleteFlags(flag Flags) {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	instance.flags = instance.flags & (^flag)
}

func (instance *Instance) addFlags(flag Flags) {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	instance.flags = instance.flags | flag
}

func (instance *Instance) GetFlags() Flags {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	return instance.flags
}

func (instance *Instance) sendPingCommand() {
	now := NowUnixMillisecond()
	instance.connection.setLastPingTime(now)
	// 仅当我们收到前一个 ping 的 pong 时，我们才会更新 act_ping_time
	if instance.connection.getActPingTime() == 0 {
		instance.connection.setActPingTime(now)
	}

	err := instance.connection.sendPing()
	if err != nil {
		LOG(instance.flags, instance.getAddress().String(), DError, "Send ping failed! %s", err.Error())
		return
	}

	now = NowUnixMillisecond()
	instance.connection.setLastAvailTime(now)
	// 收到了 PONG 响应
	instance.connection.setActPingTime(0)
	instance.connection.setLastPongTime(now)
}

func (instance *Instance) ReconnectInstance() error {

	if instance.getAddress() == nil || instance.getAddress().Port == 0 {
		return errors.New("instance address is must be nil or port must be zero")
	}

	if instance.connection.isConnected() {
		LOG(instance.flags, instance.getAddress().String(), DDebug, "instance connection is ok, don't need reconnect")
		return nil
	}

	now := NowUnixMillisecond()
	if now-instance.connection.getLastReconnTime() < PingPeriod {
		LOG(instance.flags, instance.getAddress().String(), DDebug, "It’s too close to the last reconnection time, last reconnection time: %d", instance.connection.getLastReconnTime())
		return nil
	}

	instance.connection.setLastReconnTime(now)

	go func() {
		err := instance.connection.connect()
		if err != nil {
			LOG(instance.flags, instance.getAddress().String(), DError, "Connect to instance failed, %s", err.Error())
			return
		}

		// Send a PING ASAP when reconnecting
		instance.sendPingCommand()
	}()

	return nil
}

// SentinelInstance 表示和本 Sentinel 所监控相同 MySQL 的其他 Sentinel 实例
type SentinelInstance struct {
	Instance

	// 本sentinel最后一次发送hello消息给 该instance代表的sentinel 的时间
	lastSendHelloTime int64

	// 该instance代表的sentinel 最后一次发送hello消息给 本sentinel 的时间
	lastReceiveHelloTime int64

	// 最近一次 is-master-down 响应的时间
	lastMasterDownReplyTime int64
}

func MakeSentinelInstance(runId string, ip string, port int, s *Sentinel) *SentinelInstance {
	var downAfterPeriod int64
	if s.getMaster() != nil {
		downAfterPeriod = s.getMaster().getDownAfterPeriod()
	} else {
		downAfterPeriod = SENTINEL_DEFAULT_DOWN_AFTER
	}

	addrStr := ip + ":" + strconv.Itoa(port)
	// 将字符串地址解析为net.TCPAddr
	address, err := net.ResolveTCPAddr("tcp", addrStr)
	if err != nil {
		LOG(SENTINEL, addrStr, DError, "ResolveTCPAddr failed, addr: %s, err: %s", addrStr, err.Error())
		return nil
	}

	now := NowUnixMillisecond()
	sentinelInstance := &SentinelInstance{
		Instance: Instance{
			Address:         address,
			RunId:           runId,
			sDownSinceTime:  0,
			downAfterPeriod: downAfterPeriod,
			sentinel:        s,
			leader:          "",
			leaderEpoch:     0,
			flags:           SENTINEL,
		},
		lastSendHelloTime:       now,
		lastReceiveHelloTime:    now,
		lastMasterDownReplyTime: now,
	}

	sentinelInstance.connection = MakeSentinelConnection(address, sentinelInstance)

	return sentinelInstance
}

func (sentinel *SentinelInstance) forceHelloUpdate() {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	if sentinel.lastSendHelloTime >= (SENTINEL_SEND_HELLO_PERIOD + 1) {
		sentinel.lastSendHelloTime = sentinel.lastSendHelloTime - (SENTINEL_SEND_HELLO_PERIOD + 1)
	}
}

func (sentinel *SentinelInstance) getLastSendHelloTime() int64 {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	return sentinel.lastSendHelloTime
}

func (sentinel *SentinelInstance) setLastSendHelloTime(lastSendHelloTime int64) {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	sentinel.lastSendHelloTime = lastSendHelloTime
}

func (sentinel *SentinelInstance) getLastReceiveHelloTime() int64 {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	return sentinel.lastSendHelloTime
}

func (sentinel *SentinelInstance) setLastReceiveHelloTime(lastReceiveHelloTime int64) {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()
	sentinel.lastReceiveHelloTime = lastReceiveHelloTime
}

func (sentinel *SentinelInstance) HandleInstance() {
	err := sentinel.ReconnectInstance()
	if err != nil {
		LOG(sentinel.flags, sentinel.getAddress().String(), DError, "ReconnectInstance failed! %s", err.Error())
	}

	sentinel.SendPeriodicCommands()
	sentinel.CheckSubjectivelyDown()
}

func (sentinel *SentinelInstance) GetFlags() Flags {
	return sentinel.flags
}

func (sentinel *SentinelInstance) CheckSubjectivelyDown() error {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()

	var elapsed int64
	if sentinel.connection.getActPingTime() != 0 {
		elapsed = NowUnixMillisecond() - sentinel.connection.getActPingTime()
	} else {
		elapsed = NowUnixMillisecond() - sentinel.connection.getLastAvailTime()
	}

	// 检查 连接至今已经建立成功超过 SENTINEL_MIN_LINK_RECONNECT_PERIOD 时间，但是我们仍有 ping 在一半的超时时间(downAfterPeriod)内未收到响应
	if sentinel.connection.isConnected() &&
		NowUnixMillisecond()-sentinel.connection.getConnectedTime() > SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
		sentinel.connection.getActPingTime() != 0 &&
		NowUnixMillisecond()-sentinel.connection.getActPingTime() > sentinel.downAfterPeriod/2 &&
		NowUnixMillisecond()-sentinel.connection.getLastPongTime() > sentinel.downAfterPeriod/2 {
		LOG(sentinel.flags, sentinel.getAddress().String(), DWarn, "sentinel connection is disconnected %s to %s",
			sentinel.sentinel.getAddress().String(), sentinel.Address.String())

		sentinel.connection.close()
	}

	/*
	 * 添加 SDOWN 标志，如果以下条件满足其一
	 * 1.它没有回复
	 * 2.本 Sentinel 的视角中它是 master，但是它报告自己成为了 slave，并且报告至今的时间超过了 (downAfterPeriod + SENTINEL_SHOW_SLAVE_STATUS_PERIOD * 2)
	 */
	if elapsed > sentinel.downAfterPeriod {
		// Is subjectively down
		if !sentinel.flags.Is(S_DOWN) {
			LOG(sentinel.flags, sentinel.getAddress().String(), DWarn, "+sdown")

			sentinel.sDownSinceTime = NowUnixMillisecond()
			sentinel.addFlags(S_DOWN)
		}
	} else {
		// Is subjectively up
		if sentinel.flags.Is(S_DOWN) {
			LOG(sentinel.flags, sentinel.getAddress().String(), DWarn, "-sdown")

			sentinel.deleteFlags(S_DOWN)
		}
	}

	return nil
}

func (sentinel *SentinelInstance) SendPeriodicCommands() error {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()

	// 发送 Ping 消息的周期，默认为 配置文件中配置的 downAfterPeriod
	pingPeriod := sentinel.downAfterPeriod
	if pingPeriod > SENTINEL_PING_PERIOD {
		pingPeriod = SENTINEL_PING_PERIOD
	}

	now := NowUnixMillisecond()
	if now-sentinel.connection.getLastPongTime() > pingPeriod && (now-sentinel.connection.getLastPingTime()) > pingPeriod/2 {
		go sentinel.sendPingCommand()
	}

	if (now - sentinel.lastSendHelloTime) > SENTINEL_HELLO_PERIOD {
		go sentinel.sendHelloCommand()
	}
	return nil
}

func (sentinel *SentinelInstance) sendHelloCommand() {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()

	args := HelloArgs{
		Ip:                sentinel.sentinel.address.IP.String(),
		Port:              sentinel.sentinel.address.Port,
		RunId:             sentinel.sentinel.me,
		CurrentEpoch:      sentinel.sentinel.getCurrentEpoch(),
		MasterIp:          sentinel.sentinel.getMaster().getAddress().IP.String(),
		MasterPort:        sentinel.sentinel.getMaster().getAddress().Port,
		MasterConfigEpoch: sentinel.sentinel.getMaster().configEpoch,
	}
	_, err := sentinel.connection.sendHello(&args)
	if err != nil {
		LOG(sentinel.flags, sentinel.getAddress().String(), DError, "Send hello failed! %s", err.Error())
		return
	}

	sentinel.lastSendHelloTime = NowUnixMillisecond()
}

func (sentinel *SentinelInstance) sendIsMasterDownByAddr(args *IsMasterDownByAddrArgs) {
	sentinel.mu.Lock()
	defer sentinel.mu.Unlock()

	response, err := sentinel.connection.sendIsMasterDownByAddr(args)
	if err != nil {
		LOG(sentinel.flags, sentinel.getAddress().String(), DError, "Send IsMasterDownByAddr failed! %s", err.Error())
		return
	}

	sentinel.lastMasterDownReplyTime = NowUnixMillisecond()
	if response.DownState {
		sentinel.addFlags(MASTER_DOWN)
	} else {
		sentinel.deleteFlags(MASTER_DOWN)
	}

	// 如果响应中的 runId 字段不为 *，则表明接收到 IsMasterDownByAddr 消息的 Sentinel 已经投票给了 response.LeaderRunId
	if response.LeaderRunId != "*" {
		if sentinel.leaderEpoch != response.LeaderEpoch {
			LOG(sentinel.flags, sentinel.getAddress().String(), DError, " voted for %s %d", response.LeaderRunId, response.LeaderEpoch)

		}
		sentinel.leader = response.LeaderRunId
		sentinel.leaderEpoch = response.LeaderEpoch
	}
}

func (sentinel *SentinelInstance) IsMasterDownByAddr(args *IsMasterDownByAddrArgs, reply *IsMasterDownByAddrReply) {
	ip := args.Ip
	port := args.Port
	reqEpoch := args.CurrentEpoch
	reqRunId := args.RunId

	var leaderRunId string
	var leaderEpoch int64
	var isDown bool

	instance := sentinel.sentinel.sentinelGetMasterByAddress(ip, port)
	// 检查 isMasterDown 消息中指定的主库在自身视角中是否已经主观下线了
	// 存在，并且是主库，并且已经处于 S_DOWN 状态
	if instance != nil && instance.GetFlags().Is(S_DOWN) && instance.GetFlags().Is(MASTER) {
		isDown = true
	}

	// 选举 Leader Sentinel 的逻辑
	// 如果 isMasterDown 消息中包含了 runID，则表示发送者 sentinel 寻求本 sentinel 进行投票选取发送者为 Leader sentinel，
	// 如果不包含，则发送者并不是需求本 sentinel 的投票
	if instance != nil && instance.GetFlags().Is(MASTER) && reqRunId != "*" {
		leaderRunId, leaderEpoch = sentinel.sentinel.sentinelVoteLeader(instance, reqEpoch, reqRunId)
	}
}

// MysqlInstance 表示和本 Sentinel 所监控相同 MySQL 实例
type MysqlInstance struct {
	Instance

	// slave同步master断连持续的时间，ms
	// 由于从mysql slave的状态中看不到断连时间，
	masterLinkDownTime    int64
	slaveMasterLinkStatus SentinelMasterLinkStatus

	// 每次接收到 show slave status 的响应结果时，会刷新该时间
	showSlaveStatusRefresh int64

	masterLogFile    string
	execMasterLogPos int64

	slaveMasterHost string
	slaveMasterPort int

	roleReported     Flags
	roleReportedTime int64

	// 最近一次 slave 所同步的 master 地址变化的时间
	slaveConfChangeTime int64

	// 认为其客观下线的时间
	oDownSinceTime int64

	// 认为其主观下线的时间
	sDownSinceTime int64

	// 刷新故障迁移状态的最大时限，配置文件的配置
	failoverTimeout int64

	// 发送 chang master to <new> 的时间
	slaveReconfSentTime int64

	// 待晋升为 master 的 slave
	PromotedSlave *Instance
	// failover 状态机此时的状态，见 SentinelFailoverState 定义
	failoverState FailoverState
	// 配置纪元，用于故障转移
	configEpoch int64
	// 当前启动的故障转移的纪元
	failoverEpoch int64
	// failover状态机最近一次发生状态迁移的时间
	failoverStateChangeTime int64

	// 记录下 failover 会延迟发生，它的值等于 failoverStartTime
	failoverDelayLogged int64
	// 上一次开启 failover 的时间
	failoverStartTime int64

	// 判断这个实例客观下线所需的支持投票数，配置文件的配置
	quorum int
}

func MakeMysqlInstance(flag Flags, addr *net.TCPAddr, runId string, s *Sentinel) *MysqlInstance {
	if runId == "" {
		runId = uuid.New().String()
	}
	var downAfterPeriod int64
	if s.getMaster() != nil {
		downAfterPeriod = s.getMaster().getDownAfterPeriod()
	} else {
		downAfterPeriod = SENTINEL_DEFAULT_DOWN_AFTER
	}

	mysqlInstance := &MysqlInstance{
		Instance: Instance{
			flags:           flag,
			Address:         addr,
			RunId:           runId,
			downAfterPeriod: downAfterPeriod,
			sentinel:        s,
			leader:          "",
			leaderEpoch:     0,
		},
		failoverTimeout:         Config.FailoverTimeout,
		sDownSinceTime:          0,
		oDownSinceTime:          0,
		roleReported:            flag,
		masterLinkDownTime:      0,
		slaveReconfSentTime:     0,
		slaveMasterHost:         "",
		slaveMasterPort:         0,
		slaveMasterLinkStatus:   SENTINEL_MASTER_LINK_STATUS_DOWN,
		showSlaveStatusRefresh:  0,
		roleReportedTime:        NowUnixMillisecond(),
		slaveConfChangeTime:     NowUnixMillisecond(),
		configEpoch:             0,
		quorum:                  Config.Quorum,
		failoverEpoch:           0,
		failoverState:           FailoverStateNone,
		failoverStateChangeTime: 0,
		failoverDelayLogged:     0,
		failoverStartTime:       0,
		PromotedSlave:           nil,
	}
	mysqlInstance.connection = MakeMysqlConnection(addr, mysqlInstance)
	return mysqlInstance
}

func (mysql *MysqlInstance) getFailoverStartTime() int64 {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.failoverStartTime
}

func (mysql *MysqlInstance) setFailoverStartTime(failoverStartTime int64) {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	mysql.failoverStartTime = failoverStartTime
}

func (mysql *MysqlInstance) getFailoverTimeout() int64 {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.failoverTimeout
}

func (mysql *MysqlInstance) CheckSubjectivelyDown() error {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()

	var elapsed int64
	if mysql.connection.getActPingTime() != 0 {
		elapsed = NowUnixMillisecond() - mysql.connection.getActPingTime()
	} else {
		elapsed = NowUnixMillisecond() - mysql.connection.getLastAvailTime()
	}

	// 检查 连接至今已经建立成功超过 SENTINEL_MIN_LINK_RECONNECT_PERIOD 时间，但是我们仍有 ping 在一半的超时时间(downAfterPeriod)内未收到响应
	if mysql.connection.isConnected() &&
		NowUnixMillisecond()-mysql.connection.getConnectedTime() > SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
		mysql.connection.getActPingTime() != 0 &&
		NowUnixMillisecond()-mysql.connection.getActPingTime() > mysql.downAfterPeriod/2 &&
		NowUnixMillisecond()-mysql.connection.getLastPongTime() > mysql.downAfterPeriod/2 {
		LOG(mysql.flags, mysql.getAddress().String(), DWarn, "sentinel connection is disconnected %s to %s", mysql.sentinel.getAddress().String(), mysql.Address.String())

		mysql.connection.close()
	}

	/*
	 * 添加 SDOWN 标志，如果以下条件满足其一
	 * 1.它没有回复
	 * 2.本 Sentinel 的视角中它是 master，但是它报告自己成为了 slave，并且报告至今的时间超过了 (downAfterPeriod + SENTINEL_SHOW_SLAVE_STATUS_PERIOD * 2)
	 */
	if elapsed > mysql.downAfterPeriod ||
		(mysql.flags.Is(MASTER) &&
			mysql.roleReported == SLAVE &&
			NowUnixMillisecond()-mysql.roleReportedTime > mysql.downAfterPeriod+SENTINEL_SHOW_SLAVE_STATUS_PERIOD*2) {
		// Is subjectively down
		if !mysql.flags.Is(S_DOWN) {
			LOG(mysql.flags, mysql.getAddress().String(), DWarn, "+sdown")

			mysql.sDownSinceTime = NowUnixMillisecond()
			mysql.addFlags(S_DOWN)
		}
	} else {
		// Is subjectively up
		if mysql.flags.Is(S_DOWN) {
			LOG(mysql.flags, mysql.getAddress().String(), DWarn, "-sdown")

			mysql.deleteFlags(S_DOWN)
		}
	}

	return nil
}

func (mysql *MysqlInstance) SendPeriodicCommands() error {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	// 如果该 instance 是处于 SRI_O_DOWN 状态的 master 的 slave，将开始每秒向其发送 ShowSlaveStatus 消息，而不是按照通常的 SENTINEL_SHOW_SLAVE_STATUS_PERIOD 周期
	// 在这种状态下，我们希望密切监视 slave，以防它们被另一个 sentinel 或系统管理员变成 master。
	// 同样，如果 slave 和 master 断开连接，也更频繁地监视 ShowSlaveStatus 的输出，以便我们可以获得最新的断连时间。
	var showSlaveStatusPeriod int64
	if mysql.flags.Is(SLAVE) &&
		((mysql.sentinel.getMaster().flags.Is(O_DOWN) || mysql.sentinel.getMaster().flags.Is(FAILOVER_IN_PROGRESS)) ||
			(mysql.masterLinkDownTime != 0)) {
		showSlaveStatusPeriod = 1000
	} else {
		showSlaveStatusPeriod = SENTINEL_SHOW_SLAVE_STATUS_PERIOD
	}

	// 发送 Ping 消息的周期，默认为 配置文件中配置的 downAfterPeriod
	pingPeriod := mysql.downAfterPeriod
	if pingPeriod > SENTINEL_PING_PERIOD {
		pingPeriod = SENTINEL_PING_PERIOD
	}

	now := NowUnixMillisecond()
	if mysql.showSlaveStatusRefresh == 0 || (now-mysql.showSlaveStatusRefresh) > showSlaveStatusPeriod {
		go mysql.sendShowSlavesStatusCommand()

	}

	if now-mysql.connection.getLastPongTime() > pingPeriod && (now-mysql.connection.getLastPingTime()) > pingPeriod/2 {
		go mysql.sendPingCommand()
	}

	return nil
}

func (mysql *MysqlInstance) sendShowSlavesStatusCommand() {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()

	sql := SHOW_SLAVE_STATUS_SQL
	result, err := mysql.connection.sendSqlWithoutTrx(sql)
	if err != nil {
		LOG(mysql.flags, mysql.getAddress().String(), DError, "Send ShowSlavesStatus failed! %s", err.Error())
		return
	}
	var role Flags
	if len(result) == 0 {
		role = MASTER
	} else if len(result) == 1 {
		role = SLAVE
		showSlaveStatusResp := result[0]
		if showSlaveStatusResp.MasterLogFile.Valid {
			mysql.masterLogFile = showSlaveStatusResp.MasterLogFile.String
		}
		if showSlaveStatusResp.ExecMasterLogPos.Valid {
			mysql.execMasterLogPos = showSlaveStatusResp.ExecMasterLogPos.Int64
		}

		var masterHost string
		var masterPort int32
		if showSlaveStatusResp.MasterHost.Valid {
			masterHost = showSlaveStatusResp.MasterHost.String
		}
		if showSlaveStatusResp.MasterPort.Valid {
			masterPort = showSlaveStatusResp.MasterPort.Int32
		}

		if mysql.flags.Is(SLAVE) {
			if mysql.slaveMasterHost == "" || mysql.slaveMasterHost != masterHost {
				mysql.slaveMasterHost = masterHost
			}
			if mysql.slaveMasterPort != int(masterPort) {
				mysql.slaveMasterPort = int(masterPort)
			}
		}

		var slaveIORunning string
		var slaveSqlRunning string
		if showSlaveStatusResp.SlaveIOState.Valid {
			slaveIORunning = showSlaveStatusResp.SlaveIORunning.String
		}
		if showSlaveStatusResp.SlaveSQLRunning.Valid {
			slaveSqlRunning = showSlaveStatusResp.SlaveSQLRunning.String
		}

		masterLinkDown := SENTINEL_MASTER_LINK_STATUS_DOWN
		if slaveIORunning != "" || slaveSqlRunning != "" {
			if slaveIORunning == "Yes" && slaveSqlRunning == "Yes" {
				masterLinkDown = SENTINEL_MASTER_LINK_STATUS_UP
			}
		}

		if masterLinkDown == SENTINEL_MASTER_LINK_STATUS_DOWN {
			mysql.masterLinkDownTime = (NowUnixMillisecond() - mysql.showSlaveStatusRefresh) * 2 / 3
		} else {
			mysql.masterLinkDownTime = 0
		}
		mysql.slaveMasterLinkStatus = masterLinkDown
	} else {
		LOG(mysql.flags, mysql.getAddress().String(), DError, "ShowSlavesStatus response incorrect!")
		return
	}

	mysql.showSlaveStatusRefresh = NowUnixMillisecond()

	// 如果当前角色发生变化，记录一下
	if role != mysql.roleReported {
		mysql.roleReportedTime = NowUnixMillisecond()
		mysql.roleReported = role
		if role == SLAVE {
			mysql.slaveConfChangeTime = NowUnixMillisecond()
		}

		LOG(mysql.flags, mysql.getAddress().String(), DInfo,
			"+role-change, %s new reported role is %s", mysql.getAddress().String(), mysql.flags.Str())
	}

	// 处理 master -> slave 角色转换
	if mysql.flags.Is(MASTER) && role == SLAVE {
		LOG(mysql.flags, mysql.getAddress().String(), DInfo, "this mysql role is master in this sentinel, but show slave status result this is a slave, nothing to do")
	}

	// 处理 slave -> master 角色转换
	if mysql.flags.Is(SLAVE) && role == MASTER {
		LOG(mysql.flags, mysql.getAddress().String(), DInfo, "this mysql role is slave in this sentinel, but show slave status result this is a master")
		// 如果该 instance 的角色是 promoted slave (晋升的slave，之后会成为 master)，则推动 failover 状态机的状态向前移动
		// 角色变化，instance的调整是在failover阶段，这里只是推进failover状态机的状态
		if mysql.flags.Is(PROMOTED) && mysql.sentinel.getMaster().flags.Is(FAILOVER_IN_PROGRESS) && mysql.sentinel.master.failoverState == FailoverStateWaitPromotion {
			// 现在我们可以确定该 slave 最终会成为 master，将 master 的配置纪元设置为我们赢得此次故障转移选举的纪元。这将迫使其他 Sentinel 更新他们的配置（假设没有可用的新配置）
			mysql.sentinel.getMaster().setConfigEpoch(mysql.sentinel.getMaster().getFailoverEpoch())
			mysql.sentinel.getMaster().setFailoverState(FailoverStateReconfSlaves)
			mysql.sentinel.getMaster().setFailoverStateChangeTime(NowUnixMillisecond())
			LOG(mysql.flags, mysql.getAddress().String(), DWarn, "+promoted-slave")
			LOG(mysql.flags, mysql.getAddress().String(), DWarn, "+failover-state-reconf-slaves, master: %s", mysql.sentinel.getMaster().Address.String())
			mysql.sentinel.sentinelForceHelloUpdateForSentinels()
		} else {
			// 一个 slave 变成了 master，但是其标志位和 failover 状态不对，我们认为这个角色变化是有问题的，等待一段时间之后，我们强制这个slave重新同步master，即让其重新变成 slave
			waitTime := SENTINEL_SEND_HELLO_PERIOD * 4
			LOG(mysql.flags, mysql.getAddress().String(), DError, "this mysql instance status is incorrect")

			if !mysql.flags.Is(PROMOTED) && mysql.sentinel.getMaster().masterLooksSane() &&
				mysql.MysqlInstanceNoDownFor(waitTime) &&
				(NowUnixMillisecond()-mysql.roleReportedTime > waitTime) {
				err := mysql.sendChangeMasterTo(mysql.sentinel.getMaster().getAddress())
				if err != nil {
					LOG(mysql.flags, mysql.getAddress().String(), DError, "change master to %s error: %s", mysql.sentinel.getMaster().Address.String(), err.Error())
				}
				LOG(mysql.flags, mysql.getAddress().String(), DInfo, "+convert-to-slave")
			}
		}
	}

	// 处理 slaves 同步了和之前不同的 master （master 地址不同）
	if mysql.flags.Is(SLAVE) && role == SLAVE && (mysql.slaveMasterPort != mysql.sentinel.getMaster().getAddress().Port || mysql.slaveMasterHost != mysql.sentinel.master.getAddress().IP.String()) {
		waitTime := mysql.sentinel.getMaster().getFailoverTimeout()

		// 在重新配置该 slave 实例时，需要确保 master 是正常的
		if mysql.sentinel.getMaster().masterLooksSane() &&
			mysql.MysqlInstanceNoDownFor(waitTime) &&
			NowUnixMillisecond()-mysql.slaveConfChangeTime > waitTime {
			err := mysql.sendChangeMasterTo(mysql.sentinel.getMaster().getAddress())
			if err != nil {
				LOG(mysql.flags, mysql.getAddress().String(), DError, "change master to %s error: %s", mysql.sentinel.getMaster().getAddress().String(), err.Error())
			}
			LOG(mysql.flags, mysql.getAddress().String(), DInfo, "+fix-slave-config")
		}
	}

	// 检测正在 重新配置(故障处理过程中) 的 slave 是否改变状态
	if mysql.flags.Is(SLAVE) && role == SLAVE && (mysql.flags.Is(ReconfSent) || mysql.flags.Is(ReconfInprog)) {
		// SRI_RECONF_SENT -> SRI_RECONF_INPROG
		if mysql.flags.Is(ReconfSent) && mysql.slaveMasterHost != "" &&
			mysql.sentinel.getMaster().getPromotedSlave().getAddress().IP.String() == mysql.slaveMasterHost &&
			mysql.sentinel.getMaster().getPromotedSlave().getAddress().Port == mysql.slaveMasterPort {
			mysql.deleteFlags(ReconfSent)
			mysql.addFlags(ReconfInprog)
			LOG(mysql.flags, mysql.getAddress().String(), DInfo, "+slave-reconf-inprog")
		}

		// SRI_RECONF_INPROG -> SRI_RECONF_DONE
		if mysql.flags.Is(ReconfInprog) && mysql.slaveMasterLinkStatus == SENTINEL_MASTER_LINK_STATUS_UP {
			mysql.deleteFlags(ReconfInprog)
			mysql.addFlags(ReconfDone)
			LOG(mysql.flags, mysql.getAddress().String(), DInfo, "+slave-reconf-done")
		}
	}
}

func (mysql *MysqlInstance) sendStopSlave() error {
	sqls := make([]Sql, 5)
	sqls = append(sqls, STOP_SLAVE_SQL)
	sqls = append(sqls, RESET_SLAVE_SQL)
	sqls = append(sqls, RESET_SALVE_ALL_SQL)
	sqls = append(sqls, UNSET_READONLY)
	sqls = append(sqls, UNSET_SUPER_READONLY)
	_, err := mysql.connection.sendSql(sqls)
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlInstance) sendChangeMasterTo(address *net.TCPAddr) error {
	changeMasterSql := fmt.Sprintf(string(CHANGE_MASTER_TO),
		address.IP.String(), address.Port, Config.SyncUser, Config.SyncPwd)

	sqls := make([]Sql, 5)
	sqls = append(sqls, SET_READONLY)
	sqls = append(sqls, SET_SUPER_READONLY)
	sqls = append(sqls, STOP_SLAVE_SQL)
	sqls = append(sqls, Sql(changeMasterSql))
	sqls = append(sqls, START_SLAVE_SQL)
	_, err := mysql.connection.sendSql(sqls)
	if err != nil {
		return err
	}
	return nil
}

/**
 * 如果在最近的 “waitTime” 毫秒内，此实例没有被认为 SDOWN 或 ODOWN ，则返回 true
 */
func (mysql *MysqlInstance) MysqlInstanceNoDownFor(waitTime int64) bool {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	var mostRecent int64

	mostRecent = mysql.sDownSinceTime
	if mysql.oDownSinceTime > mostRecent {
		mostRecent = mysql.oDownSinceTime
	}

	return mostRecent == 0 || (NowUnixMillisecond()-mostRecent) > waitTime
}

func (mysql *MysqlInstance) getPromotedSlave() *Instance {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.PromotedSlave
}

func (mysql *MysqlInstance) setConfigEpoch(configEpoch int64) {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	mysql.configEpoch = configEpoch
}

func (mysql *MysqlInstance) getConfigEpoch() int64 {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.configEpoch
}

func (mysql *MysqlInstance) setFailoverEpoch(failoverEpoch int64) {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	mysql.failoverEpoch = failoverEpoch
}

func (mysql *MysqlInstance) getFailoverEpoch() int64 {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.failoverEpoch
}

func (mysql *MysqlInstance) setFailoverState(failoverState FailoverState) {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	mysql.failoverState = failoverState
}

func (mysql *MysqlInstance) getFailoverState() FailoverState {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.failoverState
}

func (mysql *MysqlInstance) setFailoverStateChangeTime(failoverStateChangeTime int64) {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	mysql.failoverStateChangeTime = failoverStateChangeTime
}

func (mysql *MysqlInstance) getFailoverStateChangeTime() int64 {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.failoverStateChangeTime
}

/** 如果 master 看起来是 "sane" 的，则返回 true, 根据以下条件来判断:
 * 1) 在当前配置纪元，它确实是 master.
 * 2) 它报告自己是 master.
 * 3) 它没有处于 SDOWN 或者 ODOWN 状态.
 * 4) 我们获得的最后一个 showSlaveStatus的响应时间不超过 ShowSlaveStatus 周期的两倍.
 * */
func (mysql *MysqlInstance) masterLooksSane() bool {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return mysql.flags.Is(MASTER) &&
		mysql.roleReported == MASTER &&
		(!mysql.flags.Is(S_DOWN) && !mysql.flags.Is(O_DOWN)) &&
		(NowUnixMillisecond()-mysql.showSlaveStatusRefresh) < SENTINEL_SHOW_SLAVE_STATUS_PERIOD*2
}

func (mysql *MysqlInstance) HandleInstance() {
	err := mysql.ReconnectInstance()
	if err != nil {
		LOG(mysql.flags, mysql.getAddress().String(), DError, "ReconnectInstance failed! %s", err.Error())
	}
	err = mysql.SendPeriodicCommands()
	if err != nil {
		LOG(mysql.flags, mysql.getAddress().String(), DError, "SendPeriodicCommands failed! %s", err.Error())
	}
	err = mysql.CheckSubjectivelyDown()
	if err != nil {
		LOG(mysql.flags, mysql.getAddress().String(), DError, "CheckSubjectivelyDown failed! %s", err.Error())
	}

	if mysql.flags.Is(MASTER) {
		err := mysql.CheckObjectivelyDown()
		if err != nil {
			LOG(mysql.flags, mysql.getAddress().String(), DError, "CheckObjectivelyDown failed! %s", err.Error())
		}
		if mysql.StartFailoverIfNeeded() {
			mysql.AskMasterStateToOtherSentinels(true)
		}
		mysql.FailoverStateMachine()
		mysql.AskMasterStateToOtherSentinels(false)
	}
}

func (mysql *MysqlInstance) CheckObjectivelyDown() error {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	if !mysql.flags.Is(MASTER) {
		return errors.New("CheckObjectivelyDown must be master")
	}

	var quorum int
	var oDown bool

	if mysql.flags.Is(S_DOWN) {
		// 是否有足够的 Sentinel 同意该 instance 挂了？
		quorum = 1 // 本 Sentinel 的投票
		// 统计其他 Sentinel 的投票
		sentinels := mysql.sentinel.getSentinels()
		for _, sentinel := range sentinels {
			if sentinel.GetFlags().Is(MASTER_DOWN) {
				quorum++
			}
		}

		if quorum > mysql.quorum {
			oDown = true
		}
	}

	// 根据结果设置相应的标志
	if oDown {
		if !mysql.flags.Is(O_DOWN) {
			LOG(mysql.flags, mysql.getAddress().String(), DWarn, "+odown, quorum: %d/%d", quorum, mysql.quorum)

			mysql.addFlags(O_DOWN)
			mysql.oDownSinceTime = NowUnixMillisecond()
		}
	} else {
		LOG(mysql.flags, mysql.getAddress().String(), DWarn, "-odown")

		mysql.deleteFlags(O_DOWN)
	}
	return nil
}

func (mysql *MysqlInstance) StartFailoverIfNeeded() bool {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return false
}

func (mysql *MysqlInstance) AskMasterStateToOtherSentinels(askForce bool) error {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return nil
}

func (mysql *MysqlInstance) FailoverStateMachine() error {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
	return nil
}

func (mysql *MysqlInstance) FailoverSwitchToPromotedSlave() {
	mysql.mu.Lock()
	defer mysql.mu.Unlock()
}

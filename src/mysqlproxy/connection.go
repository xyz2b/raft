// Package connection 表示本 Sentinel 和 MySQL 实例 以及其他 Sentinel 实例的连接
package sentinel

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"mysql-proxy/labrpc"
	"net"
	"sync"
)

type SlaveResult struct {
	SlaveIOState              sql.NullString `db:"Slave_IO_State"`
	MasterHost                sql.NullString `db:"Master_Host"`
	MasterUser                sql.NullString `db:"Master_User"`
	MasterPort                sql.NullInt32  `db:"Master_Port"`
	ConnectRetry              sql.NullInt32  `db:"Connect_Retry"`
	MasterLogFile             sql.NullString `db:"Master_Log_File"`
	ReadMasterLogPos          sql.NullInt64  `db:"Read_Master_Log_Pos"`
	RelayLogFile              sql.NullString `db:"Relay_Log_File"`
	RelayLogPos               sql.NullInt64  `db:"Relay_Log_Pos"`
	RelayMasterLogFile        sql.NullString `db:"Relay_Master_Log_File"`
	SlaveIORunning            sql.NullString `db:"Slave_IO_Running"`
	SlaveSQLRunning           sql.NullString `db:"Slave_SQL_Running"`
	ReplicateDoDB             sql.NullString `db:"Replicate_Do_DB"`
	ReplicateIgnoreDB         sql.NullString `db:"Replicate_Ignore_DB"`
	ReplicateDoTable          sql.NullString `db:"Replicate_Do_Table"`
	ReplicateIgnoreTable      sql.NullString `db:"Replicate_Ignore_Table"`
	ReplicateWildDoTable      sql.NullString `db:"Replicate_Wild_Do_Table"`
	ReplicateWildIgnoreTable  sql.NullString `db:"Replicate_Wild_Ignore_Table"`
	LastErrno                 sql.NullInt32  `db:"Last_Errno"`
	LastError                 sql.NullString `db:"Last_Error"`
	SkipCounter               sql.NullInt32  `db:"Skip_Counter"`
	ExecMasterLogPos          sql.NullInt64  `db:"Exec_Master_Log_Pos"`
	RelayLogSpace             sql.NullInt64  `db:"Relay_Log_Space"`
	UntilCondition            sql.NullString `db:"Until_Condition"`
	UntilLogFile              sql.NullString `db:"Until_Log_File"`
	UntilLogPos               sql.NullInt64  `db:"Until_Log_Pos"`
	MasterSSLAllowed          sql.NullString `db:"Master_SSL_Allowed"`
	MasterSSLCAFile           sql.NullString `db:"Master_SSL_CA_File"`
	MasterSSLCAPath           sql.NullString `db:"Master_SSL_CA_Path"`
	MasterSSLCert             sql.NullString `db:"Master_SSL_Cert"`
	MasterSSLCipher           sql.NullString `db:"Master_SSL_Cipher"`
	MasterSSLKey              sql.NullString `db:"Master_SSL_Key"`
	SecondsBehindMaster       sql.NullInt64  `db:"Seconds_Behind_Master"`
	MasterSSLVerifyServerCert sql.NullString `db:"Master_SSL_Verify_Server_Cert"`
	LastIOErrno               sql.NullInt32  `db:"Last_IO_Errno"`
	LastIOError               sql.NullString `db:"Last_IO_Error"`
	LastSQLErrno              sql.NullInt32  `db:"Last_SQL_Errno"`
	LastSQLError              sql.NullString `db:"Last_SQL_Error"`
	ReplicateIgnoreServerIds  sql.NullString `db:"Replicate_Ignore_Server_Ids"`
	MasterServerId            sql.NullInt32  `db:"Master_Server_Id"`
	MasterUUID                sql.NullString `db:"Master_UUID"`
	MasterInfoFile            sql.NullString `db:"Master_Info_File"`
	SQLDelay                  sql.NullInt32  `db:"SQL_Delay"`
	SQLRemainingDelay         sql.NullInt32  `db:"SQL_Remaining_Delay"`
	SlaveSQLRunningState      sql.NullString `db:"Slave_SQL_Running_State"`
	MasterRetryCount          sql.NullInt64  `db:"Master_Retry_Count"`
	MasterBind                sql.NullString `db:"Master_Bind"`
	LastIOErrorTimestamp      sql.NullString `db:"Last_IO_Error_Timestamp"`
	LastSQLErrorTimestamp     sql.NullString `db:"Last_SQL_Error_Timestamp"`
	MasterSSLCrl              sql.NullString `db:"Master_SSL_Crl"`
	MasterSSLCrlpath          sql.NullString `db:"Master_SSL_Crlpath"`
	RetrievedGtidSet          sql.NullString `db:"Retrieved_Gtid_Set"`
	ExecutedGtidSet           sql.NullString `db:"Executed_Gtid_Set"`
	AutoPosition              sql.NullInt32  `db:"Auto_Position"`
	ReplicateRewriteDB        sql.NullString `db:"Replicate_Rewrite_DB"`
	ChannelName               sql.NullString `db:"Channel_Name"`
	MasterTLSVersion          sql.NullString `db:"Master_TLS_Version"`
	MasterPublicKeyPath       sql.NullString `db:"Master_public_key_path"`
	GetMasterPublicKey        sql.NullInt32  `db:"Get_master_public_key"`
	NetworkNamespace          sql.NullString `db:"Network_Namespace"`
}

type Connection interface {
	isConnected() bool
	sendPing() error

	close() error
	connect() error

	sendHello(helloArgs *HelloArgs) (*HelloReply, error)
	sendSql(sqls []Sql) ([]*sql.Result, error)
	sendSqlWithoutTrx(sql Sql) ([]SlaveResult, error)

	getLastReconnTime() int64
	setLastReconnTime(lastReconnTime int64)

	getLastConnTime() int64
	setLastConnTime(lastConnTime int64)

	getLastPingTime() int64
	setLastPingTime(lastPingTime int64)

	getActPingTime() int64
	setActPingTime(actPingTime int64)

	getLastPongTime() int64
	setLastPongTime(lastPongTime int64)

	getLastAvailTime() int64
	setLastAvailTime(lastAvailTime int64)

	getConnectedTime() int64
	setConnectedTime(connectedTime int64)

	sendIsMasterDownByAddr(isMasterDownByAddrArgs *IsMasterDownByAddrArgs) (*IsMasterDownByAddrReply, error)
}

type ConnectionConnection struct {
	mu sync.Mutex

	connected bool
	address   *net.TCPAddr
	// lastReconnTime记录了上次重连时间，两次间隔需要超过1秒
	lastReconnTime int64
	// 最后一次建连时间
	lastConnTime int64
	// 记录最后一次发送 PING 命令的时间
	lastPingTime int64
	// 记录了最后一条未收到 PONG 响应的 PING 命令的发送时间戳。当收到 pong 时，该字段设置为 0；如果该值为 0 并且发送新的 ping，则再次设置为当前时间
	actPingTime int64
	// 每次接收到 ping 的响应 pong 时，刷新该时间
	lastPongTime  int64
	lastAvailTime int64

	connectedTime int64
}

func (c *ConnectionConnection) setConnectedTime(connectedTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectedTime = connectedTime
}

func (c *ConnectionConnection) getConnectedTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectedTime
}

func (c *ConnectionConnection) setLastReconnTime(lastReconnTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastReconnTime = lastReconnTime
}

func (c *ConnectionConnection) getLastReconnTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastReconnTime
}

func (c *ConnectionConnection) setLastConnTime(lastConnTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastConnTime = lastConnTime
}

func (c *ConnectionConnection) getLastConnTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastConnTime
}

func (c *ConnectionConnection) getLastPingTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPingTime
}

func (c *ConnectionConnection) setLastPingTime(lastPingTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPingTime = lastPingTime
}

func (c *ConnectionConnection) getActPingTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.actPingTime
}

func (c *ConnectionConnection) setActPingTime(actPingTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actPingTime = actPingTime
}

func (c *ConnectionConnection) getLastPongTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPongTime
}

func (c *ConnectionConnection) setLastPongTime(lastPongTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPongTime = lastPongTime
}

func (c *ConnectionConnection) getLastAvailTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAvailTime
}

func (c *ConnectionConnection) setLastAvailTime(lastAvailTime int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastAvailTime = lastAvailTime
}

// MysqlConnection 表示本 Sentinel 和其所监控的 MySQL 实例的连接
type MysqlConnection struct {
	ConnectionConnection
	db       *sqlx.DB
	username string
	password string
	mysql    *MysqlInstance
}

func MakeMysqlConnection(address *net.TCPAddr, mysql *MysqlInstance) *MysqlConnection {
	return &MysqlConnection{
		ConnectionConnection: ConnectionConnection{
			address:        address,
			connected:      false,
			lastAvailTime:  NowUnixMillisecond(),
			lastPongTime:   NowUnixMillisecond(),
			actPingTime:    NowUnixMillisecond(),
			lastPingTime:   0,
			lastReconnTime: 0,
			connectedTime:  0,
		},
		username: Config.SyncUser,
		password: Config.SyncPwd,
		mysql:    mysql,
	}
}

func (mysqlConnection *MysqlConnection) sendIsMasterDownByAddr(isMasterDownByAddrArgs *IsMasterDownByAddrArgs) (*IsMasterDownByAddrReply, error) {
	return nil, errors.New("mysqlConnection not support sendIsMasterDownByAddr")

}

func (mysqlConnection *MysqlConnection) sendHello(helloArgs *HelloArgs) (*HelloReply, error) {
	return nil, errors.New("mysqlConnection not support sendHello")
}

func (mysqlConnection *MysqlConnection) close() error {
	mysqlConnection.mu.Lock()
	defer mysqlConnection.mu.Unlock()
	mysqlConnection.connected = false
	return mysqlConnection.db.Close()
}

func (mysqlConnection *MysqlConnection) isConnected() bool {
	mysqlConnection.mu.Lock()
	defer mysqlConnection.mu.Unlock()
	return mysqlConnection.connected
}

func (mysqlConnection *MysqlConnection) connect() error {
	mysqlConnection.mu.Lock()
	defer mysqlConnection.mu.Unlock()
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@tcp(%s)/", mysqlConnection.username, mysqlConnection.password, mysqlConnection.address.String()))
	if err != nil {
		return err
	}
	mysqlConnection.db = db
	mysqlConnection.connected = true
	return nil
}

func (mysqlConnection *MysqlConnection) sendPing() error {
	err := mysqlConnection.db.Ping()
	return err
}

func (mysqlConnection *MysqlConnection) sendSql(sqls []Sql) ([]*sql.Result, error) {
	return mysqlConnection.execSql(sqls)
}

func (mysqlConnection *MysqlConnection) sendSqlWithoutTrx(sql Sql) ([]SlaveResult, error) {
	return mysqlConnection.execSqlWithoutTrx(sql)
}

func (mysqlConnection *MysqlConnection) execSqlWithoutTrx(sql Sql) ([]SlaveResult, error) {
	slaveResult := []SlaveResult{}
	err := mysqlConnection.db.Select(&slaveResult, sql.toString())
	if err != nil {
		return nil, err
	}

	return slaveResult, nil
}

func (mysqlConnection *MysqlConnection) execSql(sqls []Sql) ([]*sql.Result, error) {
	// 开启事务
	tx, err := mysqlConnection.db.Begin()
	if err != nil {
		return nil, err
	}
	// 在事务中执行多条 SQL 语句
	results := make([]*sql.Result, len(sqls))
	for _, sqlStatement := range sqls {
		result, err := tx.Exec(sqlStatement.toString())
		if err != nil {
			// 如果操作出错，回滚事务
			err := tx.Rollback()
			if err != nil {
				return nil, err
			}
			return nil, err
		}
		results = append(results, &result)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return results, nil
}

// SentinelConnection 表示本 Sentinel 和其他 Sentinel 实例的连接
type SentinelConnection struct {
	ConnectionConnection

	client *labrpc.ClientEnd
	net    *labrpc.Network

	lastSendHelloTime int64

	sentinel *SentinelInstance
}

func MakeSentinelConnection(address *net.TCPAddr, sentinel *SentinelInstance) *SentinelConnection {
	sentinelConnection := &SentinelConnection{
		ConnectionConnection: ConnectionConnection{
			address:        address,
			connected:      false,
			lastAvailTime:  NowUnixMillisecond(),
			lastPongTime:   NowUnixMillisecond(),
			actPingTime:    NowUnixMillisecond(),
			lastPingTime:   0,
			lastReconnTime: 0,
			connectedTime:  0,
		},
		sentinel: sentinel,
	}
	sentinelConnection.net = labrpc.MakeNetwork()
	return sentinelConnection
}

func (sentinelConnection *SentinelConnection) sendSql(sqls []Sql) ([]*sql.Result, error) {
	return nil, errors.New("sentinelConnection not support sendSql")
}

func (sentinelConnection *SentinelConnection) sendSqlWithoutTrx(sql Sql) ([]SlaveResult, error) {
	return nil, errors.New("sentinelConnection not support sendSqlWithoutTrx")
}

func (sentinelConnection *SentinelConnection) isConnected() bool {
	sentinelConnection.mu.Lock()
	defer sentinelConnection.mu.Unlock()
	return sentinelConnection.connected
}

func (sentinelConnection *SentinelConnection) connect() error {
	sentinelConnection.mu.Lock()
	defer sentinelConnection.mu.Unlock()
	endName := sentinelConnection.address.String()
	sentinelConnection.client = sentinelConnection.net.MakeEnd(endName)
	sentinelConnection.net.Connect(endName, endName)
	sentinelConnection.connected = true
	return nil
}

type PingArgs struct {
}

type PingReply struct {
}

func (sentinelConnection *SentinelConnection) pingRPC(args *PingArgs, reply *PingReply) bool {
	ok := sentinelConnection.client.Call("SentinelConnection.Ping", args, reply)
	return ok
}

func (sentinelConnection *SentinelConnection) Ping(args *PingArgs, reply *PingReply) {
	return
}

func (sentinelConnection *SentinelConnection) sendPing() error {
	pingArgs := &PingArgs{}
	pingReply := &PingReply{}
	ok := sentinelConnection.pingRPC(pingArgs, pingReply)
	if !ok {
		return errors.New(fmt.Sprintf("send ping %s error", sentinelConnection.address.String()))
	}
	return nil
}

func (sentinelConnection *SentinelConnection) close() error {
	sentinelConnection.mu.Lock()
	defer sentinelConnection.mu.Unlock()
	sentinelConnection.connected = false
	return nil
}

type HelloArgs struct {
	// 本 sentinel 的 IP
	Ip string
	// 本 Sentinel 的端口
	Port int
	// 本 Sentinel 的 runID
	RunId string
	// 本 Sentinel 的当前任期号
	CurrentEpoch int64
	// master 的 IP
	MasterIp string
	// master 的端口
	MasterPort int
	// master 的配置纪元
	MasterConfigEpoch int64
}

type HelloReply struct {
}

func (sentinelConnection *SentinelConnection) helloRPC(args *HelloArgs, reply *HelloReply) bool {
	ok := sentinelConnection.client.Call("Sentinel.Hello", args, reply)
	return ok
}

func (sentinelConnection *SentinelConnection) sendHello(helloArgs *HelloArgs) (*HelloReply, error) {
	sentinelConnection.mu.Lock()
	defer sentinelConnection.mu.Unlock()
	helloReply := &HelloReply{}
	ok := sentinelConnection.helloRPC(helloArgs, helloReply)
	if !ok {
		return nil, errors.New(fmt.Sprintf("send hello %s error", sentinelConnection.address.String()))
	}
	sentinelConnection.lastSendHelloTime = NowUnixMillisecond()
	return helloReply, nil
}

type IsMasterDownByAddrArgs struct {
	// 被Sentinel认为主观下线的主服务器IP
	Ip string
	// 被Sentinel认为主观下线的主服务器PORT
	Port int
	// *符号代表仅仅用于检测主服务器的客观下线状态，而Sentinel RunID则用于选举leader
	RunId string
	// Sentinel的当前配置纪元
	CurrentEpoch int64
}

type IsMasterDownByAddrReply struct {
	DownState   bool
	LeaderRunId string
	LeaderEpoch int64
	MasterIP    string
	MasterPort  int
}

func (sentinelConnection *SentinelConnection) isMasterDownByAddrRPC(args *IsMasterDownByAddrArgs, reply *IsMasterDownByAddrReply) bool {
	ok := sentinelConnection.client.Call("SentinelInstance.IsMasterDownByAddr", args, reply)
	return ok
}

func (sentinelConnection *SentinelConnection) sendIsMasterDownByAddr(isMasterDownByAddrArgs *IsMasterDownByAddrArgs) (*IsMasterDownByAddrReply, error) {
	sentinelConnection.mu.Lock()
	defer sentinelConnection.mu.Unlock()
	isMasterDownByAddrReply := &IsMasterDownByAddrReply{}
	ok := sentinelConnection.isMasterDownByAddrRPC(isMasterDownByAddrArgs, isMasterDownByAddrReply)
	if !ok {
		return nil, errors.New(fmt.Sprintf("send isMasterDownByAddr %s error", sentinelConnection.address.String()))
	}
	sentinelConnection.lastSendHelloTime = NowUnixMillisecond()
	return isMasterDownByAddrReply, nil
}

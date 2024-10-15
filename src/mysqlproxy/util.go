package sentinel

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DError logTopic = "ERRO" // level = 3
	DWarn  logTopic = "WARN" // level = 2
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0

	// level = 1
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1" // sending log
	DLog2    logTopic = "LOG2" // receiving log
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DApply   logTopic = "APLY"
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DError:
		return 3
	case DWarn:
		return 2
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}

func getEnvLevel() int {
	v := os.Getenv("VERBOSE")
	level := getTopicLevel(DError) + 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var logStart time.Time
var logLevel int

func init() {
	logLevel = getEnvLevel()
	logStart = time.Now()

	// do not print verbose date
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LOG(flag Flags, ip string, topic logTopic, format string, a ...interface{}) {
	topicLevel := getTopicLevel(topic)
	if logLevel <= topicLevel {
		time := time.Since(logStart).Microseconds()
		time /= 100
		var role string
		if flag.Is(MASTER) {
			role = "Master"
		} else if flag.Is(SLAVE) {
			role = "Slave"
		} else if flag.Is(SENTINEL) {
			role = "Sentinel"
		} else if flag.Is(PROMOTED) {
			role = "Promoted"
		} else {
			role = "Unknown"
		}
		prefix := fmt.Sprintf("%06d %v %s(%s) ", time, string(topic), role, ip)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func NowUnixMillisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func AddrOrHostnameEqual(a1 *net.TCPAddr, a2 *net.TCPAddr) bool {
	return a1.Port == a2.Port && a1.IP.String() == a2.IP.String()
}

func IpPortToAddrString(ip string, port int) string {
	return ip + ":" + strconv.Itoa(port)
}

func IpPortToAddr(ip string, port int) (*net.TCPAddr, error) {
	return StringToAddr(IpPortToAddrString(ip, port))

}

func AddrToString(addr *net.TCPAddr) string {
	return addr.IP.String() + ":" + strconv.Itoa(addr.Port)
}

func StringToAddr(addr string) (*net.TCPAddr, error) {
	return net.ResolveTCPAddr("tcp", addr)
}

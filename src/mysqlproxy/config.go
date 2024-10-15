package sentinel

import (
	"github.com/tkanos/gonfig"
)

var (
	Config        SentinelConfig
	defaultConfig = SentinelConfig{
		SyncUser:        "sync",
		SyncPwd:         "12345678",
		Quorum:          2,
		FailoverTimeout: SENTINEL_DEFAULT_FAILOVER_TIMEOUT,
	}
)

type SentinelConfig struct {
	SyncUser        string `json:"sync_user"` // aomp请求地址，http://uat.aomp.weoa.com
	SyncPwd         string `json:"sync_pwd"`  // aomp appid
	Quorum          int    `json:"quorum"`
	FailoverTimeout int64  `json:"failover_timeout"`
}

func initConfigFromFile(configFile string) error {
	Config = defaultConfig
	err := gonfig.GetConf(configFile, &Config)
	if err != nil {
		return err
	}
	return nil
}

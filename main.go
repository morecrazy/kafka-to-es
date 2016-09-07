package main

import (
	"backend/common"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"
)

const (
	DEFAULT_CONF_FILE = "./kafka-logspout-es.conf"
)

var (
	wg                 sync.WaitGroup
	g_conf_file        string
	gRedisPath         string
	gRedisKey          string
	gBrokers           string
	gTopic             string
	gChannelBufferSize int64
	gBufferWriterNum   int64
	gLogSize           int64
	gLogUnit           string
	gLogBufferSize     int64

	//Elasticsearch
	gSearchGroupAddress string
	gSearchHost         string
	gIndex              string
	gDocType            string
)

func init() {
	const usage = "kafka-logspout-es [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure) {
	gRedisPath = config.External["redisPath"]
	gRedisKey = config.External["redisKey"]
	gLogUnit = config.External["logUnit"]
	gBrokers = config.External["brokers"]
	gTopic = config.External["topic"]

	//ES
	gSearchHost = config.External["esHosts"]
	gIndex = config.External["index"]
	gDocType = config.External["docType"]
	gSearchGroupAddress = "http://" + gSearchHost

	gLogSize = config.ExternalInt64["logSize"]
	gChannelBufferSize = config.ExternalInt64["channelBufferSize"]
	gBufferWriterNum = config.ExternalInt64["bufferWriterNum"]
	gLogBufferSize = config.ExternalInt64["logBufferSize"]
}

func startTimer(f func()) {
    go func() {
        for {
            f()
            now := time.Now()
            // 计算下一个零点
            next := now.Add(time.Hour * 24)
            next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
            t := time.NewTimer(next.Sub(now))
            <-t.C
        }
    }()
}

func main() {
	//set runtime variable
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get flag
	flag.Parse()

	if g_conf_file != "" {
		common.Config = new(common.Configure)
		if err := common.InitConfigFile(g_conf_file, common.Config); err != nil {
			fmt.Println("init config err : ", err)
		}
	} else {
		addrs := []string{"http://etcd.in.codoon.com:2379"}
		common.Config = new(common.Configure)
		if err := common.LoadCfgFromEtcd(addrs, "log-sink", common.Config); err != nil {
			fmt.Println("init config from etcd err : ", err)
		}
	}

	var err error
	broker := new(KafkaBroker) //注入kafka broker

	common.Logger, err = common.InitLogger("kafka-logspout-es")
	if err != nil {
		fmt.Println("init log error")
		return
	}
	InitExternalConfig(common.Config)

	fmt.Println("Sink log service is started...")
	brokerList, _ := broker.GetBrokerList()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = broker.ConsumeMsg(brokerList, gTopic)
	}()
	wg.Wait()

	if err != nil {
		fmt.Println("log sin error: ", err.Error())
	}
	fmt.Println("Sink log service is over...")
}

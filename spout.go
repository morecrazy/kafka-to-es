package main

import (
	"strings"
	"backend/common"
	"os"
	"os/signal"
	"time"
	"encoding/json"
)

func WriteToLogBuffer(logBuffer *LogBuffer, bts []byte) error {
	d := string(bts)
	data := strings.Split(d, "|")

	source := map[string]string{
		"request_id": data[0],
		"service_code": data[1],
		"user_id": data[2],
		"service_name": data[3],
		"start_time": data[4],
		"spend_time": data[5],
		"method_type": data[6],
		"host": data[7],
		"api": strings.Split(data[8], "?")[0],
		"status": data[9],
	}

	create := map[string]interface{}{
		"create" : map[string]string{
		},
	}

	docBts, err := json.Marshal(source)
	if err != nil {
		common.Logger.Error("Marshal Json failed %v", data)
		return err
	}
	creBts, _ := json.Marshal(create)

	line := []byte("\n")

	//拼接byte array
	dat := append(creBts[:], line[:]...)
	dat = append(dat[:], docBts[:]...)
	dat = append(dat[:], line[:]...)

	document := string(dat)
	common.Logger.Debug("Write document %s to Logbuffer", document)
	if _, err := logBuffer.WriteString(document); err != nil {
		return err
	}
	return nil
}

func WriteToEs(bts []byte) error {
	d := string(bts)
	data := strings.Split(d, "|")

	document := map[string]interface{}{
		"request_id": data[0],
		"service_code": data[1],
		"user_id": data[2],
		"service_name": data[3],
		"start_time": data[4],
		"spend_time": data[5],
		"method_type": data[6],
		"host": data[7],
		"api": data[8],
		"status": data[9],
	}

	index := gIndex + "-" + time.Now().Format("2006.01.02")
	if err := CreateDoc(index, gDocType, document); err != nil {
		return err
	}
	return nil
}

func logSpouter(channel chan []byte) {
	var (
		closing = make(chan struct{})
		logBuffer *LogBuffer = NewLogBuffer()
	)
	go LogBufferReader(logBuffer)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		common.Logger.Info("Initiating shutdown of consumer...")
		close(closing)
	}()

	consumed := 0
	ConsumerLoop:
	for {
		select {
		case msg := <- channel:
			if err := WriteToLogBuffer(logBuffer, msg); err != nil {
				common.Logger.Error(err.Error())
			}
			consumed++
		case <-closing:
			break ConsumerLoop
		}
	}
}

func LogBufferReader(logBuffer *LogBuffer) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <- logBuffer.ch:
			//从buf读取数据,写入到es中
			if err := logBuffer.BulkWriteToEs(); err != nil {
				common.Logger.Error(err.Error())
			}
		case <-timer.C:
			//超时时间到,强制读取数据
			//从buf读取数据,写入es中
			if err := logBuffer.BulkWriteToEs(); err != nil {
				common.Logger.Error(err.Error())
			}
		}
	}
}
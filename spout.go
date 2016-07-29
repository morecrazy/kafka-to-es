package main

import (
	"strings"
	"backend/common"
	"os"
	"os/signal"
	"time"
)

func writeToEs(bts []byte) error {
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
	)

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
			if err := writeToEs(msg); err != nil {
				common.Logger.Error(err.Error())
			}
			consumed++
		case <-closing:
			break ConsumerLoop
		}
	}
}
package main

import (
	"time"
	"strconv"
)

//清除昨天之前索引的备份
func CronClearIndexReplicas() error {
	document := map[string]interface{}{
		"index": map[string]int{
			"number_of_replicas": 0,
		},
	}
	now := time.Now()
	yesterday := now.Add(-24 * time.Hour)
	index := gIndex + "-" + yesterday.Format("2006.01.02")
	if err := ClearIndexReplicas(index, document); err != nil {
		return err
	}
	return nil
}


//删除4天之前的索引
func CronDeleteIndex() error {
	now := time.Now()
	yesterday := now.Add(-24 * 4 * time.Hour)
	index := gIndex + "-" + yesterday.Format("2006.01.02")
	if err := DeleteIndex(index); err != nil {
		return err
	}
	return nil
}

//修改索引文档properties
func CronModifyProperties() error {
	startHour := 0
	endHour := 23
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour)
	index := gIndex + "-" + tomorrow.Format("2006.01.02")
	for ; startHour <= endHour; startHour ++ {
		docType := strconv.Itoa(startHour)
		document := map[string]interface{}{
			docType: map[string]interface{}{
				"properties": map[string]interface{}{
					"api": map[string]string{
						"type": "string",
						"index": "not_analyzed",
					},
					"user_id": map[string]string{
						"type": "string",
						"index": "not_analyzed",
					},
					"request_id": map[string]string{
						"type": "string",
						"index": "no",
					},
					"host": map[string]string{
						"type": "string",
						"index": "no",
					},
					"status": map[string]string{
						"type": "string",
						"index": "no",
					},
					"start_time": map[string]string{
						"type": "string",
						"index": "no",
					},
					"spend_time": map[string]string{
						"type": "string",
						"index": "no",
					},
					"service_name": map[string]string{
						"type": "string",
						"index": "no",
					},
					"service_code": map[string]string{
						"type": "string",
						"index": "no",
					},
					"method_type": map[string]string{
						"type": "string",
						"index": "no",
					},
				},
			},
		}
		if err := ModifyProperties(index, docType, document); err != nil {
			return err
		}
	}
	return nil
}
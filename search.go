package main

import (
	"time"
	"fmt"
	"backend/common"
	"net/http"
)

type SearchResultShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type SearchResultItem struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Id     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

type SearchResultHits struct {
	Total    int                `json:"total"`
	MaxSorce float64            `json:max_score"`
	Hits     []SearchResultItem `json:"hits"`
}

type SearchResult struct {
	Took    int                `json:"took"`
	Timeout bool               `json:"timeout"`
	Shards  SearchResultShards `json:"_shards"`
	Hits    SearchResultHits   `json:"hits"`
}

func CreateDoc(index, doc_type string, document interface{}) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("create doc[index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, doc_type, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s/%s/", gSearchGroupAddress, index, doc_type)

	statusCode, response, _ := common.SendJsonRequest("POST", url, document)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusCreated {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("create doc failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}

func BulkCreateDoc(index, doc_type string, document interface{}) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("create doc[index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, doc_type, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s/%s/_bulk", gSearchGroupAddress, index, doc_type)

	statusCode, response, _ := common.SendRawRequest("POST", url, document)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusOK {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("bulk create doc failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}

func ClearIndexReplicas(index string, document interface{}) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("clear index replicas doc[index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s/_settings", gSearchGroupAddress, index)

	statusCode, response, _ := common.SendJsonRequest("PUT", url, document)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusOK {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("clear index replicas failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}

func ModifyProperties(index, doc_type string, document interface{}) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("ModifyProperties doc[index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, doc_type, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s/%s/_mapping?pretty", gSearchGroupAddress, index, doc_type)

	statusCode, response, _ := common.SendJsonRequest("PUT", url, document)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusOK {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("ModifyProperties failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}
func DeleteIndex(index string) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("Delete Index[index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s", gSearchGroupAddress, index)

	statusCode, response, _ := common.SendRawRequest("DELETE", url, nil)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusOK {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("Delete Index failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}

func OptimizeIndex(index string) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("OptimizeIndex [index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s/_optimize", gSearchGroupAddress, index)

	statusCode, response, _ := common.SendRawRequest("POST", url, nil)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusOK {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("Optimize Index failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}

func ClearIndexCache(index string) error {
	start_time := time.Now()
	defer func() {
		common.Logger.Debug("Clear index cache[index:%s][doc_type:%s][doc_id:%#v][cost:%dus]",
			index, time.Now().Sub(start_time).Nanoseconds()/1000)
	}()
	url := fmt.Sprintf("%s/%s/_cache/clear", gSearchGroupAddress, index)

	statusCode, response, _ := common.SendRawRequest("POST", url, nil)
	common.Logger.Debug("statusCode: %d", statusCode)

	if statusCode == http.StatusOK {
		return nil
	}
	if statusCode != http.StatusOK || statusCode != http.StatusCreated || statusCode != http.StatusConflict {
		common.Logger.Warning("Clear index cache failed![%s]", response)
	}
	return &SearchError{Code: statusCode, Message: response}
}
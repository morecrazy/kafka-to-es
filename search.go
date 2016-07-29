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

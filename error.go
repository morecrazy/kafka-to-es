package main

import (
	"encoding/json"
)

type SearchError struct {
	Code int
	Message string
}

func (err *SearchError) Error() string {
	error_str, _ := json.Marshal(err)
	return string(error_str)
}

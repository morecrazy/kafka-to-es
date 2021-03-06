package main

import (
	"bytes"
	"sync"
	"strconv"
	"backend/common"
	"time"
)
type LogBuffer struct {
	m *sync.Mutex
	buf *bytes.Buffer
	len int64
	ch chan bool
	name string
}

func NewLogBuffer() *LogBuffer {
	return &LogBuffer{m: new(sync.Mutex), buf: new(bytes.Buffer), len: 0, ch: make(chan bool, 1), name: ""}
}

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	b.len ++
	b.m.Unlock()
	if b.len == gLogBufferSize {
		b.ch <- true
	}
	return b.buf.WriteString(s)
}

func (b *LogBuffer) BulkWriteToEs() error {
	document := b.buf.String()
	b.m.Lock()
	b.buf.Reset()
	b.len = 0
	b.m.Unlock()
	common.Logger.Debug("starting read string from logbuffer, the document is %s", document)
	index := gIndex + "-" + time.Now().Format("2006.01.02")
	now := time.Now()
	hour,_,_ := now.Clock()
	if document != "" {
		if err := BulkCreateDoc(index, strconv.Itoa(hour), document); err != nil {
			return err
		}
	}
	return nil
}

func (b *LogBuffer) ReadString() string {
	b.m.Lock()
	defer b.m.Unlock()
	str := b.buf.String()
	common.Logger.Debug("start read string from logbuffer, the log buffer name is %s, and the length is %d", b.name, b.len)
	b.buf.Reset()
	b.len = 0
	return str
}
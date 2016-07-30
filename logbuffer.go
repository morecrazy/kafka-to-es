package main

import (
	"bytes"
	"sync"
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

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	b.len ++
	if b.len == gLogBufferSize {
		b.ch <- true
	}
	return b.buf.WriteString(s)
}

func (b *LogBuffer) BulkWriteToEs() error {
	b.m.Lock()
	defer b.m.Unlock()
	document := b.buf.String() + "\n"
	index := gIndex + "-" + time.Now().Format("2006.01.02")
	if err := BulkCreateDoc(index, gDocType, document); err != nil {
		return err
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
package data

import (
	"context"
	"encoding/gob"
	"github.com/go-kratos/kratos/v2/log"
	"os"
	"sync"
	"time"
)

// AsyncAOFWriter 结构体用于异步写入 AOF 文件
type AsyncAOFWriter struct {
	queue chan []interface{}
	file  *os.File
	wg    sync.WaitGroup
	log   *log.Helper
}

func (aw *AsyncAOFWriter) init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register("")
	gob.Register(time.Duration(0))
}

// NewAsyncAOFWriter 创建一个新的异步 AOF 写入器
func NewAsyncAOFWriter(file *os.File, log *log.Helper) *AsyncAOFWriter {
	aw := &AsyncAOFWriter{
		queue: make(chan []interface{}, 1000),
		file:  file,
		log:   log,
	}
	aw.init()
	aw.wg.Add(1)
	go aw.writeLoop()
	return aw
}

// writeLoop 异步写入 AOF 文件的循环
func (aw *AsyncAOFWriter) writeLoop() {
	defer aw.wg.Done()
	encoder := gob.NewEncoder(aw.file)
	ctx := context.Background()
	for command := range aw.queue {
		aw.log.WithContext(ctx).Infof("write command: %v", command)
		err := encoder.Encode(command)
		if err != nil {
			aw.log.WithContext(ctx).Errorf("writing to AOF file err: %v", err)
		}
		err = aw.file.Sync()
		if err != nil {
			aw.log.WithContext(ctx).Errorf("write async AOF command error: %v", err)
		}
	}
}

// Write 向异步 AOF 写入器写入命令
func (aw *AsyncAOFWriter) Write(command []interface{}) {
	aw.queue <- command
}

// Close 关闭异步 AOF 写入器
func (aw *AsyncAOFWriter) Close() {
	close(aw.queue)
	aw.wg.Wait()
}

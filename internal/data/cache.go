package data

import (
	"context"
	"encoding/gob"
	"github.com/go-kratos/kratos/v2/log"
	"gocache-service/internal/biz"
	"io"
	"os"
	"time"
)

type cacheRepo struct {
	data      *Data
	log       *log.Helper
	aofWriter *AsyncAOFWriter
	file      *os.File
}

const (
	defaultDataFile = "cache.aof"
)

func NewCacheRepo(data *Data, logger log.Logger) biz.CacheRepo {
	cacheR := &cacheRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
	file, _ := os.OpenFile(defaultDataFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	cacheR.aofWriter = NewAsyncAOFWriter(file, cacheR.log)
	cacheR.file = file
	cacheR.init()
	return cacheR
}

func (r *cacheRepo) init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register("")
	gob.Register(time.Duration(0))
}

func (r *cacheRepo) GetFile(ctx context.Context) (*os.File, error) {
	file, err := os.OpenFile(defaultDataFile, os.O_RDONLY, 0666)
	return file, err
}

func (r *cacheRepo) Write(ctx context.Context, command []interface{}) error {
	r.aofWriter.Write(command)
	return nil
}

// CleanupAOF 清理 AOF 文件中的过期记录
func (r *cacheRepo) CleanupAOF(ctx context.Context, expiredKeys []string) error {
	// 创建一个临时文件
	dir, err := os.Getwd()
	r.log.WithContext(ctx).Infof("CleanupAOF expiredKeys :%+v,dir:%s", expiredKeys, dir)
	tempFile, err := os.CreateTemp("", "cache-aof-temp-*.tmp")
	if err != nil {
		r.log.WithContext(ctx).Errorf("CreateTemp err:%v", err)
		return err
	}
	defer tempFile.Close()
	// 标记过期键
	expiredKeySet := make(map[string]bool)
	for _, key := range expiredKeys {
		expiredKeySet[key] = true
	}

	// 回到文件开头
	_, err = r.file.Seek(0, 0)
	if err != nil {
		return err
	}
	decoder := gob.NewDecoder(r.file)
	encoder := gob.NewEncoder(tempFile)
	for {
		var command []interface{}
		err = decoder.Decode(&command)
		if err != nil {
			r.log.WithContext(ctx).Errorf("CleanupAOF Decode err:%v", err)
			if err == io.EOF {
				break
			}
			return err
		}
		if len(command) == 4 && command[0] == "SET" {
			key := command[1].(string)
			// 如果键不是过期键，则写入临时文件
			if !expiredKeySet[key] {
				err := encoder.Encode(command)
				if err != nil {
					return err
				}
			}
		} else if len(command) == 2 && command[0] == "DEL" {
			key := command[1].(string)
			// 如果键不是过期键，则写入临时文件
			if !expiredKeySet[key] {
				err := encoder.Encode(command)
				if err != nil {
					return err
				}
			}
		}
	}

	// 关闭原文件
	err = r.file.Close()
	if err != nil {
		return err
	}
	// 删除原文件
	err = os.Remove(r.file.Name())
	if err != nil {
		return err
	}
	// 将临时文件重命名为原文件
	err = os.Rename(tempFile.Name(), defaultDataFile)
	if err != nil {
		return err
	}
	// 重新打开原文件
	r.file, err = os.OpenFile(r.file.Name(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	r.aofWriter.file = r.file
	return nil
}

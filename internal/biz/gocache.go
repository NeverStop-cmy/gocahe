package biz

import (
	"context"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

var (
	ErrKeyNotFound = errors.New("cache: key not found")
)

const (
	defaultSaveInterval = 30 * time.Second
	numShards           = 32
)

type CacheItem struct {
	Value     string `json:"value" gob:"value"`
	ExpiresAt int64  `json:"expires_at" gob:"expires_at"`
}

type CacheBuffer struct {
	Data map[string]CacheItem `json:"data" gob:"data"`
}

type CacheRepo interface {
	Write(ctx context.Context, command []interface{}) error
	GetFile(ctx context.Context) (*os.File, error)
	CleanupAOF(ctx context.Context, expiredKeys []string) error
}

func (c *GoCacheUsecase) init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register("")
	gob.Register(time.Duration(0))
}

type GoCacheUsecase struct {
	repo   CacheRepo
	log    *log.Helper
	shards [numShards]struct {
		active *CacheBuffer
		mu     sync.RWMutex
	}

	wg     sync.WaitGroup
	ticker *time.Ticker
	stop   chan struct{}

	timeWheel *TimeWheel
}

func NewGoCacheUsecase(repo CacheRepo, logger log.Logger) *GoCacheUsecase {
	c := &GoCacheUsecase{
		ticker: time.NewTicker(defaultSaveInterval),
		stop:   make(chan struct{}),
		repo:   repo,
		log:    log.NewHelper(logger),
	}

	for i := range c.shards {
		c.shards[i].active = &CacheBuffer{
			Data: make(map[string]CacheItem),
		}
	}

	c.timeWheel = NewTimeWheel(60, time.Second, c)
	c.init()

	// 启动后台任务
	c.loadFromDisk()
	go c.startExpirationChecker()
	return c
}

func (c *GoCacheUsecase) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	c.log.WithContext(ctx).Infof("set key:%s,value:%s,ttl:%v", key, value, ttl)
	shard := c.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry := CacheItem{
		Value: value,
	}
	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl).Unix()
	}

	shard.active.Data[key] = entry
	c.timeWheel.Add(key, ttl)
	_ = c.repo.Write(ctx, []interface{}{"SET", key, value, entry.ExpiresAt})
	return nil
}

// fnv32 计算字符串的 FNV-1a 32 位哈希值
func fnv32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// getShard 根据键获取对应的分片
func (c *GoCacheUsecase) getShard(key string) *struct {
	active *CacheBuffer
	mu     sync.RWMutex
} {
	hash := fnv32(key)
	index := hash % numShards
	return &c.shards[index]
}

func (c *GoCacheUsecase) Get(ctx context.Context, key string) (string, error) {
	c.log.WithContext(ctx).Infof("get key:%s", key)
	shard := c.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.active.Data[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	expiry := entry.ExpiresAt
	if expiry < time.Now().Unix() {
		delete(shard.active.Data, key)
		return "", ErrKeyNotFound
	}
	return entry.Value, nil
}

func (c *GoCacheUsecase) Delete(ctx context.Context, key string) error {
	shard := c.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.active.Data, key)
	_ = c.repo.Write(ctx, []interface{}{"DEL", key})
	return nil
}

func (c *GoCacheUsecase) loadFromDisk() {
	ctx := context.Background()
	file, _ := c.repo.GetFile(ctx)
	_, err := file.Seek(0, 0)
	if err != nil {
		return
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	dir, err := os.Getwd()
	c.log.WithContext(ctx).Infof("loadFromDisk start!dir:%s", dir)
	for {
		var command []interface{}
		err = decoder.Decode(&command)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		c.log.WithContext(ctx).Infof("loadFromDisk command:%v", command)
		if len(command) == 4 && command[0] == "SET" {
			key := command[1].(string)
			value := command[2].(string)
			expiresAt := command[3].(int64)
			//等于0是永不过期
			if expiresAt == 0 || time.Now().Unix() < expiresAt {
				shard := c.getShard(key)
				shard.mu.Lock()
				entry := CacheItem{
					Value:     value,
					ExpiresAt: expiresAt,
				}
				shard.active.Data[key] = entry
				shard.mu.Unlock()
				//todo 随机
				c.timeWheel.Add(key, 0)
			}
		} else if len(command) == 2 && command[0] == "DEL" {
			key := command[1].(string)
			shard := c.getShard(key)
			shard.mu.Lock()
			delete(shard.active.Data, key)
			shard.mu.Unlock()
		}
	}
	c.log.WithContext(ctx).Infof("loadFromDisk done!")
}

func (c *GoCacheUsecase) startExpirationChecker() {
	c.wg.Add(1)
	defer c.wg.Done()
	for {
		select {
		case <-c.ticker.C:
			expiredKeys := c.collectExpiredKeys()
			if len(expiredKeys) > 0 {
				c.cleanupMemory(expiredKeys)
				err := c.repo.CleanupAOF(context.Background(), expiredKeys)
				if err != nil {
					c.log.WithContext(context.Background()).Errorf("cleanup CleanupAOF err: %v", err)
				}
			}

		case <-c.stop:
			c.ticker.Stop()
			c.timeWheel.Close()
			return
		}
	}
}

// collectExpiredKeys 收集过期的键
func (c *GoCacheUsecase) collectExpiredKeys() []string {
	var expiredKeys []string
	for i := range c.shards {
		c.shards[i].mu.RLock()
		for key, entry := range c.shards[i].active.Data {
			if entry.ExpiresAt > 0 && time.Now().Unix() > (entry.ExpiresAt) {
				expiredKeys = append(expiredKeys, key)
			}
		}
		c.shards[i].mu.RUnlock()
	}
	return expiredKeys
}

// cleanupMemory 清理内存中的过期数据
func (c *GoCacheUsecase) cleanupMemory(expiredKeys []string) {
	for _, key := range expiredKeys {
		shard := c.getShard(key)
		shard.mu.Lock()
		delete(shard.active.Data, key)
		shard.mu.Unlock()
	}
}

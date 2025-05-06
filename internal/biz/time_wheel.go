package biz

import (
	"context"
	"sync"
	"time"
)

// TimeWheel 结构体用于管理过期数据
type TimeWheel struct {
	slots []map[string]int64
	tick  time.Duration
	index int
	stop  chan struct{}
	wg    sync.WaitGroup
	cache *GoCacheUsecase
	mutex sync.Mutex
}

// NewTimeWheel 创建一个新的时间轮
func NewTimeWheel(slots int, tick time.Duration, cache *GoCacheUsecase) *TimeWheel {
	tw := &TimeWheel{
		slots: make([]map[string]int64, slots),
		tick:  tick,
		index: 0,
		stop:  make(chan struct{}),
		cache: cache,
	}
	for i := range tw.slots {
		tw.slots[i] = make(map[string]int64)
	}
	tw.wg.Add(1)
	go tw.run()
	return tw
}

// Add 向时间轮添加一个键和过期时间
func (tw *TimeWheel) Add(key string, expiration time.Duration) {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()
	slotIndex := (tw.index + int(expiration/tw.tick)) % len(tw.slots)
	tw.slots[slotIndex][key] = time.Now().Add(expiration).Unix()
}

// run 时间轮的运行循环
func (tw *TimeWheel) run() {
	defer tw.wg.Done()
	ticker := time.NewTicker(tw.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			tw.mutex.Lock()
			tw.index = (tw.index + 1) % len(tw.slots)
			now := time.Now().Unix()
			for key, expiresAt := range tw.slots[tw.index] {
				if now > expiresAt {
					delete(tw.slots[tw.index], key)
					_ = tw.cache.Delete(context.Background(), key)
				}
			}
			tw.mutex.Unlock()
		case <-tw.stop:
			return
		}
	}
}

// Close 关闭时间轮
func (tw *TimeWheel) Close() {
	close(tw.stop)
	tw.wg.Wait()
}

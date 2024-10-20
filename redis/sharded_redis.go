package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/mailru/easyjson"
)

const ShardCount int = 32

type Hasher interface {
	Hash(string) uint32
}

type Fnv32Hasher struct{}

// Hash Fowler–Noll–Vo is a non-cryptographic hash function created by Glenn Fowler, Landon Curt Noll, and Kiem-Phong Vo.
// See in: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function.
func (f Fnv32Hasher) Hash(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

type Shard struct {
	mutex    *sync.RWMutex
	hotCache map[string][]byte
}

type LockFreeRedis struct {
	client *redis.Client
	queue  chan locked
	hasher Hasher
	shards []*Shard
}

func (r *LockFreeRedis) Set(ctx context.Context, key string, value easyjson.Marshaler, ttl time.Duration) error {
	val, err := easyjson.Marshal(value)
	if err != nil {
		return err
	}
	err = r.client.Set(ctx, key, val, ttl).Err()
	if err != nil && redisPoolIsDie(ctx, err) {
		return nil
	}
	return nil
}

func (r *LockFreeRedis) SetRaw(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	err := r.client.Set(ctx, key, value, ttl).Err()
	if err != nil && redisPoolIsDie(ctx, err) {
		return nil
	}
	return nil
}

func (r *LockFreeRedis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		if redisPoolIsDie(ctx, err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return val, true, nil
}

// LockedGet like as Get, only hot cache is additionally checked
func (r *LockFreeRedis) LockedGet(ctx context.Context, key string) ([]byte, bool, error) {
	shard := r.getShard(key)

	// fast path
	shard.mutex.RLock()
	if val, ok := shard.hotCache[key]; ok {
		shard.mutex.RUnlock()
		return val, true, nil
	}
	shard.mutex.RUnlock()

	// Re-checking existence of cache or taking mutex.
	// Let's say two simultaneous requests came, one sat down to take mutex for writing, other did not,
	// but both checked if there was no data.
	// Then after completing first, the second will also write new data to cache, which is unacceptable.
	// To do this, we check for second time whether there is no data in cache.
	shard.mutex.RLock()
	if val, ok := shard.hotCache[key]; ok {
		shard.mutex.RUnlock()
		return val, true, nil
	}
	shard.mutex.RUnlock()

	// Slow path.
	return r.Get(ctx, key)
}

// LockedSet like as Set, only hot cache is additionally checked.
func (r *LockFreeRedis) LockedSet(ctx context.Context, key string, value easyjson.Marshaler, ttl time.Duration) error {
	shard := r.getShard(key)

	val, err := easyjson.Marshal(value)
	if err != nil {
		return err
	}

	// Let's try to check hot cache in memory, if there is already a cache, skip it.
	shard.mutex.Lock()
	if _, ok := shard.hotCache[key]; ok {
		shard.mutex.Unlock()
		return nil
	}
	// If there is no cache, add it to heated cache, then perform operation of set it into the Redis.
	shard.hotCache[key] = val
	shard.mutex.Unlock()

	// It does not matter if set operation was completed successfully, delete key from hot cache.
	defer func() {
		shard.mutex.Lock()
		delete(shard.hotCache, key)
		shard.mutex.Unlock()
	}()

	return r.SetRaw(ctx, key, val, ttl)
}

// Tx wraps all necessary processing in one global lock.
func (r *LockFreeRedis) Tx(
	ctx context.Context,
	key string,
	ttl time.Duration,
	callback TxCallback,
) error {
	shard := r.getShard(key)

	shard.mutex.Lock()

	value, err := callback(ctx)
	if err != nil {
		shard.mutex.Unlock()
		return err
	}

	val, err := easyjson.Marshal(value)
	if err != nil {
		shard.mutex.Unlock()
		return err
	}

	// If there is no cache, add it to heated cache, then perform operation of set it into the Redis.
	shard.hotCache[key] = val
	shard.mutex.Unlock()

	r.queue <- locked{
		key: key,
		val: val,
		ttl: ttl,
	}

	return nil
}

// LockedPushQueue like as LockedSet, only insertion occurs asynchronously without blocking execution flow.
func (r *LockFreeRedis) LockedPushQueue(key string, value easyjson.Marshaler, ttl time.Duration) error {
	shard := r.getShard(key)

	val, err := easyjson.Marshal(value)
	if err != nil {
		return err
	}
	shard.mutex.Lock()
	if _, ok := shard.hotCache[key]; ok {
		shard.mutex.Unlock()
		return nil
	}
	shard.hotCache[key] = val
	shard.mutex.Unlock()

	r.queue <- locked{
		key: key,
		val: val,
		ttl: ttl,
	}
	return nil
}

// LockedSetBytes like as Set, only hot cache is additionally checked.
func (r *LockFreeRedis) LockedSetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	shard := r.getShard(key)

	// Let's try to check hot cache in memory, if there is already a cache, skip it.
	shard.mutex.Lock()
	if _, ok := shard.hotCache[key]; ok {
		shard.mutex.Unlock()
		return nil
	}
	// If there is no cache, add it to heated cache, then perform operation of set it into the Redis.
	shard.hotCache[key] = value
	shard.mutex.Unlock()

	// It does not matter if set operation was completed successfully, delete key from hot cache.
	defer func() {
		shard.mutex.Lock()
		delete(shard.hotCache, key)
		shard.mutex.Unlock()
	}()

	return r.SetRaw(ctx, key, value, ttl)
}

func (r *LockFreeRedis) Drop() error {
	return r.client.Close()
}

func (r *LockFreeRedis) DropMsg() string {
	return "close redis pool"
}

func (r *LockFreeRedis) releaseQueue(ctx context.Context) {
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case mutex := <-r.queue:
			if err = r.SetRaw(ctx, mutex.key, mutex.val, mutex.ttl); err != nil {
				log.Println(err)
			}
			shard := r.getShard(mutex.key)
			shard.mutex.Lock()
			delete(shard.hotCache, mutex.key)
			shard.mutex.Unlock()
		}
	}
}

func (r *LockFreeRedis) getShard(key string) *Shard {
	return r.shards[uint(r.hasher.Hash(key))%uint(ShardCount)]
}

func NewLockFree(ctx context.Context, opt *Opt) (*LockFreeRedis, error) {
	if opt.PoolSize <= 0 {
		return nil, ErrEmptyPoolSize
	}
	if opt.HotCacheSize <= 0 {
		return nil, ErrEmptyHotCacheSize
	}
	if opt.AsyncQueueSize <= 0 {
		return nil, ErrEmptyAsyncQueueSize
	}
	rdb := redis.NewClient(&redis.Options{
		Password: opt.Password,
		Addr:     opt.Host,
		DB:       opt.DB,
		PoolSize: opt.PoolSize,
	})

	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	rediska := &LockFreeRedis{
		client: rdb,
		shards: make([]*Shard, ShardCount),
		hasher: &Fnv32Hasher{},
		queue:  make(chan locked, opt.AsyncQueueSize),
	}

	for i := 0; i < ShardCount; i++ {
		rediska.shards[i] = &Shard{
			mutex:    &sync.RWMutex{},
			hotCache: map[string][]byte{},
		}
	}

	go rediska.releaseQueue(ctx)
	return rediska, nil
}

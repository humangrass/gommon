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

type Rediska interface {
	Set(ctx context.Context, key string, value easyjson.Marshaler, ttl time.Duration) error
	SetRaw(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, bool, error)
	LockedGet(ctx context.Context, key string) ([]byte, bool, error)
	LockedSet(ctx context.Context, key string, value easyjson.Marshaler, ttl time.Duration) error
	LockedSetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Tx(ctx context.Context, key string, ttl time.Duration, callback TxCallback) error
	LockedPushQueue(key string, value easyjson.Marshaler, ttl time.Duration) error
}

var (
	ErrEmptyPoolSize       = errors.New("redis empty pool size")
	ErrEmptyHotCacheSize   = errors.New("redis empty hot cache size")
	ErrEmptyAsyncQueueSize = errors.New("redis empty async queue size")
)

type locked struct {
	key string
	val interface{}
	ttl time.Duration
}

type Redis struct {
	client   *redis.Client
	queue    chan locked
	mutex    *sync.RWMutex
	hotCache map[string][]byte
}

type Opt struct {
	Host           string `yaml:"host"`
	Password       string `yaml:"password"`
	Port           int    `yaml:"port"`
	DB             int    `yaml:"db"`
	PoolSize       int    `yaml:"pool_size"`
	HotCacheSize   int    `yaml:"hot_cache_size"`
	AsyncQueueSize int    `yaml:"async_queue_size"`
	UseLockFree    bool   `yaml:"use_lock_free"`
}

func (r *Redis) Set(ctx context.Context, key string, value easyjson.Marshaler, ttl time.Duration) error {
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

func (r *Redis) SetRaw(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	err := r.client.Set(ctx, key, value, ttl).Err()
	if err != nil && redisPoolIsDie(ctx, err) {
		return nil
	}
	return nil
}

func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
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

// LockedGet like as Get, only hot cache is additionally checked.
func (r *Redis) LockedGet(ctx context.Context, key string) ([]byte, bool, error) {
	// Fast path.
	r.mutex.RLock()
	if val, ok := r.hotCache[key]; ok {
		r.mutex.RUnlock()
		return val, true, nil
	}
	r.mutex.RUnlock()

	// Re-checking existence of cache or taking mutex.
	// Let's say two simultaneous requests came, one sat down to take mutex for writing, other did not,
	// but both checked if there was no data.
	// Then after completing first, the second will also write new data to cache, which is unacceptable.
	// To do this, we check for second time whether there is no data in cache.
	r.mutex.RLock()
	if val, ok := r.hotCache[key]; ok {
		r.mutex.RUnlock()
		return val, true, nil
	}
	r.mutex.RUnlock()

	// Slow path.
	return r.Get(ctx, key)
}

// LockedSet like as Set, only hot cache is additionally checked.
func (r *Redis) LockedSet(ctx context.Context, key string, value easyjson.Marshaler, ttl time.Duration) error {
	val, err := easyjson.Marshal(value)
	if err != nil {
		return err
	}

	// Let's try to check hot cache in memory, if there is already a cache, skip it.
	r.mutex.Lock()
	if _, ok := r.hotCache[key]; ok {
		r.mutex.Unlock()
		return nil
	}
	// If there is no cache, add it to heated cache, then perform operation of set it into the Redis.
	r.hotCache[key] = val
	r.mutex.Unlock()

	// It does not matter if set operation was completed successfully, delete key from hot cache.
	defer func() {
		r.mutex.Lock()
		delete(r.hotCache, key)
		r.mutex.Unlock()
	}()

	return r.SetRaw(ctx, key, val, ttl)
}

type TxCallback func(ctx context.Context) (easyjson.Marshaler, error)

// Tx wraps all necessary processing in one global lock.
func (r *Redis) Tx(
	ctx context.Context,
	key string,
	ttl time.Duration,
	callback TxCallback,
) error {
	r.mutex.Lock()

	value, err := callback(ctx)
	if err != nil {
		r.mutex.Unlock()
		return err
	}

	val, err := easyjson.Marshal(value)
	if err != nil {
		r.mutex.Unlock()
		return err
	}

	// If there is no cache, add it to heated cache, then perform operation of set it into the Redis.
	r.hotCache[key] = val
	r.mutex.Unlock()

	r.queue <- locked{
		key: key,
		val: val,
		ttl: ttl,
	}

	return nil
}

// LockedPushQueue like as LockedSet, only insertion occurs asynchronously without blocking execution flow.
func (r *Redis) LockedPushQueue(key string, value easyjson.Marshaler, ttl time.Duration) error {
	val, err := easyjson.Marshal(value)
	if err != nil {
		return err
	}
	r.mutex.Lock()
	if _, ok := r.hotCache[key]; ok {
		r.mutex.Unlock()
		return nil
	}
	r.hotCache[key] = val
	r.mutex.Unlock()

	r.queue <- locked{
		key: key,
		val: val,
		ttl: ttl,
	}
	return nil
}

// LockedSetBytes like as Set, only hot cache is additionally checked.
func (r *Redis) LockedSetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Let's try to check hot cache in memory, if there is already a cache, skip it.
	r.mutex.Lock()
	if _, ok := r.hotCache[key]; ok {
		r.mutex.Unlock()
		return nil
	}
	// If there is no cache, add it to heated cache, then perform operation of set it into the Redis.
	r.hotCache[key] = value
	r.mutex.Unlock()

	// It does not matter if set operation was completed successfully, delete key from hot cache.
	defer func() {
		r.mutex.Lock()
		delete(r.hotCache, key)
		r.mutex.Unlock()
	}()

	return r.SetRaw(ctx, key, value, ttl)
}

func (r *Redis) Drop() error {
	return r.client.Close()
}

func (r *Redis) DropMsg() string {
	return "close redis pool"
}

func (r *Redis) releaseQueue(ctx context.Context) {
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case mutex := <-r.queue:
			if err = r.SetRaw(ctx, mutex.key, mutex.val, mutex.ttl); err != nil {
				log.Println(err)
			}
			r.mutex.Lock()
			delete(r.hotCache, mutex.key)
			r.mutex.Unlock()
		}
	}
}

func New(ctx context.Context, opt *Opt) (*Redis, error) {
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

	rediska := &Redis{
		client:   rdb,
		mutex:    &sync.RWMutex{},
		queue:    make(chan locked, opt.AsyncQueueSize),
		hotCache: make(map[string][]byte, opt.HotCacheSize),
	}

	go rediska.releaseQueue(ctx)
	return rediska, nil
}

func redisPoolIsDie(ctx context.Context, err error) bool {
	return errors.Is(err, redis.ErrClosed) && ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled)
}

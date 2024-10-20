package redis

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
)

type Cluster struct {
	nodes []*redis.Client
}

func (r *Cluster) GetForEachShard(ctx context.Context, key string) ([]byte, error) {
	wg := sync.WaitGroup{}
	ch := make(chan []byte, len(r.nodes))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, node := range r.nodes {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(client *redis.Client) {
			defer wg.Done()
			bytes, err := client.Get(ctx, key).Bytes()
			if err != nil {
				return
			}
			if bytes != nil {
				ch <- bytes
			}
		}(node)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return <-ch, nil
}

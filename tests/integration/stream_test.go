//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vinnedev/redis-stream-go/internal/config"
	"github.com/vinnedev/redis-stream-go/internal/observability"
	"github.com/vinnedev/redis-stream-go/internal/stream"
)

func startRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp"),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port())
}

func newIntegrationDeps(addr string) (*redis.Client, config.StreamConfig, config.WorkerConfig, *observability.Metrics) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	cfg := config.StreamConfig{
		Name:             "integ-stream",
		ConsumerGroup:    "integ-group",
		ConsumerName:     "worker",
		MaxLen:           10000,
		ReadCount:        10,
		BlockTimeout:     500 * time.Millisecond,
		DeadLetterStream: "integ-stream.dlq",
	}
	wcfg := config.WorkerConfig{
		Concurrency:    2,
		RetryAttempts:  3,
		RetryBaseDelay: 10 * time.Millisecond,
		RetryMaxDelay:  100 * time.Millisecond,
	}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	return rdb, cfg, wcfg, metrics
}

func TestPublishAndConsume(t *testing.T) {
	addr := startRedis(t)
	rdb, cfg, wcfg, metrics := newIntegrationDeps(addr)
	ctx := context.Background()

	stream.EnsureConsumerGroup(ctx, rdb, cfg.Name, cfg.ConsumerGroup)

	pub := stream.NewPublisher(rdb, cfg, wcfg, metrics)

	const n = 10
	for i := 0; i < n; i++ {
		if err := pub.Publish(ctx, map[string]any{"i": i}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	var consumed atomic.Int64
	handler := func(ctx context.Context, msg stream.Message) error {
		consumed.Add(1)
		return nil
	}

	w := stream.NewWorker(rdb, cfg, wcfg, handler, metrics)
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.Start(wCtx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if consumed.Load() == int64(n) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	w.Wait()

	if got := consumed.Load(); got != int64(n) {
		t.Fatalf("expected %d consumed, got %d", n, got)
	}
}

func TestDeadLetter(t *testing.T) {
	addr := startRedis(t)
	rdb, cfg, wcfg, metrics := newIntegrationDeps(addr)
	ctx := context.Background()

	stream.EnsureConsumerGroup(ctx, rdb, cfg.Name, cfg.ConsumerGroup)

	pub := stream.NewPublisher(rdb, cfg, wcfg, metrics)
	if err := pub.Publish(ctx, map[string]any{"fail": "true"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	handler := func(ctx context.Context, msg stream.Message) error {
		return errors.New("always fails")
	}

	wcfg.RetryAttempts = 2
	w := stream.NewWorker(rdb, cfg, wcfg, handler, metrics)
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.Start(wCtx)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n, err := rdb.XLen(ctx, cfg.DeadLetterStream).Result()
		if err == nil && n > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	w.Wait()

	n, err := rdb.XLen(ctx, cfg.DeadLetterStream).Result()
	if err != nil {
		t.Fatalf("xlen dlq: %v", err)
	}
	if n == 0 {
		t.Fatal("expected message in dead letter stream")
	}
}

func TestGracefulShutdown(t *testing.T) {
	addr := startRedis(t)
	rdb, cfg, wcfg, metrics := newIntegrationDeps(addr)
	ctx := context.Background()

	stream.EnsureConsumerGroup(ctx, rdb, cfg.Name, cfg.ConsumerGroup)

	handler := func(ctx context.Context, msg stream.Message) error {
		return nil
	}

	w := stream.NewWorker(rdb, cfg, wcfg, handler, metrics)
	wCtx, cancel := context.WithCancel(ctx)
	w.Start(wCtx)

	cancel()

	done := make(chan struct{})
	go func() {
		w.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not shut down cleanly")
	}
}

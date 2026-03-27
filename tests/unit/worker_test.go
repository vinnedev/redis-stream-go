package unit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"redis-stream-go/internal/config"
	"redis-stream-go/internal/observability"
	"redis-stream-go/internal/stream"
)

func newWorkerDeps(t *testing.T, addr string) (*redis.Client, config.StreamConfig, config.WorkerConfig, *observability.Metrics) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	cfg := config.StreamConfig{
		Name:             "test-stream",
		ConsumerGroup:    "test-group",
		ConsumerName:     "worker",
		MaxLen:           1000,
		ReadCount:        10,
		BlockTimeout:     100 * time.Millisecond,
		DeadLetterStream: "test-stream.dlq",
	}
	wcfg := config.WorkerConfig{
		Concurrency:    1,
		RetryAttempts:  2,
		RetryBaseDelay: 0,
		RetryMaxDelay:  0,
	}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	return rdb, cfg, wcfg, metrics
}

func seedMessage(t *testing.T, mr *miniredis.Miniredis, streamName, group string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	rdb.XGroupCreateMkStream(ctx, streamName, group, "0")
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		ID:     "*",
		Values: map[string]any{"event": "test"},
	})
}

func TestWorkerProcessesMessage(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb, cfg, wcfg, metrics := newWorkerDeps(t, mr.Addr())
	seedMessage(t, mr, cfg.Name, cfg.ConsumerGroup)

	var called atomic.Bool
	handler := func(ctx context.Context, msg stream.Message) error {
		called.Store(true)
		return nil
	}

	w := stream.NewWorker(rdb, cfg, wcfg, handler, metrics)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w.Start(ctx)

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if called.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	w.Wait()

	if !called.Load() {
		t.Fatal("handler was not called")
	}
	if got := testutil.ToFloat64(metrics.Consumed); got != 1 {
		t.Fatalf("expected Consumed=1, got %v", got)
	}
}

func TestWorkerDeadLettersAfterMaxRetries(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb, cfg, wcfg, metrics := newWorkerDeps(t, mr.Addr())
	seedMessage(t, mr, cfg.Name, cfg.ConsumerGroup)

	handler := func(ctx context.Context, msg stream.Message) error {
		return errors.New("always fails")
	}

	w := stream.NewWorker(rdb, cfg, wcfg, handler, metrics)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w.Start(ctx)

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(metrics.DeadLettered) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	w.Wait()

	if got := testutil.ToFloat64(metrics.DeadLettered); got != 1 {
		t.Fatalf("expected DeadLettered=1, got %v", got)
	}
}

func TestWorkerGracefulShutdown(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb, cfg, wcfg, metrics := newWorkerDeps(t, mr.Addr())

	rdb.XGroupCreateMkStream(context.Background(), cfg.Name, cfg.ConsumerGroup, "0")

	handler := func(ctx context.Context, msg stream.Message) error {
		return nil
	}

	w := stream.NewWorker(rdb, cfg, wcfg, handler, metrics)
	ctx, cancel := context.WithCancel(context.Background())

	w.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() {
		w.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not shut down within timeout")
	}
}

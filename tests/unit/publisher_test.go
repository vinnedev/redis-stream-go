package unit

import (
	"context"
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

func newTestRDB(t *testing.T, addr string) *redis.Client {
	t.Helper()
	return redis.NewClient(&redis.Options{Addr: addr})
}

func newTestMetrics(t *testing.T) (*prometheus.Registry, *observability.Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return reg, observability.NewMetrics(reg)
}

func TestPublishSuccessIncrementsMetric(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := newTestRDB(t, mr.Addr())
	_, metrics := newTestMetrics(t)

	cfg := config.StreamConfig{Name: "test-stream", MaxLen: 1000}
	wcfg := config.WorkerConfig{}
	pub := stream.NewPublisher(rdb, cfg, wcfg, metrics)

	if err := pub.Publish(context.Background(), map[string]any{"key": "value"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := testutil.ToFloat64(metrics.Published); got != 1 {
		t.Fatalf("expected Published=1, got %v", got)
	}
}

func TestPublishFailsWhenCircuitBreakerOpen(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        "localhost:19999",
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  0,
	})
	_, metrics := newTestMetrics(t)

	cfg := config.StreamConfig{Name: "test-stream", MaxLen: 1000}
	wcfg := config.WorkerConfig{}
	pub := stream.NewPublisher(rdb, cfg, wcfg, metrics)

	for i := 0; i < 10; i++ {
		pub.Publish(context.Background(), map[string]any{"k": "v"})
	}

	if got := testutil.ToFloat64(metrics.PublishErrors); got == 0 {
		t.Fatal("expected PublishErrors > 0")
	}
}

func TestPublishRespectsContextCancellation(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := newTestRDB(t, mr.Addr())
	_, metrics := newTestMetrics(t)

	cfg := config.StreamConfig{Name: "test-stream", MaxLen: 1000}
	wcfg := config.WorkerConfig{}
	pub := stream.NewPublisher(rdb, cfg, wcfg, metrics)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pub.Publish(ctx, map[string]any{"key": "value"})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

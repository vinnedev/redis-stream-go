package stream

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"redis-stream-go/internal/config"
	"redis-stream-go/internal/observability"
	"redis-stream-go/pkg/backoff"
	"redis-stream-go/pkg/circuitbreaker"
)

type Publisher struct {
	rdb     *redis.Client
	cfg     config.StreamConfig
	breaker *circuitbreaker.Breaker
	backoff backoff.Config
	metrics *observability.Metrics
}

func NewPublisher(rdb *redis.Client, cfg config.StreamConfig, wcfg config.WorkerConfig, metrics *observability.Metrics) *Publisher {
	bcfg := circuitbreaker.DefaultConfig()
	boCfg := backoff.Config{
		BaseDelay:  wcfg.RetryBaseDelay,
		MaxDelay:   wcfg.RetryMaxDelay,
		Multiplier: 2.0,
		Jitter:     0.3,
	}

	return &Publisher{
		rdb:     rdb,
		cfg:     cfg,
		breaker: circuitbreaker.New(bcfg),
		backoff: boCfg,
		metrics: metrics,
	}
}

func (p *Publisher) Publish(ctx context.Context, values map[string]any) error {
	const maxAttempts = 3

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := p.breaker.Allow(); err != nil {
			p.metrics.PublishErrors.Inc()
			return err
		}

		start := time.Now()
		err := p.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: p.cfg.Name,
			MaxLen: p.cfg.MaxLen,
			Approx: true,
			ID:     "*",
			Values: values,
		}).Err()
		duration := time.Since(start).Seconds()
		p.metrics.PublishDuration.Observe(duration)

		if err == nil {
			p.breaker.Success()
			p.metrics.Published.Inc()
			return nil
		}

		p.breaker.Failure()
		p.metrics.PublishErrors.Inc()

		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.backoff.Delay(attempt)):
			}
		}
	}

	return context.DeadlineExceeded
}

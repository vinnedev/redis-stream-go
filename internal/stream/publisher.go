package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vinnedev/redis-stream-go/internal/config"
	"github.com/vinnedev/redis-stream-go/internal/logger"
	"github.com/vinnedev/redis-stream-go/internal/observability"
	"github.com/vinnedev/redis-stream-go/pkg/backoff"
	"github.com/vinnedev/redis-stream-go/pkg/circuitbreaker"
	"go.uber.org/zap"
)

type Publisher struct {
	rdb     *redis.Client
	cfg     config.StreamConfig
	wcfg    config.WorkerConfig
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
		wcfg:    wcfg,
		breaker: circuitbreaker.New(bcfg),
		backoff: boCfg,
		metrics: metrics,
	}
}

func (p *Publisher) Publish(ctx context.Context, values map[string]any) error {
	log := logger.FromContext(ctx)

	for attempt := 0; attempt < p.wcfg.RetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := p.breaker.Allow(); err != nil {
			p.metrics.CircuitBreakerState.Set(float64(p.breaker.State()))
			p.metrics.PublishErrors.Inc()
			log.Error("publish rejected by circuit breaker", zap.Error(err))
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
		p.metrics.PublishDuration.Observe(time.Since(start).Seconds())

		if err == nil {
			p.breaker.Success()
			p.metrics.CircuitBreakerState.Set(float64(p.breaker.State()))
			p.metrics.Published.Inc()
			return nil
		}

		p.breaker.Failure()
		p.metrics.CircuitBreakerState.Set(float64(p.breaker.State()))
		p.metrics.PublishErrors.Inc()
		log.Warn("publish attempt failed", zap.Int("attempt", attempt), zap.Error(err))

		if attempt < p.wcfg.RetryAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.backoff.Delay(attempt)):
			}
		}
	}

	return fmt.Errorf("publish failed after %d attempts", p.wcfg.RetryAttempts)
}

package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"redis-stream-go/internal/config"
	"redis-stream-go/internal/logger"
	"redis-stream-go/internal/observability"
	"redis-stream-go/pkg/backoff"
)

type Worker struct {
	rdb     *redis.Client
	cfg     config.StreamConfig
	wcfg    config.WorkerConfig
	handler Handler
	backoff backoff.Config
	metrics *observability.Metrics
	wg      sync.WaitGroup
}

func NewWorker(rdb *redis.Client, cfg config.StreamConfig, wcfg config.WorkerConfig, handler Handler, metrics *observability.Metrics) *Worker {
	return &Worker{
		rdb:     rdb,
		cfg:     cfg,
		wcfg:    wcfg,
		handler: handler,
		backoff: backoff.Config{
			BaseDelay:  wcfg.RetryBaseDelay,
			MaxDelay:   wcfg.RetryMaxDelay,
			Multiplier: 2.0,
			Jitter:     0.3,
		},
		metrics: metrics,
	}
}

func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.wcfg.Concurrency; i++ {
		w.wg.Add(1)
		w.metrics.ActiveWorkers.Inc()
		go func(id int) {
			defer w.wg.Done()
			defer w.metrics.ActiveWorkers.Dec()
			w.run(ctx, id)
		}(i)
	}
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

func (w *Worker) StartLagPoller(ctx context.Context, lag prometheus.Gauge) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := w.rdb.XLen(ctx, w.cfg.Name).Result()
				if err == nil {
					lag.Set(float64(n))
				}
			}
		}
	}()
}

func (w *Worker) run(ctx context.Context, id int) {
	log := logger.FromContext(ctx).With(zap.Int("worker_id", id))
	consumerName := fmt.Sprintf("%s-%d", w.cfg.ConsumerName, id)

	for {
		if ctx.Err() != nil {
			return
		}

		streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.cfg.ConsumerGroup,
			Consumer: consumerName,
			Streams:  []string{w.cfg.Name, ">"},
			Count:    w.cfg.ReadCount,
			Block:    w.cfg.BlockTimeout,
			NoAck:    false,
		}).Result()

		if err == redis.Nil {
			continue
		}

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("xreadgroup error", zap.Error(err))
			continue
		}

		for _, s := range streams {
			for _, m := range s.Messages {
				msg := Message{
					ID:         m.ID,
					Stream:     s.Stream,
					Values:     m.Values,
					ReceivedAt: time.Now(),
				}
				w.process(ctx, log, msg)
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, log *zap.Logger, msg Message) {
	start := time.Now()

	for attempt := 0; attempt < w.wcfg.RetryAttempts; attempt++ {
		msg.Attempt = attempt

		err := w.handler(ctx, msg)
		if err == nil {
			w.metrics.Consumed.Inc()
			w.metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
			w.acknowledge(ctx, log, msg)
			return
		}

		log.Warn("handler error", zap.String("msg_id", msg.ID), zap.Int("attempt", attempt), zap.Error(err))
		w.metrics.ConsumeErrors.Inc()

		if attempt < w.wcfg.RetryAttempts-1 {
			w.metrics.RetryTotal.Inc()
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.backoff.Delay(attempt)):
			}
		}
	}

	w.metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
	w.deadLetter(ctx, log, msg)
}

func (w *Worker) acknowledge(ctx context.Context, log *zap.Logger, msg Message) {
	pipe := w.rdb.Pipeline()
	pipe.XAck(ctx, msg.Stream, w.cfg.ConsumerGroup, msg.ID)
	pipe.XDel(ctx, msg.Stream, msg.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Error("acknowledge failed", zap.String("msg_id", msg.ID), zap.Error(err))
	}
}

func (w *Worker) deadLetter(ctx context.Context, log *zap.Logger, msg Message) {
	pipe := w.rdb.Pipeline()
	pipe.XAck(ctx, msg.Stream, w.cfg.ConsumerGroup, msg.ID)
	pipe.XDel(ctx, msg.Stream, msg.ID)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: w.cfg.DeadLetterStream,
		ID:     "*",
		Values: msg.Values,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		log.Error("dead letter failed", zap.String("msg_id", msg.ID), zap.Error(err))
		return
	}

	w.metrics.DeadLettered.Inc()
	log.Warn("message dead lettered", zap.String("msg_id", msg.ID))
}

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"github.com/vinnedev/redis-stream-go/internal/config"
	"github.com/vinnedev/redis-stream-go/internal/logger"
	"github.com/vinnedev/redis-stream-go/internal/observability"
	"github.com/vinnedev/redis-stream-go/internal/stream"
)

// phaseState is shared between the phase runner and the worker handler.
type phaseState struct {
	mu      sync.RWMutex
	errRate float64
	delay   time.Duration
	allFail bool
}

func (s *phaseState) set(errRate float64, delay time.Duration, allFail bool) {
	s.mu.Lock()
	s.errRate, s.delay, s.allFail = errRate, delay, allFail
	s.mu.Unlock()
}

func (s *phaseState) get() (float64, time.Duration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errRate, s.delay, s.allFail
}

type scenario struct {
	name        string
	duration    time.Duration
	publishRate int // msg/s, 0 = pause publishing
	errRate     float64
	delay       time.Duration
	allFail     bool
	description string
}

var scenarios = []scenario{
	{
		name:        "warm-up",
		duration:    20 * time.Second,
		publishRate: 10,
		description: "Baseline at 10 msg/s — all successful. Verify throughput panels.",
	},
	{
		name:        "high-throughput",
		duration:    30 * time.Second,
		publishRate: 100,
		description: "Spike to 100 msg/s — watch throughput and active workers panels.",
	},
	{
		name:        "error-spike",
		duration:    30 * time.Second,
		publishRate: 20,
		errRate:     0.5,
		description: "50% error rate — triggers High Consume Error Rate (>5%) alert.",
	},
	{
		name:        "dlq-flood",
		duration:    30 * time.Second,
		publishRate: 10,
		allFail:     true,
		description: "All messages exhaust retries — watch DLQ rate panel and DLQ Spike alert.",
	},
	{
		name:        "consumer-lag",
		duration:    40 * time.Second,
		publishRate: 60,
		delay:       150 * time.Millisecond,
		description: "Publish 60/s, process at 150ms each — lag builds past 1000, triggers Consumer Lag High.",
	},
	{
		name:        "high-latency",
		duration:    30 * time.Second,
		publishRate: 5,
		delay:       2 * time.Second,
		description: "2s processing delay — P99 latency exceeds 1s, triggers P99 Latency High alert.",
	},
	{
		name:        "recovery",
		duration:    30 * time.Second,
		publishRate: 10,
		description: "Normal traffic — metrics recover, alerts resolve.",
	},
	{
		name:        "drain",
		duration:    20 * time.Second,
		publishRate: 0,
		description: "Stop publishing — consumer lag drains to zero.",
	},
}

// counters are updated by the publisher and handler, printed each tick.
var (
	published  atomic.Int64
	consumed   atomic.Int64
	errored    atomic.Int64
	dlqd       atomic.Int64
)

func main() {
	_ = config.LoadDotEnv()

	cfg := config.Load()

	log, err := logger.New(cfg.Log.Level, cfg.Log.JSON)
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ctx = logger.WithContext(ctx, log)

	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)

	rdb := stream.NewClient(cfg.Redis)
	defer rdb.Close()

	// Use a dedicated consumer group so the loadtest doesn't interfere with the main app.
	const ltGroup = "loadtest-workers"
	const ltConsumer = "loadtest"
	if err := stream.EnsureConsumerGroup(ctx, rdb, cfg.Stream.Name, ltGroup); err != nil {
		log.Fatal("ensure consumer group", zap.Error(err))
	}

	ltCfg := cfg.Stream
	ltCfg.ConsumerGroup = ltGroup
	ltCfg.ConsumerName = ltConsumer

	workerCfg := cfg.Worker
	workerCfg.Concurrency = 4

	state := &phaseState{}

	handler := func(_ context.Context, msg stream.Message) error {
		errRate, delay, allFail := state.get()

		if delay > 0 {
			time.Sleep(delay)
		}

		if allFail || rand.Float64() < errRate {
			errored.Add(1)
			return errors.New("simulated error")
		}

		consumed.Add(1)
		return nil
	}

	publisher := stream.NewPublisher(rdb, cfg.Stream, cfg.Worker, metrics)

	worker := stream.NewWorker(rdb, ltCfg, workerCfg, handler, metrics)
	worker.StartLagPoller(ctx, metrics.ConsumerLag)

	workerCtx, cancelWorker := context.WithCancel(ctx)
	worker.Start(workerCtx)

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║            redis-stream-go  —  load test                        ║")
	fmt.Println("║  Grafana: http://localhost:3000  Prometheus: http://localhost:9090 ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	totalScenarios := len(scenarios)
	for i, sc := range scenarios {
		if ctx.Err() != nil {
			break
		}

		fmt.Printf("─── [%d/%d] %s (%s)\n", i+1, totalScenarios, sc.name, sc.duration)
		fmt.Printf("    %s\n\n", sc.description)

		state.set(sc.errRate, sc.delay, sc.allFail)

		published.Store(0)
		consumed.Store(0)
		errored.Store(0)
		dlqd.Store(0)

		phaseCtx, phaseCancel := context.WithTimeout(ctx, sc.duration)

		// publisher goroutine
		go func(rate int) {
			if rate == 0 {
				return
			}
			interval := time.Second / time.Duration(rate)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-phaseCtx.Done():
					return
				case <-ticker.C:
					vals := map[string]any{
						"scenario":  sc.name,
						"ts":        time.Now().UnixMilli(),
						"seq":       published.Load(),
					}
					if pubErr := publisher.Publish(phaseCtx, vals); pubErr == nil {
						published.Add(1)
					}
				}
			}
		}(sc.publishRate)

		// progress ticker
		prog := time.NewTicker(5 * time.Second)
		elapsed := time.NewTicker(sc.duration)
		start := time.Now()

		done := false
		for !done {
			select {
			case <-ctx.Done():
				done = true
			case <-elapsed.C:
				done = true
			case <-prog.C:
				pct := int(time.Since(start).Seconds() / sc.duration.Seconds() * 100)
				bar := progressBar(pct)
				fmt.Printf("    %s %3d%%  pub=%-6d ok=%-6d err=%-6d dlq=%-6d\n",
					bar,
					pct,
					published.Load(),
					consumed.Load(),
					errored.Load(),
					dlqd.Load(),
				)
			}
		}

		prog.Stop()
		elapsed.Stop()
		phaseCancel()

		fmt.Printf("\n    done  pub=%-6d ok=%-6d err=%-6d dlq=%-6d\n\n",
			published.Load(),
			consumed.Load(),
			errored.Load(),
			dlqd.Load(),
		)

		if ctx.Err() == nil && i < totalScenarios-1 {
			fmt.Println("    pausing 3s before next scenario...")
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
			}
		}
	}

	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Load test complete. Check Grafana for metric patterns.          ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	cancelWorker()

	done := make(chan struct{})
	go func() {
		worker.Wait()
		close(done)
	}()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), cfg.Worker.ShutdownTimeout)
	defer shutCancel()
	select {
	case <-done:
	case <-shutCtx.Done():
		log.Warn("worker shutdown timed out")
	}

	if err := rdb.Close(); err != nil && ctx.Err() == nil {
		log.Error("redis close", zap.Error(err))
	}

	os.Exit(0)
}

func progressBar(pct int) string {
	const width = 20
	filled := pct * width / 100
	bar := make([]rune, width)
	for i := range bar {
		if i < filled {
			bar[i] = '█'
		} else {
			bar[i] = '░'
		}
	}
	return "[" + string(bar) + "]"
}

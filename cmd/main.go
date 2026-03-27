package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"redis-stream-go/internal/config"
	"redis-stream-go/internal/health"
	"redis-stream-go/internal/logger"
	"redis-stream-go/internal/observability"
	"redis-stream-go/internal/stream"
)

func main() {
	_ = config.LoadDotEnv() // best-effort local development support

	cfg := config.Load()

	log, err := logger.New(cfg.Log.Level, cfg.Log.JSON)
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	ctx := logger.WithContext(context.Background(), log)

	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)

	tracer, shutdownTracer, err := observability.NewTracer(ctx, "redis-stream-go")
	if err != nil {
		log.Fatal("tracer init failed", zap.Error(err))
	}
	_ = tracer

	rdb := stream.NewClient(cfg.Redis)

	if err := stream.EnsureConsumerGroup(ctx, rdb, cfg.Stream.Name, cfg.Stream.ConsumerGroup); err != nil {
		log.Fatal("ensure consumer group failed", zap.Error(err))
	}

	publisher := stream.NewPublisher(rdb, cfg.Stream, cfg.Worker, metrics)
	_ = publisher

	handler := func(ctx context.Context, msg stream.Message) error {
		log := logger.FromContext(ctx)
		log.Info("processing message", zap.String("msg_id", msg.ID), zap.Any("values", msg.Values))
		return nil
	}

	worker := stream.NewWorker(rdb, cfg.Stream, cfg.Worker, handler, metrics)

	metricsHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	healthServer := health.NewServer(cfg.HTTP.Addr, rdb, metricsHandler)

	workerCtx, cancelWorker := context.WithCancel(ctx)

	worker.StartLagPoller(workerCtx, metrics.ConsumerLag)
	worker.Start(workerCtx)

	go func() {
		if err := healthServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("health server error", zap.Error(err))
		}
	}()

	log.Info("service started", zap.String("addr", cfg.HTTP.Addr))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutdown initiated")

	cancelWorker()

	done := make(chan struct{})
	go func() {
		worker.Wait()
		close(done)
	}()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Worker.ShutdownTimeout)
	defer cancelShutdown()

	select {
	case <-done:
	case <-shutdownCtx.Done():
		log.Warn("worker shutdown timed out")
	}

	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelHTTP()
	if err := healthServer.Shutdown(httpCtx); err != nil {
		log.Error("health server shutdown error", zap.Error(err))
	}

	tracerCtx, cancelTracer := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTracer()
	if err := shutdownTracer(tracerCtx); err != nil {
		log.Error("tracer shutdown error", zap.Error(err))
	}

	if err := rdb.Close(); err != nil {
		log.Error("redis close error", zap.Error(err))
	}

	log.Info("shutdown complete")
}

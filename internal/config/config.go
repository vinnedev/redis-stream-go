package config

import (
	"os"
	"strconv"
	"time"
)

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxRetries   int
	PoolSize     int
	MinIdleConns int
}

type StreamConfig struct {
	Name             string
	ConsumerGroup    string
	ConsumerName     string
	MaxLen           int64
	ReadCount        int64
	BlockTimeout     time.Duration
	DeadLetterStream string
}

type WorkerConfig struct {
	Concurrency     int
	RetryAttempts   int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	ShutdownTimeout time.Duration
}

type HTTPConfig struct {
	Addr string
}

type LogConfig struct {
	Level string
	JSON  bool
}

type Config struct {
	Redis  RedisConfig
	Stream StreamConfig
	Worker WorkerConfig
	HTTP   HTTPConfig
	Log    LogConfig
}

func Load() Config {
	return Config{
		Redis: RedisConfig{
			Addr:         env("REDIS_ADDR", "localhost:6379"),
			Password:     env("REDIS_PASSWORD", ""),
			DB:           envInt("REDIS_DB", 0),
			DialTimeout:  envDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  envDuration("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: envDuration("REDIS_WRITE_TIMEOUT", 3*time.Second),
			MaxRetries:   envInt("REDIS_MAX_RETRIES", 3),
			PoolSize:     envInt("REDIS_POOL_SIZE", 10),
			MinIdleConns: envInt("REDIS_MIN_IDLE_CONNS", 2),
		},
		Stream: StreamConfig{
			Name:             env("STREAM_NAME", "events"),
			ConsumerGroup:    env("STREAM_CONSUMER_GROUP", "workers"),
			ConsumerName:     env("STREAM_CONSUMER_NAME", "worker"),
			MaxLen:           envInt64("STREAM_MAX_LEN", 10000),
			ReadCount:        envInt64("STREAM_READ_COUNT", 10),
			BlockTimeout:     envDuration("STREAM_BLOCK_TIMEOUT", 2*time.Second),
			DeadLetterStream: env("STREAM_DEAD_LETTER", "events.dlq"),
		},
		Worker: WorkerConfig{
			Concurrency:     envInt("WORKER_CONCURRENCY", 4),
			RetryAttempts:   envInt("WORKER_RETRY_ATTEMPTS", 3),
			RetryBaseDelay:  envDuration("WORKER_RETRY_BASE_DELAY", 100*time.Millisecond),
			RetryMaxDelay:   envDuration("WORKER_RETRY_MAX_DELAY", 10*time.Second),
			ShutdownTimeout: envDuration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		HTTP: HTTPConfig{
			Addr: env("HTTP_ADDR", ":8080"),
		},
		Log: LogConfig{
			Level: env("LOG_LEVEL", "info"),
			JSON:  envBool("LOG_JSON", true),
		},
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

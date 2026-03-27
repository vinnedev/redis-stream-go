package logger

import (
	"context"

	"go.uber.org/zap"
)

type contextKey struct{}

func New(level string, json bool) (*zap.Logger, error) {
	if !json {
		cfg := zap.NewDevelopmentConfig()
		if err := cfg.Level.UnmarshalText([]byte(level)); err != nil {
			return nil, err
		}
		return cfg.Build()
	}

	cfg := zap.NewProductionConfig()
	if err := cfg.Level.UnmarshalText([]byte(level)); err != nil {
		return nil, err
	}
	return cfg.Build()
}

func WithContext(ctx context.Context, log *zap.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, log)
}

func FromContext(ctx context.Context) *zap.Logger {
	if log, ok := ctx.Value(contextKey{}).(*zap.Logger); ok {
		return log
	}
	return zap.NewNop()
}

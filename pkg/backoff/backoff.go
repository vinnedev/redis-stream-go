package backoff

import (
	"math"
	"math/rand"
	"time"
)

type Config struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     float64
}

func DefaultConfig() Config {
	return Config{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   10 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.3,
	}
}

func (c Config) Delay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 0
	}

	exp := math.Pow(c.Multiplier, float64(attempt))
	delay := float64(c.BaseDelay) * exp

	if delay > float64(c.MaxDelay) {
		delay = float64(c.MaxDelay)
	}

	if c.Jitter > 0 {
		jitter := delay * c.Jitter * rand.Float64()
		delay = delay - (delay * c.Jitter / 2) + jitter
	}

	if delay < 0 {
		return c.BaseDelay
	}

	return time.Duration(delay)
}

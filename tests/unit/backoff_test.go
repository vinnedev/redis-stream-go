package unit

import (
	"testing"
	"time"

	"redis-stream-go/pkg/backoff"
)

func TestDelayIncreasesWithAttempts(t *testing.T) {
	cfg := backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   10 * time.Second,
		Multiplier: 2.0,
		Jitter:     0,
	}

	d0 := cfg.Delay(0)
	d1 := cfg.Delay(1)
	d2 := cfg.Delay(2)

	if d1 <= d0 {
		t.Errorf("expected delay(1) > delay(0), got %v <= %v", d1, d0)
	}
	if d2 <= d1 {
		t.Errorf("expected delay(2) > delay(1), got %v <= %v", d2, d1)
	}
}

func TestDelayCapAtMaxDelay(t *testing.T) {
	cfg := backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   500 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     0,
	}

	d := cfg.Delay(100)
	if d > cfg.MaxDelay {
		t.Errorf("expected delay <= MaxDelay %v, got %v", cfg.MaxDelay, d)
	}
}

func TestDelayAttemptZeroIsPositive(t *testing.T) {
	cfg := backoff.DefaultConfig()
	d := cfg.Delay(0)
	if d <= 0 {
		t.Errorf("expected positive delay for attempt=0, got %v", d)
	}
}

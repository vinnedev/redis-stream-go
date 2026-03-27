package unit

import (
	"testing"
	"time"

	"github.com/vinnedev/redis-stream-go/pkg/circuitbreaker"
)

func TestInitiallyClosedAndAllowsRequests(t *testing.T) {
	b := circuitbreaker.New(circuitbreaker.DefaultConfig())

	if b.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected StateClosed, got %s", b.State())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestOpensAfterMaxFailures(t *testing.T) {
	cfg := circuitbreaker.Config{
		MaxFailures:  3,
		ResetTimeout: 30 * time.Second,
		HalfOpenMax:  1,
	}
	b := circuitbreaker.New(cfg)

	for i := 0; i < cfg.MaxFailures; i++ {
		b.Failure()
	}

	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen, got %s", b.State())
	}
	if err := b.Allow(); err != circuitbreaker.ErrOpen {
		t.Fatalf("expected ErrOpen, got %v", err)
	}
}

func TestTransitionsToHalfOpenAfterResetTimeout(t *testing.T) {
	cfg := circuitbreaker.Config{
		MaxFailures:  1,
		ResetTimeout: 10 * time.Millisecond,
		HalfOpenMax:  1,
	}
	b := circuitbreaker.New(cfg)
	b.Failure()

	if b.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen, got %s", b.State())
	}

	time.Sleep(20 * time.Millisecond)

	if err := b.Allow(); err != nil {
		t.Fatalf("expected nil after reset timeout, got %v", err)
	}
	if b.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %s", b.State())
	}
}

func TestSuccessClosesFromHalfOpen(t *testing.T) {
	cfg := circuitbreaker.Config{
		MaxFailures:  1,
		ResetTimeout: 10 * time.Millisecond,
		HalfOpenMax:  1,
	}
	b := circuitbreaker.New(cfg)
	b.Failure()
	time.Sleep(20 * time.Millisecond)
	b.Allow()

	b.Success()

	if b.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected StateClosed after Success(), got %s", b.State())
	}
}

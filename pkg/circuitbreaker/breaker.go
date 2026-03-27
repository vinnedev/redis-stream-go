package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	StateClosed   State = iota // 0
	StateOpen     State = iota // 1
	StateHalfOpen State = iota // 2
)

var stateNames = []string{"closed", "open", "half-open"}

func (s State) String() string {
	if int(s) >= len(stateNames) {
		return "unknown"
	}
	return stateNames[s]
}

var ErrOpen = errors.New("circuit breaker is open")

type Config struct {
	MaxFailures  int
	ResetTimeout time.Duration
	HalfOpenMax  int
}

func DefaultConfig() Config {
	return Config{
		MaxFailures:  5,
		ResetTimeout: 30 * time.Second,
		HalfOpenMax:  1,
	}
}

type Breaker struct {
	mu            sync.RWMutex
	cfg           Config
	state         State
	failures      int
	halfOpenCount int
	lastFailure   time.Time
}

func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg}
}

func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateClosed {
		return nil
	}

	if b.state == StateOpen {
		if time.Since(b.lastFailure) >= b.cfg.ResetTimeout {
			b.state = StateHalfOpen
			b.halfOpenCount = 0
		} else {
			return ErrOpen
		}
	}

	if b.state == StateHalfOpen {
		if b.halfOpenCount >= b.cfg.HalfOpenMax {
			return ErrOpen
		}
		b.halfOpenCount++
		return nil
	}

	return nil
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.state = StateClosed
	b.failures = 0
	b.halfOpenCount = 0
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastFailure = time.Now()

	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.halfOpenCount = 0
		return
	}

	b.failures++
	if b.failures >= b.cfg.MaxFailures {
		b.state = StateOpen
	}
}

func (b *Breaker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

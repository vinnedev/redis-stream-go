package stream

import (
	"context"
	"time"
)

type Message struct {
	ID         string
	Stream     string
	Values     map[string]any
	Attempt    int
	ReceivedAt time.Time
}

type Handler func(ctx context.Context, msg Message) error

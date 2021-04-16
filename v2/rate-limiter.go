package batcher

import "context"

const (
	rateLimiterPhaseUninitialized = iota
	rateLimiterPhaseStarted
	rateLimiterPhaseStopped
)

type RateLimiter interface {
	MaxCapacity() uint32
	Capacity() uint32
	GiveMe(target uint32)
	Start(ctx context.Context) error
	Stop()
}

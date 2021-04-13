package batcher

import "context"

const (
	rateLimiterPhaseUninitialized = iota
	rateLimiterPhaseProvisioned
	rateLimiterPhaseStarted
	rateLimiterPhaseStopped
)

type IRateLimiter interface {
	Provision(ctx context.Context) error
	MaxCapacity() uint32
	Capacity() uint32
	GiveMe(target uint32)
	Start(ctx context.Context) error
	Stop()
}

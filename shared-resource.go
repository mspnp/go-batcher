package batcher

import "context"

type SharedResource interface {
	Provision(ctx context.Context) error
	MaxCapacity() uint32
	Capacity() uint32
	GiveMe(target uint32)
	Start(ctx context.Context)
	Stop()
}

package batcher

import (
	"context"
	"time"
)

type LeaseManager interface {
	emit(event string, val int, msg string, metadata interface{})
	provision(ctx context.Context) (err error)
	createPartitions(ctx context.Context, count int) (err error)
	leasePartition(ctx context.Context, id string, index uint32, secondsToLease uint32) (leaseTime time.Duration)
}

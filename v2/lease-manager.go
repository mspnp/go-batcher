package batcher

import (
	"context"
	"time"
)

type LeaseManager interface {
	Provision(ctx context.Context) (err error)
	CreatePartitions(ctx context.Context, count int) (err error)
	LeasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration)
}

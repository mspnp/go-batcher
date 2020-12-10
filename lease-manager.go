package batcher

import (
	"context"
	"time"
)

type leaseManager interface {
	emit(event string, val int, msg *string)
	provision(ctx context.Context) (err error)
	createPartitions(ctx context.Context, count int) (err error)
	leasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration)
}

/*
type mockLeaseManager struct {
	repeater
}

func newMockLeaseManager(parent ieventer) *mockLeaseManager {
	mgr := &mockLeaseManager{}
	mgr.parent = parent
	return mgr
}

func (m *mockLeaseManager) provision(ctx context.Context) (err error) {
	m.emit("test:provision", 0, nil)
	return
}

func (m *mockLeaseManager) createPartitions(ctx context.Context, count int) (err error) {
	for i := 0; i < count; i++ {
		m.emit("test:create-partition", 0, nil)
	}
	return
}

func (m *mockLeaseManager) leasePartition(ctx context.Context, id string, index uint32) (leaseTime time.Duration) {
	m.emit("test:lease-partition", int(index), nil)
	return 15 * time.Second
}
*/

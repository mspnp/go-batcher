package batcher

import (
	"context"
	"sync/atomic"
)

type ProvisionedResource struct {
	eventer
	maxCapacity uint32
}

func NewProvisionedResource(capacity uint32) *ProvisionedResource {
	return &ProvisionedResource{
		maxCapacity: capacity,
	}
}

func (r *ProvisionedResource) MaxCapacity() uint32 {
	return atomic.LoadUint32(&r.maxCapacity)
}

func (r *ProvisionedResource) Capacity() uint32 {
	return atomic.LoadUint32(&r.maxCapacity)
}

func (r *ProvisionedResource) SetCapacity(capacity uint32) {
	atomic.StoreUint32(&r.maxCapacity, capacity)
}

func (r *ProvisionedResource) GiveMe(target uint32) {
	// nothing to do
}

func (r *ProvisionedResource) Start(ctx context.Context) error {
	r.emit(CapacityEvent, int(r.MaxCapacity()), "", nil)
	return nil
}

func (r *ProvisionedResource) Stop() {
	r.emit(ShutdownEvent, 0, "", nil)
}

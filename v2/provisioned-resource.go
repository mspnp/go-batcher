package batcher

import (
	"context"
	"sync/atomic"
)

type ProvisionedResource interface {
	ieventer
	RateLimiter
	SetCapacity(capacity uint32)
}

type provisionedResource struct {
	eventer
	maxCapacity uint32
}

// This function should be called to create a new NewProvisionedResource. A ProvisionedResource is a rate limiter that restricts
// Operations based on the capacity specified. This is ReservedCapacity, not SharedCapacity (which this rate limiter does not address).
func NewProvisionedResource(capacity uint32) ProvisionedResource {
	return &provisionedResource{
		maxCapacity: capacity,
	}
}

// This returns the maximum capacity that could ever be obtained by the rate limiter. It is the capacity number provided when
// NewProvisionedResource() is called.
func (r *provisionedResource) MaxCapacity() uint32 {
	return atomic.LoadUint32(&r.maxCapacity)
}

// This returns the current allocated capacity. It is the capacity number provided when NewProvisionedResource() is called.
func (r *provisionedResource) Capacity() uint32 {
	return atomic.LoadUint32(&r.maxCapacity)
}

// This allows you to set the ReservedCapacity to a different value after the RateLimiter has started.
func (r *provisionedResource) SetCapacity(capacity uint32) {
	atomic.StoreUint32(&r.maxCapacity, capacity)
	r.emit(CapacityEvent, int(capacity), "", nil)
}

// You should call GiveMe() to update the capacity you are requesting. You will always specify the new amount of capacity you require.
// For instance, if you have a large queue of records to process, you might call GiveMe() every time new records are added to the queue
// and every time a batch is completed. Another common pattern is to call GiveMe() on a timer to keep it generally consistent with the
// capacity you need.
func (r *provisionedResource) GiveMe(target uint32) {
	// nothing to do
}

// Call this method to emit the starting capacity. There is no processing loop for this rate limiter.
func (r *provisionedResource) Start(ctx context.Context) error {
	r.emit(CapacityEvent, int(r.MaxCapacity()), "", nil)
	return nil
}

// Call this method to stop the processing loop. You may not restart after stopping.
func (r *provisionedResource) Stop() {
	r.emit(ShutdownEvent, 0, "", nil)
}

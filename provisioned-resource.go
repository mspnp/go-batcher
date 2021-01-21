package batcher

import "context"

type ProvisionedResource struct {
	eventer
	maxCapacity uint32
}

func NewProvisionedResource(capacity uint32) *ProvisionedResource {
	return &ProvisionedResource{
		maxCapacity: capacity,
	}
}

func (r *ProvisionedResource) Provision(ctx context.Context) error {
	return nil
}

func (r *ProvisionedResource) MaxCapacity() uint32 {
	return r.maxCapacity
}

func (r *ProvisionedResource) Capacity() uint32 {
	return r.maxCapacity
}

func (r *ProvisionedResource) GiveMe(target uint32) {
	// nothing to do
}

func (r *ProvisionedResource) Start(ctx context.Context) error {
	r.emit("capacity", int(r.MaxCapacity()), "", nil)
	return nil
}

func (r *ProvisionedResource) Stop() {
	r.emit("shutdown", 0, "", nil)
}

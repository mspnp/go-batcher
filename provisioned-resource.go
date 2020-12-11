package batcher

import "context"

type ProvisionedResource struct {
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
	// nothing to do
	return nil
}

func (r *ProvisionedResource) Stop() {
	// nothing to do
}

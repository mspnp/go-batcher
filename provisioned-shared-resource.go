package batcher

import "context"

type ProvisionedSharedResource struct {
	maxCapacity uint32
}

func NewProvisionedSharedResource() *ProvisionedSharedResource {
	return &ProvisionedSharedResource{}
}

func (r *ProvisionedSharedResource) WithCapacity(val uint32) *ProvisionedSharedResource {
	r.maxCapacity = val
	return r
}

func (r *ProvisionedSharedResource) Provision(ctx context.Context) error {
	return nil
}

func (r *ProvisionedSharedResource) MaxCapacity() uint32 {
	return r.maxCapacity
}

func (r *ProvisionedSharedResource) Capacity() uint32 {
	return r.maxCapacity
}

func (r *ProvisionedSharedResource) GiveMe(target uint32) {
	// nothing to do
}

func (r *ProvisionedSharedResource) Start(ctx context.Context) error {
	// nothing to do
	return nil
}

func (r *ProvisionedSharedResource) Stop() {
	// nothing to do
}
